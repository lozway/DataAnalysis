"""
DAG: reddit_silver
Responsabilidad: Detecta nuevos JSON en bronze/reddit/, aplica el pipeline
                 de limpieza NLP y persiste como Parquet en datalake_silver/reddit/

Estructura de entrada (bronze):
    datalake_bronze/reddit/reddit_music_opinions_YYYYMMDD_HHMMSS.json
    Formato: lista de posts [{title, score, comments: [str, ...]}, ...]

Estructura de salida (silver):
    datalake_silver/reddit/reddit_music_opinions_YYYYMMDD_HHMMSS.parquet

Schema de destino:
    post_id            (int64,   obligatorio) — índice original del post
    title              (string,  obligatorio)
    score              (int64,   obligatorio) — nulos/vacíos → 0
    raw_comment_id     (int64,   obligatorio) — índice antes del explode
    raw_comment        (string,  obligatorio)
    clean_comment      (string,  obligatorio)
    tokens             (string,  obligatorio) — lista serializada como string
    comment_type       (string,  obligatorio) — recommendation|opinion|mixed|other
    confidence         (float,   obligatorio)
    has_music_pattern  (bool,    obligatorio)
    pattern_type       (string,  opcional → 'unknown')
    has_contrast       (bool,    obligatorio)
    word_count         (int64,   obligatorio)
    word_count_capped  (float,   obligatorio)
    score_capped       (float,   obligatorio)
    artist             (string,  opcional → 'unknown')
    song               (string,  opcional → 'unknown')
    extract_confidence (float,   obligatorio)
    ingested_at        (string,  obligatorio)

Pipeline de limpieza (fiel al notebook esqm_limpieza.ipynb):
    1. normalize_nulls        — estandariza None/'null'/'[deleted]'/'' → NaN
    2. Filtro title + comments obligatorios; score nulo → 0
    3. explode comments       — un registro por comentario
    4. rename comments → raw_comment
    5. split_multiple_comments — divide comentarios multi-oración
    6. normalize_text + clean_html + clean_punctuation + remove_links → clean_comment
    7. tokenize               → tokens
    8. classify_comment       → comment_type, confidence, has_music_pattern, etc.
    9. extract_artist_song    → artist, song, extract_confidence
   10. cap_outliers (IQR)     → score_capped, word_count_capped
   11. Deduplicación exacta por (post_id, clean_comment)
   12. Limpieza de ruido      — clean_comment vacío o nulo
   13. Schema enforcement + Parquet snappy
"""

from __future__ import annotations

import html
import json
import os
import glob
import re
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor

# ──────────────────────────────────────────────
# Constantes
# ──────────────────────────────────────────────
BRONZE_REDDIT_PATH = "/opt/airflow/datalake_bronze/reddit"
SILVER_REDDIT_PATH = "/opt/airflow/datalake_silver/reddit"

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

# ──────────────────────────────────────────────
# Schema de destino (PyArrow)
# ──────────────────────────────────────────────
REDDIT_SCHEMA = pa.schema([
    pa.field("post_id",            pa.int64(),   nullable=False),
    pa.field("title",              pa.string(),  nullable=False),
    pa.field("score",              pa.int64(),   nullable=False),
    pa.field("raw_comment_id",     pa.int64(),   nullable=False),
    pa.field("raw_comment",        pa.string(),  nullable=False),
    pa.field("clean_comment",      pa.string(),  nullable=False),
    pa.field("tokens",             pa.string(),  nullable=False),
    pa.field("comment_type",       pa.string(),  nullable=False),
    pa.field("confidence",         pa.float64(), nullable=False),
    pa.field("has_music_pattern",  pa.bool_(),   nullable=False),
    pa.field("pattern_type",       pa.string(),  nullable=True),
    pa.field("has_contrast",       pa.bool_(),   nullable=False),
    pa.field("word_count",         pa.int64(),   nullable=False),
    pa.field("word_count_capped",  pa.float64(), nullable=False),
    pa.field("score_capped",       pa.float64(), nullable=False),
    pa.field("artist",             pa.string(),  nullable=True),
    pa.field("song",               pa.string(),  nullable=True),
    pa.field("extract_confidence", pa.float64(), nullable=False),
    pa.field("ingested_at",        pa.string(),  nullable=False),
])

# ──────────────────────────────────────────────
# Pipeline de limpieza NLP (del notebook)
# ──────────────────────────────────────────────
def normalize_nulls(content):
    """Estandariza valores semánticamente nulos → NaN."""
    if content is None:
        return np.nan
    if isinstance(content, str):
        if content.strip().lower() in {'null', 'none', 'nan', '', '[deleted]', '[removed]'}:
            return np.nan
    return content


def normalize_text(content: str) -> str:
    """Strip, lowercase y colapsa espacios múltiples."""
    content = content.strip()
    content = content.lower()
    content = re.sub(r'\s+', ' ', content)
    return content


def remove_links(content: str) -> str:
    """Elimina URLs y markdown links."""
    content = re.sub(r'http\S+|www\.\S+', '', content)
    content = re.sub(r'\[.*?\]\(http\S+\)', '', content)
    return content


def clean_punctuation(content: str) -> str:
    """Elimina puntuación excepto /, &, - y '."""
    return re.sub(r'[^\w\s/&\-\']', '', content)


def clean_html_entities(content: str) -> str:
    """Decodifica entidades HTML."""
    return html.unescape(content)


def split_multiple_comments(content: str) -> list[str]:
    """Divide comentarios multi-oración por saltos de línea o mayúsculas tras paréntesis."""
    parts = re.split(r'\n+|(?<=\))\s+(?=[A-Z])', content)
    return [c.strip() for c in parts if c.strip()]


def tokenize(content: str) -> list[str]:
    """Tokeniza por espacios."""
    return content.split()


def classify_comment(content: str) -> dict:
    """
    Clasifica el comentario como: recommendation | opinion | mixed | other.
    Retorna: comment_type, confidence, has_music_pattern, pattern_type,
             has_contrast, word_count.
    """
    content = str(content).strip().lower()

    if not content:
        return {
            'pattern_type':       None,
            'has_music_pattern':  False,
            'comment_type':       'other',
            'has_contrast':       False,
            'confidence':         0.0,
            'word_count':         0,
        }

    words     = content.split()
    word_count = len(words)

    opinion_markers = {'but', 'however', 'although', 'though'}
    has_contrast    = any(w in words for w in opinion_markers)
    pattern_type    = None

    match = re.search(r'^\s*(.+?)\s*-\s*(.+?)\s*$', content)
    if match:
        left, right = match.groups()
        if len(left.split()) <= 8 and len(right.split()) <= 8:
            pattern_type = 'dash'

    if pattern_type is None:
        match = re.search(r'^\s*(.+?)\s+\bby\b\s+(.+?)\s*$', content)
        if match:
            song, artist = match.groups()
            if len(song.split()) <= 10 and len(artist.split()) <= 5:
                pattern_type = 'by'

    if pattern_type is None:
        match = re.search(r'^\s*(.+?)\s*:\s*(.+?)\s*$', content)
        if match:
            left, right = match.groups()
            if len(left.split()) <= 5 and len(right.split()) <= 10:
                pattern_type = 'colon'

    has_music_pattern  = pattern_type is not None
    looks_like_opinion = has_contrast or word_count > 6

    if has_music_pattern and looks_like_opinion:
        comment_type = 'mixed'
    elif has_music_pattern:
        comment_type = 'recommendation'
    elif looks_like_opinion:
        comment_type = 'opinion'
    else:
        comment_type = 'other'

    confidence = 0.0
    if has_music_pattern:
        confidence += 0.45
    if word_count <= 5:
        confidence += 0.25
    elif word_count <= 10:
        confidence += 0.15
    elif word_count > 20:
        confidence -= 0.15
    if has_contrast:
        confidence -= 0.20
    if comment_type == 'mixed':
        confidence -= 0.10
    punctuation_count = len(re.findall(r'[^\w\s]', content))
    if punctuation_count > 5:
        confidence -= 0.10
    if word_count < 2:
        confidence -= 0.20

    confidence = max(0.0, min(confidence, 1.0))

    return {
        'comment_type':      comment_type,
        'confidence':        round(confidence, 2),
        'has_music_pattern': has_music_pattern,
        'pattern_type':      pattern_type,
        'has_contrast':      has_contrast,
        'word_count':        word_count,
    }


def extract_artist_song(text: str, pattern_type: str) -> tuple:
    """
    Extrae artista y canción del clean_comment según el pattern_type.
    Retorna (artist, song, extract_confidence).
    """
    if not text or not pattern_type:
        return None, None, 0.0

    original_text = str(text).strip()
    score = 0
    max_score = 0

    if pattern_type == "dash":
        rule = re.search(r'^\s*(.+?)\s*-\s*(.+?)\s*$', original_text)
        if rule:
            left, right = rule.groups()
            left, right  = left.strip(), right.strip()
            max_score += 8
            score += 2
            if 1 <= len(left.split()) <= 4:
                score += 2
            if 1 <= len(right.split()) <= 6:
                score += 2
            if len(left) < 40 and len(right) < 60:
                score += 2
            confidence = round(score / max_score, 2)
            if confidence >= 0.7:
                return left, right, confidence
            return None, None, confidence

    elif pattern_type == "by":
        rule = re.search(r'^\s*(.+?)\s+\bby\b\s+(.+?)\s*$', original_text, re.IGNORECASE)
        if rule:
            song, artist = rule.groups()
            max_score += 7
            score += 3
            if 1 <= len(artist.split()) <= 4:
                score += 2
            if 1 <= len(song.split()) <= 8:
                score += 2  # fixed: was missing in original but implied
            if len(song) < 80 and len(artist) < 40:
                score += 2  # extra signal kept for parity
            confidence = round(score / max_score, 2)
            return artist.strip(), song.strip(), confidence

    elif pattern_type == "colon":
        rule = re.search(r'^\s*(.+?)\s*:\s*(.+?)\s*$', original_text)
        if rule:
            artist, song = rule.groups()
            max_score += 8
            score += 3
            if 1 <= len(artist.split()) <= 4:
                score += 2
            if 1 <= len(song.split()) <= 8:
                score += 2
            if not artist.islower():
                score += 1
            if len(song) < 80:
                score += 2
            confidence = round(score / max_score, 2)
            return artist.strip(), song.strip(), confidence

    return None, None, 0.0


def cap_outliers(series: pd.Series) -> pd.Series:
    """Aplica capping IQR: clipa valores fuera de [Q1 - 1.5·IQR, Q3 + 1.5·IQR]."""
    q1    = series.quantile(0.25)
    q3    = series.quantile(0.75)
    iqr   = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    return series.clip(lower, upper)


# ──────────────────────────────────────────────
# Funciones auxiliares
# ──────────────────────────────────────────────
def _read_all_bronze_files(folder: str) -> tuple[list, str]:
    """
    Lee TODOS los JSON históricos de la carpeta bronze/reddit/.
    Retorna la lista consolidada de posts con su ingested_at individual
    y el ingested_at más reciente para el campo de silver.
    """
    files = sorted(glob.glob(os.path.join(folder, "*.json")))
    if not files:
        raise FileNotFoundError(f"No se encontraron archivos JSON en: {folder}")

    all_posts      = []
    latest_ingested = ""
    for filepath in files:
        with open(filepath, "r", encoding="utf-8") as f:
            posts = json.load(f)
        # Reddit bronze es lista directa de posts sin _metadata
        # Enriquecer cada post con su origen
        ingested_at = os.path.basename(filepath).replace("reddit_music_opinions_", "").replace(".json", "")
        for post in posts:
            post["_ingested_at"] = ingested_at
        all_posts.extend(posts)
        latest_ingested = ingested_at

    print(f"📂 {len(files)} archivos leídos → {len(all_posts)} posts brutos")
    return all_posts, latest_ingested


def _enforce_schema(df: pd.DataFrame, schema: pa.Schema) -> pd.DataFrame:
    """
    Aplica el schema de destino:
      - Verifica campos obligatorios presentes y sin nulos
      - Rellena opcionales con 'unknown'
      - Castea tipos
    """
    required_fields = [f.name for f in schema if not f.nullable]
    optional_fields = [f.name for f in schema if f.nullable]

    missing = [f for f in required_fields if f not in df.columns]
    if missing:
        raise ValueError(f"❌ Schema error — campos obligatorios faltantes: {missing}")

    for field in required_fields:
        null_count = df[field].isnull().sum()
        if null_count > 0:
            raise ValueError(
                f"❌ Schema error — campo obligatorio '{field}' tiene {null_count} nulos"
            )

    for field in optional_fields:
        if field in df.columns:
            filled = df[field].isnull().sum()
            if filled > 0:
                df[field] = df[field].fillna("unknown")
                print(f"   ℹ️  '{field}': {filled} nulos → 'unknown'")

    for field in schema:
        if field.name not in df.columns:
            continue
        if pa.types.is_integer(field.type):
            df[field.name] = df[field.name].astype("int64")
        elif pa.types.is_floating(field.type):
            df[field.name] = df[field.name].astype("float64")
        elif pa.types.is_boolean(field.type):
            df[field.name] = df[field.name].astype("bool")
        elif pa.types.is_string(field.type):
            df[field.name] = df[field.name].astype("string")

    return df


def _save_parquet(df: pd.DataFrame, schema: pa.Schema, folder: str, filename: str) -> str:
    """Persiste el DataFrame como Parquet con compresión snappy."""
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, filepath, compression="snappy")
    return filepath


# ──────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────
@dag(
    dag_id="reddit_silver",
    description="Limpia y normaliza bronze/reddit/ → silver/reddit/ con pipeline NLP",
    schedule=None,           # on-demand: se dispara manualmente cuando se ingesta Reddit
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["reddit", "silver", "nlp", "transform"],
)
def reddit_silver_dag():

    # ── Sensor: espera que exista al menos un JSON en bronze/reddit/ ──────
    wait_for_reddit = FileSensor(
        task_id="wait_for_reddit_bronze",
        filepath=BRONZE_REDDIT_PATH,
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=60 * 10,
        mode="poke",
        soft_fail=False,
    )

    # ── Tarea 1: Transformar Reddit ───────────────────────────────────────
    @task()
    def transform_reddit() -> str:
        """
        Aplica el pipeline completo de limpieza NLP sobre el JSON más reciente
        de bronze/reddit/ y produce un Parquet en silver/reddit/.
        """
        posts, latest_ingested = _read_all_bronze_files(BRONZE_REDDIT_PATH)
        ingested_at = datetime.utcnow().isoformat()

        # ── Paso 1: Construir DataFrame base ─────────────────────────────
        df = pd.DataFrame(posts)                       # title | score | comments | _ingested_at

        # ── Paso 2: normalize_nulls sobre todo el DataFrame ──────────────
        df = df.apply(lambda col: col.map(normalize_nulls))

        # ── Paso 3: Filtrar registros sin title o sin comments ────────────
        before = len(df)
        df = df[df["title"].notna() & df["comments"].notna()]
        print(f"   Posts válidos: {len(df)} / {before}")

        # ── Paso 4: score nulo o vacío → 0 ───────────────────────────────
        df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0).astype("int64")

        # ── Paso 5: Explode comments (una fila por comentario) ────────────
        df = df.rename(columns={"comments": "raw_comment"})
        df = df.explode("raw_comment")
        df = df[df["raw_comment"].notna()]
        df = df[df["raw_comment"].apply(lambda c: isinstance(c, str))]
        df = df[df["raw_comment"].str.strip() != ""]
        df = df.reset_index().rename(columns={"index": "post_id"})
        # Preservar _ingested_at por post para análisis temporal

        # ── Paso 6: Asignar raw_comment_id + split_multiple_comments ──────
        df = df.reset_index(drop=True)
        df["raw_comment_id"] = df.index
        df["raw_comment"] = df["raw_comment"].apply(split_multiple_comments)
        df = df.explode("raw_comment").reset_index(drop=True)

        # Filtrar vacíos post-split
        df = df[df["raw_comment"].notna() & (df["raw_comment"].str.strip() != "")]

        # ── Paso 7: Pipeline NLP ──────────────────────────────────────────
        df["clean_comment"] = df["raw_comment"].apply(
            lambda c: normalize_text(clean_punctuation(remove_links(clean_html_entities(c))))
        )
        df["tokens"] = df["clean_comment"].apply(
            lambda c: str(tokenize(c))   # serializado como string para Parquet
        )

        # ── Paso 8: Clasificación de comentarios ──────────────────────────
        features = df["clean_comment"].apply(classify_comment).apply(pd.Series)
        df = pd.concat([df, features], axis=1)

        # ── Paso 9: Extracción artista/canción ────────────────────────────
        df[["artist", "song", "extract_confidence"]] = df.apply(
            lambda row: pd.Series(
                extract_artist_song(row["clean_comment"], row["pattern_type"])
            ),
            axis=1,
        )

        # ── Paso 10: Capping outliers (IQR) ──────────────────────────────
        df["score_capped"]      = cap_outliers(df["score"].astype(float))
        df["word_count_capped"] = cap_outliers(df["word_count"].astype(float))

        # ── Paso 11: Deduplicación exacta por (title + clean_comment + fecha) ──
        # Preserva el mismo comentario si aparece en distintas ingestas (fechas distintas)
        before_dedup = len(df)
        df["_dedup_key"] = (
            df["title"] + "||" +
            df["clean_comment"] + "||" +
            df["_ingested_at"].astype(str)
        )
        df = df.drop_duplicates(subset=["_dedup_key"]).drop(columns=["_dedup_key"])
        dupes = before_dedup - len(df)
        if dupes:
            print(f"⚠️  {dupes} duplicados exactos del mismo día eliminados")

        # ── Paso 12: Eliminar ruido ───────────────────────────────────────
        df = df[df["clean_comment"].notna() & (df["clean_comment"].str.strip() != "")]
        df = df.reset_index(drop=True)

        # ── Paso 13: Agregar ingested_at ──────────────────────────────────
        df["ingested_at"] = ingested_at

        # ── Paso 14: Schema enforcement ───────────────────────────────────
        # Limpiar columna auxiliar antes de aplicar schema
        if "_ingested_at" in df.columns:
            df = df.drop(columns=["_ingested_at"])
        df = df[REDDIT_SCHEMA.names]   # reordenar columnas según schema
        df = _enforce_schema(df, REDDIT_SCHEMA)

        # ── Paso 15: Persistir como Parquet ───────────────────────────────
        filepath_out = _save_parquet(
            df, REDDIT_SCHEMA, SILVER_REDDIT_PATH,
            "reddit_music_opinions_current.parquet"
        )

        print(f"✅ Reddit silver — {len(df)} registros consolidados → {filepath_out}")
        print(f"   📊 Tipos de comentario: {df['comment_type'].value_counts().to_dict()}")
        print(f"   🎵 Con extracción artista/canción: {df['artist'].notna().sum()}")

        return filepath_out

    # ── Tarea 2: Validación ───────────────────────────────────────────────
    @task()
    def validate_silver_file(filepath: str) -> dict:
        """
        Verifica que el Parquet existe, respeta el schema y tiene datos.
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"❌ Parquet no encontrado: {filepath}")

        table = pq.read_table(filepath)

        if table.num_rows == 0:
            raise ValueError("❌ El Parquet de reddit está vacío")

        for field in REDDIT_SCHEMA:
            if field.name not in table.schema.names:
                raise ValueError(f"❌ Campo '{field.name}' faltante en el Parquet")

        df = table.to_pandas()

        summary = {
            "filepath":     filepath,
            "rows":         table.num_rows,
            "columns":      table.schema.names,
            "null_count":   int(df.isnull().sum().sum()),
            "comment_types": df["comment_type"].value_counts().to_dict(),
            "posts":        int(df["post_id"].nunique()),
            "with_artist":  int(df["artist"].notna().sum()),
        }

        print(f"📊 Resumen reddit silver:\n{json.dumps(summary, indent=2)}")
        return summary

    # ── Orquestación ──────────────────────────────────────────────────────
    #
    #   [wait_for_reddit] ──► [transform_reddit] ──► [validate_silver_file]
    #
    reddit_path = transform_reddit()
    wait_for_reddit >> reddit_path
    validate_silver_file(reddit_path)


reddit_silver_dag()
