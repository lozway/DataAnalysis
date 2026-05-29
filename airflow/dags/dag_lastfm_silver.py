"""
DAG: lastfm_silver
Responsabilidad: Detecta nuevos JSON en bronze, los normaliza y persiste
                como Parquet limpio en datalake_silver/

Estructura de entrada (bronze):
    datalake_bronze/lastfm_top_artists/lastfm_top_artists_YYYYMMDD_HHMMSS.json
    datalake_bronze/lastfm_top_tracks/lastfm_top_tracks_YYYYMMDD_HHMMSS.json

Estructura de salida (silver):
    datalake_silver/lastfm_top_artists/lastfm_top_artists_YYYYMMDD_HHMMSS.parquet
    datalake_silver/lastfm_top_tracks/lastfm_top_tracks_YYYYMMDD_HHMMSS.parquet

Schema artists:
    name          (string, obligatorio)
    name_tokens   (string, obligatorio)
    playcount     (int64,  obligatorio)
    listeners     (int64,  obligatorio)
    mbid          (string, opcional → 'unknown')
    ingested_at   (string, obligatorio)

Schema tracks:
    name               (string, obligatorio)
    name_tokens        (string, obligatorio)
    duration_sec       (int64,  obligatorio)
    playcount          (int64,  obligatorio)
    listeners          (int64,  obligatorio)
    mbid               (string, opcional → 'unknown')
    artist_name        (string, obligatorio)
    artist_name_tokens (string, obligatorio)
    artist_mbid        (string, opcional → 'unknown')
    ingested_at        (string, obligatorio)

Limpieza de name_tokens:
    normalize_text → clean_html → clean_punctuation → remove_links

Manejo de duplicados (por prioridad):
    1. Duplicados exactos        → eliminar, conservar primero
    2. Mismo nombre, distinta ingesta → conservar el más reciente
    3. Mismo nombre, distinto playcount/listeners → conservar el mayor
"""

from __future__ import annotations

import html
import json
import os
import glob
import re
from datetime import datetime, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor

# ──────────────────────────────────────────────
# Constantes
# ──────────────────────────────────────────────
BRONZE_ARTISTS_PATH = "/opt/airflow/datalake_bronze/lastfm_top_artists"
BRONZE_TRACKS_PATH  = "/opt/airflow/datalake_bronze/lastfm_top_tracks"

SILVER_ARTISTS_PATH = "/opt/airflow/datalake_silver/lastfm_top_artists"
SILVER_TRACKS_PATH  = "/opt/airflow/datalake_silver/lastfm_top_tracks"

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

# ──────────────────────────────────────────────
# Schemas de destino (PyArrow)
# ──────────────────────────────────────────────
ARTISTS_SCHEMA = pa.schema([
    pa.field("name",         pa.string(),  nullable=False),
    pa.field("name_tokens",  pa.string(),  nullable=False),
    pa.field("playcount",    pa.int64(),   nullable=False),
    pa.field("listeners",    pa.int64(),   nullable=False),
    pa.field("mbid",         pa.string(),  nullable=True),
    pa.field("ingested_at",  pa.string(),  nullable=False),
])

TRACKS_SCHEMA = pa.schema([
    pa.field("name",               pa.string(), nullable=False),
    pa.field("name_tokens",        pa.string(), nullable=False),
    pa.field("duration_sec",       pa.int64(),  nullable=False),
    pa.field("playcount",          pa.int64(),  nullable=False),
    pa.field("listeners",          pa.int64(),  nullable=False),
    pa.field("mbid",               pa.string(), nullable=True),
    pa.field("artist_name",        pa.string(), nullable=False),
    pa.field("artist_name_tokens", pa.string(), nullable=False),
    pa.field("artist_mbid",        pa.string(), nullable=True),
    pa.field("ingested_at",        pa.string(), nullable=False),
])

# ──────────────────────────────────────────────
# Funciones de limpieza de texto
# ──────────────────────────────────────────────
def normalize_text(content: str) -> str:
    """Normaliza texto: strip, lowercase y colapsa espacios múltiples."""
    content = content.strip()
    content = content.lower()
    content = re.sub(r'\s+', ' ', content)
    return content


def remove_links(content: str) -> str:
    """Elimina URLs y links en formato markdown."""
    content = re.sub(r'http\S+|www\.\S+', '', content)
    content = re.sub(r'\[.*?\]\(http\S+\)', '', content)
    return content


def clean_punctuation(content: str) -> str:
    """Elimina puntuación excepto /, &, - y ' (relevantes en nombres de artistas)."""
    content = re.sub(r'[^\w\s/&\-\']', '', content)
    return content


def clean_html_entities(content: str) -> str:
    """Decodifica entidades HTML (ej: &amp; → &, &#39; → ')."""
    return html.unescape(content)


def build_name_tokens(name: str) -> str:
    """
    Pipeline completo de limpieza sobre un nombre.
    normalize_text → clean_html → clean_punctuation → remove_links
    """
    name = normalize_text(name)
    name = clean_html_entities(name)
    name = clean_punctuation(name)
    name = remove_links(name)
    return name.strip()


# ──────────────────────────────────────────────
# Manejo de duplicados
# ──────────────────────────────────────────────
def deduplicate(df: pd.DataFrame, key: str) -> tuple[pd.DataFrame, int]:
    """
    Elimina duplicados en tres pasos por prioridad:
    1. Duplicados exactos        → conservar el primero
    2. Mismo nombre, distinta ingesta → conservar el más reciente (ingested_at mayor)
    3. Mismo nombre, distinto playcount/listeners → conservar el de mayor playcount

    Retorna el DataFrame limpio y el total de duplicados eliminados.
    """
    before = len(df)

    # Paso 1 — duplicados exactos
    df = df.drop_duplicates(keep="first")

    # Paso 2 — mismo nombre, distinta fecha de ingesta → más reciente
    df = (
        df.sort_values("ingested_at", ascending=False)
        .drop_duplicates(subset=[key], keep="first")
    )

    # Paso 3 — mismo nombre, mayor playcount
    df = (
        df.sort_values("playcount", ascending=False)
        .drop_duplicates(subset=[key], keep="first")
    )

    dropped = before - len(df)
    return df.reset_index(drop=True), dropped


# ──────────────────────────────────────────────
# Funciones auxiliares
# ──────────────────────────────────────────────
def _get_latest_bronze_file(folder: str) -> str:
    """Retorna el JSON más reciente en la carpeta bronze indicada."""
    files = sorted(glob.glob(os.path.join(folder, "*.json")))
    if not files:
        raise FileNotFoundError(f"No se encontraron archivos JSON en: {folder}")
    return files[-1]


def _enforce_schema(df: pd.DataFrame, schema: pa.Schema) -> pd.DataFrame:
    """
    Aplica el schema de destino al DataFrame:
    - Verifica que todos los campos obligatorios estén presentes
    - Rellena nulos en campos opcionales con 'unknown'
    - Castea tipos según el schema
    - Lanza SchemaError si un campo obligatorio tiene nulos
    """
    required_fields = [f.name for f in schema if not f.nullable]
    optional_fields = [f.name for f in schema if f.nullable]

    # Verificar campos obligatorios presentes
    missing = [f for f in required_fields if f not in df.columns]
    if missing:
        raise ValueError(f"❌ Schema error — campos obligatorios faltantes: {missing}")

    # Nulos en campos obligatorios → error
    for field in required_fields:
        null_count = df[field].isnull().sum()
        if null_count > 0:
            raise ValueError(
                f"❌ Schema error — campo obligatorio '{field}' tiene {null_count} nulos"
            )

    # Nulos en campos opcionales → 'unknown'
    for field in optional_fields:
        if field in df.columns:
            filled = df[field].isnull().sum()
            if filled > 0:
                df[field] = df[field].fillna("unknown")
                print(f"   ℹ️  '{field}': {filled} nulos reemplazados con 'unknown'")

    # Castear tipos
    for field in schema:
        if field.name not in df.columns:
            continue
        if pa.types.is_integer(field.type):
            df[field.name] = df[field.name].astype("int64")
        elif pa.types.is_string(field.type):
            df[field.name] = df[field.name].astype("string")

    return df


def _save_parquet(df: pd.DataFrame, schema: pa.Schema, folder: str, filename: str) -> str:
    """Persiste el DataFrame como Parquet con el schema definido."""
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, filepath, compression="snappy")
    return filepath


# ──────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────
@dag(
    dag_id="lastfm_silver",
    description="Normaliza bronze → silver para top artists y top tracks de Last.fm",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lastfm", "silver", "transform"],
)
def lastfm_silver_dag():

    # ── Sensores ──────────────────────────────────────────────────────────
    wait_for_artists = FileSensor(
        task_id="wait_for_artists_bronze",
        filepath=BRONZE_ARTISTS_PATH,
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=60 * 10,
        mode="poke",
        soft_fail=False,
    )

    wait_for_tracks = FileSensor(
        task_id="wait_for_tracks_bronze",
        filepath=BRONZE_TRACKS_PATH,
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=60 * 10,
        mode="poke",
        soft_fail=False,
    )

    # ── Tarea 1: Transformar Top Artists ─────────────────────────────────
    @task()
    def transform_top_artists() -> str:
        filepath = _get_latest_bronze_file(BRONZE_ARTISTS_PATH)
        print(f"📂 Procesando: {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            content = json.load(f)

        ingested_at = content["_metadata"]["ingested_at"]
        artists     = content["data"]["artists"]["artist"]

        rows = []
        for artist in artists:
            name = artist.get("name", "").strip()
            rows.append({
                "name":        name,
                "name_tokens": build_name_tokens(name),
                "playcount":   int(artist.get("playcount", 0)),
                "listeners":   int(artist.get("listeners", 0)),
                "mbid":        artist.get("mbid") or None,
                "ingested_at": ingested_at,
            })

        df = pd.DataFrame(rows)

        # 1. Filtrar registros inválidos (campos obligatorios vacíos o playcount=0)
        before = len(df)
        df = df[df["name"].str.strip().ne("") & df["name"].notna()]
        df = df[df["playcount"] > 0]
        invalid = before - len(df)
        if invalid:
            print(f"⚠️  {invalid} registros inválidos descartados")

        # 2. Deduplicar
        df, dupes = deduplicate(df, key="name")
        if dupes:
            print(f"⚠️  {dupes} duplicados eliminados")

        # 3. Aplicar schema y castear tipos
        df = _enforce_schema(df, ARTISTS_SCHEMA)

        # 4. Persistir como Parquet
        timestamp   = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filepath_out = _save_parquet(
            df, ARTISTS_SCHEMA, SILVER_ARTISTS_PATH,
            f"lastfm_top_artists_{timestamp}.parquet"
        )

        print(f"✅ Artists silver — {len(df)} registros → {filepath_out}")
        print(f"   🎤 #1: {df.iloc[0]['name']} → tokens: '{df.iloc[0]['name_tokens']}' — listeners: {df.iloc[0]['listeners']:,}")

        return filepath_out

    # ── Tarea 2: Transformar Top Tracks ──────────────────────────────────
    @task()
    def transform_top_tracks() -> str:
        filepath = _get_latest_bronze_file(BRONZE_TRACKS_PATH)
        print(f"📂 Procesando: {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            content = json.load(f)

        ingested_at = content["_metadata"]["ingested_at"]
        tracks      = content["data"]["tracks"]["track"]

        rows = []
        for track in tracks:
            artist      = track.get("artist", {})
            name        = track.get("name", "").strip()
            artist_name = artist.get("name", "").strip()
            rows.append({
                "name":               name,
                "name_tokens":        build_name_tokens(name),
                "duration_sec":       int(track.get("duration", 0)),
                "playcount":          int(track.get("playcount", 0)),
                "listeners":          int(track.get("listeners", 0)),
                "mbid":               track.get("mbid") or None,
                "artist_name":        artist_name,
                "artist_name_tokens": build_name_tokens(artist_name),
                "artist_mbid":        artist.get("mbid") or None,
                "ingested_at":        ingested_at,
            })

        df = pd.DataFrame(rows)

        # 1. Filtrar registros inválidos
        before = len(df)
        df = df[
            df["name"].str.strip().ne("") & df["name"].notna() &
            df["artist_name"].str.strip().ne("") & df["artist_name"].notna()
        ]
        df = df[df["playcount"] > 0]
        invalid = before - len(df)
        if invalid:
            print(f"⚠️  {invalid} registros inválidos descartados")

        # 2. Deduplicar por nombre de track + artista
        df["_dedup_key"] = df["name"] + "||" + df["artist_name"]
        df, dupes = deduplicate(df, key="_dedup_key")
        df = df.drop(columns=["_dedup_key"])
        if dupes:
            print(f"⚠️  {dupes} duplicados eliminados")

        # 3. Aplicar schema y castear tipos
        df = _enforce_schema(df, TRACKS_SCHEMA)

        # 4. Persistir como Parquet
        timestamp    = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filepath_out = _save_parquet(
            df, TRACKS_SCHEMA, SILVER_TRACKS_PATH,
            f"lastfm_top_tracks_{timestamp}.parquet"
        )

        print(f"✅ Tracks silver — {len(df)} registros → {filepath_out}")
        print(f"   🎵 #1: {df.iloc[0]['name']} → tokens: '{df.iloc[0]['name_tokens']}' — {df.iloc[0]['artist_name']}")

        return filepath_out

    # ── Tarea 3: Validación conjunta ──────────────────────────────────────
    @task()
    def validate_silver_files(artists_filepath: str, tracks_filepath: str) -> dict:
        """
        Verifica que ambos Parquet existen, respetan el schema y tienen datos.
        """
        summary = {}

        for label, filepath, schema in [
            ("artists", artists_filepath, ARTISTS_SCHEMA),
            ("tracks",  tracks_filepath,  TRACKS_SCHEMA),
        ]:
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"❌ Parquet no encontrado: {filepath}")

            table = pq.read_table(filepath)

            if table.num_rows == 0:
                raise ValueError(f"❌ El Parquet de {label} está vacío")

            # Verificar schema
            for field in schema:
                if field.name not in table.schema.names:
                    raise ValueError(f"❌ Campo '{field.name}' faltante en {label}")

            df = table.to_pandas()

            summary[label] = {
                "filepath":   filepath,
                "rows":       table.num_rows,
                "columns":    table.schema.names,
                "top_name":   df.iloc[0]["name"],
                "null_count": int(df.isnull().sum().sum()),
            }

        print(f"📊 Resumen silver:\n{json.dumps(summary, indent=2)}")
        return summary

    # ── Orquestación ──────────────────────────────────────────────────────
    #
    #   [wait_for_artists] ──► [transform_top_artists] ──┐
    #                                                      ├──► [validate_silver_files]
    #   [wait_for_tracks]  ──► [transform_top_tracks]  ──┘
    #
    artists_path = transform_top_artists()
    tracks_path  = transform_top_tracks()

    wait_for_artists >> artists_path
    wait_for_tracks  >> tracks_path

    validate_silver_files(artists_path, tracks_path)


lastfm_silver_dag()