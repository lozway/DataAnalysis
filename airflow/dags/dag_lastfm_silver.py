"""
DAG: lastfm_silver
Responsabilidad: Detecta nuevos JSON en bronze, los normaliza y persiste
                como CSV limpio en datalake_silver/

Estructura de entrada (bronze):
    datalake_bronze/lastfm_top_artists/lastfm_top_artists_YYYYMMDD_HHMMSS.json
    datalake_bronze/lastfm_top_tracks/lastfm_top_tracks_YYYYMMDD_HHMMSS.json

Estructura de salida (silver):
    datalake_silver/lastfm_top_artists/lastfm_top_artists_YYYYMMDD_HHMMSS.csv
    datalake_silver/lastfm_top_tracks/lastfm_top_tracks_YYYYMMDD_HHMMSS.csv

Transformaciones aplicadas:
    Artists: name, name_tokens, playcount (int), listeners (int), mbid
    Tracks : name, name_tokens, duration_sec (int), playcount (int), listeners (int),
            mbid, artist_name, artist_name_tokens, artist_mbid

Limpieza de name_tokens:
    normalize_text → clean_html → clean_punctuation → remove_links
"""

from __future__ import annotations

import html
import json
import os
import glob
import re
from datetime import datetime, timedelta

import pandas as pd
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


def clean_html(content: str) -> str:
    """Decodifica entidades HTML (ej: &amp; → &, &#39; → ')."""
    return html.unescape(content)


def build_name_tokens(name: str) -> str:
    """
    Aplica el pipeline completo de limpieza sobre un nombre y retorna
    el texto normalizado listo para tokenizar o indexar.
    Pipeline: normalize_text → clean_html → clean_punctuation → remove_links
    """
    name = normalize_text(name)
    name = clean_html(name)
    name = clean_punctuation(name)
    name = remove_links(name)
    return name.strip()


# ──────────────────────────────────────────────
# Funciones auxiliares
# ──────────────────────────────────────────────
def _get_latest_bronze_file(folder: str) -> str:
    """
    Retorna el archivo JSON más reciente en la carpeta bronze indicada.
    Lanza FileNotFoundError si no hay archivos.
    """
    files = sorted(glob.glob(os.path.join(folder, "*.json")))
    if not files:
        raise FileNotFoundError(f"No se encontraron archivos JSON en: {folder}")
    return files[-1]


def _save_silver_csv(df: pd.DataFrame, folder: str, filename: str) -> str:
    """
    Persiste el DataFrame como CSV en la carpeta silver indicada.
    Retorna el filepath completo.
    """
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)
    df.to_csv(filepath, index=False, encoding="utf-8")
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

    # ── Sensores: esperan que exista al menos un JSON en cada carpeta bronze ──

    wait_for_artists = FileSensor(
        task_id="wait_for_artists_bronze",
        filepath=BRONZE_ARTISTS_PATH,           # detecta que la carpeta tenga contenido
        fs_conn_id="fs_default",
        poke_interval=30,                        # revisa cada 30 segundos
        timeout=60 * 10,                         # falla si no aparece en 10 minutos
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
        """
        Lee el JSON más reciente de bronze/lastfm_top_artists/ y produce un CSV
        con las columnas normalizadas:
            name | name_tokens | playcount | listeners | mbid | ingested_at
        Descarta: url, streamable, image (array de URLs de thumbnails)
        """
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
                "name":         name,
                "name_tokens":  build_name_tokens(name),
                "playcount":    int(artist.get("playcount", 0)),
                "listeners":    int(artist.get("listeners", 0)),
                "mbid":         artist.get("mbid", ""),
                "ingested_at":  ingested_at,
            })

        df = pd.DataFrame(rows)

        # Validaciones de calidad
        before = len(df)
        df = df.dropna(subset=["name"]).query("name != ''")
        df = df[df["playcount"] > 0]
        dropped = before - len(df)
        if dropped:
            print(f"⚠️  Se descartaron {dropped} registros inválidos")

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filepath_out = _save_silver_csv(
            df, SILVER_ARTISTS_PATH, f"lastfm_top_artists_{timestamp}.csv"
        )

        print(f"✅ Artists silver — {len(df)} registros → {filepath_out}")
        print(f"   🎤 #1: {df.iloc[0]['name']} → tokens: '{df.iloc[0]['name_tokens']}' — listeners: {df.iloc[0]['listeners']:,}")

        return filepath_out

    # ── Tarea 2: Transformar Top Tracks ──────────────────────────────────
    @task()
    def transform_top_tracks() -> str:
        """
        Lee el JSON más reciente de bronze/lastfm_top_tracks/ y produce un CSV
        con las columnas normalizadas:
            name | name_tokens | duration_sec | playcount | listeners | mbid |
            artist_name | artist_name_tokens | artist_mbid | ingested_at
        Descarta: url, artist_url, streamable (siempre 0), image (array de thumbnails)
        """
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
                "mbid":               track.get("mbid", ""),
                "artist_name":        artist_name,
                "artist_name_tokens": build_name_tokens(artist_name),
                "artist_mbid":        artist.get("mbid", ""),
                "ingested_at":        ingested_at,
            })

        df = pd.DataFrame(rows)

        # Validaciones de calidad
        before = len(df)
        df = df.dropna(subset=["name", "artist_name"]).query("name != '' and artist_name != ''")
        df = df[df["playcount"] > 0]
        dropped = before - len(df)
        if dropped:
            print(f"⚠️  Se descartaron {dropped} registros inválidos")

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filepath_out = _save_silver_csv(
            df, SILVER_TRACKS_PATH, f"lastfm_top_tracks_{timestamp}.csv"
        )

        print(f"✅ Tracks silver — {len(df)} registros → {filepath_out}")
        print(f"   🎵 #1: {df.iloc[0]['name']} → tokens: '{df.iloc[0]['name_tokens']}' — {df.iloc[0]['artist_name']}")

        return filepath_out

    # ── Tarea 3: Validación conjunta ──────────────────────────────────────
    @task()
    def validate_silver_files(artists_filepath: str, tracks_filepath: str) -> dict:
        """
        Verifica que ambos CSV existen y tienen datos.
        Retorna un resumen consolidado de la transformación.
        """
        summary = {}

        for label, filepath in [("artists", artists_filepath), ("tracks", tracks_filepath)]:
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"❌ CSV no encontrado: {filepath}")

            df = pd.read_csv(filepath)

            if df.empty:
                raise ValueError(f"❌ El CSV de {label} está vacío: {filepath}")

            summary[label] = {
                "filepath":   filepath,
                "rows":       len(df),
                "columns":    list(df.columns),
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