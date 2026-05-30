"""
DAG: lastfm_ingest
Responsabilidad: Extrae top tracks y top artists de Last.fm y guarda
                JSON crudo en subcarpetas separadas dentro de datalake_bronze/
Frecuencia: Diaria

Estructura de salida:
    datalake_bronze/
    ├── lastfm_top_tracks/
    │   └── lastfm_top_tracks_YYYYMMDD_HHMMSS.json
    └── lastfm_top_artists/
        └── lastfm_top_artists_YYYYMMDD_HHMMSS.json
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

import requests
from airflow.decorators import dag, task

# ──────────────────────────────────────────────
# Constantes
# ──────────────────────────────────────────────
LASTFM_BASE_URL = "https://ws.audioscrobbler.com/2.0/"

BRONZE_TRACKS_PATH  = "/opt/airflow/datalake_bronze/lastfm_top_tracks"
BRONZE_ARTISTS_PATH = "/opt/airflow/datalake_bronze/lastfm_top_artists"

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

# ──────────────────────────────────────────────
# Funciones auxiliares compartidas
# ──────────────────────────────────────────────
def _call_lastfm(method: str, api_key: str, user_agent: str, limit: int = 50) -> dict:
    """
    Realiza la petición GET a la API de Last.fm y retorna el JSON.
    Lanza ValueError si la API responde con un error lógico.
    """
    payload = {
        "method": method,
        "api_key": api_key,
        "format": "json",
        "limit": limit,
    }
    headers = {"user-agent": user_agent}

    response = requests.get(
        LASTFM_BASE_URL,
        headers=headers,
        params=payload,
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

    if "error" in data:
        raise ValueError(
            f"Last.fm API error {data['error']}: {data.get('message', '')}"
        )

    return data


def _save_json(data: dict, folder: str, filename: str) -> str:
    """
    Persiste el JSON con metadatos en la carpeta indicada.
    Retorna el filepath completo.
    """
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    return filepath


# ──────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────
@dag(
    dag_id="lastfm_ingest",
    description="Ingesta diaria de top tracks y top artists desde Last.fm API → bronze layer",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lastfm", "ingest", "bronze"],
)
def lastfm_ingest_dag():

    # ── Tarea 1: Top Tracks ───────────────────────────────────────────────
    @task()
    def extract_top_tracks() -> str:
        """
        Llama a chart.getTopTracks y persiste el JSON en:
            datalake_bronze/lastfm_top_tracks/lastfm_top_tracks_YYYYMMDD_HHMMSS.json

        Campos relevantes por track:
            - name, playcount, listeners, mbid, url
            - artist.name, artist.mbid, artist.url
        """
        api_key    = os.getenv("LASTFM_API_KEY")
        user_agent = os.getenv("LASTFM_USER_AGENT", "AirflowPipeline")

        if not api_key:
            raise ValueError("❌ LASTFM_API_KEY no está definida en el entorno (.env)")

        data = _call_lastfm("chart.getTopTracks", api_key, user_agent, limit=50)

        tracks = data.get("tracks", {}).get("track", [])

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename  = f"lastfm_top_tracks_{timestamp}.json"

        payload_to_save = {
            "_metadata": {
                "ingested_at": datetime.utcnow().isoformat(),
                "source": "last.fm",
                "method": "chart.getTopTracks",
                "track_count": len(tracks),
            },
            "data": data,
        }

        filepath = _save_json(payload_to_save, BRONZE_TRACKS_PATH, filename)

        print(f"✅ Top Tracks — {len(tracks)} registros guardados en: {filepath}")
        if tracks:
            print(f"   🎵 #1: {tracks[0].get('name')} — {tracks[0].get('artist', {}).get('name')}")

        return filepath

    # ── Tarea 2: Top Artists ──────────────────────────────────────────────
    @task()
    def extract_top_artists() -> str:
        """
        Llama a chart.getTopArtists y persiste el JSON en:
            datalake_bronze/lastfm_top_artists/lastfm_top_artists_YYYYMMDD_HHMMSS.json

        Campos relevantes por artista:
            - name, playcount, listeners, mbid, url
        """
        api_key    = os.getenv("LASTFM_API_KEY")
        user_agent = os.getenv("LASTFM_USER_AGENT", "AirflowPipeline")

        if not api_key:
            raise ValueError("❌ LASTFM_API_KEY no está definida en el entorno (.env)")

        data = _call_lastfm("chart.getTopArtists", api_key, user_agent, limit=50)

        artists = data.get("artists", {}).get("artist", [])

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename  = f"lastfm_top_artists_{timestamp}.json"

        payload_to_save = {
            "_metadata": {
                "ingested_at": datetime.utcnow().isoformat(),
                "source": "last.fm",
                "method": "chart.getTopArtists",
                "artist_count": len(artists),
            },
            "data": data,
        }

        filepath = _save_json(payload_to_save, BRONZE_ARTISTS_PATH, filename)

        print(f"✅ Top Artists — {len(artists)} registros guardados en: {filepath}")
        if artists:
            print(f"   🎤 #1: {artists[0].get('name')} — listeners: {int(artists[0].get('listeners', 0)):,}")

        return filepath

    # ── Tarea 3: Validación conjunta ──────────────────────────────────────
    @task()
    def validate_bronze_files(tracks_filepath: str, artists_filepath: str) -> dict:
        """
        Valida que ambos archivos JSON fueron creados correctamente
        y retorna un resumen consolidado de la ingesta.
        """
        summary = {}

        for label, filepath, data_key, count_key in [
            ("tracks",  tracks_filepath,  "tracks",  "track"),
            ("artists", artists_filepath, "artists", "artist"),
        ]:
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"❌ Archivo no encontrado: {filepath}")

            with open(filepath, "r", encoding="utf-8") as f:
                content = json.load(f)

            records = content["data"].get(data_key, {}).get(count_key, [])
            top     = records[0] if records else {}

            summary[label] = {
                "filepath":    filepath,
                "ingested_at": content["_metadata"]["ingested_at"],
                "count":       len(records),
                "top_name":    top.get("name", "N/A"),
            }

        print(f"📊 Resumen de ingesta:\n{json.dumps(summary, indent=2)}")

        return summary

    # ── Orquestación ──────────────────────────────────────────────────────
    # Las dos extracciones corren en paralelo; la validación espera a ambas.
    # Silver corre bajo su propio schedule (@weekly) leyendo todos los JSONs
    # acumulados en bronze — no se dispara desde aquí.
    tracks_path  = extract_top_tracks()
    artists_path = extract_top_artists()
    validate_bronze_files(tracks_path, artists_path)


lastfm_ingest_dag()