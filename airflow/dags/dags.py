"""
DAG: lastfm_ingest
Responsabilidad: Extrae top artists de Last.fm y guarda JSON crudo en datalake/bronze/
Frecuencia: Diaria
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
BRONZE_PATH = "/opt/airflow/datalake/bronze/lastfm"

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

# ──────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────
@dag(
    dag_id="lastfm_ingest",
    description="Ingesta diaria de top artists desde Last.fm API → bronze layer",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lastfm", "ingest", "bronze"],
)
def lastfm_ingest_dag():

    @task()
    def extract_top_artists() -> str:
        """
        Llama a chart.gettopartists en Last.fm y persiste el JSON crudo.
        Retorna el filepath para que la siguiente tarea lo consuma.
        """

        # ✅ Leer desde .env
        api_key = os.getenv("LASTFM_API_KEY")
        user_agent = os.getenv("LASTFM_USER_AGENT", "AirflowPipeline")

        # 🚨 Validación importante
        if not api_key:
            raise ValueError("❌ LASTFM_API_KEY no está definida en el entorno (.env)")

        payload = {
            "method": "chart.gettopartists",
            "api_key": api_key,  # ✅ corregido
            "format": "json",
            "limit": 20,
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

        # Validar error de API
        if "error" in data:
            raise ValueError(
                f"Last.fm API error {data['error']}: {data.get('message', '')}"
            )

        # Guardar en bronze
        os.makedirs(BRONZE_PATH, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"lastfm_top_artists_{timestamp}.json"
        filepath = os.path.join(BRONZE_PATH, filename)

        payload_to_save = {
            "_metadata": {
                "ingested_at": datetime.utcnow().isoformat(),
                "source": "last.fm",
                "method": "chart.gettopartists",
            },
            "data": data,
        }

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(payload_to_save, f, ensure_ascii=False, indent=4)

        print(
            f"✅ Guardados {len(data.get('artists', {}).get('artist', []))} artistas en: {filepath}"
        )

        return filepath

    @task()
    def validate_bronze_file(filepath: str) -> dict:
        """
        Valida que el archivo JSON fue creado correctamente.
        """

        if not os.path.exists(filepath):
            raise FileNotFoundError(f"El archivo no existe: {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            content = json.load(f)

        artists = content["data"].get("artists", {}).get("artist", [])

        if not artists:
            raise ValueError("❌ El JSON no contiene artistas")

        summary = {
            "filepath": filepath,
            "ingested_at": content["_metadata"]["ingested_at"],
            "artist_count": len(artists),
            "top_artist": artists[0].get("name", "N/A"),
        }

        print(f"📊 Resumen de ingesta: {json.dumps(summary, indent=2)}")

        return summary

    # ── Orquestación ──
    filepath = extract_top_artists()
    validate_bronze_file(filepath)


lastfm_ingest_dag()