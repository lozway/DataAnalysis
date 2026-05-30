"""
Shared utilities: data loading, color palette, helper functions.
"""

import glob
import os

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
DASHBOARD_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR      = os.path.dirname(DASHBOARD_DIR)
GOLD_DIR      = os.path.join(ROOT_DIR, "datalake_gold")
NB_DIR        = os.path.join(ROOT_DIR, "notebooks")

# ── Color palette ─────────────────────────────────────────────────────────────
COLORS = {
    "positive":       "#16a34a",
    "negative":       "#dc2626",
    "neutral":        "#6b7280",
    "primary":        "#1e40af",
    "reddit":         "#5b9bd5",
    "lastfm_artists": "#70ad47",
    "lastfm_tracks":  "#ffc000",
    "background":     "#f0f4f8",
    "card":           "#ffffff",
    "text":           "#1e293b",
    "muted":          "#64748b",
    "border":         "#e2e8f0",
    "ok":             "#16a34a",
    "warn":           "#d97706",
    "error":          "#dc2626",
}

SENTIMENT_COLORS = {
    "positive": COLORS["positive"],
    "negative": COLORS["negative"],
    "neutral":  COLORS["neutral"],
}

SOURCE_COLORS = {
    "reddit":         COLORS["reddit"],
    "lastfm_artists": COLORS["lastfm_artists"],
    "lastfm_tracks":  COLORS["lastfm_tracks"],
}

SOURCE_LABELS = {
    "reddit":         "Reddit (Scraping)",
    "lastfm_artists": "Last.fm — Artists (API)",
    "lastfm_tracks":  "Last.fm — Tracks (API)",
}


# ── Data loading ──────────────────────────────────────────────────────────────
#
# Gold usa particionamiento mensual:
#   datalake_gold/<name>/year=YYYY/month=MM/<name>.parquet
#
# load_* lee TODAS las particiones históricas y las concatena en un único
# DataFrame, permitiendo análisis cross-month. Para filtrar por período
# específico usa los campos ingest_date o computed_at del DataFrame resultante.

def _load_gold_dataset(name: str) -> pd.DataFrame:
    """
    Lee todas las particiones de un dataset de gold (governance o storytelling)
    concatenándolas en un único DataFrame ordenado por computed_at.

    Busca en:  datalake_gold/<name>/year=*/month=*/<name>.parquet
    Fallback:  notebooks/export_<name>.csv  (desarrollo sin gold generado)
    """
    pattern  = os.path.join(GOLD_DIR, name, "year=*", "month=*", f"{name}.parquet")
    files    = sorted(glob.glob(pattern))

    if files:
        dfs = [pd.read_parquet(f) for f in files]
        df  = pd.concat(dfs, ignore_index=True)
        # Normalizar nombre del campo de fecha — el DAG puede haberlo escrito
        # como 'data_date' o 'ingest_date' según la versión; estandarizamos a 'ingest_date'
        if "data_date" in df.columns and "ingest_date" not in df.columns:
            df = df.rename(columns={"data_date": "ingest_date"})
        # Ordenar por computed_at para que .max() retorne siempre el más reciente
        if "computed_at" in df.columns:
            df = df.sort_values("computed_at").reset_index(drop=True)
        return df

    # Fallback CSV para desarrollo local sin gold generado
    csv = os.path.join(NB_DIR, f"export_{name}.csv")
    if os.path.exists(csv):
        return pd.read_csv(csv)

    return pd.DataFrame()


def load_governance() -> pd.DataFrame:
    return _load_gold_dataset("governance")


def load_storytelling() -> pd.DataFrame:
    return _load_gold_dataset("storytelling")


def last_updated(df: pd.DataFrame) -> str:
    if df.empty or "computed_at" not in df.columns:
        return "N/A"
    ts = pd.to_datetime(df["computed_at"].max())
    return ts.strftime("%Y-%m-%d  %H:%M UTC")


def severity_color(value: float) -> str:
    """Traffic-light color for null/outlier rates (percentage values)."""
    if value == 0:
        return COLORS["ok"]
    if value <= 5:
        return COLORS["warn"]
    return COLORS["error"]


def gold_state_key() -> str:
    """
    Fingerprint del estado actual de datalake_gold/.
    Detecta cambios en cualquier partición (nueva, modificada o eliminada)
    para que los dashboards decidan si re-renderizar.
    Recorre toda la estructura year=*/month=*/ en lugar de solo la raíz.
    """
    pattern = os.path.join(GOLD_DIR, "**", "*.parquet")
    files   = sorted(glob.glob(pattern, recursive=True))
    if not files:
        return "empty"
    return "|".join(
        f"{os.path.relpath(f, GOLD_DIR)}:{os.path.getmtime(f)}" for f in files
    )