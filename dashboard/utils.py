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

def _latest_file(pattern: str) -> str | None:
    files = sorted(glob.glob(os.path.join(GOLD_DIR, pattern)))
    return files[-1] if files else None


def load_governance() -> pd.DataFrame:
    path = _latest_file("governance_*.parquet")
    if path:
        return pd.read_parquet(path)
    csv = os.path.join(NB_DIR, "export_governance.csv")
    if os.path.exists(csv):
        return pd.read_csv(csv)
    return pd.DataFrame()


def load_storytelling() -> pd.DataFrame:
    path = _latest_file("storytelling_*.parquet")
    if path:
        return pd.read_parquet(path)
    csv = os.path.join(NB_DIR, "export_storytelling.csv")
    if os.path.exists(csv):
        return pd.read_csv(csv)
    return pd.DataFrame()


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
    Returns a fingerprint of the datalake_gold/ folder state.
    Changes when Parquet files are added, removed, or modified —
    used by dashboards to decide whether to re-render.
    """
    files = sorted(glob.glob(os.path.join(GOLD_DIR, "*.parquet")))
    if not files:
        return "empty"
    return "|".join(
        f"{os.path.basename(f)}:{os.path.getmtime(f)}" for f in files
    )
