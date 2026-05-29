import json
import os
from datetime import datetime, timezone

import requests

LASTFM_BASE_URL = "https://ws.audioscrobbler.com/2.0/"
BRONZE_PATH = "datalake_bronze"

api_key = os.environ["LASTFM_API_KEY"]
user_agent = os.getenv("LASTFM_USER_AGENT", "DataAnalysisPipeline")
timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
ingested_at = datetime.now(timezone.utc).isoformat()


def fetch(method: str, extra_params: dict = {}) -> dict:
    params = {
        "method": method,
        "api_key": api_key,
        "format": "json",
        "limit": 50,
        **extra_params,
    }
    resp = requests.get(
        LASTFM_BASE_URL,
        headers={"user-agent": user_agent},
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise ValueError(f"Last.fm API error {data['error']}: {data.get('message', '')}")
    return data


# ── Top Artists ───────────────────────────────────────────────────────────────
artists_data = fetch("chart.gettopartists")
artists = artists_data.get("artists", {}).get("artist", [])

artists_dir = os.path.join(BRONZE_PATH, "lastfm_top_artists")
os.makedirs(artists_dir, exist_ok=True)
artists_path = os.path.join(artists_dir, f"lastfm_top_artists_{timestamp}.json")

with open(artists_path, "w", encoding="utf-8") as f:
    json.dump(
        {
            "_metadata": {
                "ingested_at": ingested_at,
                "source": "last.fm",
                "method": "chart.getTopArtists",
                "artist_count": len(artists),
            },
            "data": artists_data,
        },
        f,
        ensure_ascii=False,
        indent=4,
    )

print(f"Saved {len(artists)} artists → {artists_path}")

# ── Top Tracks ────────────────────────────────────────────────────────────────
tracks_data = fetch("chart.gettoptracks")
tracks = tracks_data.get("tracks", {}).get("track", [])

tracks_dir = os.path.join(BRONZE_PATH, "lastfm_top_tracks")
os.makedirs(tracks_dir, exist_ok=True)
tracks_path = os.path.join(tracks_dir, f"lastfm_top_tracks_{timestamp}.json")

with open(tracks_path, "w", encoding="utf-8") as f:
    json.dump(
        {
            "_metadata": {
                "ingested_at": ingested_at,
                "source": "last.fm",
                "method": "chart.getTopTracks",
                "track_count": len(tracks),
            },
            "data": tracks_data,
        },
        f,
        ensure_ascii=False,
        indent=4,
    )

print(f"Saved {len(tracks)} tracks → {tracks_path}")
