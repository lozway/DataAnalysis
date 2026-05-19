import json
import os
from datetime import datetime, timezone

import requests

LASTFM_BASE_URL = "https://ws.audioscrobbler.com/2.0/"
BRONZE_PATH = "datalake_bronze"

api_key = os.environ["LASTFM_API_KEY"]

params = {
    "method": "chart.gettopartists",
    "api_key": api_key,
    "format": "json",
    "limit": 20,
}

response = requests.get(
    LASTFM_BASE_URL,
    headers={"user-agent": os.getenv("LASTFM_USER_AGENT", "DataAnalysisPipeline")},
    params=params,
    timeout=30,
)
response.raise_for_status()
data = response.json()

if "error" in data:
    raise ValueError(f"Last.fm API error {data['error']}: {data.get('message', '')}")

os.makedirs(BRONZE_PATH, exist_ok=True)
timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
filepath = os.path.join(BRONZE_PATH, f"lastfm_top_artists_{timestamp}.json")

payload = {
    "_metadata": {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "last.fm",
        "method": "chart.gettopartists",
    },
    "data": data,
}

with open(filepath, "w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False, indent=4)

artists = data.get("artists", {}).get("artist", [])
print(f"Saved {len(artists)} artists → {filepath}")
