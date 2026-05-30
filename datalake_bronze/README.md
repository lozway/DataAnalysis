# datalake_bronze — Bronze Layer (Raw Data)

Stores **raw, immutable data** exactly as received from each source. Every pipeline run produces a new timestamped file — existing files are never overwritten or deleted.

---

## Structure

```
datalake_bronze/
├── lastfm_top_artists/
│   └── lastfm_top_artists_YYYYMMDD_HHMMSS.json   # One file per daily run
├── lastfm_top_tracks/
│   └── lastfm_top_tracks_YYYYMMDD_HHMMSS.json    # One file per daily run
└── reddit/
    └── reddit_music_opinions_YYYYMMDD_HHMMSS.json # One file per manual scrape
```

---

## File Format

### Last.fm (`lastfm_top_artists_*.json` / `lastfm_top_tracks_*.json`)

Standard metadata wrapper + full API response:

```json
{
  "_metadata": {
    "ingested_at": "2026-05-29T03:14:46.597506",
    "source": "last.fm",
    "method": "chart.getTopArtists",
    "artist_count": 50
  },
  "data": { ...original Last.fm API response... }
}
```

**Fields per artist:** `name`, `playcount`, `listeners`, `mbid`, `url`, `image[]`, `streamable`  
**Fields per track:** `name`, `duration`, `playcount`, `listeners`, `artist{}`, `mbid`, `url`

### Reddit (`reddit_music_opinions_*.json`)

Array of posts, each with its comments:

```json
[
  {
    "title": "Boards of Canada - Inferno",
    "score": 312,
    "comments": ["first comment...", "second comment...", "..."]
  }
]
```

Sources: `r/indieheads` and `r/hiphopheads`. Up to 5 top-level comments per post. Flair prefixes (`[FRESH ALBUM]`, `[DISCUSSION]`, etc.) are stripped from titles during scraping.

---

## Data Origin

| Folder | Producer DAG | Frequency | Alternative script |
|---|---|---|---|
| `lastfm_top_artists/` | `lastfm_ingest` | Daily (`@daily`) | `ingest_lastfm.py` |
| `lastfm_top_tracks/` | `lastfm_ingest` | Daily (`@daily`) | `ingest_lastfm.py` |
| `reddit/` | Manual | On-demand | `workshop_1/scraping/scraping_reddit.py` |

---

## Bronze Layer Principles

- **Immutability:** no existing file is modified or deleted.
- **Completeness:** the full API response is persisted without filtering any fields.
- **Traceability:** the `_metadata.ingested_at` field allows exact attribution of each snapshot.
- **Idempotency:** the `lastfm_ingest` DAG can run multiple times per day without conflict, thanks to the timestamp in the filename.
