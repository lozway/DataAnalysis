# Workshop 1 — Project Definition and Source Research

**Course:** Data Analysis Programming — Semester 2026-I  
**Project:** Music Artists & Albums Public Perception

---

## Team

| Name | Student ID |
|---|---|
| Carlos Andres Celis Herrera | 20222020051 |
| Juan Diego Lozada Gonzalez | 20222020014 |
| Cristian Santiago Lopez Cadena | 20222020027 |

---

## Objective

Define the project scope, validate data sources and establish the extraction strategy. This workshop produces the first raw data for the pipeline and sets up the base folder architecture.

---

## Validated Data Sources

### 1. API — Last.fm
- **Endpoints used:** `chart.getTopArtists`, `chart.getTopTracks`
- **Authentication:** API Key via `LASTFM_API_KEY` environment variable
- **Relevant fields:**
  - Artists: `name`, `playcount`, `listeners`, `mbid`, `url`
  - Tracks: `name`, `duration`, `playcount`, `listeners`, `artist.name`, `mbid`
- **Script:** `data_api.py`

### 2. Web Scraping — Reddit
- **Communities:** `r/indieheads` and `r/hiphopheads`
- **Method:** BeautifulSoup on `old.reddit.com` (static HTML, no API key required)
- **Filter:** Posts containing reaction keywords: `[FRESH]`, `thoughts`, `opinion`, `rate`, `album`, `aoty`, `underrated`, etc.
- **Data extracted per post:** title, score (karma), up to 5 top-level comments
- **Script:** `scraping/scraping_reddit.py`

---

## Folder Structure

```
workshop_1/
├── data/
│   ├── lastfm_music_20260321_222004.json   # Last.fm API sample (artists)
│   └── reddit_music_opinions.json          # Reddit scraping sample
├── scraping/
│   └── scraping_reddit.py                  # Web scraper (BeautifulSoup)
├── data_api.py                             # Last.fm API test script
└── README.md
```

---

## Extraction Strategy

| Aspect | Decision |
|---|---|
| Format | Raw JSON with metadata wrapper |
| Naming | `{source}_{topic}_{YYYYMMDD}_{HHMMSS}.json` |
| Storage | `datalake_bronze/` — immutable, one file per run |
| Orchestration | Apache Airflow (DAG `lastfm_ingest` for production) |

---

## Run the Scraper

```powershell
# From the project root
poetry run python workshop_1/scraping/scraping_reddit.py
```

Generates a file in `datalake_bronze/reddit/reddit_music_opinions_YYYYMMDD_HHMMSS.json` with approximately 70 records (35 per subreddit).

---

## Main Deliverable

[**Workshop 1 Report (PDF)**](./WORKSHOP1_Music_Artists_Albums.pdf) — Full technical report including User Stories, architecture overview and source characterisation.
