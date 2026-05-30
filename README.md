# Music Artists & Albums Public Perception

**Course:** Data Analysis Programming — Semester 2026-I  
**Universidad Distrital Francisco José de Caldas**

**Team:**
| Name | Student ID |
|---|---|
| Carlos Andres Celis Herrera | 20222020051 |
| Juan Diego Lozada Gonzalez | 20222020014 |
| Cristian Santiago Lopez Cadena | 20222020027 |

---

## Description

An end-to-end data pipeline that analyses how the public perceives artists, albums and music trends. It combines quantitative data from the Last.fm API with qualitative opinions scraped from Reddit communities to produce sentiment metrics and data governance KPIs.

**Functional user:** Music label analysts or artist managers monitoring public reception of new releases.

---

## Medallion Architecture

```
External sources
    ├── Last.fm API  (chart.getTopArtists / chart.getTopTracks)
    └── Reddit scraping  (r/indieheads, r/hiphopheads)
            │
            ▼
    datalake_bronze/   ← Raw JSON files, immutable, timestamped
            │
            ▼  (Airflow transformation DAGs)
    datalake_silver/   ← Normalised, typed, deduplicated Parquet files
            │
            ▼  (PySpark @weekly)
    datalake_gold/     ← Governance KPIs + dashboard aggregations
```

---

## Repository Structure

```
.
├── airflow/
│   ├── dags/                    # Airflow DAGs (orchestration)
│   └── sql/                     # PostgreSQL initialisation scripts
├── datalake_bronze/             # Bronze layer — raw data
│   ├── lastfm_top_artists/
│   ├── lastfm_top_tracks/
│   └── reddit/
├── datalake_silver/             # Silver layer — processed data
│   ├── lastfm_top_artists/
│   ├── lastfm_top_tracks/
│   └── reddit/
├── datalake_gold/               # Gold layer — KPIs and aggregations
├── dashboard/                   # Plotly Dash application
├── notebooks/                   # Exploratory analysis notebooks
├── workshop_1/                  # Workshop 1 deliverables
├── workshop_2/                  # Workshop 2 deliverables
├── docker-compose.yml           # Stack: PostgreSQL + Airflow
├── Dockerfile                   # Custom image with Java + PySpark + VADER
├── ingest_lastfm.py             # Standalone Last.fm ingestion script
└── pyproject.toml               # Poetry dependencies
```

---

## DAGs and Schedules

| DAG | Schedule | Layer | Description |
|---|---|---|---|
| `lastfm_ingest` | `@daily` | Bronze | Extracts top 50 artists and tracks from Last.fm |
| `lastfm_silver` | `@weekly` | Silver | Normalises and consolidates the Last.fm historical data |
| `reddit_silver` | Manual | Silver | Applies NLP pipeline to Reddit comments |
| `gold_pipeline` | `@weekly` | Gold | Governance KPIs + storytelling aggregations with PySpark |

---

## Quick Start

```bash
# 1. Set up environment variables
cp .env.example .env   # edit LASTFM_API_KEY

# 2. Build image (includes Java + PySpark) and start stack
docker compose build
docker compose up -d

# 3. Airflow UI
# http://localhost:8080  |  user: admin  |  password: admin

# 4. Manual Reddit scraping (run from project root)
poetry run python workshop_1/scraping/scraping_reddit.py
```

---

## Data Sources

| Source | Type | Method | Frequency |
|---|---|---|---|
| [Last.fm API](https://www.last.fm/api) | REST API | `chart.getTopArtists`, `chart.getTopTracks` | Daily |
| [r/indieheads](https://old.reddit.com/r/indieheads/) | Web scraping | BeautifulSoup + old.reddit.com | Manual |
| [r/hiphopheads](https://old.reddit.com/r/hiphopheads/) | Web scraping | BeautifulSoup + old.reddit.com | Manual |
