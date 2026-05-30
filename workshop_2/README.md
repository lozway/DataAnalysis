# Workshop 2 — Technical Architecture and Bronze/Silver Pipeline

**Course:** Data Analysis Programming — Semester 2026-I  
**Project:** Music Artists & Albums Public Perception  
**Universidad Distrital Francisco José de Caldas**

---

## Overview

This workshop covers the full technical implementation of the Bronze and Silver layers
of the Medallion Architecture pipeline. Starting from the project definition established
in Workshop 1, we configured the complete Docker-based infrastructure, implemented two
Airflow DAGs for Bronze ingestion from the Last.fm API, and developed two Silver
processing DAGs that normalize and persist the data as structured Parquet files.

---

## Folder Structure

```
workshop_2/
├── data/
│   ├── lastfm_top_artists_YYYYMMDD_HHMMSS.json      # Bronze sample — Last.fm artists
│   ├── lastfm_top_artists_YYYYMMDD_HHMMSS.parquet   # Silver sample — Last.fm artists
│   ├── lastfm_top_tracks_YYYYMMDD_HHMMSS.json       # Bronze sample — Last.fm tracks
│   ├── lastfm_top_tracks_YYYYMMDD_HHMMSS.parquet    # Silver sample — Last.fm tracks
│   └── reddit_music_opinions_YYYYMMDD_HHMMSS.json   # Bronze sample — Reddit scraping
└── WORKSHOP2_Music_Artists_Albums.pdf                # Full technical report
```

---

## What Was Built

### Infrastructure

- **Docker Compose** stack with Apache Airflow (webserver, scheduler, triggerer)
  and PostgreSQL as the metadata database.
- Local volume mounts for `datalake_bronze/`, `datalake_silver/`, and `airflow/dags/`
  to persist pipeline outputs on the host filesystem.
- Environment variables managed via `.env` file for API credentials.

### DAG 1 — `lastfm_ingest` (Bronze Layer)

Extracts daily global rankings from the Last.fm API using two endpoints in parallel
and persists raw JSON files in dedicated subfolders inside `datalake_bronze/`.

| Task | Description |
|---|---|
| `extract_top_tracks` | Calls `chart.getTopTracks` → saves to `datalake_bronze/lastfm_top_tracks/` |
| `extract_top_artists` | Calls `chart.getTopArtists` → saves to `datalake_bronze/lastfm_top_artists/` |
| `validate_bronze_files` | Validates existence and integrity of both JSON files |

**Schedule:** `@daily` — Bronze and Silver are decoupled; Silver runs independently on `@weekly`.  
**Output format:** `lastfm_top_{source}_YYYYMMDD_HHMMSS.json`

### DAG 2 — `lastfm_silver` (Silver Layer — Last.fm)

Triggered automatically by `lastfm_ingest`. Reads the latest Bronze JSON files,
applies the full cleaning and normalization pipeline, and persists structured
Parquet files in `datalake_silver/`.

| Task | Description |
|---|---|
| `wait_for_artists_bronze` | FileSensor — polls `datalake_bronze/lastfm_top_artists/` |
| `wait_for_tracks_bronze` | FileSensor — polls `datalake_bronze/lastfm_top_tracks/` |
| `transform_top_artists` | Cleans, deduplicates, enforces schema → Parquet |
| `transform_top_tracks` | Cleans, deduplicates, enforces schema → Parquet |
| `validate_silver_files` | Validates schema compliance and row count of both Parquet files |

**Preprocessing steps:** invalid record filtering → text normalization →
HTML entity decoding → punctuation cleaning → deduplication (3-pass) →
schema enforcement → type casting → Snappy Parquet persistence.

**Output format:** `lastfm_top_{source}_YYYYMMDD_HHMMSS.parquet`

### DAG 3 — `reddit_silver` (Silver Layer — Reddit)

Processes the Reddit scraping data from `datalake_bronze/reddit/` applying a full
NLP pipeline derived from exploratory notebook analysis.

| Task | Description |
|---|---|
| `wait_for_reddit_bronze` | FileSensor — polls `datalake_bronze/reddit/` |
| `transform_reddit` | Full NLP pipeline → Parquet |
| `validate_silver_file` | Validates schema, row count, and field presence |

**NLP pipeline steps:** null standardization → record filtering → comment explosion →
multi-sentence splitting → HTML/link/punctuation cleaning → tokenization →
comment classification (`recommendation` / `opinion` / `mixed` / `other`) →
artist & song extraction → IQR outlier capping → deduplication → noise removal →
schema enforcement.

**Output format:** `reddit_music_opinions_YYYYMMDD_HHMMSS.parquet`

---

## Silver Schemas

### `lastfm_top_artists`

| Field | Type | Required |
|---|---|---|
| `name` | string | Yes |
| `name_tokens` | string | Yes |
| `playcount` | int64 | Yes |
| `listeners` | int64 | Yes |
| `mbid` | string | No — default `unknown` |
| `ingested_at` | string | Yes |

### `lastfm_top_tracks`

| Field | Type | Required |
|---|---|---|
| `name` | string | Yes |
| `name_tokens` | string | Yes |
| `duration_sec` | int64 | Yes |
| `playcount` | int64 | Yes |
| `listeners` | int64 | Yes |
| `mbid` | string | No — default `unknown` |
| `artist_name` | string | Yes |
| `artist_name_tokens` | string | Yes |
| `artist_mbid` | string | No — default `unknown` |
| `ingested_at` | string | Yes |

### `reddit_music_opinions`

| Field | Type | Required |
|---|---|---|
| `post_id` | int64 | Yes |
| `title` | string | Yes |
| `score` | int64 | Yes |
| `raw_comment_id` | int64 | Yes |
| `raw_comment` | string | Yes |
| `clean_comment` | string | Yes |
| `tokens` | string | Yes |
| `comment_type` | string | Yes |
| `confidence` | float64 | Yes |
| `has_music_pattern` | bool | Yes |
| `pattern_type` | string | No — default `unknown` |
| `has_contrast` | bool | Yes |
| `word_count` | int64 | Yes |
| `word_count_capped` | float64 | Yes |
| `score_capped` | float64 | Yes |
| `artist` | string | No — default `unknown` |
| `song` | string | No — default `unknown` |
| `extract_confidence` | float64 | Yes |
| `ingested_at` | string | Yes |

---

## Data Samples

The `data/` folder contains one representative sample file per source and layer:

| File | Layer | Source | Description |
|---|---|---|---|
| `lastfm_top_artists_*.json` | Bronze | Last.fm API | Raw JSON from `chart.getTopArtists` with `_metadata` wrapper |
| `lastfm_top_tracks_*.json` | Bronze | Last.fm API | Raw JSON from `chart.getTopTracks` with `_metadata` wrapper |
| `lastfm_top_artists_*.parquet` | Silver | Last.fm API | Normalized artists with `name_tokens`, types cast to int64 |
| `lastfm_top_tracks_*.parquet` | Silver | Last.fm API | Normalized tracks with `name_tokens` and `artist_name_tokens` |
| `reddit_music_opinions_*.json` | Bronze | Reddit scraping | Raw posts with title, score, and comments list |

---

## How to Run

### 1. Set up environment variables

Create a `.env` file in the project root:

```env
LASTFM_API_KEY=your_api_key_here
LASTFM_USER_AGENT=AirflowPipeline
AIRFLOW_UID=50000
```

### 2. Start the Docker stack

```bash
docker compose up -d
```

Airflow UI will be available at [http://localhost:8080](http://localhost:8080)  
Default credentials: `airflow` / `airflow`

### 3. Trigger the pipeline

Enable and trigger `lastfm_ingest` from the Airflow UI. It will automatically
trigger `lastfm_silver` upon completion.

For the Reddit Silver DAG, trigger `reddit_silver` manually after placing
the Bronze JSON file in `datalake_bronze/reddit/`.

### 4. Verify outputs

```bash
# Bronze outputs
ls datalake_bronze/lastfm_top_tracks/
ls datalake_bronze/lastfm_top_artists/
ls datalake_bronze/reddit/

# Silver outputs
ls datalake_silver/lastfm_top_tracks/
ls datalake_silver/lastfm_top_artists/
ls datalake_silver/reddit/
```

---

## Team

| Name | Student ID |
|---|---|
| Carlos Andres Celis Herrera | 20222020051 |
| Juan Diego Lozada Gonzalez | 20222020014 |
| Cristian Santiago Lopez Cadena | 20220200 |
