# Airflow — Pipeline Orchestration

Contains all DAGs and SQL scripts that form the orchestration layer. The stack runs in Docker with Apache Airflow 2.8.1 and PostgreSQL 15.

---

## DAGs

### `dag_lastfm_ingest.py` — Bronze · `@daily`

Extracts Last.fm global rankings and persists raw JSON files in `datalake_bronze/`.

```
extract_top_tracks ─┐
                     ├─► validate_bronze_files
extract_top_artists─┘
```

| Task | Description |
|---|---|
| `extract_top_tracks` | Calls `chart.getTopTracks` (top 50) → `datalake_bronze/lastfm_top_tracks/` |
| `extract_top_artists` | Calls `chart.getTopArtists` (top 50) → `datalake_bronze/lastfm_top_artists/` |
| `validate_bronze_files` | Verifies existence and integrity of both JSON files |

---

### `dag_lastfm_silver.py` — Silver · `@weekly`

Reads **all** historical bronze JSONs, consolidates daily snapshots, deduplicates and produces normalised Parquet files.

```
wait_for_artists_bronze ─► transform_top_artists ─┐
                                                    ├─► validate_silver_files
wait_for_tracks_bronze  ─► transform_top_tracks  ─┘
```

**Cleaning pipeline:** invalid record filter → text normalisation → HTML decode → punctuation cleaning → 3-pass deduplication → schema enforcement → Snappy Parquet.

---

### `dag_reddit_silver.py` — Silver · Manual (on-demand)

Applies the full NLP pipeline to Reddit comments and produces a structured Parquet file.

```
wait_for_reddit_bronze ─► transform_reddit ─► validate_silver_file
```

**NLP pipeline (15 steps):** null normalisation → comment explosion → multi-sentence split → HTML/link/punctuation cleaning → tokenisation → comment classification (`recommendation` / `opinion` / `mixed` / `other`) → artist/song extraction → IQR capping → deduplication → schema enforcement.

---

### `dag_gold.py` — Gold · `@weekly`

Reads silver Parquet files with PySpark in local mode and produces two gold output files.

```
compute_governance   ─┐
                       ├─► validate_gold
compute_storytelling ─┘
```

| Task | Output | Content |
|---|---|---|
| `compute_governance` | `governance_*.parquet` | KPIs: null rate, volume, schema compliance, outlier rate (IQR), text length stats, ingestion days |
| `compute_storytelling` | `storytelling_*.parquet` | VADER sentiment, trends, top keywords, comment types, top LastFM artists/tracks |

**PySpark config:** `local[*]`, driver memory 1 GB, shuffle partitions 4, UI disabled.

---

## SQL

### `sql/init_es_db.sql`

Initialisation script executed by PostgreSQL on first startup. Creates the secondary database required by the stack.

---

## Required Airflow Connection

The file sensor (`FileSensor`) requires the `fs_default` connection of type `File (path)`. It is created automatically by the `airflow-init` service in docker-compose:

```bash
airflow connections add fs_default --conn-type fs --conn-extra '{"path": "/"}'
```

---

## Required Environment Variables

| Variable | Description |
|---|---|
| `LASTFM_API_KEY` | Last.fm API key |
| `LASTFM_USER_AGENT` | User agent for requests (default: `AirflowPipeline`) |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | PostgreSQL credentials |
| `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` | Airflow admin credentials |
