# Workshop 4 — Dashboard Implementation

**Course:** Data Analysis Programming — Semester 2026-I  
**Project:** Music Artists & Albums Public Perception  
**Universidad Distrital Francisco José de Caldas**

---

## Overview

Workshop 4 delivers the two functional dashboards that consume the Gold layer Parquet files produced by the `gold_pipeline` DAG. Both applications are containerised and integrated into the existing Docker Compose stack, running alongside Airflow and PostgreSQL.

---

## What Was Built

### 1. Shared Utilities — `dashboard/utils.py`

Central module imported by both dashboard applications:

| Component | Description |
|---|---|
| `COLORS` | Unified color palette (positive green, negative red, neutral gray, per-source colors) |
| `SOURCE_COLORS / SOURCE_LABELS` | Consistent mapping of source identifiers to display labels and colors |
| `load_governance()` | Reads latest `governance_*.parquet` from `datalake_gold/`; falls back to `notebooks/export_governance.csv` |
| `load_storytelling()` | Reads latest `storytelling_*.parquet` from `datalake_gold/`; falls back to `notebooks/export_storytelling.csv` |
| `last_updated(df)` | Formats the most recent `computed_at` timestamp for display |
| `gold_state_key()` | Generates a fingerprint of all Parquet files in `datalake_gold/` (filename + mtime); used by dashboards to detect changes |

---

### 2. Governance Dashboard — `dashboard/governance_app.py`

**URL:** http://localhost:8050

Designed for data engineers to monitor pipeline quality metrics.

#### Components

| Component | Data source | Description |
|---|---|---|
| **KPI Cards** (4) | `kpi_type = volume / schema_compliance / null_rate` | Total Reddit records, Last.fm Artists volume, schema compliance %, highest null rate field |
| **Null Rate Bar Chart** | `kpi_type = null_rate` | Horizontal bar per field, gradient green→yellow→red by severity |
| **Outlier Rate Chart** | `kpi_type = outlier_rate` | IQR ×1.5 outlier % per numeric field, colored by source |
| **Volume by Source** | `kpi_type = volume` | Total records per source (Reddit, Last.fm Artists, Last.fm Tracks) |
| **Mean Text Length Chart** | `kpi_type = text_len_mean` | Mean character length per text field, colored by source |
| **Full KPI Table** | All KPI types | Filterable and sortable DataTable with conditional row coloring (red for high null rates, green for 100% compliance) |

---

### 3. Storytelling Dashboard — `dashboard/storytelling_app.py`

**URL:** http://localhost:8051

Designed for the functional user (music label analyst / artist manager) — plain language, insight-first layout.

#### Components

| Component | Data source | Description |
|---|---|---|
| **Narrative Summary Card** | Computed from data | Dynamic text card showing key insight: sentiment percentages, top keywords, leading Last.fm artist |
| **Sentiment Donut Chart** | `metric_type = sentiment_dist` | Positive / Negative / Neutral share with total comment count annotation |
| **Sentiment Trend Line** | `metric_type = sentiment_trend` | Average VADER compound score per ingestion date with positive/negative zone shading |
| **Top Keywords Bar Chart** | `metric_type = top_keyword` | Top 20 tokens colored by associated sentiment (positive/negative/neutral) |
| **Comment Type Breakdown** | `metric_type = comment_type_dist` | recommendation / opinion / mixed / other distribution |
| **Volume Activity Chart** | `metric_type = volume_trend` | Records per date per source (grouped bar) |
| **Source Comparison Chart** | `metric_type = top_artist_lastfm / reddit_artist` | Last.fm listener count with Reddit mention overlay for shared artists |

---

### 4. Real-time File Watching

Both dashboards implement folder-level change detection instead of a fixed 5-minute refresh:

```
Every 15 seconds:
  current_state = fingerprint of all *.parquet in datalake_gold/
                  (relative path + modification timestamp per file)

  if triggered by Interval AND current_state == stored_state:
      → no_update (skip render — gold layer unchanged)

  else (file added / deleted / modified, or manual Refresh button):
      → reload Parquet, rebuild all charts, save new state to Store
```

This means dashboards react within 15 seconds to any change in the Gold layer — including new runs of `gold_pipeline`, manual file deletions, or Parquet overwrites — without constant re-rendering when the data is stable.

---

### 5. Docker Integration — `Dockerfile.dashboard`

A lightweight image separate from the heavy Airflow image:

```dockerfile
FROM python:3.11-slim
RUN pip install dash==2.18.2 plotly==5.24.1 \
    dash-bootstrap-components==1.7.1 pandas==2.3.3 pyarrow==24.0.0
```

Two new services added to `docker-compose.yml`:

| Service | Port | Command |
|---|---|---|
| `governance-dashboard` | 8050 | `python /app/dashboard/governance_app.py` |
| `storytelling-dashboard` | 8051 | `python /app/dashboard/storytelling_app.py` |

Both services mount `./dashboard`, `./datalake_gold` and `./notebooks` as volumes — code changes are reflected without rebuilding the image.

---

## Full Stack URLs

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 |
| Governance Dashboard | http://localhost:8050 |
| Storytelling Dashboard | http://localhost:8051 |
| PostgreSQL | localhost:5432 |

---

## Start Everything

```powershell
docker compose up -d
```

To start only the dashboards (without Airflow):

```powershell
docker compose up -d governance-dashboard storytelling-dashboard
```

To view live logs:

```powershell
docker compose logs -f governance-dashboard storytelling-dashboard
```

---

## Dependencies Added

Registered in `pyproject.toml`:

```toml
"dash (>=2.14.0,<3.0.0)"
"plotly (>=5.18.0,<6.0.0)"
"dash-bootstrap-components (>=1.5.0,<2.0.0)"
"pandas (>=2.0.0,<3.0.0)"
```

> `pyarrow` is installed separately via `poetry run pip install pyarrow` because Python 3.14 (the active environment) does not yet have a pre-built wheel compatible with Poetry's resolver. It is available in the Docker container via `python:3.11-slim`.

---

## Team

| Name | Student ID |
|---|---|
| Carlos Andres Celis Herrera | 20222020051 |
| Juan Diego Lozada Gonzalez | 20222020014 |
| Cristian Santiago Lopez Cadena | 20222020027 |
