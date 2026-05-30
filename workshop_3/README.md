# Workshop 3 — Gold Layer, Governance and Storytelling

**Course:** Data Analysis Programming — Semester 2026-I  
**Project:** Music Artists & Albums Public Perception  
**Universidad Distrital Francisco José de Caldas**

---

## Overview

This workshop extends the Medallion Architecture pipeline built in Workshop 2 by introducing the **Gold layer**, a **data governance framework** and a **storytelling summary** designed to feed a functional dashboard for music label analysts and artist managers.

---

## What Was Built

### Gold Layer — `dag_gold.py` · `@weekly`

A PySpark pipeline that reads all Silver Parquet files and produces two output datasets in `datalake_gold/`:

| Task | Output | Contents |
|---|---|---|
| `compute_governance` | `governance_*.parquet` | 9 data quality KPI types across 3 sources — null rate, volume, schema compliance, outlier rate (IQR), text length stats, ingestion days |
| `compute_storytelling` | `storytelling_*.parquet` | 8 metric types — VADER sentiment distribution, sentiment trend, top keywords, comment type breakdown, volume trends, top artists/tracks from Last.fm |

**PySpark config:** `local[*]`, 1 GB driver memory, 4 shuffle partitions, UI disabled.

---

## Pipeline Improvements from Workshop 2

| Component | Before | After |
|---|---|---|
| Docker — `fs_default` | Created manually in UI after every `docker compose up` | Auto-created in `airflow-init` entrypoint |
| Docker image | `apache/airflow:2.8.1` — no Java, no PySpark | Custom `Dockerfile` with OpenJDK 17, PySpark 3.5.1, vaderSentiment 3.3.2 |
| `lastfm_ingest` | Triggered Silver via `TriggerDagRunOperator` | Decoupled — Silver runs on its own `@weekly` schedule |
| `lastfm_silver` | `@daily`; read only latest Bronze JSON | `@weekly`; reads all accumulated Bronze files |
| Deduplication key | `name` only — lost temporal snapshots | `name + date` — preserves daily time series |
| Silver output | New timestamped Parquet per run | Single `_current.parquet` overwritten weekly |
| `reddit_silver` | Latest Bronze JSON only; `post_id`-based dedup | All Bronze files; stable `title + clean_comment + date` key |

---

## Data Quality Findings (Silver Layer)

| Source | Rows | Schema Compliance | Key Finding |
|---|---|---|---|
| Reddit | 228 | 100% | `artist`/`song` >95% `unknown` — users discuss albums, not individual songs |
| Last.fm Artists | 650+ | 100% | `playcount` heavily right-skewed (BTS: 4.12B plays vs median 270M) |
| Last.fm Tracks | 650+ | 100% | `mbid` 25–40% unknown — not all entities registered in MusicBrainz |

Reddit comment type breakdown: **78.5% opinions**, 21.1% other, 0.4% mixed, **0% recommendations** → pivot to sentiment analysis as primary output.

---

## Key Analytical Results

| Metric | Value |
|---|---|
| Reddit comments analysed | 228 |
| Positive sentiment | 155 (67.98%) · avg VADER score: +0.62 |
| Negative sentiment | 32 (14.04%) · avg VADER score: −0.52 |
| Neutral sentiment | 41 (17.98%) |
| Overall avg compound score | +0.3483 |
| Top artist (Last.fm) | BTS — 6.8M listeners, 4.12B plays |
| Governance KPI rows produced | 73 |
| Storytelling rows produced | 79 |

---

## Notebooks

| Notebook | Purpose |
|---|---|
| [data_quality.ipynb](../notebooks/data_quality.ipynb) | Reads Silver Parquet files — descriptive stats, null rates, IQR outlier boxplots, text length distributions, NLP classification findings, data quality findings report |
| [gold_preview.ipynb](../notebooks/gold_preview.ipynb) | Reads Gold Parquet files — governance KPI preview, sentiment charts, keyword analysis, Last.fm rankings, dashboard narrative |

---

## Folder Structure

```
workshop_3/
├── WORKSHOP3_Music_Artists_Albums.pdf
└── README.md
```

---

## Team

| Name | Student ID |
|---|---|
| Carlos Andres Celis Herrera | 20222020051 |
| Juan Diego Lozada Gonzalez | 20222020014 |
| Cristian Santiago Lopez Cadena | 20222020027 |
