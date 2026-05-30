# datalake_gold — Gold Layer (KPIs and Aggregations)

Stores the final datasets produced by the `gold_pipeline` DAG using PySpark. Contains two file types: data governance metrics and storytelling aggregations for the dashboard.

---

## Structure

```
datalake_gold/
├── governance_YYYYMMDD_HHMMSS.parquet    # Data quality KPIs
└── storytelling_YYYYMMDD_HHMMSS.parquet  # Dashboard aggregations
```

---

## `governance_*.parquet` — Data Governance

Each row represents a KPI measured over a specific source and field.

| Field | Type | Description |
|---|---|---|
| `source` | string | `reddit` / `lastfm_artists` / `lastfm_tracks` |
| `field_name` | string | Field name evaluated, or `ALL` for global metrics |
| `kpi_type` | string | KPI type (see table below) |
| `value` | float64 | Numeric KPI value |
| `unit` | string | `count` / `percentage` / `characters` / `days` |
| `computed_at` | string | ISO-8601 computation timestamp |

### KPIs Implemented

| `kpi_type` | Description | Justification |
|---|---|---|
| `volume` | Total records per source | Verifies ingestion completeness |
| `null_rate` | % nulls per field | Detects source degradation |
| `schema_compliance` | % rows with all required fields non-null | Measures silver schema adherence |
| `outlier_rate` | % records outside IQR fence (×1.5) | Identifies anomalies in numeric fields |
| `text_len_mean` | Mean character length | Evaluates text richness |
| `text_len_median` | Median character length | Robust to extremes |
| `text_len_min` | Minimum character length | Detects empty comments post-cleaning |
| `text_len_max` | Maximum character length | Detects abnormally long comments |
| `ingestion_days` | Distinct ingestion dates recorded | Verifies ingestion frequency compliance |

---

## `storytelling_*.parquet` — Dashboard Aggregations

Each row is a data point for a dashboard visualisation.

| Field | Type | Description |
|---|---|---|
| `metric_type` | string | Aggregation type (see table below) |
| `dim1` | string | Primary dimension (label, date, artist, keyword…) |
| `dim2` | string | Secondary dimension (source name, artist for tracks) |
| `record_count` | int64 | Number of records in the group |
| `pct` | float64 | Percentage over the metric total |
| `avg_score` | float64 | Average score (sentiment, listeners, playcount depending on metric) |
| `computed_at` | string | ISO-8601 computation timestamp |

### Metrics Implemented

| `metric_type` | Dimensions | Description | Linked User Story |
|---|---|---|---|
| `sentiment_dist` | `positive/negative/neutral` × `reddit` | VADER sentiment label distribution | Analyst needs positive/negative reception overview |
| `sentiment_trend` | date × `reddit` | Average sentiment score per ingestion date | Temporal tracking of public opinion |
| `comment_type_dist` | `recommendation/opinion/mixed/other` × `reddit` | NLP comment type distribution | Differentiates recommendations from raw opinions |
| `top_keyword` | keyword × `reddit` | Top 25 most frequent tokens (stop-words excluded) | Identifies most-discussed topics and artists |
| `volume_trend` | date × source | Records per date per source | Detects community activity peaks |
| `reddit_artist` | artist × `reddit` | Most mentioned artists in Reddit comments | Artists with highest organic conversation |
| `top_artist_lastfm` | artist × `lastfm` | Top 20 artists by unique listener count | Quantitative popularity ranking |
| `top_track_lastfm` | track × artist | Top 20 tracks by play count | Most-played songs globally |

---

## Sentiment Analysis

**VADER** (Valence Aware Dictionary and sEntiment Reasoner) is applied to the `clean_comment` field from Reddit:

- `compound ≥ 0.05` → `positive`
- `compound ≤ -0.05` → `negative`
- Between both thresholds → `neutral`

VADER is particularly effective for informal social media text (capitalisation, exclamation marks, slang), making it ideal for Reddit music discussions.

---

## Producer DAG

| DAG | Schedule | Technology |
|---|---|---|
| `gold_pipeline` | `@weekly` | PySpark `local[*]`, 1 GB driver memory |
