# datalake_silver — Silver Layer (Processed Data)

Stores **normalised, typed and deduplicated** data in Parquet format (Snappy compression). Silver files are produced by transformation DAGs from the raw bronze JSON files.

---

## Structure

```
datalake_silver/
├── lastfm_top_artists/
│   └── lastfm_top_artists_YYYYMMDD_HHMMSS.parquet  # Consolidated from all bronze snapshots
├── lastfm_top_tracks/
│   └── lastfm_top_tracks_YYYYMMDD_HHMMSS.parquet   # Consolidated from all bronze snapshots
└── reddit/
    └── reddit_music_opinions_YYYYMMDD_HHMMSS.parquet  # NLP-processed Reddit data
```

---

## Schemas

### `lastfm_top_artists`

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | Artist name |
| `name_tokens` | string | Yes | Normalised name (lowercase, no HTML or punctuation) |
| `playcount` | int64 | Yes | Total play count |
| `listeners` | int64 | Yes | Unique listener count |
| `mbid` | string | No → `unknown` | MusicBrainz ID |
| `ingested_at` | string | Yes | ISO-8601 timestamp of the source bronze file |

### `lastfm_top_tracks`

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | Track name |
| `name_tokens` | string | Yes | Normalised name |
| `duration_sec` | int64 | Yes | Duration in seconds |
| `playcount` | int64 | Yes | Total play count |
| `listeners` | int64 | Yes | Unique listener count |
| `mbid` | string | No → `unknown` | Track MusicBrainz ID |
| `artist_name` | string | Yes | Artist name |
| `artist_name_tokens` | string | Yes | Normalised artist name |
| `artist_mbid` | string | No → `unknown` | Artist MusicBrainz ID |
| `ingested_at` | string | Yes | ISO-8601 timestamp of the source bronze file |

### `reddit_music_opinions`

| Field | Type | Required | Description |
|---|---|---|---|
| `post_id` | int64 | Yes | Original post index |
| `title` | string | Yes | Post title (flair prefix removed) |
| `score` | int64 | Yes | Post karma (null → 0) |
| `raw_comment_id` | int64 | Yes | Comment index before explode |
| `raw_comment` | string | Yes | Original comment text |
| `clean_comment` | string | Yes | Cleaned comment (HTML, links, punctuation removed) |
| `tokens` | string | Yes | Token list serialised as string |
| `comment_type` | string | Yes | `recommendation` / `opinion` / `mixed` / `other` |
| `confidence` | float64 | Yes | Classification confidence (0.0 – 1.0) |
| `has_music_pattern` | bool | Yes | Music artist/song pattern detected |
| `pattern_type` | string | No → `unknown` | `dash` / `by` / `colon` |
| `has_contrast` | bool | Yes | Contains opinion markers (but, however…) |
| `word_count` | int64 | Yes | Token count |
| `word_count_capped` | float64 | Yes | IQR-capped word count |
| `score_capped` | float64 | Yes | IQR-capped post score |
| `artist` | string | No → `unknown` | Artist extracted from comment |
| `song` | string | No → `unknown` | Song extracted from comment |
| `extract_confidence` | float64 | Yes | Artist/song extraction confidence |
| `ingested_at` | string | Yes | ISO-8601 ingestion timestamp |

---

## Transformation Pipelines

### Last.fm (`dag_lastfm_silver.py`)

1. Reads **all** historical bronze JSONs (not just the latest)
2. Consolidates daily snapshots, each tagged with its individual `ingested_at`
3. Filters invalid records (empty name, playcount = 0)
4. 3-pass deduplication: exact → same name latest date → highest playcount
5. Builds `name_tokens`: normalise → HTML decode → punctuation cleaning → link removal
6. Casts types and fills optional fields with `"unknown"`
7. Writes as Snappy Parquet

### Reddit (`dag_reddit_silver.py`)

1. Normalises nulls (`None`, `"[deleted]"`, `""` → NaN)
2. Filters posts without title or comments
3. Explodes comments (one row per comment)
4. Splits multi-sentence comments
5. Cleans HTML, links and punctuation → `clean_comment`
6. Tokenises → `tokens`
7. Classifies comment → `comment_type`, `confidence`, `has_music_pattern`, `pattern_type`, `has_contrast`
8. Extracts artist/song → `artist`, `song`, `extract_confidence`
9. Applies IQR capping on `score` and `word_count`
10. Deduplicates by `(post_id, clean_comment)`
11. Removes noise (empty clean_comment)
12. Enforces schema and writes as Snappy Parquet

---

## Producer DAGs

| Folder | DAG | Schedule |
|---|---|---|
| `lastfm_top_artists/` | `lastfm_silver` | `@weekly` |
| `lastfm_top_tracks/` | `lastfm_silver` | `@weekly` |
| `reddit/` | `reddit_silver` | Manual |
