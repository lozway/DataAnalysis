# notebooks — Exploratory Analysis and Pipeline Development

Contains Jupyter notebooks used to explore raw data, prototype NLP cleaning functions and validate schemas before porting them to Airflow DAGs.

---

## Notebooks

### `esqm_limpieza.ipynb` — Reddit NLP Cleaning Pipeline

Develops and validates the full comment cleaning and classification pipeline that is later ported to the `reddit_silver` DAG.

**Steps documented in the notebook:**

| Step | Function | Description |
|---|---|---|
| 1 | `normalize_nulls` | Standardises `None`, `"[deleted]"`, `""` → NaN |
| 2 | Filter | Drops posts without title or comments |
| 3 | `explode` | One row per comment |
| 4 | `split_multiple_comments` | Splits by line breaks or capitalisation after parentheses |
| 5 | `clean_html_entities` | Decodes HTML entities (`&amp;`, `&#39;`…) |
| 6 | `remove_links` | Removes URLs and Markdown links |
| 7 | `clean_punctuation` | Removes punctuation except `/`, `&`, `-`, `'` |
| 8 | `normalize_text` | Lowercase + collapse multiple spaces |
| 9 | `tokenize` | Split by spaces |
| 10 | `classify_comment` | Classifies as `recommendation` / `opinion` / `mixed` / `other` with confidence score |
| 11 | `extract_artist_song` | Extracts artist and song using `dash`, `by`, `colon` patterns |
| 12 | `cap_outliers` | IQR capping on `word_count` and `score` |
| 13 | Deduplication | By `(post_id, clean_comment)` |
| 14 | Schema enforcement | Type casting and optional fields filled with `"unknown"` |

**Comment classification:**
- `recommendation` → detects music pattern without contrast markers
- `opinion` → more than 6 words or markers like `but`, `however`, `although`
- `mixed` → has music pattern AND contrast markers
- `other` → does not meet any of the above criteria

### `data_quality.ipynb` — Silver Layer Data Quality Findings

Reads **Silver Parquet files** directly and documents all data quality observations:
- Descriptive statistics per source (Reddit, Last.fm Artists, Last.fm Tracks)
- Null rate analysis per field
- Outlier analysis using IQR method (boxplots + statistics table)
- Text length distributions (raw vs clean comment)
- NLP classification findings
- Duplicate rate analysis
- Schema compliance rate
- Structured Data Quality Findings Report (table for the workshop report)

### `gold_preview.ipynb` — Gold Layer Preview

Reads **Gold Parquet files** (`governance_*.parquet` and `storytelling_*.parquet`) and previews:
- Governance KPI table with justification for each selected metric
- Null rate, outlier rate and text length visualisations from the gold layer
- Sentiment distribution (VADER: positive / negative / neutral)
- Sentiment trend over time
- Top keywords and sentiment association
- Comment type distribution
- Volume trends by source
- Top artists and tracks from Last.fm
- Dashboard narrative: what story the data tells

---

## Dependencies

```toml
# pyproject.toml
pandas
pyarrow
matplotlib
jupyter
```

```powershell
poetry run jupyter notebook
```
