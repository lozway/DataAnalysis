# dashboard — Visualisation Application

Contains the Plotly Dash application that consumes the `datalake_gold/` Parquet files to present the public music perception analysis to the functional user.

---

## Purpose

The dashboard allows music label analysts and artist managers to:

- View the **sentiment distribution** (positive / negative / neutral) across Reddit comments
- Track the **temporal evolution of sentiment** week by week
- Identify the **most frequent terms** in album discussions
- Compare **most-listened artists** on Last.fm vs. **most-mentioned artists** on Reddit
- Review the **top tracks** by global play count

---

## Data Source

The dashboard reads directly from `datalake_gold/`:

| File | Usage |
|---|---|
| `storytelling_*.parquet` | All dashboard visualisations |
| `governance_*.parquet` | Data quality KPI panel (internal metrics) |

The most recent file of each type is selected automatically.

---

## Dashboard Narrative

> **"What is the community talking about, and how positive is it?"**

The central thread is the contrast between quantitative popularity (Last.fm: plays, listeners) and qualitative perception (Reddit: sentiment, mentioned topics). An artist can have millions of Last.fm plays while Reddit reception is mixed or negative — or conversely, an emerging artist generates enthusiastic conversation before scaling in the charts.

### Planned Visualisations

1. **Sentiment distribution** — pie or stacked bar chart (positive / negative / neutral)
2. **Sentiment trend** — time series with average score per week
3. **Top keywords** — word cloud or horizontal bar chart with the 25 most frequent terms
4. **Comment type breakdown** — bars for `recommendation`, `opinion`, `mixed`, `other`
5. **Top Last.fm artists** — horizontal bars by unique listeners
6. **Top Last.fm tracks** — table with track, artist, play count
7. **Artists mentioned on Reddit** — bars with mention count and average sentiment
8. **Governance KPIs** — data quality metrics table (null rates, volume, compliance)

---

## Tech Stack

| Component | Technology |
|---|---|
| UI Framework | Plotly Dash |
| Visualisations | Plotly Express / Graph Objects |
| Data reading | PyArrow / Pandas |
| Server | Flask (bundled with Dash) |

---

## Run (pending implementation)

```powershell
poetry run python dashboard/app.py
# http://localhost:8050
```
