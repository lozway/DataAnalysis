# DataAnalysis

## Projeect Description

**Music Artists & Albums PublicPerception**

Analyze how the public perceives musicians, 
albums, and music trends through community discussions, reviews, and social commentary. The Last.fm API provides artist metadata, tags, and listener statistics, while scraping music communities on Reddit or review platforms such as Pitchfork captures opinionated text for sentiment analysis. The functional user could be a music label analyst or an artist manager monitoring public reception of new releases.
###  Structure of the project

- datalake_bronze/ — for raw JSON ingestion outputs
- datalake_silver/— for processed Parquet files
- datalake_gold/ — for aggregated summary files
- airflow/dags/ — for Airflow DAG definitions
- dashboard/ — for Plotly Dash applications
- notebooks/ — for exploratory analysis notebooks
- workshop_1/ — for this workshop’s deliverables and data samples