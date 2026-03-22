# Workshop 1: Project Definition and Source Research

## Project Title
**Music Artists & Albums Public Perception** 
## Team Members
* [cite_start]**Carlos Andres Celis Herrera** - 20222020051 [cite: 138]
* [cite_start]**Juan Diego Lozada Gonzalez** - 20222020014 [cite: 138]
* [cite_start]**Cristian Santiago Lopez Cadena** - 202220200 [cite: 138]

## 1. Project Overview
[cite_start]This project aims to build an end-to-end data engineering and NLP pipeline to analyze how the public perceives music, artists, and albums[cite: 151, 163]. [cite_start]By combining quantitative data from APIs and qualitative data from web scraping, we seek to identify patterns and trends that enable informed decision-making in the music market[cite: 152, 163].

## 2. Validated Data Sources
[cite_start]In accordance with the workshop requirements, two main sources have been identified and validated[cite: 23, 174]:

* [cite_start]**API Source (Last.fm):** Provides structured metadata such as play counts, listener statistics, and artist tags[cite: 191, 193, 222].
* [cite_start]**Web Scraping Source (Reddit):** Specifically targeting the `r/musicsuggestions` subreddit to extract organic opinions, recommendations, and complex qualitative data[cite: 195, 200, 207].

## 3. Data Samples
[cite_start]The `workshop_1/` directory includes representative samples (minimum 20 records each) to verify accessibility and data quality[cite: 25, 47]:
* [cite_start]**`lastfm_music_sample.json`**: Raw JSON output from the Last.fm API containing artist metadata[cite: 232, 233].
* [cite_start]**`reddit_music_opinions.json`**: Structured records of titles and comments extracted from Reddit[cite: 284, 311].

## 4. Extraction Strategy
* [cite_start]**Format:** Raw data is persisted in JSON format[cite: 53, 329].
* [cite_start]**Nomenclature:** Files follow the standard `source_topic_YYYYMMDD_HHMMSS.json`[cite: 53, 330].
* [cite_start]**Ingestion:** The process is designed to be orchestrated via Apache Airflow to maintain an immutable history in the Bronze layer[cite: 331, 348].

## 5. Repository Structure
[cite_start]Following the course guidelines, the project is organized as follows[cite: 90]:
* [cite_start]`datalake_bronze/`: Storage for raw JSON ingestion outputs[cite: 91, 92].
* [cite_start]`datalake_silver/`: For processed Parquet files[cite: 93, 94].
* [cite_start]`datalake_gold/`: For aggregated summary files[cite: 95, 96].
* [cite_start]`workshop_1/`: Deliverables and data samples for this workshop[cite: 102].

## 6. Main Deliverable
* [cite_start][**Project Definition Report (PDF)**](./WORKSHOP1_Music_Artists_Albums.pdf): Full technical report including User Stories, Architecture Overview, and Source Characterization[cite: 78, 79].
