# Workshop 1: Project Definition and Source Research

## Project Title
**Music Artists & Albums Public Perception** 
## Team Members
* **Carlos Andres Celis Herrera** - 20222020051 
* [cite_start]**Juan Diego Lozada Gonzalez** - 20222020014 
* [cite_start]**Cristian Santiago Lopez Cadena** - 20222020027 

## 1. Project Overview
This project aims to build an end-to-end data engineering and NLP pipeline to analyze how the public perceives music, artists, and albums. By combining quantitative data from APIs and qualitative data from web scraping, we seek to identify patterns and trends that enable informed decision-making in the music market.

## 2. Validated Data Sources
In accordance with the workshop requirements, two main sources have been identified and validated[cite: 23, 174]:

* **API Source (Last.fm):** Provides structured metadata such as play counts, listener statistics, and artist tags.
* **Web Scraping Source (Reddit):** Specifically targeting the `r/musicsuggestions` subreddit to extract organic opinions, recommendations, and complex qualitative data.

## 3. Data Samples
The `workshop_1/` directory includes representative samples (minimum 20 records each) to verify accessibility and data quality:
* **`lastfm_music_sample.json`**: Raw JSON output from the Last.fm API containing artist metadata.
* **`reddit_music_opinions.json`**: Structured records of titles and comments extracted from Reddit.

## 4. Extraction Strategy
* **Format:** Raw data is persisted in JSON format.
* **Nomenclature:** Files follow the standard `source_topic_YYYYMMDD_HHMMSS.json`.
* **Ingestion:** The process is designed to be orchestrated via Apache Airflow to maintain an immutable history in the Bronze layer.

## 5. Repository Structure
Following the course guidelines, the project is organized as follows:
* `datalake_bronze/`: Storage for raw JSON ingestion outputs.
* `datalake_silver/`: For processed Parquet files.
* `datalake_gold/`: For aggregated summary files.
* `workshop_1/`: Deliverables and data samples for this workshop.

## 6. Main Deliverable
* [**Project Definition Report (PDF)**](./WORKSHOP1_Music_Artists_Albums.pdf): Full technical report including User Stories, Architecture Overview, and Source Characterization.
