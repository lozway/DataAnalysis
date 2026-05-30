# Airflow — Orquestación del Pipeline

Contiene todos los DAGs y scripts SQL que conforman la capa de orquestación del proyecto. El stack corre en Docker con Apache Airflow 2.8.1 y PostgreSQL 15.

---

## DAGs

### `dag_lastfm_ingest.py` — Bronze · `@daily`

Extrae los rankings globales de Last.fm y persiste los JSON crudos en `datalake_bronze/`.

```
extract_top_tracks ─┐
                     ├─► validate_bronze_files
extract_top_artists─┘
```

| Task | Descripción |
|---|---|
| `extract_top_tracks` | Llama `chart.getTopTracks` (top 50) → `datalake_bronze/lastfm_top_tracks/` |
| `extract_top_artists` | Llama `chart.getTopArtists` (top 50) → `datalake_bronze/lastfm_top_artists/` |
| `validate_bronze_files` | Verifica existencia e integridad de ambos JSON |

---

### `dag_lastfm_silver.py` — Silver · `@weekly`

Lee **todos** los JSON históricos de bronze, consolida snapshots diarios, deduplica y produce Parquet normalizados.

```
wait_for_artists_bronze ─► transform_top_artists ─┐
                                                    ├─► validate_silver_files
wait_for_tracks_bronze  ─► transform_top_tracks  ─┘
```

**Pipeline de limpieza:** filtro de inválidos → normalización de texto → decode HTML → limpieza de puntuación → deduplicación 3-pass → enforcement de schema → Parquet Snappy.

---

### `dag_reddit_silver.py` — Silver · Manual (on-demand)

Aplica el pipeline NLP completo a los comentarios de Reddit y produce un Parquet estructurado.

```
wait_for_reddit_bronze ─► transform_reddit ─► validate_silver_file
```

**Pipeline NLP (15 pasos):** normalización de nulos → explosión de comentarios → split multi-oración → limpieza HTML/links/puntuación → tokenización → clasificación de comentario (`recommendation` / `opinion` / `mixed` / `other`) → extracción de artista/canción → capping IQR → deduplicación → enforcement de schema.

---

### `dag_gold.py` — Gold · `@weekly`

Lee los Parquet de silver con PySpark en modo local y produce dos archivos gold.

```
compute_governance   ─┐
                       ├─► validate_gold
compute_storytelling ─┘
```

| Task | Output | Contenido |
|---|---|---|
| `compute_governance` | `governance_*.parquet` | KPIs: null rate, volumen, schema compliance, outlier rate (IQR), text length stats, ingestion days |
| `compute_storytelling` | `storytelling_*.parquet` | Sentiment VADER, trends, top keywords, comment types, top artistas/tracks LastFM |

**Configuración PySpark:** `local[*]`, driver memory 1 GB, shuffle partitions 4, UI deshabilitada.

---

## SQL

### `sql/init_es_db.sql`

Script de inicialización que PostgreSQL ejecuta al primer arranque. Crea la base de datos secundaria requerida por el stack.

---

## Conexión Airflow necesaria

El sensor de archivos (`FileSensor`) requiere la conexión `fs_default` de tipo `File (path)`. Se crea automáticamente en el servicio `airflow-init` del docker-compose:

```bash
airflow connections add fs_default --conn-type fs --conn-extra '{"path": "/"}'
```

---

## Variables de Entorno requeridas

| Variable | Descripción |
|---|---|
| `LASTFM_API_KEY` | API key de Last.fm |
| `LASTFM_USER_AGENT` | User agent para peticiones (default: `AirflowPipeline`) |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | Credenciales de PostgreSQL |
| `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` | Credenciales del admin de Airflow |
