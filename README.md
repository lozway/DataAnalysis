# Music Artists & Albums Public Perception

**Curso:** Programación para Análisis de Datos — Semestre 2026-I  
**Universidad Distrital Francisco José de Caldas**

**Equipo:**
| Nombre | Código |
|---|---|
| Carlos Andres Celis Herrera | 20222020051 |
| Juan Diego Lozada Gonzalez | 20222020014 |
| Cristian Santiago Lopez Cadena | 20222020027 |

---

## Descripción

Pipeline de datos end-to-end que analiza cómo el público percibe artistas, álbumes y tendencias musicales. Combina datos cuantitativos de la API de Last.fm con opiniones cualitativas extraídas de comunidades de Reddit para producir métricas de sentimiento y gobernanza.

**Usuario funcional:** Analistas de sellos discográficos o managers de artistas que monitorean la recepción pública de lanzamientos.

---

## Arquitectura Medallion

```
Fuentes externas
    ├── Last.fm API  (chart.getTopArtists / chart.getTopTracks)
    └── Reddit scraping  (r/indieheads, r/hiphopheads)
            │
            ▼
    datalake_bronze/   ← JSON crudos, inmutables, con timestamp
            │
            ▼  (Airflow DAGs de transformación)
    datalake_silver/   ← Parquet normalizados, tipados, deduplicados
            │
            ▼  (PySpark @weekly)
    datalake_gold/     ← KPIs de gobernanza + agregaciones para dashboard
```

---

## Estructura del Repositorio

```
.
├── airflow/
│   ├── dags/                    # DAGs de Airflow (orquestación)
│   └── sql/                     # Scripts de inicialización de PostgreSQL
├── datalake_bronze/             # Capa bronze — datos crudos
│   ├── lastfm_top_artists/
│   ├── lastfm_top_tracks/
│   └── reddit/
├── datalake_silver/             # Capa silver — datos procesados
│   ├── lastfm_top_artists/
│   ├── lastfm_top_tracks/
│   └── reddit/
├── datalake_gold/               # Capa gold — KPIs y agregaciones
├── dashboard/                   # Aplicación Plotly Dash
├── notebooks/                   # Notebooks de análisis exploratorio
├── workshop_1/                  # Entregables Workshop 1
├── workshop_2/                  # Entregables Workshop 2
├── docker-compose.yml           # Stack: PostgreSQL + Airflow
├── Dockerfile                   # Imagen custom con Java + PySpark + VADER
├── ingest_lastfm.py             # Script standalone de ingesta Last.fm
└── pyproject.toml               # Dependencias Poetry
```

---

## DAGs y Schedules

| DAG | Schedule | Capa | Descripción |
|---|---|---|---|
| `lastfm_ingest` | `@daily` | Bronze | Extrae top 50 artistas y tracks de Last.fm |
| `lastfm_silver` | `@weekly` | Silver | Normaliza y consolida histórico de Last.fm |
| `reddit_silver` | Manual | Silver | Aplica pipeline NLP a comentarios de Reddit |
| `gold_pipeline` | `@weekly` | Gold | KPIs de gobernanza + agregaciones con PySpark |

---

## Inicio Rápido

```bash
# 1. Configurar variables de entorno
cp .env.example .env   # editar LASTFM_API_KEY

# 2. Construir imagen (incluye Java + PySpark) y levantar stack
docker compose build
docker compose up -d

# 3. Airflow UI
# http://localhost:8080  |  usuario: admin  |  contraseña: admin

# 4. Scraping manual de Reddit (ejecutar desde la raíz del proyecto)
poetry run python workshop_1/scraping/scraping_reddit.py
```

---

## Fuentes de Datos

| Fuente | Tipo | Método | Frecuencia |
|---|---|---|---|
| [Last.fm API](https://www.last.fm/api) | API REST | `chart.getTopArtists`, `chart.getTopTracks` | Diaria |
| [r/indieheads](https://old.reddit.com/r/indieheads/) | Web scraping | BeautifulSoup + old.reddit.com | Manual |
| [r/hiphopheads](https://old.reddit.com/r/hiphopheads/) | Web scraping | BeautifulSoup + old.reddit.com | Manual |
