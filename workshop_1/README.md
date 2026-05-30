# Workshop 1 — Definición del Proyecto e Investigación de Fuentes

**Curso:** Programación para Análisis de Datos — Semestre 2026-I  
**Proyecto:** Music Artists & Albums Public Perception

---

## Equipo

| Nombre | Código |
|---|---|
| Carlos Andres Celis Herrera | 20222020051 |
| Juan Diego Lozada Gonzalez | 20222020014 |
| Cristian Santiago Lopez Cadena | 20222020027 |

---

## Objetivo

Definir el alcance del proyecto, validar las fuentes de datos y establecer la estrategia de extracción. Este workshop produce los primeros datos crudos del pipeline y establece la arquitectura base de carpetas.

---

## Fuentes Validadas

### 1. API — Last.fm
- **Endpoints usados:** `chart.getTopArtists`, `chart.getTopTracks`
- **Autenticación:** API Key vía variable de entorno `LASTFM_API_KEY`
- **Campos relevantes:**
  - Artistas: `name`, `playcount`, `listeners`, `mbid`, `url`
  - Tracks: `name`, `duration`, `playcount`, `listeners`, `artist.name`, `mbid`
- **Script:** `data_api.py`

### 2. Web Scraping — Reddit
- **Comunidades:** `r/indieheads` y `r/hiphopheads`
- **Método:** BeautifulSoup sobre `old.reddit.com` (HTML estático, sin API key)
- **Filtro:** Posts con keywords de reacción: `[FRESH]`, `thoughts`, `opinion`, `rate`, `album`, `aoty`, `underrated`, etc.
- **Datos extraídos por post:** título, score (karma), hasta 5 comentarios de primer nivel
- **Script:** `scraping/scraping_reddit.py`

---

## Estructura de la Carpeta

```
workshop_1/
├── data/
│   ├── lastfm_music_20260321_222004.json   # Muestra API Last.fm (artistas)
│   └── reddit_music_opinions.json          # Muestra Reddit scraping
├── scraping/
│   └── scraping_reddit.py                  # Web scraper (BeautifulSoup)
├── data_api.py                             # Script de prueba Last.fm API
└── README.md
```

---

## Estrategia de Extracción

| Aspecto | Decisión |
|---|---|
| Formato | JSON crudo con wrapper de metadatos |
| Nomenclatura | `{fuente}_{tema}_{YYYYMMDD}_{HHMMSS}.json` |
| Almacenamiento | `datalake_bronze/` — inmutable, un archivo por ejecución |
| Orquestación | Apache Airflow (DAG `lastfm_ingest` para producción) |

---

## Ejecutar el Scraper

```powershell
# Desde la raíz del proyecto
poetry run python workshop_1/scraping/scraping_reddit.py
```

Genera un archivo en `datalake_bronze/reddit/reddit_music_opinions_YYYYMMDD_HHMMSS.json` con aproximadamente 70 registros (35 por subreddit).

---

## Entregable Principal

[**Reporte Workshop 1 (PDF)**](./WORKSHOP1_Music_Artists_Albums.pdf) — Documento técnico con User Stories, arquitectura general y caracterización de fuentes.
