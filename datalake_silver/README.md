# datalake_silver — Capa Silver (Datos Procesados)

Almacena los datos **normalizados, tipados y deduplicados** en formato Parquet (compresión Snappy). Los archivos silver son producidos por los DAGs de transformación a partir de los JSON crudos de bronze.

---

## Estructura

```
datalake_silver/
├── lastfm_top_artists/
│   └── lastfm_top_artists_YYYYMMDD_HHMMSS.parquet  # Consolidado de todos los bronze
├── lastfm_top_tracks/
│   └── lastfm_top_tracks_YYYYMMDD_HHMMSS.parquet   # Consolidado de todos los bronze
└── reddit/
    └── reddit_music_opinions_YYYYMMDD_HHMMSS.parquet  # NLP procesado
```

---

## Schemas

### `lastfm_top_artists`

| Campo | Tipo | Requerido | Descripción |
|---|---|---|---|
| `name` | string | Sí | Nombre del artista |
| `name_tokens` | string | Sí | Nombre normalizado (lowercase, sin HTML ni puntuación) |
| `playcount` | int64 | Sí | Total de reproducciones |
| `listeners` | int64 | Sí | Oyentes únicos |
| `mbid` | string | No → `unknown` | ID de MusicBrainz |
| `ingested_at` | string | Sí | Timestamp ISO-8601 del archivo bronze de origen |

### `lastfm_top_tracks`

| Campo | Tipo | Requerido | Descripción |
|---|---|---|---|
| `name` | string | Sí | Nombre del track |
| `name_tokens` | string | Sí | Nombre normalizado |
| `duration_sec` | int64 | Sí | Duración en segundos |
| `playcount` | int64 | Sí | Total de reproducciones |
| `listeners` | int64 | Sí | Oyentes únicos |
| `mbid` | string | No → `unknown` | ID de MusicBrainz del track |
| `artist_name` | string | Sí | Nombre del artista |
| `artist_name_tokens` | string | Sí | Nombre del artista normalizado |
| `artist_mbid` | string | No → `unknown` | ID de MusicBrainz del artista |
| `ingested_at` | string | Sí | Timestamp ISO-8601 del archivo bronze de origen |

### `reddit_music_opinions`

| Campo | Tipo | Requerido | Descripción |
|---|---|---|---|
| `post_id` | int64 | Sí | Índice original del post |
| `title` | string | Sí | Título del post (sin prefijo de flair) |
| `score` | int64 | Sí | Karma del post (nulo → 0) |
| `raw_comment_id` | int64 | Sí | Índice del comentario antes del explode |
| `raw_comment` | string | Sí | Texto original del comentario |
| `clean_comment` | string | Sí | Comentario limpio (HTML, links, puntuación removidos) |
| `tokens` | string | Sí | Lista de tokens serializada como string |
| `comment_type` | string | Sí | `recommendation` / `opinion` / `mixed` / `other` |
| `confidence` | float64 | Sí | Confianza de la clasificación (0.0 – 1.0) |
| `has_music_pattern` | bool | Sí | Detectó patrón artista/canción |
| `pattern_type` | string | No → `unknown` | `dash` / `by` / `colon` |
| `has_contrast` | bool | Sí | Contiene marcadores de opinión (but, however…) |
| `word_count` | int64 | Sí | Cantidad de tokens |
| `word_count_capped` | float64 | Sí | word_count con capping IQR |
| `score_capped` | float64 | Sí | score del post con capping IQR |
| `artist` | string | No → `unknown` | Artista extraído del comentario |
| `song` | string | No → `unknown` | Canción extraída del comentario |
| `extract_confidence` | float64 | Sí | Confianza de la extracción artista/canción |
| `ingested_at` | string | Sí | Timestamp ISO-8601 de ingesta |

---

## Pipeline de Transformación

### Last.fm (`dag_lastfm_silver.py`)

1. Lee **todos** los JSON históricos de bronze (no solo el último)
2. Consolida snapshots diarios etiquetados con su `ingested_at` individual
3. Filtra registros inválidos (nombre vacío, playcount = 0)
4. Deduplica en 3 pasos: exactos → mismo nombre más reciente → mayor playcount
5. Genera `name_tokens`: normalización → decode HTML → limpieza de puntuación → eliminación de links
6. Castea tipos y rellena opcionales con `"unknown"`
7. Persiste como Parquet Snappy

### Reddit (`dag_reddit_silver.py`)

1. Normaliza nulos (`None`, `"[deleted]"`, `""` → NaN)
2. Filtra posts sin título o sin comentarios
3. Explota comentarios (una fila por comentario)
4. Divide comentarios multi-oración
5. Limpia HTML, links y puntuación → `clean_comment`
6. Tokeniza → `tokens`
7. Clasifica comentario → `comment_type`, `confidence`, `has_music_pattern`, `pattern_type`, `has_contrast`
8. Extrae artista/canción → `artist`, `song`, `extract_confidence`
9. Aplica capping IQR sobre `score` y `word_count`
10. Deduplica por `(post_id, clean_comment)`
11. Elimina ruido (clean_comment vacío)
12. Enforcea schema y persiste como Parquet Snappy

---

## DAGs Productores

| Carpeta | DAG | Schedule |
|---|---|---|
| `lastfm_top_artists/` | `lastfm_silver` | `@weekly` |
| `lastfm_top_tracks/` | `lastfm_silver` | `@weekly` |
| `reddit/` | `reddit_silver` | Manual |
