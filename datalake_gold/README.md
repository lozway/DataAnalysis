# datalake_gold — Capa Gold (KPIs y Agregaciones)

Almacena los datasets finales producidos por el DAG `gold_pipeline` con PySpark. Contiene dos tipos de archivos: métricas de gobernanza de datos y agregaciones para el dashboard de storytelling.

---

## Estructura

```
datalake_gold/
├── governance_YYYYMMDD_HHMMSS.parquet    # KPIs de calidad de datos
└── storytelling_YYYYMMDD_HHMMSS.parquet  # Agregaciones para dashboard
```

---

## `governance_*.parquet` — Gobernanza de Datos

Cada fila representa un KPI medido sobre una fuente y campo específico.

| Campo | Tipo | Descripción |
|---|---|---|
| `source` | string | `reddit` / `lastfm_artists` / `lastfm_tracks` |
| `field_name` | string | Nombre del campo evaluado, o `ALL` para métricas globales |
| `kpi_type` | string | Tipo de KPI (ver tabla abajo) |
| `value` | float64 | Valor numérico del KPI |
| `unit` | string | `count` / `percentage` / `characters` / `days` |
| `computed_at` | string | Timestamp ISO-8601 de cómputo |

### KPIs implementados

| `kpi_type` | Descripción | Justificación |
|---|---|---|
| `volume` | Total de registros por fuente | Verifica completitud de ingesta |
| `null_rate` | % de nulos por campo | Detecta degradación en fuentes |
| `schema_compliance` | % de filas con todos los campos no nulos | Mide adherencia al schema silver |
| `outlier_rate` | % de registros fuera del rango IQR (×1.5) | Identifica anomalías en numéricos |
| `text_len_mean` | Longitud media en caracteres | Evalúa riqueza del texto |
| `text_len_median` | Longitud mediana en caracteres | Robusto ante extremos |
| `text_len_min` | Longitud mínima en caracteres | Detecta comentarios vacíos post-limpieza |
| `text_len_max` | Longitud máxima en caracteres | Detecta comentarios anómalamente largos |
| `ingestion_days` | Días distintos de ingesta registrados | Verifica frecuencia de ingesta |

---

## `storytelling_*.parquet` — Agregaciones para Dashboard

Cada fila es un punto de datos para una visualización del dashboard.

| Campo | Tipo | Descripción |
|---|---|---|
| `metric_type` | string | Tipo de agregación (ver tabla abajo) |
| `dim1` | string | Dimensión primaria (etiqueta, fecha, artista, keyword…) |
| `dim2` | string | Dimensión secundaria (fuente, nombre del artista para tracks) |
| `record_count` | int64 | Cantidad de registros en el grupo |
| `pct` | float64 | Porcentaje sobre el total de la métrica |
| `avg_score` | float64 | Score promedio (sentimiento, listeners, playcount según métrica) |
| `computed_at` | string | Timestamp ISO-8601 de cómputo |

### Métricas implementadas

| `metric_type` | Dimensiones | Descripción | User Story |
|---|---|---|---|
| `sentiment_dist` | `positive/negative/neutral` × `reddit` | Distribución de sentimiento VADER por label | Analista quiere saber si la recepción es positiva o negativa |
| `sentiment_trend` | fecha × `reddit` | Sentimiento promedio por fecha de ingesta | Seguimiento temporal de la opinión pública |
| `comment_type_dist` | `recommendation/opinion/mixed/other` × `reddit` | Distribución del tipo de comentario | Diferencia recomendaciones de opiniones crudas |
| `top_keyword` | keyword × `reddit` | Top 25 términos más frecuentes (sin stop-words) | Identifica temas y artistas más mencionados |
| `volume_trend` | fecha × fuente | Registros por fecha por fuente | Picos de actividad de la comunidad |
| `reddit_artist` | artista × `reddit` | Artistas más mencionados en comentarios | Artistas con mayor conversación orgánica |
| `top_artist_lastfm` | artista × `lastfm` | Top 20 artistas por oyentes únicos | Ranking cuantitativo de popularidad |
| `top_track_lastfm` | track × artista | Top 20 tracks por reproducciones | Canciones más escuchadas globalmente |

---

## Análisis de Sentimiento

Se usa **VADER** (Valence Aware Dictionary and sEntiment Reasoner) aplicado sobre el campo `clean_comment` de Reddit:

- `compound ≥ 0.05` → `positive`
- `compound ≤ -0.05` → `negative`
- Entre ambos → `neutral`

VADER es especialmente efectivo para texto informal de redes sociales (capitalización, signos de exclamación, jerga), lo que lo hace idóneo para comentarios de Reddit sobre música.

---

## DAG Productor

| DAG | Schedule | Tecnología |
|---|---|---|
| `gold_pipeline` | `@weekly` | PySpark `local[*]`, driver 1 GB |
