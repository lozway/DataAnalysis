# notebooks — Análisis Exploratorio y Desarrollo de Pipelines

Contiene los notebooks de Jupyter usados para explorar los datos crudos, prototipar las funciones de limpieza NLP y validar los schemas antes de llevarlos a los DAGs de Airflow.

---

## Notebooks

### `esqm_limpieza.ipynb` — Pipeline NLP para Reddit

Desarrolla y valida el pipeline completo de limpieza y clasificación de comentarios de Reddit que luego es portado al DAG `reddit_silver`.

**Pasos documentados en el notebook:**

| Paso | Función | Descripción |
|---|---|---|
| 1 | `normalize_nulls` | Estandariza `None`, `"[deleted]"`, `""` → NaN |
| 2 | Filtro | Descarta posts sin título o sin comentarios |
| 3 | `explode` | Una fila por comentario |
| 4 | `split_multiple_comments` | Divide por saltos de línea o mayúsculas tras paréntesis |
| 5 | `clean_html_entities` | Decodifica entidades HTML (`&amp;`, `&#39;`…) |
| 6 | `remove_links` | Elimina URLs y links Markdown |
| 7 | `clean_punctuation` | Elimina puntuación excepto `/`, `&`, `-`, `'` |
| 8 | `normalize_text` | Lowercase + colapso de espacios múltiples |
| 9 | `tokenize` | Split por espacios |
| 10 | `classify_comment` | Clasifica en `recommendation` / `opinion` / `mixed` / `other` con score de confianza |
| 11 | `extract_artist_song` | Extrae artista y canción usando patrones `dash`, `by`, `colon` |
| 12 | `cap_outliers` | Capping IQR sobre `word_count` y `score` |
| 13 | Deduplicación | Por `(post_id, clean_comment)` |
| 14 | Schema enforcement | Casteo de tipos y relleno de opcionales con `"unknown"` |

**Clasificación de comentarios:**

- `recommendation` → detecta patrón musical sin marcadores de contraste
- `opinion` → más de 6 palabras o marcadores como `but`, `however`, `although`
- `mixed` → tiene patrón musical Y marcadores de contraste
- `other` → no cumple ningún criterio anterior

La confianza se calcula sumando pesos: presencia de patrón (+0.45), longitud corta (+0.25/+0.15), contraste (-0.20), tipo mixed (-0.10), puntuación excesiva (-0.10).

---

## Dependencias

```toml
# pyproject.toml
pandas
pyarrow
jupyter
```

```powershell
poetry run jupyter notebook
```
