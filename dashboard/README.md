# dashboard — Aplicación de Visualización

Contiene la aplicación Plotly Dash que consume los Parquet de `datalake_gold/` para presentar el análisis de percepción pública de música al usuario funcional.

---

## Propósito

El dashboard permite a analistas de sellos discográficos y managers de artistas:

- Ver la **distribución de sentimiento** (positivo / negativo / neutral) sobre los comentarios de Reddit
- Seguir la **evolución temporal del sentimiento** semana a semana
- Identificar los **términos más frecuentes** en las discusiones de álbumes
- Comparar **artistas más escuchados** en Last.fm vs. artistas más mencionados en Reddit
- Revisar el **top de tracks** por reproducciones globales

---

## Fuente de Datos

El dashboard lee directamente de `datalake_gold/`:

| Archivo | Uso |
|---|---|
| `storytelling_*.parquet` | Todas las visualizaciones del dashboard |
| `governance_*.parquet` | Panel de calidad de datos (KPIs internos) |

Se selecciona automáticamente el archivo más reciente de cada tipo.

---

## Narrativa del Dashboard

> **"¿Qué está hablando la comunidad y qué tan positivo es?"**

El hilo conductor es el contraste entre popularidad cuantitativa (Last.fm: reproducciones, oyentes) y percepción cualitativa (Reddit: sentimiento, temas mencionados). Un artista puede tener millones de plays en Last.fm mientras su recepción en Reddit es mixta o negativa — o viceversa, un artista emergente genera conversación entusiasta antes de escalar en los charts.

### Visualizaciones planeadas

1. **Distribución de sentimiento** — gráfico de torta o barras apiladas (positivo / negativo / neutral)
2. **Tendencia de sentimiento** — línea temporal con score promedio por semana
3. **Top Keywords** — nube de palabras o barras horizontales con los 25 términos más frecuentes
4. **Tipos de comentario** — barras con `recommendation`, `opinion`, `mixed`, `other`
5. **Top artistas Last.fm** — barras horizontales por oyentes únicos
6. **Top tracks Last.fm** — tabla con track, artista, reproducciones
7. **Artistas mencionados en Reddit** — barras con cantidad de menciones y sentimiento promedio
8. **KPIs de gobernanza** — tabla de métricas de calidad (null rates, volumen, compliance)

---

## Stack Técnico

| Componente | Tecnología |
|---|---|
| Framework UI | Plotly Dash |
| Visualizaciones | Plotly Express / Graph Objects |
| Lectura de datos | PyArrow / Pandas |
| Servidor | Flask (incluido en Dash) |

---

## Ejecutar (pendiente de implementación)

```powershell
poetry run python dashboard/app.py
# http://localhost:8050
```
