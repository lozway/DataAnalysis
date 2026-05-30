# datalake_bronze — Capa Bronze (Datos Crudos)

Almacena los datos **crudos e inmutables** tal como los producen las fuentes. Cada ejecución genera un archivo independiente con timestamp — nunca se sobreescribe un archivo existente.

---

## Estructura

```
datalake_bronze/
├── lastfm_top_artists/
│   └── lastfm_top_artists_YYYYMMDD_HHMMSS.json   # Un archivo por ejecución diaria
├── lastfm_top_tracks/
│   └── lastfm_top_tracks_YYYYMMDD_HHMMSS.json    # Un archivo por ejecución diaria
└── reddit/
    └── reddit_music_opinions_YYYYMMDD_HHMMSS.json # Un archivo por scraping manual
```

---

## Formato de Archivos

### Last.fm (`lastfm_top_artists_*.json` / `lastfm_top_tracks_*.json`)

Wrapper estándar con metadatos + respuesta completa de la API:

```json
{
  "_metadata": {
    "ingested_at": "2026-05-29T03:14:46.597506",
    "source": "last.fm",
    "method": "chart.getTopArtists",
    "artist_count": 50
  },
  "data": { ...respuesta original de la API de Last.fm... }
}
```

**Campos por artista:** `name`, `playcount`, `listeners`, `mbid`, `url`, `image[]`, `streamable`  
**Campos por track:** `name`, `duration`, `playcount`, `listeners`, `artist{}`, `mbid`, `url`

### Reddit (`reddit_music_opinions_*.json`)

Lista de posts, cada uno con sus comentarios:

```json
[
  {
    "title": "Boards of Canada - Inferno",
    "score": 312,
    "comments": ["primer comentario...", "segundo comentario...", "..."]
  }
]
```

Fuentes: `r/indieheads` y `r/hiphopheads`. Máximo 5 comentarios de primer nivel por post. Los prefijos de flair (`[FRESH ALBUM]`, `[DISCUSSION]`, etc.) son eliminados del título durante el scraping.

---

## Origen de los Datos

| Carpeta | DAG productor | Frecuencia | Script alternativo |
|---|---|---|---|
| `lastfm_top_artists/` | `lastfm_ingest` | Diaria (`@daily`) | `ingest_lastfm.py` |
| `lastfm_top_tracks/` | `lastfm_ingest` | Diaria (`@daily`) | `ingest_lastfm.py` |
| `reddit/` | Manual | On-demand | `workshop_1/scraping/scraping_reddit.py` |

---

## Principios de la Capa Bronze

- **Inmutabilidad:** ningún archivo existente se modifica o elimina.
- **Completitud:** se persiste la respuesta completa de la API, sin filtrar campos.
- **Trazabilidad:** el campo `_metadata.ingested_at` permite rastrear exactamente cuándo se capturó cada snapshot.
- **Idempotencia:** el DAG `lastfm_ingest` puede ejecutarse varias veces al día sin conflicto gracias al timestamp en el nombre del archivo.
