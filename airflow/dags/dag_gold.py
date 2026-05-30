"""
DAG: gold_pipeline
Responsabilidad: Lee todos los Parquet de datalake_silver/ con PySpark,
                 produce dos datasets en datalake_gold/:
                   · governance_YYYYMMDD_HHMMSS.parquet  — KPIs de calidad de datos
                   · storytelling_YYYYMMDD_HHMMSS.parquet — agregaciones para dashboard

Configuración PySpark:
    master:                    local[*]   (modo local, usa todos los cores del contenedor)
    spark.driver.memory:       1g
    spark.sql.shuffle.partitions: 4      (dataset pequeño, evita overhead de 200 particiones)
    spark.ui.enabled:          false     (ahorra recursos en Docker)

Schedules:
    gold_pipeline → @weekly  (mismo que silver, corre después)
"""

from __future__ import annotations

import ast
import glob as glob_module
import json
import os
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
from airflow.decorators import dag, task

# ──────────────────────────────────────────────
# Rutas
# ──────────────────────────────────────────────
SILVER_REDDIT_PATH  = "/opt/airflow/datalake_silver/reddit"
SILVER_ARTISTS_PATH = "/opt/airflow/datalake_silver/lastfm_top_artists"
SILVER_TRACKS_PATH  = "/opt/airflow/datalake_silver/lastfm_top_tracks"
GOLD_PATH           = "/opt/airflow/datalake_gold"

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Stop-words básicas para análisis de keywords
STOP_WORDS = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "it", "its", "is", "was", "are", "be", "by", "as",
    "this", "that", "i", "my", "me", "you", "your", "we", "our", "they",
    "their", "he", "she", "his", "her", "have", "has", "had", "do", "does",
    "not", "no", "so", "if", "up", "out", "just", "like", "from", "get",
    "got", "can", "will", "would", "could", "should", "one", "all", "more",
    "also", "very", "really", "about", "there", "what", "when", "how",
    "who", "which", "than", "into", "other", "been", "some", "any",
    # Ruido detectado en keywords de Reddit (bots, links, plataformas)
    "links", "bot", "message", "send", "youtube", "deezer", "spotify",
    "http", "www", "com", "amp", "gt", "lt", "via", "here", "check",
    "listen", "song", "songs", "track", "tracks", "playlist", "link",
}

# ──────────────────────────────────────────────
# SparkSession factory — importada dentro de
# los tasks para no romper el DAG al parsear
# ──────────────────────────────────────────────
def _get_spark(app_name: str):
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.maxResultSize", "512m")
        .getOrCreate()
    )


def _save_parquet(rows: list, schema: pa.Schema, folder: str, filename: str) -> str:
    """
    Append-with-dedup: si el archivo ya existe, lee el histórico,
    concatena los nuevos registros y deduplica por (todas las columnas
    excepto computed_at) + date(computed_at), preservando un snapshot
    por run semanal. Si no existe, escribe directamente.
    """
    import pandas as pd

    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)

    # Construir DataFrame con los nuevos registros
    new_df = pd.DataFrame(rows, columns=[f.name for f in schema])

    if os.path.exists(filepath):
        # Leer histórico existente
        existing_df = pq.read_table(filepath).to_pandas()
        combined   = pd.concat([existing_df, new_df], ignore_index=True)

        # Clave de dedup: todos los campos de dimensión + fecha del run
        # Preserva un registro por (dimensión + día), elimina runs duplicados del mismo día
        dim_cols  = [f.name for f in schema if f.name != "computed_at"]
        combined["_date"] = pd.to_datetime(combined["computed_at"]).dt.date.astype(str)
        dedup_key = dim_cols + ["_date"]
        combined  = combined.drop_duplicates(subset=dedup_key, keep="last")
        combined  = combined.drop(columns=["_date"]).reset_index(drop=True)

        print(f"   📚 Histórico: {len(existing_df)} + {len(new_df)} nuevos → {len(combined)} tras dedup")
        df_to_write = combined
    else:
        print(f"   🆕 Primer run — escribiendo {len(new_df)} registros")
        df_to_write = new_df

    # Castear tipos según schema antes de escribir
    for field in schema:
        if field.name not in df_to_write.columns:
            continue
        if pa.types.is_integer(field.type):
            df_to_write[field.name] = df_to_write[field.name].astype("int64")
        elif pa.types.is_floating(field.type):
            df_to_write[field.name] = df_to_write[field.name].astype("float64")
        elif pa.types.is_string(field.type):
            df_to_write[field.name] = df_to_write[field.name].astype("string")

    table = pa.Table.from_pandas(df_to_write, schema=schema, preserve_index=False)
    pq.write_table(table, filepath, compression="snappy")
    return filepath


# ──────────────────────────────────────────────
# Schemas Gold
# ──────────────────────────────────────────────
GOVERNANCE_SCHEMA = pa.schema([
    pa.field("source",      pa.string(),  nullable=False),
    pa.field("field_name",  pa.string(),  nullable=False),
    pa.field("kpi_type",    pa.string(),  nullable=False),
    pa.field("value",       pa.float64(), nullable=False),
    pa.field("unit",        pa.string(),  nullable=False),
    pa.field("ingest_date",   pa.string(),  nullable=False),  # fecha más reciente de ingesta en silver
    pa.field("computed_at", pa.string(),  nullable=False),
])

STORYTELLING_SCHEMA = pa.schema([
    pa.field("metric_type", pa.string(),  nullable=False),
    pa.field("dim1",        pa.string(),  nullable=False),
    pa.field("dim2",        pa.string(),  nullable=True),
    pa.field("record_count",pa.int64(),   nullable=False),
    pa.field("pct",         pa.float64(), nullable=False),
    pa.field("avg_score",   pa.float64(), nullable=False),
    pa.field("ingest_date",   pa.string(),  nullable=False),  # fecha más reciente de ingesta en silver
    pa.field("computed_at", pa.string(),  nullable=False),
])


# ──────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────
@dag(
    dag_id="gold_pipeline",
    description="Silver → Gold con PySpark: governance KPIs + storytelling aggregations",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["gold", "pyspark", "governance", "storytelling"],
)
def gold_pipeline_dag():

    # ── Tarea 1: Governance (KPIs de calidad) ────────────────────────────
    @task()
    def compute_governance() -> str:
        from pyspark.sql import functions as F

        spark = _get_spark("gold_governance")
        computed_at = datetime.utcnow().isoformat()
        rows = []

        sources = {
            "reddit":         (SILVER_REDDIT_PATH,  ["word_count", "score", "confidence", "extract_confidence"],  ["clean_comment", "raw_comment"]),
            "lastfm_artists": (SILVER_ARTISTS_PATH, ["playcount", "listeners"],                                    ["name"]),
            "lastfm_tracks":  (SILVER_TRACKS_PATH,  ["playcount", "listeners", "duration_sec"],                    ["name", "artist_name"]),
        }

        for source, (path, num_cols, text_cols) in sources.items():
            if not glob_module.glob(os.path.join(path, "*.parquet")):
                print(f"⚠ Sin parquets en {path} — omitiendo {source}")
                continue

            df = spark.read.parquet(path)
            total = df.count()
            if total == 0:
                continue

            # Fecha más reciente de ingesta en silver para este source
            ingest_date = df.agg(F.max("ingested_at")).collect()[0][0][:10]

            # KPI: volumen total
            rows.append((source, "ALL", "volume", float(total), "count", ingest_date, computed_at))

            # KPI: null_rate por campo
            for col in df.columns:
                nulls = df.filter(F.col(col).isNull() | (F.col(col).cast("string") == "")).count()
                rows.append((source, col, "null_rate", round(nulls / total * 100, 4), "percentage", ingest_date, computed_at))

            # KPI: schema_compliance (% filas donde todos los campos no son nulos)
            from functools import reduce
            non_null_filter = reduce(lambda a, b: a & b, [F.col(c).isNotNull() for c in df.columns])
            compliant = df.filter(non_null_filter).count()
            rows.append((source, "ALL", "schema_compliance", round(compliant / total * 100, 4), "percentage", ingest_date, computed_at))

            # KPI: outlier_rate en columnas numéricas (IQR)
            for col in num_cols:
                if col not in df.columns:
                    continue
                q1, q3 = df.stat.approxQuantile(col, [0.25, 0.75], 0.01)
                iqr = q3 - q1
                if iqr == 0:
                    rows.append((source, col, "outlier_rate", 0.0, "percentage", ingest_date, computed_at))
                    continue
                lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
                outliers = df.filter((F.col(col) < lower) | (F.col(col) > upper)).count()
                rows.append((source, col, "outlier_rate", round(outliers / total * 100, 4), "percentage", ingest_date, computed_at))

            # KPI: text_length stats en columnas de texto
            for col in text_cols:
                if col not in df.columns:
                    continue
                stats = df.select(
                    F.mean(F.length(F.col(col))).alias("mean"),
                    F.expr(f"percentile_approx(length(`{col}`), 0.5)").alias("median"),
                    F.min(F.length(F.col(col))).alias("min"),
                    F.max(F.length(F.col(col))).alias("max"),
                ).first()
                for kpi_name, val in [("text_len_mean", stats["mean"]), ("text_len_median", stats["median"]),
                                    ("text_len_min", stats["min"]),   ("text_len_max", stats["max"])]:
                    rows.append((source, col, kpi_name, float(val or 0), "characters", ingest_date, computed_at))

            # KPI: ingestion_days — nro de fechas distintas de ingesta
            days = df.select(F.to_date(F.col("ingested_at")).alias("d")).distinct().count()
            rows.append((source, "ingested_at", "ingestion_days", float(days), "days", ingest_date, computed_at))

        spark.stop()

        path_out = _save_parquet(rows, GOVERNANCE_SCHEMA, GOLD_PATH, "governance_current.parquet")
        print(f"✅ Governance — {len(rows)} KPIs → {path_out}")
        return path_out

    # ── Tarea 2: Storytelling (agregaciones para dashboard) ──────────────
    @task()
    def compute_storytelling() -> str:
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType, DoubleType

        spark = _get_spark("gold_storytelling")
        computed_at = datetime.utcnow().isoformat()
        rows = []

        # ── Sentiment UDFs (VADER) ────────────────────────────────────────
        @F.udf(StringType())
        def sentiment_label(text):
            if not text:
                return "neutral"
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
            c = SentimentIntensityAnalyzer().polarity_scores(str(text))["compound"]
            return "positive" if c >= 0.05 else ("negative" if c <= -0.05 else "neutral")

        @F.udf(DoubleType())
        def sentiment_score(text):
            if not text:
                return 0.0
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
            return float(SentimentIntensityAnalyzer().polarity_scores(str(text))["compound"])

        # ── UDF: parsear tokens (string de lista Python → array) ──────────
        from pyspark.sql.types import ArrayType
        @F.udf(ArrayType(StringType()))
        def parse_tokens(s):
            if not s:
                return []
            try:
                return ast.literal_eval(s)
            except Exception:
                return []

        # ════════════════════════════════════════════════════════════════
        # REDDIT
        # ════════════════════════════════════════════════════════════════
        reddit_files = glob_module.glob(os.path.join(SILVER_REDDIT_PATH, "*.parquet"))
        if reddit_files:
            df_r = spark.read.parquet(SILVER_REDDIT_PATH)
            df_r = df_r.withColumn("sentiment", sentiment_label(F.col("clean_comment")))
            df_r = df_r.withColumn("score_sent", sentiment_score(F.col("clean_comment")))
            total_r = df_r.count()
            ingest_date_r = df_r.agg(F.max("ingested_at")).collect()[0][0][:10]

            # 1. Sentiment distribution
            sent_dist = (
                df_r.groupBy("sentiment")
                    .agg(F.count("*").alias("cnt"), F.avg("score_sent").alias("avg_s"))
                    .collect()
            )
            for row in sent_dist:
                rows.append(("sentiment_dist", row["sentiment"], "reddit",
                            int(row["cnt"]), round(row["cnt"] / total_r * 100, 2),
                            round(row["avg_s"], 4), ingest_date_r, computed_at))

            # 2. Sentiment trend by ingestion date
            trend = (
                df_r.withColumn("date", F.to_date(F.col("ingested_at")))
                    .groupBy("date")
                    .agg(F.count("*").alias("cnt"), F.avg("score_sent").alias("avg_s"))
                    .orderBy("date")
                    .collect()
            )
            for row in trend:
                rows.append(("sentiment_trend", str(row["date"]), "reddit",
                            int(row["cnt"]), 0.0, round(row["avg_s"], 4), str(row["date"]), computed_at))

            # 3. Comment type distribution
            ct_dist = (
                df_r.groupBy("comment_type")
                    .agg(F.count("*").alias("cnt"), F.avg("confidence").alias("avg_conf"))
                    .collect()
            )
            for row in ct_dist:
                rows.append(("comment_type_dist", row["comment_type"], "reddit",
                            int(row["cnt"]), round(row["cnt"] / total_r * 100, 2),
                            round(row["avg_conf"], 4), ingest_date_r, computed_at))

            # 4. Top 25 keywords (excluye stop-words)
            stop_words_bc = spark.sparkContext.broadcast(STOP_WORDS)

            @F.udf(ArrayType(StringType()))
            def filter_tokens(s):
                if not s:
                    return []
                try:
                    tokens = ast.literal_eval(s)
                except Exception:
                    return []
                return [t for t in tokens if t and t not in stop_words_bc.value and len(t) > 2]

            df_tokens = (
                df_r.withColumn("kw", F.explode(filter_tokens(F.col("tokens"))))
                    .groupBy("kw")
                    .agg(F.count("*").alias("cnt"), F.avg("score_sent").alias("avg_s"))
                    .orderBy(F.col("cnt").desc())
                    .limit(25)
                    .collect()
            )
            total_kw = sum(r["cnt"] for r in df_tokens)
            for row in df_tokens:
                rows.append(("top_keyword", row["kw"], "reddit",
                            int(row["cnt"]),
                            round(row["cnt"] / total_kw * 100, 2) if total_kw else 0.0,
                            round(row["avg_s"], 4), ingest_date_r, computed_at))

            # 5. Volume trend (records por fecha)
            vol = (
                df_r.withColumn("date", F.to_date(F.col("ingested_at")))
                    .groupBy("date")
                    .count()
                    .orderBy("date")
                    .collect()
            )
            for row in vol:
                rows.append(("volume_trend", str(row["date"]), "reddit",
                            int(row["count"]), 0.0, 0.0, str(row["date"]), computed_at))

            # 6. Top artistas mencionados en comentarios (campo artist)
            artists_r = (
                df_r.filter(
                    F.col("artist").isNotNull() &
                    (F.col("artist") != "unknown") &
                    (F.col("artist") != "")
                )
                .groupBy("artist")
                .agg(F.count("*").alias("cnt"), F.avg("score_sent").alias("avg_s"))
                .orderBy(F.col("cnt").desc())
                .limit(20)
                .collect()
            )
            total_ar = sum(r["cnt"] for r in artists_r) or 1
            for row in artists_r:
                rows.append(("reddit_artist", row["artist"], "reddit",
                            int(row["cnt"]), round(row["cnt"] / total_ar * 100, 2),
                            round(row["avg_s"], 4), ingest_date_r, computed_at))

        # ════════════════════════════════════════════════════════════════
        # LASTFM — Top Artists
        # ════════════════════════════════════════════════════════════════
        if glob_module.glob(os.path.join(SILVER_ARTISTS_PATH, "*.parquet")):
            df_a = spark.read.parquet(SILVER_ARTISTS_PATH)
            # Snapshot diario — groupBy name + date para análisis temporal
            daily_artists = (
                df_a.withColumn("date", F.to_date(F.col("ingested_at")))
                    .groupBy("name", "date")
                    .agg(
                        F.max("listeners").alias("listeners"),
                        F.max("playcount").alias("playcount"),
                    )
                    .orderBy(F.col("date").asc(), F.col("listeners").desc())
                    .collect()
            )
            # Calcular pct dentro de cada día
            from collections import defaultdict
            totals_by_date = defaultdict(int)
            for r in daily_artists:
                totals_by_date[str(r["date"])] += r["listeners"]

            for row in daily_artists:
                data_date_a = str(row["date"])
                total_day   = totals_by_date[data_date_a] or 1
                rows.append(("top_artist_lastfm", row["name"], "lastfm",
                            int(row["playcount"]),
                            round(row["listeners"] / total_day * 100, 2),
                            float(row["listeners"]), data_date_a, computed_at))

            # Volume trend LastFM artists
            vol_a = (
                df_a.withColumn("date", F.to_date(F.col("ingested_at")))
                    .groupBy("date")
                    .count()
                    .orderBy("date")
                    .collect()
            )
            for row in vol_a:
                rows.append(("volume_trend", str(row["date"]), "lastfm_artists",
                            int(row["count"]), 0.0, 0.0, str(row["date"]), computed_at))

        # ════════════════════════════════════════════════════════════════
        # LASTFM — Top Tracks
        # ════════════════════════════════════════════════════════════════
        if glob_module.glob(os.path.join(SILVER_TRACKS_PATH, "*.parquet")):
            df_t = spark.read.parquet(SILVER_TRACKS_PATH)
            # Snapshot diario — groupBy name + artist_name + date para análisis temporal
            daily_tracks = (
                df_t.withColumn("date", F.to_date(F.col("ingested_at")))
                    .groupBy("name", "artist_name", "date")
                    .agg(
                        F.max("playcount").alias("playcount"),
                        F.max("listeners").alias("listeners"),
                    )
                    .orderBy(F.col("date").asc(), F.col("playcount").desc())
                    .collect()
            )
            totals_tracks_by_date = defaultdict(int)
            for r in daily_tracks:
                totals_tracks_by_date[str(r["date"])] += r["playcount"]

            for row in daily_tracks:
                data_date_t = str(row["date"])
                total_day   = totals_tracks_by_date[data_date_t] or 1
                rows.append(("top_track_lastfm", row["name"], row["artist_name"],
                            int(row["playcount"]),
                            round(row["playcount"] / total_day * 100, 2),
                            float(row["listeners"]), data_date_t, computed_at))

            # Volume trend LastFM tracks
            vol_t = (
                df_t.withColumn("date", F.to_date(F.col("ingested_at")))
                    .groupBy("date")
                    .count()
                    .orderBy("date")
                    .collect()
            )
            for row in vol_t:
                rows.append(("volume_trend", str(row["date"]), "lastfm_tracks",
                            int(row["count"]), 0.0, 0.0, str(row["date"]), computed_at))

        spark.stop()

        path_out = _save_parquet(rows, STORYTELLING_SCHEMA, GOLD_PATH, "storytelling_current.parquet")
        print(f"✅ Storytelling — {len(rows)} filas → {path_out}")
        return path_out

    # ── Tarea 3: Validación ───────────────────────────────────────────────
    @task()
    def validate_gold(gov_path: str, story_path: str) -> dict:
        summary = {}
        for label, path in [("governance", gov_path), ("storytelling", story_path)]:
            if not os.path.exists(path):
                raise FileNotFoundError(f"❌ {label} no encontrado: {path}")
            table = pq.read_table(path)
            if table.num_rows == 0:
                raise ValueError(f"❌ {label} está vacío")
            summary[label] = {"path": path, "rows": table.num_rows}
            print(f"📊 {label}: {table.num_rows} filas → {path}")
        return summary

    # ── Orquestación ──────────────────────────────────────────────────────
    #
    #   [compute_governance]   ──┐
    #                             ├──► [validate_gold]
    #   [compute_storytelling] ──┘
    #
    gov_path   = compute_governance()
    story_path = compute_storytelling()
    validate_gold(gov_path, story_path)


gold_pipeline_dag()