"""AWS Glue PySpark job: transforms raw Wistia JSON into Bronze/Silver/Gold
Delta tables (dim_media, dim_visitor, fact_media_engagement per FR10).

Standalone, single-file version of glue_jobs/transformation_job.py with
src/wistia_pipeline/transform.py inlined, for pasting directly into the
Glue Studio Script tab so no separate "Python library path" (--extra-py-files)
zip is needed. The modular version remains the source of truth for local
development, tests, and CI - keep both in sync if you change the
transformation logic.

Required Job parameters (Job details tab, one per line, "--name" / value):
  --raw-bucket           S3 bucket holding the ingestion job's raw zone
  --curated-bucket       S3 bucket to write bronze/silver/gold Delta tables to
Optional (sensible defaults below if omitted):
  --raw-prefix, --curated-prefix, --channel-mapping-json, --default-channel
Also set under Job details:
  --datalake-formats = delta
  Job type = Spark, Glue version = 5.0 (Spark 3.5 + Delta Lake 3.x)
"""
import argparse
import json
import logging
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("wistia_transformation_job")

# --- wistia_pipeline.transform (inlined) --------------------------------------

_THUMBNAIL_SCHEMA = StructType(
    [
        StructField("url", StringType()),
        StructField("width", LongType()),
        StructField("height", LongType()),
    ]
)

MEDIA_STATS_SCHEMA = StructType(
    [
        StructField(
            "media",
            StructType(
                [
                    StructField("id", LongType()),
                    StructField("hashed_id", StringType()),
                    StructField("type", StringType()),
                    StructField("archived", BooleanType()),
                    StructField("name", StringType()),
                    StructField("duration", DoubleType()),
                    StructField("created", StringType()),
                    StructField("updated", StringType()),
                    StructField("status", StringType()),
                    StructField("tags", ArrayType(StringType())),
                    StructField(
                        "project",
                        StructType(
                            [
                                StructField("id", LongType()),
                                StructField("hashed_id", StringType()),
                                StructField("name", StringType()),
                            ]
                        ),
                    ),
                    StructField(
                        "share_link",
                        StructType(
                            [
                                StructField("hashed_id", StringType()),
                                StructField("url", StringType()),
                                StructField("visibility", StringType()),
                            ]
                        ),
                    ),
                ]
            ),
        ),
        StructField(
            "stats",
            StructType(
                [
                    StructField("load_count", LongType()),
                    StructField("play_count", LongType()),
                    StructField("play_rate", DoubleType()),
                    StructField("hours_watched", DoubleType()),
                    StructField("engagement", DoubleType()),
                    StructField("visitors", LongType()),
                ]
            ),
        ),
    ]
)

VISITOR_EVENTS_SCHEMA = StructType(
    [
        StructField("received_at", StringType()),
        StructField("event_key", StringType()),
        StructField("ip", StringType()),
        StructField("country", StringType()),
        StructField("region", StringType()),
        StructField("city", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("org", StringType()),
        StructField("percent_viewed", DoubleType()),
        StructField("visitor_key", StringType()),
        StructField(
            "user_agent_details",
            StructType(
                [
                    StructField("browser", StringType()),
                    StructField("browser_version", StringType()),
                    StructField("platform", StringType()),
                    StructField("mobile", BooleanType()),
                ]
            ),
        ),
        StructField("media_id", StringType()),
        StructField("media_name", StringType()),
        StructField("media_url", StringType()),
        StructField("thumbnail", _THUMBNAIL_SCHEMA),
    ]
)


def read_bronze_media_stats(spark, raw_path):
    return spark.read.schema(MEDIA_STATS_SCHEMA).option("multiLine", True).json(f"{raw_path}/media_stats")


def read_bronze_events(spark, raw_path):
    return spark.read.schema(VISITOR_EVENTS_SCHEMA).option("multiLine", True).json(f"{raw_path}/visitor_stats")


def build_silver_media_stats(bronze_media_stats_df):
    df = bronze_media_stats_df
    return df.select(
        F.col("media.hashed_id").alias("media_id"),
        F.col("media.name").alias("title"),
        F.col("media.share_link.url").alias("url"),
        F.col("media.duration").alias("duration_seconds"),
        F.to_timestamp("media.created").alias("created_at"),
        F.to_timestamp("media.updated").alias("updated_at"),
        F.col("load_date"),
        F.col("stats.load_count").alias("load_count"),
        F.col("stats.play_count").alias("play_count"),
        F.col("stats.play_rate").alias("play_rate"),
        F.col("stats.hours_watched").alias("hours_watched"),
        F.col("stats.engagement").alias("engagement"),
        F.col("stats.visitors").alias("visitors"),
    ).where(F.col("media_id").isNotNull())


def build_silver_visitor_events(bronze_events_df):
    df = bronze_events_df.where(F.col("event_key").isNotNull()).dropDuplicates(["event_key"])
    return df.select(
        "event_key",
        "media_id",
        "visitor_key",
        F.to_timestamp("received_at").alias("received_at"),
        F.to_date("received_at").alias("date"),
        "ip",
        "country",
        "percent_viewed",
    )


def build_dim_media(silver_media_stats_df, channel_mapping=None, default_channel="Unknown"):
    channel_mapping = channel_mapping or {}
    latest = Window.partitionBy("media_id").orderBy(F.col("updated_at").desc_nulls_last(), F.col("load_date").desc())
    ranked = silver_media_stats_df.withColumn("_rank", F.row_number().over(latest)).where(F.col("_rank") == 1)

    mapping_expr = F.create_map([F.lit(x) for pair in channel_mapping.items() for x in pair])
    channel_col = (
        F.coalesce(mapping_expr[F.col("media_id")], F.lit(default_channel))
        if channel_mapping
        else F.lit(default_channel)
    )
    return ranked.select(
        "media_id",
        "title",
        "url",
        channel_col.alias("channel"),
        "created_at",
        "duration_seconds",
    )


def build_dim_visitor(silver_visitor_events_df):
    latest = Window.partitionBy("visitor_key").orderBy(F.col("received_at").desc_nulls_last())
    ranked = silver_visitor_events_df.withColumn("_rank", F.row_number().over(latest)).where(F.col("_rank") == 1)
    return ranked.select(
        F.col("visitor_key").alias("visitor_id"),
        F.col("ip").alias("ip_address"),
        "country",
    )


def build_fact_media_engagement(silver_visitor_events_df, dim_media_df, silver_media_stats_df):
    events = silver_visitor_events_df.where(F.col("visitor_key").isNotNull() & F.col("date").isNotNull())

    aggregated = events.groupBy("media_id", F.col("visitor_key").alias("visitor_id"), "date").agg(
        F.count("event_key").alias("play_count"),
        F.max("percent_viewed").alias("watched_percent"),
        F.sum("percent_viewed").alias("_percent_viewed_sum"),
    )

    duration = dim_media_df.select("media_id", "duration_seconds")
    aggregated = aggregated.join(duration, on="media_id", how="left").withColumn(
        "total_watch_time", F.col("_percent_viewed_sum") * F.coalesce(F.col("duration_seconds"), F.lit(0.0))
    )

    latest_stats = Window.partitionBy("media_id").orderBy(F.col("load_date").desc())
    latest_play_rate = (
        silver_media_stats_df.withColumn("_rank", F.row_number().over(latest_stats))
        .where(F.col("_rank") == 1)
        .select("media_id", "play_rate")
    )

    return aggregated.join(latest_play_rate, on="media_id", how="left").select(
        "media_id",
        "visitor_id",
        "date",
        "play_count",
        F.col("play_rate"),
        "total_watch_time",
        "watched_percent",
    )


# --- job entrypoint (was glue_jobs/transformation_job.py) ---------------------


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Wistia raw-to-curated transformation job")
    parser.add_argument("--raw-bucket", default=None)
    parser.add_argument("--raw-prefix", default="wistia-video-analytics/raw")
    parser.add_argument("--curated-bucket", default=None)
    parser.add_argument("--curated-prefix", default="wistia-video-analytics/curated")
    parser.add_argument("--channel-mapping-json", default="{}")
    parser.add_argument("--default-channel", default="Unknown")
    # Glue's Spark job runner always injects its own arguments (--JOB_NAME,
    # --TempDir, --enable-glue-datacatalog, --datalake-formats, ...) whether
    # or not this job uses them, so unknown args are ignored rather than
    # treated as a parse error.
    args, unknown = parser.parse_known_args(argv)
    if unknown:
        logger.info("Ignoring extra arguments injected by the Glue job runner: %s", unknown)

    missing = [
        name
        for name, value in (("--raw-bucket", args.raw_bucket), ("--curated-bucket", args.curated_bucket))
        if not value
    ]
    if missing:
        parser.error(f"missing required value(s): {', '.join(missing)}")
    return args


def get_spark_session():
    try:
        from awsglue.context import GlueContext
        from pyspark.context import SparkContext

        return GlueContext(SparkContext.getOrCreate()).spark_session
    except ImportError:
        # Not running inside Glue (e.g. local dev) - build a plain SparkSession
        # with Delta Lake wired up via delta-spark.
        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder.appName("wistia-transformation-local")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        try:
            from delta import configure_spark_with_delta_pip

            builder = configure_spark_with_delta_pip(builder)
        except ImportError:
            pass
        return builder.getOrCreate()


def run(args, spark):
    raw_path = f"s3://{args.raw_bucket}/{args.raw_prefix}"
    curated_path = f"s3://{args.curated_bucket}/{args.curated_prefix}"
    channel_mapping = json.loads(args.channel_mapping_json)

    bronze_media_stats = read_bronze_media_stats(spark, raw_path)
    bronze_events = read_bronze_events(spark, raw_path)
    bronze_media_stats.write.format("delta").mode("overwrite").save(f"{curated_path}/bronze/media_stats")
    bronze_events.write.format("delta").mode("overwrite").save(f"{curated_path}/bronze/visitor_events")
    logger.info(
        "Bronze: %d media_stats row(s), %d visitor_event row(s)",
        bronze_media_stats.count(),
        bronze_events.count(),
    )

    silver_media_stats = build_silver_media_stats(bronze_media_stats)
    silver_visitor_events = build_silver_visitor_events(bronze_events)
    silver_media_stats.write.format("delta").mode("overwrite").save(f"{curated_path}/silver/media_stats")
    silver_visitor_events.write.format("delta").mode("overwrite").save(f"{curated_path}/silver/visitor_events")

    dim_media = build_dim_media(silver_media_stats, channel_mapping, args.default_channel)
    dim_visitor = build_dim_visitor(silver_visitor_events)
    fact_media_engagement = build_fact_media_engagement(silver_visitor_events, dim_media, silver_media_stats)

    dim_media.drop("duration_seconds").write.format("delta").mode("overwrite").save(f"{curated_path}/gold/dim_media")
    dim_visitor.write.format("delta").mode("overwrite").save(f"{curated_path}/gold/dim_visitor")
    fact_media_engagement.write.format("delta").mode("overwrite").partitionBy("date").save(
        f"{curated_path}/gold/fact_media_engagement"
    )
    logger.info(
        "Gold: %d dim_media row(s), %d dim_visitor row(s), %d fact_media_engagement row(s)",
        dim_media.count(),
        dim_visitor.count(),
        fact_media_engagement.count(),
    )


def main(argv=None):
    args = parse_args(argv)
    spark = get_spark_session()
    try:
        run(args, spark)
    except Exception:
        logger.exception("Transformation job failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
