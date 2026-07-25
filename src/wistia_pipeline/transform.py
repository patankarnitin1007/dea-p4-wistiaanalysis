"""PySpark transformation logic: raw Wistia JSON -> Bronze -> Silver -> Gold Delta tables.

Kept separate from glue_jobs/transformation_job.py so the DataFrame logic can
be unit tested with a local SparkSession, independent of S3/Glue wiring.

Bronze:  raw JSON parsed against an explicit schema, one row per source record.
Silver:  typed/cleaned/deduplicated, still close to source grain.
Gold:    the FR10 dimensional model - dim_media, dim_visitor, fact_media_engagement.
"""
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

# --- bronze schemas -----------------------------------------------------------

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
    """Reads media_stats/load_date=.../*.json (one JSON object per file)."""
    return (
        spark.read.schema(MEDIA_STATS_SCHEMA)
        .option("multiLine", True)
        .json(f"{raw_path}/media_stats")
    )


def read_bronze_events(spark, raw_path):
    """Reads visitor_stats/load_date=.../*.json (one JSON array per file)."""
    return (
        spark.read.schema(VISITOR_EVENTS_SCHEMA)
        .option("multiLine", True)
        .json(f"{raw_path}/visitor_stats")
    )


# --- silver ---------------------------------------------------------------


def build_silver_media_stats(bronze_media_stats_df):
    """One row per media_id per load_date, struct fields flattened and typed."""
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
    """Deduplicated visitor events, typed timestamp, with a `date` column."""
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


# --- gold: FR10 dimensional model ------------------------------------------


def build_dim_media(silver_media_stats_df, channel_mapping=None, default_channel="Unknown"):
    """Latest snapshot per media_id -> media_id, title, url, channel, created_at.

    Wistia's media object has no native "channel" (e.g. YouTube/Facebook)
    field, so it's resolved from a media_id -> channel mapping supplied by
    the caller (see config/pipeline_config.yaml: transformation.channel_mapping),
    defaulting to `default_channel` for any media_id not listed.
    """
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
    """Latest ip/country per visitor_key -> visitor_id, ip_address, country."""
    latest = Window.partitionBy("visitor_key").orderBy(F.col("received_at").desc_nulls_last())
    ranked = silver_visitor_events_df.withColumn("_rank", F.row_number().over(latest)).where(F.col("_rank") == 1)
    return ranked.select(
        F.col("visitor_key").alias("visitor_id"),
        F.col("ip").alias("ip_address"),
        "country",
    )


def build_fact_media_engagement(silver_visitor_events_df, dim_media_df, silver_media_stats_df):
    """Grain: one row per (media_id, visitor_id, date).

    - play_count: number of play events that visitor generated for that
      media on that date.
    - watched_percent: furthest point reached that day (max percent_viewed).
    - total_watch_time: approximate seconds watched that day, summing
      percent_viewed * media duration across that day's events.
    - play_rate: media-level stat from the latest stats snapshot, denormalized
      onto every row for that media_id (Wistia doesn't expose play_rate at
      visitor grain).
    """
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

    fact = aggregated.join(latest_play_rate, on="media_id", how="left").select(
        "media_id",
        "visitor_id",
        "date",
        "play_count",
        F.col("play_rate"),
        "total_watch_time",
        "watched_percent",
    )
    return fact
