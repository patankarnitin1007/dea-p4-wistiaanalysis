"""AWS Glue PySpark job: transforms raw Wistia JSON into Bronze/Silver/Gold
Delta tables (dim_media, dim_visitor, fact_media_engagement per FR10).

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
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from wistia_pipeline.config import load_yaml_defaults
from wistia_pipeline.transform import (
    build_dim_media,
    build_dim_visitor,
    build_fact_media_engagement,
    build_silver_media_stats,
    build_silver_visitor_events,
    read_bronze_events,
    read_bronze_media_stats,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("wistia_transformation_job")


def parse_args(argv=None):
    defaults = load_yaml_defaults(section="transformation")
    parser = argparse.ArgumentParser(description="Wistia raw-to-curated transformation job")
    parser.add_argument("--raw-bucket", default=defaults.get("raw_bucket"))
    parser.add_argument("--raw-prefix", default=defaults.get("raw_prefix", "wistia-video-analytics/raw"))
    parser.add_argument("--curated-bucket", default=defaults.get("curated_bucket"))
    parser.add_argument(
        "--curated-prefix", default=defaults.get("curated_prefix", "wistia-video-analytics/curated")
    )
    parser.add_argument("--channel-mapping-json", default=json.dumps(defaults.get("channel_mapping", {})))
    parser.add_argument("--default-channel", default=defaults.get("default_channel", "Unknown"))
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
