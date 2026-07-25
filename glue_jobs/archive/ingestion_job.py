"""AWS Glue Python Shell job: ingests Wistia media + visitor stats to the S3 raw zone.

Runs as a plain Python script so it works both as a Glue Python Shell job
(job parameters become --arg-name values configured in Glue) and locally /
in CI for testing. Defaults for non-secret settings come from
config/pipeline_config.yaml; any value can be overridden via CLI args.
"""
import argparse
import datetime as dt
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from wistia_pipeline.checkpoint import CheckpointStore
from wistia_pipeline.config import load_yaml_defaults
from wistia_pipeline.s3_writer import RawJsonWriter
from wistia_pipeline.secrets import get_wistia_api_token
from wistia_pipeline.wistia_client import WistiaAPIError, WistiaClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("wistia_ingestion_job")


def parse_args(argv=None):
    defaults = load_yaml_defaults()
    parser = argparse.ArgumentParser(description="Wistia Stats API ingestion job")
    parser.add_argument("--media-ids", default=",".join(defaults.get("media_ids", [])))
    parser.add_argument("--raw-bucket", default=defaults.get("raw_bucket"))
    parser.add_argument("--raw-prefix", default=defaults.get("raw_prefix", "wistia-video-analytics/raw"))
    parser.add_argument("--checkpoint-bucket", default=defaults.get("checkpoint_bucket"))
    parser.add_argument(
        "--checkpoint-key",
        default=defaults.get("checkpoint_key", "wistia-video-analytics/raw/_checkpoint/checkpoint.json"),
    )
    parser.add_argument("--secret-name", default=defaults.get("secret_name"))
    parser.add_argument("--aws-region", default=defaults.get("aws_region"))
    parser.add_argument("--events-per-page", type=int, default=defaults.get("events_per_page", 100))
    parser.add_argument("--max-event-pages", type=int, default=defaults.get("max_event_pages", 50))
    parser.add_argument("--load-date", default=None, help="Override load_date partition, defaults to today (UTC)")
    args = parser.parse_args(argv)

    missing = [
        name
        for name, value in (
            ("--media-ids", args.media_ids),
            ("--raw-bucket", args.raw_bucket),
            ("--checkpoint-bucket", args.checkpoint_bucket),
        )
        if not value
    ]
    if missing:
        parser.error(f"missing required value(s): {', '.join(missing)}")
    return args


def ingest_media(media_id, client, writer, checkpoint, load_date, events_per_page, max_event_pages):
    logger.info("Ingesting media_id=%s", media_id)
    media_checkpoint = checkpoint.setdefault(media_id, {})

    media_snapshot = {
        "media": client.get_media(media_id),
        "stats": client.get_media_stats(media_id),
    }
    writer.write("media_stats", load_date, f"{media_id}.json", media_snapshot)

    last_seen_at = media_checkpoint.get("last_event_received_at")
    latest_seen_at = last_seen_at
    new_events = []
    for event in client.iter_media_events(media_id, per_page=events_per_page, max_pages=max_event_pages):
        received_at = event.get("received_at")
        if not last_seen_at or not received_at or received_at > last_seen_at:
            new_events.append(event)
            if received_at and (not latest_seen_at or received_at > latest_seen_at):
                latest_seen_at = received_at

    if new_events:
        timestamp = dt.datetime.now(dt.timezone.utc).strftime("%H%M%S")
        writer.write("visitor_stats", load_date, f"{media_id}_{timestamp}.json", new_events)
        media_checkpoint["last_event_received_at"] = latest_seen_at
        logger.info("Wrote %d new event(s) for media_id=%s", len(new_events), media_id)
    else:
        logger.info("No new events for media_id=%s since %s", media_id, last_seen_at)

    media_checkpoint["last_ingested_at"] = dt.datetime.now(dt.timezone.utc).isoformat()


def run(args):
    media_ids = [m.strip() for m in args.media_ids.split(",") if m.strip()]
    load_date = args.load_date or dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")

    api_token = get_wistia_api_token(secret_name=args.secret_name, region_name=args.aws_region)
    client = WistiaClient(api_token)
    writer = RawJsonWriter(args.raw_bucket, args.raw_prefix)
    checkpoint_store = CheckpointStore(args.checkpoint_bucket, args.checkpoint_key)
    checkpoint = checkpoint_store.load()

    failures = []
    for media_id in media_ids:
        try:
            ingest_media(
                media_id, client, writer, checkpoint, load_date, args.events_per_page, args.max_event_pages
            )
        except WistiaAPIError:
            logger.exception("Failed to ingest media_id=%s", media_id)
            failures.append(media_id)

    checkpoint_store.save(checkpoint)

    if failures:
        raise RuntimeError(f"Ingestion failed for media_ids: {failures}")


def main(argv=None):
    args = parse_args(argv)
    try:
        run(args)
    except Exception:
        logger.exception("Ingestion job failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
