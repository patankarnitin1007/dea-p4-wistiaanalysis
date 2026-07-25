"""AWS Glue Python Shell job: ingests Wistia media + visitor stats to the S3 raw zone.

Standalone, single-file version of glue_jobs/ingestion_job.py with the
wistia_pipeline package inlined, for pasting directly into the Glue Studio
Script tab so no separate "Python library path" zip is needed. The
modular version (glue_jobs/ingestion_job.py + src/wistia_pipeline/) remains
the source of truth for local development, tests, and CI/CD to ECR - keep
both in sync if you change the ingestion logic.

Required Job parameters (Job details tab, one per line, "--name" / value):
  --media-ids            comma-separated Wistia hashed media IDs, e.g. 8hunphufxp,9k4tbcdfg0
  --raw-bucket           S3 bucket for the raw zone
  --checkpoint-bucket    S3 bucket for checkpoint.json (can be the same bucket)
Optional (sensible defaults below if omitted):
  --raw-prefix, --checkpoint-key, --secret-name, --aws-region,
  --events-per-page, --max-event-pages, --load-date
Also set under Job details:
  --additional-python-modules = requests==2.31.0
(boto3 ships with the Glue Python Shell runtime already.)
"""
import argparse
import datetime as dt
import json
import logging
import sys
import time

import boto3
import requests
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("wistia_ingestion_job")

# --- wistia_pipeline.secrets -------------------------------------------------

ENV_TOKEN_VAR = "WISTIA_API_TOKEN"
DEFAULT_SECRET_KEY = "api_token"


def get_wistia_api_token(secret_name=None, secret_key=DEFAULT_SECRET_KEY, region_name=None, secrets_client=None):
    import os

    env_token = os.environ.get(ENV_TOKEN_VAR)
    if env_token:
        logger.info("Using Wistia API token from %s environment variable", ENV_TOKEN_VAR)
        return env_token

    if not secret_name:
        raise RuntimeError(
            f"No Wistia API token available: set {ENV_TOKEN_VAR} or pass --secret-name "
            "pointing at an AWS Secrets Manager secret"
        )

    client = secrets_client or boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret_string = response["SecretString"]
    try:
        return json.loads(secret_string)[secret_key]
    except (json.JSONDecodeError, KeyError):
        # Secret was stored as a plain string rather than a {"api_token": ...} blob.
        return secret_string


# --- wistia_pipeline.wistia_client -------------------------------------------

WISTIA_API_BASE = "https://api.wistia.com/v1"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class WistiaAPIError(Exception):
    """Raised for any non-recoverable failure calling the Wistia API."""


class WistiaClient:
    def __init__(self, api_token, base_url=WISTIA_API_BASE, timeout=30, max_retries=5, backoff_factor=2.0):
        self._session = requests.Session()
        self._session.headers.update({"Authorization": f"Bearer {api_token}"})
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_retries = max_retries
        self._backoff_factor = backoff_factor

    def get_media(self, media_id):
        """Media metadata: title, hashed_id, created, updated, duration, etc."""
        return self._get(f"medias/{media_id}.json")

    def get_media_stats(self, media_id):
        """Media-level engagement stats: play_count, play_rate, hours_watched, etc."""
        return self._get(f"stats/medias/{media_id}.json")

    def iter_media_events(self, media_id, per_page=100, max_pages=50):
        """Yields visitor-level play events for a media, one dict per event."""
        page = 1
        while page <= max_pages:
            batch = self._get(
                "stats/events.json",
                params={"media_id": media_id, "page": page, "per_page": per_page},
            )
            if not batch:
                return
            yield from batch
            if len(batch) < per_page:
                return
            page += 1
        logger.warning(
            "Reached max_pages=%d fetching events for media_id=%s; there may be more pages",
            max_pages,
            media_id,
        )

    def _get(self, path, params=None):
        url = f"{self._base_url}/{path.lstrip('/')}"
        attempt = 0
        while True:
            attempt += 1
            try:
                response = self._session.get(url, params=params, timeout=self._timeout)
            except requests.RequestException as exc:
                if attempt > self._max_retries:
                    raise WistiaAPIError(f"Request to {url} failed after {attempt} attempts: {exc}") from exc
                self._sleep_before_retry(attempt)
                continue

            if response.status_code == 200:
                return response.json()
            if response.status_code == 401:
                raise WistiaAPIError(f"Unauthorized calling {url}: check the Wistia API token permissions")
            if response.status_code == 404:
                raise WistiaAPIError(f"Not found calling {url} with params={params}")
            if response.status_code in RETRYABLE_STATUS_CODES:
                if attempt > self._max_retries:
                    raise WistiaAPIError(
                        f"{url} failed with status {response.status_code} after {attempt} attempts: {response.text}"
                    )
                self._sleep_before_retry(attempt, retry_after=response.headers.get("Retry-After"))
                continue
            raise WistiaAPIError(f"Unexpected status {response.status_code} calling {url}: {response.text}")

    def _sleep_before_retry(self, attempt, retry_after=None):
        delay = float(retry_after) if retry_after else self._backoff_factor**attempt
        logger.warning("Retrying request in %.1fs (attempt %d/%d)", delay, attempt, self._max_retries)
        time.sleep(delay)


# --- wistia_pipeline.checkpoint -----------------------------------------------


class CheckpointStore:
    def __init__(self, bucket, key, s3_client=None):
        self._bucket = bucket
        self._key = key
        self._s3 = s3_client or boto3.client("s3")

    def load(self):
        try:
            obj = self._s3.get_object(Bucket=self._bucket, Key=self._key)
            return json.loads(obj["Body"].read())
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code in ("NoSuchKey", "404"):
                logger.info("No checkpoint found at s3://%s/%s, starting from scratch", self._bucket, self._key)
                return {}
            raise

    def save(self, checkpoint):
        self._s3.put_object(
            Bucket=self._bucket,
            Key=self._key,
            Body=json.dumps(checkpoint, indent=2, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info("Saved checkpoint to s3://%s/%s", self._bucket, self._key)


# --- wistia_pipeline.s3_writer -------------------------------------------------


class RawJsonWriter:
    def __init__(self, bucket, prefix, s3_client=None):
        self._bucket = bucket
        self._prefix = prefix.strip("/")
        self._s3 = s3_client or boto3.client("s3")

    def write(self, dataset, load_date, file_name, payload):
        key = f"{self._prefix}/{dataset}/load_date={load_date}/{file_name}"
        body = json.dumps(payload, default=str).encode("utf-8")
        self._s3.put_object(Bucket=self._bucket, Key=key, Body=body, ContentType="application/json")
        logger.info("Wrote %d bytes to s3://%s/%s", len(body), self._bucket, key)
        return key


# --- job entrypoint (was glue_jobs/ingestion_job.py) ---------------------------


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Wistia Stats API ingestion job")
    parser.add_argument("--media-ids", default="")
    parser.add_argument("--raw-bucket", default=None)
    parser.add_argument("--raw-prefix", default="wistia-video-analytics/raw")
    parser.add_argument("--checkpoint-bucket", default=None)
    parser.add_argument("--checkpoint-key", default="wistia-video-analytics/raw/_checkpoint/checkpoint.json")
    parser.add_argument("--secret-name", default="wistia/api-token")
    parser.add_argument("--aws-region", default=None)
    parser.add_argument("--events-per-page", type=int, default=100)
    parser.add_argument("--max-event-pages", type=int, default=50)
    parser.add_argument("--load-date", default=None, help="Override load_date partition, defaults to today (UTC)")
    # Glue's Python Shell runner always injects its own arguments (--JOB_NAME,
    # --TempDir, --enable-glue-datacatalog, --library-set, ...) whether or not
    # this job uses them, so unknown args are ignored rather than treated as
    # a parse error.
    args, unknown = parser.parse_known_args(argv)
    if unknown:
        logger.info("Ignoring extra arguments injected by the Glue job runner: %s", unknown)

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
