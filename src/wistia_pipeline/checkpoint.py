"""Reads/writes the ingestion checkpoint.json that tracks incremental state per media."""
import json
import logging

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


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
