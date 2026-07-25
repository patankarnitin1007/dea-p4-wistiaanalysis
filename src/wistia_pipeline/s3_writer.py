"""Writes raw Wistia API responses to the S3 raw zone, partitioned by load_date."""
import json
import logging

import boto3

logger = logging.getLogger(__name__)


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
