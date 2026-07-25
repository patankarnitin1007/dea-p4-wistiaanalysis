import json

import boto3
from moto import mock_aws

from wistia_pipeline.s3_writer import RawJsonWriter

BUCKET = "test-raw-bucket"


@mock_aws
def test_write_puts_object_at_partitioned_key():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    writer = RawJsonWriter(BUCKET, "wistia/raw", s3_client=s3)

    key = writer.write("media_stats", "2026-07-24", "abc123.json", {"hashed_id": "abc123"})

    assert key == "wistia/raw/media_stats/load_date=2026-07-24/abc123.json"
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    assert json.loads(body) == {"hashed_id": "abc123"}
