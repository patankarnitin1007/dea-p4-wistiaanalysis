import boto3
from moto import mock_aws

from wistia_pipeline.checkpoint import CheckpointStore

BUCKET = "test-checkpoint-bucket"
KEY = "wistia/checkpoint.json"


def _make_store():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    return CheckpointStore(BUCKET, KEY, s3_client=s3), s3


@mock_aws
def test_load_returns_empty_dict_when_missing():
    store, _ = _make_store()

    assert store.load() == {}


@mock_aws
def test_save_then_load_round_trips():
    store, _ = _make_store()

    store.save({"abc123": {"last_event_received_at": "2026-07-20T00:00:00Z"}})
    result = store.load()

    assert result == {"abc123": {"last_event_received_at": "2026-07-20T00:00:00Z"}}
