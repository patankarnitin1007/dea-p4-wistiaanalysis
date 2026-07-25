import pytest
from ingestion_job import ingest_media, parse_args, run

from wistia_pipeline.wistia_client import WistiaAPIError


class FakeClient:
    def __init__(self, media, stats, events):
        self._media = media
        self._stats = stats
        self._events = events
        self.calls = []

    def get_media(self, media_id):
        self.calls.append(("get_media", media_id))
        return self._media[media_id]

    def get_media_stats(self, media_id):
        self.calls.append(("get_media_stats", media_id))
        return self._stats[media_id]

    def iter_media_events(self, media_id, per_page, max_pages):
        self.calls.append(("iter_media_events", media_id))
        return iter(self._events.get(media_id, []))


class FailingClient(FakeClient):
    def get_media(self, media_id):
        raise WistiaAPIError("boom")


class FakeWriter:
    def __init__(self):
        self.written = []

    def write(self, dataset, load_date, file_name, payload):
        self.written.append((dataset, load_date, file_name, payload))
        return f"{dataset}/{file_name}"


def test_ingest_media_writes_snapshot_and_new_events_only():
    client = FakeClient(
        media={"abc123": {"hashed_id": "abc123", "name": "Demo"}},
        stats={"abc123": {"play_count": 10}},
        events={
            "abc123": [
                {"event_key": "old", "received_at": "2026-07-01T00:00:00Z"},
                {"event_key": "new", "received_at": "2026-07-23T00:00:00Z"},
            ]
        },
    )
    writer = FakeWriter()
    checkpoint = {"abc123": {"last_event_received_at": "2026-07-10T00:00:00Z"}}

    ingest_media("abc123", client, writer, checkpoint, "2026-07-24", events_per_page=100, max_event_pages=10)

    media_writes = [w for w in writer.written if w[0] == "media_stats"]
    event_writes = [w for w in writer.written if w[0] == "visitor_stats"]
    assert media_writes[0][3] == {"media": {"hashed_id": "abc123", "name": "Demo"}, "stats": {"play_count": 10}}
    assert len(event_writes) == 1
    assert [e["event_key"] for e in event_writes[0][3]] == ["new"]
    assert checkpoint["abc123"]["last_event_received_at"] == "2026-07-23T00:00:00Z"


def test_ingest_media_skips_write_when_no_new_events():
    client = FakeClient(
        media={"abc123": {}},
        stats={"abc123": {}},
        events={"abc123": [{"event_key": "old", "received_at": "2026-07-01T00:00:00Z"}]},
    )
    writer = FakeWriter()
    checkpoint = {"abc123": {"last_event_received_at": "2026-07-10T00:00:00Z"}}

    ingest_media("abc123", client, writer, checkpoint, "2026-07-24", events_per_page=100, max_event_pages=10)

    assert not any(w[0] == "visitor_stats" for w in writer.written)


def test_run_raises_when_any_media_fails(monkeypatch):
    args = parse_args(
        [
            "--media-ids",
            "abc123",
            "--raw-bucket",
            "raw-bucket",
            "--checkpoint-bucket",
            "checkpoint-bucket",
        ]
    )
    monkeypatch.setattr("ingestion_job.get_wistia_api_token", lambda **kwargs: "fake-token")
    monkeypatch.setattr("ingestion_job.WistiaClient", lambda token: FailingClient({}, {}, {}))
    monkeypatch.setattr("ingestion_job.RawJsonWriter", lambda bucket, prefix: FakeWriter())

    class FakeCheckpointStore:
        def __init__(self, bucket, key):
            self.saved = None

        def load(self):
            return {}

        def save(self, checkpoint):
            self.saved = checkpoint

    monkeypatch.setattr("ingestion_job.CheckpointStore", FakeCheckpointStore)

    with pytest.raises(RuntimeError, match="abc123"):
        run(args)


def test_parse_args_errors_on_missing_required_values():
    with pytest.raises(SystemExit):
        parse_args(["--media-ids", "", "--raw-bucket", "", "--checkpoint-bucket", ""])
