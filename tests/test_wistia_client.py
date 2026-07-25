import responses

from wistia_pipeline.wistia_client import WistiaAPIError, WistiaClient

BASE = "https://api.wistia.com/v1"


def make_client(**kwargs):
    return WistiaClient("fake-token", max_retries=2, backoff_factor=0.01, **kwargs)


@responses.activate
def test_get_media_returns_json():
    responses.add(responses.GET, f"{BASE}/medias/abc123.json", json={"hashed_id": "abc123"}, status=200)

    result = make_client().get_media("abc123")

    assert result == {"hashed_id": "abc123"}
    assert responses.calls[0].request.headers["Authorization"] == "Bearer fake-token"


@responses.activate
def test_get_media_stats_returns_json():
    responses.add(responses.GET, f"{BASE}/stats/medias/abc123.json", json={"play_count": 42}, status=200)

    result = make_client().get_media_stats("abc123")

    assert result == {"play_count": 42}


@responses.activate
def test_unauthorized_raises_wistia_api_error():
    responses.add(responses.GET, f"{BASE}/medias/abc123.json", json={"error": "unauthorized"}, status=401)

    try:
        make_client().get_media("abc123")
        assert False, "expected WistiaAPIError"
    except WistiaAPIError as exc:
        assert "Unauthorized" in str(exc)


@responses.activate
def test_retries_then_succeeds_on_500():
    responses.add(responses.GET, f"{BASE}/medias/abc123.json", status=500)
    responses.add(responses.GET, f"{BASE}/medias/abc123.json", json={"hashed_id": "abc123"}, status=200)

    result = make_client().get_media("abc123")

    assert result == {"hashed_id": "abc123"}
    assert len(responses.calls) == 2


@responses.activate
def test_exhausted_retries_raise():
    for _ in range(4):
        responses.add(responses.GET, f"{BASE}/medias/abc123.json", status=503)

    try:
        make_client().get_media("abc123")
        assert False, "expected WistiaAPIError"
    except WistiaAPIError as exc:
        assert "503" in str(exc)


@responses.activate
def test_iter_media_events_paginates_until_short_page():
    responses.add(
        responses.GET,
        f"{BASE}/stats/events.json",
        json=[{"event_key": "1"}, {"event_key": "2"}],
        status=200,
    )
    responses.add(
        responses.GET,
        f"{BASE}/stats/events.json",
        json=[{"event_key": "3"}],
        status=200,
    )

    events = list(make_client().iter_media_events("abc123", per_page=2, max_pages=10))

    assert [e["event_key"] for e in events] == ["1", "2", "3"]
    assert len(responses.calls) == 2


@responses.activate
def test_iter_media_events_stops_at_max_pages():
    full_page = [{"event_key": str(i)} for i in range(2)]
    responses.add(responses.GET, f"{BASE}/stats/events.json", json=full_page, status=200)
    responses.add(responses.GET, f"{BASE}/stats/events.json", json=full_page, status=200)

    events = list(make_client().iter_media_events("abc123", per_page=2, max_pages=2))

    assert len(events) == 4
    assert len(responses.calls) == 2
