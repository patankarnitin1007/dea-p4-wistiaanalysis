import json
from functools import reduce

from pyspark.sql import functions as F

from wistia_pipeline.transform import (
    MEDIA_STATS_SCHEMA,
    VISITOR_EVENTS_SCHEMA,
    build_dim_media,
    build_dim_visitor,
    build_fact_media_engagement,
    build_silver_media_stats,
    build_silver_visitor_events,
)

MEDIA_ID = "8hunphufxp"


def _media_record(load_date, updated, duration=988.694, play_rate=0.0116, name="rivas testimonial"):
    return {
        "media": {
            "id": 120331489,
            "hashed_id": MEDIA_ID,
            "type": "Video",
            "archived": False,
            "name": name,
            "duration": duration,
            "created": "2024-06-10T04:56:20+00:00",
            "updated": updated,
            "status": "ready",
            "tags": [],
            "project": {"id": 9180604, "hashed_id": "5vs6grcrlq", "name": "Testimonials"},
            "share_link": {
                "hashed_id": "hoyiah8g8mdcbb0",
                "url": "https://chrisgarzon19.wistia.com/s/hoyiah8g8mdcbb0",
                "visibility": "unlocked",
            },
        },
        "stats": {
            "load_count": 98421,
            "play_count": 849,
            "play_rate": play_rate,
            "hours_watched": 35.396546995049995,
            "engagement": 0.1518,
            "visitors": 71164,
        },
    }, load_date


def _media_stats_df(spark, records_with_load_date):
    dfs = []
    for record, load_date in records_with_load_date:
        rdd = spark.sparkContext.parallelize([json.dumps(record)])
        df = spark.read.schema(MEDIA_STATS_SCHEMA).json(rdd).withColumn("load_date", F.lit(load_date))
        dfs.append(df)
    return reduce(lambda a, b: a.unionByName(b), dfs)


def _event(event_key, received_at, visitor_key, percent_viewed, media_id=MEDIA_ID, ip="209.236.157.13", country="US"):
    return {
        "received_at": received_at,
        "event_key": event_key,
        "ip": ip,
        "country": country,
        "region": "Mississippi",
        "city": "Olive Branch",
        "lat": 34.9618,
        "lon": -89.8295,
        "org": "Northcentral Electric Cooperative",
        "percent_viewed": percent_viewed,
        "visitor_key": visitor_key,
        "user_agent_details": {"browser": "Instagram", "browser_version": "439", "platform": "Android", "mobile": True},
        "media_id": media_id,
        "media_name": "rivas testimonial",
        "media_url": "https://chrisgarzon19.wistia.com/medias/8hunphufxp",
        "thumbnail": {"url": "http://embed.wistia.com/deliveries/f08449e4279efca48214319343fda674.bin", "width": 1708, "height": 948},
    }


def _events_df(spark, events, load_date):
    rdd = spark.sparkContext.parallelize([json.dumps(events)])
    return (
        spark.read.schema(VISITOR_EVENTS_SCHEMA)
        .option("multiLine", True)
        .json(rdd)
        .withColumn("load_date", F.lit(load_date))
    )


def test_build_silver_media_stats_flattens_and_types(spark):
    record, load_date = _media_record("2026-07-24", "2026-07-24T10:00:00+00:00")
    bronze = _media_stats_df(spark, [(record, load_date)])

    silver = build_silver_media_stats(bronze).collect()

    assert len(silver) == 1
    row = silver[0]
    assert row.media_id == MEDIA_ID
    assert row.title == "rivas testimonial"
    assert row.url == "https://chrisgarzon19.wistia.com/s/hoyiah8g8mdcbb0"
    assert row.duration_seconds == 988.694
    assert row.play_count == 849
    assert row.load_date == "2026-07-24"


def test_build_dim_media_picks_latest_snapshot_and_applies_channel_mapping(spark):
    older, older_load_date = _media_record("2026-07-23", "2026-07-23T10:00:00+00:00", name="old title")
    newer, newer_load_date = _media_record("2026-07-24", "2026-07-24T22:50:14+00:00", name="new title")
    bronze = _media_stats_df(spark, [(older, older_load_date), (newer, newer_load_date)])
    silver = build_silver_media_stats(bronze)

    dim_media = build_dim_media(silver, channel_mapping={MEDIA_ID: "YouTube"}, default_channel="Unknown").collect()

    assert len(dim_media) == 1
    assert dim_media[0].title == "new title"
    assert dim_media[0].channel == "YouTube"


def test_build_dim_media_defaults_unmapped_media_to_default_channel(spark):
    record, load_date = _media_record("2026-07-24", "2026-07-24T22:50:14+00:00")
    bronze = _media_stats_df(spark, [(record, load_date)])
    silver = build_silver_media_stats(bronze)

    dim_media = build_dim_media(silver, channel_mapping={}, default_channel="Unknown").collect()

    assert dim_media[0].channel == "Unknown"


def test_build_silver_visitor_events_dedups_by_event_key(spark):
    events = [
        _event("evt-1", "2026-07-24T01:13:57.000Z", "visitor-a", 0.5),
        _event("evt-1", "2026-07-24T01:13:57.000Z", "visitor-a", 0.5),  # duplicate, e.g. re-ingested
        _event("evt-2", "2026-07-24T02:00:00.000Z", "visitor-a", 0.2),
    ]
    bronze = _events_df(spark, events, "2026-07-24")

    silver = build_silver_visitor_events(bronze)

    assert silver.count() == 2


def test_build_dim_visitor_uses_latest_ip_and_country(spark):
    events = [
        _event("evt-1", "2026-07-24T01:00:00.000Z", "visitor-a", 0.1, ip="1.1.1.1", country="US"),
        _event("evt-2", "2026-07-24T05:00:00.000Z", "visitor-a", 0.2, ip="2.2.2.2", country="CA"),
    ]
    bronze = _events_df(spark, events, "2026-07-24")
    silver = build_silver_visitor_events(bronze)

    dim_visitor = build_dim_visitor(silver).collect()

    assert len(dim_visitor) == 1
    assert dim_visitor[0].visitor_id == "visitor-a"
    assert dim_visitor[0].ip_address == "2.2.2.2"
    assert dim_visitor[0].country == "CA"


def test_build_fact_media_engagement_grain_and_aggregation(spark):
    media_record, media_load_date = _media_record("2026-07-24", "2026-07-24T22:50:14+00:00", duration=1000.0, play_rate=0.05)
    media_bronze = _media_stats_df(spark, [(media_record, media_load_date)])
    silver_media_stats = build_silver_media_stats(media_bronze)
    dim_media = build_dim_media(silver_media_stats, channel_mapping={}, default_channel="Unknown")

    events = [
        _event("evt-1", "2026-07-24T01:00:00.000Z", "visitor-a", 0.10),
        _event("evt-2", "2026-07-24T02:00:00.000Z", "visitor-a", 0.30),  # same visitor/media/day -> aggregated
        _event("evt-3", "2026-07-25T01:00:00.000Z", "visitor-a", 0.50),  # different day -> separate row
    ]
    events_bronze = _events_df(spark, events, "2026-07-24")
    silver_events = build_silver_visitor_events(events_bronze)

    fact = build_fact_media_engagement(silver_events, dim_media, silver_media_stats)
    rows = {(r.date.isoformat(), r.visitor_id): r for r in fact.collect()}

    day1 = rows[("2026-07-24", "visitor-a")]
    assert day1.play_count == 2
    assert day1.watched_percent == 0.30
    assert round(day1.total_watch_time, 3) == round((0.10 + 0.30) * 1000.0, 3)
    assert round(day1.play_rate, 4) == 0.05

    day2 = rows[("2026-07-25", "visitor-a")]
    assert day2.play_count == 1
    assert day2.watched_percent == 0.50
