"""Athena query helpers for the Wistia analytics Streamlit dashboard.

Reads the FR10 gold model (dim_media, dim_visitor, fact_media_engagement)
via Athena rather than Spark/Delta directly - see infra/aws/athena_ddl.sql
for how those tables are registered in the Glue Data Catalog.
"""
import os

import awswrangler as wr
import pandas as pd
import streamlit as st

DATABASE = os.environ.get("WISTIA_DASHBOARD_DATABASE", "wistia_video_analytics")
WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
S3_OUTPUT = os.environ.get("ATHENA_S3_OUTPUT")  # optional; falls back to the workgroup's configured location


@st.cache_data(ttl=300, show_spinner="Querying Athena...")
def run_query(sql: str) -> pd.DataFrame:
    return wr.athena.read_sql_query(sql, database=DATABASE, workgroup=WORKGROUP, s3_output=S3_OUTPUT)


def kpi_summary() -> pd.DataFrame:
    return run_query(
        """
        SELECT
            SUM(play_count) AS total_plays,
            ROUND(SUM(total_watch_time) / 3600.0, 2) AS total_watch_hours,
            COUNT(DISTINCT visitor_id) AS unique_visitors,
            ROUND(AVG(watched_percent) * 100, 2) AS avg_watched_percent
        FROM fact_media_engagement
        """
    )


def engagement_by_media() -> pd.DataFrame:
    return run_query(
        """
        SELECT
            d.title,
            d.channel,
            SUM(f.play_count) AS plays,
            ROUND(SUM(f.total_watch_time) / 3600.0, 2) AS watch_hours
        FROM fact_media_engagement f
        JOIN dim_media d ON f.media_id = d.media_id
        GROUP BY d.title, d.channel
        ORDER BY plays DESC
        """
    )


def daily_plays_trend() -> pd.DataFrame:
    return run_query(
        """
        SELECT date, SUM(play_count) AS plays
        FROM fact_media_engagement
        GROUP BY date
        ORDER BY date
        """
    )


def visitor_geography() -> pd.DataFrame:
    return run_query(
        """
        SELECT country, COUNT(*) AS visitors
        FROM dim_visitor
        WHERE country IS NOT NULL
        GROUP BY country
        ORDER BY visitors DESC
        """
    )
