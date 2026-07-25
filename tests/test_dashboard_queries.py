import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dashboard"))

import queries


@pytest.fixture(autouse=True)
def _clear_query_cache():
    queries.run_query.clear()
    yield
    queries.run_query.clear()


@patch("queries.wr.athena.read_sql_query")
def test_kpi_summary_queries_fact_table(mock_read_sql_query):
    mock_read_sql_query.return_value = pd.DataFrame([{"total_plays": 10}])

    result = queries.kpi_summary()

    assert mock_read_sql_query.call_args.kwargs["database"] == queries.DATABASE
    sql = mock_read_sql_query.call_args.args[0]
    assert "fact_media_engagement" in sql
    assert result.iloc[0]["total_plays"] == 10


@patch("queries.wr.athena.read_sql_query")
def test_engagement_by_media_joins_dim_media(mock_read_sql_query):
    mock_read_sql_query.return_value = pd.DataFrame([{"title": "x", "plays": 1}])

    queries.engagement_by_media()

    sql = mock_read_sql_query.call_args.args[0]
    assert "fact_media_engagement" in sql
    assert "dim_media" in sql
    assert "JOIN dim_media d ON f.media_id = d.media_id" in sql


@patch("queries.wr.athena.read_sql_query")
def test_visitor_geography_queries_dim_visitor(mock_read_sql_query):
    mock_read_sql_query.return_value = pd.DataFrame([{"country": "US", "visitors": 5}])

    result = queries.visitor_geography()

    sql = mock_read_sql_query.call_args.args[0]
    assert "dim_visitor" in sql
    assert result.iloc[0]["country"] == "US"
