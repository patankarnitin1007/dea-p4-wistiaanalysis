"""Streamlit dashboard for Wistia video analytics (architecture stage 6).

Run locally:
    pip install -r requirements-dashboard.txt
    export AWS_PROFILE=your-profile   # or AWS_REGION + default credentials
    streamlit run dashboard/app.py
"""
import plotly.express as px
import streamlit as st
from queries import (
    daily_plays_trend,
    engagement_by_media,
    kpi_summary,
    visitor_geography,
)

st.set_page_config(page_title="Wistia Video Analytics", layout="wide")
st.title("Wistia Video Analytics")

kpis = kpi_summary().iloc[0]
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total plays", f"{int(kpis['total_plays'] or 0):,}")
col2.metric("Total watch hours", f"{kpis['total_watch_hours'] or 0:,.1f}")
col3.metric("Unique visitors", f"{int(kpis['unique_visitors'] or 0):,}")
col4.metric("Avg. watched", f"{kpis['avg_watched_percent'] or 0:.1f}%")

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("Plays by media")
    media_df = engagement_by_media()
    st.plotly_chart(
        px.bar(media_df, x="title", y="plays", color="channel", labels={"title": "Media", "plays": "Plays"}),
        use_container_width=True,
    )

with right:
    st.subheader("Daily plays trend")
    trend_df = daily_plays_trend()
    st.plotly_chart(px.line(trend_df, x="date", y="plays", markers=True), use_container_width=True)

st.subheader("Visitors by country")
geo_df = visitor_geography()
st.plotly_chart(px.bar(geo_df.head(20), x="country", y="visitors"), use_container_width=True)

st.subheader("Engagement detail (by media)")
st.dataframe(media_df, use_container_width=True)
