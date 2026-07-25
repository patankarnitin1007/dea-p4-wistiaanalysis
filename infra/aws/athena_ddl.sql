-- Registers the gold Delta tables (FR10 dimensional model) in the Glue Data
-- Catalog so they're queryable from Athena. Athena engine v3 has native
-- Delta Lake support - TBLPROPERTIES ('table_type'='DELTA') is enough to
-- read schema straight from the table's _delta_log, no crawler needed.
--
-- Run these in the Athena query editor (Data source: AwsDataCatalog).
-- Replace the bucket/prefix below if yours differs from
-- config/pipeline_config.yaml's transformation.curated_bucket/curated_prefix.

CREATE DATABASE IF NOT EXISTS wistia_video_analytics
COMMENT 'Wistia video analytics curated gold layer (FR10 dimensional model)';

CREATE EXTERNAL TABLE wistia_video_analytics.dim_media
LOCATION 's3://xxdea-p4-wistia-bucket/wistia-video-analytics/curated/gold/dim_media/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE wistia_video_analytics.dim_visitor
LOCATION 's3://xxdea-p4-wistia-bucket/wistia-video-analytics/curated/gold/dim_visitor/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE wistia_video_analytics.fact_media_engagement
LOCATION 's3://xxdea-p4-wistia-bucket/wistia-video-analytics/curated/gold/fact_media_engagement/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- Sanity checks -----------------------------------------------------------

SELECT * FROM wistia_video_analytics.dim_media LIMIT 10;
SELECT * FROM wistia_video_analytics.dim_visitor LIMIT 10;
SELECT * FROM wistia_video_analytics.fact_media_engagement LIMIT 10;

-- Example analytical query: plays and watch time per media/channel
SELECT
    d.title,
    d.channel,
    COUNT(*) AS engagement_rows,
    SUM(f.play_count) AS total_plays,
    ROUND(SUM(f.total_watch_time) / 3600.0, 2) AS total_watch_hours
FROM wistia_video_analytics.fact_media_engagement f
JOIN wistia_video_analytics.dim_media d ON f.media_id = d.media_id
GROUP BY d.title, d.channel
ORDER BY total_plays DESC;
