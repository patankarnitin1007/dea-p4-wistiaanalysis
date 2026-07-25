# FR8: 7-consecutive-day production run log

Evidence for the requirement doc's FR8 (pipeline running in production for 7
consecutive days) and FR12 (final submission). Fill in one row per day from
the Glue console: **Workflows → wistia-video-analytics-workflow → History**
tab shows each run's start time and the status of every job in it.

| # | Date (UTC) | Ingestion job | Transformation job | Notes |
|---|------------|----------------|---------------------|-------|
| 1 |            |                |                     |       |
| 2 |            |                |                     |       |
| 3 |            |                |                     |       |
| 4 |            |                |                     |       |
| 5 |            |                |                     |       |
| 6 |            |                |                     |       |
| 7 |            |                |                     |       |

A screenshot of the Workflow History tab showing 7 consecutive `SUCCEEDED`
rows (or documented failures + same-day fixes, still counted as the pipeline
running in production) is the strongest evidence to attach to the final
submission alongside this table.
