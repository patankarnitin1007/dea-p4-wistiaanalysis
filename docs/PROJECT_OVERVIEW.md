# Project Overview — Wistia Video Analytics Pipeline

A reference document describing what this project is, how it's built, and
why it's built that way. For step-by-step deployment instructions, see
[`RUNBOOK.md`](RUNBOOK.md) instead — this document is the "what and why,"
the runbook is the "how."

## 1. Purpose

An end-to-end AWS data engineering pipeline that ingests Wistia media and
visitor engagement statistics from the Wistia Stats API, lands them in an
S3 data lake, transforms them into a Bronze/Silver/Gold Delta Lake model,
makes them queryable via Athena, and visualizes them in a Streamlit
dashboard — running in production on a daily schedule with failure
alerting.

## 2. Technical constraints (from the requirement doc)

- No DBT.
- AWS services only (no non-AWS/Azure tooling in the runtime path).
- Python for API ingestion, PySpark for transformation.
- GitHub for version control and CI/CD.
- The pipeline must run in production for 7 consecutive days before final
  submission.

## 3. Architecture

```
Wistia Stats API
      |  HTTPS
      v
[1] Ingestion Job (Glue Python Shell)
      |  raw JSON
      v
[2] Raw Zone (S3, load_date= partitions)
      |
      v
[3] Transformation Job (Glue PySpark)
      |  Bronze / Silver / Gold (Delta)
      v
[4] Curated Zone (S3 Delta Lake)
      |
      v
[5] Glue Data Catalog + Athena
      |
      v
[6] Streamlit Dashboard
```

Cross-cutting:
- **Secrets:** AWS Secrets Manager holds the Wistia API token.
- **Orchestration:** an AWS Glue Workflow runs ingestion daily, then
  transformation once ingestion succeeds.
- **Monitoring:** Glue job failures emit EventBridge events, routed to an
  SNS topic that emails an alert.
- **CI/CD:** GitHub Actions lints and tests every push/PR, and builds +
  pushes a Docker image to ECR via OIDC federation (no long-lived AWS
  keys in GitHub).

All six stages are implemented in this repo.

## 4. Data model (FR10)

Star schema in the gold layer:

| Table | Columns | Grain |
|---|---|---|
| `dim_media` | media_id, title, url, channel, created_at | one row per media_id (latest snapshot) |
| `dim_visitor` | visitor_id, ip_address, country | one row per visitor_id (latest known location) |
| `fact_media_engagement` | media_id, visitor_id, date, play_count, play_rate, total_watch_time, watched_percent | one row per media/visitor/day |

## 5. Repo layout

```
config/pipeline_config.yaml   non-secret defaults (bucket names, media ids, channel mapping, ...)
src/wistia_pipeline/
  wistia_client.py            Wistia Data/Stats API client (pagination, retries)
  checkpoint.py                S3-backed checkpoint.json read/write
  s3_writer.py                 writes raw JSON to the S3 raw zone
  secrets.py                   resolves the Wistia API token (Secrets Manager or env var)
  config.py                    loads config/pipeline_config.yaml (by section)
  transform.py                 PySpark bronze/silver/gold DataFrame logic
glue_jobs/
  ingestion_job.py                    Glue Python Shell job entrypoint (stage 1)
  ingestion_job_standalone.py         single-file deployment copy for the Glue Script tab
  transformation_job.py               Glue PySpark job entrypoint (stage 3)
  transformation_job_standalone.py    single-file deployment copy for the Glue Script tab
dashboard/
  app.py                       Streamlit app (architecture stage 6)
  queries.py                   Athena query helpers used by the dashboard
tests/                         pytest unit tests (moto for AWS, responses for HTTP, local Spark for transform)
infra/aws/                     IAM policies, EventBridge pattern, Athena DDL - see RUNBOOK.md
docs/
  PROJECT_OVERVIEW.md           this document
  RUNBOOK.md                    step-by-step deployment/operational instructions
  production-run-log.md         FR8 7-consecutive-day run tracking template
requirements.txt               core ingestion deps
requirements-dev.txt           dev/test deps (pytest, ruff, pyspark, delta-spark, dashboard deps)
requirements-dashboard.txt     dashboard-only deps
```

## 6. Design decisions & assumptions

- **Incremental ingestion (FR7):** media metadata + stats are re-fetched in
  full every run (small, single-object responses). Visitor events
  (`stats/events.json`) are paginated in full each run, up to a
  `max_event_pages` safety cap (default 50 pages), and filtered
  client-side by comparing `received_at` against a per-media checkpoint.
  The Wistia events endpoint's sort order isn't a documented guarantee, so
  the job inspects every event returned each run rather than stopping at
  the first "old" event. Hitting the page cap logs a warning.
- **Checkpoint granularity:** one `checkpoint.json` in S3 keyed by
  `media_id`, holding `last_event_received_at` and `last_ingested_at`. A
  partial run (one media fails) still saves progress for the media that
  succeeded; the job exits non-zero if any media failed so Glue/CloudWatch
  can alert.
- **Retries:** the Wistia client retries on network errors, `429`, and
  `5xx` with exponential backoff (honoring `Retry-After` when present);
  `401`/`404` fail fast since retrying won't help.
- **No native "channel" field:** Wistia's media object exposes `project`
  and `share_link`, but nothing identifying a distribution channel (e.g.
  YouTube/Facebook). `dim_media.channel` is resolved from a
  `media_id -> channel` mapping in `config/pipeline_config.yaml`
  (`transformation.channel_mapping`), defaulting to `"Unknown"` for any
  media_id not listed.
- **`fact_media_engagement` grain and metrics** (one row per
  media_id/visitor_id/date):
  - `play_count` = number of visitor events for that media/visitor/day.
  - `watched_percent` = furthest point reached that day
    (`max(percent_viewed)`).
  - `total_watch_time` = `sum(percent_viewed * media.duration)` — an
    approximation of seconds watched, since Wistia's events API reports
    `percent_viewed` per event, not raw watch seconds.
  - `play_rate` = the media-level `stats.play_rate` from the latest
    ingested snapshot, denormalized onto every row for that media_id —
    Wistia doesn't expose play_rate broken out by visitor or day.
- **Latest-snapshot dimensions:** `dim_media` and `dim_visitor` take the
  most recent snapshot per key rather than tracking full history (no
  SCD2) — the requirement doc's model is a plain star schema.
- **Idempotent writes:** each transformation run overwrites the
  bronze/silver/gold Delta tables from the full raw zone rather than
  appending, so reruns are safe with no double-counting. Trades
  incremental-transform efficiency for simplicity.
- **Dashboard reads via Athena, not Spark/Delta directly:** keeps the
  dashboard dependency-light (no Spark) and automatically in sync with
  whatever the Athena DDL registered. `awswrangler` is called with
  `ctas_approach=False` deliberately — the default stages results through
  a temporary Glue table needing `glue:CreateTable`/`DeleteTable`, more
  write access than a read-only dashboard should have.

## 7. Notable issues hit during deployment (and their fixes)

Kept here as a troubleshooting reference — several of these are easy to
hit again if the pattern is repeated elsewhere in the project.

| Symptom | Root cause | Fix |
|---|---|---|
| Glue job "succeeded" but did nothing | Glue Studio's Script tab silently diverged from the intended script | Re-paste/verify the Script tab content directly, don't trust the Script path setting alone |
| `ModuleNotFoundError: No module named 'wistia_pipeline'` | Python library zip had `src/wistia_pipeline/...` instead of `wistia_pipeline/...` at zip root; Glue's `--extra-py-files`/library-path resolution is picky | Switched to standalone single-file scripts (`*_standalone.py`) with all logic inlined - no zip packaging needed |
| `unrecognized arguments: --enable-glue-datacatalog ...` | Glue's job runner auto-injects its own CLI args | `parser.parse_known_args()` instead of `parse_args()`, logging ignored args |
| `Invalid bucket name` | A placeholder or an `s3://...` URI (with prefix) was pasted into a bare-bucket-name job parameter | Bucket params take bare names only; prefix/key params take paths |
| GitHub Actions `Not authorized to perform sts:AssumeRoleWithWebIdentity` (persisted across secret re-saves) | GitHub now appends the owner/repo numeric IDs into the OIDC `sub` claim (e.g. `repo:owner@291383738/repo@1311362993:ref:...`) as anti-subject-reuse hardening; the trust policy's `StringLike` prefix match (`repo:owner/repo:*`) no longer matches because of the inserted `@<id>` segments - found via CloudTrail's `AssumeRoleWithWebIdentity` failure event, which shows the exact `sub` claim presented | Added a second `StringLike` pattern to `infra/aws/github-oidc-trust-policy.json` matching the ID-suffixed format |
| IAM policy editor `Wildcard Usage Too Permissive` on the `sub` condition | Access Analyzer requires 6+ literal characters immediately before any wildcard; a trailing bare `:*` (only preceded by `:`) trips it | Made the pattern explicit about matching branch refs: `...:ref:refs/heads/*` instead of a bare trailing `:*` |
| `DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG` | `--datalake-formats delta` provisions the Delta JARs but does **not** set the required Spark SQL extension/catalog config | Added a `--conf` job parameter setting both `spark.sql.extensions` and `spark.sql.catalog.spark_catalog` |
| Dashboard `AccessDeniedException` on `glue:DeleteTable` | `awswrangler`'s default `ctas_approach=True` stages Athena results through a temporary Glue table it then drops | Pass `ctas_approach=False` |
| Dashboard `AccessDeniedException` on `glue:GetDatabase` | The IAM user used for local dashboard credentials only had S3 permissions | Attached `infra/aws/dashboard-athena-glue-read-policy.json` |
| CI/CD `docker push` → `denied: ... ecr:InitiateLayerUpload ... not authorized` (after OIDC was fixed) | The ECR permissions policy's `Resource` ARN referenced `wistia-video-analytics-ingestion` (the originally suggested repo name) but the actual ECR repo created was `xxdea-p4-wistia-repo` | Updated `infra/aws/ecr-push-permissions-policy.json`'s `Resource` ARN to the actual repo name |

## 8. Testing

```bash
pip install -r requirements-dev.txt
pytest -q
ruff check .
```

27 tests total: `moto` mocks AWS (S3, Secrets Manager) for the ingestion
job, `responses` mocks the Wistia HTTP API, a local Spark+Delta session
tests the bronze/silver/gold transformation logic, and the dashboard's
Athena query layer is tested with a mocked `awswrangler` call. No network
calls or real AWS credentials are needed to run the suite.

## 9. Requirement traceability

| FR | Status |
|----|--------|
| FR2 Authenticate to Wistia Stats API | Done — bearer token via `WistiaClient` |
| FR3 Extract media metadata | Done — `get_media` |
| FR4 Extract engagement metrics | Done — `get_media_stats` |
| FR5 Extract visitor-level data | Done — `iter_media_events` |
| FR6 Pagination | Done — `iter_media_events` pages until a short page |
| FR7 Incremental ingestion | Done — checkpoint-based |
| FR8 7-day production run | Instrumented (Glue Workflow + SNS/EventBridge); pending 7 consecutive daily runs, see `production-run-log.md` |
| FR9 CI/CD | Done — `.github/workflows/ci.yml`; lint/test and build/push to ECR both confirmed green end-to-end |
| FR10 Transform to dim/fact model | Done — `glue_jobs/transformation_job.py` |
| FR11 Data Catalog + SQL querying | Done — `infra/aws/athena_ddl.sql` |
| Dashboard/visualization | Done — `dashboard/app.py`; verify against live data |
| FR1, FR12 | Pending — architecture doc alignment, final submission writeup |
