# DEA Project 4 – Wistia Video Analytics Pipeline

End-to-end data engineering project that ingests Wistia media and visitor
engagement stats, stores them in an S3 data lake, and (per the target
architecture below) transforms them through a Bronze/Silver/Gold Delta Lake
model queryable via Athena.

## Architecture

```
Wistia Stats API
      |  HTTPS
      v
[1] Ingestion Job (Glue Python Shell)         <- implemented in this repo
      |  raw JSON
      v
[2] Raw Zone (S3, load_date= partitions)
      |
      v
[3] Transformation Job (Glue PySpark)         <- implemented in this repo
      |  Bronze / Silver / Gold (Delta)
      v
[4] Curated Zone (S3 Delta Lake)
      |
      v
[5] Glue Data Catalog + Athena                <- not yet implemented
      |
      v
[6] Streamlit Dashboard                       <- not yet implemented
```

Orchestration: AWS Glue Workflow, daily schedule, run for 7 consecutive days.
Secrets: AWS Secrets Manager. Monitoring: CloudWatch logs/metrics + SNS
alerts on job failure. CI/CD: GitHub Actions (lint + test on every push/PR).

This repo currently implements **stages [1] and [3]**, the ingestion and
transformation jobs. Stages 5–6 are future work tracked against the
requirement doc's FR list.

## Repo layout

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
  ingestion_job.py             Glue Python Shell job entrypoint (stage 1)
  transformation_job.py        Glue PySpark job entrypoint (stage 3)
tests/                        pytest unit tests (moto for AWS, responses for HTTP, local Spark for transform)
```

## Wistia API token handling

The requirement doc's API token must never be committed to source control.
The job resolves it in this order:

1. `WISTIA_API_TOKEN` environment variable — **local development only**.
2. An AWS Secrets Manager secret (`--secret-name`, default `wistia/api-token`
   from `config/pipeline_config.yaml`) — used in Glue. The secret can be a
   plain string or a `{"api_token": "..."}` JSON blob. The Glue job's IAM
   role needs `secretsmanager:GetSecretValue` on that secret.

## Running locally

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

export WISTIA_API_TOKEN=your-token-here   # never commit this
export AWS_PROFILE=your-aws-profile        # for S3 read/write

python glue_jobs/ingestion_job.py \
  --media-ids 8hunphufxp,9k4tbcdfg0 \
  --raw-bucket my-raw-bucket \
  --checkpoint-bucket my-raw-bucket
```

CLI arguments override `config/pipeline_config.yaml`; anything not passed
falls back to that file so the job can run with just `--raw-bucket` and
`--checkpoint-bucket` set once media ids etc. are configured there.

## Running as a Glue job

Upload `glue_jobs/ingestion_job.py` plus the `src/wistia_pipeline` package
as the Glue Python Shell script/library, and configure Job Parameters
matching the CLI flags above (e.g. `--raw-bucket`, `--secret-name`). Attach
an IAM role with `s3:GetObject`/`s3:PutObject` on the raw bucket and
`secretsmanager:GetSecretValue` on the token secret.

## Transformation job (Bronze / Silver / Gold)

`glue_jobs/transformation_job.py` (logic in `src/wistia_pipeline/transform.py`)
reads the raw zone written by the ingestion job and produces Delta tables in
a curated S3 zone:

- **Bronze** (`bronze/media_stats`, `bronze/visitor_events`): raw JSON parsed
  against an explicit schema, one row per source record, no cleaning.
- **Silver** (`silver/media_stats`, `silver/visitor_events`): typed/flattened,
  visitor events deduplicated by `event_key`, a `date` column added.
- **Gold**, matching the FR10 model:
  - `dim_media` (media_id, title, url, channel, created_at) — latest
    metadata snapshot per media_id.
  - `dim_visitor` (visitor_id, ip_address, country) — latest known
    ip/country per visitor_key.
  - `fact_media_engagement` (media_id, visitor_id, date, play_count,
    play_rate, total_watch_time, watched_percent), partitioned by `date`.
    Grain is one row per media/visitor/day; see assumptions below for how
    each metric is derived.

### Running locally

```bash
pip install -r requirements-dev.txt   # includes pyspark + delta-spark
python glue_jobs/transformation_job.py \
  --raw-bucket my-raw-bucket \
  --curated-bucket my-raw-bucket
```

### Running as a Glue job

Create a **Spark** job (not Python Shell), Glue version **5.0** (Spark 3.5 +
Delta Lake 3.x). Upload `glue_jobs/transformation_job.py` plus the
`src/wistia_pipeline` package the same way as the ingestion job. Under Job
details, set:

- `--datalake-formats` = `delta`
- Job parameters: `--raw-bucket`, `--curated-bucket` (required); optionally
  `--raw-prefix`, `--curated-prefix`, `--default-channel`, and
  `--channel-mapping-json` (a JSON string like `{"8hunphufxp":"YouTube"}`,
  overriding `config/pipeline_config.yaml`'s `transformation.channel_mapping`).

Attach an IAM role with `s3:GetObject` on the raw bucket and
`s3:GetObject`/`s3:PutObject`/`s3:DeleteObject` on the curated bucket.

## CI/CD: build & push to ECR

`.github/workflows/ci.yml` has two jobs: `lint-and-test` (always) and
`build-and-push` (only on a push, after tests pass), which builds the
`Dockerfile` image and pushes it to Amazon ECR using GitHub's OIDC
federation — no long-lived AWS keys stored in GitHub.

> Note: this containerized image is for running the ingestion job on a
> container platform (ECS/Fargate scheduled task, AWS Batch, or local Docker
> parity) — plain Glue Python Shell jobs run scripts from S3, not ECR
> images. If the target stays Glue Python Shell, this image is for local
> dev/testing parity; if you'd rather run ingestion on ECS Fargate on a
> schedule (still all-AWS, no Glue), say so and I'll wire that up instead
> of/alongside the Glue job.

One-time AWS setup:

1. Create the ECR repository:
   ```bash
   aws ecr create-repository --repository-name wistia-video-analytics-ingestion --region <REGION>
   ```
2. Create the GitHub OIDC identity provider (skip if your account already
   has one — one provider is shared across all repos):
   ```bash
   aws iam create-open-id-connect-provider \
     --url https://token.actions.githubusercontent.com \
     --client-id-list sts.amazonaws.com \
     --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
   ```
3. Create the IAM role GitHub Actions will assume, using
   `infra/aws/github-oidc-trust-policy.json` (fill in `<ACCOUNT_ID>`) as
   the trust policy and `infra/aws/ecr-push-permissions-policy.json` (fill
   in `<REGION>`/`<ACCOUNT_ID>`) as an inline/attached permissions policy:
   ```bash
   aws iam create-role --role-name github-actions-wistia-ecr-push \
     --assume-role-policy-document file://infra/aws/github-oidc-trust-policy.json
   aws iam put-role-policy --role-name github-actions-wistia-ecr-push \
     --policy-name ecr-push --policy-document file://infra/aws/ecr-push-permissions-policy.json
   ```
4. In the GitHub repo settings, add:
   - Secret `AWS_GITHUB_ACTIONS_ROLE_ARN` = the role's ARN from step 3.
   - Variable `AWS_REGION` = e.g. `us-east-1`.
   - Variable `ECR_REPOSITORY` = `wistia-video-analytics-ingestion`.

After that, every push runs lint + tests, and (if they pass) builds the
image and pushes it to ECR tagged with the commit SHA and `latest`.

## Tests

```bash
pytest -q
ruff check .
```

Unit tests mock AWS (via `moto`) and the Wistia API (via `responses`) — no
network calls or real credentials are needed to run the suite.

## Design decisions & assumptions

- **Incremental ingestion (FR7):** media metadata + stats are re-fetched in
  full every run (they're small, single-object responses). Visitor events
  (`stats/events.json`) are paginated in full each run, up to a
  `max_event_pages` safety cap (default 50 pages), and filtered client-side
  by comparing `received_at` against the per-media checkpoint. The Wistia
  events endpoint's sort order isn't a documented guarantee, so this design
  doesn't rely on "stop once we see an old event" — it inspects every event
  returned and only writes/checkpoints the new ones. Hitting the page cap
  logs a warning (surfaced via CloudWatch/SNS in the full architecture) so
  an unusually high-volume day is visible rather than silently truncated.
- **Checkpoint granularity:** one `checkpoint.json` in S3 keyed by
  `media_id`, holding `last_event_received_at` and `last_ingested_at`. A
  partial run (one media fails) still saves progress for the media that
  succeeded; the job exits non-zero if any media failed so Glue/CloudWatch
  can alert.
- **Retries:** the Wistia client retries on network errors, `429`, and
  `5xx` with exponential backoff (honoring `Retry-After` when present);
  `401`/`404` fail fast since retrying won't help.
- **No DBT / no non-AWS services**, per the technical constraints — Python
  for ingestion, PySpark for transformation.
- **No native "channel" field:** Wistia's media object exposes `project`
  and `share_link`, but nothing identifying a distribution channel (e.g.
  YouTube/Facebook). `dim_media.channel` is resolved from a
  `media_id -> channel` mapping in `config/pipeline_config.yaml`
  (`transformation.channel_mapping`), defaulting to `"Unknown"` for any
  media_id not listed — update that mapping by hand as media are added.
- **`fact_media_engagement` grain and metrics** (one row per
  media_id/visitor_id/date):
  - `play_count` = number of visitor events for that media/visitor/day
    (each event is treated as one playback session).
  - `watched_percent` = the furthest point reached that day
    (`max(percent_viewed)` across that day's events).
  - `total_watch_time` = `sum(percent_viewed * media.duration)` across that
    day's events — an approximation of seconds watched, since Wistia's
    events API reports `percent_viewed` per event, not raw watch seconds.
  - `play_rate` = the media-level `stats.play_rate` from the latest
    ingested snapshot, denormalized onto every row for that media_id —
    Wistia doesn't expose play_rate broken out by visitor or day.
- **Latest-snapshot dimensions:** `dim_media` and `dim_visitor` take the
  most recent ingested snapshot per key (media's `updated` timestamp;
  visitor's most recent event) rather than tracking full history
  (no SCD2) — acceptable since the requirement doc's model is a plain
  star schema, not a history-tracking one.
- **Idempotent writes:** each transformation run overwrites the bronze/
  silver/gold Delta tables from the full raw zone rather than appending,
  so reruns are safe and there's no double-counting from re-running a
  failed job. This trades incremental-transform efficiency for simplicity;
  revisit if the raw zone grows large enough for a full rebuild to be slow.

## Requirement traceability

| FR | Status |
|----|--------|
| FR2 Authenticate to Wistia Stats API | Done — bearer token via `WistiaClient` |
| FR3 Extract media metadata | Done — `get_media` |
| FR4 Extract engagement metrics | Done — `get_media_stats` |
| FR5 Extract visitor-level data | Done — `iter_media_events` |
| FR6 Pagination | Done — `iter_media_events` pages until a short page |
| FR7 Incremental ingestion | Done — checkpoint-based, see above |
| FR9 CI/CD | Done — `.github/workflows/ci.yml` runs lint + tests |
| FR10 Transform to dim/fact model | Done — `glue_jobs/transformation_job.py`, see above |
| FR1, FR8, FR11–FR12 | Pending — architecture doc, 7-day production run, Athena/dashboard, final submission |
