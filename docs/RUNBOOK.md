# Runbook — Wistia Video Analytics Pipeline

Step-by-step AWS Console instructions to deploy and operate every stage of
this pipeline, in order. For the "what and why" behind these steps, see
[`PROJECT_OVERVIEW.md`](PROJECT_OVERVIEW.md).

Values used throughout this project (substitute your own if different):

| Setting | Value |
|---|---|
| AWS account | `017535066714` |
| AWS region | `us-east-2` |
| S3 bucket | `xxdea-p4-wistia-bucket` |
| Raw prefix | `wistia-video-analytics/raw` |
| Curated prefix | `wistia-video-analytics/curated` |
| Wistia media IDs | `8hunphufxp`, `9k4tbcdfg0` |
| Secrets Manager secret | `wistia/api-token` |
| Ingestion Glue job name | `xxdea-p4-wistia-ingestion-job` |
| Transformation Glue job name | `xxdea-p4-wistia-transformation-job` |
| Athena database | `wistia_video_analytics` |
| SNS topic | `wistia-pipeline-alerts` |
| EventBridge rule | `wistia-glue-job-failure-alerts` |
| Glue Workflow | `wistia-video-analytics-workflow` |

---

## Stage 0: Prerequisites

1. **S3 bucket** — create (or reuse) one bucket for the whole pipeline;
   raw and curated data live under different prefixes in the same bucket,
   no separate buckets needed.
2. **Secrets Manager secret** for the Wistia API token:
   - Secrets Manager console → **Store a new secret** → "Other type of
     secret" → key/value pair, key `api_token`, value = your Wistia token
     → name it `wistia/api-token` → Store.
3. **IAM role for the ingestion job** (Glue Python Shell):
   - Trust policy: standard Glue service role trust (`glue.amazonaws.com`).
   - Permissions: `s3:GetObject`/`s3:PutObject`/`s3:ListBucket` on the
     bucket (raw + checkpoint prefixes), `secretsmanager:GetSecretValue`
     on the `wistia/api-token` secret, plus the AWS-managed
     `AWSGlueServiceRole` policy for CloudWatch Logs access.

---

## Stage 1: Ingestion job (Glue Python Shell)

1. Glue console → ETL jobs → **Create job** → **Script editor**.
2. Job details tab:
   - Name: `xxdea-p4-wistia-ingestion-job`
   - Type: **Python Shell**
   - Python version: 3.9
   - IAM role: the role from Stage 0.
   - Job parameters (one `--name` / value pair per row):
     - `--media-ids` = `8hunphufxp,9k4tbcdfg0`
     - `--raw-bucket` = `xxdea-p4-wistia-bucket`
     - `--checkpoint-bucket` = `xxdea-p4-wistia-bucket`
     - (optional, defaults shown) `--raw-prefix` =
       `wistia-video-analytics/raw`, `--checkpoint-key` =
       `wistia-video-analytics/raw/_checkpoint/checkpoint.json`,
       `--secret-name` = `wistia/api-token`
   - Under Advanced properties: `--additional-python-modules` =
     `requests==2.31.0` (boto3 ships with the runtime already).
3. Script tab: paste the full contents of
   `glue_jobs/ingestion_job_standalone.py` (single-file, no separate
   library zip needed — this is what avoided the
   `ModuleNotFoundError: No module named 'wistia_pipeline'` issue).
4. Save, then **Run**. Check CloudWatch Logs for the run for
   `Wrote N new event(s)` / `No new events` log lines.

## Stage 2: Raw zone (S3) — verify

Check S3 for:
```
s3://xxdea-p4-wistia-bucket/wistia-video-analytics/raw/media_stats/load_date=YYYY-MM-DD/<media_id>.json
s3://xxdea-p4-wistia-bucket/wistia-video-analytics/raw/visitor_stats/load_date=YYYY-MM-DD/<media_id>_<time>.json
s3://xxdea-p4-wistia-bucket/wistia-video-analytics/raw/_checkpoint/checkpoint.json
```

## Stage 3: Transformation job (Glue Spark)

1. Glue console → ETL jobs → **Create job** → **Script editor**.
2. Job details tab:
   - Name: `xxdea-p4-wistia-transformation-job`
   - Type: **Spark** (not Python Shell)
   - Glue version: **5.0** (Spark 3.5 + Delta Lake 3.x)
   - IAM role: extend the Stage 0 role (or a copy) with
     `s3:GetObject`/`s3:PutObject`/`s3:DeleteObject` on the curated
     prefix too, not just raw/checkpoint.
   - Worker type: G.1X, Number of workers: 2 (data volume is small).
   - Job parameters:
     - `--raw-bucket` = `xxdea-p4-wistia-bucket`
     - `--curated-bucket` = `xxdea-p4-wistia-bucket`
     - `--datalake-formats` = `delta`
     - `--conf` =
       `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`
       (yes, a second `--conf` inline in the value — this is the
       documented way to pass multiple Spark configs through one Glue job
       parameter; **omitting this causes**
       `DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG`, since
       `--datalake-formats` alone only provisions the Delta JARs, it
       doesn't configure the Spark session for them).
3. Script tab: paste the full contents of
   `glue_jobs/transformation_job_standalone.py`.
4. Save, then **Run**. Check CloudWatch Logs for the
   `Bronze: N media_stats row(s)...` / `Gold: N dim_media row(s)...` log
   lines.

## Stage 4: Curated zone (S3 Delta Lake) — verify

Check S3 for `_delta_log/` subfolders (proof of real Delta tables, not
plain files) under:
```
s3://xxdea-p4-wistia-bucket/wistia-video-analytics/curated/bronze/{media_stats,visitor_events}/
s3://xxdea-p4-wistia-bucket/wistia-video-analytics/curated/silver/{media_stats,visitor_events}/
s3://xxdea-p4-wistia-bucket/wistia-video-analytics/curated/gold/{dim_media,dim_visitor,fact_media_engagement}/
```

## Stage 5: Glue Data Catalog + Athena

1. Athena console → Settings → **Manage** → set the query result location,
   e.g. `s3://xxdea-p4-wistia-bucket/athena-results/`.
2. Run `infra/aws/athena_ddl.sql` in the Athena query editor. No crawler
   needed — Athena engine v3 reads Delta schema natively from
   `_delta_log`, so `CREATE EXTERNAL TABLE ... TBLPROPERTIES
   ('table_type' = 'DELTA')` is enough.
3. Verify: `SELECT * FROM wistia_video_analytics.dim_media LIMIT 10;`
   (also in the DDL file, along with `dim_visitor`, `fact_media_engagement`,
   and an example join query).

## Stage 6: Streamlit dashboard

1. `pip install -r requirements-dashboard.txt`
2. Ensure the AWS identity running it has:
   - Confirm with: `aws sts get-caller-identity`
   - Attach `infra/aws/dashboard-athena-glue-read-policy.json` (Athena
     query execution + Glue Catalog read, scoped to the
     `wistia_video_analytics` database) if not already granted — this is
     the fix for both `AccessDeniedException: glue:GetDatabase` and (via
     `ctas_approach=False` already set in the code) avoids needing
     `glue:CreateTable`/`DeleteTable` at all.
   - S3 read on the curated bucket + write on the Athena query-results
     location (usually already covered by an existing S3 policy).
3. `export AWS_PROFILE=your-profile` (or set `AWS_REGION` + default
   credentials).
4. `streamlit run dashboard/app.py` — opens locally, reads gold tables
   through Athena.

## Stage 7: Scheduling & monitoring (FR8)

### 7a. SNS topic for failure alerts

1. SNS console → Topics → **Create topic** → Standard → name
   `wistia-pipeline-alerts` → Create.
2. **Create subscription** → Protocol `Email` → your alert address →
   Create subscription.
3. Confirm the subscription from the email inbox — SNS delivers nothing
   until confirmed.

### 7b. EventBridge rule: Glue job failure → SNS

1. EventBridge console → Rules → **Create rule** → name
   `wistia-glue-job-failure-alerts`, event bus `default`, rule type "Rule
   with an event pattern".
2. Event pattern → Custom pattern (JSON editor) → paste
   `infra/aws/eventbridge-glue-failure-rule.json`.
3. Target → SNS topic → `wistia-pipeline-alerts` (console auto-grants the
   publish permission).
4. Create rule.

### 7c. Glue Workflow: daily schedule, ingestion → transformation

1. Glue console → Workflows → **Add workflow** → name
   `wistia-video-analytics-workflow` → Create.
2. **Add trigger** → New:
   - Name `daily-schedule-trigger`, type **Schedule**, cron e.g.
     `cron(0 6 * * ? *)` (6 AM UTC daily — adjust as needed), activate on
     creation.
   - Actions → Add job → `xxdea-p4-wistia-ingestion-job`.
3. **Add trigger** → New:
   - Name `transformation-after-ingestion-trigger`, type **Event**
     (conditional), "Start after ALL watched jobs/crawlers succeed",
     watching `xxdea-p4-wistia-ingestion-job` for `SUCCEEDED`, activate on
     creation.
   - Actions → Add job → `xxdea-p4-wistia-transformation-job`.
4. Save, then **Run** the workflow once manually to confirm the chain
   works before relying on the schedule.

### 7d. Tracking the 7 consecutive days

Glue console → Workflows → `wistia-video-analytics-workflow` → **History**
tab. Log one row per day in `docs/production-run-log.md`; a screenshot of
7 consecutive `SUCCEEDED` runs is the evidence for FR8/FR12.

## Stage 8: CI/CD (GitHub Actions → ECR)

1. Create the ECR repository:
   ```bash
   aws ecr create-repository --repository-name wistia-video-analytics-ingestion --region <REGION>
   ```
2. Create the GitHub OIDC identity provider (once per AWS account):
   ```bash
   aws iam create-open-id-connect-provider \
     --url https://token.actions.githubusercontent.com \
     --client-id-list sts.amazonaws.com \
     --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
   ```
3. Create the IAM role GitHub Actions assumes:
   ```bash
   aws iam create-role --role-name github-actions-wistia-ecr-push \
     --assume-role-policy-document file://infra/aws/github-oidc-trust-policy.json
   aws iam put-role-policy --role-name github-actions-wistia-ecr-push \
     --policy-name ecr-push --policy-document file://infra/aws/ecr-push-permissions-policy.json
   ```
   (fill in `<ACCOUNT_ID>`/`<REGION>` placeholders in those two JSON files
   first.)
4. GitHub repo → Settings → Secrets and variables → Actions:
   - Secret `AWS_GITHUB_ACTIONS_ROLE_ARN` = the role ARN from step 3.
   - Variable `AWS_REGION` = e.g. `us-east-1`.
   - Variable `ECR_REPOSITORY` = `wistia-video-analytics-ingestion`.
5. Every push now runs lint + tests (`.github/workflows/ci.yml`), and on
   success builds + pushes the Docker image to ECR tagged with the commit
   SHA and `latest`.

---

## Common failure modes and fixes

See `PROJECT_OVERVIEW.md` §7 for the full table of issues hit during
deployment and their root causes/fixes (Glue Script tab mismatches, the
`wistia_pipeline` module-not-found issue, the Delta `--conf` gotcha, the
dashboard IAM gaps, etc.) — worth checking there before re-diagnosing a
new but similar-looking error from scratch.
