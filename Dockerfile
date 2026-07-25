FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY glue_jobs ./glue_jobs
COPY config ./config

ENTRYPOINT ["python", "glue_jobs/ingestion_job_standalone.py"]
