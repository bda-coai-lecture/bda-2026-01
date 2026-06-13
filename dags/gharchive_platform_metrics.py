from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

from utils.slack_alert import notify_failure

PROJECT_DIR = "/opt/airflow/project"
LOCAL_TZ = ZoneInfo("Asia/Seoul")
BDA_PYTHON = "/home/airflow/bda_venv/bin/python"

# Evaluate the rolling window in KST. These DAGs are scheduled in KST, and UTC
# `yesterday` is one local day too old around the early-morning runs.
BACKFILL_START = "2025-09-01"
WINDOW_START = "$(TZ=Asia/Seoul date -d '90 days ago' +%F)"
WINDOW_END = "$(TZ=Asia/Seoul date -d 'yesterday' +%F)"
MAX_DAYS = 90
METADATA_WINDOW_START = WINDOW_START
METADATA_WINDOW_END = WINDOW_END

UV_BASE = (
    "uv run --no-project "
    "--with pandas "
    "--with pyarrow "
    "--with numpy "
    "--with requests "
    "--with duckdb "
    "--with google-cloud-bigquery "
    "--with google-cloud-bigquery-storage "
    "--with db-dtypes "
)

REFRESH_METADATA_COMMAND = (
    f"{BDA_PYTHON} scripts/refresh_repo_metadata.py "
    "--source bigquery "
    "--project bda-coai "
    "--dataset mart "
    "--fact-table fact_user_repo_activity "
    "--metadata-table repo_metadata "
    f"--start {METADATA_WINDOW_START} "
    f"--end {METADATA_WINDOW_END} "
    f"--sample-date {METADATA_WINDOW_END} "
    "--top-n 1000 "
    "--systematic-sample "
    "--sample-seed bda-repo-metadata-v1 "
    "--cache-tier warm "
    "--max-fetch 1000 "
    "--rate-limit-pause 0.2"
)

PLAN_METRICS_COMMAND = (
    UV_BASE
    + "python scripts/week8_backfill_compact_marts.py "
    "--project bda-coai "
    "--dataset mart "
    "--metadata-source bigquery "
    "--metadata-table repo_metadata "
    f"--start {BACKFILL_START} "
    f"--end {WINDOW_END} "
)

SYNC_METRICS_COMMAND = PLAN_METRICS_COMMAND

default_env = {
    "GCP_KEY_PATH": "/opt/airflow/gcp-key.json",
    "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
    "PYTHONPATH": "/opt/airflow/project/src",
    "UV_CACHE_DIR": "/opt/airflow/uv-cache",
    "UV_PROJECT_ENVIRONMENT": "/opt/airflow/uv-env/bda-2",
}

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}

metadata_task_defaults = {
    "cwd": PROJECT_DIR,
    "env": default_env,
    "append_env": True,
    "execution_timeout": timedelta(hours=1),
}

metrics_task_defaults = {
    "cwd": PROJECT_DIR,
    "env": default_env,
    "append_env": True,
}

with DAG(
    dag_id="gharchive_repo_metadata_refresh",
    description="Refresh the local GitHub repo metadata cache for trend dashboards.",
    start_date=datetime(2026, 5, 1, tzinfo=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["bda", "gharchive", "metadata", "github"],
) as metadata_dag:
    refresh_repo_metadata = BashOperator(
        task_id="refresh_repo_metadata",
        bash_command=REFRESH_METADATA_COMMAND,
        **metadata_task_defaults,
    )

with DAG(
    dag_id="gharchive_platform_metrics",
    description="Rebuild platform metrics from the GitHub Archive fact table loaded by dbt metrics.",
    start_date=datetime(2026, 5, 1, tzinfo=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["bda", "gharchive", "bigquery", "metabase"],
) as dag:
    plan_metric_sync = BashOperator(
        task_id="plan_metric_sync",
        bash_command=PLAN_METRICS_COMMAND + " --dry-run",
        execution_timeout=timedelta(minutes=5),
        **metrics_task_defaults,
    )

    sync_metrics = BashOperator(
        task_id="sync_metrics",
        bash_command=SYNC_METRICS_COMMAND,
        execution_timeout=timedelta(hours=2),
        **metrics_task_defaults,
    )

    plan_metric_sync >> sync_metrics
