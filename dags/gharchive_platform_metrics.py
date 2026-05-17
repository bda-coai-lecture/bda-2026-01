from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"
LOCAL_TZ = ZoneInfo("Asia/Seoul")

WINDOW_START = "2026-04-04"
WINDOW_END = "2026-05-08"
MAX_DAYS = 35
METADATA_WINDOW_START = WINDOW_START
METADATA_WINDOW_END = WINDOW_END

UV_BASE = (
    "uv run --no-project "
    "--with pandas "
    "--with pyarrow "
    "--with requests "
    "--with duckdb "
    "--with google-cloud-bigquery "
    "--with db-dtypes "
)

REFRESH_METADATA_COMMAND = (
    "uv run --no-project "
    "--with pandas "
    "--with requests "
    "--with duckdb "
    "python scripts/refresh_repo_metadata.py "
    "--parquet-dir data/daily_agg "
    f"--start {METADATA_WINDOW_START} "
    f"--end {METADATA_WINDOW_END} "
    f"--sample-date {METADATA_WINDOW_END} "
    "--top-n 1000 "
    "--systematic-sample "
    "--sample-seed bda-repo-metadata-v1 "
    "--cache-tier warm "
    "--max-fetch 4500 "
    "--rate-limit-pause 0.2"
)

PLAN_METRICS_COMMAND = (
    UV_BASE
    + "python scripts/sync_bq_metrics.py "
    "--project bda-coai "
    "--dataset mart "
    "--parquet-dir data/daily_agg "
    f"--start {WINDOW_START} "
    f"--end {WINDOW_END} "
    f"--max-days {MAX_DAYS} "
    "--mode replace-all "
    "--skip-fact "
    "--build-metrics "
    "--plan-only"
)

SYNC_METRICS_COMMAND = PLAN_METRICS_COMMAND.removesuffix(" --plan-only")

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
}

metadata_task_defaults = {
    "cwd": PROJECT_DIR,
    "env": default_env,
    "append_env": True,
    "execution_timeout": timedelta(minutes=30),
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
    schedule="0 0,4,8,12,16,20 * * *",
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
    description="Sync local GitHub Archive parquet files to BigQuery and rebuild platform metrics.",
    start_date=datetime(2026, 5, 1, tzinfo=LOCAL_TZ),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["bda", "gharchive", "bigquery", "metabase"],
) as dag:
    plan_metric_sync = BashOperator(
        task_id="plan_metric_sync",
        bash_command=PLAN_METRICS_COMMAND,
        execution_timeout=timedelta(minutes=5),
        **metrics_task_defaults,
    )

    sync_metrics = BashOperator(
        task_id="sync_metrics",
        bash_command=SYNC_METRICS_COMMAND,
        execution_timeout=timedelta(minutes=45),
        **metrics_task_defaults,
    )

    plan_metric_sync >> sync_metrics
