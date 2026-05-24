from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = os.environ.get("BDA_PROJECT_DIR", "/opt/airflow/project")
if not Path(PROJECT_DIR).exists():
    PROJECT_DIR = str(Path(__file__).resolve().parents[1])
LOCAL_TZ = ZoneInfo("Asia/Seoul")

# The 00:30 KST run is 15:30 UTC on the previous calendar day.  GitHub Archive
# daily tables are usually ready through UTC yesterday at that point.
WINDOW_START = "$(date -u -d '90 days ago' +%F)"
WINDOW_END = "$(date -u -d 'yesterday' +%F)"
MAX_DAYS = 90

UV_BASE = (
    "uv run --no-project "
    "--with pandas "
    "--with pyarrow "
    "--with duckdb "
    "--with google-cloud-bigquery "
    "--with google-cloud-bigquery-storage "
    "--with db-dtypes "
)

PLAN_FACT_COMMAND = (
    UV_BASE
    + "python scripts/sync_bq_metrics.py "
    "--project bda-coai "
    "--dataset mart "
    "--source bigquery "
    f"--start {WINDOW_START} "
    f"--end {WINDOW_END} "
    f"--max-days {MAX_DAYS} "
    "--mode replace-days "
    "--plan-only"
)

SYNC_FACT_COMMAND = PLAN_FACT_COMMAND.removesuffix(" --plan-only") + " --no-summary"

DBT_BUILD_COMMAND = (
    "dbt build "
    "--project-dir dbt/gharchive_metrics "
    "--profiles-dir dbt/profiles"
)

default_env = {
    "GCP_KEY_PATH": "/opt/airflow/gcp-key.json",
    "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/gcp-key.json",
    "DBT_BIGQUERY_PROJECT": "bda-coai",
    "DBT_BIGQUERY_DATASET": "mart",
    "DBT_BIGQUERY_LOCATION": "US",
    "PATH": "/home/airflow/.local/bin:/home/airflow/dbt_venv/bin:/usr/local/bin:/usr/bin:/bin",
    "PYTHONPATH": "/opt/airflow/project/src",
    "UV_CACHE_DIR": "/opt/airflow/uv-cache",
    "UV_PROJECT_ENVIRONMENT": "/opt/airflow/uv-env/bda-2",
}

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

fact_task_defaults = {
    "cwd": PROJECT_DIR,
    "env": default_env,
    "append_env": True,
}

with DAG(
    dag_id="gharchive_dbt_metrics",
    description="Load the rolling GitHub Archive fact table used for drill-downs and dbt demos.",
    start_date=datetime(2026, 5, 12, tzinfo=LOCAL_TZ),
    schedule="30 0 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["bda", "gharchive", "dbt", "cosmos", "semantic-layer"],
) as dag:
    plan_fact_sync = BashOperator(
        task_id="plan_fact_sync",
        bash_command=PLAN_FACT_COMMAND,
        execution_timeout=timedelta(minutes=5),
        **fact_task_defaults,
    )

    sync_fact = BashOperator(
        task_id="sync_fact",
        bash_command=SYNC_FACT_COMMAND,
        execution_timeout=timedelta(minutes=45),
        **fact_task_defaults,
    )

    build_dbt_metrics = BashOperator(
        task_id="build_dbt_metrics",
        bash_command=DBT_BUILD_COMMAND,
        execution_timeout=timedelta(minutes=30),
        **fact_task_defaults,
    )

    plan_fact_sync >> sync_fact >> build_dbt_metrics
