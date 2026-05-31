from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

from utils.slack_alert import notify_failure

PROJECT_DIR = os.environ.get("BDA_PROJECT_DIR", "/opt/airflow/project")
if not Path(PROJECT_DIR).exists():
    PROJECT_DIR = str(Path(__file__).resolve().parents[1])
LOCAL_TZ = ZoneInfo("Asia/Seoul")

# Evaluate the rolling window in KST. At 00:30 KST, `date -u ... yesterday`
# would point two local calendar days back, leaving the newest ready shard out.
WINDOW_START = "$(TZ=Asia/Seoul date -d '90 days ago' +%F)"
WINDOW_END = "$(TZ=Asia/Seoul date -d 'yesterday' +%F)"
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
    "RAW_START_DATE=\""
    + WINDOW_START
    + "\" RAW_END_DATE=\""
    + WINDOW_END
    + "\" "
    "dbt build "
    "--project-dir dbt/gharchive_metrics "
    "--profiles-dir dbt/profiles "
    "--vars \"{raw_start_date: '$RAW_START_DATE', raw_end_date: '$RAW_END_DATE'}\""
)

# Warn-only input-drift check on the latest BigQuery fact partition. Always exits
# 0; a breach posts a Slack warning but does not fail the task.
DRIFT_DETECT_COMMAND = (
    "uv run --no-project "
    "--with pandas --with pyarrow --with numpy "
    "--with google-cloud-bigquery --with db-dtypes "
    "python scripts/drift_detect_platform.py "
    "--source bigquery "
    "--project bda-coai "
    "--dataset mart "
    "--fact-table fact_user_repo_activity "
    "--slack"
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
    "on_failure_callback": notify_failure,
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
        execution_timeout=timedelta(hours=3),
        **fact_task_defaults,
    )

    build_dbt_metrics = BashOperator(
        task_id="build_dbt_metrics",
        bash_command=DBT_BUILD_COMMAND,
        execution_timeout=timedelta(minutes=30),
        **fact_task_defaults,
    )

    detect_metric_drift = BashOperator(
        task_id="detect_metric_drift",
        bash_command=DRIFT_DETECT_COMMAND,
        execution_timeout=timedelta(minutes=15),
        **fact_task_defaults,
    )

    plan_fact_sync >> sync_fact >> build_dbt_metrics >> detect_metric_drift
