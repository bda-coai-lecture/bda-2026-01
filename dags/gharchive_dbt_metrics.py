from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.constants import ExecutionMode, InvocationMode

PROJECT_DIR = os.environ.get("BDA_PROJECT_DIR", "/opt/airflow/project")
if not Path(PROJECT_DIR).exists():
    PROJECT_DIR = str(Path(__file__).resolve().parents[1])
DBT_PROJECT_DIR = f"{PROJECT_DIR}/dbt/gharchive_metrics"
DBT_PROFILES_DIR = f"{PROJECT_DIR}/dbt/profiles"
DBT_EXECUTABLE_PATH = os.environ.get("DBT_EXECUTABLE_PATH", "/home/airflow/dbt_venv/bin/dbt")
LOCAL_TZ = ZoneInfo("Asia/Seoul")

WINDOW_START = "2026-04-04"
WINDOW_END = "2026-05-08"
MAX_DAYS = 35

UV_BASE = (
    "uv run --no-project "
    "--with pandas "
    "--with pyarrow "
    "--with duckdb "
    "--with google-cloud-bigquery "
    "--with db-dtypes "
)

PLAN_FACT_COMMAND = (
    UV_BASE
    + "python scripts/sync_bq_metrics.py "
    "--project bda-coai "
    "--dataset mart "
    "--parquet-dir data/daily_agg "
    f"--start {WINDOW_START} "
    f"--end {WINDOW_END} "
    f"--max-days {MAX_DAYS} "
    "--mode replace-all "
    "--plan-only"
)

SYNC_FACT_COMMAND = PLAN_FACT_COMMAND.removesuffix(" --plan-only") + " --no-summary"

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
    description="Load GitHub Archive fact data and build metric marts with dbt through Cosmos.",
    start_date=datetime(2026, 5, 12, tzinfo=LOCAL_TZ),
    schedule="0 7 * * *",
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

    dbt_metrics = DbtTaskGroup(
        group_id="dbt_metrics",
        project_config=ProjectConfig(dbt_project_path=Path(DBT_PROJECT_DIR)),
        profile_config=ProfileConfig(
            profile_name="gharchive_metrics",
            target_name="dev",
            profiles_yml_filepath=Path(DBT_PROFILES_DIR) / "profiles.yml",
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            invocation_mode=InvocationMode.DBT_RUNNER,
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
        operator_args={
            "env": default_env,
            "append_env": True,
            "install_deps": False,
            "execution_timeout": timedelta(minutes=30),
        },
    )

    plan_fact_sync >> sync_fact >> dbt_metrics
