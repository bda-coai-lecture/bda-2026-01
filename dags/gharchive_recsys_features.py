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

UV_BASE = (
    "uv run --no-project "
    "--with pandas "
    "--with pyarrow "
    "--with numpy "
    "--with scipy "
    "--with implicit "
    "--with lightgbm "
    "--with tqdm "
)

CHECK_MARTS_COMMAND = (
    "test -s data/marts/week6/user_repo_interaction_mart.parquet && "
    "test -s data/marts/week6/user_profile_mart.parquet && "
    "test -s data/marts/week6/repo_feature_mart.parquet && "
    "test -s data/marts/week6/repo_repo_related_mart.parquet && "
    "test -s data/marts/week6/experiment_split_mart.parquet"
)

BUILD_FEATURES_COMMAND = (
    UV_BASE
    + "python scripts/week6_build_recsys_features.py "
    "--history-start 2026-03-14 "
    "--history-end 2026-04-24 "
    "--rank-start 2026-04-25 "
    "--rank-end 2026-05-01 "
    "--test-start 2026-05-02 "
    "--test-end 2026-05-08 "
    "--use-marts always "
    "--mart-dir data/marts/week6 "
    "--output-dir data/features/week6 "
    "--output-suffix airflow_light "
    "--retrieval-model als "
    "--max-items 100000 "
    "--candidate-k 120 "
    "--hybrid-extra 80 "
    "--related-candidate-cap 30 "
    "--related-top-per-anchor 10 "
    "--related-max-seen-anchors 10 "
    "--rank-users 10000 "
    "--eval-users 3000 "
    "--factors 48 "
    "--iterations 4 "
    "--als-regularization 0.03 "
    "--chunk-size 1000 "
    "--parquet-batch-rows 50000"
)

default_env = {
    "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
    "PYTHONPATH": "/opt/airflow/project/src:/opt/airflow/project/scripts",
    "UV_CACHE_DIR": "/opt/airflow/uv-cache",
    "UV_PROJECT_ENVIRONMENT": "/opt/airflow/uv-env/bda-2",
}

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

task_defaults = {
    "cwd": PROJECT_DIR,
    "env": default_env,
    "append_env": True,
}

with DAG(
    dag_id="gharchive_recsys_features",
    description="Build Week 6 recommendation marts and reusable ranker feature cache.",
    start_date=datetime(2026, 5, 14, tzinfo=LOCAL_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["bda", "gharchive", "recsys", "features"],
) as dag:
    check_recsys_marts = BashOperator(
        task_id="check_recsys_marts",
        bash_command=CHECK_MARTS_COMMAND,
        execution_timeout=timedelta(minutes=2),
        **task_defaults,
    )

    build_ranker_features = BashOperator(
        task_id="build_ranker_features",
        bash_command=BUILD_FEATURES_COMMAND,
        execution_timeout=timedelta(hours=3),
        **task_defaults,
    )

    check_recsys_marts >> build_ranker_features
