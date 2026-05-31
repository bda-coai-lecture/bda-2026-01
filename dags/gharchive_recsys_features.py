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

HISTORY_START = "2026-03-22"
HISTORY_END = "2026-05-02"
RANK_START = "2026-05-03"
RANK_END = "2026-05-09"
TEST_START = "2026-05-10"
TEST_END = "2026-05-16"
EXPERIMENT_ID = "week6_20260322_20260516"
OUTPUT_SUFFIX = "airflow_20260516"
MART_READY_FILE = f"data/marts/week6/_{EXPERIMENT_ID}.ready"
FEATURE_PARQUET = f"data/features/week6/ranker_features_{OUTPUT_SUFFIX}.parquet"
FEATURE_SUMMARY = f"data/features/week6/ranker_features_{OUTPUT_SUFFIX}_summary.json"

UV_BASE = (
    "uv run --no-project "
    "--with pandas "
    "--with pyarrow "
    "--with numpy "
    "--with scipy "
    "--with implicit "
    "--with lightgbm "
    "--with mlflow "
    "--with tqdm "
)

CHECK_MARTS_COMMAND = (
    "test -s data/marts/week6/user_repo_interaction_mart.parquet && "
    "test -s data/marts/week6/user_profile_mart.parquet && "
    "test -s data/marts/week6/repo_feature_mart.parquet && "
    "test -s data/marts/week6/repo_repo_related_mart.parquet && "
    "test -s data/marts/week6/experiment_split_mart.parquet && "
    f"test -s {MART_READY_FILE}"
)

BUILD_MARTS_COMMAND = (
    "set -euo pipefail\n"
    f"READY_FILE={MART_READY_FILE}\n"
    "if "
    "test -s data/marts/week6/user_repo_interaction_mart.parquet && "
    "test -s data/marts/week6/user_profile_mart.parquet && "
    "test -s data/marts/week6/repo_feature_mart.parquet && "
    "test -s data/marts/week6/repo_repo_related_mart.parquet && "
    "test -s data/marts/week6/experiment_split_mart.parquet && "
    "test -s \"$READY_FILE\"; "
    "then echo 'Recsys marts already exist for this experiment; skipping rebuild.'; exit 0; fi\n"
    "uv run --no-project "
    "--with pandas "
    "--with pyarrow "
    "--with numpy "
    "--with tqdm "
    "python scripts/week6_build_recsys_marts.py "
    f"--history-start {HISTORY_START} "
    f"--history-end {HISTORY_END} "
    f"--rank-label-start {RANK_START} "
    f"--rank-label-end {RANK_END} "
    f"--test-start {TEST_START} "
    f"--test-end {TEST_END} "
    f"--experiment-id {EXPERIMENT_ID} && "
    f"printf 'experiment_id={EXPERIMENT_ID}\\n"
    f"history={HISTORY_START}..{HISTORY_END}\\n"
    f"rank={RANK_START}..{RANK_END}\\n"
    f"test={TEST_START}..{TEST_END}\\n' > \"$READY_FILE\""
)

BUILD_FEATURES_COMMAND = (
    "set -euo pipefail\n"
    "if "
    f"test -s {FEATURE_PARQUET} && "
    f"test -s {FEATURE_SUMMARY}; "
    "then echo 'Recsys feature cache already exists for this output suffix; skipping rebuild.'; exit 0; fi\n"
    + UV_BASE
    + "python scripts/week6_build_recsys_features.py "
    f"--history-start {HISTORY_START} "
    f"--history-end {HISTORY_END} "
    f"--rank-start {RANK_START} "
    f"--rank-end {RANK_END} "
    f"--test-start {TEST_START} "
    f"--test-end {TEST_END} "
    "--use-marts always "
    "--mart-dir data/marts/week6 "
    "--output-dir data/features/week6 "
    f"--output-suffix {OUTPUT_SUFFIX} "
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

# Warn-only train/serve feature-skew check: score the freshly built ranker
# feature parquet against the blessed reference vintage. Thresholds are a
# bootstrap noise floor (scripts/drift_calibrate_recsys.py). Always exits 0.
DETECT_DRIFT_COMMAND = (
    "uv run --no-project "
    "--with pandas --with pyarrow --with numpy "
    "python scripts/drift_detect_recsys.py --slack "
    f"--parquet {FEATURE_PARQUET} "
    f"--summary {FEATURE_SUMMARY}"
)

default_env = {
    "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
    "PYTHONPATH": "/opt/airflow/project/src:/opt/airflow/project/scripts",
    "UV_CACHE_DIR": "/opt/airflow/uv-cache",
    "UV_PROJECT_ENVIRONMENT": "/opt/airflow/uv-env/bda-2",
    "MPLCONFIGDIR": "/tmp/matplotlib",
}

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
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
    schedule="0 9 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["bda", "gharchive", "recsys", "features"],
) as dag:
    build_recsys_marts = BashOperator(
        task_id="build_recsys_marts",
        bash_command=BUILD_MARTS_COMMAND,
        execution_timeout=timedelta(hours=2),
        **task_defaults,
    )

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

    detect_feature_drift = BashOperator(
        task_id="detect_feature_drift",
        bash_command=DETECT_DRIFT_COMMAND,
        execution_timeout=timedelta(minutes=15),
        **task_defaults,
    )

    build_recsys_marts >> check_recsys_marts >> build_ranker_features >> detect_feature_drift
