"""Backfill MLflow runs from existing Week 6/7 recommender artifacts.

This script does not retrain models. It reads local metrics CSV and summary JSON
files that were already used in the lecture docs, then logs comparable params,
core metrics, and lightweight artifacts into the SQLite MLflow tracking store.

Usage:
    uv run python scripts/backfill_mlflow_from_week6_artifacts.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import mlflow
import pandas as pd
from mlflow.tracking import MlflowClient


ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = ROOT / "data/models/week6"
TRACKING_URI = f"sqlite:///{ROOT / 'mlflow.db'}"

TWO_STAGE_EXPERIMENT = "recsys-two-stage"
RERANK_EXPERIMENT = "recsys-rerank"
RETRIEVAL_EXPERIMENT = "recsys-retrieval"

TWO_STAGE_SUFFIXES = [
    "latest",
    "feature_only_like_latest",
    "tune_full_als96_i12_lgbm63",
    "tune_full_als96_i12_lgbm63_diagnostics",
    "weight_screen_baseline",
    "weight_screen_activity",
    "weight_screen_conservative_contrib",
    "weight_screen_explicit_interest",
    "weight_activity_full_als64_i12_lgbm31",
    "sourcecap_screen_baseline",
    "sourcecap_screen_als200_recent40",
    "sourcecap_screen_recent40_pop40",
    "sourcecap_screen_recent0_pop80",
    "related_source_screen_related40",
    "related_source_screen_related80",
    "related_source_screen_related80_anchor20",
    "related80_anchor20_full_als96_i12_lgbm63",
]

RERANK_SUFFIXES = [
    "hist28_mini_neural",
    "hist28_mid_cache",
    "hist28_mid_e3",
    "week7_dlrm_mid_e3",
    "neural75_mid_k300_h200_cache",
    "neural75_mid_k300_h200_dlrm_e5_lr7e4",
    "week7_full_500k_u100k_e3_reuse_b32768",
]

RETRIEVAL_RUNS = [
    ("two_tower_week6_full_e5", MODEL_DIR / "two_tower_week6_full_e5_metrics.csv"),
    ("two_tower_week6_full_v2_e5", MODEL_DIR / "two_tower_week6_full_v2_e5_metrics.csv"),
]

EXPERIMENT_TAGS = {
    TWO_STAGE_EXPERIMENT: {
        "experiment_role": "candidate",
        "experiment_stage": "candidate_generation_and_re-rank_baseline",
        "mlflow.note.content": (
            "Candidate experiment: backfilled historical ALS/recent/popular/"
            "related candidate source and LGBM re-rank runs from local artifacts."
        ),
    },
    RERANK_EXPERIMENT: {
        "experiment_role": "re-rank",
        "experiment_stage": "shared_candidate_re-rank_comparison",
        "mlflow.note.content": (
            "Re-rank experiment: backfilled LGBM/FM/Deep&Wide/DeepFM/DLRM "
            "comparisons from local artifacts."
        ),
    },
    RETRIEVAL_EXPERIMENT: {
        "experiment_role": "retrieval",
        "experiment_stage": "two_tower_vs_als_retrieval",
        "mlflow.note.content": (
            "Retrieval experiment: backfilled Two-Tower vs ALS/Popularity "
            "results from local artifacts."
        ),
    },
}


def metric_model_key(model_name: str) -> str:
    return (
        model_name.lower()
        .replace("/", "_")
        .replace("&", "and")
        .replace("-", "_")
        .replace(" ", "_")
    )


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def scalar(value: Any) -> str | int | float | bool:
    if isinstance(value, (str, int, float, bool)):
        return value
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return ",".join(str(part) for part in value)
    return str(value)


def set_experiment_metadata(experiment_name: str) -> str:
    mlflow.set_experiment(experiment_name)
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        raise RuntimeError(f"Could not create MLflow experiment: {experiment_name}")
    client = MlflowClient(tracking_uri=mlflow.get_tracking_uri())
    for key, value in EXPERIMENT_TAGS[experiment_name].items():
        client.set_experiment_tag(experiment.experiment_id, key, value)
    return experiment.experiment_id


def existing_run_id(experiment_id: str, run_name: str) -> str | None:
    client = MlflowClient(tracking_uri=mlflow.get_tracking_uri())
    runs = client.search_runs(
        [experiment_id],
        filter_string=f"attributes.run_name = '{run_name}'",
        max_results=1,
    )
    if not runs:
        return None
    return runs[0].info.run_id


def log_param_if_possible(key: str, value: Any) -> None:
    if value is None:
        return
    try:
        mlflow.log_param(key, scalar(value))
    except Exception as exc:
        # Existing runs may already have an immutable param with a different
        # value. Keep going so metrics/artifacts can still be repaired.
        print(f"skip param conflict: {key} ({exc})")


def log_args_and_focus_params(summary: dict[str, Any], role: str) -> None:
    args = summary.get("args") or {}
    focus_map = {
        "history_start": "exp_data_history_start",
        "history_end": "exp_data_history_end",
        "rank_start": "exp_data_rank_start",
        "rank_end": "exp_data_rank_end",
        "test_start": "exp_data_test_start",
        "test_end": "exp_data_test_end",
        "sample_ratio": "exp_data_sample_ratio",
        "max_items": "exp_data_max_items",
        "rank_users": "exp_data_rank_users",
        "eval_users": "exp_data_eval_users_requested",
        "candidate_k": "exp_candidate_k",
        "hybrid_extra": "exp_candidate_hybrid_extra",
        "recent_candidate_cap": "exp_candidate_recent_cap",
        "popular_candidate_cap": "exp_candidate_popular_cap",
        "related_candidate_cap": "exp_candidate_related_cap",
        "related_top_per_anchor": "exp_candidate_related_top_per_anchor",
        "related_max_seen_anchors": "exp_candidate_related_max_seen_anchors",
        "retrieval_model": "exp_ranker_retrieval_model",
        "factors": "exp_ranker_factors",
        "iterations": "exp_ranker_iterations",
        "lgbm_num_leaves": "exp_ranker_lgbm_num_leaves",
        "lgbm_learning_rate": "exp_ranker_lgbm_learning_rate",
        "lgbm_min_child_samples": "exp_ranker_lgbm_min_child_samples",
        "lgbm_estimators": "exp_ranker_lgbm_n_estimators",
        "lgbm_colsample": "exp_ranker_lgbm_colsample",
        "rankers": "exp_ranker_names",
        "epochs": "exp_ranker_epochs",
        "batch_size": "exp_ranker_batch_size",
        "lr": "exp_ranker_learning_rate",
        "hidden_dims": "exp_ranker_hidden_dims",
        "device": "exp_ranker_device",
        "reuse_feature_cache": "exp_run_reuse_feature_cache",
        "write_feature_cache": "exp_run_write_feature_cache",
        "feature_cache_path": "exp_run_feature_cache_path",
    }
    for source, target in focus_map.items():
        if source in args:
            log_param_if_possible(target, args[source])

    log_param_if_possible("exp_run_role", role)
    log_param_if_possible("exp_run_backfilled", True)
    log_param_if_possible("exp_actual_eval_users", summary.get("eval_users"))
    log_param_if_possible("exp_elapsed_min", summary.get("elapsed_min"))
    log_param_if_possible("exp_candidate_related_anchor_count", summary.get("related_anchor_count"))
    ranker = summary.get("ranker") or summary.get("rank_data") or {}
    log_param_if_possible("actual_rank_rows", ranker.get("rank_rows"))
    log_param_if_possible("actual_rank_users", ranker.get("rank_users"))
    log_param_if_possible("actual_positive_rate", ranker.get("positive_rate"))

    cache_args = summary.get("feature_cache_args") or {}
    for key in ["candidate_k", "hybrid_extra", "max_items", "rank_users", "eval_users"]:
        if key in cache_args:
            log_param_if_possible(f"cache_{key}", cache_args[key])


def choose_primary_model(metrics: pd.DataFrame, preferred: str | None) -> str:
    models = set(metrics["model"].astype(str))
    if preferred and preferred in models:
        return preferred
    at_100 = metrics[metrics["k"].astype(int).eq(100)].copy()
    if at_100.empty:
        at_100 = metrics.copy()
    best = at_100.sort_values(["ndcg", "recall"], ascending=False).iloc[0]
    return str(best["model"])


def log_metrics(metrics: pd.DataFrame, primary_model: str) -> None:
    for row in metrics.itertuples(index=False):
        k = int(row.k)
        model_name = str(row.model)
        if model_name == "Popularity":
            continue
        if k not in {10, 100}:
            continue
        model_key = metric_model_key(model_name)
        if k == 100:
            mlflow.log_metric(f"{model_key}_ndcg_at_100", float(row.ndcg))
            mlflow.log_metric(f"{model_key}_recall_at_100", float(row.recall))
            mlflow.log_metric(
                f"{model_key}_unique_recommended_at_100",
                int(row.unique_recommended),
            )

        if model_name == primary_model:
            if k == 10:
                mlflow.log_metric("core_ndcg_at_10", float(row.ndcg))
            if k == 100:
                mlflow.log_metric("core_ndcg_at_100", float(row.ndcg))
                mlflow.log_metric("core_recall_at_100", float(row.recall))
                mlflow.log_metric("core_unique_at_100", int(row.unique_recommended))


def artifact_candidates(prefix: str, suffix: str, summary: dict[str, Any]) -> list[Path]:
    paths = [
        MODEL_DIR / f"{prefix}_{suffix}_metrics.csv",
        MODEL_DIR / f"{prefix}_{suffix}_summary.json",
        MODEL_DIR / f"week6_user_diagnostics_{suffix}.parquet",
        MODEL_DIR / f"week6_qual_cases_{suffix}.parquet",
    ]
    for path in (MODEL_DIR / "diagnostics").glob(f"{suffix}_*"):
        paths.append(path)

    summary_paths = summary.get("paths") or {}
    for value in summary_paths.values():
        if isinstance(value, str):
            paths.append(ROOT / value)
        elif isinstance(value, dict):
            paths.extend(ROOT / nested for nested in value.values() if isinstance(nested, str))

    small_model_patterns = [
        f"lgbm_ranker_{suffix}.txt",
        f"lgbm_ranker_compare_{suffix}.txt",
        f"fm_ranker_{suffix}.pt",
        f"deepwide_ranker_{suffix}.pt",
        f"deepfm_ranker_{suffix}.pt",
        f"dlrm_ranker_{suffix}.pt",
    ]
    for pattern in small_model_patterns:
        paths.append(MODEL_DIR / pattern)

    unique: list[Path] = []
    seen = set()
    for path in paths:
        if not path.exists() or path in seen:
            continue
        seen.add(path)
        # Avoid copying large ALS/model cache artifacts into MLflow.
        if path.suffix == ".pkl":
            continue
        if path.stat().st_size > 100 * 1024 * 1024:
            continue
        unique.append(path)
    return unique


def log_artifacts(paths: list[Path]) -> None:
    for path in paths:
        if path.exists():
            mlflow.log_artifact(str(path))


def backfill_run(
    experiment_name: str,
    run_name: str,
    metrics_path: Path,
    summary_path: Path,
    role: str,
    preferred_primary: str | None,
    prefix: str,
    suffix: str,
) -> str:
    if not metrics_path.exists() or not summary_path.exists():
        return f"missing artifacts: {run_name}"

    experiment_id = set_experiment_metadata(experiment_name)
    run_id = existing_run_id(experiment_id, run_name)
    summary = load_json(summary_path)
    metrics = pd.read_csv(metrics_path)
    primary_model = choose_primary_model(metrics, preferred_primary)

    start_kwargs = {"run_id": run_id} if run_id else {"run_name": run_name}
    with mlflow.start_run(**start_kwargs):
        mlflow.set_tag("backfilled_from_artifacts", True)
        mlflow.set_tag("source_metrics_path", str(metrics_path.relative_to(ROOT)))
        mlflow.set_tag("source_summary_path", str(summary_path.relative_to(ROOT)))
        mlflow.set_tag("primary_model", primary_model)
        mlflow.set_tag("experiment_role", role)
        mlflow.set_tag("ui_metric_1", "core_ndcg_at_100")
        mlflow.set_tag("ui_metric_2", "core_recall_at_100")
        log_args_and_focus_params(summary, role)
        log_param_if_possible("artifact_suffix", suffix)
        log_metrics(metrics, primary_model)
        elapsed = summary.get("elapsed_min")
        if elapsed is not None:
            mlflow.log_metric("elapsed_min", float(elapsed))
        eval_users = summary.get("eval_users")
        if eval_users is not None:
            mlflow.log_metric("eval_users", float(eval_users))
        log_artifacts(artifact_candidates(prefix, suffix, summary))

    action = "updated" if run_id else "created"
    return f"{action}: {experiment_name}/{run_name} primary={primary_model}"


def backfill_two_stage() -> list[str]:
    messages = []
    for suffix in TWO_STAGE_SUFFIXES:
        messages.append(
            backfill_run(
                TWO_STAGE_EXPERIMENT,
                suffix,
                MODEL_DIR / f"week6_two_stage_{suffix}_metrics.csv",
                MODEL_DIR / f"week6_two_stage_{suffix}_summary.json",
                "candidate",
                "Two-Stage/Fallback",
                "week6_two_stage",
                suffix,
            )
        )
    return messages


def backfill_rerank() -> list[str]:
    messages = []
    for suffix in RERANK_SUFFIXES:
        messages.append(
            backfill_run(
                RERANK_EXPERIMENT,
                suffix,
                MODEL_DIR / f"week6_ranker_compare_{suffix}_metrics.csv",
                MODEL_DIR / f"week6_ranker_compare_{suffix}_summary.json",
                "re-rank",
                None,
                "week6_ranker_compare",
                suffix,
            )
        )
    return messages


def backfill_retrieval() -> list[str]:
    messages = []
    for run_name, metrics_path in RETRIEVAL_RUNS:
        suffix = run_name.removeprefix("two_tower_")
        summary_path = metrics_path.with_name(f"{run_name}_summary.json")
        messages.append(
            backfill_run(
                RETRIEVAL_EXPERIMENT,
                run_name,
                metrics_path,
                summary_path,
                "retrieval",
                "Two-Tower",
                "two_tower",
                suffix,
            )
        )
    return messages


def main() -> None:
    mlflow.set_tracking_uri(TRACKING_URI)
    messages = []
    messages.extend(backfill_two_stage())
    messages.extend(backfill_rerank())
    messages.extend(backfill_retrieval())
    for message in messages:
        print(message)


if __name__ == "__main__":
    main()
