"""Week 6 two-stage recommendation experiment.

Temporal split:
    history     -> train ALS retrieval and build features
    rank-label  -> train LGBM ranker from ALS candidates
    test        -> final offline evaluation

Usage:
    uv run python scripts/week6_two_stage_v2.py --smoke
    uv run python scripts/week6_two_stage_v2.py
"""

from __future__ import annotations

import argparse
import json
import math
import pickle
import sqlite3
import subprocess
import time
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from implicit.als import AlternatingLeastSquares
from implicit.bpr import BayesianPersonalizedRanking
from scipy import sparse
from tqdm import tqdm

from gharchive.loader import load_period
DATA_DIR = Path("data/daily_agg")
MART_DIR = Path("data/marts/week6")
MODEL_DIR = Path("data/models/week6")
DB_PATH = Path("data/repo_metadata.db")
DEFAULT_RELATED_PATH = MODEL_DIR / "item2item_related_latest.parquet"
BEST_FULL_SUFFIX = "related80_anchor20_full_als96_i12_lgbm63"
RANKER_FEATURE_META_COLUMNS = {
    "group_index",
    "actor_id",
    "repo_id",
    "label",
    "raw_candidate_rank",
    "raw_candidate_score",
    "raw_candidate_source",
}

DEFAULT_WEIGHTS = {
    "WatchEvent": 1.0,
    "ForkEvent": 2.0,
    "IssuesEvent": 0.5,
    "PullRequestEvent": 3.0,
    "IssueCommentEvent": 0.3,
    "PushEvent": 0.2,
}


def jsonable_args(args: argparse.Namespace) -> dict:
    return {
        k: ",".join(str(part) for part in v)
        if isinstance(v, list)
        else str(v)
        if isinstance(v, (date, Path))
        else v
        for k, v in vars(args).items()
    }


def git_metadata() -> dict[str, str | bool]:
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        dirty = bool(
            subprocess.check_output(
                ["git", "status", "--porcelain"],
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
        )
    except (OSError, subprocess.CalledProcessError):
        sha = "unknown"
        dirty = "unknown"
    return {"git_sha": sha, "git_dirty": dirty}


def metric_model_key(model_name: str) -> str:
    return (
        model_name.lower()
        .replace("/", "_")
        .replace("&", "and")
        .replace("-", "_")
        .replace(" ", "_")
    )


def log_mlflow_focus_params(args: argparse.Namespace, run_summary: dict) -> None:
    """Expose the recommendation experiment knobs that matter in MLflow tables."""
    focus_params = {
        "exp_data_history_start": args.history_start,
        "exp_data_history_end": args.history_end,
        "exp_data_rank_start": args.rank_start,
        "exp_data_rank_end": args.rank_end,
        "exp_data_test_start": args.test_start,
        "exp_data_test_end": args.test_end,
        "exp_data_max_items": args.max_items,
        "exp_data_rank_users": args.rank_users,
        "exp_data_eval_users": args.eval_users,
        "exp_candidate_k": args.candidate_k,
        "exp_candidate_hybrid_extra": args.hybrid_extra,
        "exp_candidate_recent_cap": args.recent_candidate_cap,
        "exp_candidate_popular_cap": args.popular_candidate_cap,
        "exp_candidate_related_cap": args.related_candidate_cap,
        "exp_candidate_related_top_per_anchor": args.related_top_per_anchor,
        "exp_candidate_related_max_seen_anchors": args.related_max_seen_anchors,
        "exp_candidate_related_anchor_count": run_summary.get("related_anchor_count"),
        "exp_ranker_retrieval_model": args.retrieval_model,
        "exp_ranker_factors": args.factors,
        "exp_ranker_iterations": args.iterations,
        "exp_ranker_lgbm_num_leaves": args.lgbm_num_leaves,
        "exp_ranker_lgbm_learning_rate": args.lgbm_learning_rate,
        "exp_ranker_lgbm_min_child_samples": args.lgbm_min_child_samples,
        "exp_ranker_lgbm_n_estimators": args.lgbm_estimators,
        "exp_ranker_lgbm_colsample": args.lgbm_colsample,
        "exp_run_use_marts": run_summary.get("use_marts"),
        "exp_run_feature_source": run_summary.get("feature_source"),
        "exp_run_save_user_diagnostics": args.save_user_diagnostics,
    }
    for key, value in focus_params.items():
        if value is not None:
            mlflow.log_param(
                key,
                str(value)
                if isinstance(value, (date, Path, tuple, list))
                else value,
            )


def dominant_event(df: pd.DataFrame) -> pd.Series:
    event_cols = [
        "user_watch_share",
        "user_pr_share",
        "user_fork_share",
        "user_push_share",
        "user_issue_share",
        "user_comment_share",
    ]
    labels = {col: col.replace("user_", "").replace("_share", "") for col in event_cols}
    values = df[event_cols].fillna(0)
    out = values.idxmax(axis=1).map(labels)
    out.loc[values.sum(axis=1) == 0] = "none"
    return out


def quartile_segment(series: pd.Series) -> pd.Series:
    if len(series) < 4:
        return pd.Series(["all"] * len(series), index=series.index, dtype="object")
    try:
        return pd.qcut(
            series.rank(method="first"),
            4,
            labels=["low", "mid_low", "mid_high", "high"],
        )
    except ValueError:
        return pd.Series(["all"] * len(series), index=series.index, dtype="object")


def diagnostics_segment_summary(diagnostics: pd.DataFrame) -> pd.DataFrame:
    if diagnostics.empty:
        return pd.DataFrame()

    df = diagnostics.copy()
    df["dominant_event"] = dominant_event(df)
    df["activity_bin"] = quartile_segment(df["log_user_total_score"])
    df["recent_bin"] = quartile_segment(df["user_recent_score_share"])

    rows = []
    for group_col in ["dominant_event", "activity_bin", "recent_bin"]:
        grouped = df.groupby(group_col, observed=True)
        for segment, part in grouped:
            rows.append(
                {
                    "segment_group": group_col,
                    "segment": str(segment),
                    "users": int(len(part)),
                    "ts_hit_at_10": float(part["ts_hit@10"].mean()),
                    "als_hit_at_10": float(part["als_hit@10"].mean()),
                    "ts_ndcg_at_10": float(part["ts_ndcg@10"].mean()),
                    "als_ndcg_at_10": float(part["als_ndcg@10"].mean()),
                    "ts_recall_at_100": float(part["ts_recall@100"].mean()),
                    "als_recall_at_100": float(part["als_recall@100"].mean()),
                    "ts_minus_als_ndcg_at_10": float(
                        part["ts_minus_als_ndcg@10"].mean()
                    ),
                    "ts_minus_als_recall_at_100": float(
                        part["ts_minus_als_recall@100"].mean()
                    ),
                    "top10_source_als_share": float(
                        part.get("top10_source_als_share", pd.Series(dtype=float)).mean()
                    ),
                    "top10_source_recent_share": float(
                        part.get("top10_source_recent_share", pd.Series(dtype=float)).mean()
                    ),
                    "top10_source_related_share": float(
                        part.get("top10_source_related_share", pd.Series(dtype=float)).mean()
                    ),
                }
            )
    return pd.DataFrame(rows)


def log_mlflow_segment_metrics(segment_summary: pd.DataFrame) -> None:
    if segment_summary.empty:
        return
    metric_cols = [
        "ts_ndcg_at_10",
        "als_ndcg_at_10",
        "ts_recall_at_100",
        "als_recall_at_100",
        "ts_minus_als_ndcg_at_10",
        "ts_minus_als_recall_at_100",
        "top10_source_related_share",
    ]
    for row in segment_summary.itertuples(index=False):
        segment = str(row.segment).lower().replace(" ", "_")
        group = str(row.segment_group).lower().replace(" ", "_")
        for metric in metric_cols:
            value = getattr(row, metric)
            if pd.notna(value):
                mlflow.log_metric(f"segment_{group}_{segment}_{metric}", float(value))


def log_mlflow_run(
    args: argparse.Namespace,
    suffix: str,
    run_summary: dict,
    results: pd.DataFrame,
    artifact_paths: list[Path],
) -> None:
    if args.no_mlflow:
        return

    mlflow.set_tracking_uri(args.mlflow_tracking_uri)
    mlflow.set_experiment(args.mlflow_experiment)
    with mlflow.start_run(run_name=suffix) as run:
        run_summary["mlflow_run_id"] = run.info.run_id
        for path in artifact_paths:
            if path.name.endswith("_summary.json"):
                path.write_text(json.dumps(run_summary, indent=2), encoding="utf-8")
        mlflow.set_tags(git_metadata())
        mlflow.set_tag("script", "scripts/week6_two_stage_v2.py")
        mlflow.set_tag("feature_source", run_summary["feature_source"])
        mlflow.set_tag("primary_model", "Two-Stage/Fallback")
        mlflow.log_params(jsonable_args(args))
        log_mlflow_focus_params(args, run_summary)
        mlflow.log_param("suffix", suffix)
        mlflow.log_param("feature_count", len(run_summary["feature_names"]))
        mlflow.log_param("actual_rank_rows", run_summary["ranker"]["rank_rows"])
        mlflow.log_param("actual_rank_users", run_summary["ranker"]["rank_users"])
        mlflow.log_param("mlflow_run_id", run.info.run_id)
        mlflow.log_metric("elapsed_min", run_summary["elapsed_min"])
        mlflow.log_metric("eval_users", run_summary["eval_users"])
        for row in results.itertuples(index=False):
            model_key = metric_model_key(str(row.model))
            mlflow.log_metric(f"{model_key}_precision_at_{row.k}", float(row.precision))
            mlflow.log_metric(f"{model_key}_recall_at_{row.k}", float(row.recall))
            mlflow.log_metric(f"{model_key}_ndcg_at_{row.k}", float(row.ndcg))
            mlflow.log_metric(
                f"{model_key}_unique_recommended_at_{row.k}",
                int(row.unique_recommended),
            )
            if row.model == "Two-Stage/Fallback":
                mlflow.log_metric(f"primary_ndcg_at_{row.k}", float(row.ndcg))
                mlflow.log_metric(f"primary_recall_at_{row.k}", float(row.recall))
        segment_path = run_summary["paths"].get("segment_summary")
        if segment_path and Path(segment_path).exists():
            log_mlflow_segment_metrics(pd.read_csv(segment_path))
        for path in artifact_paths:
            if path.exists():
                mlflow.log_artifact(str(path))


def parse_k_values(value: str) -> list[int]:
    k_values = sorted({int(part.strip()) for part in value.split(",") if part.strip()})
    if not k_values or any(k <= 0 for k in k_values):
        raise argparse.ArgumentTypeError("k values must be positive integers")
    return k_values


def apply_best_full_preset(args: argparse.Namespace) -> None:
    """Apply the current canonical full-run configuration."""
    args.max_items = 300_000
    args.candidate_k = 300
    args.hybrid_extra = 200
    args.related_candidate_cap = 80
    args.related_top_per_anchor = 10
    args.related_max_seen_anchors = 20
    args.rank_users = 100_000
    args.eval_users = 10_000_000
    args.qual_users = 300
    args.factors = 96
    args.iterations = 12
    args.als_regularization = 0.03
    args.lgbm_num_leaves = 63
    args.lgbm_min_child_samples = 50
    args.lgbm_colsample = 0.85
    args.lgbm_lambdarank_truncation = 200
    args.save_user_diagnostics = True
    if args.output_suffix is None:
        args.output_suffix = BEST_FULL_SUFFIX


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def parse_event_weights(overrides: list[str] | None) -> dict[str, float]:
    weights = dict(DEFAULT_WEIGHTS)
    for override in overrides or []:
        if "=" not in override:
            raise argparse.ArgumentTypeError(
                f"event weight must use EventType=value format: {override}"
            )
        event_type, raw_weight = override.split("=", 1)
        event_type = event_type.strip()
        if not event_type:
            raise argparse.ArgumentTypeError(f"empty event type in weight override: {override}")
        try:
            weight = float(raw_weight)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                f"invalid numeric weight for {event_type}: {raw_weight}"
            ) from exc
        if weight < 0:
            raise argparse.ArgumentTypeError(f"event weight must be non-negative: {override}")
        weights[event_type] = weight
    return weights


def build_feedback(df: pd.DataFrame, weights: dict[str, float]) -> pd.DataFrame:
    out = df.copy()
    out["score"] = out["type"].map(weights).fillna(0) * out["cnt"]
    fb = out.groupby(["actor_id", "repo_id"], observed=True)["score"].sum().reset_index()
    return fb[fb["score"] > 0]


def empty_activity_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "actor_id": pd.Series(dtype="int64"),
            "repo_id": pd.Series(dtype="int64"),
            "type": pd.Series(dtype="string"),
            "cnt": pd.Series(dtype="int64"),
        }
    )


def empty_feedback_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "actor_id": pd.Series(dtype="int64"),
            "repo_id": pd.Series(dtype="int64"),
            "score": pd.Series(dtype="float32"),
        }
    )


def load_mart_feedback(path: Path, split: str | None = None) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"mart not found: {path}")
    columns = ["actor_id", "repo_id", "weighted_score"]
    if split is not None:
        columns = ["split", *columns]
    df = pd.read_parquet(path, columns=columns)
    if split is not None:
        df = df[df["split"] == split].drop(columns=["split"])
    out = df.rename(columns={"weighted_score": "score"})
    out = out.dropna(subset=["actor_id", "repo_id", "score"])
    return out.astype({"actor_id": "int64", "repo_id": "int64", "score": "float32"})


def should_use_marts(mode: str, mart_dir: Path) -> bool:
    if mode == "never":
        return False
    required = [
        mart_dir / "user_repo_interaction_mart.parquet",
        mart_dir / "user_profile_mart.parquet",
        mart_dir / "repo_feature_mart.parquet",
        mart_dir / "experiment_split_mart.parquet",
        mart_dir / "repo_repo_related_mart.parquet",
    ]
    exists = all(path.exists() for path in required)
    if mode == "always" and not exists:
        missing = [str(path) for path in required if not path.exists()]
        raise FileNotFoundError(f"missing required mart files: {missing}")
    return exists


def feedback_popularity(feedback: pd.DataFrame) -> pd.Series:
    return feedback.groupby("repo_id", observed=True)["score"].sum().sort_values(ascending=False)


def load_related_candidates(
    path: Path,
    item2idx: dict[int, int],
    top_per_anchor: int,
) -> dict[int, list[tuple[int, float]]]:
    if top_per_anchor <= 0 or not path.exists():
        return {}
    related = pd.read_parquet(path, columns=["anchor_repo_id", "rank", "related_repo_id", "score"])
    valid_items = set(item2idx)
    related = related[
        related["anchor_repo_id"].isin(valid_items)
        & related["related_repo_id"].isin(valid_items)
        & (related["rank"] <= top_per_anchor)
    ]
    if related.empty:
        return {}
    out: dict[int, list[tuple[int, float]]] = {}
    for anchor, rows in related.sort_values(["anchor_repo_id", "rank"]).groupby(
        "anchor_repo_id", observed=True
    ):
        out[int(anchor)] = [
            (int(row.related_repo_id), float(row.score))
            for row in rows.itertuples(index=False)
        ]
    return out


def load_related_candidates_from_mart(
    path: Path,
    item2idx: dict[int, int],
    top_per_anchor: int,
) -> dict[int, list[tuple[int, float]]]:
    if top_per_anchor <= 0 or not path.exists():
        return {}
    related = pd.read_parquet(
        path,
        columns=["anchor_repo_id", "rank", "related_repo_id", "cooc_score"],
    ).rename(columns={"cooc_score": "score"})
    valid_items = set(item2idx)
    related = related[
        related["anchor_repo_id"].isin(valid_items)
        & related["related_repo_id"].isin(valid_items)
        & (related["rank"] <= top_per_anchor)
    ]
    if related.empty:
        return {}
    out: dict[int, list[tuple[int, float]]] = {}
    for anchor, rows in related.sort_values(["anchor_repo_id", "rank"]).groupby(
        "anchor_repo_id", observed=True
    ):
        out[int(anchor)] = [
            (int(row.related_repo_id), float(row.score))
            for row in rows.itertuples(index=False)
        ]
    return out


def aggregate_user_activity(feedback: pd.DataFrame) -> dict[int, dict[str, float]]:
    if feedback.empty:
        return {}
    return (
        feedback.groupby("actor_id")
        .agg(user_total_score=("score", "sum"), user_unique_repos=("repo_id", "nunique"))
        .to_dict(orient="index")
    )


def load_feature_marts(
    mart_dir: Path,
    user_ids: set[int] | None = None,
    repo_ids: set[int] | None = None,
) -> dict[str, pd.DataFrame]:
    user_cols = [
        "actor_id",
        "total_score",
        "unique_repos",
        "watch_share",
        "fork_share",
        "pr_share",
        "push_share",
        "issue_share",
        "comment_share",
        "event_entropy",
        "recent_score_share",
        "score_growth_ratio",
    ]
    repo_cols = [
        "repo_id",
        "total_score_7d",
        "unique_users_7d",
        "fork_users_7d",
        "issue_users_7d",
        "pr_users_7d",
        "push_users_7d",
        "watch_users_7d",
        "comment_users_7d",
        "total_score_28d",
        "unique_users_28d",
        "fork_users_28d",
        "issue_users_28d",
        "pr_users_28d",
        "push_users_28d",
        "watch_users_28d",
        "comment_users_28d",
        "total_score_42d",
        "unique_users_42d",
        "fork_users_42d",
        "issue_users_42d",
        "pr_users_42d",
        "push_users_42d",
        "watch_users_42d",
        "comment_users_42d",
        "language",
        "stars",
        "forks",
        "archived",
    ]
    user_profile = pd.read_parquet(mart_dir / "user_profile_mart.parquet", columns=user_cols)
    repo_feature = pd.read_parquet(mart_dir / "repo_feature_mart.parquet", columns=repo_cols)
    if user_ids is not None:
        user_profile = user_profile[user_profile["actor_id"].isin(user_ids)].copy()
    if repo_ids is not None:
        repo_feature = repo_feature[repo_feature["repo_id"].isin(repo_ids)].copy()
    return {"user_profile": user_profile, "repo_feature": repo_feature}


def user_activity_from_profile(profile: pd.DataFrame) -> dict[int, dict[str, float]]:
    if profile.empty:
        return {}
    return (
        profile.set_index("actor_id")[["total_score", "unique_repos"]]
        .rename(columns={"total_score": "user_total_score", "unique_repos": "user_unique_repos"})
        .to_dict(orient="index")
    )


def user_event_from_profile(profile: pd.DataFrame) -> dict[int, dict[str, float]]:
    if profile.empty:
        return {}
    out = pd.DataFrame(index=profile["actor_id"].astype("int64"))
    out["log_user_events"] = np.log1p(profile["total_score"].astype(float).to_numpy())
    out["user_watch_share"] = profile["watch_share"].fillna(0).to_numpy()
    out["user_pr_share"] = profile["pr_share"].fillna(0).to_numpy()
    out["user_fork_share"] = profile["fork_share"].fillna(0).to_numpy()
    out["user_push_share"] = profile["push_share"].fillna(0).to_numpy()
    out["user_issue_share"] = profile["issue_share"].fillna(0).to_numpy()
    out["user_comment_share"] = profile["comment_share"].fillna(0).to_numpy()
    out["user_event_entropy"] = profile["event_entropy"].fillna(0).to_numpy()
    return out.astype(np.float32).to_dict(orient="index")


def user_recent_prior_activity_from_profile(
    profile: pd.DataFrame,
) -> tuple[dict[int, dict[str, float]], dict[int, dict[str, float]]]:
    if profile.empty:
        return {}, {}
    total_score = profile["total_score"].fillna(0).astype(float)
    unique_repos = profile["unique_repos"].fillna(0).astype(float)
    recent_share = profile["recent_score_share"].fillna(0).astype(float).clip(lower=0, upper=1)
    growth = profile["score_growth_ratio"].fillna(0).astype(float)
    recent_score = total_score * recent_share
    denom = growth + 1.0
    prior_score = ((recent_score - growth) / denom.where(denom.abs() > 1e-6, np.nan)).fillna(0)
    prior_score = prior_score.clip(lower=0)
    recent_unique = (unique_repos * recent_share).clip(lower=0)
    prior_unique = (unique_repos - recent_unique).clip(lower=0)

    recent = pd.DataFrame(
        {
            "actor_id": profile["actor_id"].astype("int64"),
            "user_total_score": recent_score,
            "user_unique_repos": recent_unique,
        }
    )
    prior = pd.DataFrame(
        {
            "actor_id": profile["actor_id"].astype("int64"),
            "user_total_score": prior_score,
            "user_unique_repos": prior_unique,
        }
    )
    return (
        recent.set_index("actor_id").astype(np.float32).to_dict(orient="index"),
        prior.set_index("actor_id").astype(np.float32).to_dict(orient="index"),
    )


def repo_event_from_feature_mart(repo_feature: pd.DataFrame, days: int) -> dict[int, dict[str, float]]:
    if repo_feature.empty:
        return {}
    cols = {
        "watch": f"watch_users_{days}d",
        "pr": f"pr_users_{days}d",
        "fork": f"fork_users_{days}d",
        "push": f"push_users_{days}d",
        "issue": f"issue_users_{days}d",
        "comment": f"comment_users_{days}d",
    }
    counts = repo_feature[list(cols.values())].fillna(0).astype(float)
    total = counts.sum(axis=1).replace(0, 1.0)
    out = pd.DataFrame(index=repo_feature["repo_id"].astype("int64"))
    out["log_item_events"] = np.log1p(repo_feature[f"total_score_{days}d"].fillna(0).astype(float))
    out["item_watch_share"] = counts[cols["watch"]] / total
    out["item_pr_share"] = counts[cols["pr"]] / total
    out["item_fork_share"] = counts[cols["fork"]] / total
    out["item_push_share"] = counts[cols["push"]] / total
    out["item_issue_share"] = counts[cols["issue"]] / total
    out["item_comment_share"] = counts[cols["comment"]] / total
    out["item_pr_per_user_event"] = counts[cols["pr"]] / total
    out["item_fork_per_user_event"] = counts[cols["fork"]] / total
    out["item_issue_comment_ratio"] = counts[cols["comment"]] / (counts[cols["issue"]] + 1.0)
    return out.astype(np.float32).to_dict(orient="index")


def repo_metadata_from_feature_mart(
    repo_feature: pd.DataFrame,
) -> tuple[dict[int, dict[str, float]], dict[str, int]]:
    if repo_feature.empty:
        return {}, {}
    languages = sorted(
        lang for lang in repo_feature["language"].dropna().unique() if isinstance(lang, str)
    )
    lang2idx = {lang: i + 1 for i, lang in enumerate(languages)}
    out = pd.DataFrame(index=repo_feature["repo_id"].astype("int64"))
    out["log_stars"] = np.log1p(repo_feature["stars"].fillna(0).astype(float))
    out["log_forks"] = np.log1p(repo_feature["forks"].fillna(0).astype(float))
    out["language_idx"] = repo_feature["language"].map(lang2idx).fillna(0).astype(float).to_numpy()
    out["archived"] = repo_feature["archived"].fillna(0).astype(float).to_numpy()
    return out.astype(np.float32).to_dict(orient="index"), lang2idx


def repo_score_series_from_feature_mart(
    repo_feature: pd.DataFrame,
    item2idx: dict[int, int],
    column: str,
) -> pd.Series:
    if repo_feature.empty or column not in repo_feature:
        return pd.Series(dtype="float64")
    scores = (
        repo_feature[repo_feature["repo_id"].isin(item2idx)]
        .set_index("repo_id")[column]
        .fillna(0)
        .astype(float)
    )
    return scores[scores > 0].sort_values(ascending=False)


def rank_percentiles(scores: pd.Series, item2idx: dict[int, int]) -> dict[int, float]:
    if scores.empty:
        return {}
    valid = scores[scores.index.isin(item2idx)]
    n_items = max(len(item2idx), 1)
    return {int(repo_id): float(rank / n_items) for rank, repo_id in enumerate(valid.index, start=1)}


def event_stats(df: pd.DataFrame) -> tuple[dict[int, dict[str, float]], dict[int, dict[str, float]]]:
    """Build compact user/item event-mix stats from daily aggregate rows."""
    if df.empty:
        return {}, {}
    by_item = df.groupby(["repo_id", "type"], observed=True)["cnt"].sum().unstack(fill_value=0)
    item_total = by_item.sum(axis=1).replace(0, 1)
    item = pd.DataFrame(index=by_item.index)
    item["log_item_events"] = np.log1p(item_total)
    item["item_watch_share"] = by_item.get("WatchEvent", 0) / item_total
    item["item_pr_share"] = by_item.get("PullRequestEvent", 0) / item_total
    item["item_fork_share"] = by_item.get("ForkEvent", 0) / item_total
    item["item_push_share"] = by_item.get("PushEvent", 0) / item_total
    item["item_issue_share"] = by_item.get("IssuesEvent", 0) / item_total
    item["item_comment_share"] = (
        by_item.get("IssueCommentEvent", 0) + by_item.get("CommitCommentEvent", 0)
    ) / item_total
    item["item_pr_per_user_event"] = by_item.get("PullRequestEvent", 0) / item_total
    item["item_fork_per_user_event"] = by_item.get("ForkEvent", 0) / item_total
    item["item_issue_comment_ratio"] = by_item.get("IssueCommentEvent", 0) / (
        by_item.get("IssuesEvent", 0) + 1
    )

    by_user = df.groupby(["actor_id", "type"], observed=True)["cnt"].sum().unstack(fill_value=0)
    user_total = by_user.sum(axis=1).replace(0, 1)
    user = pd.DataFrame(index=by_user.index)
    user["log_user_events"] = np.log1p(user_total)
    user["user_watch_share"] = by_user.get("WatchEvent", 0) / user_total
    user["user_pr_share"] = by_user.get("PullRequestEvent", 0) / user_total
    user["user_fork_share"] = by_user.get("ForkEvent", 0) / user_total
    user["user_push_share"] = by_user.get("PushEvent", 0) / user_total
    user["user_issue_share"] = by_user.get("IssuesEvent", 0) / user_total
    user["user_comment_share"] = (
        by_user.get("IssueCommentEvent", 0) + by_user.get("CommitCommentEvent", 0)
    ) / user_total
    return (
        item.astype(np.float32).to_dict(orient="index"),
        user.astype(np.float32).to_dict(orient="index"),
    )


def load_metadata(db_path: Path) -> tuple[dict[int, dict[str, float]], dict[str, int]]:
    if not db_path.exists():
        return {}, {}
    conn = sqlite3.connect(str(db_path))
    df = pd.read_sql_query(
        """
        SELECT repo_id, language, stargazers, forks, archived
        FROM repo_metadata
        WHERE http_status = 200
        """,
        conn,
    )
    conn.close()
    if df.empty:
        return {}, {}
    languages = sorted(lang for lang in df["language"].dropna().unique() if isinstance(lang, str))
    lang2idx = {lang: i + 1 for i, lang in enumerate(languages)}
    meta = {}
    for row in df.itertuples(index=False):
        meta[int(row.repo_id)] = {
            "log_stars": float(np.log1p(row.stargazers or 0)),
            "log_forks": float(np.log1p(row.forks or 0)),
            "language_idx": float(lang2idx.get(row.language, 0)),
            "archived": float(row.archived or 0),
        }
    return meta, lang2idx


def filter_catalog(
    history_fb: pd.DataFrame,
    rank_fb: pd.DataFrame,
    test_fb: pd.DataFrame,
    min_item_users: int,
    min_user_items: int,
    max_items: int | None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    item_users = history_fb.groupby("repo_id")["actor_id"].nunique()
    keep_items = set(item_users[item_users >= min_item_users].index)
    if max_items:
        top_items = (
            history_fb[history_fb["repo_id"].isin(keep_items)]
            .groupby("repo_id")["score"]
            .sum()
            .sort_values(ascending=False)
            .head(max_items)
            .index
        )
        keep_items = set(top_items)

    history_fb = history_fb[history_fb["repo_id"].isin(keep_items)]
    user_items = history_fb.groupby("actor_id")["repo_id"].nunique()
    keep_users = set(user_items[user_items >= min_user_items].index)

    history_fb = history_fb[history_fb["actor_id"].isin(keep_users)]
    rank_fb = rank_fb[rank_fb["repo_id"].isin(keep_items)]
    test_fb = test_fb[test_fb["repo_id"].isin(keep_items)]
    return history_fb, rank_fb, test_fb


def sample_users(
    history_fb: pd.DataFrame,
    rank_fb: pd.DataFrame,
    test_fb: pd.DataFrame,
    ratio: float,
    seed: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if ratio >= 1:
        return history_fb, rank_fb, test_fb
    rng = np.random.default_rng(seed)
    users = np.array(sorted(set(history_fb["actor_id"])))
    n = max(1, int(len(users) * ratio))
    sampled = set(rng.choice(users, size=n, replace=False))
    return (
        history_fb[history_fb["actor_id"].isin(sampled)],
        rank_fb[rank_fb["actor_id"].isin(sampled)],
        test_fb[test_fb["actor_id"].isin(sampled)],
    )


def make_matrix(feedback: pd.DataFrame):
    user_ids = feedback["actor_id"].unique()
    item_ids = feedback["repo_id"].unique()
    user2idx = {uid: i for i, uid in enumerate(user_ids)}
    item2idx = {iid: i for i, iid in enumerate(item_ids)}
    idx2item = {i: iid for iid, i in item2idx.items()}

    rows = feedback["actor_id"].map(user2idx).to_numpy()
    cols = feedback["repo_id"].map(item2idx).to_numpy()
    data = feedback["score"].astype(np.float32).to_numpy()
    mat = sparse.csr_matrix((data, (rows, cols)), shape=(len(user_ids), len(item_ids)))
    return mat, user2idx, item2idx, idx2item


def recommend_batch(
    model: AlternatingLeastSquares | BayesianPersonalizedRanking,
    matrix: sparse.csr_matrix,
    user2idx: dict[int, int],
    idx2item: dict[int, int],
    users: list[int],
    candidate_k: int,
    chunk_size: int,
) -> dict[int, list[tuple[int, float]]]:
    out = {}
    valid_users = [u for u in users if u in user2idx]
    for start in tqdm(range(0, len(valid_users), chunk_size), desc="retrieval"):
        chunk = valid_users[start : start + chunk_size]
        idxs = np.array([user2idx[u] for u in chunk])
        item_idxs, scores = model.recommend(
            idxs,
            matrix[idxs],
            N=candidate_k,
            filter_already_liked_items=True,
        )
        for row_idx, uid in enumerate(chunk):
            out[uid] = [
                (idx2item[int(iidx)], float(scores[row_idx][rank]))
                for rank, iidx in enumerate(item_idxs[row_idx])
            ]
    return out


def hybridize_candidates(
    als_retrieval: dict[int, list[tuple[int, float]]],
    users: list[int],
    popularity_candidates: list[int],
    recent_candidates: list[int],
    train_seen: dict[int, set[int]],
    item2idx: dict[int, int],
    max_candidates: int,
    recent_candidate_cap: int | None = None,
    popular_candidate_cap: int | None = None,
    related_candidates: dict[int, list[tuple[int, float]]] | None = None,
    related_candidate_cap: int = 0,
    related_seed_items: dict[int, list[int]] | None = None,
) -> dict[int, list[tuple[int, float, int]]]:
    """Union ALS candidates with related, popularity, and recent-pop hard negatives."""
    out = {}
    valid_items = set(item2idx)
    for uid in users:
        seen = train_seen.get(uid, set())
        merged = {}
        recent_added = 0
        popular_added = 0
        for rid, score in als_retrieval.get(uid, []):
            if rid in valid_items and rid not in seen:
                merged[rid] = (float(score), 1)
        if related_candidates and related_candidate_cap > 0:
            related_scores: defaultdict[int, float] = defaultdict(float)
            anchors = related_seed_items.get(uid, []) if related_seed_items else list(seen)
            for seen_rid in anchors:
                for rid, score in related_candidates.get(seen_rid, []):
                    if rid in valid_items and rid not in seen and rid not in merged:
                        related_scores[rid] += float(score)
            for rank, (rid, score) in enumerate(
                sorted(related_scores.items(), key=lambda x: x[1], reverse=True)
            ):
                if len(merged) >= max_candidates or rank >= related_candidate_cap:
                    break
                merged[rid] = (float(score), 4)
        for rank, rid in enumerate(recent_candidates):
            if len(merged) >= max_candidates:
                break
            if recent_candidate_cap is not None and recent_added >= recent_candidate_cap:
                break
            if rid in valid_items and rid not in seen and rid not in merged:
                merged[rid] = (-0.001 * (rank + 1), 2)
                recent_added += 1
        for rank, rid in enumerate(popularity_candidates):
            if len(merged) >= max_candidates:
                break
            if popular_candidate_cap is not None and popular_added >= popular_candidate_cap:
                break
            if rid in valid_items and rid not in seen and rid not in merged:
                merged[rid] = (-0.001 * (rank + 1), 3)
                popular_added += 1
        out[uid] = [(rid, score, source) for rid, (score, source) in merged.items()][:max_candidates]
    return out


def precision_recall_ndcg(recommended: list[int], relevant: set[int], k: int):
    recs = recommended[:k]
    hits = set(recs) & relevant
    precision = len(hits) / k
    recall = len(hits) / len(relevant) if relevant else 0.0
    dcg = sum(1.0 / math.log2(i + 2) for i, rid in enumerate(recs) if rid in relevant)
    idcg = sum(1.0 / math.log2(i + 2) for i in range(min(len(relevant), k)))
    ndcg = dcg / idcg if idcg else 0.0
    return precision, recall, ndcg


def source_mix_for_recs(
    recommended: list[int],
    source_map: dict[int, int],
    k: int,
) -> dict[str, float]:
    recs = recommended[:k]
    denom = max(len(recs), 1)
    return {
        f"top{k}_source_als_share": sum(1 for rid in recs if source_map.get(rid) == 1) / denom,
        f"top{k}_source_recent_share": sum(1 for rid in recs if source_map.get(rid) == 2) / denom,
        f"top{k}_source_popular_share": sum(1 for rid in recs if source_map.get(rid) == 3) / denom,
        f"top{k}_source_related_share": sum(1 for rid in recs if source_map.get(rid) == 4) / denom,
    }


def build_feature_context(
    history_df: pd.DataFrame,
    recent_df: pd.DataFrame,
    prior_df: pd.DataFrame,
    history_fb: pd.DataFrame,
    recent_fb: pd.DataFrame,
    prior_fb: pd.DataFrame,
    model: AlternatingLeastSquares | BayesianPersonalizedRanking,
    user2idx: dict[int, int],
    item2idx: dict[int, int],
    feature_marts: dict[str, pd.DataFrame] | None = None,
):
    pop_series = feedback_popularity(history_fb)
    recent_pop_series = feedback_popularity(recent_fb) if len(recent_fb) else pd.Series(dtype=float)
    prior_pop_series = feedback_popularity(prior_fb) if len(prior_fb) else pd.Series(dtype=float)
    user_activity = aggregate_user_activity(history_fb)
    recent_user_activity = aggregate_user_activity(recent_fb)
    prior_user_activity = aggregate_user_activity(prior_fb)
    item_user_counts = history_fb.groupby("repo_id")["actor_id"].nunique().to_dict()
    recent_item_users = recent_fb.groupby("repo_id")["actor_id"].nunique().to_dict()
    prior_item_users = prior_fb.groupby("repo_id")["actor_id"].nunique().to_dict()
    user_seen_iidxs = (
        history_fb.assign(item_idx=history_fb["repo_id"].map(item2idx))
        .dropna(subset=["item_idx"])
        .groupby("actor_id")["item_idx"]
        .apply(lambda s: s.astype(np.int32).to_numpy())
        .to_dict()
    )
    if feature_marts:
        item_event, user_event, recent_item_event = {}, {}, {}
        meta, lang2idx = {}, {}
        feature_source = "mart"
        user_profile = feature_marts.get("user_profile", pd.DataFrame())
        repo_feature = feature_marts.get("repo_feature", pd.DataFrame())
        if not user_profile.empty:
            user_activity = user_activity_from_profile(user_profile)
            user_event = user_event_from_profile(user_profile)
            recent_user_activity, prior_user_activity = user_recent_prior_activity_from_profile(
                user_profile
            )
        if not repo_feature.empty:
            repo_feature = repo_feature[repo_feature["repo_id"].isin(item2idx)].copy()
            item_user_counts = repo_feature.set_index("repo_id")["unique_users_42d"].fillna(0).to_dict()
            recent_item_users = repo_feature.set_index("repo_id")["unique_users_7d"].fillna(0).to_dict()
            prior_item_users = repo_feature.set_index("repo_id")["unique_users_28d"].fillna(0).to_dict()
            item_event = repo_event_from_feature_mart(repo_feature, 42)
            recent_item_event = repo_event_from_feature_mart(repo_feature, 7)
            meta, lang2idx = repo_metadata_from_feature_mart(repo_feature)
            pop_series = repo_score_series_from_feature_mart(repo_feature, item2idx, "total_score_42d")
            recent_pop_series = repo_score_series_from_feature_mart(
                repo_feature, item2idx, "total_score_7d"
            )
            prior_pop_series = (
                repo_feature[repo_feature["repo_id"].isin(item2idx)]
                .assign(
                    prior_score=lambda df: (
                        df["total_score_28d"].fillna(0).astype(float)
                        - df["total_score_7d"].fillna(0).astype(float)
                    ).clip(lower=0)
                )
                .set_index("repo_id")["prior_score"]
                .sort_values(ascending=False)
            )
    else:
        item_event, user_event = event_stats(history_df)
        recent_item_event, _ = event_stats(recent_df) if len(recent_df) else ({}, {})
        meta, lang2idx = load_metadata(DB_PATH)
        feature_source = "raw"

    pop = pop_series.to_dict()
    recent_pop = recent_pop_series.to_dict()
    prior_pop = prior_pop_series.to_dict()
    pop_rank_pct = rank_percentiles(pop_series, item2idx)
    recent_rank_pct = rank_percentiles(recent_pop_series, item2idx)

    user_factors = model.user_factors.astype(np.float32)
    item_factors = model.item_factors.astype(np.float32)
    user_norms = np.linalg.norm(user_factors, axis=1, keepdims=True)
    item_norms = np.linalg.norm(item_factors, axis=1, keepdims=True)
    user_norms[user_norms == 0] = 1.0
    item_norms[item_norms == 0] = 1.0
    item_normed = item_factors / item_norms

    user_profile = np.zeros_like(user_factors, dtype=np.float32)
    weighted_seen = (
        history_fb.assign(
            user_idx=history_fb["actor_id"].map(user2idx),
            item_idx=history_fb["repo_id"].map(item2idx),
        )
        .dropna(subset=["user_idx", "item_idx"])
        .sort_values(["actor_id", "score"], ascending=[True, False])
    )
    for uid, rows in weighted_seen.groupby("actor_id", observed=True):
        uidx = user2idx.get(uid)
        if uidx is None:
            continue
        rows = rows.head(100)
        idxs = rows["item_idx"].astype(np.int32).to_numpy()
        weights = np.log1p(rows["score"].astype(np.float32).to_numpy())
        weight_sum = float(weights.sum())
        if weight_sum > 0:
            user_profile[uidx] = (item_normed[idxs] * weights[:, None]).sum(axis=0) / weight_sum
    profile_norms = np.linalg.norm(user_profile, axis=1, keepdims=True)
    profile_norms[profile_norms == 0] = 1.0

    item_feature_names = [
        "log_popularity",
        "log_item_user_count",
        "log_recent_popularity",
        "log_recent_item_user_count",
        "log_prior_popularity",
        "log_prior_item_user_count",
        "recent_pop_share",
        "recent_user_share",
        "pop_growth",
        "pop_growth_ratio",
        "pop_rank_pct",
        "recent_rank_pct",
        "log_item_events",
        "item_watch_share",
        "item_pr_share",
        "item_fork_share",
        "item_push_share",
        "item_issue_share",
        "item_comment_share",
        "item_pr_per_user",
        "item_fork_per_user",
        "item_issue_comment_ratio",
        "recent_item_watch_share",
        "recent_item_pr_share",
        "recent_item_fork_share",
        "recent_item_push_share",
        "recent_item_issue_share",
        "recent_item_comment_share",
        "log_stars",
        "log_forks",
        "language_idx",
        "archived",
    ]
    item_static = np.zeros((len(item2idx), len(item_feature_names)), dtype=np.float32)
    for repo_id, iidx in item2idx.items():
        i_event = item_event.get(repo_id, {})
        r_event = recent_item_event.get(repo_id, {})
        i_meta = meta.get(repo_id, {})
        pop_value = float(pop.get(repo_id, 0.0))
        recent_value = float(recent_pop.get(repo_id, 0.0))
        prior_value = float(prior_pop.get(repo_id, 0.0))
        item_users = float(item_user_counts.get(repo_id, 0))
        recent_users = float(recent_item_users.get(repo_id, 0))
        prior_users = float(prior_item_users.get(repo_id, 0))
        log_recent = np.log1p(recent_value)
        log_prior = np.log1p(prior_value)
        item_static[iidx] = np.array(
            [
                np.log1p(pop_value),
                np.log1p(item_users),
                log_recent,
                np.log1p(recent_users),
                log_prior,
                np.log1p(prior_users),
                recent_value / (pop_value + 1e-6),
                recent_users / (item_users + 1e-6),
                log_recent - log_prior,
                (recent_value - prior_value) / (prior_value + 1.0),
                pop_rank_pct.get(repo_id, 1.0),
                recent_rank_pct.get(repo_id, 1.0),
                i_event.get("log_item_events", 0.0),
                i_event.get("item_watch_share", 0.0),
                i_event.get("item_pr_share", 0.0),
                i_event.get("item_fork_share", 0.0),
                i_event.get("item_push_share", 0.0),
                i_event.get("item_issue_share", 0.0),
                i_event.get("item_comment_share", 0.0),
                pop_value / (item_users + 1.0),
                i_event.get("item_fork_share", 0.0) * pop_value / (item_users + 1.0),
                i_event.get("item_issue_comment_ratio", 0.0),
                r_event.get("item_watch_share", 0.0),
                r_event.get("item_pr_share", 0.0),
                r_event.get("item_fork_share", 0.0),
                r_event.get("item_push_share", 0.0),
                r_event.get("item_issue_share", 0.0),
                r_event.get("item_comment_share", 0.0),
                i_meta.get("log_stars", 0.0),
                i_meta.get("log_forks", 0.0),
                i_meta.get("language_idx", 0.0),
                i_meta.get("archived", 0.0),
            ],
            dtype=np.float32,
        )

    return {
        "pop": pop,
        "recent_pop": recent_pop,
        "user_activity": user_activity,
        "recent_user_activity": recent_user_activity,
        "prior_user_activity": prior_user_activity,
        "user_event": user_event,
        "user_seen_iidxs": user_seen_iidxs,
        "user_normed": user_factors / user_norms,
        "item_normed": item_normed,
        "user_profile_normed": user_profile / profile_norms,
        "item_static": item_static,
        "item_feature_names": item_feature_names,
        "lang2idx": lang2idx,
        "feature_source": feature_source,
    }


def features_for_candidates(
    uid: int,
    candidates: list[tuple],
    user2idx: dict[int, int],
    item2idx: dict[int, int],
    context: dict,
) -> tuple[np.ndarray, list[int]]:
    uidx = user2idx.get(uid)
    if uidx is None or not candidates:
        return np.empty((0, 0), dtype=np.float32), []

    repo_ids, iidxs, als_scores, sources, original_ranks = [], [], [], [], []
    for cand_rank, cand in enumerate(candidates, start=1):
        if len(cand) == 2:
            rid, score = cand
            source = 1
        else:
            rid, score, source = cand
        iidx = item2idx.get(rid)
        if iidx is None:
            continue
        repo_ids.append(rid)
        iidxs.append(iidx)
        als_scores.append(score)
        sources.append(source)
        original_ranks.append(cand_rank)
    if not repo_ids:
        return np.empty((0, 0), dtype=np.float32), []

    n = len(repo_ids)
    iidxs_arr = np.array(iidxs)
    user_activity = context["user_activity"].get(
        uid, {"user_total_score": 0.0, "user_unique_repos": 0.0}
    )
    recent_user_activity = context["recent_user_activity"].get(
        uid, {"user_total_score": 0.0, "user_unique_repos": 0.0}
    )
    prior_user_activity = context["prior_user_activity"].get(
        uid, {"user_total_score": 0.0, "user_unique_repos": 0.0}
    )
    user_event = context["user_event"].get(uid, {})
    cos = context["user_normed"][uidx] @ context["item_normed"][iidxs_arr].T
    profile_cos = context["user_profile_normed"][uidx] @ context["item_normed"][iidxs_arr].T
    item_features = context["item_static"][iidxs_arr]
    item_feature_names = context["item_feature_names"]
    feature_idx = {name: i for i, name in enumerate(item_feature_names)}

    item_watch = item_features[:, feature_idx["item_watch_share"]]
    item_pr = item_features[:, feature_idx["item_pr_share"]]
    item_fork = item_features[:, feature_idx["item_fork_share"]]
    item_push = item_features[:, feature_idx["item_push_share"]]
    item_issue = item_features[:, feature_idx["item_issue_share"]]
    item_comment = item_features[:, feature_idx["item_comment_share"]]
    user_watch = float(user_event.get("user_watch_share", 0.0))
    user_pr = float(user_event.get("user_pr_share", 0.0))
    user_fork = float(user_event.get("user_fork_share", 0.0))
    user_push = float(user_event.get("user_push_share", 0.0))
    user_issue = float(user_event.get("user_issue_share", 0.0))
    user_comment = float(user_event.get("user_comment_share", 0.0))
    event_match_dot = (
        item_watch * user_watch
        + item_pr * user_pr
        + item_fork * user_fork
        + item_push * user_push
        + item_issue * user_issue
        + item_comment * user_comment
    )
    event_l1_distance = (
        np.abs(item_watch - user_watch)
        + np.abs(item_pr - user_pr)
        + np.abs(item_fork - user_fork)
        + np.abs(item_push - user_push)
        + np.abs(item_issue - user_issue)
        + np.abs(item_comment - user_comment)
    )
    user_event_mix = np.array(
        [user_watch, user_pr, user_fork, user_push, user_issue, user_comment], dtype=np.float32
    )
    event_entropy = float(
        -(user_event_mix[user_event_mix > 0] * np.log(user_event_mix[user_event_mix > 0])).sum()
    )

    source_arr = np.array(sources, dtype=np.int8)
    source_is_als = (source_arr == 1).astype(np.float32)
    source_is_recent = (source_arr == 2).astype(np.float32)
    source_is_popular = (source_arr == 3).astype(np.float32)
    source_is_related = (source_arr == 4).astype(np.float32)
    candidate_rank = np.array(original_ranks, dtype=np.float32)
    candidate_rank_pct = candidate_rank / max(len(candidates), 1)
    reciprocal_candidate_rank = 1.0 / candidate_rank

    seen_iidxs = context["user_seen_iidxs"].get(uid)
    if seen_iidxs is not None and len(seen_iidxs):
        seen_iidxs = seen_iidxs[:100]
        sims = context["item_normed"][seen_iidxs] @ context["item_normed"][iidxs_arr].T
        seen_max_cos = sims.max(axis=0)
        seen_mean_cos = sims.mean(axis=0)
    else:
        seen_max_cos = np.zeros(n, dtype=np.float32)
        seen_mean_cos = np.zeros(n, dtype=np.float32)

    x = np.column_stack(
        [
            np.array(als_scores, dtype=np.float32),
            cos.astype(np.float32),
            profile_cos.astype(np.float32),
            item_features,
            np.full(n, np.log1p(user_activity["user_total_score"]), dtype=np.float32),
            np.full(n, np.log1p(user_activity["user_unique_repos"]), dtype=np.float32),
            np.full(n, np.log1p(recent_user_activity["user_total_score"]), dtype=np.float32),
            np.full(n, np.log1p(recent_user_activity["user_unique_repos"]), dtype=np.float32),
            np.full(n, np.log1p(prior_user_activity["user_total_score"]), dtype=np.float32),
            np.full(n, np.log1p(prior_user_activity["user_unique_repos"]), dtype=np.float32),
            np.full(
                n,
                recent_user_activity["user_total_score"]
                / (user_activity["user_total_score"] + 1e-6),
                dtype=np.float32,
            ),
            np.full(
                n,
                (recent_user_activity["user_total_score"] - prior_user_activity["user_total_score"])
                / (prior_user_activity["user_total_score"] + 1.0),
                dtype=np.float32,
            ),
            np.full(
                n,
                (recent_user_activity["user_unique_repos"] - prior_user_activity["user_unique_repos"])
                / (prior_user_activity["user_unique_repos"] + 1.0),
                dtype=np.float32,
            ),
            np.full(n, user_event.get("log_user_events", 0.0), dtype=np.float32),
            np.full(n, user_event.get("user_watch_share", 0.0), dtype=np.float32),
            np.full(n, user_event.get("user_pr_share", 0.0), dtype=np.float32),
            np.full(n, user_event.get("user_fork_share", 0.0), dtype=np.float32),
            np.full(n, user_event.get("user_push_share", 0.0), dtype=np.float32),
            np.full(n, user_event.get("user_issue_share", 0.0), dtype=np.float32),
            np.full(n, user_event.get("user_comment_share", 0.0), dtype=np.float32),
            np.full(n, event_entropy, dtype=np.float32),
            event_match_dot.astype(np.float32),
            event_l1_distance.astype(np.float32),
            seen_max_cos.astype(np.float32),
            seen_mean_cos.astype(np.float32),
            candidate_rank.astype(np.float32),
            candidate_rank_pct.astype(np.float32),
            reciprocal_candidate_rank.astype(np.float32),
            source_is_als,
            source_is_recent,
            source_is_popular,
            source_is_related,
            source_is_recent * user_push,
            source_is_recent * user_issue,
            source_is_recent * user_pr,
            source_is_recent * np.log1p(user_activity["user_unique_repos"]),
            source_is_recent * event_l1_distance,
            source_is_related * np.log1p(user_activity["user_unique_repos"]),
            source_is_related * seen_max_cos,
            source_is_als * user_watch,
            source_is_als * user_fork,
            source_is_als * profile_cos,
            source_is_popular * user_push,
            source_is_popular * candidate_rank_pct,
        ]
    )
    return x.astype(np.float32), repo_ids


def feature_names_for_context(context: dict) -> list[str]:
    return [
        "als_score",
        "factor_cosine",
        "profile_cosine",
        *context["item_feature_names"],
        "log_user_total_score",
        "log_user_unique_repos",
        "log_recent_user_total_score",
        "log_recent_user_unique_repos",
        "log_prior_user_total_score",
        "log_prior_user_unique_repos",
        "user_recent_score_share",
        "user_score_growth_ratio",
        "user_unique_repo_growth_ratio",
        "log_user_events",
        "user_watch_share",
        "user_pr_share",
        "user_fork_share",
        "user_push_share",
        "user_issue_share",
        "user_comment_share",
        "user_event_entropy",
        "event_match_dot",
        "event_l1_distance",
        "seen_max_cosine",
        "seen_mean_cosine",
        "candidate_rank",
        "candidate_rank_pct",
        "reciprocal_candidate_rank",
        "source_is_als",
        "source_is_recent",
        "source_is_popular",
        "source_is_related",
        "source_recent_x_user_push",
        "source_recent_x_user_issue",
        "source_recent_x_user_pr",
        "source_recent_x_log_user_unique_repos",
        "source_recent_x_event_l1_distance",
        "source_related_x_log_user_unique_repos",
        "source_related_x_seen_max_cosine",
        "source_als_x_user_watch",
        "source_als_x_user_fork",
        "source_als_x_profile_cosine",
        "source_popular_x_user_push",
        "source_popular_x_candidate_rank_pct",
    ]


def train_ranker(
    retrieval: dict[int, list[tuple[int, float]]],
    labels_by_user: dict[int, set[int]],
    user2idx: dict[int, int],
    item2idx: dict[int, int],
    context: dict,
    max_rank_users: int,
    seed: int,
    lgbm_params: dict,
):
    rng = np.random.default_rng(seed)
    users = [u for u in retrieval if labels_by_user.get(u)]
    if len(users) > max_rank_users:
        users = list(rng.choice(np.array(users), size=max_rank_users, replace=False))

    xs, ys, groups = [], [], []
    positive_labels = 0
    for uid in tqdm(users, desc="rank data"):
        x, repo_ids = features_for_candidates(uid, retrieval[uid], user2idx, item2idx, context)
        if len(repo_ids) == 0:
            continue
        y = np.array([1 if rid in labels_by_user[uid] else 0 for rid in repo_ids], dtype=np.int32)
        if y.sum() == 0:
            continue
        xs.append(x)
        ys.append(y)
        groups.append(len(y))
        positive_labels += int(y.sum())

    if not xs:
        raise RuntimeError("No positive ranker labels found in ALS candidates.")

    x_train = np.vstack(xs)
    y_train = np.concatenate(ys)
    ranker = lgb.LGBMRanker(
        objective="lambdarank",
        metric="ndcg",
        n_estimators=lgbm_params["n_estimators"],
        learning_rate=lgbm_params["learning_rate"],
        num_leaves=lgbm_params["num_leaves"],
        min_child_samples=lgbm_params["min_child_samples"],
        subsample=lgbm_params["subsample"],
        colsample_bytree=lgbm_params["colsample_bytree"],
        reg_lambda=lgbm_params["reg_lambda"],
        lambdarank_truncation_level=lgbm_params["lambdarank_truncation_level"],
        random_state=seed,
        verbose=-1,
    )
    ranker.fit(x_train, y_train, group=groups)
    return ranker, {
        "rank_users": len(groups),
        "rank_rows": int(len(y_train)),
        "positive_labels": positive_labels,
        "positive_rate": float(y_train.mean()),
    }


def load_ranker_feature_names(parquet_path: Path, summary_path: Path | None) -> list[str]:
    if summary_path is not None:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        names = summary.get("features", {}).get("feature_names")
        if names:
            return list(names)

    return [
        name
        for name in pq.ParquetFile(parquet_path).schema_arrow.names
        if name not in RANKER_FEATURE_META_COLUMNS
    ]


def train_ranker_from_feature_parquet(
    parquet_path: Path,
    summary_path: Path | None,
    expected_feature_names: list[str],
    lgbm_params: dict,
    seed: int,
):
    if summary_path is None:
        candidate_summary = parquet_path.with_name(f"{parquet_path.stem}_summary.json")
        if candidate_summary.exists():
            summary_path = candidate_summary
    feature_names = load_ranker_feature_names(parquet_path, summary_path)
    missing = sorted(set(feature_names) - set(expected_feature_names))
    if missing:
        raise RuntimeError(
            f"ranker feature parquet contains unknown feature columns: {missing[:10]}"
        )

    columns = ["group_index", "label", *feature_names]
    frame = pd.read_parquet(parquet_path, columns=columns)
    if frame.empty:
        raise RuntimeError(f"ranker feature parquet is empty: {parquet_path}")

    groups = frame.groupby("group_index", sort=False, observed=True).size().astype(int).tolist()
    y_train = frame["label"].astype(np.int32).to_numpy()
    x_train = frame[feature_names].astype(np.float32).to_numpy()

    ranker = lgb.LGBMRanker(
        objective="lambdarank",
        metric="ndcg",
        n_estimators=lgbm_params["n_estimators"],
        learning_rate=lgbm_params["learning_rate"],
        num_leaves=lgbm_params["num_leaves"],
        min_child_samples=lgbm_params["min_child_samples"],
        subsample=lgbm_params["subsample"],
        colsample_bytree=lgbm_params["colsample_bytree"],
        reg_lambda=lgbm_params["reg_lambda"],
        lambdarank_truncation_level=lgbm_params["lambdarank_truncation_level"],
        random_state=seed,
        verbose=-1,
    )
    ranker.fit(x_train, y_train, group=groups)
    return ranker, {
        "rank_users": int(len(groups)),
        "rank_rows": int(len(y_train)),
        "positive_labels": int(y_train.sum()),
        "positive_rate": float(y_train.mean()),
        "feature_parquet": str(parquet_path),
        "feature_summary": str(summary_path) if summary_path else None,
    }, feature_names


def evaluate(
    als_retrieval: dict[int, list[tuple[int, float]]],
    ts_retrieval: dict[int, list[tuple[int, float]]],
    test_labels: dict[int, set[int]],
    ranker: lgb.LGBMRanker,
    user2idx: dict[int, int],
    item2idx: dict[int, int],
    context: dict,
    popularity_candidates: list[int],
    fallback_candidates: list[int],
    train_seen: dict[int, set[int]],
    k_values: list[int],
    feature_names: list[str],
    feature_indices: list[int],
    qual_top_k: int,
    qual_users: int,
    seed: int,
    retrieval_label: str,
    save_user_diagnostics: bool,
):
    retrieval_model_name = f"{retrieval_label}/Fallback"
    metrics = {
        name: {k: defaultdict(list) for k in k_values}
        for name in ["Popularity", retrieval_model_name, "Two-Stage/Fallback"]
    }
    coverage = {name: {k: set() for k in k_values} for name in metrics}

    rng = np.random.default_rng(seed)
    eval_users = [u for u in test_labels if test_labels.get(u)]
    qual_user_set = set(eval_users)
    if len(eval_users) > qual_users:
        qual_user_set = set(rng.choice(np.array(eval_users), size=qual_users, replace=False))
    qual_rows = []
    diagnostic_rows = []

    for uid in tqdm(eval_users, desc="eval"):
        relevant = test_labels[uid]
        seen = train_seen.get(uid, set())
        pop_recs = [rid for rid in popularity_candidates if rid not in seen]
        fallback_recs = [rid for rid in fallback_candidates if rid not in seen]
        als_pairs = als_retrieval.get(uid, [])
        ts_pairs = ts_retrieval.get(uid, als_pairs)
        als_recs = [rid for rid, _ in als_pairs] if als_pairs else fallback_recs
        als_score_map = dict(als_pairs)
        ts_source_map = {
            int(cand[0]): int(cand[2]) if len(cand) > 2 else 1
            for cand in ts_pairs
        }

        x_all, repo_ids = features_for_candidates(uid, ts_pairs, user2idx, item2idx, context)
        x = x_all[:, feature_indices] if len(repo_ids) else x_all
        if len(repo_ids):
            scores = ranker.booster_.predict(x)
            ranked_idx = np.argsort(-scores)
            ts_recs = [repo_ids[i] for i in ranked_idx]
            ts_score_map = {repo_ids[i]: float(scores[i]) for i in ranked_idx}
            ts_feature_map = {
                repo_ids[i]: {feature_names[j]: float(x[i, j]) for j in range(len(feature_names))}
                for i in ranked_idx[:qual_top_k]
            }
        else:
            ts_recs = als_recs if als_pairs else fallback_recs
            ts_score_map = {}
            ts_feature_map = {}

        if save_user_diagnostics:
            user_activity = context["user_activity"].get(
                uid, {"user_total_score": 0.0, "user_unique_repos": 0.0}
            )
            recent_user_activity = context["recent_user_activity"].get(
                uid, {"user_total_score": 0.0, "user_unique_repos": 0.0}
            )
            prior_user_activity = context["prior_user_activity"].get(
                uid, {"user_total_score": 0.0, "user_unique_repos": 0.0}
            )
            user_event = context["user_event"].get(uid, {})
            diagnostic_row = {
                "actor_id": int(uid),
                "relevant_count": int(len(relevant)),
                "seen_count": int(len(seen)),
                "is_warm": bool(uid in user2idx),
                "has_als_candidates": bool(als_pairs),
                "als_candidate_count": int(len(als_pairs)),
                "ts_candidate_count": int(len(ts_pairs)),
                "log_user_total_score": float(np.log1p(user_activity["user_total_score"])),
                "log_user_unique_repos": float(np.log1p(user_activity["user_unique_repos"])),
                "log_recent_user_total_score": float(
                    np.log1p(recent_user_activity["user_total_score"])
                ),
                "log_recent_user_unique_repos": float(
                    np.log1p(recent_user_activity["user_unique_repos"])
                ),
                "log_prior_user_total_score": float(
                    np.log1p(prior_user_activity["user_total_score"])
                ),
                "log_prior_user_unique_repos": float(
                    np.log1p(prior_user_activity["user_unique_repos"])
                ),
                "user_recent_score_share": float(
                    recent_user_activity["user_total_score"]
                    / (user_activity["user_total_score"] + 1e-6)
                ),
                "user_score_growth_ratio": float(
                    (
                        recent_user_activity["user_total_score"]
                        - prior_user_activity["user_total_score"]
                    )
                    / (prior_user_activity["user_total_score"] + 1.0)
                ),
                "user_watch_share": float(user_event.get("user_watch_share", 0.0)),
                "user_pr_share": float(user_event.get("user_pr_share", 0.0)),
                "user_fork_share": float(user_event.get("user_fork_share", 0.0)),
                "user_push_share": float(user_event.get("user_push_share", 0.0)),
                "user_issue_share": float(user_event.get("user_issue_share", 0.0)),
                "user_comment_share": float(user_event.get("user_comment_share", 0.0)),
                "user_event_entropy": float(
                    -sum(
                        share * math.log(share)
                        for share in [
                            user_event.get("user_watch_share", 0.0),
                            user_event.get("user_pr_share", 0.0),
                            user_event.get("user_fork_share", 0.0),
                            user_event.get("user_push_share", 0.0),
                            user_event.get("user_issue_share", 0.0),
                            user_event.get("user_comment_share", 0.0),
                        ]
                        if share > 0
                    )
                ),
            }
            for model_name, recs in [
                ("pop", pop_recs),
                ("als", als_recs),
                ("ts", ts_recs),
            ]:
                for k in k_values:
                    p, r, n = precision_recall_ndcg(recs, relevant, k)
                    diagnostic_row[f"{model_name}_precision@{k}"] = float(p)
                    diagnostic_row[f"{model_name}_recall@{k}"] = float(r)
                    diagnostic_row[f"{model_name}_ndcg@{k}"] = float(n)
                    diagnostic_row[f"{model_name}_hit@{k}"] = bool(set(recs[:k]) & relevant)
            diagnostic_row["ts_minus_als_ndcg@10"] = (
                diagnostic_row["ts_ndcg@10"] - diagnostic_row["als_ndcg@10"]
            )
            diagnostic_row["ts_minus_als_recall@100"] = (
                diagnostic_row["ts_recall@100"] - diagnostic_row["als_recall@100"]
            )
            diagnostic_row.update(source_mix_for_recs(ts_recs, ts_source_map, 10))
            diagnostic_row.update(source_mix_for_recs(ts_recs, ts_source_map, 50))
            diagnostic_row.update(source_mix_for_recs(ts_recs, ts_source_map, 100))
            if len(x):
                top_n = min(10, len(repo_ids))
                top_idx = [repo_ids.index(rid) for rid in ts_recs[:top_n] if rid in repo_ids]
                if top_idx:
                    for feature in [
                        "profile_cosine",
                        "seen_max_cosine",
                        "seen_mean_cosine",
                        "event_match_dot",
                        "event_l1_distance",
                        "item_watch_share",
                        "item_pr_share",
                        "item_push_share",
                        "recent_rank_pct",
                    ]:
                        if feature in feature_names:
                            diagnostic_row[f"ts_top10_mean_{feature}"] = float(
                                x[top_idx, feature_names.index(feature)].mean()
                            )
            diagnostic_rows.append(diagnostic_row)

        for name, recs in [
            ("Popularity", pop_recs),
            (retrieval_model_name, als_recs),
            ("Two-Stage/Fallback", ts_recs),
        ]:
            for k in k_values:
                p, r, n = precision_recall_ndcg(recs, relevant, k)
                metrics[name][k]["precision"].append(p)
                metrics[name][k]["recall"].append(r)
                metrics[name][k]["ndcg"].append(n)
                coverage[name][k].update(recs[:k])

        if uid in qual_user_set:
            history_ids = [int(rid) for rid in list(seen)[:50]]
            test_ids = [int(rid) for rid in list(relevant)[:50]]
            pop_score_map = context["pop"]
            for name, recs in [
                ("Popularity", pop_recs[:qual_top_k]),
                (retrieval_model_name, als_recs[:qual_top_k]),
                ("Two-Stage/Fallback", ts_recs[:qual_top_k]),
            ]:
                for rank, rid in enumerate(recs, start=1):
                    score = (
                        ts_score_map.get(rid, 0.0)
                        if name == "Two-Stage/Fallback"
                        else als_score_map.get(rid, 0.0)
                        if name == retrieval_model_name
                        else pop_score_map.get(rid, 0.0)
                    )
                    row = {
                        "actor_id": int(uid),
                        "model": name,
                        "rank": rank,
                        "repo_id": int(rid),
                        "score": float(score),
                        "is_hit": rid in relevant,
                        "history_repo_ids": json.dumps(history_ids),
                        "test_repo_ids": json.dumps(test_ids),
                    }
                    if name == "Two-Stage/Fallback":
                        row.update(ts_feature_map.get(rid, {}))
                    qual_rows.append(row)

    rows = []
    for name in ["Popularity", retrieval_model_name, "Two-Stage/Fallback"]:
        for k in k_values:
            rows.append(
                {
                    "model": name,
                    "k": k,
                    "precision": float(np.mean(metrics[name][k]["precision"])),
                    "recall": float(np.mean(metrics[name][k]["recall"])),
                    "ndcg": float(np.mean(metrics[name][k]["ndcg"])),
                    "unique_recommended": len(coverage[name][k]),
                }
            )
    return (
        pd.DataFrame(rows),
        len(eval_users),
        pd.DataFrame(qual_rows),
        pd.DataFrame(diagnostic_rows),
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--history-start", type=parse_date, default=date(2026, 3, 14))
    parser.add_argument("--history-end", type=parse_date, default=date(2026, 4, 24))
    parser.add_argument("--rank-start", type=parse_date, default=date(2026, 4, 25))
    parser.add_argument("--rank-end", type=parse_date, default=date(2026, 5, 1))
    parser.add_argument("--test-start", type=parse_date, default=date(2026, 5, 2))
    parser.add_argument("--test-end", type=parse_date, default=date(2026, 5, 8))
    parser.add_argument("--mart-dir", type=Path, default=MART_DIR)
    parser.add_argument(
        "--use-marts",
        choices=["auto", "always", "never"],
        default="auto",
        help="Read feedback and related candidates from Week 6 mart parquet files.",
    )
    parser.add_argument("--sample-ratio", type=float, default=1.0)
    parser.add_argument("--min-item-users", type=int, default=3)
    parser.add_argument("--min-user-items", type=int, default=1)
    parser.add_argument("--max-items", type=int, default=500_000)
    parser.add_argument("--candidate-k", type=int, default=300)
    parser.add_argument("--hybrid-extra", type=int, default=200)
    parser.add_argument("--recent-candidate-cap", type=int, default=None)
    parser.add_argument("--popular-candidate-cap", type=int, default=None)
    parser.add_argument("--related-candidate-cap", type=int, default=0)
    parser.add_argument("--related-top-per-anchor", type=int, default=20)
    parser.add_argument("--related-max-seen-anchors", type=int, default=20)
    parser.add_argument("--related-path", type=Path, default=DEFAULT_RELATED_PATH)
    parser.add_argument("--rank-users", type=int, default=30_000)
    parser.add_argument("--eval-users", type=int, default=30_000)
    parser.add_argument("--qual-users", type=int, default=1000)
    parser.add_argument("--qual-top-k", type=int, default=20)
    parser.add_argument("--retrieval-model", choices=["als", "bpr"], default="als")
    parser.add_argument("--factors", type=int, default=64)
    parser.add_argument("--iterations", type=int, default=12)
    parser.add_argument("--als-regularization", type=float, default=0.01)
    parser.add_argument("--als-alpha", type=float, default=1.0)
    parser.add_argument("--bpr-learning-rate", type=float, default=0.01)
    parser.add_argument("--lgbm-estimators", type=int, default=120)
    parser.add_argument("--lgbm-learning-rate", type=float, default=0.05)
    parser.add_argument("--lgbm-num-leaves", type=int, default=31)
    parser.add_argument("--lgbm-min-child-samples", type=int, default=20)
    parser.add_argument("--lgbm-subsample", type=float, default=1.0)
    parser.add_argument("--lgbm-colsample", type=float, default=1.0)
    parser.add_argument("--lgbm-reg-l2", type=float, default=0.0)
    parser.add_argument(
        "--lgbm-lambdarank-truncation",
        type=int,
        default=200,
        help="LambdaRank truncation level. Keep near the target NDCG cutoff.",
    )
    parser.add_argument(
        "--k-values",
        type=parse_k_values,
        default=[10, 50, 100, 200],
        help="Comma-separated cutoffs for precision/recall/nDCG evaluation.",
    )
    parser.add_argument("--chunk-size", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument(
        "--best-full",
        action="store_true",
        help=f"Use the current canonical full-run preset ({BEST_FULL_SUFFIX}).",
    )
    parser.add_argument("--output-suffix", type=str, default=None)
    parser.add_argument("--mlflow-tracking-uri", type=str, default="sqlite:///mlflow.db")
    parser.add_argument("--mlflow-experiment", type=str, default="bda-week6-two-stage")
    parser.add_argument("--no-mlflow", action="store_true")
    parser.add_argument("--save-user-diagnostics", action="store_true")
    parser.add_argument(
        "--ranker-feature-parquet",
        type=Path,
        default=None,
        help="Train the LGBM ranker from a reusable ranker feature parquet instead of rebuilding rank features.",
    )
    parser.add_argument(
        "--ranker-feature-summary",
        type=Path,
        default=None,
        help="Optional summary JSON produced next to --ranker-feature-parquet.",
    )
    parser.add_argument(
        "--event-weight",
        action="append",
        default=[],
        help="Override feedback weight, repeatable. Example: --event-weight PullRequestEvent=2.5",
    )
    args = parser.parse_args()

    if args.best_full and not args.smoke:
        apply_best_full_preset(args)

    if args.smoke:
        args.sample_ratio = 0.01
        args.max_items = 50_000
        args.candidate_k = 80
        args.hybrid_extra = 40
        args.rank_users = 1000
        args.eval_users = 1000
        args.qual_users = 200
        args.qual_top_k = 10
        args.factors = 32
        args.iterations = 3
        if args.output_suffix is None:
            args.output_suffix = "smoke"

    event_weights = parse_event_weights(args.event_weight)

    started = time.time()
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    print("1. load data")
    use_marts = should_use_marts(args.use_marts, args.mart_dir)
    if use_marts:
        if args.event_weight:
            raise ValueError("--event-weight cannot be combined with mart feedback; rebuild marts instead.")
        print(f"   using marts from {args.mart_dir}")
        history_df = empty_activity_frame()
        recent_df = empty_activity_frame()
        prior_df = empty_activity_frame()
        recent_fb = empty_feedback_frame()
        prior_fb = empty_feedback_frame()
        history_fb = load_mart_feedback(args.mart_dir / "user_repo_interaction_mart.parquet")
        split_mart = args.mart_dir / "experiment_split_mart.parquet"
        rank_fb = load_mart_feedback(split_mart, "rank_label")
        test_fb = load_mart_feedback(split_mart, "test")
    else:
        history_df = load_period(DATA_DIR, args.history_start, args.history_end)
        recent_start = max(args.history_start, args.history_end - timedelta(days=13))
        prior_end = recent_start - timedelta(days=1)
        recent_df = load_period(DATA_DIR, recent_start, args.history_end)
        prior_df = (
            load_period(DATA_DIR, args.history_start, prior_end)
            if prior_end >= args.history_start
            else history_df.iloc[0:0].copy()
        )
        rank_df = load_period(DATA_DIR, args.rank_start, args.rank_end)
        test_df = load_period(DATA_DIR, args.test_start, args.test_end)
        history_fb = build_feedback(history_df, event_weights)
        rank_fb = build_feedback(rank_df, event_weights)
        test_fb = build_feedback(test_df, event_weights)
        recent_fb = build_feedback(recent_df, event_weights)
        prior_fb = build_feedback(prior_df, event_weights) if len(prior_df) else empty_feedback_frame()

    print("2. filter catalog/users")
    history_fb, rank_fb, test_fb = filter_catalog(
        history_fb,
        rank_fb,
        test_fb,
        args.min_item_users,
        args.min_user_items,
        args.max_items,
    )
    history_fb, rank_fb, test_fb = sample_users(
        history_fb, rank_fb, test_fb, args.sample_ratio, args.seed
    )
    keep_users = set(history_fb["actor_id"].unique())
    keep_items = set(history_fb["repo_id"].unique())
    if not use_marts:
        history_df = history_df[
            history_df["actor_id"].isin(keep_users) & history_df["repo_id"].isin(keep_items)
        ]
        recent_df = recent_df[
            recent_df["actor_id"].isin(keep_users) & recent_df["repo_id"].isin(keep_items)
        ]
        prior_df = prior_df[
            prior_df["actor_id"].isin(keep_users) & prior_df["repo_id"].isin(keep_items)
        ]
        recent_fb = recent_fb[
            recent_fb["actor_id"].isin(keep_users) & recent_fb["repo_id"].isin(keep_items)
        ]
        prior_fb = prior_fb[
            prior_fb["actor_id"].isin(keep_users) & prior_fb["repo_id"].isin(keep_items)
        ]
    print(
        f"   history={len(history_fb):,} interactions, "
        f"users={history_fb.actor_id.nunique():,}, repos={history_fb.repo_id.nunique():,}"
    )
    print(f"   rank labels={len(rank_fb):,}, test labels={len(test_fb):,}")

    print(f"3. train {args.retrieval_model.upper()}")
    train_sparse, user2idx, item2idx, idx2item = make_matrix(history_fb)
    feature_marts = (
        load_feature_marts(args.mart_dir, set(user2idx), set(item2idx)) if use_marts else None
    )
    if args.retrieval_model == "als":
        model = AlternatingLeastSquares(
            factors=args.factors,
            regularization=args.als_regularization,
            alpha=args.als_alpha,
            iterations=args.iterations,
            random_state=args.seed,
        )
    else:
        model = BayesianPersonalizedRanking(
            factors=args.factors,
            learning_rate=args.bpr_learning_rate,
            regularization=args.als_regularization,
            iterations=args.iterations,
            random_state=args.seed,
        )
    model.fit(train_sparse)

    print("4. retrieve candidates")
    rng = np.random.default_rng(args.seed)
    rank_labels = rank_fb.groupby("actor_id")["repo_id"].apply(set).to_dict()
    test_labels = test_fb.groupby("actor_id")["repo_id"].apply(set).to_dict()
    train_seen = history_fb.groupby("actor_id")["repo_id"].apply(set).to_dict()
    related_seed_items = (
        history_fb.sort_values(["actor_id", "score"], ascending=[True, False])
        .groupby("actor_id", observed=True)["repo_id"]
        .apply(lambda s: [int(rid) for rid in s.head(args.related_max_seen_anchors)])
        .to_dict()
        if args.related_candidate_cap > 0 and args.related_max_seen_anchors > 0
        else {}
    )
    rank_users = sorted(set(rank_labels) & set(user2idx))
    eval_users_all = sorted(test_labels)
    if len(eval_users_all) > args.eval_users:
        eval_users_all = list(rng.choice(np.array(eval_users_all), size=args.eval_users, replace=False))
        test_labels = {uid: test_labels[uid] for uid in eval_users_all}
    test_users = sorted(set(test_labels) & set(user2idx))

    if use_marts and feature_marts:
        repo_feature = feature_marts.get("repo_feature", pd.DataFrame())
        pop_scores = repo_score_series_from_feature_mart(repo_feature, item2idx, "total_score_42d")
        recent_scores = repo_score_series_from_feature_mart(repo_feature, item2idx, "total_score_7d")
    else:
        pop_scores = feedback_popularity(history_fb)
        recent_scores = feedback_popularity(recent_fb)
    popularity_candidates = pop_scores[pop_scores.index.isin(item2idx)].head(args.candidate_k + args.hybrid_extra + 500).index.tolist()
    recent_candidates = recent_scores[recent_scores.index.isin(item2idx)].head(args.candidate_k + args.hybrid_extra + 500).index.tolist()
    if use_marts:
        related_candidates = load_related_candidates_from_mart(
            args.mart_dir / "repo_repo_related_mart.parquet",
            item2idx,
            args.related_top_per_anchor,
        )
    else:
        related_candidates = load_related_candidates(
            args.related_path,
            item2idx,
            args.related_top_per_anchor,
        )

    rank_retrieval = recommend_batch(
        model, train_sparse, user2idx, idx2item, rank_users, args.candidate_k, args.chunk_size
    )
    test_retrieval = recommend_batch(
        model, train_sparse, user2idx, idx2item, test_users, args.candidate_k, args.chunk_size
    )
    rank_hybrid = hybridize_candidates(
        rank_retrieval,
        rank_users,
        popularity_candidates,
        recent_candidates,
        train_seen,
        item2idx,
        args.candidate_k + args.hybrid_extra,
        args.recent_candidate_cap,
        args.popular_candidate_cap,
        related_candidates,
        args.related_candidate_cap,
        related_seed_items,
    )
    test_hybrid = hybridize_candidates(
        test_retrieval,
        test_users,
        popularity_candidates,
        recent_candidates,
        train_seen,
        item2idx,
        args.candidate_k + args.hybrid_extra,
        args.recent_candidate_cap,
        args.popular_candidate_cap,
        related_candidates,
        args.related_candidate_cap,
        related_seed_items,
    )

    print("5. train ranker")
    context = build_feature_context(
        history_df,
        recent_df,
        prior_df,
        history_fb,
        recent_fb,
        prior_fb,
        model,
        user2idx,
        item2idx,
        feature_marts,
    )
    canonical_feature_names = feature_names_for_context(context)
    lgbm_params = {
        "n_estimators": args.lgbm_estimators,
        "learning_rate": args.lgbm_learning_rate,
        "num_leaves": args.lgbm_num_leaves,
        "min_child_samples": args.lgbm_min_child_samples,
        "subsample": args.lgbm_subsample,
        "colsample_bytree": args.lgbm_colsample,
        "reg_lambda": args.lgbm_reg_l2,
        "lambdarank_truncation_level": args.lgbm_lambdarank_truncation,
    }
    if args.ranker_feature_parquet is not None:
        ranker, rank_summary, feature_names = train_ranker_from_feature_parquet(
            args.ranker_feature_parquet,
            args.ranker_feature_summary,
            canonical_feature_names,
            lgbm_params,
            args.seed,
        )
    else:
        ranker, rank_summary = train_ranker(
            rank_hybrid,
            rank_labels,
            user2idx,
            item2idx,
            context,
            args.rank_users,
            args.seed,
            lgbm_params,
        )
        feature_names = canonical_feature_names
    feature_indices = [canonical_feature_names.index(name) for name in feature_names]

    print("6. evaluate")
    results, eval_user_count, qual_cases, user_diagnostics = evaluate(
        test_retrieval,
        test_hybrid,
        test_labels,
        ranker,
        user2idx,
        item2idx,
        context,
        popularity_candidates,
        recent_candidates,
        train_seen,
        args.k_values,
        feature_names,
        feature_indices,
        args.qual_top_k,
        args.qual_users,
        args.seed,
        args.retrieval_model.upper(),
        args.save_user_diagnostics,
    )

    suffix = args.output_suffix or "latest"
    results_path = MODEL_DIR / f"week6_two_stage_{suffix}_metrics.csv"
    summary_path = MODEL_DIR / f"week6_two_stage_{suffix}_summary.json"
    als_path = MODEL_DIR / f"als_{suffix}.pkl"
    ranker_path = MODEL_DIR / f"lgbm_ranker_{suffix}.txt"
    mappings_path = MODEL_DIR / f"mappings_{suffix}.pkl"
    qual_path = MODEL_DIR / f"week6_qual_cases_{suffix}.parquet"
    diagnostics_path = MODEL_DIR / f"week6_user_diagnostics_{suffix}.parquet"
    segment_summary_path = MODEL_DIR / f"week6_segment_summary_{suffix}.csv"

    run_summary = {
        "args": jsonable_args(args),
        "git": git_metadata(),
        "history_interactions": int(len(history_fb)),
        "history_users": int(history_fb.actor_id.nunique()),
        "history_repos": int(history_fb.repo_id.nunique()),
        "rank_label_interactions": int(len(rank_fb)),
        "test_label_interactions": int(len(test_fb)),
        "eval_users": eval_user_count,
        "eval_warm_users": int(sum(1 for uid in test_labels if uid in user2idx)),
        "eval_cold_users": int(sum(1 for uid in test_labels if uid not in user2idx)),
        "ranker": rank_summary,
        "event_weights": event_weights,
        "use_marts": use_marts,
        "mart_dir": str(args.mart_dir),
        "related_anchor_count": len(related_candidates),
        "feature_source": context["feature_source"],
        "feature_names": feature_names,
        "metadata_language_count": len(context["lang2idx"]),
        "paths": {
            "metrics": str(results_path),
            "summary": str(summary_path),
            "lgbm": str(ranker_path),
            "als": str(als_path),
            "mappings": str(mappings_path),
            "qual_cases": str(qual_path),
            "user_diagnostics": str(diagnostics_path)
            if args.save_user_diagnostics
            else None,
            "segment_summary": str(segment_summary_path)
            if args.save_user_diagnostics
            else None,
        },
        "elapsed_min": round((time.time() - started) / 60, 2),
    }

    results.to_csv(results_path, index=False)
    qual_cases.to_parquet(qual_path, index=False)
    if args.save_user_diagnostics:
        user_diagnostics.to_parquet(diagnostics_path, index=False)
        diagnostics_segment_summary(user_diagnostics).to_csv(segment_summary_path, index=False)
    summary_path.write_text(json.dumps(run_summary, indent=2), encoding="utf-8")
    als_path.write_bytes(pickle.dumps(model))
    ranker.booster_.save_model(str(ranker_path))
    mappings_path.write_bytes(
        pickle.dumps(
            {
                "user2idx": user2idx,
                "item2idx": item2idx,
                "idx2item": idx2item,
                "weights": event_weights,
                "feature_names": feature_names,
            }
        )
    )
    artifact_paths = [results_path, summary_path, ranker_path, qual_path]
    if args.save_user_diagnostics:
        artifact_paths.append(diagnostics_path)
        artifact_paths.append(segment_summary_path)
    log_mlflow_run(args, suffix, run_summary, results, artifact_paths)

    print("\nresults")
    print(results.to_string(index=False))
    print(f"\nsaved: {MODEL_DIR}")


if __name__ == "__main__":
    main()
