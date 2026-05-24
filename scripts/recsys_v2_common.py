"""Shared utilities for the V2 retrieval/re-rank experiment.

V2 keeps the temporal roles explicit:
history is context/seen/filter/features, rank_label is the only train positive
split, and test is used only for final metrics.
"""

from __future__ import annotations

import argparse
import json
import math
import pickle
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from implicit.als import AlternatingLeastSquares
from scipy import sparse

DATA_DIR = Path("data/daily_agg")
MART_DIR = Path("data/marts/week6")
FEATURE_DIR = Path("data/features/recsys_v2")
MODEL_DIR = Path("data/models/recsys_v2")
RESULT_DIR = Path("data/results/recsys_v2")

DEFAULT_WEIGHTS = {
    "WatchEvent": 1.0,
    "ForkEvent": 2.0,
    "IssuesEvent": 0.5,
    "PullRequestEvent": 3.0,
    "IssueCommentEvent": 0.3,
    "PushEvent": 0.2,
}

SOURCE_HARD = "retrieval_hard"
SOURCE_POPULAR = "popular_recent"
SOURCE_RELATED = "related_source"
SOURCE_RANDOM = "random_catalog"
SOURCE_POSITIVE = "rank_label_positive"

SOURCE_CODE = {
    SOURCE_POSITIVE: 0,
    SOURCE_HARD: 1,
    SOURCE_POPULAR: 2,
    SOURCE_RELATED: 3,
    SOURCE_RANDOM: 4,
}

FEATURE_COLUMNS = [
    "retrieval_score",
    "candidate_rank",
    "candidate_source_code",
    "source_rank",
    "source_score",
    "log_user_history_score",
    "log_user_history_repos",
    "log_item_history_score",
    "log_item_history_users",
    "user_item_history_seen",
    "user_history_score_share",
]


@dataclass(frozen=True)
class Paths:
    suffix: str

    @property
    def canonical(self) -> Path:
        return FEATURE_DIR / f"canonical_{self.suffix}.parquet"

    @property
    def canonical_summary(self) -> Path:
        return FEATURE_DIR / f"canonical_{self.suffix}_summary.json"

    @property
    def retrieval_model(self) -> Path:
        return MODEL_DIR / f"retrieval_als_{self.suffix}.pkl"

    @property
    def candidates(self) -> Path:
        return FEATURE_DIR / f"retrieval_candidates_{self.suffix}.parquet"

    @property
    def retrieval_summary(self) -> Path:
        return MODEL_DIR / f"retrieval_als_{self.suffix}_summary.json"

    @property
    def rerank_train(self) -> Path:
        return FEATURE_DIR / f"rerank_train_{self.suffix}.parquet"

    @property
    def rerank_summary(self) -> Path:
        return FEATURE_DIR / f"rerank_train_{self.suffix}_summary.json"

    @property
    def ranker_model(self) -> Path:
        return MODEL_DIR / f"ranker_lgbm_{self.suffix}.pkl"

    @property
    def ranker_summary(self) -> Path:
        return MODEL_DIR / f"ranker_lgbm_{self.suffix}_summary.json"

    @property
    def eval_metrics(self) -> Path:
        return RESULT_DIR / f"eval_metrics_{self.suffix}.csv"

    @property
    def eval_summary(self) -> Path:
        return RESULT_DIR / f"eval_{self.suffix}_summary.json"


def ensure_dirs() -> None:
    for path in [FEATURE_DIR, MODEL_DIR, RESULT_DIR]:
        path.mkdir(parents=True, exist_ok=True)


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def json_default(value: Any) -> Any:
    if isinstance(value, (date, Path)):
        return str(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=json_default), encoding="utf-8")


def parse_event_weights(overrides: list[str] | None) -> dict[str, float]:
    weights = dict(DEFAULT_WEIGHTS)
    for override in overrides or []:
        if "=" not in override:
            raise argparse.ArgumentTypeError(
                f"event weight must use EventType=value format: {override}"
            )
        key, raw = override.split("=", 1)
        weights[key.strip()] = float(raw)
    return weights


def build_feedback(df: pd.DataFrame, weights: dict[str, float]) -> pd.DataFrame:
    if df.empty:
        return empty_feedback()
    out = df.copy()
    out["score"] = out["type"].map(weights).fillna(0).astype(float) * out["cnt"].astype(float)
    out = out.groupby(["actor_id", "repo_id"], observed=True)["score"].sum().reset_index()
    out = out[out["score"] > 0]
    return normalize_feedback(out)


def empty_feedback() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "actor_id": pd.Series(dtype="int64"),
            "repo_id": pd.Series(dtype="int64"),
            "score": pd.Series(dtype="float32"),
        }
    )


def normalize_feedback(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return empty_feedback()
    out = df.rename(columns={"weighted_score": "score"}).copy()
    out = out.dropna(subset=["actor_id", "repo_id", "score"])
    return out.astype({"actor_id": "int64", "repo_id": "int64", "score": "float32"})


def load_mart_split(
    path: Path,
    split: str,
    expected_start: date,
    expected_end: date,
    allow_unversioned: bool,
) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    names = set(pq.ParquetFile(path).schema_arrow.names)
    if {"split_start_date", "split_end_date"}.issubset(names):
        cols = ["split", "split_start_date", "split_end_date", "actor_id", "repo_id", "weighted_score"]
    elif allow_unversioned:
        cols = ["split", "actor_id", "repo_id", "weighted_score"]
    else:
        raise RuntimeError(
            f"{path} has no split_start_date/split_end_date. Rebuild marts, use daily source, "
            "or pass --allow-unversioned-mart intentionally."
        )
    df = pd.read_parquet(path, columns=cols)
    df = df[df["split"] == split].copy()
    if {"split_start_date", "split_end_date"}.issubset(df.columns):
        starts = set(pd.to_datetime(df["split_start_date"]).dt.date)
        ends = set(pd.to_datetime(df["split_end_date"]).dt.date)
        if starts != {expected_start} or ends != {expected_end}:
            raise RuntimeError(
                f"mart split {split} covers {sorted(starts)}..{sorted(ends)}, "
                f"not requested {expected_start}..{expected_end}"
            )
    return normalize_feedback(df)


def canonical_frame(history: pd.DataFrame, rank: pd.DataFrame, test: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for split, frame in [("history", history), ("rank_label", rank), ("test", test)]:
        part = frame.copy()
        part.insert(0, "split", split)
        parts.append(part)
    return pd.concat(parts, ignore_index=True)


def split_canonical(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    return (
        normalize_feedback(df[df["split"] == "history"]),
        normalize_feedback(df[df["split"] == "rank_label"]),
        normalize_feedback(df[df["split"] == "test"]),
    )


def load_canonical(path: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not path.exists():
        raise FileNotFoundError(f"canonical dataset not found: {path}")
    return split_canonical(pd.read_parquet(path))


def filter_catalog(
    history: pd.DataFrame,
    rank: pd.DataFrame,
    test: pd.DataFrame,
    min_item_users: int,
    min_user_items: int,
    max_items: int | None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    item_users = history.groupby("repo_id", observed=True)["actor_id"].nunique()
    keep_items = set(item_users[item_users >= min_item_users].index)
    if max_items:
        top_items = (
            history[history["repo_id"].isin(keep_items)]
            .groupby("repo_id", observed=True)["score"]
            .sum()
            .sort_values(ascending=False)
            .head(max_items)
            .index
        )
        keep_items = set(top_items)
    history = history[history["repo_id"].isin(keep_items)].copy()
    user_items = history.groupby("actor_id", observed=True)["repo_id"].nunique()
    keep_users = set(user_items[user_items >= min_user_items].index)
    history = history[history["actor_id"].isin(keep_users)].copy()
    rank = rank[rank["actor_id"].isin(keep_users) & rank["repo_id"].isin(keep_items)].copy()
    test = test[test["actor_id"].isin(keep_users) & test["repo_id"].isin(keep_items)].copy()
    summary = {
        "retained_users": len(keep_users),
        "retained_items": len(keep_items),
        "min_item_users": min_item_users,
        "min_user_items": min_user_items,
        "max_items": max_items,
    }
    return history, rank, test, summary


def maybe_sample_users(
    history: pd.DataFrame,
    rank: pd.DataFrame,
    test: pd.DataFrame,
    sample_ratio: float,
    seed: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    if sample_ratio >= 1:
        return history, rank, test, {"sample_ratio": sample_ratio, "sampled_users": None}
    rng = np.random.default_rng(seed)
    users = np.array(sorted(history["actor_id"].unique()))
    n = max(1, int(len(users) * sample_ratio))
    sampled = set(rng.choice(users, size=n, replace=False))
    return (
        history[history["actor_id"].isin(sampled)].copy(),
        rank[rank["actor_id"].isin(sampled)].copy(),
        test[test["actor_id"].isin(sampled)].copy(),
        {"sample_ratio": sample_ratio, "sampled_users": n},
    )


def make_matrix(feedback: pd.DataFrame):
    user_ids = feedback["actor_id"].drop_duplicates().to_numpy()
    item_ids = feedback["repo_id"].drop_duplicates().to_numpy()
    user2idx = {int(uid): i for i, uid in enumerate(user_ids)}
    item2idx = {int(iid): i for i, iid in enumerate(item_ids)}
    idx2item = {i: iid for iid, i in item2idx.items()}
    rows = feedback["actor_id"].map(user2idx).to_numpy()
    cols = feedback["repo_id"].map(item2idx).to_numpy()
    data = feedback["score"].astype(np.float32).to_numpy()
    mat = sparse.csr_matrix((data, (rows, cols)), shape=(len(user2idx), len(item2idx)))
    return mat, user2idx, item2idx, idx2item


def train_als(rank: pd.DataFrame, factors: int, iterations: int, regularization: float, alpha: float, seed: int):
    matrix, user2idx, item2idx, idx2item = make_matrix(rank)
    model = AlternatingLeastSquares(
        factors=factors,
        iterations=iterations,
        regularization=regularization,
        alpha=alpha,
        random_state=seed,
    )
    model.fit(matrix)
    return model, matrix, user2idx, item2idx, idx2item


def seen_by_user(feedback: pd.DataFrame) -> dict[int, set[int]]:
    if feedback.empty:
        return {}
    return feedback.groupby("actor_id", observed=True)["repo_id"].apply(lambda s: set(map(int, s))).to_dict()


def labels_by_user(feedback: pd.DataFrame) -> dict[int, set[int]]:
    return seen_by_user(feedback)


def recommend_users(
    model: AlternatingLeastSquares,
    train_matrix: sparse.csr_matrix,
    user2idx: dict[int, int],
    idx2item: dict[int, int],
    users: list[int],
    history_seen: dict[int, set[int]],
    candidate_k: int,
    overgenerate: int,
    chunk_size: int,
) -> pd.DataFrame:
    rows = []
    valid_users = [u for u in users if u in user2idx]
    n = candidate_k + overgenerate
    for start in range(0, len(valid_users), chunk_size):
        chunk = valid_users[start : start + chunk_size]
        idxs = np.array([user2idx[u] for u in chunk])
        item_idxs, scores = model.recommend(
            idxs,
            train_matrix[idxs],
            N=n,
            filter_already_liked_items=True,
        )
        for row_idx, uid in enumerate(chunk):
            seen = history_seen.get(uid, set())
            kept = 0
            for raw_rank, iidx in enumerate(item_idxs[row_idx], start=1):
                repo_id = int(idx2item[int(iidx)])
                if repo_id in seen:
                    continue
                kept += 1
                rows.append(
                    {
                        "actor_id": int(uid),
                        "repo_id": repo_id,
                        "candidate_rank": kept,
                        "raw_candidate_rank": raw_rank,
                        "retrieval_score": float(scores[row_idx][raw_rank - 1]),
                    }
                )
                if kept >= candidate_k:
                    break
    return pd.DataFrame(rows)


def popularity_list(feedback: pd.DataFrame) -> list[int]:
    return (
        feedback.groupby("repo_id", observed=True)["score"]
        .sum()
        .sort_values(ascending=False)
        .index.astype(int)
        .tolist()
    )


def related_map_from_history(history: pd.DataFrame, max_anchors_per_user: int = 50) -> dict[int, list[int]]:
    """Build a lightweight same-user co-occurrence map for related/source negatives."""
    related: dict[int, dict[int, float]] = {}
    for _, part in history.sort_values(["actor_id", "score"], ascending=[True, False]).groupby(
        "actor_id", observed=True
    ):
        items = [int(x) for x in part["repo_id"].head(max_anchors_per_user)]
        for anchor in items:
            bucket = related.setdefault(anchor, {})
            for rid in items:
                if rid != anchor:
                    bucket[rid] = bucket.get(rid, 0.0) + 1.0
    return {
        anchor: [rid for rid, _ in sorted(scores.items(), key=lambda kv: kv[1], reverse=True)[:200]]
        for anchor, scores in related.items()
    }


def feature_stats(history: pd.DataFrame, rank: pd.DataFrame) -> dict[str, Any]:
    user_hist = history.groupby("actor_id", observed=True).agg(
        user_history_score=("score", "sum"),
        user_history_repos=("repo_id", "nunique"),
    )
    item_hist = history.groupby("repo_id", observed=True).agg(
        item_history_score=("score", "sum"),
        item_history_users=("actor_id", "nunique"),
    )
    return {
        "user_hist": user_hist,
        "item_hist": item_hist,
        "max_user_history_score": float(user_hist["user_history_score"].max()) if len(user_hist) else 0.0,
        "history_seen": seen_by_user(history),
    }


def attach_features(rows: pd.DataFrame, stats: dict[str, Any]) -> pd.DataFrame:
    if rows.empty:
        return rows
    out = rows.copy()
    out = out.merge(stats["user_hist"], left_on="actor_id", right_index=True, how="left")
    out = out.merge(stats["item_hist"], left_on="repo_id", right_index=True, how="left")
    fill_cols = [
        "user_history_score",
        "user_history_repos",
        "item_history_score",
        "item_history_users",
    ]
    out[fill_cols] = out[fill_cols].fillna(0)
    out["log_user_history_score"] = np.log1p(out["user_history_score"].astype(float))
    out["log_user_history_repos"] = np.log1p(out["user_history_repos"].astype(float))
    out["log_item_history_score"] = np.log1p(out["item_history_score"].astype(float))
    out["log_item_history_users"] = np.log1p(out["item_history_users"].astype(float))
    seen = stats["history_seen"]
    out["user_item_history_seen"] = [
        1.0 if int(r.repo_id) in seen.get(int(r.actor_id), set()) else 0.0
        for r in out[["actor_id", "repo_id"]].itertuples(index=False)
    ]
    out["user_history_score_share"] = (
        out["user_history_score"].astype(float) / (stats["max_user_history_score"] + 1e-6)
    )
    out["retrieval_score"] = out["retrieval_score"].fillna(0).astype("float32")
    out["candidate_rank"] = out["candidate_rank"].fillna(0).astype("float32")
    if "candidate_source_code" not in out.columns:
        out["candidate_source_code"] = SOURCE_CODE[SOURCE_HARD]
    if "source_rank" not in out.columns:
        out["source_rank"] = out["candidate_rank"]
    if "source_score" not in out.columns:
        out["source_score"] = out["retrieval_score"]
    out["candidate_source_code"] = out["candidate_source_code"].fillna(SOURCE_CODE[SOURCE_HARD]).astype("float32")
    out["source_rank"] = out["source_rank"].fillna(0).astype("float32")
    out["source_score"] = out["source_score"].fillna(0).astype("float32")
    return out


def precision_recall_ndcg(recommended: list[int], relevant: set[int], k: int) -> tuple[float, float, float]:
    recs = recommended[:k]
    hits = set(recs) & relevant
    precision = len(hits) / k
    recall = len(hits) / len(relevant) if relevant else 0.0
    dcg = sum(1.0 / math.log2(i + 2) for i, rid in enumerate(recs) if rid in relevant)
    idcg = sum(1.0 / math.log2(i + 2) for i in range(min(len(relevant), k)))
    return precision, recall, dcg / idcg if idcg else 0.0


def dump_pickle(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        pickle.dump(payload, f)


def load_pickle(path: Path) -> Any:
    with path.open("rb") as f:
        return pickle.load(f)


def date_range_days(start: date, end: date) -> list[date]:
    days = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)
    return days
