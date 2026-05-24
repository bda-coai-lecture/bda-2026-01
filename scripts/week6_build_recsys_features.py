"""Build reusable Week 6 recommendation ranker features.

This pipeline materializes the expensive part of the two-stage recommender:
catalog filtering, retrieval model fitting, hybrid candidate generation, and
candidate-level ranker feature construction. Training scripts can then reuse
the cache instead of rebuilding the same features for every ranker variant.

Usage:
    uv run python scripts/week6_build_recsys_features.py --smoke
    uv run python scripts/week6_build_recsys_features.py --use-marts always
"""

from __future__ import annotations

import argparse
import json
import pickle
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from implicit.als import AlternatingLeastSquares
from implicit.bpr import BayesianPersonalizedRanking

import week6_two_stage_v2 as base


FEATURE_DIR = Path("data/features/week6")


def feature_names_from_context(context: dict[str, Any]) -> list[str]:
    return base.feature_names_for_context(context)


def validate_feature_width(x: np.ndarray, feature_names: list[str]) -> None:
    actual = x.shape[1] if x.ndim == 2 else None
    expected = len(feature_names)
    if actual != expected:
        raise RuntimeError(
            f"feature matrix has {actual} columns, but feature_names has {expected}; "
            "keep week6_build_recsys_features.py aligned with week6_two_stage_v2.py"
        )


def standardize_features(x: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    mean = x.mean(axis=0).astype(np.float32)
    std = x.std(axis=0).astype(np.float32)
    std[std < 1e-6] = 1.0
    return ((x - mean) / std).astype(np.float32), mean, std


def candidate_rows(
    actor_id: int,
    candidates: list[tuple],
    labels: set[int],
    group_index: int,
) -> list[dict[str, int | float]]:
    rows = []
    for rank, cand in enumerate(candidates, start=1):
        if len(cand) == 2:
            repo_id, score = cand
            source = 1
        else:
            repo_id, score, source = cand
        rows.append(
            {
                "group_index": group_index,
                "actor_id": int(actor_id),
                "repo_id": int(repo_id),
                "label": int(repo_id in labels),
                "candidate_rank": rank,
                "candidate_score": float(score),
                "candidate_source": int(source),
            }
        )
    return rows


def build_rank_feature_data(
    retrieval: dict[int, list[tuple[int, float, int]]],
    labels_by_user: dict[int, set[int]],
    user2idx: dict[int, int],
    item2idx: dict[int, int],
    context: dict[str, Any],
    max_rank_users: int,
    seed: int,
) -> tuple[np.ndarray, np.ndarray, list[int], pd.DataFrame, dict[str, int | float]]:
    rng = np.random.default_rng(seed)
    users = [uid for uid in retrieval if labels_by_user.get(uid)]
    if len(users) > max_rank_users:
        users = list(rng.choice(np.array(users), size=max_rank_users, replace=False))

    xs: list[np.ndarray] = []
    ys: list[np.ndarray] = []
    groups: list[int] = []
    rows: list[dict[str, int | float]] = []
    positive_labels = 0

    for uid in users:
        x, repo_ids = base.features_for_candidates(uid, retrieval[uid], user2idx, item2idx, context)
        if len(repo_ids) == 0:
            continue
        labels = labels_by_user[uid]
        y = np.array([1 if repo_id in labels else 0 for repo_id in repo_ids], dtype=np.float32)
        if y.sum() == 0:
            continue
        group_index = len(groups)
        xs.append(x)
        ys.append(y)
        groups.append(len(y))
        positive_labels += int(y.sum())
        meta_repo_ids, ranks, scores, sources, _ = filtered_candidate_metadata(
            retrieval[uid], item2idx
        )
        if meta_repo_ids != [int(repo_id) for repo_id in repo_ids]:
            raise RuntimeError(f"candidate metadata mismatch for actor_id={uid}")
        rows.extend(
            {
                "group_index": group_index,
                "actor_id": int(uid),
                "repo_id": int(repo_id),
                "label": int(repo_id in labels),
                "raw_candidate_rank": int(rank),
                "raw_candidate_score": float(score),
                "raw_candidate_source": int(source),
            }
            for repo_id, rank, score, source in zip(
                repo_ids, ranks, scores, sources, strict=True
            )
        )

    if not xs:
        raise RuntimeError("No positive ranker labels found in candidate lists.")

    y_train = np.concatenate(ys).astype(np.float32)
    summary = {
        "rank_users": len(groups),
        "rank_rows": int(len(y_train)),
        "positive_labels": positive_labels,
        "positive_rate": float(y_train.mean()),
    }
    return (
        np.vstack(xs).astype(np.float32),
        y_train,
        groups,
        pd.DataFrame(rows),
        summary,
    )


def write_feature_parquet_materialized(
    path: Path,
    x: np.ndarray,
    candidates: pd.DataFrame,
    feature_names: list[str],
) -> dict[str, list[float]]:
    validate_feature_width(x, feature_names)
    path.parent.mkdir(parents=True, exist_ok=True)
    columns: dict[str, pa.Array] = {
        "group_index": pa.array(candidates["group_index"].to_numpy(np.int32), type=pa.int32()),
        "actor_id": pa.array(candidates["actor_id"].to_numpy(np.int64), type=pa.int64()),
        "repo_id": pa.array(candidates["repo_id"].to_numpy(np.int64), type=pa.int64()),
        "label": pa.array(candidates["label"].to_numpy(np.int8), type=pa.int8()),
        "raw_candidate_rank": pa.array(
            candidates["raw_candidate_rank"].to_numpy(np.int32), type=pa.int32()
        ),
        "raw_candidate_score": pa.array(
            candidates["raw_candidate_score"].to_numpy(np.float32), type=pa.float32()
        ),
        "raw_candidate_source": pa.array(
            candidates["raw_candidate_source"].to_numpy(np.int8), type=pa.int8()
        ),
    }
    for index, name in enumerate(feature_names):
        columns[name] = pa.array(x[:, index].astype(np.float32, copy=False), type=pa.float32())
    pq.write_table(pa.table(columns), path, compression="zstd")

    mean = x.mean(axis=0).astype(float)
    std = x.std(axis=0).astype(float)
    std[std < 1e-6] = 1.0
    return {
        "feature_mean": mean.tolist(),
        "feature_std": std.tolist(),
    }


def filtered_candidate_metadata(
    candidates: list[tuple],
    item2idx: dict[int, int],
) -> tuple[list[int], list[int], list[float], list[int], list[int]]:
    repo_ids, ranks, scores, sources, item_indices = [], [], [], [], []
    for cand_rank, cand in enumerate(candidates, start=1):
        if len(cand) == 2:
            repo_id, score = cand
            source = 1
        else:
            repo_id, score, source = cand
        item_idx = item2idx.get(repo_id)
        if item_idx is None:
            continue
        repo_ids.append(int(repo_id))
        ranks.append(cand_rank)
        scores.append(float(score))
        sources.append(int(source))
        item_indices.append(item_idx)
    return repo_ids, ranks, scores, sources, item_indices


def write_feature_parquet_stream(
    path: Path,
    retrieval: dict[int, list[tuple[int, float, int]]],
    labels_by_user: dict[int, set[int]],
    user2idx: dict[int, int],
    item2idx: dict[int, int],
    context: dict[str, Any],
    feature_names: list[str],
    max_rank_users: int,
    seed: int,
    batch_rows: int,
) -> tuple[list[int], dict[str, int | float], dict[str, list[float]]]:
    rng = np.random.default_rng(seed)
    users = [uid for uid in retrieval if labels_by_user.get(uid)]
    if len(users) > max_rank_users:
        users = list(rng.choice(np.array(users), size=max_rank_users, replace=False))

    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema(
        [
            pa.field("group_index", pa.int32()),
            pa.field("actor_id", pa.int64()),
            pa.field("repo_id", pa.int64()),
            pa.field("label", pa.int8()),
            pa.field("raw_candidate_rank", pa.int32()),
            pa.field("raw_candidate_score", pa.float32()),
            pa.field("raw_candidate_source", pa.int8()),
            *[pa.field(name, pa.float32()) for name in feature_names],
        ]
    )

    writer: pq.ParquetWriter | None = None
    groups: list[int] = []
    pending: list[pd.DataFrame] = []
    pending_rows = 0
    positive_labels = 0
    row_count = 0
    feature_sum = np.zeros(len(feature_names), dtype=np.float64)
    feature_sumsq = np.zeros(len(feature_names), dtype=np.float64)

    def flush() -> None:
        nonlocal writer, pending, pending_rows
        if not pending:
            return
        batch_df = pd.concat(pending, ignore_index=True)
        table = pa.Table.from_pandas(batch_df, schema=schema, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(path, schema=schema, compression="zstd")
        writer.write_table(table)
        pending = []
        pending_rows = 0

    try:
        for uid in users:
            x, repo_ids = base.features_for_candidates(
                uid, retrieval[uid], user2idx, item2idx, context
            )
            if len(repo_ids) == 0:
                continue
            validate_feature_width(x, feature_names)
            labels = labels_by_user[uid]
            y = np.array([1 if repo_id in labels else 0 for repo_id in repo_ids], dtype=np.int8)
            if int(y.sum()) == 0:
                continue

            meta_repo_ids, ranks, scores, sources, _ = filtered_candidate_metadata(
                retrieval[uid], item2idx
            )
            if meta_repo_ids != [int(repo_id) for repo_id in repo_ids]:
                raise RuntimeError(f"candidate metadata mismatch for actor_id={uid}")

            group_index = len(groups)
            groups.append(len(y))
            positive_labels += int(y.sum())
            row_count += len(y)
            feature_sum += x.sum(axis=0, dtype=np.float64)
            feature_sumsq += np.square(x, dtype=np.float64).sum(axis=0, dtype=np.float64)

            frame = pd.DataFrame(
                {
                    "group_index": np.full(len(y), group_index, dtype=np.int32),
                    "actor_id": np.full(len(y), int(uid), dtype=np.int64),
                    "repo_id": np.array(repo_ids, dtype=np.int64),
                    "label": y,
                    "raw_candidate_rank": np.array(ranks, dtype=np.int32),
                    "raw_candidate_score": np.array(scores, dtype=np.float32),
                    "raw_candidate_source": np.array(sources, dtype=np.int8),
                }
            )
            for index, name in enumerate(feature_names):
                frame[name] = x[:, index].astype(np.float32, copy=False)
            pending.append(frame)
            pending_rows += len(frame)
            if pending_rows >= batch_rows:
                flush()
        flush()
    finally:
        if writer is not None:
            writer.close()

    if row_count == 0:
        raise RuntimeError("No positive ranker labels found in candidate lists.")

    mean = feature_sum / row_count
    variance = np.maximum(feature_sumsq / row_count - np.square(mean), 0.0)
    std = np.sqrt(variance)
    std[std < 1e-6] = 1.0
    rank_summary = {
        "rank_users": len(groups),
        "rank_rows": int(row_count),
        "positive_labels": int(positive_labels),
        "positive_rate": float(positive_labels / row_count),
    }
    stats = {
        "feature_mean": mean.astype(float).tolist(),
        "feature_std": std.astype(float).tolist(),
    }
    return groups, rank_summary, stats


def jsonable_args(args: argparse.Namespace) -> dict[str, Any]:
    return {
        key: str(value) if isinstance(value, (date, Path)) else value
        for key, value in vars(args).items()
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(f"saved {path}")


def write_outputs(
    args: argparse.Namespace,
    suffix: str,
    payload: dict[str, Any],
    rank_candidates: pd.DataFrame,
    x_train_std: np.ndarray,
    feature_mean: np.ndarray,
    feature_std: np.ndarray,
    summary: dict[str, Any],
) -> None:
    args.output_dir.mkdir(parents=True, exist_ok=True)

    npz_path = args.output_dir / f"ranker_features_{suffix}.npz"
    np.savez_compressed(
        npz_path,
        x_train=payload["x_train_raw"],
        x_train_std=x_train_std,
        y_train=payload["y_train"],
        groups=np.array(payload["groups"], dtype=np.int32),
        feature_mean=feature_mean,
        feature_std=feature_std,
    )
    print(f"saved {npz_path}")

    rank_path = args.output_dir / f"ranker_candidates_{suffix}.parquet"
    rank_candidates.to_parquet(rank_path, index=False)
    print(f"saved {rank_path} rows={len(rank_candidates):,}")

    cache_path = None
    if args.write_pickle_cache:
        cache_path = args.output_dir / f"ranker_feature_cache_{suffix}.pkl"
        cache_path.write_bytes(pickle.dumps(payload))
        print(f"saved {cache_path}")

    summary["outputs"]["pickle_cache"] = str(cache_path) if cache_path else None
    write_json(args.output_dir / f"ranker_features_{suffix}_summary.json", summary)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=base.DATA_DIR)
    parser.add_argument("--mart-dir", type=Path, default=base.MART_DIR)
    parser.add_argument("--output-dir", type=Path, default=FEATURE_DIR)
    parser.add_argument("--history-start", type=base.parse_date, default=date(2026, 3, 14))
    parser.add_argument("--history-end", type=base.parse_date, default=date(2026, 4, 24))
    parser.add_argument("--rank-start", type=base.parse_date, default=date(2026, 4, 25))
    parser.add_argument("--rank-end", type=base.parse_date, default=date(2026, 5, 1))
    parser.add_argument("--test-start", type=base.parse_date, default=date(2026, 5, 2))
    parser.add_argument("--test-end", type=base.parse_date, default=date(2026, 5, 8))
    parser.add_argument("--use-marts", choices=["auto", "always", "never"], default="auto")
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
    parser.add_argument("--related-path", type=Path, default=base.DEFAULT_RELATED_PATH)
    parser.add_argument("--rank-users", type=int, default=30_000)
    parser.add_argument("--eval-users", type=int, default=30_000)
    parser.add_argument("--retrieval-model", choices=["als", "bpr"], default="als")
    parser.add_argument("--factors", type=int, default=64)
    parser.add_argument("--iterations", type=int, default=12)
    parser.add_argument("--als-regularization", type=float, default=0.01)
    parser.add_argument("--als-alpha", type=float, default=1.0)
    parser.add_argument("--bpr-learning-rate", type=float, default=0.01)
    parser.add_argument("--chunk-size", type=int, default=2000)
    parser.add_argument("--parquet-batch-rows", type=int, default=100_000)
    parser.add_argument(
        "--materialize-in-memory",
        action="store_true",
        help="Build the full feature matrix in memory before writing parquet. Faster, but higher RSS.",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-suffix", type=str, default=None)
    parser.add_argument(
        "--write-pickle-cache",
        action="store_true",
        help="Also write a full Python object cache. This can be very large for full runs.",
    )
    parser.add_argument("--smoke", action="store_true")
    return parser.parse_args()


def apply_smoke_defaults(args: argparse.Namespace) -> None:
    if not args.smoke:
        return
    args.output_dir = args.output_dir / "smoke"
    args.history_start = date(2026, 4, 18)
    args.history_end = date(2026, 4, 24)
    args.rank_start = date(2026, 4, 25)
    args.rank_end = date(2026, 4, 26)
    args.test_start = date(2026, 4, 27)
    args.test_end = date(2026, 4, 28)
    args.sample_ratio = min(args.sample_ratio, 0.2)
    args.max_items = min(args.max_items, 50_000)
    args.candidate_k = min(args.candidate_k, 100)
    args.hybrid_extra = min(args.hybrid_extra, 50)
    args.rank_users = min(args.rank_users, 2_000)
    args.eval_users = min(args.eval_users, 2_000)
    args.factors = min(args.factors, 32)
    args.iterations = min(args.iterations, 4)
    args.chunk_size = min(args.chunk_size, 1000)


def main() -> None:
    args = parse_args()
    apply_smoke_defaults(args)
    suffix = args.output_suffix or ("smoke" if args.smoke else "latest")
    started = time.time()

    print("1. load feedback")
    event_weights = dict(base.DEFAULT_WEIGHTS)
    use_marts = base.should_use_marts(args.use_marts, args.mart_dir)
    if use_marts:
        print(f"   using marts from {args.mart_dir}")
        history_df = base.empty_activity_frame()
        recent_df = base.empty_activity_frame()
        prior_df = base.empty_activity_frame()
        recent_fb = base.empty_feedback_frame()
        prior_fb = base.empty_feedback_frame()
        history_fb = base.load_mart_feedback(args.mart_dir / "user_repo_interaction_mart.parquet")
        split_mart = args.mart_dir / "experiment_split_mart.parquet"
        rank_fb = base.load_mart_feedback(
            split_mart, "rank_label", args.rank_start, args.rank_end
        )
        test_fb = base.load_mart_feedback(split_mart, "test", args.test_start, args.test_end)
    else:
        history_df = base.load_period(args.data_dir, args.history_start, args.history_end)
        recent_start = max(args.history_start, args.history_end - timedelta(days=13))
        prior_end = recent_start - timedelta(days=1)
        recent_df = base.load_period(args.data_dir, recent_start, args.history_end)
        prior_df = (
            base.load_period(args.data_dir, args.history_start, prior_end)
            if prior_end >= args.history_start
            else history_df.iloc[0:0].copy()
        )
        rank_df = base.load_period(args.data_dir, args.rank_start, args.rank_end)
        test_df = base.load_period(args.data_dir, args.test_start, args.test_end)
        history_fb = base.build_feedback(history_df, event_weights)
        rank_fb = base.build_feedback(rank_df, event_weights)
        test_fb = base.build_feedback(test_df, event_weights)
        recent_fb = base.build_feedback(recent_df, event_weights)
        prior_fb = base.build_feedback(prior_df, event_weights) if len(prior_df) else base.empty_feedback_frame()

    print("2. filter catalog/users")
    history_fb, rank_fb, test_fb = base.filter_catalog(
        history_fb,
        rank_fb,
        test_fb,
        args.min_item_users,
        args.min_user_items,
        args.max_items,
    )
    history_fb, rank_fb, test_fb = base.sample_users(
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
        f"   history={len(history_fb):,}, users={history_fb.actor_id.nunique():,}, "
        f"repos={history_fb.repo_id.nunique():,}"
    )
    print(f"   rank labels={len(rank_fb):,}, test labels={len(test_fb):,}")

    print(f"3. train {args.retrieval_model.upper()}")
    train_sparse, user2idx, item2idx, idx2item = base.make_matrix(history_fb)
    feature_marts = (
        base.load_feature_marts(args.mart_dir, set(user2idx), set(item2idx)) if use_marts else None
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

    print("4. retrieve and hybridize candidates")
    rng = np.random.default_rng(args.seed)
    rank_labels = rank_fb.groupby("actor_id")["repo_id"].apply(set).to_dict()
    test_labels = test_fb.groupby("actor_id")["repo_id"].apply(set).to_dict()
    train_seen = history_fb.groupby("actor_id")["repo_id"].apply(set).to_dict()
    rank_users = sorted(set(rank_labels) & set(user2idx))
    eval_users = sorted(test_labels)
    if len(eval_users) > args.eval_users:
        eval_users = list(rng.choice(np.array(eval_users), size=args.eval_users, replace=False))
        test_labels = {uid: test_labels[uid] for uid in eval_users}
    test_users = sorted(set(test_labels) & set(user2idx))

    if use_marts and feature_marts:
        repo_feature = feature_marts.get("repo_feature", pd.DataFrame())
        pop_scores = base.repo_score_series_from_feature_mart(
            repo_feature, item2idx, "total_score_42d"
        )
        recent_scores = base.repo_score_series_from_feature_mart(
            repo_feature, item2idx, "total_score_7d"
        )
    else:
        pop_scores = base.feedback_popularity(history_fb)
        recent_scores = base.feedback_popularity(recent_fb)
    pool_size = args.candidate_k + args.hybrid_extra + 500
    popularity_candidates = pop_scores[pop_scores.index.isin(item2idx)].head(pool_size).index.tolist()
    recent_candidates = recent_scores[recent_scores.index.isin(item2idx)].head(pool_size).index.tolist()

    related_seed_items = (
        history_fb.sort_values(["actor_id", "score"], ascending=[True, False])
        .groupby("actor_id", observed=True)["repo_id"]
        .apply(lambda s: [int(repo_id) for repo_id in s.head(args.related_max_seen_anchors)])
        .to_dict()
        if args.related_candidate_cap > 0 and args.related_max_seen_anchors > 0
        else {}
    )
    if use_marts:
        related_candidates = base.load_related_candidates_from_mart(
            args.mart_dir / "repo_repo_related_mart.parquet",
            item2idx,
            args.related_top_per_anchor,
        )
    else:
        related_candidates = base.load_related_candidates(
            args.related_path,
            item2idx,
            args.related_top_per_anchor,
        )

    rank_retrieval = base.recommend_batch(
        model, train_sparse, user2idx, idx2item, rank_users, args.candidate_k, args.chunk_size
    )
    test_retrieval = base.recommend_batch(
        model, train_sparse, user2idx, idx2item, test_users, args.candidate_k, args.chunk_size
    )
    max_candidates = args.candidate_k + args.hybrid_extra
    rank_hybrid = base.hybridize_candidates(
        rank_retrieval,
        rank_users,
        popularity_candidates,
        recent_candidates,
        train_seen,
        item2idx,
        max_candidates,
        args.recent_candidate_cap,
        args.popular_candidate_cap,
        related_candidates,
        args.related_candidate_cap,
        related_seed_items,
    )
    rank_hybrid = base.add_label_only_candidates(
        rank_hybrid,
        rank_labels,
        train_seen,
        item2idx,
    )
    test_hybrid = base.hybridize_candidates(
        test_retrieval,
        test_users,
        popularity_candidates,
        recent_candidates,
        train_seen,
        item2idx,
        max_candidates,
        args.recent_candidate_cap,
        args.popular_candidate_cap,
        related_candidates,
        args.related_candidate_cap,
        related_seed_items,
    )

    print("5. build ranker features")
    context = base.build_feature_context(
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
    feature_names = feature_names_from_context(context)
    feature_path = args.output_dir / f"ranker_features_{suffix}.parquet"
    if args.materialize_in_memory:
        x_train_raw, y_train, groups, rank_candidates, rank_summary = build_rank_feature_data(
            rank_hybrid,
            rank_labels,
            user2idx,
            item2idx,
            context,
            args.rank_users,
            args.seed,
        )
        if len(rank_candidates) != len(y_train):
            raise RuntimeError(
                f"candidate rows mismatch: candidates={len(rank_candidates)} labels={len(y_train)}"
            )
        feature_stats = write_feature_parquet_materialized(
            feature_path,
            x_train_raw,
            rank_candidates,
            feature_names,
        )
    else:
        groups, rank_summary, feature_stats = write_feature_parquet_stream(
            feature_path,
            rank_hybrid,
            rank_labels,
            user2idx,
            item2idx,
            context,
            feature_names,
            args.rank_users,
            args.seed,
            args.parquet_batch_rows,
        )

    data_summary = {
        "history_interactions": int(len(history_fb)),
        "history_users": int(history_fb.actor_id.nunique()),
        "history_repos": int(history_fb.repo_id.nunique()),
        "rank_label_interactions": int(len(rank_fb)),
        "test_label_interactions": int(len(test_fb)),
        "rank_retrieval_users": len(rank_retrieval),
        "test_retrieval_users": len(test_retrieval),
        "rank_label_only_candidates": base.count_source_rows(
            rank_hybrid, base.SOURCE_LABEL_ONLY
        ),
        "feature_source": context.get("feature_source", "raw"),
        "use_marts": use_marts,
    }
    summary = {
        "args": jsonable_args(args),
        "data": data_summary,
        "rank_data": rank_summary,
        "features": {
            "n_features": len(feature_names),
            "feature_names": feature_names,
            **feature_stats,
        },
        "outputs": {
            "feature_parquet": str(feature_path),
            "pickle_cache": None,
        },
        "elapsed_min": round((time.time() - started) / 60, 2),
    }
    write_json(args.output_dir / f"ranker_features_{suffix}_summary.json", summary)
    print(f"elapsed_min={summary['elapsed_min']}")


if __name__ == "__main__":
    main()
