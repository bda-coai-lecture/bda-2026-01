"""Repo-to-repo recommendation experiments for the V2 canonical dataset.

The pipeline builds positive repo2repo labels from temporal user splits:

* train: anchors from history, targets from rank_label
* test: anchors from history + rank_label, targets from test

Candidates are item-to-item recommendations generated from the same context
splits used by each label split. This keeps train/test temporal roles explicit.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.decomposition import TruncatedSVD
from sklearn.preprocessing import normalize

try:
    import faiss
except Exception:  # pragma: no cover - exercised only in minimal envs.
    faiss = None

try:
    from implicit.als import AlternatingLeastSquares
except Exception:  # pragma: no cover - exercised only in minimal envs.
    AlternatingLeastSquares = None


FEATURE_DIR = Path("data/features/recsys_v2")
RESULT_DIR = Path("data/results/recsys_v2")
DEFAULT_CANONICAL = FEATURE_DIR / "canonical_retrieval_rerank_v2_week7_full_20260502.parquet"


def parse_methods(value: str) -> list[str]:
    allowed = {"cooc_norm", "als_item_cosine", "hybrid_rule_als", "item2vec_svd"}
    methods = [part.strip() for part in value.split(",") if part.strip()]
    unknown = sorted(set(methods) - allowed)
    if unknown:
        raise argparse.ArgumentTypeError(f"unknown methods: {unknown}; allowed={sorted(allowed)}")
    return methods or ["cooc_norm", "als_item_cosine", "hybrid_rule_als"]


def json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    raise TypeError(f"{type(value).__name__} is not JSON serializable")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=json_default), encoding="utf-8")


def load_canonical(path: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_parquet(path, columns=["split", "actor_id", "repo_id", "score"])
    df = df.dropna(subset=["split", "actor_id", "repo_id", "score"]).copy()
    df = df.astype({"actor_id": "int64", "repo_id": "int64", "score": "float32"})
    return (
        df[df["split"] == "history"][["actor_id", "repo_id", "score"]].copy(),
        df[df["split"] == "rank_label"][["actor_id", "repo_id", "score"]].copy(),
        df[df["split"] == "test"][["actor_id", "repo_id", "score"]].copy(),
    )


def apply_caps(
    history: pd.DataFrame,
    rank: pd.DataFrame,
    test: pd.DataFrame,
    max_users: int | None,
    max_items: int | None,
    max_items_include_test: bool,
    seed: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    rng = np.random.default_rng(seed)
    summary: dict[str, Any] = {
        "max_users": max_users,
        "max_items": max_items,
        "max_items_include_test": max_items_include_test,
        "input_rows": {
            "history": int(len(history)),
            "rank_label": int(len(rank)),
            "test": int(len(test)),
        },
    }
    if max_users:
        users = np.array(sorted(set(history["actor_id"]) | set(rank["actor_id"]) | set(test["actor_id"])))
        keep_users = set(rng.choice(users, size=min(max_users, len(users)), replace=False))
        history = history[history["actor_id"].isin(keep_users)].copy()
        rank = rank[rank["actor_id"].isin(keep_users)].copy()
        test = test[test["actor_id"].isin(keep_users)].copy()
    if max_items:
        item_cap_frames = [history, rank]
        if max_items_include_test:
            item_cap_frames.append(test)
        item_score = (
            pd.concat(item_cap_frames, ignore_index=True)
            .groupby("repo_id", observed=True)["score"]
            .sum()
            .sort_values(ascending=False)
        )
        keep_items = set(item_score.head(max_items).index.astype(int))
        history = history[history["repo_id"].isin(keep_items)].copy()
        rank = rank[rank["repo_id"].isin(keep_items)].copy()
        test = test[test["repo_id"].isin(keep_items)].copy()
    summary["output_rows"] = {
        "history": int(len(history)),
        "rank_label": int(len(rank)),
        "test": int(len(test)),
    }
    summary["output_users"] = int(len(set(history["actor_id"]) | set(rank["actor_id"]) | set(test["actor_id"])))
    summary["output_items"] = int(len(set(history["repo_id"]) | set(rank["repo_id"]) | set(test["repo_id"])))
    return history, rank, test, summary


def top_items_by_user(frame: pd.DataFrame, max_items_per_user: int) -> dict[int, list[int]]:
    if frame.empty:
        return {}
    top = (
        frame.sort_values(["actor_id", "score"], ascending=[True, False])
        .groupby("actor_id", observed=True)
        .head(max_items_per_user)
    )
    return top.groupby("actor_id", observed=True)["repo_id"].agg(lambda s: [int(x) for x in s]).to_dict()


def top_items_and_scores_by_user(
    frame: pd.DataFrame,
    max_items_per_user: int,
) -> tuple[dict[int, list[int]], dict[int, dict[int, float]]]:
    if frame.empty:
        return {}, {}
    top = (
        frame.sort_values(["actor_id", "score"], ascending=[True, False])
        .groupby("actor_id", observed=True)
        .head(max_items_per_user)
    )
    items = top.groupby("actor_id", observed=True)["repo_id"].agg(lambda s: [int(x) for x in s]).to_dict()
    scores = {
        int(uid): dict(zip(part["repo_id"].astype(int), part["score"].astype(float), strict=False))
        for uid, part in top.groupby("actor_id", observed=True)
    }
    return items, scores


def select_anchor_cap(
    context: pd.DataFrame,
    anchors: pd.Series,
    max_anchors: int,
    order: str,
) -> set[int]:
    available = set(anchors.astype(int))
    if order == "repo_id":
        return set(sorted(available)[:max_anchors])
    popularity = (
        context[context["repo_id"].isin(available)]
        .groupby("repo_id", observed=True)
        .agg(context_users=("actor_id", "nunique"), context_score=("score", "sum"))
        .reset_index()
        .sort_values(
            ["context_users", "context_score", "repo_id"],
            ascending=[False, False, True],
        )
    )
    return set(popularity["repo_id"].head(max_anchors).astype(int))


def build_labels_for_split(
    context: pd.DataFrame,
    labels: pd.DataFrame,
    split: str,
    max_anchor_items_per_user: int,
    max_target_items_per_user: int,
    max_anchors: int | None,
    max_anchors_order: str,
) -> pd.DataFrame:
    label_user_ids = labels["actor_id"].drop_duplicates()
    context = context[context["actor_id"].isin(label_user_ids)]
    context_by_user = top_items_by_user(context, max_anchor_items_per_user)
    labels_by_user, label_scores_by_user = top_items_and_scores_by_user(labels, max_target_items_per_user)
    rows: dict[tuple[int, int], dict[str, float]] = {}
    for uid, anchors in context_by_user.items():
        targets = labels_by_user.get(uid)
        if not targets:
            continue
        target_scores = label_scores_by_user.get(uid, {})
        for anchor in anchors:
            for target in targets:
                if anchor == target:
                    continue
                key = (anchor, target)
                row = rows.setdefault(key, {"support_users": 0.0, "label_score": 0.0})
                row["support_users"] += 1.0
                row["label_score"] += float(target_scores.get(target, 1.0))
    out_rows = [
        {
            "label_split": split,
            "anchor_repo_id": int(anchor),
            "target_repo_id": int(target),
            "label": 1,
            "support_users": int(values["support_users"]),
            "label_score": float(values["label_score"]),
        }
        for (anchor, target), values in rows.items()
    ]
    out = pd.DataFrame(out_rows)
    if out.empty:
        return empty_labels()
    out = out.sort_values(
        ["support_users", "label_score", "anchor_repo_id", "target_repo_id"],
        ascending=[False, False, True, True],
    )
    if max_anchors:
        keep = select_anchor_cap(
            context,
            out["anchor_repo_id"].drop_duplicates(),
            max_anchors,
            max_anchors_order,
        )
        out = out[out["anchor_repo_id"].isin(keep)].copy()
    return normalize_labels(out)


def empty_labels() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "label_split": pd.Series(dtype="object"),
            "anchor_repo_id": pd.Series(dtype="int64"),
            "target_repo_id": pd.Series(dtype="int64"),
            "label": pd.Series(dtype="int8"),
            "support_users": pd.Series(dtype="int32"),
            "label_score": pd.Series(dtype="float32"),
        }
    )


def normalize_labels(labels: pd.DataFrame) -> pd.DataFrame:
    if labels.empty:
        return empty_labels()
    return labels[
        ["label_split", "anchor_repo_id", "target_repo_id", "label", "support_users", "label_score"]
    ].astype(
        {
            "label_split": "object",
            "anchor_repo_id": "int64",
            "target_repo_id": "int64",
            "label": "int8",
            "support_users": "int32",
            "label_score": "float32",
        }
    )


def build_labels(
    history: pd.DataFrame,
    rank: pd.DataFrame,
    test: pd.DataFrame,
    max_anchor_items_per_user: int,
    max_target_items_per_user: int,
    max_anchors: int | None,
    max_anchors_order: str,
) -> pd.DataFrame:
    train = build_labels_for_split(
        history,
        rank,
        "train",
        max_anchor_items_per_user,
        max_target_items_per_user,
        max_anchors,
        max_anchors_order,
    )
    test_labels = build_labels_for_split(
        pd.concat([history, rank], ignore_index=True),
        test,
        "test",
        max_anchor_items_per_user,
        max_target_items_per_user,
        max_anchors,
        max_anchors_order,
    )
    return normalize_labels(pd.concat([train, test_labels], ignore_index=True))


def item_user_counts(context: pd.DataFrame) -> dict[int, int]:
    return context.groupby("repo_id", observed=True)["actor_id"].nunique().astype(int).to_dict()


def build_cooc_candidates(
    context: pd.DataFrame,
    label_anchors: dict[str, set[int]],
    top_k: int,
    max_items_per_user: int,
) -> pd.DataFrame:
    counts = item_user_counts(context)
    anchors_all = set().union(*label_anchors.values()) if label_anchors else set()
    cooc: dict[int, dict[int, float]] = defaultdict(lambda: defaultdict(float))
    for items in top_items_by_user(context, max_items_per_user).values():
        kept = [rid for rid in items if rid in counts]
        anchor_items = [rid for rid in kept if rid in anchors_all]
        for anchor in anchor_items:
            bucket = cooc[anchor]
            for target in kept:
                if target != anchor:
                    bucket[target] += 1.0

    rows = []
    for label_split, anchors in label_anchors.items():
        for anchor in sorted(anchors):
            scored = []
            denom_anchor = math.sqrt(max(counts.get(anchor, 1), 1))
            for target, value in cooc.get(anchor, {}).items():
                denom = denom_anchor * math.sqrt(max(counts.get(target, 1), 1))
                scored.append((target, value / denom))
            scored.sort(key=lambda x: (-x[1], x[0]))
            for rank, (target, score) in enumerate(scored[:top_k], start=1):
                rows.append(candidate_row(label_split, "cooc_norm", anchor, target, rank, score, score, np.nan))
    return normalize_candidates(pd.DataFrame(rows))


def make_matrix(context: pd.DataFrame) -> tuple[sparse.csr_matrix, dict[int, int], dict[int, int], np.ndarray]:
    user_ids = context["actor_id"].drop_duplicates().astype("int64").to_numpy()
    item_ids = context["repo_id"].drop_duplicates().astype("int64").to_numpy()
    user2idx = {int(uid): i for i, uid in enumerate(user_ids)}
    item2idx = {int(iid): i for i, iid in enumerate(item_ids)}
    rows = context["actor_id"].map(user2idx).to_numpy(dtype=np.int32, copy=False)
    cols = context["repo_id"].map(item2idx).to_numpy(dtype=np.int32, copy=False)
    data = context["score"].to_numpy(dtype=np.float32, copy=False)
    matrix = sparse.csr_matrix((data, (rows, cols)), shape=(len(user2idx), len(item2idx)))
    return matrix, user2idx, item2idx, item_ids


def item_embeddings_from_als_or_svd(
    context: pd.DataFrame,
    factors: int,
    iterations: int,
    regularization: float,
    alpha: float,
    seed: int,
) -> tuple[np.ndarray, np.ndarray, str]:
    matrix, _, _, item_ids = make_matrix(context)
    if matrix.shape[1] < 2:
        raise RuntimeError("item embedding baseline needs at least two items")
    n_factors = max(2, min(factors, matrix.shape[1] - 1, matrix.shape[0] - 1))
    if AlternatingLeastSquares is not None:
        model = AlternatingLeastSquares(
            factors=n_factors,
            iterations=iterations,
            regularization=regularization,
            alpha=alpha,
            random_state=seed,
        )
        model.fit(matrix)
        emb = np.asarray(model.item_factors, dtype=np.float32)
        return item_ids, normalize(emb), "implicit_als"

    svd = TruncatedSVD(n_components=n_factors, random_state=seed)
    emb = svd.fit_transform(matrix.T)
    return item_ids, normalize(emb.astype(np.float32, copy=False)), "sklearn_truncated_svd"


def top_cosine_rows(
    item_ids: np.ndarray,
    emb: np.ndarray,
    label_anchors: dict[str, set[int]],
    method: str,
    top_k: int,
    batch_size: int,
) -> pd.DataFrame:
    item2idx = {int(rid): i for i, rid in enumerate(item_ids)}
    rows = []
    if faiss is not None:
        emb32 = np.ascontiguousarray(emb.astype("float32", copy=False))
        index = faiss.IndexFlatIP(emb32.shape[1])
        index.add(emb32)
        for label_split, anchors in label_anchors.items():
            valid = [anchor for anchor in sorted(anchors) if anchor in item2idx]
            for start in range(0, len(valid), batch_size):
                chunk = valid[start : start + batch_size]
                idxs = np.array([item2idx[a] for a in chunk], dtype=np.int64)
                scores, indices = index.search(emb32[idxs], min(top_k + 1, len(item_ids)))
                for row_idx, anchor in enumerate(chunk):
                    anchor_idx = item2idx[anchor]
                    rank = 0
                    for target_idx, score in zip(indices[row_idx], scores[row_idx], strict=False):
                        if int(target_idx) < 0 or int(target_idx) == anchor_idx:
                            continue
                        rank += 1
                        target = int(item_ids[int(target_idx)])
                        rows.append(
                            candidate_row(
                                label_split,
                                method,
                                anchor,
                                target,
                                rank,
                                float(score),
                                np.nan,
                                float(score),
                            )
                        )
                        if rank >= top_k:
                            break
        return normalize_candidates(pd.DataFrame(rows))

    for label_split, anchors in label_anchors.items():
        valid = [anchor for anchor in sorted(anchors) if anchor in item2idx]
        for start in range(0, len(valid), batch_size):
            chunk = valid[start : start + batch_size]
            idxs = np.array([item2idx[a] for a in chunk], dtype=np.int32)
            scores = emb[idxs] @ emb.T
            for row_idx, anchor in enumerate(chunk):
                anchor_idx = item2idx[anchor]
                row = scores[row_idx]
                n = min(top_k + 1, len(row))
                top_idx = np.argpartition(-row, n - 1)[:n]
                ordered = top_idx[np.argsort(-row[top_idx])]
                rank = 0
                for target_idx in ordered:
                    if int(target_idx) == anchor_idx:
                        continue
                    rank += 1
                    target = int(item_ids[int(target_idx)])
                    score = float(row[int(target_idx)])
                    rows.append(candidate_row(label_split, method, anchor, target, rank, score, np.nan, score))
                    if rank >= top_k:
                        break
    return normalize_candidates(pd.DataFrame(rows))


def item_embeddings_from_item2vec_svd(
    context: pd.DataFrame,
    factors: int,
    max_items_per_user: int,
    seed: int,
) -> tuple[np.ndarray, np.ndarray, str]:
    user_items = top_items_by_user(context, max_items_per_user)
    item_ids = context["repo_id"].drop_duplicates().astype("int64").to_numpy()
    item2idx = {int(iid): i for i, iid in enumerate(item_ids)}
    unigram = np.zeros(len(item_ids), dtype=np.float32)
    pair_counts: defaultdict[tuple[int, int], float] = defaultdict(float)
    total_pairs = 0.0
    for items_raw in user_items.values():
        items = [item2idx[int(rid)] for rid in items_raw if int(rid) in item2idx]
        if len(items) < 2:
            continue
        for idx in items:
            unigram[idx] += 1.0
        weight = 1.0 / max(1.0, len(items) - 1.0)
        for i, left in enumerate(items):
            for right in items[i + 1 :]:
                if left == right:
                    continue
                pair_counts[(left, right)] += weight
                pair_counts[(right, left)] += weight
                total_pairs += 2.0 * weight
    if not pair_counts:
        raise RuntimeError("item2vec_svd needs at least one co-occurring item pair")

    rows = np.fromiter((key[0] for key in pair_counts), dtype=np.int32)
    cols = np.fromiter((key[1] for key in pair_counts), dtype=np.int32)
    values = np.fromiter(pair_counts.values(), dtype=np.float32)
    denom = np.maximum(unigram[rows] * unigram[cols], 1.0)
    ppmi = np.log((values * max(total_pairs, 1.0)) / denom)
    ppmi = np.maximum(ppmi, 0.0).astype(np.float32, copy=False)
    keep = ppmi > 0
    matrix = sparse.csr_matrix(
        (ppmi[keep], (rows[keep], cols[keep])),
        shape=(len(item_ids), len(item_ids)),
    )
    n_factors = max(2, min(factors, matrix.shape[0] - 1, matrix.shape[1] - 1))
    emb = TruncatedSVD(n_components=n_factors, random_state=seed).fit_transform(matrix)
    return item_ids, normalize(emb.astype(np.float32, copy=False)), "item2vec_ppmi_svd"


def candidate_row(
    label_split: str,
    run: str,
    anchor: int,
    target: int,
    rank: int,
    score: float,
    cooc_score: float,
    als_score: float,
) -> dict[str, Any]:
    return {
        "label_split": label_split,
        "run": run,
        "anchor_repo_id": int(anchor),
        "target_repo_id": int(target),
        "rank": int(rank),
        "score": float(score),
        "cooc_score": cooc_score,
        "als_score": als_score,
    }


def normalize_candidates(candidates: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "label_split",
        "run",
        "anchor_repo_id",
        "target_repo_id",
        "rank",
        "score",
        "cooc_score",
        "als_score",
    ]
    if candidates.empty:
        return pd.DataFrame(
            {
                "label_split": pd.Series(dtype="object"),
                "run": pd.Series(dtype="object"),
                "anchor_repo_id": pd.Series(dtype="int64"),
                "target_repo_id": pd.Series(dtype="int64"),
                "rank": pd.Series(dtype="int32"),
                "score": pd.Series(dtype="float32"),
                "cooc_score": pd.Series(dtype="float32"),
                "als_score": pd.Series(dtype="float32"),
            }
        )
    out = candidates[columns].copy()
    return out.astype(
        {
            "label_split": "object",
            "run": "object",
            "anchor_repo_id": "int64",
            "target_repo_id": "int64",
            "rank": "int32",
            "score": "float32",
            "cooc_score": "float32",
            "als_score": "float32",
        }
    )


def build_hybrid(cooc: pd.DataFrame, als: pd.DataFrame, top_k: int, cooc_weight: float) -> pd.DataFrame:
    if cooc.empty and als.empty:
        return normalize_candidates(pd.DataFrame())
    frames = []
    if not cooc.empty:
        frames.append(cooc[["label_split", "anchor_repo_id", "target_repo_id", "cooc_score"]])
    if not als.empty:
        frames.append(als[["label_split", "anchor_repo_id", "target_repo_id", "als_score"]])
    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.merge(frame, on=["label_split", "anchor_repo_id", "target_repo_id"], how="outer")
    merged["cooc_score"] = merged["cooc_score"].fillna(0.0)
    merged["als_score"] = merged["als_score"].fillna(0.0)
    merged["cooc_norm_score"] = merged.groupby(["label_split", "anchor_repo_id"], observed=True)[
        "cooc_score"
    ].transform(lambda s: s / s.max() if s.max() > 0 else s)
    merged["als_norm_score"] = merged.groupby(["label_split", "anchor_repo_id"], observed=True)[
        "als_score"
    ].transform(lambda s: (s - s.min()) / (s.max() - s.min()) if s.max() > s.min() else s)
    merged["score"] = cooc_weight * merged["cooc_norm_score"] + (1.0 - cooc_weight) * merged["als_norm_score"]
    merged = merged.sort_values(
        ["label_split", "anchor_repo_id", "score", "cooc_score", "als_score", "target_repo_id"],
        ascending=[True, True, False, False, False, True],
    )
    merged["rank"] = merged.groupby(["label_split", "anchor_repo_id"], observed=True).cumcount() + 1
    merged = merged[merged["rank"] <= top_k].copy()
    merged["run"] = "hybrid_rule_als"
    return normalize_candidates(
        merged[
            [
                "label_split",
                "run",
                "anchor_repo_id",
                "target_repo_id",
                "rank",
                "score",
                "cooc_score",
                "als_score",
            ]
        ]
    )


def labels_by_anchor(labels: pd.DataFrame, split: str) -> dict[int, set[int]]:
    part = labels[(labels["label_split"] == split) & (labels["label"] > 0)]
    if part.empty:
        return {}
    return part.groupby("anchor_repo_id", observed=True)["target_repo_id"].apply(
        lambda s: set(map(int, s))
    ).to_dict()


def average_precision_at_k(recs: list[int], relevant: set[int], k: int) -> float:
    if not relevant:
        return 0.0
    hits = 0
    score = 0.0
    for i, rid in enumerate(recs[:k], start=1):
        if rid in relevant:
            hits += 1
            score += hits / i
    return score / min(len(relevant), k)


def ndcg_at_k(recs: list[int], relevant: set[int], k: int) -> float:
    dcg = sum(1.0 / math.log2(i + 2) for i, rid in enumerate(recs[:k]) if rid in relevant)
    idcg = sum(1.0 / math.log2(i + 2) for i in range(min(len(relevant), k)))
    return dcg / idcg if idcg else 0.0


def recall_at_k(recs: list[int], relevant: set[int], k: int) -> float:
    return len(set(recs[:k]) & relevant) / len(relevant) if relevant else 0.0


def evaluate_run(candidates: pd.DataFrame, labels: pd.DataFrame, run: str, split: str = "test") -> dict[str, Any]:
    truth = labels_by_anchor(labels, split)
    run_rows = candidates[(candidates["run"] == run) & (candidates["label_split"] == split)]
    if run_rows.empty:
        return {
            "run": run,
            "label_split": split,
            "eval_anchors": 0,
            "candidate_anchors": 0,
            "anchor_coverage": 0.0,
            "ndcg_at_10": 0.0,
            "ndcg_at_50": 0.0,
            "recall_at_50": 0.0,
            "recall_at_100": 0.0,
            "map_at_50": 0.0,
            "hit_rate_at_10": 0.0,
            "unique_recommended_at_10": 0,
            "unique_recommended_at_50": 0,
            "unique_recommended_at_100": 0,
        }
    recs_by_anchor = {
        int(anchor): [int(x) for x in part.sort_values("rank")["target_repo_id"]]
        for anchor, part in run_rows.groupby("anchor_repo_id", observed=True)
    }
    eval_anchors = sorted(truth)
    metrics = {
        "ndcg_at_10": [],
        "ndcg_at_50": [],
        "recall_at_50": [],
        "recall_at_100": [],
        "map_at_50": [],
        "hit_rate_at_10": [],
    }
    for anchor in eval_anchors:
        relevant = truth[anchor]
        recs = recs_by_anchor.get(anchor, [])
        metrics["ndcg_at_10"].append(ndcg_at_k(recs, relevant, 10))
        metrics["ndcg_at_50"].append(ndcg_at_k(recs, relevant, 50))
        metrics["recall_at_50"].append(recall_at_k(recs, relevant, 50))
        metrics["recall_at_100"].append(recall_at_k(recs, relevant, 100))
        metrics["map_at_50"].append(average_precision_at_k(recs, relevant, 50))
        metrics["hit_rate_at_10"].append(1.0 if set(recs[:10]) & relevant else 0.0)
    row: dict[str, Any] = {
        "run": run,
        "label_split": split,
        "eval_anchors": int(len(eval_anchors)),
        "candidate_anchors": int(len(set(recs_by_anchor) & set(truth))),
        "anchor_coverage": float(len(set(recs_by_anchor) & set(truth)) / len(truth)) if truth else 0.0,
    }
    for key, values in metrics.items():
        row[key] = float(np.mean(values)) if values else 0.0
    for k in [10, 50, 100]:
        top = run_rows[run_rows["rank"] <= k]
        row[f"unique_recommended_at_{k}"] = int(top["target_repo_id"].nunique())
    return row


def write_candidate_files(candidates: pd.DataFrame, suffix: str) -> dict[str, str]:
    paths = {}
    for run, part in candidates.groupby("run", observed=True):
        path = FEATURE_DIR / f"repo2repo_candidates_{run}_{suffix}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        part.sort_values(["label_split", "anchor_repo_id", "rank"]).to_parquet(path, index=False)
        paths[str(run)] = str(path)
    return paths


def export_mart(candidates: pd.DataFrame, run: str, label_split: str, output_path: Path) -> None:
    part = candidates[(candidates["run"] == run) & (candidates["label_split"] == label_split)].copy()
    if part.empty:
        raise RuntimeError(f"no {label_split} candidates found for mart run={run}")
    out = part.sort_values(["anchor_repo_id", "rank"])[
        ["anchor_repo_id", "target_repo_id", "rank", "score", "cooc_score", "als_score"]
    ].rename(
        columns={
            "target_repo_id": "related_repo_id",
            "score": "cooc_score",
            "cooc_score": "source_cooc_score",
        }
    )
    out["mart_run"] = run
    out["label_split"] = label_split
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(output_path, index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--suffix", default="week7_full_20260502")
    parser.add_argument("--canonical-path", type=Path, default=DEFAULT_CANONICAL)
    parser.add_argument("--top-k", type=int, default=100)
    parser.add_argument(
        "--methods",
        type=parse_methods,
        default=parse_methods("cooc_norm,als_item_cosine,hybrid_rule_als"),
    )
    parser.add_argument("--max-anchors", type=int, default=None)
    parser.add_argument(
        "--max-anchors-order",
        choices=("context_popularity", "repo_id"),
        default="context_popularity",
        help=(
            "Anchor cap order. context_popularity uses only each split's context "
            "(history for train, history+rank_label for test), avoiding label-score leakage."
        ),
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=None,
        help="Cap catalog using history+rank_label only by default, then filter all splits to that catalog.",
    )
    parser.add_argument(
        "--max-items-include-test",
        action="store_true",
        help=(
            "Include test rows when choosing the --max-items catalog. This leaks final-test information; "
            "use only for smoke/debug runs."
        ),
    )
    parser.add_argument("--max-users", type=int, default=None)
    parser.add_argument("--max-anchor-items-per-user", type=int, default=50)
    parser.add_argument("--max-target-items-per-user", type=int, default=50)
    parser.add_argument("--max-cooc-items-per-user", type=int, default=50)
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--als-factors", type=int, default=64)
    parser.add_argument("--als-iterations", type=int, default=20)
    parser.add_argument("--als-regularization", type=float, default=0.05)
    parser.add_argument("--als-alpha", type=float, default=20.0)
    parser.add_argument("--cosine-batch-size", type=int, default=256)
    parser.add_argument("--hybrid-cooc-weight", type=float, default=0.5)
    parser.add_argument("--export-mart-path", type=Path, default=None)
    parser.add_argument(
        "--export-mart-run",
        choices=("hybrid_rule_als", "cooc_norm", "als_item_cosine", "item2vec_svd"),
        default="hybrid_rule_als",
    )
    parser.add_argument(
        "--export-mart-label-split",
        choices=("train", "test"),
        default="train",
        help=(
            "Label/context split to export. train uses history->rank_label and is safe for "
            "personalization train candidates. test uses history+rank_label->test and leaks final-test labels "
            "if reused for training; select it only for explicit final-test export/evaluation."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    np.random.seed(args.random_seed)
    FEATURE_DIR.mkdir(parents=True, exist_ok=True)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"1. load canonical: {args.canonical_path}")
    history, rank, test = load_canonical(args.canonical_path)
    history, rank, test, cap_summary = apply_caps(
        history,
        rank,
        test,
        max_users=args.max_users,
        max_items=args.max_items,
        max_items_include_test=args.max_items_include_test,
        seed=args.random_seed,
    )

    print("2. build repo2repo labels")
    labels = build_labels(
        history,
        rank,
        test,
        max_anchor_items_per_user=args.max_anchor_items_per_user,
        max_target_items_per_user=args.max_target_items_per_user,
        max_anchors=args.max_anchors,
        max_anchors_order=args.max_anchors_order,
    )
    labels_path = FEATURE_DIR / f"repo2repo_labels_{args.suffix}.parquet"
    labels.to_parquet(labels_path, index=False)
    label_anchors = {
        split: set(labels.loc[labels["label_split"].eq(split), "anchor_repo_id"].astype(int))
        for split in ["train", "test"]
    }

    print("3. generate repo2repo candidates")
    candidates_by_run: list[pd.DataFrame] = []
    embedding_backend: dict[str, str] = {}
    contexts = {
        "train": history,
        "test": pd.concat([history, rank], ignore_index=True),
    }
    if "cooc_norm" in args.methods or "hybrid_rule_als" in args.methods:
        cooc_parts = [
            build_cooc_candidates(
                contexts[split],
                {split: anchors},
                top_k=args.top_k,
                max_items_per_user=args.max_cooc_items_per_user,
            )
            for split, anchors in label_anchors.items()
            if anchors
        ]
        cooc = normalize_candidates(
            pd.concat(cooc_parts, ignore_index=True) if cooc_parts else pd.DataFrame()
        )
        if "cooc_norm" in args.methods:
            candidates_by_run.append(cooc)
    else:
        cooc = normalize_candidates(pd.DataFrame())

    if "als_item_cosine" in args.methods or "hybrid_rule_als" in args.methods:
        als_parts = []
        for split, anchors in label_anchors.items():
            if not anchors:
                continue
            item_ids, emb, backend = item_embeddings_from_als_or_svd(
                contexts[split],
                factors=args.als_factors,
                iterations=args.als_iterations,
                regularization=args.als_regularization,
                alpha=args.als_alpha,
                seed=args.random_seed,
            )
            embedding_backend[split] = backend
            als_parts.append(
                top_cosine_rows(
                    item_ids,
                    emb,
                    {split: anchors},
                    method="als_item_cosine",
                    top_k=args.top_k,
                    batch_size=args.cosine_batch_size,
                )
            )
        als = normalize_candidates(
            pd.concat(als_parts, ignore_index=True) if als_parts else pd.DataFrame()
        )
        if "als_item_cosine" in args.methods:
            candidates_by_run.append(als)
    else:
        als = normalize_candidates(pd.DataFrame())

    if "hybrid_rule_als" in args.methods:
        candidates_by_run.append(build_hybrid(cooc, als, args.top_k, args.hybrid_cooc_weight))

    if "item2vec_svd" in args.methods:
        item2vec_parts = []
        for split, anchors in label_anchors.items():
            if not anchors:
                continue
            item_ids, emb, backend = item_embeddings_from_item2vec_svd(
                contexts[split],
                factors=args.als_factors,
                max_items_per_user=args.max_cooc_items_per_user,
                seed=args.random_seed,
            )
            embedding_backend[f"item2vec_svd_{split}"] = backend
            item2vec_parts.append(
                top_cosine_rows(
                    item_ids,
                    emb,
                    {split: anchors},
                    method="item2vec_svd",
                    top_k=args.top_k,
                    batch_size=args.cosine_batch_size,
                )
            )
        candidates_by_run.append(
            normalize_candidates(
                pd.concat(item2vec_parts, ignore_index=True) if item2vec_parts else pd.DataFrame()
            )
        )

    candidates = normalize_candidates(pd.concat(candidates_by_run, ignore_index=True))
    candidate_paths = write_candidate_files(candidates, args.suffix)

    print("4. evaluate test split")
    metric_rows = [evaluate_run(candidates, labels, run, "test") for run in sorted(candidates["run"].unique())]
    metrics = pd.DataFrame(metric_rows)
    metrics_path = RESULT_DIR / f"repo2repo_eval_metrics_{args.suffix}.csv"
    metrics.to_csv(metrics_path, index=False)

    mart_path = None
    if args.export_mart_path:
        export_mart(
            candidates,
            args.export_mart_run,
            args.export_mart_label_split,
            args.export_mart_path,
        )
        mart_path = str(args.export_mart_path)

    summary = {
        "suffix": args.suffix,
        "canonical_path": str(args.canonical_path),
        "labels_path": str(labels_path),
        "candidate_paths": candidate_paths,
        "metrics_path": str(metrics_path),
        "mart_export_path": mart_path,
        "embedding_backend": embedding_backend,
        "args": vars(args),
        "caps": cap_summary,
        "label_rows": labels.groupby("label_split", observed=True).size().astype(int).to_dict(),
        "candidate_rows": candidates.groupby("run", observed=True).size().astype(int).to_dict(),
        "metrics": metric_rows,
    }
    summary_path = RESULT_DIR / f"repo2repo_eval_summary_{args.suffix}.json"
    write_json(summary_path, summary)

    print(f"labels: {labels_path}")
    for run, path in candidate_paths.items():
        print(f"candidates[{run}]: {path}")
    print(f"metrics: {metrics_path}")
    print(f"summary: {summary_path}")
    if mart_path:
        print(f"mart_export: {mart_path}")
    if not metrics.empty:
        print(metrics.to_string(index=False))


if __name__ == "__main__":
    main()
