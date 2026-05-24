"""Evaluate the V2 retrieval/re-rank pipeline on the held-out test split."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

import recsys_neural_rankers as neural
from recsys_v2_common import (
    FEATURE_COLUMNS,
    RESULT_DIR,
    SOURCE_HARD,
    Paths,
    attach_features,
    feature_stats,
    labels_by_user,
    load_canonical,
    load_pickle,
    precision_recall_ndcg,
    write_json,
)


NEURAL_MODEL_TYPES = {f"neural_{name}" for name in neural.NEURAL_RANKERS}


def parse_k_values(value: str) -> list[int]:
    out = sorted({int(part.strip()) for part in value.split(",") if part.strip()})
    if not out or any(k <= 0 for k in out):
        raise argparse.ArgumentTypeError("k values must be positive integers")
    return out


def ranker_predict(
    model_payload,
    x: pd.DataFrame,
    device: str,
    batch_size: int,
    actor_ids: np.ndarray | None = None,
    repo_ids: np.ndarray | None = None,
) -> np.ndarray:
    model = model_payload.get("model", model_payload) if isinstance(model_payload, dict) else model_payload
    if isinstance(model_payload, dict) and model_payload.get("model_type") in NEURAL_MODEL_TYPES:
        torch_device = neural.choose_device(device)
        return neural.predict_payload(
            model_payload,
            x.to_numpy(dtype=np.float32, copy=False),
            torch_device,
            batch_size,
            actor_ids=actor_ids,
            repo_ids=repo_ids,
        )
    if hasattr(model, "booster_"):
        scores = []
        for start in range(0, len(x), batch_size):
            scores.append(model.booster_.predict(x.iloc[start : start + batch_size]))
        return np.concatenate(scores).astype(np.float32, copy=False)
    scores = []
    for start in range(0, len(x), batch_size):
        scores.append(model.predict(x.iloc[start : start + batch_size]))
    return np.concatenate(scores).astype(np.float32, copy=False)


def load_feature_names(model_payload) -> list[str]:
    if isinstance(model_payload, dict) and model_payload.get("feature_names"):
        return list(model_payload["feature_names"])
    return FEATURE_COLUMNS


def display_name(model_payload) -> str:
    if isinstance(model_payload, dict) and model_payload.get("display_name"):
        return str(model_payload["display_name"])
    return "LGBM Re-rank"


def id_embedding_eval_unknowns(
    model_payload,
    actor_ids: np.ndarray,
    repo_ids: np.ndarray,
) -> dict[str, int] | None:
    if not isinstance(model_payload, dict) or not neural.uses_id_embeddings(str(model_payload.get("model_name", ""))):
        return None
    actor_vocab = {int(value) for value in model_payload.get("actor_id_vocab", {})}
    repo_vocab = {int(value) for value in model_payload.get("repo_id_vocab", {})}
    actor_unknown = np.fromiter(
        (int(value) not in actor_vocab for value in actor_ids),
        dtype=bool,
        count=len(actor_ids),
    )
    repo_unknown = np.fromiter(
        (int(value) not in repo_vocab for value in repo_ids),
        dtype=bool,
        count=len(repo_ids),
    )
    return {
        "unknown_actor_rows": int(actor_unknown.sum()),
        "unknown_actor_unique": int(len({int(value) for value in actor_ids[actor_unknown]})),
        "unknown_repo_rows": int(repo_unknown.sum()),
        "unknown_repo_unique": int(len({int(value) for value in repo_ids[repo_unknown]})),
    }


def evaluate_user_groups(
    candidates: pd.DataFrame,
    test_labels: dict[int, set[int]],
    k_values: list[int],
    score_col: str,
) -> pd.DataFrame:
    metrics = {
        k: {"precision": [], "recall": [], "ndcg": [], "coverage": set()}
        for k in k_values
    }
    eval_users = 0
    for uid, part in candidates.groupby("actor_id", observed=True):
        relevant = test_labels.get(int(uid), set())
        if not relevant:
            continue
        eval_users += 1
        recs = part.sort_values(score_col, ascending=False)["repo_id"].astype(int).tolist()
        for k in k_values:
            p, r, n = precision_recall_ndcg(recs, relevant, k)
            metrics[k]["precision"].append(p)
            metrics[k]["recall"].append(r)
            metrics[k]["ndcg"].append(n)
            metrics[k]["coverage"].update(recs[:k])

    rows = []
    for k in k_values:
        rows.append(
            {
                "k": k,
                "eval_users": eval_users,
                "precision": float(np.mean(metrics[k]["precision"])) if metrics[k]["precision"] else 0.0,
                "recall": float(np.mean(metrics[k]["recall"])) if metrics[k]["recall"] else 0.0,
                "ndcg": float(np.mean(metrics[k]["ndcg"])) if metrics[k]["ndcg"] else 0.0,
                "unique_recommended": len(metrics[k]["coverage"]),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--suffix", default="latest")
    parser.add_argument("--canonical-path", type=Path, default=None)
    parser.add_argument("--candidate-path", type=Path, default=None)
    parser.add_argument("--ranker-path", type=Path, default=None)
    parser.add_argument("--k-values", type=parse_k_values, default=[10, 50, 100, 200])
    parser.add_argument("--device", type=str, default="cpu")
    parser.add_argument("--predict-batch-size", type=int, default=262144)
    args = parser.parse_args()

    paths = Paths(args.suffix)
    canonical_path = args.canonical_path or paths.canonical
    candidate_path = args.candidate_path or paths.candidates
    ranker_path = args.ranker_path or paths.ranker_model

    RESULT_DIR.mkdir(parents=True, exist_ok=True)

    history, rank, test = load_canonical(canonical_path)
    test_labels = labels_by_user(test)
    candidates = pd.read_parquet(candidate_path)
    if candidates.empty:
        raise RuntimeError(f"candidate cache is empty: {candidate_path}")

    candidates = candidates.copy()
    candidates["source"] = SOURCE_HARD
    candidates["label"] = [
        1 if int(row.repo_id) in test_labels.get(int(row.actor_id), set()) else 0
        for row in candidates[["actor_id", "repo_id"]].itertuples(index=False)
    ]
    candidates["retrieval_sort_score"] = -candidates["candidate_rank"].astype(float)

    retrieval_metrics = evaluate_user_groups(
        candidates,
        test_labels,
        args.k_values,
        "retrieval_sort_score",
    )
    retrieval_metrics.insert(0, "model", "ALS Retrieval")

    model_payload = load_pickle(ranker_path)
    feature_names = load_feature_names(model_payload)
    missing = sorted(set(feature_names) - set(FEATURE_COLUMNS))
    if missing:
        raise RuntimeError(f"ranker feature names are not supported by eval: {missing}")

    stats = feature_stats(history, rank)
    featured = attach_features(candidates, stats)
    featured["rerank_score"] = ranker_predict(
        model_payload,
        featured[feature_names],
        args.device,
        args.predict_batch_size,
        actor_ids=featured["actor_id"].to_numpy(dtype=np.int64, copy=False),
        repo_ids=featured["repo_id"].to_numpy(dtype=np.int64, copy=False),
    )
    rerank_metrics = evaluate_user_groups(
        featured,
        test_labels,
        args.k_values,
        "rerank_score",
    )
    rerank_metrics.insert(0, "model", display_name(model_payload))

    metrics = pd.concat([retrieval_metrics, rerank_metrics], ignore_index=True)
    metrics.to_csv(paths.eval_metrics, index=False)

    candidate_recall = retrieval_metrics[["k", "recall"]].rename(
        columns={"recall": "candidate_recall"}
    )
    rerank_ndcg = rerank_metrics[["k", "ndcg"]].rename(columns={"ndcg": "rerank_ndcg"})
    summary = {
        "suffix": args.suffix,
        "canonical_path": str(canonical_path),
        "candidate_path": str(candidate_path),
        "ranker_path": str(ranker_path),
        "eval_positive_split": "test",
        "train_positive_split": "rank_label",
        "test_positive_count": int(len(test)),
        "test_users": int(test["actor_id"].nunique()),
        "candidate_rows": int(len(candidates)),
        "candidate_users": int(candidates["actor_id"].nunique()),
        "candidate_recall": candidate_recall.to_dict(orient="records"),
        "rerank_ndcg": rerank_ndcg.to_dict(orient="records"),
        "metrics_path": str(paths.eval_metrics),
    }
    id_unknowns = id_embedding_eval_unknowns(
        model_payload,
        featured["actor_id"].to_numpy(dtype=np.int64, copy=False),
        featured["repo_id"].to_numpy(dtype=np.int64, copy=False),
    )
    if id_unknowns is not None:
        summary["id_embedding_eval_unknowns"] = id_unknowns
    write_json(paths.eval_summary, summary)
    print(metrics.to_string(index=False))
    print(f"saved: {paths.eval_metrics}")
    print(f"saved: {paths.eval_summary}")


if __name__ == "__main__":
    main()
