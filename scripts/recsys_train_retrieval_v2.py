"""Train V2 retrieval models from canonical rank_label positives."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import torch
from torch import nn
from torch.utils.data import DataLoader, TensorDataset
from tqdm import tqdm

from recsys_v2_common import (
    Paths,
    dump_pickle,
    ensure_dirs,
    labels_by_user,
    load_canonical,
    recommend_users,
    seen_by_user,
    train_als,
    write_json,
)


class TwoTower(nn.Module):
    def __init__(self, n_users: int, n_items: int, embedding_dim: int) -> None:
        super().__init__()
        self.user_embedding = nn.Embedding(n_users, embedding_dim)
        self.item_embedding = nn.Embedding(n_items, embedding_dim)
        nn.init.xavier_uniform_(self.user_embedding.weight)
        nn.init.xavier_uniform_(self.item_embedding.weight)

    def forward(self, user_idx: torch.Tensor, item_idx: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        users = nn.functional.normalize(self.user_embedding(user_idx), dim=1)
        items = nn.functional.normalize(self.item_embedding(item_idx), dim=1)
        return users, items


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suffix", default="latest")
    parser.add_argument("--output-suffix", default=None)
    parser.add_argument("--canonical-path", type=Path, default=None)
    parser.add_argument("--retriever", choices=("als", "two_tower"), default="als")
    parser.add_argument("--candidate-k", type=int, default=300)
    parser.add_argument("--overgenerate", type=int, default=100)
    parser.add_argument("--chunk-size", type=int, default=2048)
    parser.add_argument("--factors", type=int, default=128)
    parser.add_argument("--iterations", type=int, default=30)
    parser.add_argument("--regularization", type=float, default=0.05)
    parser.add_argument("--alpha", type=float, default=20.0)
    parser.add_argument("--embedding-dim", type=int, default=64)
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=8192)
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument("--temperature", type=float, default=0.07)
    parser.add_argument("--torch-threads", type=int, default=1)
    parser.add_argument("--candidate-batch-size", type=int, default=512)
    parser.add_argument(
        "--keep-rank-label-items",
        action="store_true",
        help="Do not filter rank_label train positives from generated two-tower candidates.",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Use smaller ALS/candidate settings for a fast syntax-and-flow check.",
    )
    return parser.parse_args()


def configure_torch_threads(torch_threads: int) -> None:
    torch.set_num_threads(torch_threads)
    try:
        torch.set_num_interop_threads(max(1, min(torch_threads, 4)))
    except RuntimeError:
        pass


def candidate_positive_coverage(
    candidates: pd.DataFrame,
    labels: pd.DataFrame,
) -> dict[str, Any]:
    label_users = set(labels["actor_id"].astype(int).unique())
    if not label_users:
        return {
            "label_users": 0,
            "candidate_users": 0,
            "user_coverage": 0.0,
            "positive_pairs": 0,
            "covered_positive_pairs": 0,
            "positive_pair_coverage": 0.0,
        }

    candidate_users = set(candidates["actor_id"].astype(int).unique())
    label_pairs = labels[["actor_id", "repo_id"]].drop_duplicates()
    candidate_pairs = candidates[["actor_id", "repo_id"]].drop_duplicates()
    covered_pairs = label_pairs.merge(candidate_pairs, on=["actor_id", "repo_id"], how="inner")
    return {
        "label_users": int(len(label_users)),
        "candidate_users": int(len(label_users & candidate_users)),
        "user_coverage": float(len(label_users & candidate_users) / len(label_users)),
        "positive_pairs": int(len(label_pairs)),
        "covered_positive_pairs": int(len(covered_pairs)),
        "positive_pair_coverage": float(len(covered_pairs) / len(label_pairs))
        if len(label_pairs)
        else 0.0,
    }


def empty_candidates() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "actor_id": pd.Series(dtype="int64"),
            "repo_id": pd.Series(dtype="int64"),
            "candidate_rank": pd.Series(dtype="int32"),
            "raw_candidate_rank": pd.Series(dtype="int32"),
            "retrieval_score": pd.Series(dtype="float32"),
        }
    )


def normalize_candidates(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates.empty:
        return empty_candidates()
    return candidates[
        ["actor_id", "repo_id", "candidate_rank", "raw_candidate_rank", "retrieval_score"]
    ].astype(
        {
            "actor_id": "int64",
            "repo_id": "int64",
            "candidate_rank": "int32",
            "raw_candidate_rank": "int32",
            "retrieval_score": "float32",
        }
    )


def train_als_retriever(
    args: argparse.Namespace,
    rank: pd.DataFrame,
    test: pd.DataFrame,
    history_seen: dict[int, set[int]],
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    model, matrix, user2idx, item2idx, idx2item = train_als(
        rank=rank,
        factors=args.factors,
        iterations=args.iterations,
        regularization=args.regularization,
        alpha=args.alpha,
        seed=args.seed,
    )

    print("3. generate candidate cache")
    rank_users = set(rank["actor_id"].astype(int).unique())
    test_users = set(test["actor_id"].astype(int).unique())
    users = sorted((rank_users | test_users) & set(user2idx))
    candidates = recommend_users(
        model=model,
        train_matrix=matrix,
        user2idx=user2idx,
        idx2item=idx2item,
        users=users,
        history_seen=history_seen,
        candidate_k=args.candidate_k,
        overgenerate=args.overgenerate,
        chunk_size=args.chunk_size,
    )
    payload = {
        "model": model,
        "train_positive_split": "rank_label",
        "retriever": "als",
        "args": vars(args),
    }
    mappings = {
        "user2idx": user2idx,
        "item2idx": item2idx,
        "idx2item": idx2item,
    }
    retriever_summary = {
        "name": "als",
        "factors": int(args.factors),
        "iterations": int(args.iterations),
        "regularization": float(args.regularization),
        "alpha": float(args.alpha),
        "seed": int(args.seed),
    }
    return normalize_candidates(candidates), payload, {"mappings": mappings, "summary": retriever_summary}


def train_two_tower_model(
    args: argparse.Namespace,
    rank: pd.DataFrame,
) -> tuple[TwoTower, dict[int, int], dict[int, int], dict[int, int], list[float]]:
    configure_torch_threads(args.torch_threads)
    torch.manual_seed(args.seed)
    np.random.seed(args.seed)
    user_ids = rank["actor_id"].drop_duplicates().astype("int64").to_numpy()
    item_ids = rank["repo_id"].drop_duplicates().astype("int64").to_numpy()
    user2idx = {int(uid): i for i, uid in enumerate(user_ids)}
    item2idx = {int(iid): i for i, iid in enumerate(item_ids)}
    idx2item = {i: iid for iid, i in item2idx.items()}

    user_idx = rank["actor_id"].map(user2idx).to_numpy(dtype=np.int64, copy=False)
    item_idx = rank["repo_id"].map(item2idx).to_numpy(dtype=np.int64, copy=False)
    dataset = TensorDataset(torch.from_numpy(user_idx), torch.from_numpy(item_idx))
    loader = DataLoader(dataset, batch_size=args.batch_size, shuffle=True, drop_last=False)
    if len(loader) == 0:
        raise RuntimeError("two_tower training needs at least one full batch; reduce --batch-size")

    model = TwoTower(len(user2idx), len(item2idx), args.embedding_dim)
    optimizer = torch.optim.AdamW(model.parameters(), lr=args.lr)
    losses: list[float] = []

    for epoch in range(1, args.epochs + 1):
        model.train()
        total_loss = 0.0
        total_batches = 0
        for user_batch, item_batch in tqdm(loader, desc=f"two_tower epoch {epoch}/{args.epochs}"):
            if len(user_batch) < 2:
                continue
            user_vec, item_vec = model(user_batch, item_batch)
            logits = user_vec @ item_vec.T / args.temperature
            target = torch.arange(len(user_batch))
            loss = nn.functional.cross_entropy(logits, target)
            optimizer.zero_grad(set_to_none=True)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), max_norm=5.0)
            optimizer.step()
            total_loss += float(loss.detach().cpu())
            total_batches += 1
        losses.append(total_loss / max(1, total_batches))
    return model, user2idx, item2idx, idx2item, losses


@torch.no_grad()
def generate_two_tower_candidates(
    model: TwoTower,
    user2idx: dict[int, int],
    idx2item: dict[int, int],
    users: list[int],
    history_seen: dict[int, set[int]],
    candidate_k: int,
    overgenerate: int,
    batch_size: int,
) -> pd.DataFrame:
    model.eval()
    item_vectors = nn.functional.normalize(model.item_embedding.weight.detach(), dim=1)
    valid_users = [u for u in users if u in user2idx]
    n = min(item_vectors.shape[0], candidate_k + overgenerate)
    rows: list[dict[str, Any]] = []
    for start in tqdm(range(0, len(valid_users), batch_size), desc="two_tower candidates"):
        chunk = valid_users[start : start + batch_size]
        idxs = torch.tensor([user2idx[u] for u in chunk], dtype=torch.long)
        user_vectors = nn.functional.normalize(model.user_embedding(idxs), dim=1)
        scores = user_vectors @ item_vectors.T
        top_scores, top_indices = torch.topk(scores, k=n, dim=1)
        for row_idx, uid in enumerate(chunk):
            seen = history_seen.get(uid, set())
            kept = 0
            for raw_rank, item_idx in enumerate(top_indices[row_idx].tolist(), start=1):
                repo_id = int(idx2item[int(item_idx)])
                if repo_id in seen:
                    continue
                kept += 1
                rows.append(
                    {
                        "actor_id": int(uid),
                        "repo_id": repo_id,
                        "candidate_rank": kept,
                        "raw_candidate_rank": raw_rank,
                        "retrieval_score": float(top_scores[row_idx, raw_rank - 1].item()),
                    }
                )
                if kept >= candidate_k:
                    break
    return normalize_candidates(pd.DataFrame(rows))


def train_two_tower_retriever(
    args: argparse.Namespace,
    rank: pd.DataFrame,
    test: pd.DataFrame,
    history_seen: dict[int, set[int]],
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    print("2. train Two-Tower from rank_label positives")
    model, user2idx, item2idx, idx2item, losses = train_two_tower_model(args, rank)
    print("3. generate candidate cache")
    rank_users = set(rank["actor_id"].astype(int).unique())
    test_users = set(test["actor_id"].astype(int).unique())
    users = sorted((rank_users | test_users) & set(user2idx))
    if args.keep_rank_label_items:
        train_seen = history_seen
        filter_seen_splits = ["history"]
    else:
        train_seen = {
            uid: set(history_seen.get(uid, set())) | repos
            for uid, repos in seen_by_user(rank).items()
        }
        for uid, repos in history_seen.items():
            train_seen.setdefault(uid, set()).update(repos)
        filter_seen_splits = ["history", "rank_label"]
    candidates = generate_two_tower_candidates(
        model=model,
        user2idx=user2idx,
        idx2item=idx2item,
        users=users,
        history_seen=train_seen,
        candidate_k=args.candidate_k,
        overgenerate=args.overgenerate,
        batch_size=args.candidate_batch_size,
    )
    payload = {
        "model_type": "two_tower",
        "state_dict": {key: value.detach().cpu() for key, value in model.state_dict().items()},
        "train_positive_split": "rank_label",
        "args": vars(args),
        "n_users": len(user2idx),
        "n_items": len(item2idx),
        "embedding_dim": int(args.embedding_dim),
        "losses": losses,
    }
    mappings = {
        "user2idx": user2idx,
        "item2idx": item2idx,
        "idx2item": idx2item,
    }
    retriever_summary = {
        "name": "two_tower",
        "embedding_dim": int(args.embedding_dim),
        "epochs": int(args.epochs),
        "batch_size": int(args.batch_size),
        "lr": float(args.lr),
        "temperature": float(args.temperature),
        "torch_threads": int(args.torch_threads),
        "losses": losses,
        "filter_seen_splits": filter_seen_splits,
        "seed": int(args.seed),
    }
    return candidates, payload, {"mappings": mappings, "summary": retriever_summary}


def main() -> None:
    args = parse_args()
    ensure_dirs()
    if args.smoke:
        args.factors = min(args.factors, 32)
        args.iterations = min(args.iterations, 5)
        args.candidate_k = min(args.candidate_k, 80)
        args.overgenerate = min(args.overgenerate, 40)
        args.embedding_dim = min(args.embedding_dim, 16)
        args.epochs = min(args.epochs, 1)
        args.batch_size = min(args.batch_size, 16)
        args.candidate_batch_size = min(args.candidate_batch_size, 32)

    output_suffix = args.output_suffix or args.suffix
    paths = Paths(output_suffix)
    canonical_path = args.canonical_path or Paths(args.suffix).canonical

    print("1. load canonical dataset")
    history, rank, test = load_canonical(canonical_path)
    if rank.empty:
        raise RuntimeError("rank_label split is empty; cannot train V2 retrieval")

    history_seen = seen_by_user(history)
    if args.retriever == "als":
        print("2. train ALS from rank_label positives")
        candidates, payload, extra = train_als_retriever(args, rank, test, history_seen)
    else:
        candidates, payload, extra = train_two_tower_retriever(args, rank, test, history_seen)

    paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    candidates.to_parquet(paths.candidates, index=False)

    mappings_path = paths.retrieval_model.with_name(f"retrieval_mappings_{output_suffix}.pkl")
    dump_pickle(paths.retrieval_model, payload)
    dump_pickle(mappings_path, extra["mappings"])

    rank_labels = labels_by_user(rank)
    test_labels = labels_by_user(test)
    candidate_users = set(candidates["actor_id"].astype(int).unique())
    summary = {
        "suffix": output_suffix,
        "base_suffix": args.suffix,
        "canonical_path": str(canonical_path),
        "retriever": args.retriever,
        "train_positive_split": "rank_label",
        "train_positive_pairs": int(len(rank)),
        "train_positive_users": int(rank["actor_id"].nunique()),
        "train_positive_items": int(rank["repo_id"].nunique()),
        "history_context_pairs": int(len(history)),
        "history_context_users": int(history["actor_id"].nunique()),
        "history_context_items": int(history["repo_id"].nunique()),
        "candidate_users": int(len(candidate_users)),
        "candidate_rows": int(len(candidates)),
        "candidate_k": int(args.candidate_k),
        "rank_user_coverage": candidate_positive_coverage(candidates, rank),
        "test_user_coverage": candidate_positive_coverage(candidates, test),
        "rank_label_users_with_candidates": int(len(set(rank_labels) & candidate_users)),
        "test_users_with_candidates": int(len(set(test_labels) & candidate_users)),
        "retriever_params": extra["summary"],
        "paths": {
            "model": str(paths.retrieval_model),
            "mappings": str(mappings_path),
            "candidates": str(paths.candidates),
            "summary": str(paths.retrieval_summary),
        },
    }
    write_json(paths.retrieval_summary, summary)

    print(f"wrote {paths.retrieval_model}")
    print(f"wrote {mappings_path}")
    print(f"wrote {paths.candidates}")
    print(f"wrote {paths.retrieval_summary}")


if __name__ == "__main__":
    main()
