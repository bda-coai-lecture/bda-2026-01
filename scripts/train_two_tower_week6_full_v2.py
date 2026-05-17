"""Train feature-rich Two-Tower on the Week 6 best-full catalog.

This script intentionally does not retrain ALS. It reuses the Week 6
best-full ALS model and mappings so Two-Tower is evaluated on the same
300k-item catalog.

Usage:
    OMP_NUM_THREADS=1 uv run python scripts/train_two_tower_week6_full_v2.py
"""

from __future__ import annotations

import argparse
import json
import math
import pickle
import time
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from scipy import sparse
from torch import nn
from torch.utils.data import DataLoader, TensorDataset
from tqdm import tqdm


MART_DIR = Path("data/marts/week6")
MODEL_DIR = Path("data/models/week6")
DEFAULT_SUFFIX = "related80_anchor20_full_als96_i12_lgbm63"
K_VALUES = [10, 50, 100]


USER_DENSE_COLS = [
    "total_score",
    "unique_repos",
    "active_days",
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

ITEM_DENSE_COLS = [
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
    "score_growth_ratio",
    "user_growth_ratio",
    "stars",
    "forks",
    "archived",
]

LOG1P_COLS = {
    "total_score",
    "unique_repos",
    "active_days",
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
    "stars",
    "forks",
}


class TwoTower(nn.Module):
    def __init__(
        self,
        n_users: int,
        n_items: int,
        n_langs: int,
        user_feat_dim: int,
        item_feat_dim: int,
        embed_dim: int,
    ) -> None:
        super().__init__()
        self.user_embed = nn.Embedding(n_users, embed_dim)
        self.item_embed = nn.Embedding(n_items, embed_dim)
        self.lang_embed = nn.Embedding(n_langs + 1, 8)
        self.user_mlp = nn.Sequential(
            nn.Linear(embed_dim + user_feat_dim, embed_dim),
            nn.LayerNorm(embed_dim),
            nn.ReLU(),
            nn.Linear(embed_dim, embed_dim),
        )
        self.item_mlp = nn.Sequential(
            nn.Linear(embed_dim + 8 + item_feat_dim, embed_dim),
            nn.LayerNorm(embed_dim),
            nn.ReLU(),
            nn.Linear(embed_dim, embed_dim),
        )
        nn.init.xavier_uniform_(self.user_embed.weight)
        nn.init.xavier_uniform_(self.item_embed.weight)

    def forward(
        self,
        user_idx: torch.Tensor,
        item_idx: torch.Tensor,
        user_feats: torch.Tensor,
        item_feats: torch.Tensor,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        user_input = torch.cat([self.user_embed(user_idx), user_feats], dim=1)
        user_vec = nn.functional.normalize(self.user_mlp(user_input), dim=1)
        item_emb = self.item_embed(item_idx)
        lang = self.lang_embed(item_feats[:, -1].clamp(0).long())
        item_input = torch.cat([item_emb, lang, item_feats[:, :-1]], dim=1)
        item_vec = nn.functional.normalize(self.item_mlp(item_input), dim=1)
        return user_vec, item_vec

    @torch.no_grad()
    def get_item_vectors(
        self,
        n_items: int,
        item_feat_tensor: torch.Tensor,
        batch_size: int,
    ) -> np.ndarray:
        vecs: list[np.ndarray] = []
        self.eval()
        for start in tqdm(range(0, n_items, batch_size), desc="item vectors"):
            end = min(start + batch_size, n_items)
            idxs = torch.arange(start, end)
            item_emb = self.item_embed(idxs)
            batch_feats = item_feat_tensor[start:end]
            lang = self.lang_embed(batch_feats[:, -1].clamp(0).long())
            item_input = torch.cat([item_emb, lang, batch_feats[:, :-1]], dim=1)
            item_vec = nn.functional.normalize(self.item_mlp(item_input), dim=1)
            vecs.append(item_vec.cpu().numpy().astype(np.float32))
        return np.vstack(vecs)

    @torch.no_grad()
    def get_user_vectors(
        self,
        user_feat_tensor: torch.Tensor,
        batch_size: int,
    ) -> np.ndarray:
        vecs: list[np.ndarray] = []
        self.eval()
        n_users = len(user_feat_tensor)
        for start in tqdm(range(0, n_users, batch_size), desc="user vectors"):
            end = min(start + batch_size, n_users)
            idxs = torch.arange(start, end)
            user_input = torch.cat([self.user_embed(idxs), user_feat_tensor[start:end]], dim=1)
            user_vec = nn.functional.normalize(self.user_mlp(user_input), dim=1)
            vecs.append(user_vec.cpu().numpy().astype(np.float32))
        return np.vstack(vecs)


def safe_log1p(value: object) -> float:
    if value is None or pd.isna(value):
        return 0.0
    return float(np.log1p(max(float(value), 0.0)))


def normalize_dense(values: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    values = np.nan_to_num(values.astype(np.float32), nan=0.0, posinf=0.0, neginf=0.0)
    mean = values.mean(axis=0, keepdims=True)
    std = values.std(axis=0, keepdims=True)
    std[std < 1e-6] = 1.0
    return ((values - mean) / std).astype(np.float32), mean.squeeze(0), std.squeeze(0)


def transform_numeric_frame(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = df[columns].copy()
    for col in columns:
        values = pd.to_numeric(out[col], errors="coerce").fillna(0.0).clip(lower=0.0 if col in LOG1P_COLS else None)
        if col in LOG1P_COLS:
            values = np.log1p(values)
        out[col] = values
    return out.astype("float32")


def precision_recall_ndcg(recommended: list[int], relevant: set[int], k: int) -> tuple[float, float, float]:
    recs = recommended[:k]
    hits = set(recs) & relevant
    precision = len(hits) / k
    recall = len(hits) / len(relevant) if relevant else 0.0
    dcg = sum(1.0 / math.log2(rank + 2) for rank, rid in enumerate(recs) if rid in relevant)
    idcg = sum(1.0 / math.log2(rank + 2) for rank in range(min(len(relevant), k)))
    ndcg = dcg / idcg if idcg else 0.0
    return precision, recall, ndcg


def load_feedback(path: Path, valid_users: set[int], valid_items: set[int], split: str | None = None) -> pd.DataFrame:
    columns = ["actor_id", "repo_id", "weighted_score"]
    if split is not None:
        columns = ["split", *columns]
    df = pd.read_parquet(path, columns=columns)
    if split is not None:
        df = df[df["split"] == split].drop(columns=["split"])
    df = df[
        df["actor_id"].isin(valid_users)
        & df["repo_id"].isin(valid_items)
        & df["weighted_score"].notna()
    ]
    return (
        df.rename(columns={"weighted_score": "score"})
        .astype({"actor_id": "int64", "repo_id": "int64", "score": "float32"})
        .reset_index(drop=True)
    )


def build_user_features(mart_dir: Path, user2idx: dict[int, int]) -> tuple[np.ndarray, dict]:
    user_profile = pd.read_parquet(
        mart_dir / "user_profile_mart.parquet",
        columns=["actor_id", *USER_DENSE_COLS],
    )
    user_profile = user_profile[user_profile["actor_id"].isin(user2idx)]
    raw = np.zeros((len(user2idx), len(USER_DENSE_COLS)), dtype=np.float32)
    dense = transform_numeric_frame(user_profile, USER_DENSE_COLS).to_numpy(dtype=np.float32)
    for actor_id, row in zip(user_profile["actor_id"].to_numpy(), dense):
        raw[user2idx[int(actor_id)]] = row
    normed, mean, std = normalize_dense(raw)
    stats = {"columns": USER_DENSE_COLS, "mean": mean.tolist(), "std": std.tolist()}
    return normed, stats


def build_item_features(mart_dir: Path, item2idx: dict[int, int]) -> tuple[np.ndarray, dict[str, int], dict]:
    repo_feature = pd.read_parquet(
        mart_dir / "repo_feature_mart.parquet",
        columns=["repo_id", "language", *ITEM_DENSE_COLS],
    )
    repo_feature = repo_feature[repo_feature["repo_id"].isin(item2idx)]
    languages = sorted(
        lang for lang in repo_feature["language"].dropna().unique().tolist() if isinstance(lang, str)
    )
    lang2idx = {lang: i + 1 for i, lang in enumerate(languages)}
    raw_dense = np.zeros((len(item2idx), len(ITEM_DENSE_COLS)), dtype=np.float32)
    dense = transform_numeric_frame(repo_feature, ITEM_DENSE_COLS).to_numpy(dtype=np.float32)
    lang_arr = np.zeros((len(item2idx), 1), dtype=np.float32)
    for row, dense_row in zip(repo_feature.itertuples(index=False), dense):
        idx = item2idx[int(row.repo_id)]
        raw_dense[idx] = dense_row
        lang_arr[idx, 0] = lang2idx.get(row.language, 0)
    normed, mean, std = normalize_dense(raw_dense)
    item_feat = np.hstack([normed, lang_arr]).astype(np.float32)
    stats = {"columns": ITEM_DENSE_COLS, "mean": mean.tolist(), "std": std.tolist()}
    return item_feat, lang2idx, stats


def make_sparse(train_fb: pd.DataFrame, user2idx: dict[int, int], item2idx: dict[int, int]) -> sparse.csr_matrix:
    rows = train_fb["actor_id"].map(user2idx).to_numpy()
    cols = train_fb["repo_id"].map(item2idx).to_numpy()
    data = train_fb["score"].to_numpy(dtype=np.float32)
    return sparse.csr_matrix((data, (rows, cols)), shape=(len(user2idx), len(item2idx)))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mart-dir", type=Path, default=MART_DIR)
    parser.add_argument("--model-dir", type=Path, default=MODEL_DIR)
    parser.add_argument("--suffix", type=str, default=DEFAULT_SUFFIX)
    parser.add_argument("--epochs", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=2048)
    parser.add_argument("--embed-dim", type=int, default=64)
    parser.add_argument("--temperature", type=float, default=0.05)
    parser.add_argument("--eval-users", type=int, default=30_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--item-vector-batch-size", type=int, default=50_000)
    parser.add_argument("--user-vector-batch-size", type=int, default=50_000)
    parser.add_argument("--output-suffix", type=str, default="week6_full_v2")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    started = time.time()
    rng = np.random.default_rng(args.seed)
    torch.manual_seed(args.seed)

    mappings_path = args.model_dir / f"mappings_{args.suffix}.pkl"
    als_path = args.model_dir / f"als_{args.suffix}.pkl"
    print(f"1. load saved ALS/mappings: {args.suffix}")
    mappings = pickle.loads(mappings_path.read_bytes())
    als_model = pickle.loads(als_path.read_bytes())
    user2idx: dict[int, int] = mappings["user2idx"]
    item2idx: dict[int, int] = mappings["item2idx"]
    idx2item: dict[int, int] = mappings["idx2item"]
    valid_users = set(user2idx)
    valid_items = set(item2idx)
    print(f"   users={len(user2idx):,}, items={len(item2idx):,}")

    print("2. load filtered marts")
    train_fb = load_feedback(
        args.mart_dir / "user_repo_interaction_mart.parquet",
        valid_users,
        valid_items,
    )
    test_fb = load_feedback(
        args.mart_dir / "experiment_split_mart.parquet",
        valid_users,
        valid_items,
        split="test",
    )
    print(f"   train={len(train_fb):,}, test={len(test_fb):,}")

    print("3. build features/matrix")
    user_feat, user_feat_stats = build_user_features(args.mart_dir, user2idx)
    item_feat, lang2idx, item_feat_stats = build_item_features(args.mart_dir, item2idx)
    train_sparse = make_sparse(train_fb, user2idx, item2idx)
    train_seen = train_fb.groupby("actor_id", observed=True)["repo_id"].apply(set).to_dict()
    test_labels = test_fb.groupby("actor_id", observed=True)["repo_id"].apply(set).to_dict()
    eval_users = sorted(test_labels)
    if len(eval_users) > args.eval_users:
        eval_users = rng.choice(np.array(eval_users), size=args.eval_users, replace=False).tolist()
    print(
        f"   eval warm users={len(eval_users):,}, languages={len(lang2idx):,}, "
        f"user_features={user_feat.shape[1]}, item_features={item_feat.shape[1] - 1}"
    )

    print("4. train Two-Tower")
    model = TwoTower(
        len(user2idx),
        len(item2idx),
        len(lang2idx),
        user_feat.shape[1],
        item_feat.shape[1] - 1,
        args.embed_dim,
    )
    user_t = torch.tensor(train_fb["actor_id"].map(user2idx).to_numpy(), dtype=torch.long)
    item_t = torch.tensor(train_fb["repo_id"].map(item2idx).to_numpy(), dtype=torch.long)
    user_feat_t = torch.tensor(user_feat[user_t.numpy()], dtype=torch.float32)
    feat_t = torch.tensor(item_feat[item_t.numpy()], dtype=torch.float32)
    loader = DataLoader(
        TensorDataset(user_t, item_t, user_feat_t, feat_t),
        batch_size=args.batch_size,
        shuffle=True,
        drop_last=True,
    )
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-3, weight_decay=1e-5)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=args.epochs)
    losses: list[float] = []
    for epoch in range(1, args.epochs + 1):
        model.train()
        total_loss = 0.0
        total_batches = 0
        t0 = time.time()
        for bu, bi, buf, bif in tqdm(loader, desc=f"epoch {epoch}/{args.epochs}"):
            user_vec, item_vec = model(bu, bi, buf, bif)
            logits = user_vec @ item_vec.T / args.temperature
            loss = nn.functional.cross_entropy(logits, torch.arange(len(bu)))
            optimizer.zero_grad(set_to_none=True)
            loss.backward()
            optimizer.step()
            total_loss += float(loss.detach().cpu())
            total_batches += 1
        scheduler.step()
        mean_loss = total_loss / max(1, total_batches)
        losses.append(mean_loss)
        print(f"   epoch {epoch}: loss={mean_loss:.4f}, elapsed={time.time() - t0:.1f}s")

    print("5. build FAISS index")
    import faiss

    model.eval()
    user_vectors = model.get_user_vectors(
        torch.tensor(user_feat),
        args.user_vector_batch_size,
    )
    item_vectors = model.get_item_vectors(
        len(item2idx),
        torch.tensor(item_feat),
        args.item_vector_batch_size,
    )
    tt_index = faiss.IndexFlatIP(args.embed_dim)
    tt_index.add(np.ascontiguousarray(item_vectors))

    print("6. evaluate saved ALS vs Two-Tower")
    pop_scores = train_fb.groupby("repo_id", observed=True)["score"].sum().sort_values(ascending=False)
    pop_candidates = pop_scores.head(max(K_VALUES) + 1000).index.tolist()
    metrics = {
        name: {k: {"precision": [], "recall": [], "ndcg": []} for k in K_VALUES}
        for name in ["Popularity", "ALS", "Two-Tower"]
    }
    coverage = {name: {k: set() for k in K_VALUES} for name in metrics}
    max_k = max(K_VALUES)
    for start in tqdm(range(0, len(eval_users), 1000), desc="eval"):
        chunk = eval_users[start : start + 1000]
        idxs = np.array([user2idx[uid] for uid in chunk])
        als_item_idxs, _ = als_model.recommend(
            idxs,
            train_sparse[idxs],
            N=max_k + 100,
            filter_already_liked_items=True,
        )
        _, tt_item_idxs = tt_index.search(user_vectors[idxs], max_k + 100)
        for row_idx, uid in enumerate(chunk):
            seen = train_seen.get(uid, set())
            relevant = test_labels[uid]
            recs_by_model = {
                "Popularity": [rid for rid in pop_candidates if rid not in seen][:max_k],
                "ALS": [
                    idx2item[int(iidx)]
                    for iidx in als_item_idxs[row_idx]
                    if int(iidx) in idx2item and idx2item[int(iidx)] not in seen
                ][:max_k],
                "Two-Tower": [
                    idx2item[int(iidx)]
                    for iidx in tt_item_idxs[row_idx]
                    if int(iidx) in idx2item and idx2item[int(iidx)] not in seen
                ][:max_k],
            }
            for name, recs in recs_by_model.items():
                for k in K_VALUES:
                    p, r, n = precision_recall_ndcg(recs, relevant, k)
                    metrics[name][k]["precision"].append(p)
                    metrics[name][k]["recall"].append(r)
                    metrics[name][k]["ndcg"].append(n)
                    coverage[name][k].update(recs[:k])

    rows = []
    for name in ["Popularity", "ALS", "Two-Tower"]:
        for k in K_VALUES:
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
    results = pd.DataFrame(rows)

    output_prefix = args.model_dir / f"two_tower_{args.output_suffix}"
    model_path = output_prefix.with_suffix(".pt")
    metrics_path = args.model_dir / f"two_tower_{args.output_suffix}_metrics.csv"
    summary_path = args.model_dir / f"two_tower_{args.output_suffix}_summary.json"
    torch.save(
        {
            "model_state": model.state_dict(),
            "n_users": len(user2idx),
            "n_items": len(item2idx),
            "n_langs": len(lang2idx),
            "user_feat_dim": user_feat.shape[1],
            "item_feat_dim": item_feat.shape[1] - 1,
            "embed_dim": args.embed_dim,
            "user2idx": user2idx,
            "item2idx": item2idx,
            "idx2item": idx2item,
            "user_feat": user_feat,
            "item_feat": item_feat,
            "user_feat_stats": user_feat_stats,
            "item_feat_stats": item_feat_stats,
            "lang2idx": lang2idx,
            "source_suffix": args.suffix,
        },
        model_path,
    )
    results.to_csv(metrics_path, index=False)
    summary_path.write_text(
        json.dumps(
            {
                "args": vars(args) | {"mart_dir": str(args.mart_dir), "model_dir": str(args.model_dir)},
                "source_suffix": args.suffix,
                "train_interactions": int(len(train_fb)),
                "test_interactions": int(len(test_fb)),
                "eval_users": int(len(eval_users)),
                "user_feature_columns": USER_DENSE_COLS,
                "item_feature_columns": ITEM_DENSE_COLS,
                "language_count": len(lang2idx),
                "losses": losses,
                "elapsed_min": round((time.time() - started) / 60, 2),
                "paths": {
                    "model": str(model_path),
                    "metrics": str(metrics_path),
                    "summary": str(summary_path),
                },
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    print("\nresults")
    print(results.to_string(index=False))
    print(f"\nsaved model: {model_path}")
    print(f"saved metrics: {metrics_path}")
    print(f"saved summary: {summary_path}")


if __name__ == "__main__":
    main()
