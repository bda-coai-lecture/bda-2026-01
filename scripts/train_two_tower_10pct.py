"""Two-Tower 학습 + ALS 비교 평가 (10% 샘플).

Usage:
    OMP_NUM_THREADS=1 uv run python scripts/train_two_tower_10pct.py
"""

import math
import pickle
import sqlite3
import time
import argparse
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from scipy import sparse
from torch.utils.data import DataLoader, TensorDataset
from tqdm import tqdm

from gharchive.loader import load_period
from ghrec.recommend import popularity_scores

OUTPUT_DIR = Path("data/daily_agg")
MODEL_DIR = Path("data/models")
MART_DIR = Path("data/marts/week6")
DB_PATH = Path("data/repo_metadata.db")

TRAIN_START, TRAIN_END = date(2026, 3, 15), date(2026, 3, 28)
TEST_START, TEST_END = date(2026, 3, 29), date(2026, 4, 3)
WEIGHTS = {
    "WatchEvent": 1.0, "ForkEvent": 2.0, "IssuesEvent": 0.5,
    "PullRequestEvent": 3.0, "IssueCommentEvent": 0.3, "PushEvent": 0.2,
}
SAMPLE_RATIO = 0.10
EMBED_DIM = 64
BATCH_SIZE = 2048
N_EPOCHS = 5
K_VALUES = [10, 50, 100]


class TwoTower(nn.Module):
    def __init__(self, n_users, n_items, n_langs, embed_dim=64):
        super().__init__()
        self.user_embed = nn.Embedding(n_users, embed_dim)
        self.item_embed = nn.Embedding(n_items, embed_dim)
        self.lang_embed = nn.Embedding(n_langs + 1, 8)
        self.item_mlp = nn.Sequential(
            nn.Linear(embed_dim + 8 + 2, embed_dim),
            nn.ReLU(),
            nn.Linear(embed_dim, embed_dim),
        )
        nn.init.xavier_uniform_(self.user_embed.weight)
        nn.init.xavier_uniform_(self.item_embed.weight)

    def forward(self, user_idx, item_idx, item_feats):
        u = nn.functional.normalize(self.user_embed(user_idx), dim=1)
        i_emb = self.item_embed(item_idx)
        lang = self.lang_embed(item_feats[:, 2].clamp(0).long())
        i = nn.functional.normalize(
            self.item_mlp(torch.cat([i_emb, lang, item_feats[:, :2]], dim=1)), dim=1
        )
        return u, i

    @torch.no_grad()
    def get_all_item_vectors(self, n_items, item_feat_tensor):
        vecs = []
        for start in range(0, n_items, 50_000):
            end = min(start + 50_000, n_items)
            idxs = torch.arange(start, end)
            i_emb = self.item_embed(idxs)
            lang = self.lang_embed(item_feat_tensor[start:end, 2].clamp(0).long())
            inp = torch.cat([i_emb, lang, item_feat_tensor[start:end, :2]], dim=1)
            v = nn.functional.normalize(self.item_mlp(inp), dim=1)
            vecs.append(v.numpy())
        return np.vstack(vecs).astype(np.float32)


def load_mart_feedback(path: Path, split: str | None = None) -> pd.DataFrame:
    columns = ["actor_id", "repo_id", "weighted_score"]
    if split is not None:
        columns = ["split", *columns]
    df = pd.read_parquet(path, columns=columns)
    if split is not None:
        df = df[df["split"] == split].drop(columns=["split"])
    return (
        df.rename(columns={"weighted_score": "score"})
        .dropna(subset=["actor_id", "repo_id", "score"])
        .astype({"actor_id": "int64", "repo_id": "int64", "score": "float32"})
    )


def load_repo_feature_mart(mart_dir: Path, repo_ids: set[int]) -> pd.DataFrame:
    path = mart_dir / "repo_feature_mart.parquet"
    cols = ["repo_id", "language", "stars", "forks"]
    df = pd.read_parquet(path, columns=cols)
    return df[df["repo_id"].isin(repo_ids)].copy()


def build_fb_from_raw(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["score"] = df["type"].map(WEIGHTS).fillna(0) * df["cnt"]
    fb = df.groupby(["actor_id", "repo_id"])["score"].sum().reset_index()
    return fb[fb["score"] > 0]


def safe_log1p(value) -> float:
    if value is None or pd.isna(value):
        return 0.0
    return float(np.log1p(max(float(value), 0.0)))


def precision_recall_ndcg(recommended, relevant, k):
    rec_set = set(recommended[:k])
    hits = rec_set & relevant
    p = len(hits) / k if k > 0 else 0
    r = len(hits) / len(relevant) if relevant else 0
    dcg = sum(1.0 / math.log2(i + 2) for i, rid in enumerate(recommended[:k]) if rid in relevant)
    idcg = sum(1.0 / math.log2(i + 2) for i in range(min(len(relevant), k)))
    n = dcg / idcg if idcg > 0 else 0
    return p, r, n


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mart-dir", type=Path, default=MART_DIR)
    parser.add_argument("--use-marts", choices=["auto", "always", "never"], default="auto")
    parser.add_argument("--sample-ratio", type=float, default=SAMPLE_RATIO)
    parser.add_argument("--eval-users", type=int, default=5_000)
    parser.add_argument("--epochs", type=int, default=N_EPOCHS)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--output-path", type=Path, default=MODEL_DIR / "two_tower_10pct.pt")
    args = parser.parse_args()

    t_total = time.time()

    # ── 1. Data ──
    print("=" * 60)
    rng = np.random.default_rng(42)
    required_marts = [
        args.mart_dir / "user_repo_interaction_mart.parquet",
        args.mart_dir / "experiment_split_mart.parquet",
        args.mart_dir / "repo_feature_mart.parquet",
    ]
    use_marts = args.use_marts != "never" and all(path.exists() for path in required_marts)
    if args.use_marts == "always" and not use_marts:
        missing = [str(path) for path in required_marts if not path.exists()]
        raise FileNotFoundError(f"missing required marts: {missing}")

    print(f"1. Loading data ({args.sample_ratio:.0%} sample)...")
    if use_marts:
        print(f"   using marts from {args.mart_dir}")
        train_fb = load_mart_feedback(args.mart_dir / "user_repo_interaction_mart.parquet")
        test_fb = load_mart_feedback(args.mart_dir / "experiment_split_mart.parquet", "test")
    else:
        train_df = load_period(OUTPUT_DIR, TRAIN_START, TRAIN_END)
        test_df = load_period(OUTPUT_DIR, TEST_START, TEST_END)
        train_fb = build_fb_from_raw(train_df)
        test_fb = build_fb_from_raw(test_df)

    all_users = set(train_fb["actor_id"].unique()) | set(test_fb["actor_id"].unique())
    sampled = set(rng.choice(list(all_users), size=int(len(all_users) * args.sample_ratio), replace=False))
    train_fb = train_fb[train_fb["actor_id"].isin(sampled)]
    test_fb = test_fb[test_fb["actor_id"].isin(sampled)]

    user_ids = train_fb["actor_id"].unique()
    item_ids = train_fb["repo_id"].unique()
    user2idx = {u: i for i, u in enumerate(user_ids)}
    item2idx = {it: i for i, it in enumerate(item_ids)}
    idx2item = {i: it for it, i in item2idx.items()}
    n_users, n_items = len(user_ids), len(item_ids)

    eval_users_all = sorted(set(train_fb["actor_id"]) & set(test_fb["actor_id"]))
    eval_users = list(rng.choice(eval_users_all, size=min(args.eval_users, len(eval_users_all)), replace=False))
    test_gt = test_fb.groupby("actor_id")["repo_id"].apply(set).to_dict()
    train_seen = train_fb.groupby("actor_id")["repo_id"].apply(set).to_dict()

    print(f"   {len(train_fb):,} interactions, {n_users:,} users, {n_items:,} items")
    print(f"   Eval users: {len(eval_users):,}")

    # ── 2. Item features ──
    meta_dict = {}
    if use_marts:
        repo_feature = load_repo_feature_mart(args.mart_dir, set(item_ids))
        meta_dict = (
            repo_feature.rename(columns={"stars": "stargazers"})
            .set_index("repo_id")[["language", "stargazers", "forks"]]
            .to_dict(orient="index")
        )
    elif DB_PATH.exists():
        conn = sqlite3.connect(str(DB_PATH))
        mdf = pd.read_sql_query("SELECT repo_id, language, stargazers, forks FROM repo_metadata WHERE http_status=200", conn)
        meta_dict = mdf.set_index("repo_id").to_dict(orient="index")
        conn.close()

    all_langs = sorted(l for l in set(m.get("language") for m in meta_dict.values()) if isinstance(l, str))
    lang2idx = {l: i + 1 for i, l in enumerate(all_langs)}

    item_feat = np.zeros((n_items, 3), dtype=np.float32)
    for iid, idx in item2idx.items():
        m = meta_dict.get(iid, {})
        item_feat[idx, 0] = safe_log1p(m.get("stargazers", 0))
        item_feat[idx, 1] = safe_log1p(m.get("forks", 0))
        item_feat[idx, 2] = lang2idx.get(m.get("language"), 0)

    # ── 2.5. ALS Training ──
    print("\n2.5. ALS Training...")
    from implicit.als import AlternatingLeastSquares
    row = train_fb["actor_id"].map(user2idx).values
    col = train_fb["repo_id"].map(item2idx).values
    data_sp = train_fb["score"].values.astype(np.float32)
    train_sparse = sparse.csr_matrix((data_sp, (row, col)), shape=(n_users, n_items))

    als = AlternatingLeastSquares(factors=64, regularization=0.01, iterations=15, random_state=42)
    als.fit(train_sparse)

    import faiss
    als_norms = np.linalg.norm(als.item_factors, axis=1)
    min_norm = max(np.percentile(als_norms[als_norms > 0], 90), 0.1)
    als_valid = np.where(als_norms > min_norm)[0]
    als_normed = (als.item_factors[als_valid] / np.linalg.norm(als.item_factors[als_valid], axis=1, keepdims=True)).astype(np.float32)
    als_index = faiss.IndexFlatIP(64)
    als_index.add(np.ascontiguousarray(als_normed))
    print(f"   ALS FAISS: {als_index.ntotal:,} items")

    # ── 3. Two-Tower Training ──
    print(f"\n3. Two-Tower Training (CPU, BS={args.batch_size}, {args.epochs} epochs)...")
    model = TwoTower(n_users, n_items, len(lang2idx), EMBED_DIM)

    user_t = torch.tensor(train_fb["actor_id"].map(user2idx).values, dtype=torch.long)
    item_t = torch.tensor(train_fb["repo_id"].map(item2idx).values, dtype=torch.long)
    feat_t = torch.tensor(item_feat[item_t.numpy()], dtype=torch.float32)

    loader = DataLoader(TensorDataset(user_t, item_t, feat_t), batch_size=args.batch_size, shuffle=True, drop_last=True)
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=args.epochs)

    for epoch in range(args.epochs):
        t0 = time.time()
        model.train()
        total_loss = 0
        for bu, bi, bf in loader:
            u, i = model(bu, bi, bf)
            logits = u @ i.T / 0.05
            loss = nn.functional.cross_entropy(logits, torch.arange(len(bu)))
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            total_loss += loss.item()
        scheduler.step()
        print(f"   Epoch {epoch+1}: loss={total_loss/len(loader):.4f}, {time.time()-t0:.1f}s")

    # ── 4. Extract embeddings ──
    print("\n4. Extracting embeddings...")
    model.eval()
    with torch.no_grad():
        user_vectors = nn.functional.normalize(model.user_embed(torch.arange(n_users)), dim=1).numpy().astype(np.float32)
    item_vectors = model.get_all_item_vectors(n_items, torch.tensor(item_feat))

    tt_index = faiss.IndexFlatIP(EMBED_DIM)
    tt_index.add(np.ascontiguousarray(item_vectors))
    print(f"   FAISS index: {tt_index.ntotal:,} items")

    # ── 5. Evaluate ──
    print(f"\n5. Evaluating ({len(eval_users):,} users)...")
    MAX_K = max(K_VALUES)
    pop_scores = train_fb.groupby("repo_id")["score"].sum().sort_values(ascending=False)
    pop_cands = pop_scores.head(MAX_K + 500).index.tolist()

    results = {n: {k: {"p": [], "r": [], "n": []} for k in K_VALUES} for n in ["Popularity", "ALS", "Two-Tower"]}

    for uid in tqdm(eval_users, desc="Eval"):
        gt = test_gt.get(uid, set())
        if not gt or uid not in user2idx:
            continue
        uidx = user2idx[uid]
        seen = train_seen.get(uid, set())

        pop_r = [r for r in pop_cands if r not in seen][:MAX_K]

        uvec = (als.user_factors[uidx] / (np.linalg.norm(als.user_factors[uidx]) + 1e-9)).reshape(1, -1).astype(np.float32)
        _, idxs = als_index.search(uvec, MAX_K + 50)
        als_r = [idx2item[als_valid[i]] for i in idxs[0] if i >= 0 and idx2item.get(als_valid[i]) and idx2item[als_valid[i]] not in seen][:MAX_K]

        uvec_tt = user_vectors[uidx].reshape(1, -1)
        _, idxs_tt = tt_index.search(uvec_tt, MAX_K + 50)
        tt_r = [idx2item[i] for i in idxs_tt[0] if i >= 0 and idx2item[i] not in seen][:MAX_K]

        for name, recs in [("Popularity", pop_r), ("ALS", als_r), ("Two-Tower", tt_r)]:
            for k in K_VALUES:
                p, r, n = precision_recall_ndcg(recs, gt, k)
                results[name][k]["p"].append(p)
                results[name][k]["r"].append(r)
                results[name][k]["n"].append(n)

    # ── 6. Results ──
    elapsed = (time.time() - t_total) / 60
    print(f"\n{'=' * 60}")
    print(f"RESULTS (eval={len(eval_users):,}, {args.sample_ratio:.0%} sample, {elapsed:.1f}min)")
    print(f"{'=' * 60}")
    print(f"{'Model':<14} {'K':>4}  {'Precision':>10} {'Recall':>10} {'NDCG':>10}")
    print("-" * 54)
    for name in ["Popularity", "ALS", "Two-Tower"]:
        for k in K_VALUES:
            p = np.mean(results[name][k]["p"])
            r = np.mean(results[name][k]["r"])
            n = np.mean(results[name][k]["n"])
            print(f"{name:<14} {k:>4}  {p:>10.5f} {r:>10.5f} {n:>10.5f}")
        print()

    # Save
    torch.save({
        "model_state": model.state_dict(),
        "n_users": n_users, "n_items": n_items,
        "n_langs": len(lang2idx), "embed_dim": EMBED_DIM,
        "user2idx": user2idx, "item2idx": item2idx, "idx2item": idx2item,
        "item_feat": item_feat,
        "use_marts": use_marts,
        "mart_dir": str(args.mart_dir),
    }, args.output_path)
    print(f"Saved: {args.output_path}")


if __name__ == "__main__":
    main()
