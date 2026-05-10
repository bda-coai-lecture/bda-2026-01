"""Week 6 neural re-ranker comparison.

This script reuses the Week 6 two-stage split, ALS retrieval, hybrid
candidates, and dense candidate features, then trains BCE re-rankers:

    - Two-Stage/LGBM
    - FM
    - Deep&Wide
    - DeepFM

Usage:
    uv run python scripts/week6_neural_rankers.py --smoke
    uv run python scripts/week6_neural_rankers.py --output-suffix hist28
"""

from __future__ import annotations

import argparse
import json
import pickle
import time
from datetime import date, timedelta
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
import torch
from implicit.als import AlternatingLeastSquares
from torch import nn
from torch.utils.data import DataLoader, TensorDataset
from tqdm import tqdm

import week6_two_stage_v2 as base


MODEL_DIR = Path("data/models/week6")
K_VALUES = [10, 50, 100]


def jsonable_args(args: argparse.Namespace) -> dict:
    return {
        k: str(v) if isinstance(v, (date, Path)) else v
        for k, v in vars(args).items()
    }


def choose_device(value: str) -> torch.device:
    if value != "auto":
        return torch.device(value)
    if torch.cuda.is_available():
        return torch.device("cuda")
    if torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


def set_seed(seed: int) -> None:
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


class DenseFM(nn.Module):
    def __init__(self, n_features: int, factor_dim: int = 16) -> None:
        super().__init__()
        self.linear = nn.Linear(n_features, 1)
        self.factors = nn.Parameter(torch.empty(n_features, factor_dim))
        nn.init.xavier_uniform_(self.factors)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        linear = self.linear(x).squeeze(-1)
        xv = x @ self.factors
        x2v2 = (x.square()) @ self.factors.square()
        interactions = 0.5 * (xv.square() - x2v2).sum(dim=1)
        return linear + interactions


class DeepWide(nn.Module):
    def __init__(self, n_features: int, hidden_dims: tuple[int, ...], dropout: float) -> None:
        super().__init__()
        self.wide = nn.Linear(n_features, 1)
        self.deep = make_mlp(n_features, hidden_dims, dropout)
        self.out = nn.Linear(hidden_dims[-1], 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.wide(x).squeeze(-1) + self.out(self.deep(x)).squeeze(-1)


class DeepFM(nn.Module):
    def __init__(
        self,
        n_features: int,
        factor_dim: int,
        hidden_dims: tuple[int, ...],
        dropout: float,
    ) -> None:
        super().__init__()
        self.fm = DenseFM(n_features, factor_dim)
        self.deep = make_mlp(n_features, hidden_dims, dropout)
        self.out = nn.Linear(hidden_dims[-1], 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.fm(x) + self.out(self.deep(x)).squeeze(-1)


def make_mlp(n_features: int, hidden_dims: tuple[int, ...], dropout: float) -> nn.Sequential:
    layers: list[nn.Module] = []
    in_dim = n_features
    for hidden_dim in hidden_dims:
        layers.extend(
            [
                nn.Linear(in_dim, hidden_dim),
                nn.ReLU(),
                nn.BatchNorm1d(hidden_dim),
                nn.Dropout(dropout),
            ]
        )
        in_dim = hidden_dim
    return nn.Sequential(*layers)


def parse_hidden_dims(value: str) -> tuple[int, ...]:
    dims = tuple(int(part.strip()) for part in value.split(",") if part.strip())
    if not dims:
        raise argparse.ArgumentTypeError("hidden dims must contain at least one integer")
    return dims


def build_feature_names(context: dict) -> list[str]:
    return [
        "als_score",
        "factor_cosine",
        *context["item_feature_names"],
        "log_user_total_score",
        "log_user_unique_repos",
        "log_user_events",
        "user_watch_share",
        "user_pr_share",
        "user_fork_share",
        "user_push_share",
        "event_match_dot",
        "event_l1_distance",
        "seen_max_cosine",
        "seen_mean_cosine",
        "source_is_als",
        "source_is_recent",
        "source_is_popular",
    ]


def build_light_feature_context(
    history_df: pd.DataFrame,
    recent_df: pd.DataFrame,
    prior_df: pd.DataFrame,
    history_fb: pd.DataFrame,
    recent_fb: pd.DataFrame,
    prior_fb: pd.DataFrame,
    model: AlternatingLeastSquares,
    user2idx: dict[int, int],
    item2idx: dict[int, int],
) -> dict:
    """Build the compact feature context without heavy per-user profile objects."""
    pop = base.feedback_popularity(history_fb).to_dict()
    recent_pop = base.feedback_popularity(recent_fb).to_dict() if len(recent_fb) else {}
    prior_pop = base.feedback_popularity(prior_fb).to_dict() if len(prior_fb) else {}
    user_activity = base.aggregate_user_activity(history_fb)
    item_user_counts = history_fb.groupby("repo_id", observed=True)["actor_id"].nunique().to_dict()
    recent_item_users = recent_fb.groupby("repo_id", observed=True)["actor_id"].nunique().to_dict()
    item_event, user_event = base.event_stats(history_df)
    recent_item_event, _ = base.event_stats(recent_df) if len(recent_df) else ({}, {})
    meta, lang2idx = base.load_metadata(base.DB_PATH)

    user_factors = model.user_factors.astype(np.float32)
    item_factors = model.item_factors.astype(np.float32)
    user_norms = np.linalg.norm(user_factors, axis=1, keepdims=True)
    item_norms = np.linalg.norm(item_factors, axis=1, keepdims=True)
    user_norms[user_norms == 0] = 1.0
    item_norms[item_norms == 0] = 1.0

    item_feature_names = [
        "log_popularity",
        "log_item_user_count",
        "log_recent_popularity",
        "log_recent_item_user_count",
        "pop_growth",
        "log_item_events",
        "item_watch_share",
        "item_pr_share",
        "item_fork_share",
        "item_push_share",
        "recent_item_watch_share",
        "recent_item_pr_share",
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
        log_recent = np.log1p(recent_pop.get(repo_id, 0.0))
        log_prior = np.log1p(prior_pop.get(repo_id, 0.0))
        item_static[iidx] = np.array(
            [
                np.log1p(pop.get(repo_id, 0.0)),
                np.log1p(item_user_counts.get(repo_id, 0)),
                log_recent,
                np.log1p(recent_item_users.get(repo_id, 0)),
                log_recent - log_prior,
                i_event.get("log_item_events", 0.0),
                i_event.get("item_watch_share", 0.0),
                i_event.get("item_pr_share", 0.0),
                i_event.get("item_fork_share", 0.0),
                i_event.get("item_push_share", 0.0),
                r_event.get("item_watch_share", 0.0),
                r_event.get("item_pr_share", 0.0),
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
        "user_event": user_event,
        "user_normed": user_factors / user_norms,
        "item_normed": item_factors / item_norms,
        "item_static": item_static,
        "item_feature_names": item_feature_names,
        "lang2idx": lang2idx,
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

    repo_ids, iidxs, als_scores, sources = [], [], [], []
    for cand in candidates:
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
    if not repo_ids:
        return np.empty((0, 0), dtype=np.float32), []

    n = len(repo_ids)
    iidxs_arr = np.array(iidxs)
    user_activity = context["user_activity"].get(
        uid, {"user_total_score": 0.0, "user_unique_repos": 0.0}
    )
    user_event = context["user_event"].get(uid, {})
    cos = context["user_normed"][uidx] @ context["item_normed"][iidxs_arr].T
    item_features = context["item_static"][iidxs_arr]
    feature_idx = {name: i for i, name in enumerate(context["item_feature_names"])}

    item_watch = item_features[:, feature_idx["item_watch_share"]]
    item_pr = item_features[:, feature_idx["item_pr_share"]]
    item_fork = item_features[:, feature_idx["item_fork_share"]]
    item_push = item_features[:, feature_idx["item_push_share"]]
    user_watch = float(user_event.get("user_watch_share", 0.0))
    user_pr = float(user_event.get("user_pr_share", 0.0))
    user_fork = float(user_event.get("user_fork_share", 0.0))
    user_push = float(user_event.get("user_push_share", 0.0))
    event_match_dot = (
        item_watch * user_watch
        + item_pr * user_pr
        + item_fork * user_fork
        + item_push * user_push
    )
    event_l1_distance = (
        np.abs(item_watch - user_watch)
        + np.abs(item_pr - user_pr)
        + np.abs(item_fork - user_fork)
        + np.abs(item_push - user_push)
    )

    source_arr = np.array(sources, dtype=np.int8)
    source_is_als = (source_arr == 1).astype(np.float32)
    source_is_recent = (source_arr == 2).astype(np.float32)
    source_is_popular = (source_arr == 3).astype(np.float32)
    seen_max_cos = np.zeros(n, dtype=np.float32)
    seen_mean_cos = np.zeros(n, dtype=np.float32)

    x = np.column_stack(
        [
            np.array(als_scores, dtype=np.float32),
            cos.astype(np.float32),
            item_features,
            np.full(n, np.log1p(user_activity["user_total_score"]), dtype=np.float32),
            np.full(n, np.log1p(user_activity["user_unique_repos"]), dtype=np.float32),
            np.full(n, user_event.get("log_user_events", 0.0), dtype=np.float32),
            np.full(n, user_event.get("user_watch_share", 0.0), dtype=np.float32),
            np.full(n, user_event.get("user_pr_share", 0.0), dtype=np.float32),
            np.full(n, user_event.get("user_fork_share", 0.0), dtype=np.float32),
            np.full(n, user_event.get("user_push_share", 0.0), dtype=np.float32),
            event_match_dot.astype(np.float32),
            event_l1_distance.astype(np.float32),
            seen_max_cos,
            seen_mean_cos,
            source_is_als,
            source_is_recent,
            source_is_popular,
        ]
    )
    return x.astype(np.float32), repo_ids


def build_rank_data(
    retrieval: dict[int, list[tuple[int, float, int]]],
    labels_by_user: dict[int, set[int]],
    user2idx: dict[int, int],
    item2idx: dict[int, int],
    context: dict,
    max_rank_users: int,
    seed: int,
) -> tuple[np.ndarray, np.ndarray, list[int], dict]:
    rng = np.random.default_rng(seed)
    users = [uid for uid in retrieval if labels_by_user.get(uid)]
    if len(users) > max_rank_users:
        users = list(rng.choice(np.array(users), size=max_rank_users, replace=False))

    xs: list[np.ndarray] = []
    ys: list[np.ndarray] = []
    groups: list[int] = []
    positive_labels = 0

    for uid in tqdm(users, desc="rank data"):
        x, repo_ids = features_for_candidates(uid, retrieval[uid], user2idx, item2idx, context)
        if len(repo_ids) == 0:
            continue
        y = np.array([1 if rid in labels_by_user[uid] else 0 for rid in repo_ids], dtype=np.float32)
        if y.sum() == 0:
            continue
        xs.append(x)
        ys.append(y)
        groups.append(len(y))
        positive_labels += int(y.sum())

    if not xs:
        raise RuntimeError("No positive ranker labels found in candidate lists.")

    x_train = np.vstack(xs).astype(np.float32)
    y_train = np.concatenate(ys).astype(np.float32)
    summary = {
        "rank_users": len(groups),
        "rank_rows": int(len(y_train)),
        "positive_labels": positive_labels,
        "positive_rate": float(y_train.mean()),
    }
    return x_train, y_train, groups, summary


def standardize_features(x: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    mean = x.mean(axis=0, dtype=np.float64).astype(np.float32)
    std = x.std(axis=0, dtype=np.float64).astype(np.float32)
    std[std < 1e-6] = 1.0
    return ((x - mean) / std).astype(np.float32), mean, std


def train_lgbm_ranker(
    x_train: np.ndarray,
    y_train: np.ndarray,
    groups: list[int],
    seed: int,
    n_estimators: int,
) -> lgb.LGBMRanker:
    ranker = lgb.LGBMRanker(
        objective="lambdarank",
        metric="ndcg",
        n_estimators=n_estimators,
        learning_rate=0.05,
        num_leaves=31,
        min_child_samples=20,
        random_state=seed,
        verbose=-1,
    )
    ranker.fit(x_train, y_train.astype(np.int32), group=groups)
    return ranker


def make_neural_model(
    model_name: str,
    n_features: int,
    factor_dim: int,
    hidden_dims: tuple[int, ...],
    dropout: float,
) -> nn.Module:
    if model_name == "fm":
        return DenseFM(n_features, factor_dim)
    if model_name == "deepwide":
        return DeepWide(n_features, hidden_dims, dropout)
    if model_name == "deepfm":
        return DeepFM(n_features, factor_dim, hidden_dims, dropout)
    raise ValueError(f"unknown neural model: {model_name}")


def train_neural_ranker(
    model_name: str,
    x_train: np.ndarray,
    y_train: np.ndarray,
    args: argparse.Namespace,
    device: torch.device,
) -> tuple[nn.Module, dict]:
    model = make_neural_model(
        model_name,
        x_train.shape[1],
        args.fm_factors,
        args.hidden_dims,
        args.dropout,
    ).to(device)

    positives = float(y_train.sum())
    negatives = float(len(y_train) - positives)
    pos_weight = torch.tensor([max(1.0, negatives / max(1.0, positives))], device=device)
    loss_fn = nn.BCEWithLogitsLoss(pos_weight=pos_weight)
    optimizer = torch.optim.AdamW(model.parameters(), lr=args.lr, weight_decay=args.weight_decay)

    dataset = TensorDataset(torch.from_numpy(x_train), torch.from_numpy(y_train))
    loader = DataLoader(dataset, batch_size=args.batch_size, shuffle=True, drop_last=False)

    losses: list[float] = []
    for epoch in range(1, args.epochs + 1):
        model.train()
        total_loss = 0.0
        total_rows = 0
        for xb, yb in tqdm(loader, desc=f"{model_name} epoch {epoch}/{args.epochs}"):
            xb = xb.to(device)
            yb = yb.to(device)
            optimizer.zero_grad(set_to_none=True)
            loss = loss_fn(model(xb), yb)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), max_norm=5.0)
            optimizer.step()
            batch_rows = int(yb.numel())
            total_loss += float(loss.detach().cpu()) * batch_rows
            total_rows += batch_rows
        losses.append(total_loss / max(1, total_rows))

    return model, {
        "epochs": args.epochs,
        "batch_size": args.batch_size,
        "lr": args.lr,
        "weight_decay": args.weight_decay,
        "pos_weight": float(pos_weight.detach().cpu().item()),
        "losses": losses,
    }


def predict_neural(
    model: nn.Module,
    x: np.ndarray,
    mean: np.ndarray,
    std: np.ndarray,
    device: torch.device,
    batch_size: int,
) -> np.ndarray:
    x_scaled = ((x - mean) / std).astype(np.float32)
    loader = DataLoader(TensorDataset(torch.from_numpy(x_scaled)), batch_size=batch_size)
    scores: list[np.ndarray] = []
    model.eval()
    with torch.no_grad():
        for (xb,) in loader:
            logits = model(xb.to(device)).detach().cpu().numpy()
            scores.append(logits.astype(np.float32))
    return np.concatenate(scores) if scores else np.empty(0, dtype=np.float32)


def evaluate_rankers(
    als_retrieval: dict[int, list[tuple[int, float]]],
    hybrid_retrieval: dict[int, list[tuple[int, float, int]]],
    test_labels: dict[int, set[int]],
    lgbm_ranker: lgb.LGBMRanker,
    neural_rankers: dict[str, nn.Module],
    neural_mean: np.ndarray,
    neural_std: np.ndarray,
    device: torch.device,
    args: argparse.Namespace,
    user2idx: dict[int, int],
    item2idx: dict[int, int],
    context: dict,
    popularity_candidates: list[int],
    fallback_candidates: list[int],
    train_seen: dict[int, set[int]],
) -> tuple[pd.DataFrame, int]:
    model_names = [
        "Popularity",
        "ALS/Fallback",
        "Two-Stage/LGBM",
        "FM",
        "Deep&Wide",
        "DeepFM",
    ]
    metrics = {
        name: {k: {"precision": [], "recall": [], "ndcg": []} for k in K_VALUES}
        for name in model_names
    }
    coverage = {name: {k: set() for k in K_VALUES} for name in model_names}

    eval_users = [uid for uid in test_labels if test_labels.get(uid)]
    for uid in tqdm(eval_users, desc="eval"):
        relevant = test_labels[uid]
        seen = train_seen.get(uid, set())
        pop_recs = [rid for rid in popularity_candidates if rid not in seen]
        fallback_recs = [rid for rid in fallback_candidates if rid not in seen]
        als_pairs = als_retrieval.get(uid, [])
        hybrid_pairs = hybrid_retrieval.get(uid, als_pairs)
        als_recs = [rid for rid, *_ in als_pairs] if als_pairs else fallback_recs

        recs_by_model: dict[str, list[int]] = {
            "Popularity": pop_recs,
            "ALS/Fallback": als_recs,
        }

        x, repo_ids = features_for_candidates(uid, hybrid_pairs, user2idx, item2idx, context)
        if len(repo_ids):
            lgbm_scores = lgbm_ranker.booster_.predict(x)
            recs_by_model["Two-Stage/LGBM"] = [
                repo_ids[i] for i in np.argsort(-lgbm_scores)
            ]
            for display_name, ranker_key in [
                ("FM", "fm"),
                ("Deep&Wide", "deepwide"),
                ("DeepFM", "deepfm"),
            ]:
                scores = predict_neural(
                    neural_rankers[ranker_key],
                    x,
                    neural_mean,
                    neural_std,
                    device,
                    args.predict_batch_size,
                )
                recs_by_model[display_name] = [repo_ids[i] for i in np.argsort(-scores)]
        else:
            recs_by_model["Two-Stage/LGBM"] = als_recs
            recs_by_model["FM"] = als_recs
            recs_by_model["Deep&Wide"] = als_recs
            recs_by_model["DeepFM"] = als_recs

        for model_name, recs in recs_by_model.items():
            for k in K_VALUES:
                p, r, n = base.precision_recall_ndcg(recs, relevant, k)
                metrics[model_name][k]["precision"].append(p)
                metrics[model_name][k]["recall"].append(r)
                metrics[model_name][k]["ndcg"].append(n)
                coverage[model_name][k].update(recs[:k])

    rows = []
    for model_name in model_names:
        for k in K_VALUES:
            rows.append(
                {
                    "model": model_name,
                    "k": k,
                    "precision": float(np.mean(metrics[model_name][k]["precision"])),
                    "recall": float(np.mean(metrics[model_name][k]["recall"])),
                    "ndcg": float(np.mean(metrics[model_name][k]["ndcg"])),
                    "unique_recommended": len(coverage[model_name][k]),
                }
            )
    return pd.DataFrame(rows), len(eval_users)


def save_torch_model(
    path: Path,
    model_name: str,
    model: nn.Module,
    mean: np.ndarray,
    std: np.ndarray,
    feature_names: list[str],
    args: argparse.Namespace,
) -> None:
    torch.save(
        {
            "model_name": model_name,
            "state_dict": model.state_dict(),
            "feature_mean": mean,
            "feature_std": std,
            "feature_names": feature_names,
            "n_features": len(feature_names),
            "fm_factors": args.fm_factors,
            "hidden_dims": args.hidden_dims,
            "dropout": args.dropout,
        },
        path,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--history-start", type=base.parse_date, default=date(2026, 3, 14))
    parser.add_argument("--history-end", type=base.parse_date, default=date(2026, 4, 24))
    parser.add_argument("--rank-start", type=base.parse_date, default=date(2026, 4, 25))
    parser.add_argument("--rank-end", type=base.parse_date, default=date(2026, 5, 1))
    parser.add_argument("--test-start", type=base.parse_date, default=date(2026, 5, 2))
    parser.add_argument("--test-end", type=base.parse_date, default=date(2026, 5, 8))
    parser.add_argument("--sample-ratio", type=float, default=1.0)
    parser.add_argument("--min-item-users", type=int, default=3)
    parser.add_argument("--min-user-items", type=int, default=1)
    parser.add_argument("--max-items", type=int, default=500_000)
    parser.add_argument("--candidate-k", type=int, default=300)
    parser.add_argument("--hybrid-extra", type=int, default=200)
    parser.add_argument("--rank-users", type=int, default=30_000)
    parser.add_argument("--eval-users", type=int, default=30_000)
    parser.add_argument("--factors", type=int, default=64)
    parser.add_argument("--iterations", type=int, default=12)
    parser.add_argument("--chunk-size", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--output-suffix", type=str, default=None)
    parser.add_argument("--write-feature-cache", action="store_true")
    parser.add_argument("--reuse-feature-cache", action="store_true")
    parser.add_argument("--feature-cache-path", type=Path, default=None)

    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=8192)
    parser.add_argument("--predict-batch-size", type=int, default=32768)
    parser.add_argument("--device", type=str, default="auto")
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument("--weight-decay", type=float, default=1e-5)
    parser.add_argument("--dropout", type=float, default=0.1)
    parser.add_argument("--hidden-dims", type=parse_hidden_dims, default=(128, 64))
    parser.add_argument("--fm-factors", type=int, default=16)
    parser.add_argument("--lgbm-estimators", type=int, default=120)
    parser.add_argument("--torch-threads", type=int, default=1)
    return parser.parse_args()


def apply_smoke_defaults(args: argparse.Namespace) -> None:
    if not args.smoke:
        return
    args.sample_ratio = 0.01
    args.max_items = 50_000
    args.candidate_k = 80
    args.hybrid_extra = 40
    args.rank_users = 1000
    args.eval_users = 1000
    args.factors = 32
    args.iterations = 3
    args.epochs = min(args.epochs, 1)
    args.batch_size = min(args.batch_size, 4096)
    args.lgbm_estimators = min(args.lgbm_estimators, 40)


def main() -> None:
    args = parse_args()
    apply_smoke_defaults(args)
    torch.set_num_threads(args.torch_threads)
    torch.set_num_interop_threads(max(1, min(args.torch_threads, 4)))
    set_seed(args.seed)
    device = choose_device(args.device)
    started = time.time()
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    suffix = args.output_suffix or ("smoke" if args.smoke else "latest")
    feature_cache_path = (
        args.feature_cache_path
        if args.feature_cache_path is not None
        else MODEL_DIR / f"week6_ranker_compare_{suffix}_features.pkl"
    )

    print(f"device={device}")
    cached_args = None
    if args.reuse_feature_cache:
        print(f"1. load feature cache: {feature_cache_path}")
        cached = pickle.loads(feature_cache_path.read_bytes())
        cached_args = cached.get("args")
        als_model = cached["als_model"]
        user2idx = cached["user2idx"]
        item2idx = cached["item2idx"]
        idx2item = cached["idx2item"]
        test_retrieval = cached["test_retrieval"]
        test_hybrid = cached["test_hybrid"]
        test_labels = cached["test_labels"]
        train_seen = cached["train_seen"]
        popularity_candidates = cached["popularity_candidates"]
        recent_candidates = cached["recent_candidates"]
        context = cached["context"]
        feature_names = cached["feature_names"]
        x_train_raw = cached["x_train_raw"]
        y_train = cached["y_train"]
        groups = cached["groups"]
        rank_data_summary = cached["rank_data_summary"]
        data_summary = cached["data_summary"]
        x_train, feature_mean, feature_std = standardize_features(x_train_raw)
    else:
        print("1. load data")
        history_df = base.load_period(base.DATA_DIR, args.history_start, args.history_end)
        rank_df = base.load_period(base.DATA_DIR, args.rank_start, args.rank_end)
        test_df = base.load_period(base.DATA_DIR, args.test_start, args.test_end)
        recent_start = max(args.history_start, args.history_end - timedelta(days=13))
        prior_end = recent_start - timedelta(days=1)
        recent_df = base.load_period(base.DATA_DIR, recent_start, args.history_end)
        prior_df = (
            base.load_period(base.DATA_DIR, args.history_start, prior_end)
            if prior_end >= args.history_start
            else history_df.iloc[0:0].copy()
        )
        history_fb, rank_fb, test_fb = map(base.build_feedback, [history_df, rank_df, test_df])
        recent_fb = base.build_feedback(recent_df)
        prior_fb = base.build_feedback(prior_df) if len(prior_df) else history_fb.iloc[0:0].copy()

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
        data_summary = {
            "history_interactions": int(len(history_fb)),
            "history_users": int(history_fb.actor_id.nunique()),
            "history_repos": int(history_fb.repo_id.nunique()),
            "rank_label_interactions": int(len(rank_fb)),
            "test_label_interactions": int(len(test_fb)),
        }
        print(
            f"   history={len(history_fb):,} interactions, "
            f"users={history_fb.actor_id.nunique():,}, repos={history_fb.repo_id.nunique():,}"
        )
        print(f"   rank labels={len(rank_fb):,}, test labels={len(test_fb):,}")

        print("3. train ALS")
        train_sparse, user2idx, item2idx, idx2item = base.make_matrix(history_fb)
        als_model = AlternatingLeastSquares(
            factors=args.factors,
            regularization=0.01,
            iterations=args.iterations,
            random_state=args.seed,
        )
        als_model.fit(train_sparse)

        print("4. retrieve candidates")
        rng = np.random.default_rng(args.seed)
        rank_labels = rank_fb.groupby("actor_id")["repo_id"].apply(set).to_dict()
        test_labels = test_fb.groupby("actor_id")["repo_id"].apply(set).to_dict()
        train_seen = history_fb.groupby("actor_id")["repo_id"].apply(set).to_dict()
        rank_users = sorted(set(rank_labels) & set(user2idx))
        eval_users_all = sorted(test_labels)
        if len(eval_users_all) > args.eval_users:
            eval_users_all = list(
                rng.choice(np.array(eval_users_all), size=args.eval_users, replace=False)
            )
            test_labels = {uid: test_labels[uid] for uid in eval_users_all}
        test_users = sorted(set(test_labels) & set(user2idx))

        pop_scores = base.feedback_popularity(history_fb)
        recent_scores = base.feedback_popularity(recent_fb)
        candidate_pool_size = args.candidate_k + args.hybrid_extra + 500
        popularity_candidates = (
            pop_scores[pop_scores.index.isin(item2idx)].head(candidate_pool_size).index.tolist()
        )
        recent_candidates = (
            recent_scores[recent_scores.index.isin(item2idx)].head(candidate_pool_size).index.tolist()
        )

        rank_retrieval = base.recommend_batch(
            als_model,
            train_sparse,
            user2idx,
            idx2item,
            rank_users,
            args.candidate_k,
            args.chunk_size,
        )
        test_retrieval = base.recommend_batch(
            als_model,
            train_sparse,
            user2idx,
            idx2item,
            test_users,
            args.candidate_k,
            args.chunk_size,
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
        )
        test_hybrid = base.hybridize_candidates(
            test_retrieval,
            test_users,
            popularity_candidates,
            recent_candidates,
            train_seen,
            item2idx,
            max_candidates,
        )

        print("5. build features")
        context = build_light_feature_context(
            history_df,
            recent_df,
            prior_df,
            history_fb,
            recent_fb,
            prior_fb,
            als_model,
            user2idx,
            item2idx,
        )
        feature_names = build_feature_names(context)
        x_train_raw, y_train, groups, rank_data_summary = build_rank_data(
            rank_hybrid,
            rank_labels,
            user2idx,
            item2idx,
            context,
            args.rank_users,
            args.seed,
        )
        x_train, feature_mean, feature_std = standardize_features(x_train_raw)

        if args.write_feature_cache:
            print(f"   save feature cache: {feature_cache_path}")
            feature_cache_path.parent.mkdir(parents=True, exist_ok=True)
            feature_cache_path.write_bytes(
                pickle.dumps(
                    {
                        "args": jsonable_args(args),
                        "data_summary": data_summary,
                        "als_model": als_model,
                        "user2idx": user2idx,
                        "item2idx": item2idx,
                        "idx2item": idx2item,
                        "test_retrieval": test_retrieval,
                        "test_hybrid": test_hybrid,
                        "test_labels": test_labels,
                        "train_seen": train_seen,
                        "popularity_candidates": popularity_candidates,
                        "recent_candidates": recent_candidates,
                        "context": context,
                        "feature_names": feature_names,
                        "x_train_raw": x_train_raw,
                        "y_train": y_train,
                        "groups": groups,
                        "rank_data_summary": rank_data_summary,
                    }
                )
            )

    print("6. train rankers")
    neural_rankers: dict[str, nn.Module] = {}
    neural_summaries: dict[str, dict] = {}
    for model_name in ["fm", "deepwide", "deepfm"]:
        ranker, summary = train_neural_ranker(model_name, x_train, y_train, args, device)
        neural_rankers[model_name] = ranker
        neural_summaries[model_name] = summary
    lgbm_ranker = train_lgbm_ranker(
        x_train_raw,
        y_train,
        groups,
        args.seed,
        args.lgbm_estimators,
    )

    print("7. evaluate")
    results, eval_user_count = evaluate_rankers(
        test_retrieval,
        test_hybrid,
        test_labels,
        lgbm_ranker,
        neural_rankers,
        feature_mean,
        feature_std,
        device,
        args,
        user2idx,
        item2idx,
        context,
        popularity_candidates,
        recent_candidates,
        train_seen,
    )

    suffix = args.output_suffix or ("smoke" if args.smoke else "latest")
    metrics_path = MODEL_DIR / f"week6_ranker_compare_{suffix}_metrics.csv"
    summary_path = MODEL_DIR / f"week6_ranker_compare_{suffix}_summary.json"
    lgbm_path = MODEL_DIR / f"lgbm_ranker_compare_{suffix}.txt"
    mappings_path = MODEL_DIR / f"week6_ranker_compare_{suffix}_mappings.pkl"
    als_path = MODEL_DIR / f"als_ranker_compare_{suffix}.pkl"
    pt_paths = {
        "fm": MODEL_DIR / f"fm_ranker_{suffix}.pt",
        "deepwide": MODEL_DIR / f"deepwide_ranker_{suffix}.pt",
        "deepfm": MODEL_DIR / f"deepfm_ranker_{suffix}.pt",
    }

    results.to_csv(metrics_path, index=False)
    lgbm_ranker.booster_.save_model(str(lgbm_path))
    als_path.write_bytes(pickle.dumps(als_model))
    mappings_path.write_bytes(
        pickle.dumps(
            {
                "user2idx": user2idx,
                "item2idx": item2idx,
                "idx2item": idx2item,
                "weights": base.WEIGHTS,
                "feature_names": feature_names,
                "feature_mean": feature_mean,
                "feature_std": feature_std,
            }
        )
    )
    for model_name, path in pt_paths.items():
        save_torch_model(
            path,
            model_name,
            neural_rankers[model_name],
            feature_mean,
            feature_std,
            feature_names,
            args,
        )

    run_summary = {
        "args": jsonable_args(args),
        "feature_cache_args": cached_args,
        "device": str(device),
        "history_interactions": data_summary["history_interactions"],
        "history_users": data_summary["history_users"],
        "history_repos": data_summary["history_repos"],
        "rank_label_interactions": data_summary["rank_label_interactions"],
        "test_label_interactions": data_summary["test_label_interactions"],
        "eval_users": eval_user_count,
        "eval_warm_users": int(sum(1 for uid in test_labels if uid in user2idx)),
        "eval_cold_users": int(sum(1 for uid in test_labels if uid not in user2idx)),
        "rank_data": rank_data_summary,
        "neural_rankers": neural_summaries,
        "feature_names": feature_names,
        "metadata_language_count": len(context["lang2idx"]),
        "paths": {
            "metrics": str(metrics_path),
            "summary": str(summary_path),
            "lgbm": str(lgbm_path),
            "als": str(als_path),
            "mappings": str(mappings_path),
            "pt": {name: str(path) for name, path in pt_paths.items()},
            "feature_cache": str(feature_cache_path) if args.write_feature_cache else None,
        },
        "elapsed_min": round((time.time() - started) / 60, 2),
    }
    summary_path.write_text(json.dumps(run_summary, indent=2), encoding="utf-8")

    print("\nresults")
    print(results.to_string(index=False))
    print(f"\nsaved metrics: {metrics_path}")
    print(f"saved summary: {summary_path}")


if __name__ == "__main__":
    main()
