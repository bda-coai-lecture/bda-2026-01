"""Week 6 neural re-ranker comparison.

This script reuses the Week 6 two-stage split, ALS retrieval, hybrid
candidates, and dense candidate features, then trains BCE re-rankers:

    - Two-Stage/LGBM
    - FM
    - Deep&Wide
    - DeepFM
    - DLRM-style dense interaction model

Usage:
    uv run python scripts/week6_neural_rankers.py --smoke
    uv run python scripts/week6_neural_rankers.py --output-suffix hist28
"""

from __future__ import annotations

import argparse
import json
import pickle
import subprocess
import time
from datetime import date, timedelta
from pathlib import Path

import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
import torch
from implicit.als import AlternatingLeastSquares
from mlflow.tracking import MlflowClient
from torch import nn
from torch.utils.data import DataLoader, TensorDataset
from tqdm import tqdm

import week6_two_stage_v2 as base


MODEL_DIR = Path("data/models/week6")
K_VALUES = [10, 50, 100, 200]
NEURAL_RANKERS = ("fm", "deepwide", "deepfm", "dlrm")
RANKER_DISPLAY_NAMES = {
    "lgbm": "Two-Stage/LGBM",
    "fm": "FM",
    "deepwide": "Deep&Wide",
    "deepfm": "DeepFM",
    "dlrm": "DLRM",
}


def jsonable_args(args: argparse.Namespace) -> dict:
    return {
        k: ",".join(str(part) for part in v)
        if isinstance(v, tuple)
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


def set_mlflow_experiment_metadata(experiment_name: str) -> None:
    mlflow.set_experiment(experiment_name)
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        return
    client = MlflowClient(tracking_uri=mlflow.get_tracking_uri())
    tags = {
        "experiment_role": "re-rank",
        "experiment_stage": "shared_candidate_re-rank_comparison",
        "mlflow.note.content": (
            "Re-rank experiment: compare LGBM, FM, Deep&Wide, DeepFM, "
            "and DLRM on a shared candidate cache."
        ),
    }
    for key, value in tags.items():
        client.set_experiment_tag(experiment.experiment_id, key, value)


def log_mlflow_focus_params(args: argparse.Namespace, run_summary: dict) -> None:
    """Expose comparable recommendation experiment knobs in MLflow tables."""
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
        "exp_candidate_related_cap": getattr(args, "related_candidate_cap", None),
        "exp_candidate_related_top_per_anchor": getattr(args, "related_top_per_anchor", None),
        "exp_candidate_related_max_seen_anchors": getattr(args, "related_max_seen_anchors", None),
        "exp_ranker_family": "neural_compare",
        "exp_ranker_names": ",".join(args.rankers),
        "exp_ranker_epochs": args.epochs,
        "exp_ranker_batch_size": args.batch_size,
        "exp_ranker_learning_rate": args.lr,
        "exp_ranker_hidden_dims": args.hidden_dims,
        "exp_ranker_dlrm_embedding_dim": args.dlrm_embedding_dim or args.fm_factors,
        "exp_ranker_fm_factors": args.fm_factors,
        "exp_ranker_device": args.device,
        "exp_run_feature_cache_source": run_summary.get("feature_cache_source"),
        "exp_run_reuse_feature_cache": args.reuse_feature_cache,
        "exp_run_write_feature_cache": args.write_feature_cache,
        "exp_run_use_marts": run_summary.get("use_marts"),
        "exp_run_ranker_feature_parquet": str(args.ranker_feature_parquet)
        if getattr(args, "ranker_feature_parquet", None)
        else None,
        "exp_run_merged_metrics": bool(args.merge_metrics_from),
    }
    for key, value in focus_params.items():
        if value is not None:
            mlflow.log_param(
                key,
                str(value)
                if isinstance(value, (date, Path, tuple, list))
                else value,
            )

    cache_args = run_summary.get("feature_cache_args") or {}
    for key in [
        "history_start",
        "history_end",
        "rank_start",
        "rank_end",
        "test_start",
        "test_end",
        "max_items",
        "candidate_k",
        "hybrid_extra",
        "rank_users",
        "eval_users",
        "output_suffix",
    ]:
        if key in cache_args:
            mlflow.log_param(f"cache_{key}", cache_args[key])


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


class DenseDLRM(nn.Module):
    """DLRM-style ranker for the existing dense candidate feature matrix.

    The original levit DLRM embeds each sparse/dense feature field and feeds
    pairwise dot products to a DNN. Our Week 6 ranker data is already a dense
    matrix, so each scalar feature is treated as one field and projected into a
    shared embedding space before interaction.
    """

    def __init__(
        self,
        n_features: int,
        embedding_dim: int,
        hidden_dims: tuple[int, ...],
        dropout: float,
    ) -> None:
        super().__init__()
        self.field_weight = nn.Parameter(torch.empty(n_features, embedding_dim))
        self.field_bias = nn.Parameter(torch.zeros(n_features, embedding_dim))
        nn.init.xavier_uniform_(self.field_weight)
        self.register_buffer(
            "triu_indices",
            torch.triu_indices(n_features, n_features, offset=1),
        )
        interaction_dim = n_features * (n_features - 1) // 2
        self.deep = make_mlp(n_features + interaction_dim, hidden_dims, dropout)
        self.out = nn.Linear(hidden_dims[-1], 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        field_embeddings = x.unsqueeze(-1) * self.field_weight + self.field_bias
        dot_products = torch.bmm(field_embeddings, field_embeddings.transpose(1, 2))
        interactions = dot_products[:, self.triu_indices[0], self.triu_indices[1]]
        return self.out(self.deep(torch.cat([x, interactions], dim=1))).squeeze(-1)


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


def parse_rankers(value: str) -> tuple[str, ...]:
    rankers = tuple(part.strip().lower() for part in value.split(",") if part.strip())
    valid = {"lgbm", *NEURAL_RANKERS}
    unknown = sorted(set(rankers) - valid)
    if unknown:
        raise argparse.ArgumentTypeError(f"unknown rankers: {', '.join(unknown)}")
    if not rankers:
        raise argparse.ArgumentTypeError("rankers must contain at least one model")
    return rankers


def merge_metrics(existing_path: Path, current: pd.DataFrame) -> pd.DataFrame:
    existing = pd.read_csv(existing_path)
    current_models = set(current["model"])
    merged = pd.concat(
        [existing[~existing["model"].isin(current_models)], current],
        ignore_index=True,
    )
    order = {
        "Popularity": 0,
        "ALS/Fallback": 1,
        "Two-Stage/LGBM": 2,
        "FM": 3,
        "Deep&Wide": 4,
        "DeepFM": 5,
        "DLRM": 6,
    }
    merged["_model_order"] = merged["model"].map(order).fillna(99)
    return (
        merged.sort_values(["_model_order", "k"])
        .drop(columns=["_model_order"])
        .reset_index(drop=True)
    )


def build_feature_names(context: dict) -> list[str]:
    return base.feature_names_for_context(context)


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
    """Build feature context using the canonical two-stage implementation."""
    return base.build_feature_context(
        history_df,
        recent_df,
        prior_df,
        history_fb,
        recent_fb,
        prior_fb,
        model,
        user2idx,
        item2idx,
    )


def features_for_candidates(
    uid: int,
    candidates: list[tuple],
    user2idx: dict[int, int],
    item2idx: dict[int, int],
    context: dict,
) -> tuple[np.ndarray, list[int]]:
    return base.features_for_candidates(uid, candidates, user2idx, item2idx, context)


def validate_feature_matrix(
    x: np.ndarray,
    feature_names: list[str],
    cache_path: Path | None = None,
) -> None:
    expected_cols = len(feature_names)
    actual_cols = x.shape[1] if x.ndim == 2 else None
    if actual_cols != expected_cols:
        location = f" in {cache_path}" if cache_path is not None else ""
        raise RuntimeError(
            f"Feature matrix{location} has {actual_cols} columns, "
            f"but feature_names has {expected_cols}. Rebuild the feature cache "
            "without --reuse-feature-cache or choose a new --feature-cache-path."
        )


def validate_feature_cache(
    cache_path: Path,
    context: dict,
    feature_names: list[str],
    x_train_raw: np.ndarray,
) -> None:
    required_context_keys = {
        "recent_user_activity",
        "prior_user_activity",
        "user_seen_iidxs",
        "user_profile_normed",
        "item_feature_names",
    }
    missing = sorted(required_context_keys - set(context))
    if missing:
        raise RuntimeError(
            f"Feature cache {cache_path} is incompatible with the current neural feature set; "
            f"missing context keys: {', '.join(missing)}. Rebuild without "
            "--reuse-feature-cache or choose a new --feature-cache-path."
        )

    expected_names = build_feature_names(context)
    if feature_names != expected_names:
        mismatch = next(
            (
                i
                for i, (cached_name, expected_name) in enumerate(
                    zip(feature_names, expected_names, strict=False)
                )
                if cached_name != expected_name
            ),
            min(len(feature_names), len(expected_names)),
        )
        cached_name = feature_names[mismatch] if mismatch < len(feature_names) else "<missing>"
        expected_name = expected_names[mismatch] if mismatch < len(expected_names) else "<missing>"
        raise RuntimeError(
            f"Feature cache {cache_path} uses stale feature_names "
            f"(cached={len(feature_names)}, expected={len(expected_names)}, "
            f"first mismatch at {mismatch}: {cached_name!r} != {expected_name!r}). "
            "Rebuild without --reuse-feature-cache or choose a new --feature-cache-path."
        )

    validate_feature_matrix(x_train_raw, feature_names, cache_path)


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
    x = np.nan_to_num(x, nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32)
    mean = x.mean(axis=0, dtype=np.float64).astype(np.float32)
    std = x.std(axis=0, dtype=np.float64).astype(np.float32)
    std[std < 1e-6] = 1.0
    return ((x - mean) / std).astype(np.float32), mean, std


def load_rank_data_from_feature_parquet(
    parquet_path: Path,
    summary_path: Path | None,
    expected_feature_names: list[str],
) -> tuple[np.ndarray, np.ndarray, list[int], list[str], dict]:
    if summary_path is None:
        candidate_summary = parquet_path.with_name(f"{parquet_path.stem}_summary.json")
        if candidate_summary.exists():
            summary_path = candidate_summary

    feature_names = base.load_ranker_feature_names(parquet_path, summary_path)
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
    y_train = frame["label"].astype(np.float32).to_numpy()
    x_train_raw = frame[feature_names].astype(np.float32).to_numpy()
    summary = {
        "rank_users": int(len(groups)),
        "rank_rows": int(len(y_train)),
        "positive_labels": int(y_train.sum()),
        "positive_rate": float(y_train.mean()),
        "feature_parquet": str(parquet_path),
        "feature_summary": str(summary_path) if summary_path else None,
    }
    return x_train_raw, y_train, groups, feature_names, summary


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
    if model_name == "dlrm":
        return DenseDLRM(n_features, factor_dim, hidden_dims, dropout)
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
    lgbm_ranker: lgb.LGBMRanker | None,
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
    model_names = ["Popularity", "ALS/Fallback"]
    if lgbm_ranker is not None:
        model_names.append(RANKER_DISPLAY_NAMES["lgbm"])
    model_names.extend(RANKER_DISPLAY_NAMES[name] for name in neural_rankers)
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
            if lgbm_ranker is not None:
                lgbm_scores = lgbm_ranker.booster_.predict(x)
                recs_by_model[RANKER_DISPLAY_NAMES["lgbm"]] = [
                    repo_ids[i] for i in np.argsort(-lgbm_scores)
                ]
            for ranker_key, ranker in neural_rankers.items():
                scores = predict_neural(
                    ranker,
                    x,
                    neural_mean,
                    neural_std,
                    device,
                    args.predict_batch_size,
                )
                recs_by_model[RANKER_DISPLAY_NAMES[ranker_key]] = [
                    repo_ids[i] for i in np.argsort(-scores)
                ]
        else:
            if lgbm_ranker is not None:
                recs_by_model[RANKER_DISPLAY_NAMES["lgbm"]] = als_recs
            for ranker_key in neural_rankers:
                recs_by_model[RANKER_DISPLAY_NAMES[ranker_key]] = als_recs

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
    factor_dim = (
        args.dlrm_embedding_dim or args.fm_factors
        if model_name == "dlrm"
        else args.fm_factors
    )
    torch.save(
        {
            "model_name": model_name,
            "state_dict": model.state_dict(),
            "feature_mean": mean,
            "feature_std": std,
            "feature_names": feature_names,
            "n_features": len(feature_names),
            "fm_factors": factor_dim,
            "hidden_dims": args.hidden_dims,
            "dropout": args.dropout,
        },
        path,
    )


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
    set_mlflow_experiment_metadata(args.mlflow_experiment)
    current_model_names = {"Popularity", "ALS/Fallback"}
    if "lgbm" in args.rankers:
        current_model_names.add(RANKER_DISPLAY_NAMES["lgbm"])
    current_model_names.update(RANKER_DISPLAY_NAMES[name] for name in run_summary["neural_rankers"])
    first_neural = next(iter(run_summary["neural_rankers"]), None)
    primary_model = (
        RANKER_DISPLAY_NAMES["dlrm"]
        if "dlrm" in run_summary["neural_rankers"]
        else RANKER_DISPLAY_NAMES["lgbm"]
        if "lgbm" in args.rankers
        else RANKER_DISPLAY_NAMES[first_neural]
        if first_neural is not None
        else "ALS/Fallback"
    )

    with mlflow.start_run(run_name=suffix) as run:
        run_summary["mlflow_run_id"] = run.info.run_id
        for path in artifact_paths:
            if path.name.endswith("_summary.json"):
                path.write_text(json.dumps(run_summary, indent=2), encoding="utf-8")
        mlflow.set_tags(git_metadata())
        mlflow.set_tag("script", "scripts/week6_neural_rankers.py")
        mlflow.set_tag("primary_model", primary_model)
        mlflow.set_tag("merged_metrics", bool(args.merge_metrics_from))
        mlflow.set_tag("ui_metric_1", "core_ndcg_at_100")
        mlflow.set_tag("ui_metric_2", "core_recall_at_100")
        log_mlflow_focus_params(args, run_summary)
        mlflow.log_param("suffix", suffix)
        mlflow.log_param("exp_ranker_count", len(args.rankers))
        mlflow.log_param("rankers", ",".join(args.rankers))
        mlflow.log_param("feature_count", len(run_summary["feature_names"]))
        mlflow.log_param("feature_cache_source", run_summary["feature_cache_source"])
        mlflow.log_param("actual_rank_rows", run_summary["rank_data"]["rank_rows"])
        mlflow.log_param("actual_rank_users", run_summary["rank_data"]["rank_users"])
        mlflow.log_param("mlflow_run_id", run.info.run_id)
        mlflow.log_metric("elapsed_min", run_summary["elapsed_min"])
        mlflow.log_metric("eval_users", run_summary["eval_users"])
        for row in results.itertuples(index=False):
            if row.model == "Popularity":
                continue
            if int(row.k) not in {10, 100}:
                continue
            metric_prefix = "" if row.model in current_model_names else "merged_"
            model_key = metric_model_key(str(row.model))
            if int(row.k) == 100:
                mlflow.log_metric(
                    f"{metric_prefix}{model_key}_recall_at_100",
                    float(row.recall),
                )
                mlflow.log_metric(
                    f"{metric_prefix}{model_key}_ndcg_at_100",
                    float(row.ndcg),
                )
                mlflow.log_metric(
                    f"{metric_prefix}{model_key}_unique_recommended_at_100",
                    int(row.unique_recommended),
                )
            if row.model == primary_model:
                if int(row.k) == 10:
                    mlflow.log_metric("core_ndcg_at_10", float(row.ndcg))
                if int(row.k) == 100:
                    mlflow.log_metric("core_ndcg_at_100", float(row.ndcg))
                    mlflow.log_metric("core_recall_at_100", float(row.recall))
                    mlflow.log_metric("core_unique_at_100", int(row.unique_recommended))
        for model_name, summary in run_summary["neural_rankers"].items():
            losses = summary.get("losses", [])
            if losses:
                mlflow.log_metric(f"{model_name}_final_train_loss", float(losses[-1]))
        for path in artifact_paths:
            if path.exists():
                mlflow.log_artifact(str(path))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--history-start", type=base.parse_date, default=date(2026, 3, 14))
    parser.add_argument("--history-end", type=base.parse_date, default=date(2026, 4, 24))
    parser.add_argument("--rank-start", type=base.parse_date, default=date(2026, 4, 25))
    parser.add_argument("--rank-end", type=base.parse_date, default=date(2026, 5, 1))
    parser.add_argument("--test-start", type=base.parse_date, default=date(2026, 5, 2))
    parser.add_argument("--test-end", type=base.parse_date, default=date(2026, 5, 8))
    parser.add_argument("--mart-dir", type=Path, default=base.MART_DIR)
    parser.add_argument(
        "--use-marts",
        choices=["auto", "always", "never"],
        default="auto",
        help="Read feedback, related candidates, and feature context from Week 6 marts.",
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
    parser.add_argument("--related-path", type=Path, default=base.DEFAULT_RELATED_PATH)
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
    parser.add_argument(
        "--ranker-feature-parquet",
        type=Path,
        default=None,
        help="Train rankers from reusable ranker feature parquet instead of rebuilding train features.",
    )
    parser.add_argument(
        "--ranker-feature-summary",
        type=Path,
        default=None,
        help="Optional summary JSON produced next to --ranker-feature-parquet.",
    )
    parser.add_argument(
        "--merge-metrics-from",
        type=Path,
        default=None,
        help="Existing metrics CSV to merge with newly evaluated models.",
    )

    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=8192)
    parser.add_argument("--predict-batch-size", type=int, default=32768)
    parser.add_argument("--device", type=str, default="auto")
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument("--weight-decay", type=float, default=1e-5)
    parser.add_argument("--dropout", type=float, default=0.1)
    parser.add_argument("--hidden-dims", type=parse_hidden_dims, default=(128, 64))
    parser.add_argument("--fm-factors", type=int, default=16)
    parser.add_argument(
        "--dlrm-embedding-dim",
        type=int,
        default=None,
        help="DLRM field embedding dim. Defaults to --fm-factors for comparable size.",
    )
    parser.add_argument("--lgbm-estimators", type=int, default=120)
    parser.add_argument("--torch-threads", type=int, default=1)
    parser.add_argument(
        "--rankers",
        type=parse_rankers,
        default=("lgbm", "fm", "deepwide", "deepfm", "dlrm"),
        help="Comma-separated rankers to train/evaluate: lgbm,fm,deepwide,deepfm,dlrm.",
    )
    parser.add_argument("--mlflow-tracking-uri", type=str, default="sqlite:///mlflow.db")
    parser.add_argument("--mlflow-experiment", type=str, default="recsys-rerank")
    parser.add_argument("--no-mlflow", action="store_true")
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
    if args.dlrm_embedding_dim is not None:
        args.dlrm_embedding_dim = min(args.dlrm_embedding_dim, 16)


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
        validate_feature_cache(feature_cache_path, context, feature_names, x_train_raw)
        x_train, feature_mean, feature_std = standardize_features(x_train_raw)
    else:
        print("1. load data")
        use_marts = base.should_use_marts(args.use_marts, args.mart_dir)
        if use_marts:
            print(f"   using marts from {args.mart_dir}")
            history_df = base.empty_activity_frame()
            recent_df = base.empty_activity_frame()
            prior_df = base.empty_activity_frame()
            recent_fb = base.empty_feedback_frame()
            prior_fb = base.empty_feedback_frame()
            history_fb = base.load_mart_feedback(
                args.mart_dir / "user_repo_interaction_mart.parquet"
            )
            split_mart = args.mart_dir / "experiment_split_mart.parquet"
            rank_fb = base.load_mart_feedback(split_mart, "rank_label")
            test_fb = base.load_mart_feedback(split_mart, "test")
        else:
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
            event_weights = dict(base.DEFAULT_WEIGHTS)
            history_fb = base.build_feedback(history_df, event_weights)
            rank_fb = base.build_feedback(rank_df, event_weights)
            test_fb = base.build_feedback(test_df, event_weights)
            recent_fb = base.build_feedback(recent_df, event_weights)
            prior_fb = (
                base.build_feedback(prior_df, event_weights)
                if len(prior_df)
                else base.empty_feedback_frame()
            )

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
        data_summary = {
            "history_interactions": int(len(history_fb)),
            "history_users": int(history_fb.actor_id.nunique()),
            "history_repos": int(history_fb.repo_id.nunique()),
            "rank_label_interactions": int(len(rank_fb)),
            "test_label_interactions": int(len(test_fb)),
            "use_marts": bool(use_marts),
        }
        print(
            f"   history={len(history_fb):,} interactions, "
            f"users={history_fb.actor_id.nunique():,}, repos={history_fb.repo_id.nunique():,}"
        )
        print(f"   rank labels={len(rank_fb):,}, test labels={len(test_fb):,}")

        print("3. train ALS")
        train_sparse, user2idx, item2idx, idx2item = base.make_matrix(history_fb)
        feature_marts = (
            base.load_feature_marts(args.mart_dir, set(user2idx), set(item2idx))
            if use_marts
            else None
        )
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
            eval_users_all = list(
                rng.choice(np.array(eval_users_all), size=args.eval_users, replace=False)
            )
            test_labels = {uid: test_labels[uid] for uid in eval_users_all}
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
        candidate_pool_size = args.candidate_k + args.hybrid_extra + 500
        popularity_candidates = (
            pop_scores[pop_scores.index.isin(item2idx)].head(candidate_pool_size).index.tolist()
        )
        recent_candidates = (
            recent_scores[recent_scores.index.isin(item2idx)].head(candidate_pool_size).index.tolist()
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
            args.recent_candidate_cap,
            args.popular_candidate_cap,
            related_candidates,
            args.related_candidate_cap,
            related_seed_items,
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

        print("5. build features")
        context = base.build_feature_context(
            history_df,
            recent_df,
            prior_df,
            history_fb,
            recent_fb,
            prior_fb,
            als_model,
            user2idx,
            item2idx,
            feature_marts,
        )
        canonical_feature_names = build_feature_names(context)
        if args.ranker_feature_parquet is not None:
            x_train_raw, y_train, groups, feature_names, rank_data_summary = (
                load_rank_data_from_feature_parquet(
                    args.ranker_feature_parquet,
                    args.ranker_feature_summary,
                    canonical_feature_names,
                )
            )
        else:
            feature_names = canonical_feature_names
            x_train_raw, y_train, groups, rank_data_summary = build_rank_data(
                rank_hybrid,
                rank_labels,
                user2idx,
                item2idx,
                context,
                args.rank_users,
                args.seed,
            )
        validate_feature_matrix(x_train_raw, feature_names)
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
    dlrm_embedding_dim = args.dlrm_embedding_dim or args.fm_factors
    for model_name in [name for name in NEURAL_RANKERS if name in args.rankers]:
        if model_name == "dlrm":
            original_factor_dim = args.fm_factors
            args.fm_factors = dlrm_embedding_dim
        ranker, summary = train_neural_ranker(model_name, x_train, y_train, args, device)
        if model_name == "dlrm":
            args.fm_factors = original_factor_dim
            summary["embedding_dim"] = dlrm_embedding_dim
        neural_rankers[model_name] = ranker
        neural_summaries[model_name] = summary
    lgbm_ranker = None
    if "lgbm" in args.rankers:
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
        name: MODEL_DIR / f"{name}_ranker_{suffix}.pt"
        for name in neural_rankers
    }

    if args.merge_metrics_from is not None:
        results = merge_metrics(args.merge_metrics_from, results)
    results.to_csv(metrics_path, index=False)
    if lgbm_ranker is not None:
        lgbm_ranker.booster_.save_model(str(lgbm_path))
    als_path.write_bytes(pickle.dumps(als_model))
    mappings_path.write_bytes(
        pickle.dumps(
            {
                "user2idx": user2idx,
                "item2idx": item2idx,
                "idx2item": idx2item,
                "weights": base.DEFAULT_WEIGHTS,
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
        "git": git_metadata(),
        "feature_cache_args": cached_args,
        "feature_cache_source": "reused" if args.reuse_feature_cache else "built",
        "device": str(device),
        "history_interactions": data_summary["history_interactions"],
        "history_users": data_summary["history_users"],
        "history_repos": data_summary["history_repos"],
        "rank_label_interactions": data_summary["rank_label_interactions"],
        "test_label_interactions": data_summary["test_label_interactions"],
        "use_marts": data_summary.get("use_marts"),
        "eval_users": eval_user_count,
        "eval_warm_users": int(sum(1 for uid in test_labels if uid in user2idx)),
        "eval_cold_users": int(sum(1 for uid in test_labels if uid not in user2idx)),
        "rank_data": rank_data_summary,
        "neural_rankers": neural_summaries,
        "merged_metrics_from": str(args.merge_metrics_from) if args.merge_metrics_from else None,
        "feature_names": feature_names,
        "metadata_language_count": len(context["lang2idx"]),
        "paths": {
            "metrics": str(metrics_path),
            "summary": str(summary_path),
            "lgbm": str(lgbm_path) if lgbm_ranker is not None else None,
            "als": str(als_path),
            "mappings": str(mappings_path),
            "pt": {name: str(path) for name, path in pt_paths.items()},
            "feature_cache": str(feature_cache_path) if args.write_feature_cache else None,
        },
        "elapsed_min": round((time.time() - started) / 60, 2),
    }
    summary_path.write_text(json.dumps(run_summary, indent=2), encoding="utf-8")

    artifact_paths = [metrics_path, summary_path, *pt_paths.values()]
    if lgbm_ranker is not None:
        artifact_paths.append(lgbm_path)
    log_mlflow_run(args, suffix, run_summary, results, artifact_paths)

    print("\nresults")
    print(results.to_string(index=False))
    print(f"\nsaved metrics: {metrics_path}")
    print(f"saved summary: {summary_path}")


if __name__ == "__main__":
    main()
