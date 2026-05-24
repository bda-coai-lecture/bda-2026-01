"""Small dense neural rankers for the V2 re-rank feature matrix."""

from __future__ import annotations

import argparse
from typing import Any

import numpy as np
import torch
from torch import nn
from torch.utils.data import DataLoader, TensorDataset
from tqdm import tqdm


ID_EMBEDDING_RANKERS = {"deepwide_idemb"}
NEURAL_RANKERS = ("fm", "deepwide", "deepfm", "dlrm", "deepwide_idemb")
DISPLAY_NAMES = {
    "fm": "FM Re-rank",
    "deepwide": "Deep&Wide Re-rank",
    "deepfm": "DeepFM Re-rank",
    "dlrm": "DLRM Re-rank",
    "deepwide_idemb": "Deep&Wide ID Emb Re-rank",
}


def parse_hidden_dims(value: str) -> tuple[int, ...]:
    dims = tuple(int(part.strip()) for part in value.split(",") if part.strip())
    if not dims:
        raise argparse.ArgumentTypeError("hidden dims must contain at least one integer")
    return dims


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


def standardize_features(x: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    x = np.nan_to_num(x, nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32, copy=False)
    mean = x.mean(axis=0, dtype=np.float64).astype(np.float32)
    std = x.std(axis=0, dtype=np.float64).astype(np.float32)
    std[std < 1e-6] = 1.0
    return ((x - mean) / std).astype(np.float32, copy=False), mean, std


def uses_id_embeddings(model_name: str) -> bool:
    return model_name in ID_EMBEDDING_RANKERS


def build_id_vocab(values: np.ndarray) -> dict[int, int]:
    unique_values = pd_unique_int(values)
    return {value: idx for idx, value in enumerate(unique_values, start=1)}


def pd_unique_int(values: np.ndarray) -> list[int]:
    return [int(value) for value in np.unique(values.astype(np.int64, copy=False))]


def encode_ids(values: np.ndarray, vocab: dict[int, int]) -> np.ndarray:
    return np.asarray(
        [vocab.get(int(value), 0) for value in values],
        dtype=np.int64,
    )


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


class DeepWideIdEmb(nn.Module):
    def __init__(
        self,
        n_features: int,
        n_actor_ids: int,
        n_repo_ids: int,
        embedding_dim: int,
        hidden_dims: tuple[int, ...],
        dropout: float,
    ) -> None:
        super().__init__()
        self.actor_embedding = nn.Embedding(n_actor_ids + 1, embedding_dim, padding_idx=0)
        self.repo_embedding = nn.Embedding(n_repo_ids + 1, embedding_dim, padding_idx=0)
        self.wide = nn.Linear(n_features, 1)
        self.deep = make_mlp(n_features + embedding_dim * 2, hidden_dims, dropout)
        self.out = nn.Linear(hidden_dims[-1], 1)

    def forward(
        self,
        x: torch.Tensor,
        actor_idx: torch.Tensor,
        repo_idx: torch.Tensor,
    ) -> torch.Tensor:
        dense = torch.cat(
            [
                x,
                self.actor_embedding(actor_idx),
                self.repo_embedding(repo_idx),
            ],
            dim=1,
        )
        return self.wide(x).squeeze(-1) + self.out(self.deep(dense)).squeeze(-1)


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


def make_model(
    model_name: str,
    n_features: int,
    factor_dim: int,
    hidden_dims: tuple[int, ...],
    dropout: float,
    n_actor_ids: int = 0,
    n_repo_ids: int = 0,
    id_embedding_dim: int = 32,
) -> nn.Module:
    if model_name == "fm":
        return DenseFM(n_features, factor_dim)
    if model_name == "deepwide":
        return DeepWide(n_features, hidden_dims, dropout)
    if model_name == "deepfm":
        return DeepFM(n_features, factor_dim, hidden_dims, dropout)
    if model_name == "dlrm":
        return DenseDLRM(n_features, factor_dim, hidden_dims, dropout)
    if model_name == "deepwide_idemb":
        return DeepWideIdEmb(
            n_features,
            n_actor_ids,
            n_repo_ids,
            id_embedding_dim,
            hidden_dims,
            dropout,
        )
    raise ValueError(f"unknown neural model: {model_name}")


def score_model(
    model: nn.Module,
    x: torch.Tensor,
    actor_idx: torch.Tensor | None = None,
    repo_idx: torch.Tensor | None = None,
) -> torch.Tensor:
    if isinstance(model, DeepWideIdEmb):
        if actor_idx is None or repo_idx is None:
            raise ValueError("actor_idx and repo_idx are required for DeepWideIdEmb")
        return model(x, actor_idx, repo_idx)
    return model(x)


def train_neural_ranker(
    model_name: str,
    x_train: np.ndarray,
    y_train: np.ndarray,
    args: argparse.Namespace,
    device: torch.device,
    group_values: np.ndarray | None = None,
    actor_idx: np.ndarray | None = None,
    repo_idx: np.ndarray | None = None,
    n_actor_ids: int = 0,
    n_repo_ids: int = 0,
) -> tuple[nn.Module, dict[str, Any]]:
    if uses_id_embeddings(model_name) and (actor_idx is None or repo_idx is None):
        raise ValueError(f"{model_name} requires actor_idx and repo_idx")
    model = make_model(
        model_name,
        x_train.shape[1],
        args.fm_factors,
        args.hidden_dims,
        args.dropout,
        n_actor_ids=n_actor_ids,
        n_repo_ids=n_repo_ids,
        id_embedding_dim=args.id_embedding_dim,
    ).to(device)

    optimizer = torch.optim.AdamW(model.parameters(), lr=args.lr, weight_decay=args.weight_decay)

    positives = float(y_train.sum())
    negatives = float(len(y_train) - positives)
    pos_weight = torch.tensor([max(1.0, negatives / max(1.0, positives))], device=device)

    losses: list[float] = []
    if args.neural_loss == "bce":
        loss_fn = nn.BCEWithLogitsLoss(pos_weight=pos_weight)
        if uses_id_embeddings(model_name):
            dataset = TensorDataset(
                torch.from_numpy(x_train),
                torch.from_numpy(actor_idx),
                torch.from_numpy(repo_idx),
                torch.from_numpy(y_train),
            )
        else:
            dataset = TensorDataset(torch.from_numpy(x_train), torch.from_numpy(y_train))
        loader = DataLoader(dataset, batch_size=args.batch_size, shuffle=True, drop_last=False)

        for epoch in range(1, args.epochs + 1):
            model.train()
            total_loss = 0.0
            total_rows = 0
            for batch in tqdm(loader, desc=f"{model_name} epoch {epoch}/{args.epochs}"):
                if uses_id_embeddings(model_name):
                    xb, actor_batch, repo_batch, yb = batch
                    actor_batch = actor_batch.to(device)
                    repo_batch = repo_batch.to(device)
                else:
                    xb, yb = batch
                    actor_batch = None
                    repo_batch = None
                xb = xb.to(device)
                yb = yb.to(device)
                optimizer.zero_grad(set_to_none=True)
                loss = loss_fn(score_model(model, xb, actor_batch, repo_batch), yb)
                loss.backward()
                nn.utils.clip_grad_norm_(model.parameters(), max_norm=5.0)
                optimizer.step()
                batch_rows = int(yb.numel())
                total_loss += float(loss.detach().cpu()) * batch_rows
                total_rows += batch_rows
            losses.append(total_loss / max(1, total_rows))
    else:
        if group_values is None:
            raise ValueError("group_values are required for pairwise neural losses")
        pos_idx, neg_by_group, pairwise_summary = make_pairwise_index(y_train, group_values)
        pos_loader = DataLoader(
            TensorDataset(torch.from_numpy(pos_idx)),
            batch_size=args.batch_size,
            shuffle=True,
            drop_last=False,
        )
        rng = np.random.default_rng(args.seed)
        x_tensor = torch.from_numpy(x_train)
        actor_tensor = torch.from_numpy(actor_idx) if actor_idx is not None else None
        repo_tensor = torch.from_numpy(repo_idx) if repo_idx is not None else None

        for epoch in range(1, args.epochs + 1):
            model.train()
            total_loss = 0.0
            total_pairs = 0
            for (pos_batch,) in tqdm(pos_loader, desc=f"{model_name} {args.neural_loss} epoch {epoch}/{args.epochs}"):
                pos_np = pos_batch.numpy()
                neg_np = sample_negative_indices(pos_np, group_values, neg_by_group, rng)
                xb_pos = x_tensor[pos_batch].to(device)
                xb_neg = x_tensor[torch.from_numpy(neg_np)].to(device)
                if uses_id_embeddings(model_name):
                    pos_actor = actor_tensor[pos_batch].to(device)
                    pos_repo = repo_tensor[pos_batch].to(device)
                    neg_actor = actor_tensor[torch.from_numpy(neg_np)].to(device)
                    neg_repo = repo_tensor[torch.from_numpy(neg_np)].to(device)
                else:
                    pos_actor = None
                    pos_repo = None
                    neg_actor = None
                    neg_repo = None
                optimizer.zero_grad(set_to_none=True)
                pos_score = score_model(model, xb_pos, pos_actor, pos_repo)
                neg_score = score_model(model, xb_neg, neg_actor, neg_repo)
                loss = -torch.nn.functional.logsigmoid(pos_score - neg_score).mean()
                loss.backward()
                nn.utils.clip_grad_norm_(model.parameters(), max_norm=5.0)
                optimizer.step()
                batch_pairs = int(pos_batch.numel())
                total_loss += float(loss.detach().cpu()) * batch_pairs
                total_pairs += batch_pairs
            losses.append(total_loss / max(1, total_pairs))

    train_summary = {
        "epochs": args.epochs,
        "batch_size": args.batch_size,
        "lr": args.lr,
        "weight_decay": args.weight_decay,
        "dropout": args.dropout,
        "hidden_dims": list(args.hidden_dims),
        "fm_factors": args.fm_factors,
        "id_embedding_dim": args.id_embedding_dim,
        "n_actor_ids": n_actor_ids,
        "n_repo_ids": n_repo_ids,
        "pos_weight": float(pos_weight.detach().cpu().item()),
        "neural_loss": args.neural_loss,
        "losses": losses,
    }
    if args.neural_loss != "bce":
        train_summary.update(pairwise_summary)
    return model, train_summary


def make_pairwise_index(
    y_train: np.ndarray,
    group_values: np.ndarray,
) -> tuple[np.ndarray, dict[int, np.ndarray], dict[str, int]]:
    labels = y_train.astype(np.int8, copy=False)
    group_values = group_values.astype(np.int64, copy=False)
    pos_idx = np.flatnonzero(labels > 0).astype(np.int64, copy=False)
    neg_idx = np.flatnonzero(labels <= 0)
    positive_groups = {int(group_values[idx]) for idx in pos_idx}
    neg_by_group: dict[int, list[int]] = {}
    for idx in neg_idx:
        neg_by_group.setdefault(int(group_values[idx]), []).append(int(idx))
    usable_pos_idx = [
        int(idx)
        for idx in pos_idx
        if int(group_values[idx]) in neg_by_group
    ]
    if not usable_pos_idx:
        raise RuntimeError("No positive rows have in-group negatives for pairwise training.")
    groups_with_negatives = set(neg_by_group)
    skipped_positive_groups = positive_groups - groups_with_negatives
    return (
        np.asarray(usable_pos_idx, dtype=np.int64),
        {group: np.asarray(indices, dtype=np.int64) for group, indices in neg_by_group.items()},
        {
            "pairwise_positive_count": int(len(usable_pos_idx)),
            "pairwise_skipped_positive_count": int(len(pos_idx) - len(usable_pos_idx)),
            "pairwise_positive_groups": int(len(positive_groups)),
            "pairwise_groups_with_negatives": int(len(positive_groups & groups_with_negatives)),
            "pairwise_skipped_positive_groups": int(len(skipped_positive_groups)),
            "pairwise_negative_count": int(len(neg_idx)),
        },
    )


def sample_negative_indices(
    pos_idx: np.ndarray,
    group_values: np.ndarray,
    neg_by_group: dict[int, np.ndarray],
    rng: np.random.Generator,
) -> np.ndarray:
    out = np.empty(len(pos_idx), dtype=np.int64)
    for i, idx in enumerate(pos_idx):
        negatives = neg_by_group[int(group_values[int(idx)])]
        out[i] = negatives[int(rng.integers(0, len(negatives)))]
    return out


def state_payload(
    model_name: str,
    model: nn.Module,
    mean: np.ndarray,
    std: np.ndarray,
    feature_names: list[str],
    train_summary: dict[str, Any],
    actor_vocab: dict[int, int] | None = None,
    repo_vocab: dict[int, int] | None = None,
) -> dict[str, Any]:
    payload = {
        "model_type": f"neural_{model_name}",
        "model_name": model_name,
        "display_name": DISPLAY_NAMES[model_name],
        "state_dict": {key: value.detach().cpu() for key, value in model.state_dict().items()},
        "feature_mean": mean,
        "feature_std": std,
        "feature_names": feature_names,
        "n_features": len(feature_names),
        "train_summary": train_summary,
    }
    if actor_vocab is not None and repo_vocab is not None:
        payload["actor_id_vocab"] = actor_vocab
        payload["repo_id_vocab"] = repo_vocab
    return payload


def load_model_from_payload(payload: dict[str, Any], device: torch.device) -> nn.Module:
    train_summary = payload["train_summary"]
    model = make_model(
        payload["model_name"],
        int(payload["n_features"]),
        int(train_summary["fm_factors"]),
        tuple(int(v) for v in train_summary["hidden_dims"]),
        float(train_summary["dropout"]),
        n_actor_ids=int(train_summary.get("n_actor_ids", 0)),
        n_repo_ids=int(train_summary.get("n_repo_ids", 0)),
        id_embedding_dim=int(train_summary.get("id_embedding_dim", 32)),
    ).to(device)
    model.load_state_dict(payload["state_dict"])
    model.eval()
    return model


def predict_payload(
    payload: dict[str, Any],
    x: np.ndarray,
    device: torch.device,
    batch_size: int,
    actor_ids: np.ndarray | None = None,
    repo_ids: np.ndarray | None = None,
) -> np.ndarray:
    model = load_model_from_payload(payload, device)
    mean = payload["feature_mean"].astype(np.float32, copy=False)
    std = payload["feature_std"].astype(np.float32, copy=False)
    x_scaled = ((np.nan_to_num(x, nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32) - mean) / std).astype(np.float32)
    if uses_id_embeddings(payload["model_name"]):
        if actor_ids is None or repo_ids is None:
            raise ValueError(f"{payload['model_name']} prediction requires actor_ids and repo_ids")
        actor_idx = encode_ids(actor_ids, payload["actor_id_vocab"])
        repo_idx = encode_ids(repo_ids, payload["repo_id_vocab"])
        dataset = TensorDataset(
            torch.from_numpy(x_scaled),
            torch.from_numpy(actor_idx),
            torch.from_numpy(repo_idx),
        )
    else:
        dataset = TensorDataset(torch.from_numpy(x_scaled))
    loader = DataLoader(dataset, batch_size=batch_size)
    scores: list[np.ndarray] = []
    with torch.no_grad():
        for batch in loader:
            if uses_id_embeddings(payload["model_name"]):
                xb, actor_batch, repo_batch = batch
                logits = score_model(
                    model,
                    xb.to(device),
                    actor_batch.to(device),
                    repo_batch.to(device),
                ).detach().cpu().numpy()
            else:
                (xb,) = batch
                logits = score_model(model, xb.to(device)).detach().cpu().numpy()
            scores.append(logits.astype(np.float32, copy=False))
    return np.concatenate(scores) if scores else np.empty(0, dtype=np.float32)
