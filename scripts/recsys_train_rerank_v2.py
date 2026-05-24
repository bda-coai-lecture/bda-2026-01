"""Train the V2 LGBM LambdaRank re-ranker from reusable sampled rows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import torch

import recsys_neural_rankers as neural
from recsys_v2_common import (
    FEATURE_COLUMNS,
    MODEL_DIR,
    Paths,
    dump_pickle,
    ensure_dirs,
    write_json,
)


def load_feature_names(parquet_path: Path, summary_path: Path) -> list[str]:
    if summary_path.exists():
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        names = summary.get("feature_names")
        if names:
            return [str(name) for name in names]
    columns = set(pq.ParquetFile(parquet_path).schema_arrow.names)
    return [name for name in FEATURE_COLUMNS if name in columns]


def load_training_frame(
    path: Path,
    feature_names: list[str],
    include_entity_ids: bool = False,
) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"rerank train parquet not found: {path}")
    parquet_columns = set(pq.ParquetFile(path).schema_arrow.names)
    group_key = "group_index" if "group_index" in parquet_columns else "actor_id"
    required_columns = {"actor_id", "label", *feature_names}
    if group_key == "group_index":
        required_columns.add("group_index")
        if include_entity_ids:
            required_columns.add("repo_id")
    else:
        required_columns.add("repo_id")
    missing_columns = sorted(required_columns - parquet_columns)
    if missing_columns:
        raise RuntimeError(f"missing columns in rerank train parquet: {missing_columns[:10]}")

    columns = [name for name in ["actor_id", "repo_id", "label", "group_index", *feature_names] if name in required_columns]
    frame = pd.read_parquet(path, columns=columns)
    if frame.empty:
        raise RuntimeError(f"rerank train parquet is empty: {path}")
    missing = [name for name in feature_names if name not in frame.columns]
    if missing:
        raise RuntimeError(f"missing feature columns in rerank train parquet: {missing[:10]}")

    frame["label"] = pd.to_numeric(frame["label"], downcast="integer").astype(np.int8, copy=False)
    for name in feature_names:
        frame[name] = pd.to_numeric(frame[name], errors="coerce").astype(np.float32, copy=False)

    if group_key == "group_index":
        frame["group_index"] = pd.to_numeric(frame["group_index"], downcast="integer")
        if not frame["group_index"].is_monotonic_increasing:
            frame = frame.sort_values("group_index", kind="mergesort", ignore_index=True)
        if not include_entity_ids:
            frame = frame.drop(columns=["actor_id"])
    else:
        frame = frame.sort_values(["actor_id", "label", "repo_id"], ascending=[True, False, True], kind="mergesort", ignore_index=True)
        if not include_entity_ids:
            frame = frame.drop(columns=["repo_id"])
    return frame


def train_lgbm_ranker(frame: pd.DataFrame, feature_names: list[str], args: argparse.Namespace) -> tuple[lgb.LGBMRanker, dict[str, Any]]:
    labels = frame.pop("label").to_numpy(dtype=np.int8, copy=False)
    positives = int(labels.sum())
    if positives <= 0:
        raise RuntimeError("No positive labels found in rerank training data.")

    group_key = "group_index" if "group_index" in frame.columns else "actor_id"
    group_values = frame.pop(group_key).to_numpy(copy=False)
    group_starts = np.flatnonzero(np.r_[True, group_values[1:] != group_values[:-1]])
    group_sizes_array = np.diff(np.r_[group_starts, len(group_values)])
    positive_by_group = np.add.reduceat(labels, group_starts)
    if int((positive_by_group > 0).sum()) != len(group_sizes_array):
        raise RuntimeError("Every actor_id group must contain at least one positive label.")
    group_sizes = group_sizes_array.astype(np.int32, copy=False).tolist()

    x_train = frame[feature_names].to_numpy(dtype=np.float32, copy=False)
    ranker = lgb.LGBMRanker(
        objective="lambdarank",
        metric="ndcg",
        n_estimators=args.n_estimators,
        learning_rate=args.learning_rate,
        num_leaves=args.num_leaves,
        min_child_samples=args.min_child_samples,
        subsample=args.subsample,
        colsample_bytree=args.colsample,
        random_state=args.seed,
        n_jobs=args.n_jobs,
    )
    ranker.fit(x_train, labels, group=group_sizes)

    negative_count = int(len(labels) - positives)
    summary = {
        "ranker": "lgbm_lambdarank",
        "rows": int(len(labels)),
        "groups": int(len(group_sizes)),
        "positive_count": positives,
        "negative_count": negative_count,
        "positive_rate": positives / len(labels),
        "feature_names": feature_names,
        "feature_importance_gain": {
            name: float(value)
            for name, value in zip(
                feature_names,
                ranker.booster_.feature_importance(importance_type="gain"),
                strict=False,
            )
        },
        "params": {
            "n_estimators": args.n_estimators,
            "learning_rate": args.learning_rate,
            "num_leaves": args.num_leaves,
            "min_child_samples": args.min_child_samples,
            "subsample": args.subsample,
            "colsample": args.colsample,
            "seed": args.seed,
            "n_jobs": args.n_jobs,
        },
    }
    return ranker, summary


def train_neural(frame: pd.DataFrame, feature_names: list[str], args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any]]:
    labels = frame.pop("label").to_numpy(dtype=np.float32, copy=False)
    positives = int(labels.sum())
    if positives <= 0:
        raise RuntimeError("No positive labels found in rerank training data.")
    group_key = "group_index" if "group_index" in frame.columns else "actor_id"
    group_values = frame[group_key].to_numpy(dtype=np.int64, copy=False)
    group_count = int(frame[group_key].nunique())
    actor_vocab = None
    repo_vocab = None
    actor_idx = None
    repo_idx = None
    if neural.uses_id_embeddings(args.ranker):
        missing_id_columns = sorted({"actor_id", "repo_id"} - set(frame.columns))
        if missing_id_columns:
            raise RuntimeError(f"{args.ranker} requires id columns: {missing_id_columns}")
        actor_ids = frame["actor_id"].to_numpy(dtype=np.int64, copy=False)
        repo_ids = frame["repo_id"].to_numpy(dtype=np.int64, copy=False)
        actor_vocab = neural.build_id_vocab(actor_ids)
        repo_vocab = neural.build_id_vocab(repo_ids)
        actor_idx = neural.encode_ids(actor_ids, actor_vocab)
        repo_idx = neural.encode_ids(repo_ids, repo_vocab)
    for key in ["group_index", "actor_id", "repo_id"]:
        if key in frame.columns:
            frame = frame.drop(columns=[key])
    x_raw = frame[feature_names].to_numpy(dtype=np.float32, copy=False)
    x_train, mean, std = neural.standardize_features(x_raw)
    device = neural.choose_device(args.device)
    ranker, train_summary = neural.train_neural_ranker(
        args.ranker,
        x_train,
        labels,
        args,
        device,
        group_values=group_values,
        actor_idx=actor_idx,
        repo_idx=repo_idx,
        n_actor_ids=len(actor_vocab or {}),
        n_repo_ids=len(repo_vocab or {}),
    )
    payload = neural.state_payload(
        args.ranker,
        ranker,
        mean,
        std,
        feature_names,
        train_summary,
        actor_vocab=actor_vocab,
        repo_vocab=repo_vocab,
    )
    negative_count = int(len(labels) - positives)
    summary = {
        "ranker": f"neural_{args.ranker}",
        "rows": int(len(labels)),
        "groups": group_count,
        "positive_count": positives,
        "negative_count": negative_count,
        "positive_rate": positives / len(labels),
        "feature_names": feature_names,
        "params": {
            "epochs": args.epochs,
            "batch_size": args.batch_size,
            "lr": args.lr,
            "weight_decay": args.weight_decay,
            "dropout": args.dropout,
            "hidden_dims": args.hidden_dims,
            "fm_factors": args.fm_factors,
            "id_embedding_dim": args.id_embedding_dim,
            "seed": args.seed,
            "device": str(device),
            "torch_threads": args.torch_threads,
            "neural_loss": args.neural_loss,
        },
        "train_summary": train_summary,
    }
    return payload, summary


def model_paths(args: argparse.Namespace) -> tuple[Path, Path, str]:
    if args.ranker == "lgbm" and args.output_suffix is None:
        paths = Paths(args.suffix)
        return paths.ranker_model, paths.ranker_summary, args.suffix
    output_suffix = args.output_suffix or f"{args.suffix}_{args.ranker}"
    return (
        MODEL_DIR / f"ranker_{args.ranker}_{output_suffix}.pkl",
        MODEL_DIR / f"ranker_{args.ranker}_{output_suffix}_summary.json",
        output_suffix,
    )


def configure_torch_threads(torch_threads: int) -> None:
    torch.set_num_threads(torch_threads)
    interop_threads = max(1, min(torch_threads, 4))
    try:
        torch.set_num_interop_threads(interop_threads)
    except RuntimeError as exc:
        if "set_num_interop_threads" not in str(exc) and "interop threads" not in str(exc):
            raise


def run(args: argparse.Namespace) -> dict[str, Any]:
    ensure_dirs()
    configure_torch_threads(args.torch_threads)
    neural.set_seed(args.seed)
    paths = Paths(args.suffix)
    parquet_path = args.input or paths.rerank_train
    summary_path = args.input_summary or paths.rerank_summary
    feature_names = load_feature_names(parquet_path, summary_path)
    frame = load_training_frame(
        parquet_path,
        feature_names,
        include_entity_ids=neural.uses_id_embeddings(args.ranker),
    )
    model_path, model_summary_path, output_suffix = model_paths(args)
    if args.ranker == "lgbm":
        ranker, summary = train_lgbm_ranker(frame, feature_names, args)
        payload = {
            "model": ranker,
            "feature_names": feature_names,
            "model_type": "lgbm_lambdarank",
            "display_name": "LGBM Re-rank",
        }
    else:
        payload, summary = train_neural(frame, feature_names, args)
    dump_pickle(model_path, payload)
    summary.update(
        {
            "suffix": args.suffix,
            "output_suffix": output_suffix,
            "input_path": str(parquet_path),
            "input_summary_path": str(summary_path),
            "model_path": str(model_path),
        }
    )
    write_json(model_summary_path, summary)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--suffix", default="latest")
    parser.add_argument("--output-suffix", default=None)
    parser.add_argument("--input", type=Path, default=None)
    parser.add_argument("--input-summary", type=Path, default=None)
    parser.add_argument("--ranker", choices=("lgbm", *neural.NEURAL_RANKERS), default="lgbm")
    parser.add_argument("--n-estimators", type=int, default=300)
    parser.add_argument("--learning-rate", type=float, default=0.05)
    parser.add_argument("--num-leaves", type=int, default=63)
    parser.add_argument("--min-child-samples", type=int, default=20)
    parser.add_argument("--subsample", type=float, default=0.9)
    parser.add_argument("--colsample", type=float, default=0.9)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--n-jobs", type=int, default=-1)
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=65536)
    parser.add_argument("--device", type=str, default="cpu")
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument("--weight-decay", type=float, default=1e-5)
    parser.add_argument("--dropout", type=float, default=0.1)
    parser.add_argument("--hidden-dims", type=neural.parse_hidden_dims, default=(128, 64))
    parser.add_argument("--fm-factors", type=int, default=16)
    parser.add_argument("--id-embedding-dim", type=int, default=32)
    parser.add_argument("--torch-threads", type=int, default=4)
    parser.add_argument("--neural-loss", choices=("bce", "bpr"), default="bce")
    return parser.parse_args()


def main() -> None:
    summary = run(parse_args())
    print(
        "wrote {model_path} rows={rows:,} groups={groups:,} "
        "positive_rate={positive_rate:.4f}".format(**summary)
    )


if __name__ == "__main__":
    main()
