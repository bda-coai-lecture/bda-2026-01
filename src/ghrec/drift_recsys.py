"""Recsys-layer adapter for input drift: the 75 LGBM ranker features.

A feature parquet (one training/serving vintage) holds many candidate
(user, repo) rows. Drift is measured on each feature's distribution across rows,
comparing a fresh vintage against a blessed reference vintage (train/serve skew).
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

# Non-feature columns in a ranker feature parquet.
META_COLS = {
    "group_index",
    "actor_id",
    "repo_id",
    "label",
    "raw_candidate_rank",
    "raw_candidate_score",
    "raw_candidate_source",
}


def feature_names(summary_path: str | Path | None, parquet_path: str | Path) -> list[str]:
    """Canonical feature list: prefer the run summary's feature_names, else infer."""
    if summary_path and Path(summary_path).exists():
        summary = json.loads(Path(summary_path).read_text(encoding="utf-8"))
        names = summary.get("features", {}).get("feature_names")
        if names:
            return list(names)
    cols = pq.read_schema(parquet_path).names
    return [c for c in cols if c not in META_COLS]


def feature_distributions(
    parquet_path: str | Path,
    names: list[str],
    max_sample: int = 80000,
    rng: np.random.Generator | None = None,
    min_finite: int = 1000,
) -> dict[str, np.ndarray]:
    """Per-feature value arrays (finite only) from a feature parquet, sampled.

    Features with fewer than `min_finite` finite values are dropped: they are
    too sparse to form a stable distribution and make PSI meaningless (e.g.
    event-decomposition features populated for only a handful of rows).
    """
    if rng is None:
        rng = np.random.default_rng(0)
    available = set(pq.read_schema(parquet_path).names)
    present = [n for n in names if n and n in available]
    df = pd.read_parquet(parquet_path, columns=present)
    if len(df) > max_sample:
        df = df.iloc[rng.choice(len(df), size=max_sample, replace=False)]

    out: dict[str, np.ndarray] = {}
    for name in present:
        values = df[name].to_numpy(dtype=float)
        values = values[np.isfinite(values)]
        if values.size < min_finite:
            continue
        out[name] = values
    return out
