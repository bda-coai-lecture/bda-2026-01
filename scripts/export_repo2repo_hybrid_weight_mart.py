"""Export weighted cooc/ALS repo2repo marts from existing candidate files."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cooc-path", type=Path, required=True)
    parser.add_argument("--als-path", type=Path, required=True)
    parser.add_argument("--output-path", type=Path, required=True)
    parser.add_argument("--cooc-weight", type=float, required=True)
    parser.add_argument("--top-k", type=int, default=100)
    parser.add_argument("--label-split", default="train", choices=("train", "test"))
    return parser.parse_args()


def normalized_score(frame: pd.DataFrame, score_col: str, out_col: str) -> pd.Series:
    grouped = frame.groupby("anchor_repo_id", observed=True)[score_col]
    if score_col == "als_score":
        return grouped.transform(lambda s: (s - s.min()) / (s.max() - s.min()) if s.max() > s.min() else s)
    return grouped.transform(lambda s: s / s.max() if s.max() > 0 else s)


def main() -> None:
    args = parse_args()
    cooc = pd.read_parquet(args.cooc_path)
    als = pd.read_parquet(args.als_path)
    cooc = cooc[cooc["label_split"].eq(args.label_split)][
        ["anchor_repo_id", "target_repo_id", "cooc_score"]
    ]
    als = als[als["label_split"].eq(args.label_split)][
        ["anchor_repo_id", "target_repo_id", "als_score"]
    ]
    merged = cooc.merge(als, on=["anchor_repo_id", "target_repo_id"], how="outer")
    merged["cooc_score"] = merged["cooc_score"].fillna(0.0)
    merged["als_score"] = merged["als_score"].fillna(0.0)
    merged["cooc_norm_score"] = normalized_score(merged, "cooc_score", "cooc_norm_score")
    merged["als_norm_score"] = normalized_score(merged, "als_score", "als_norm_score")
    merged["score"] = (
        args.cooc_weight * merged["cooc_norm_score"]
        + (1.0 - args.cooc_weight) * merged["als_norm_score"]
    )
    merged = merged.sort_values(
        ["anchor_repo_id", "score", "cooc_score", "als_score", "target_repo_id"],
        ascending=[True, False, False, False, True],
    )
    merged["rank"] = merged.groupby("anchor_repo_id", observed=True).cumcount() + 1
    merged = merged[merged["rank"] <= args.top_k].copy()
    out = merged.rename(
        columns={
            "target_repo_id": "related_repo_id",
            "score": "cooc_score",
            "cooc_score": "source_cooc_score",
        }
    )[
        [
            "anchor_repo_id",
            "related_repo_id",
            "rank",
            "cooc_score",
            "source_cooc_score",
            "als_score",
        ]
    ]
    out["mart_run"] = f"hybrid_cooc_weight_{args.cooc_weight:g}"
    out["label_split"] = args.label_split
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(args.output_path, index=False)
    print(f"wrote {args.output_path} rows={len(out):,} anchors={out['anchor_repo_id'].nunique():,}")


if __name__ == "__main__":
    main()
