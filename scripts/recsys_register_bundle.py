"""Register a local recommendation artifact bundle."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from ghrec.mlops_registry import (
    ArtifactBundle,
    LocalBundleRegistry,
    normalize_manifest_metrics,
    utc_now_iso,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle-id", required=True)
    parser.add_argument("--dataset-suffix", required=True)
    parser.add_argument("--candidate-suffix", required=True)
    parser.add_argument("--ranker-suffix", required=True)
    parser.add_argument("--eval-suffix", required=True)
    parser.add_argument("--promote", action="store_true")
    parser.add_argument("--promoted-by", default="local")
    parser.add_argument("--reason", default=None)
    return parser.parse_args()


def metric_map(eval_metrics_path: Path) -> dict[str, float]:
    if not eval_metrics_path.exists():
        return {}
    frame = pd.read_csv(eval_metrics_path)
    return normalize_manifest_metrics(frame.to_dict(orient="records"))


def build_bundle(args: argparse.Namespace) -> ArtifactBundle:
    paths = {
        "canonical": f"data/features/recsys_v2/canonical_{args.dataset_suffix}.parquet",
        "candidates": f"data/features/recsys_v2/retrieval_candidates_{args.candidate_suffix}.parquet",
        "ranker_model": f"data/models/recsys_v2/ranker_lgbm_{args.ranker_suffix}.pkl",
        "ranker_summary": f"data/models/recsys_v2/ranker_lgbm_{args.ranker_suffix}_summary.json",
        "eval_metrics": f"data/results/recsys_v2/eval_metrics_{args.eval_suffix}.csv",
        "eval_summary": f"data/results/recsys_v2/eval_{args.eval_suffix}_summary.json",
    }
    return ArtifactBundle(
        bundle_id=args.bundle_id,
        status="candidate",
        created_at=utc_now_iso(),
        dataset_suffix=args.dataset_suffix,
        candidate_suffix=args.candidate_suffix,
        ranker_suffix=args.ranker_suffix,
        paths=paths,
        metrics=metric_map(Path(paths["eval_metrics"])),
    )


def main() -> None:
    args = parse_args()
    registry = LocalBundleRegistry()
    bundle = registry.upsert_bundle(build_bundle(args))
    if args.promote:
        bundle = registry.promote(bundle.bundle_id, promoted_by=args.promoted_by, reason=args.reason)
    print(f"registered {bundle.bundle_id} status={bundle.status}")


if __name__ == "__main__":
    main()
