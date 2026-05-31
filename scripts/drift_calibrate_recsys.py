"""Bootstrap-calibrate input-drift thresholds for the recsys ranker features.

We lack multiple clean temporal vintages of the 75-feature parquet, so the null
is the *finite-sample noise floor*: draw size-N subsamples from the reference
population and score them against the reference bins. PSI here arises purely from
sampling noise, so its upper quantiles are the smallest shifts distinguishable
from noise at the detector's window size. Real train/serve skew above this fires.

Limitation: this null captures sampling noise only, not seasonal/temporal
variation (no historical vintages available). Recalibrate with real vintages
once the recsys DAG produces rolling-window feature snapshots.

Outputs:
  data/drift/reference/recsys_reference.json
  data/drift/thresholds_recsys.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from ghrec.drift import build_reference, score_window  # noqa: E402
from ghrec.drift_recsys import feature_distributions, feature_names  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", default="data/features/week6/ranker_features_airflow_20260516.parquet")
    ap.add_argument("--summary", default="data/features/week6/ranker_features_airflow_20260516_summary.json")
    ap.add_argument("--n-bins", type=int, default=10)
    ap.add_argument("--sample-size", type=int, default=60000, help="null subsample size N (≈ detector window)")
    ap.add_argument("--bootstrap", type=int, default=100, help="number of null draws")
    ap.add_argument("--warn-q", type=float, default=0.95)
    ap.add_argument("--alert-q", type=float, default=0.99)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--reference-out", default="data/drift/reference/recsys_reference.json")
    ap.add_argument("--thresholds-out", default="data/drift/thresholds_recsys.json")
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    names = feature_names(args.summary, args.parquet)
    # Full reference population per feature (no sampling: build stable bins).
    ref_dists = feature_distributions(args.parquet, names, max_sample=10_000_000, rng=rng)
    print(f"reference rows-source={Path(args.parquet).name}  scored_features={len(ref_dists)}/{len(names)}")

    reference = build_reference(
        ref_dists,
        layer="recsys",
        reference_window={"parquet": Path(args.parquet).name},
        n_bins=args.n_bins,
    )

    # Bootstrap noise-floor null: subsample reference -> score against reference bins.
    samples: dict[str, list[float]] = {f: [] for f in ref_dists}
    for _ in range(args.bootstrap):
        window = {}
        for f, arr in ref_dists.items():
            # True bootstrap (with replacement) so the null reflects real
            # finite-sample variance, not the understated variance of a
            # without-replacement subsample of the same population.
            window[f] = rng.choice(arr, size=args.sample_size, replace=True)
        for f, psi in score_window(reference, window).items():
            samples[f].append(psi)

    thresholds = {}
    for f, vals in samples.items():
        a = np.asarray(vals, dtype=float)
        thresholds[f] = {
            "warn": float(np.quantile(a, args.warn_q)),
            "alert": float(np.quantile(a, args.alert_q)),
            "n_windows": int(a.size),
            "null_max": float(a.max()),
            "null_median": float(np.median(a)),
        }

    reference.to_json(args.reference_out)
    Path(args.thresholds_out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.thresholds_out).write_text(
        json.dumps(
            {
                "layer": "recsys",
                "null": "bootstrap_noise_floor",
                "params": {
                    "parquet": Path(args.parquet).name,
                    "n_bins": args.n_bins,
                    "sample_size": args.sample_size,
                    "bootstrap": args.bootstrap,
                    "warn_q": args.warn_q,
                    "alert_q": args.alert_q,
                },
                "features": thresholds,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    ordered = sorted(thresholds.items(), key=lambda kv: kv[1]["alert"], reverse=True)
    print("\ntop-10 noise-floor thresholds (highest alert):")
    print("feature                              warn(p95)  alert(p99)  null_max")
    for f, t in ordered[:10]:
        print(f"{f:<36} {t['warn']:>8.5f}  {t['alert']:>9.5f}  {t['null_max']:>8.5f}")
    print(f"\nwrote {args.reference_out}\nwrote {args.thresholds_out}")


if __name__ == "__main__":
    main()
