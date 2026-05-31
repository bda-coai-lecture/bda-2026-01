"""Backtest-calibrate input-drift thresholds for the analytics layer.

Drift is measured on *within-day distributions* (not daily scalar aggregates,
which are dominated by trend/seasonality and saturate PSI). For each day we take
the distribution across users/repos of:

  events_per_user   per-user total event count   (log1p)
  events_per_repo   per-repo total event count   (log1p)
  repos_per_user    distinct repos touched / user (log1p)

A reference distribution is pooled from the first `--ref-days`. Each later day is
scored against it (one PSI per feature per day). Per-feature warn/alert thresholds
are upper quantiles of that daily-PSI null distribution.

Outputs:
  data/drift/reference/analytics_reference.json
  data/drift/thresholds.json
  data/drift/reference/analytics_daily_psi.csv   (null distribution, for inspection)
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from ghrec.drift import build_reference, score_window  # noqa: E402
from ghrec.drift_analytics import FEATURES, day_distributions  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data/daily_agg")
    ap.add_argument("--ref-days", type=int, default=45)
    ap.add_argument(
        "--null-end",
        default=None,
        help="YYYYMMDD; calibrate the null only on stable post-ref days up to this "
        "date so later real drift is not absorbed into thresholds. Default: all days.",
    )
    ap.add_argument("--n-bins", type=int, default=10)
    ap.add_argument("--max-sample", type=int, default=50000)
    ap.add_argument("--warn-q", type=float, default=0.95)
    ap.add_argument("--alert-q", type=float, default=0.99)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--reference-out", default="data/drift/reference/analytics_reference.json")
    ap.add_argument("--thresholds-out", default="data/drift/thresholds.json")
    ap.add_argument("--psi-out", default="data/drift/reference/analytics_daily_psi.csv")
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    paths = sorted(Path(args.data_dir).glob("*.parquet"))
    if len(paths) < args.ref_days + 2:
        raise SystemExit(f"not enough days: {len(paths)}")
    print(f"{len(paths)} days: {paths[0].stem}..{paths[-1].stem}")

    ref_paths = paths[: args.ref_days]
    pooled: dict[str, list[np.ndarray]] = {f: [] for f in FEATURES}
    for p in ref_paths:
        for f, arr in day_distributions(p, args.max_sample, rng).items():
            pooled[f].append(arr)
    reference = build_reference(
        {f: np.concatenate(v) for f, v in pooled.items()},
        layer="analytics",
        reference_window={"start": ref_paths[0].stem, "end": ref_paths[-1].stem},
        n_bins=args.n_bins,
    )

    # Each post-reference day is one null sample.
    psi_rows = {}
    for p in paths[args.ref_days :]:
        psi_rows[p.stem] = score_window(reference, day_distributions(p, args.max_sample, rng))
    psi_frame = pd.DataFrame.from_dict(psi_rows, orient="index").sort_index()
    psi_frame.index.name = "date"

    # Calibrate the null on the stable window only (exclude later real drift).
    null_frame = psi_frame
    if args.null_end is not None:
        null_frame = psi_frame[psi_frame.index <= args.null_end]
        if null_frame.empty:
            raise SystemExit(f"--null-end {args.null_end} leaves no calibration days")

    # Per-feature warn/alert = upper quantiles of the daily-PSI null distribution.
    thresholds = {}
    for f in FEATURES:
        arr = null_frame[f].to_numpy(dtype=float)
        thresholds[f] = {
            "warn": float(np.quantile(arr, args.warn_q)),
            "alert": float(np.quantile(arr, args.alert_q)),
            "n_windows": int(arr.size),
            "null_max": float(arr.max()),
            "null_median": float(np.median(arr)),
        }

    reference.to_json(args.reference_out)
    Path(args.thresholds_out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.thresholds_out).write_text(
        json.dumps(
            {
                "layer": "analytics",
                "params": {
                    "ref_days": args.ref_days,
                    "null_end": args.null_end,
                    "n_bins": args.n_bins,
                    "max_sample": args.max_sample,
                    "warn_q": args.warn_q,
                    "alert_q": args.alert_q,
                },
                "features": thresholds,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    psi_frame.to_csv(args.psi_out)

    null_range = f"{null_frame.index[0]}..{null_frame.index[-1]}" if args.null_end else "all"
    print(f"\nnull calibration window: {null_range} ({len(null_frame)} days)")
    print("\nfeature              warn(p95)  alert(p99)  null_med  null_max")
    for f in FEATURES:
        t = thresholds[f]
        print(
            f"{f:<20} {t['warn']:>8.4f}  {t['alert']:>9.4f}  "
            f"{t['null_median']:>7.4f}  {t['null_max']:>7.4f}"
        )

    # Validation: which days breach the calibrated thresholds (alert level)?
    print("\nalert-level breaches across full series:")
    any_breach = False
    for f in FEATURES:
        alert = thresholds[f]["alert"]
        breaches = psi_frame.index[psi_frame[f] >= alert].tolist()
        if breaches:
            any_breach = True
            shown = ", ".join(f"{d}({psi_frame.loc[d, f]:.3f})" for d in breaches[:8])
            print(f"  {f:<20} {len(breaches)}d: {shown}")
    if not any_breach:
        print("  none")
    print(f"\nwrote {args.reference_out}\nwrote {args.thresholds_out}\nwrote {args.psi_out}")


if __name__ == "__main__":
    main()
