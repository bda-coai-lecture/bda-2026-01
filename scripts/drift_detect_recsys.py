"""Recsys ranker-feature input-drift check (warn-only).

Scores a target feature parquet (a fresh training/serving vintage) against the
blessed reference vintage and calibrated noise-floor thresholds, writes a JSON
report, and optionally posts a warn-only Slack alert. Always exits 0.

Called by the gharchive_recsys_features DAG after the ranker feature build.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR / "src"))
sys.path.insert(0, str(PROJECT_DIR / "dags"))  # for utils.slack_alert
from ghrec.drift import DriftReference, evaluate  # noqa: E402
from ghrec.drift_recsys import feature_distributions, feature_names  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", default="data/features/week6/ranker_features_airflow_20260516.parquet")
    ap.add_argument("--summary", default=None, help="run summary for canonical feature_names")
    ap.add_argument("--reference", default="data/drift/reference/recsys_reference.json")
    ap.add_argument("--thresholds", default="data/drift/thresholds_recsys.json")
    ap.add_argument("--report-dir", default="data/drift/reports")
    ap.add_argument("--max-sample", type=int, default=80000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--slack", action="store_true")
    args = ap.parse_args()

    if not Path(args.parquet).exists():
        raise SystemExit(f"missing feature parquet: {args.parquet}")

    reference = DriftReference.from_json(args.reference)
    thresholds = json.loads(Path(args.thresholds).read_text(encoding="utf-8"))["features"]

    rng = np.random.default_rng(args.seed)
    names = feature_names(args.summary, args.parquet)
    window = feature_distributions(args.parquet, names, max_sample=args.max_sample, rng=rng)
    report = evaluate(reference, window, thresholds)
    report["parquet"] = Path(args.parquet).name

    tag = Path(args.parquet).stem
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"recsys_{tag}.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"[{tag}] overall_status={report['overall_status']}")
    flagged = [f for f in report["features"] if f["status"] != "ok"]
    for f in flagged[:12]:
        print(f"  {f['feature']:<36} psi={f['psi']:.5f}  status={f['status']}")
    if not flagged:
        print("  (no features above threshold)")
    print(f"wrote {report_path}")

    if args.slack and report["overall_status"] != "ok":
        try:
            from utils.slack_alert import notify_drift

            notify_drift("recsys", report)
            print("slack: drift alert posted")
        except Exception as exc:
            print(f"slack: alert skipped ({exc})")


if __name__ == "__main__":
    main()
