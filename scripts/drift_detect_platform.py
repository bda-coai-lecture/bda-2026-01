"""Daily analytics input-drift check (warn-only).

Scores one day's within-day distributions against the calibrated reference and
thresholds, writes a JSON report, and (optionally) posts a warn-only Slack alert.
Always exits 0 — drift is informational, not a pipeline failure.

Called by the gharchive_dbt_metrics DAG after the metrics build.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import numpy as np

PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR / "src"))
sys.path.insert(0, str(PROJECT_DIR / "dags"))  # for utils.slack_alert
from ghrec.drift import DriftReference, evaluate  # noqa: E402
from ghrec.drift_analytics import day_distributions, day_distributions_from_bq  # noqa: E402


def _resolve_date(data_dir: Path, date: str | None) -> str:
    if date:
        return date
    # Default to the latest available daily_agg file.
    files = sorted(data_dir.glob("*.parquet"))
    if not files:
        raise SystemExit(f"no parquet files in {data_dir}")
    return files[-1].stem


def _normalize_date(date: str) -> tuple[str, str]:
    compact = date.replace("-", "")
    parsed = datetime.strptime(compact, "%Y%m%d").date()
    return parsed.strftime("%Y%m%d"), parsed.isoformat()


def _resolve_bq_date(client, table_id: str, date: str | None) -> tuple[str, str]:
    if date:
        return _normalize_date(date)
    row = next(
        iter(client.query(f"select max(activity_date) as max_date from `{table_id}`").result())
    )
    if not row.max_date:
        raise SystemExit(f"no activity_date rows in {table_id}")
    return row.max_date.strftime("%Y%m%d"), row.max_date.isoformat()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["bigquery", "parquet"], default="bigquery")
    ap.add_argument("--data-dir", default="data/daily_agg")
    ap.add_argument("--date", default=None, help="YYYYMMDD; default = latest available")
    ap.add_argument("--project", default="bda-coai")
    ap.add_argument("--dataset", default="mart")
    ap.add_argument("--fact-table", default="fact_user_repo_activity")
    ap.add_argument("--key-path", default=None)
    ap.add_argument("--reference", default="data/drift/reference/analytics_reference.json")
    ap.add_argument("--thresholds", default="data/drift/thresholds.json")
    ap.add_argument("--report-dir", default="data/drift/reports")
    ap.add_argument("--max-sample", type=int, default=50000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--slack", action="store_true", help="post a warn-only Slack alert on drift")
    args = ap.parse_args()

    reference = DriftReference.from_json(args.reference)
    thresholds = json.loads(Path(args.thresholds).read_text(encoding="utf-8"))["features"]
    rng = np.random.default_rng(args.seed)

    if args.source == "parquet":
        data_dir = Path(args.data_dir)
        date = _resolve_date(data_dir, args.date)
        date, _ = _normalize_date(date)
        parquet = data_dir / f"{date}.parquet"
        if not parquet.exists():
            raise SystemExit(f"missing daily_agg for {date}: {parquet}")
        window = day_distributions(parquet, args.max_sample, rng)
    else:
        try:
            from google.cloud import bigquery
        except ImportError as exc:
            raise SystemExit(f"missing dependency for BigQuery source: {exc}") from exc
        table_id = f"{args.project}.{args.dataset}.{args.fact_table}"
        if args.key_path:
            client = bigquery.Client.from_service_account_json(args.key_path, project=args.project)
        else:
            client = bigquery.Client(project=args.project)
        date, iso_date = _resolve_bq_date(client, table_id, args.date)
        window = day_distributions_from_bq(client, table_id, iso_date, args.max_sample, rng)

    report = evaluate(reference, window, thresholds)
    report["date"] = date

    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"analytics_{date}.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"[{date}] overall_status={report['overall_status']}")
    for f in report["features"]:
        print(f"  {f['feature']:<20} psi={f['psi']:.4f}  status={f['status']}")
    print(f"wrote {report_path}")

    if args.slack and report["overall_status"] != "ok":
        try:
            from utils.slack_alert import notify_drift

            notify_drift("analytics", report)
            print("slack: drift alert posted")
        except Exception as exc:  # never fail the task on alerting issues
            print(f"slack: alert skipped ({exc})")


if __name__ == "__main__":
    main()
