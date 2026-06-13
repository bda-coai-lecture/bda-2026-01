#!/usr/bin/env python
"""Fail when recent BigQuery on-demand usage exceeds a small guardrail."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from google.cloud import bigquery


PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR / "dags"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="bda-coai")
    parser.add_argument("--location", default="US")
    parser.add_argument("--key-path", default=os.environ.get("GCP_KEY_PATH"))
    parser.add_argument("--lookback-hours", type=int, default=6)
    parser.add_argument("--max-usd", type=float, default=3.0)
    parser.add_argument("--price-per-tib", type=float, default=6.25)
    parser.add_argument("--slack", action="store_true")
    return parser.parse_args()


def make_client(project: str, key_path: str | None) -> bigquery.Client:
    if key_path:
        return bigquery.Client.from_service_account_json(key_path, project=project)
    return bigquery.Client(project=project)


def source_case() -> str:
    return """
      CASE
        WHEN STARTS_WITH(query, '-- Metabase') THEN 'Metabase'
        WHEN STARTS_WITH(query, '/* {"app": "dbt"') THEN 'dbt'
        WHEN query LIKE 'DELETE FROM `bda-coai.mart.fact_user_repo_activity`%' THEN 'sync_delete'
        WHEN query LIKE '%githubarchive.day%' THEN 'githubarchive_scan'
        WHEN query LIKE '%fact_user_repo_activity%' THEN 'fact_scan'
        ELSE 'other'
      END
    """


def main() -> None:
    args = parse_args()
    client = make_client(args.project, args.key_path)
    region = f"region-{args.location.lower()}"
    price_expr = f"SAFE_DIVIDE(total_bytes_billed, POW(1024, 4)) * {args.price_per_tib}"

    summary_sql = f"""
    SELECT
      COUNT(*) AS jobs,
      COALESCE(SUM(total_bytes_billed), 0) AS bytes_billed,
      COALESCE(SUM({price_expr}), 0) AS estimated_usd
    FROM `{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
    WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {args.lookback_hours} HOUR)
      AND job_type = 'QUERY'
      AND state = 'DONE'
      AND total_bytes_billed > 0
    """
    summary = next(iter(client.query(summary_sql).result()))

    top_sql = f"""
    SELECT
      {source_case()} AS source_guess,
      COUNT(*) AS jobs,
      COALESCE(SUM(total_bytes_billed), 0) AS bytes_billed,
      COALESCE(SUM({price_expr}), 0) AS estimated_usd
    FROM `{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
    WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {args.lookback_hours} HOUR)
      AND job_type = 'QUERY'
      AND state = 'DONE'
      AND total_bytes_billed > 0
    GROUP BY source_guess
    ORDER BY estimated_usd DESC
    LIMIT 10
    """
    top_sources = [
        {
            "source_guess": row.source_guess,
            "jobs": row.jobs,
            "bytes_billed": row.bytes_billed,
            "estimated_usd": float(row.estimated_usd),
        }
        for row in client.query(top_sql).result()
    ]

    report = {
        "project": args.project,
        "lookback_hours": args.lookback_hours,
        "max_usd": args.max_usd,
        "jobs": summary.jobs,
        "bytes_billed": summary.bytes_billed,
        "estimated_usd": float(summary.estimated_usd),
        "top_sources": top_sources,
    }
    print("BIGQUERY_COST_GUARD " + json.dumps(report, ensure_ascii=False, sort_keys=True))

    if report["estimated_usd"] > args.max_usd:
        if args.slack:
            try:
                from utils.slack_alert import notify_cost_guard

                notify_cost_guard(report)
                print("slack: cost guard alert posted")
            except Exception as exc:
                print(f"slack: alert skipped ({exc})")
        raise SystemExit(
            f"BigQuery cost guard exceeded: ${report['estimated_usd']:.2f} > ${args.max_usd:.2f}"
        )


if __name__ == "__main__":
    main()
