#!/usr/bin/env python
"""Backfill mart.fact_user_repo_activity from GitHub Archive public tables."""

from __future__ import annotations

import argparse
import os
from datetime import datetime

from google.cloud import bigquery


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="bda-coai")
    parser.add_argument("--dataset", default="mart")
    parser.add_argument("--table", default="fact_user_repo_activity")
    parser.add_argument("--staging-table")
    parser.add_argument("--key-path", default=os.environ.get("GCP_KEY_PATH"))
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--max-bytes-billed-gib", type=int, default=100)
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args()


def make_client(project: str, key_path: str | None) -> bigquery.Client:
    if key_path:
        return bigquery.Client.from_service_account_json(key_path, project=project)
    if os.path.exists("gcp-key.json"):
        return bigquery.Client.from_service_account_json("gcp-key.json", project=project)
    return bigquery.Client(project=project)


def suffix(value: str) -> str:
    return datetime.strptime(value, "%Y-%m-%d").strftime("%Y%m%d")[3:]


def main() -> None:
    args = parse_args()
    client = make_client(args.project, args.key_path)
    table_id = f"{args.project}.{args.dataset}.{args.table}"
    staging_name = args.staging_table or (
        f"{args.table}__backfill_{args.start.replace('-', '')}_{args.end.replace('-', '')}"
    )
    staging_table_id = f"{args.project}.{args.dataset}.{staging_name}"
    sql = f"""
    CREATE OR REPLACE TABLE `{staging_table_id}`
    PARTITION BY activity_date
    CLUSTER BY action, repo_id AS
    SELECT
      CAST(actor.id AS INT64) AS user_id,
      CAST(repo.id AS INT64) AS repo_id,
      CAST(type AS STRING) AS action,
      COUNT(*) AS event_count,
      PARSE_DATE('%Y%m%d', CONCAT('202', _TABLE_SUFFIX)) AS activity_date
    FROM `githubarchive.day.202*`
    WHERE _TABLE_SUFFIX BETWEEN @start_suffix AND @end_suffix
      AND actor.id IS NOT NULL
      AND repo.id IS NOT NULL
      AND type IS NOT NULL
    GROUP BY user_id, repo_id, action, activity_date
    """
    params = [
        bigquery.ScalarQueryParameter("start_suffix", "STRING", suffix(args.start)),
        bigquery.ScalarQueryParameter("end_suffix", "STRING", suffix(args.end)),
    ]
    dry_config = bigquery.QueryJobConfig(
        dry_run=True,
        use_query_cache=False,
        query_parameters=params,
    )
    dry_job = client.query(sql, job_config=dry_config)
    estimated_bytes = dry_job.total_bytes_processed or 0
    print(
        "DRY_RUN "
        f"range={args.start}..{args.end} bytes={estimated_bytes} "
        f"tib={estimated_bytes / 1024**4:.3f}",
        flush=True,
    )
    if not args.execute:
        print("PLAN_ONLY pass --execute to run the backfill", flush=True)
        return

    job_config = bigquery.QueryJobConfig(
        query_parameters=params,
        maximum_bytes_billed=args.max_bytes_billed_gib * 1024**3,
        use_query_cache=False,
    )
    job = client.query(
        sql,
        job_config=job_config,
        job_id_prefix=f"{args.table}_backfill_{args.start.replace('-', '')}_",
    )
    print(f"JOB_ID {job.job_id}", flush=True)
    print(f"STAGING {staging_table_id}", flush=True)
    print(f"TARGET {table_id}", flush=True)
    job.result()
    staging_table = client.get_table(staging_table_id)
    print(
        "STAGING_DONE "
        f"job_id={job.job_id} state={job.state} "
        f"bytes={job.total_bytes_processed} slot_ms={job.slot_millis}",
        flush=True,
    )
    print(
        "STAGING_TABLE "
        f"rows={staging_table.num_rows} logical_gib={staging_table.num_bytes / 1024**3:.2f} "
        "partitioning="
        f"{staging_table.time_partitioning.field if staging_table.time_partitioning else None}",
        flush=True,
    )
    client.delete_table(table_id, not_found_ok=True)
    copy_job = client.copy_table(staging_table_id, table_id)
    print(f"COPY_JOB_ID {copy_job.job_id}", flush=True)
    copy_job.result()
    client.delete_table(staging_table_id, not_found_ok=True)
    table = client.get_table(table_id)
    print(
        "DONE "
        f"target={table_id} rows={table.num_rows} logical_gib={table.num_bytes / 1024**3:.2f} "
        f"partitioning={table.time_partitioning.field if table.time_partitioning else None}",
        flush=True,
    )


if __name__ == "__main__":
    main()
