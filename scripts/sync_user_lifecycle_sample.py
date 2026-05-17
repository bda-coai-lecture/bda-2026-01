"""Build a systematic-sample user lifecycle mart from local daily activity."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd
from google.cloud import bigquery


DEFAULT_PROJECT = "bda-coai"
DEFAULT_DATASET = "mart"
DEFAULT_TABLE = "metrics_user_lifecycle_sample_daily"
DEFAULT_PARQUET_DIR = Path("data/daily_agg")
DEFAULT_SAMPLE_SEED = "bda-user-lifecycle-v1"


def parse_day(path: Path) -> date:
    return datetime.strptime(path.stem, "%Y%m%d").date()


def iter_parquet_files(parquet_dir: Path, start: str | None, end: str | None) -> list[Path]:
    start_day = datetime.strptime(start, "%Y-%m-%d").date() if start else None
    end_day = datetime.strptime(end, "%Y-%m-%d").date() if end else None
    files = []
    for path in sorted(parquet_dir.glob("*.parquet")):
        day = parse_day(path)
        if start_day and day < start_day:
            continue
        if end_day and day > end_day:
            continue
        files.append(path)
    return files


def load_sample_activity(files: list[Path], sample_seed: str, sample_k: int) -> dict[date, set[int]]:
    del sample_seed
    file_list = ", ".join("'" + path.as_posix().replace("'", "''") + "'" for path in files)
    con = duckdb.connect(database=":memory:")
    con.execute("SET memory_limit = '4GB'")
    con.execute("SET threads = 4")
    rows = con.sql(
        f"""
        SELECT
          CAST(strptime(regexp_extract(filename, '([0-9]{{8}})\\.parquet$', 1), '%Y%m%d') AS DATE) AS activity_date,
          CAST(actor_id AS BIGINT) AS user_id
        FROM read_parquet([{file_list}])
        WHERE actor_id IS NOT NULL
          AND hash(CAST(actor_id AS BIGINT)) % {int(sample_k)} = 0
        GROUP BY activity_date, user_id
        ORDER BY activity_date, user_id
        """
    ).fetchall()
    activity: dict[date, set[int]] = {}
    for day, user_id in rows:
        activity.setdefault(day, set()).add(int(user_id))
    for path in files:
        activity.setdefault(parse_day(path), set())
    return activity


def build_lifecycle(activity: dict[date, set[int]], sample_seed: str, sample_k: int) -> pd.DataFrame:
    days = sorted(activity)
    first_seen: dict[int, date] = {}
    user_days: dict[int, set[date]] = defaultdict(set)
    for day in days:
        for user_id in activity[day]:
            first_seen.setdefault(user_id, day)
            user_days[user_id].add(day)

    rows = []
    min_day = days[0]
    for day in days:
        active_today = activity[day]
        prev_28_days = {day - timedelta(days=offset) for offset in range(1, 29)}
        churn_base_day = day - timedelta(days=29)
        churn_base_users = activity.get(churn_base_day, set())
        recent_window_with_today = {day - timedelta(days=offset) for offset in range(0, 29)}

        new_users = {user_id for user_id in active_today if first_seen[user_id] == day}
        existing_users = {
            user_id
            for user_id in active_today
            if first_seen[user_id] < day and user_days[user_id].intersection(prev_28_days)
        }
        returning_users = {
            user_id
            for user_id in active_today
            if first_seen[user_id] < day and not user_days[user_id].intersection(prev_28_days)
        }
        churned_users = {
            user_id
            for user_id in churn_base_users
            if not user_days[user_id].intersection(recent_window_with_today)
        }

        rows.append(
            {
                "activity_date": day,
                "sample_active_users": len(active_today),
                "new_users": len(new_users),
                "existing_users": len(existing_users),
                "returning_users": len(returning_users),
                "churned_users": len(churned_users),
                "churn_base_date": churn_base_day if churn_base_day >= min_day else None,
                "sample_seed": sample_seed,
                "sample_k": sample_k,
                "is_complete_28d_window": day >= min_day + timedelta(days=29),
                "synced_at": datetime.now(timezone.utc),
            }
        )
    return pd.DataFrame(rows)


def upload(client: bigquery.Client, table_id: str, df: pd.DataFrame) -> None:
    schema = [
        bigquery.SchemaField("activity_date", "DATE"),
        bigquery.SchemaField("sample_active_users", "INTEGER"),
        bigquery.SchemaField("new_users", "INTEGER"),
        bigquery.SchemaField("existing_users", "INTEGER"),
        bigquery.SchemaField("returning_users", "INTEGER"),
        bigquery.SchemaField("churned_users", "INTEGER"),
        bigquery.SchemaField("churn_base_date", "DATE"),
        bigquery.SchemaField("sample_seed", "STRING"),
        bigquery.SchemaField("sample_k", "INTEGER"),
        bigquery.SchemaField("is_complete_28d_window", "BOOLEAN"),
        bigquery.SchemaField("synced_at", "TIMESTAMP"),
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--key-path", type=Path, default=Path("gcp-key.json"))
    parser.add_argument("--parquet-dir", type=Path, default=DEFAULT_PARQUET_DIR)
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--sample-seed", default=DEFAULT_SAMPLE_SEED)
    parser.add_argument("--sample-k", type=int, default=607)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    files = iter_parquet_files(args.parquet_dir, args.start, args.end)
    if not files:
        raise FileNotFoundError(f"No parquet files found in {args.parquet_dir}")
    activity = load_sample_activity(files, args.sample_seed, args.sample_k)
    df = build_lifecycle(activity, args.sample_seed, args.sample_k)
    client = bigquery.Client.from_service_account_json(args.key_path, project=args.project)
    full_table_id = f"{args.project}.{args.dataset}.{args.table}"
    upload(client, full_table_id, df)
    print(
        f"UPLOADED {full_table_id} rows={len(df):,} "
        f"range={df['activity_date'].min()}..{df['activity_date'].max()} sample_k={args.sample_k}"
    )


if __name__ == "__main__":
    main()
