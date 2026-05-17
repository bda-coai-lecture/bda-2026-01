"""Upload a small repo metadata sample mart to BigQuery.

This mart is for BI inspection, not model training. It shows repo creation dates
for high-activity repos and repos discovered by the metadata systematic sample.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
from google.cloud import bigquery

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from refresh_repo_metadata import (  # noqa: E402
    daily_active_users,
    iter_parquet_files,
    latest_file,
    parse_day,
    repo_ids_for_users,
    sampled_users,
    top_repo_ids,
)


DEFAULT_PROJECT = "bda-coai"
DEFAULT_DATASET = "mart"
DEFAULT_TABLE = "metrics_repo_metadata_sample"
DEFAULT_DB_PATH = Path("data/repo_metadata.db")
DEFAULT_PARQUET_DIR = Path("data/daily_agg")
DEFAULT_SAMPLE_SEED = "bda-repo-metadata-v1"


def table_id(project: str, dataset: str, table: str) -> str:
    return f"{project}.{dataset}.{table}"


def load_metadata(db_path: Path) -> pd.DataFrame:
    with sqlite3.connect(db_path) as con:
        df = pd.read_sql_query(
            """
            SELECT
              repo_id,
              repo_name,
              description,
              language,
              stargazers,
              forks,
              topics,
              license_key,
              created_at,
              updated_at,
              archived,
              fetched_at,
              http_status
            FROM repo_metadata
            WHERE created_at IS NOT NULL
            """,
            con,
        )
    for col in ["created_at", "updated_at", "fetched_at"]:
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    return df


def activity_counts(files: list[Path], repo_ids: list[int]) -> pd.DataFrame:
    if not repo_ids:
        return pd.DataFrame(columns=["repo_id", "recent_events"])
    con = duckdb.connect(database=":memory:")
    con.execute("SET memory_limit = '2GB'")
    con.execute("SET threads = 1")
    con.execute("CREATE TEMP TABLE selected_repos(repo_id BIGINT)")
    con.executemany("INSERT INTO selected_repos VALUES (?)", [(int(repo_id),) for repo_id in repo_ids])
    file_list = ", ".join("'" + path.as_posix().replace("'", "''") + "'" for path in files)
    return con.sql(
        f"""
        SELECT CAST(d.repo_id AS BIGINT) AS repo_id, SUM(d.cnt) AS recent_events
        FROM read_parquet([{file_list}]) d
        JOIN selected_repos s
          ON CAST(d.repo_id AS BIGINT) = s.repo_id
        GROUP BY d.repo_id
        """
    ).df()


def build_sample(
    db_path: Path,
    parquet_dir: Path,
    start: str | None,
    end: str | None,
    top_n: int,
    output_per_group: int,
    sample_seed: str,
    sample_k: int,
) -> pd.DataFrame:
    files = iter_parquet_files(parquet_dir, start, end)
    if not files:
        raise FileNotFoundError(f"No parquet files found in {parquet_dir}")

    metadata = load_metadata(db_path)
    recent_top_ids = top_repo_ids(files, top_n)
    day_file = latest_file(files)
    if day_file is None:
        raise FileNotFoundError("No latest parquet file found")

    active_users = daily_active_users(day_file)
    sample_user_ids = sampled_users(active_users, sample_seed, sample_k)
    top_set = set(recent_top_ids)
    sample_ids = [
        repo_id for repo_id in repo_ids_for_users(day_file, sample_user_ids)
        if repo_id not in top_set
    ]

    selected_ids = list(dict.fromkeys(recent_top_ids[:top_n] + sample_ids))
    activity = activity_counts(files, selected_ids)
    enriched = metadata.merge(activity, on="repo_id", how="left")
    enriched["recent_events"] = enriched["recent_events"].fillna(0).astype("int64")

    top = (
        enriched[enriched["repo_id"].isin(recent_top_ids)]
        .sort_values(["recent_events", "stargazers", "repo_id"], ascending=[False, False, True])
        .head(output_per_group)
        .copy()
    )
    top["sample_type"] = "top_activity_repo"

    systematic = (
        enriched[enriched["repo_id"].isin(sample_ids)]
        .sort_values(["fetched_at", "recent_events", "repo_id"], ascending=[False, False, True])
        .head(output_per_group)
        .copy()
    )
    systematic["sample_type"] = "systematic_sample_repo"

    out = pd.concat([top, systematic], ignore_index=True)
    if out.empty:
        raise RuntimeError("No sample rows with repo metadata were found")

    out.insert(1, "sample_rank", out.groupby("sample_type").cumcount() + 1)
    out["sample_date"] = parse_day(day_file)
    out["activity_start_date"] = parse_day(files[0])
    out["activity_end_date"] = parse_day(files[-1])
    out["sample_seed"] = sample_seed
    out["sample_k"] = int(sample_k)
    out["synced_at"] = datetime.now(timezone.utc)
    out["stargazers"] = out["stargazers"].fillna(0).astype("int64")
    out["forks"] = out["forks"].fillna(0).astype("int64")
    out["archived"] = out["archived"].fillna(0).astype("int64")
    out["http_status"] = out["http_status"].fillna(0).astype("int64")

    columns = [
        "sample_type",
        "sample_rank",
        "repo_id",
        "repo_name",
        "description",
        "language",
        "stargazers",
        "forks",
        "topics",
        "license_key",
        "created_at",
        "updated_at",
        "archived",
        "fetched_at",
        "http_status",
        "recent_events",
        "sample_date",
        "activity_start_date",
        "activity_end_date",
        "sample_seed",
        "sample_k",
        "synced_at",
    ]
    return out[columns]


def upload(client: bigquery.Client, full_table_id: str, df: pd.DataFrame) -> None:
    schema = [
        bigquery.SchemaField("sample_type", "STRING"),
        bigquery.SchemaField("sample_rank", "INTEGER"),
        bigquery.SchemaField("repo_id", "INTEGER"),
        bigquery.SchemaField("repo_name", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("language", "STRING"),
        bigquery.SchemaField("stargazers", "INTEGER"),
        bigquery.SchemaField("forks", "INTEGER"),
        bigquery.SchemaField("topics", "STRING"),
        bigquery.SchemaField("license_key", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("archived", "INTEGER"),
        bigquery.SchemaField("fetched_at", "TIMESTAMP"),
        bigquery.SchemaField("http_status", "INTEGER"),
        bigquery.SchemaField("recent_events", "INTEGER"),
        bigquery.SchemaField("sample_date", "DATE"),
        bigquery.SchemaField("activity_start_date", "DATE"),
        bigquery.SchemaField("activity_end_date", "DATE"),
        bigquery.SchemaField("sample_seed", "STRING"),
        bigquery.SchemaField("sample_k", "INTEGER"),
        bigquery.SchemaField("synced_at", "TIMESTAMP"),
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.load_table_from_dataframe(df, full_table_id, job_config=job_config).result()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--key-path", type=Path, default=Path("gcp-key.json"))
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--parquet-dir", type=Path, default=DEFAULT_PARQUET_DIR)
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--top-n", type=int, default=1000)
    parser.add_argument("--output-per-group", type=int, default=25)
    parser.add_argument("--sample-seed", default=DEFAULT_SAMPLE_SEED)
    parser.add_argument("--sample-k", type=int, default=607)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = build_sample(
        db_path=args.db_path,
        parquet_dir=args.parquet_dir,
        start=args.start,
        end=args.end,
        top_n=args.top_n,
        output_per_group=args.output_per_group,
        sample_seed=args.sample_seed,
        sample_k=args.sample_k,
    )
    client = bigquery.Client.from_service_account_json(args.key_path, project=args.project)
    full_table_id = table_id(args.project, args.dataset, args.table)
    upload(client, full_table_id, df)
    print(
        f"UPLOADED {full_table_id} rows={len(df):,} "
        f"sample_types={sorted(df['sample_type'].unique())}"
    )


if __name__ == "__main__":
    main()
