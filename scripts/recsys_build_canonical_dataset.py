"""Build the canonical split dataset for the V2 recommender experiment.

The canonical dataset keeps temporal roles explicit:
history is context/seen/filter data, rank_label is the train positive split,
and test is reserved for final evaluation.
"""

from __future__ import annotations

import argparse
import os
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from gharchive.loader import load_period
from recsys_v2_common import (
    DATA_DIR,
    MART_DIR,
    Paths,
    build_feedback,
    canonical_frame,
    ensure_dirs,
    filter_catalog,
    load_mart_split,
    maybe_sample_users,
    parse_date,
    parse_event_weights,
    write_json,
)


DEFAULT_HISTORY_START = date(2026, 3, 14)
DEFAULT_HISTORY_END = date(2026, 4, 24)
DEFAULT_RANK_START = date(2026, 4, 25)
DEFAULT_RANK_END = date(2026, 5, 1)
DEFAULT_TEST_START = date(2026, 5, 2)
DEFAULT_TEST_END = date(2026, 5, 8)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=["daily", "mart", "bigquery"], default="daily")
    parser.add_argument("--daily-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--mart-path", type=Path, default=MART_DIR / "experiment_split_mart.parquet")
    parser.add_argument("--allow-unversioned-mart", action="store_true")
    parser.add_argument("--bq-project", default="bda-coai")
    parser.add_argument("--bq-dataset", default="mart")
    parser.add_argument("--bq-table", default="fact_user_repo_activity")
    parser.add_argument("--bq-location", default="US")
    parser.add_argument("--gcp-key-path", type=Path, default=Path(os.environ.get("GCP_KEY_PATH", "gcp-key.json")))
    parser.add_argument("--suffix", default="latest")
    parser.add_argument("--history-start", type=parse_date, default=DEFAULT_HISTORY_START)
    parser.add_argument("--history-end", type=parse_date, default=DEFAULT_HISTORY_END)
    parser.add_argument("--rank-start", type=parse_date, default=DEFAULT_RANK_START)
    parser.add_argument("--rank-end", type=parse_date, default=DEFAULT_RANK_END)
    parser.add_argument("--test-start", type=parse_date, default=DEFAULT_TEST_START)
    parser.add_argument("--test-end", type=parse_date, default=DEFAULT_TEST_END)
    parser.add_argument("--event-weight", action="append", default=None)
    parser.add_argument("--min-item-users", type=int, default=2)
    parser.add_argument("--min-user-items", type=int, default=2)
    parser.add_argument("--max-items", type=int, default=None)
    parser.add_argument("--sample-ratio", type=float, default=1.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Use a small deterministic user/item sample for fast local checks.",
    )
    return parser.parse_args()


def validate_windows(args: argparse.Namespace) -> None:
    windows = [
        ("history", args.history_start, args.history_end),
        ("rank_label", args.rank_start, args.rank_end),
        ("test", args.test_start, args.test_end),
    ]
    for name, start, end in windows:
        if start > end:
            raise ValueError(f"{name} start date {start} is after end date {end}")
    if not (args.history_end < args.rank_start <= args.rank_end < args.test_start <= args.test_end):
        raise ValueError(
            "split windows must be ordered as history < rank_label < test with no overlap"
        )
    if args.source == "mart" and args.event_weight:
        raise ValueError("--event-weight cannot be combined with --source mart")
    if args.source == "bigquery" and not args.gcp_key_path.exists():
        raise FileNotFoundError(
            f"BigQuery source requires a service account key: {args.gcp_key_path}"
        )


def load_daily_feedback(
    daily_dir: Path,
    start: date,
    end: date,
    event_weights: dict[str, float],
) -> pd.DataFrame:
    events = load_period(daily_dir, start, end)
    return build_feedback(events, event_weights)


def load_bigquery_feedback(
    project: str,
    dataset: str,
    table: str,
    location: str,
    key_path: Path,
    start: date,
    end: date,
    event_weights: dict[str, float],
) -> pd.DataFrame:
    from google.cloud import bigquery

    client = bigquery.Client.from_service_account_json(
        str(key_path),
        project=project,
        location=location,
    )
    table_id = f"`{project}.{dataset}.{table}`"
    query = f"""
    SELECT
      CAST(user_id AS INT64) AS actor_id,
      CAST(repo_id AS INT64) AS repo_id,
      CAST(action AS STRING) AS type,
      SUM(CAST(event_count AS INT64)) AS cnt
    FROM {table_id}
    WHERE activity_date BETWEEN @start_date AND @end_date
      AND user_id IS NOT NULL
      AND repo_id IS NOT NULL
      AND action IS NOT NULL
    GROUP BY actor_id, repo_id, type
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start),
            bigquery.ScalarQueryParameter("end_date", "DATE", end),
        ],
        use_query_cache=True,
    )
    events = client.query(query, job_config=job_config).to_dataframe()
    return build_feedback(events, event_weights)


def load_split_feedback(
    args: argparse.Namespace,
    event_weights: dict[str, float],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if args.source == "daily":
        return (
            load_daily_feedback(args.daily_dir, args.history_start, args.history_end, event_weights),
            load_daily_feedback(args.daily_dir, args.rank_start, args.rank_end, event_weights),
            load_daily_feedback(args.daily_dir, args.test_start, args.test_end, event_weights),
        )
    if args.source == "bigquery":
        return (
            load_bigquery_feedback(
                args.bq_project,
                args.bq_dataset,
                args.bq_table,
                args.bq_location,
                args.gcp_key_path,
                args.history_start,
                args.history_end,
                event_weights,
            ),
            load_bigquery_feedback(
                args.bq_project,
                args.bq_dataset,
                args.bq_table,
                args.bq_location,
                args.gcp_key_path,
                args.rank_start,
                args.rank_end,
                event_weights,
            ),
            load_bigquery_feedback(
                args.bq_project,
                args.bq_dataset,
                args.bq_table,
                args.bq_location,
                args.gcp_key_path,
                args.test_start,
                args.test_end,
                event_weights,
            ),
        )
    return (
        load_mart_split(
            args.mart_path,
            "history",
            args.history_start,
            args.history_end,
            args.allow_unversioned_mart,
        ),
        load_mart_split(
            args.mart_path,
            "rank_label",
            args.rank_start,
            args.rank_end,
            args.allow_unversioned_mart,
        ),
        load_mart_split(
            args.mart_path,
            "test",
            args.test_start,
            args.test_end,
            args.allow_unversioned_mart,
        ),
    )


def split_summary(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "positive_pairs": int(len(frame)),
        "users": int(frame["actor_id"].nunique()),
        "items": int(frame["repo_id"].nunique()),
        "score_sum": float(frame["score"].sum()),
    }


def main() -> None:
    args = parse_args()
    validate_windows(args)
    ensure_dirs()

    if args.smoke:
        args.sample_ratio = min(args.sample_ratio, 0.03)
        args.max_items = args.max_items or 5000

    paths = Paths(args.suffix)
    event_weights = parse_event_weights(args.event_weight)

    print("1. load split feedback")
    history, rank, test = load_split_feedback(args, event_weights)

    raw_summary = {
        "history": split_summary(history),
        "rank_label": split_summary(rank),
        "test": split_summary(test),
    }

    print("2. retain warm catalog/users")
    history, rank, test, filter_summary = filter_catalog(
        history,
        rank,
        test,
        min_item_users=args.min_item_users,
        min_user_items=args.min_user_items,
        max_items=args.max_items,
    )
    history, rank, test, sample_summary = maybe_sample_users(
        history,
        rank,
        test,
        sample_ratio=args.sample_ratio,
        seed=args.seed,
    )

    canonical = canonical_frame(history, rank, test)
    paths.canonical.parent.mkdir(parents=True, exist_ok=True)
    canonical.to_parquet(paths.canonical, index=False)

    retained_items = set(history["repo_id"].unique())
    retained_users = set(history["actor_id"].unique())
    summary = {
        "source": args.source,
        "daily_dir": str(args.daily_dir),
        "mart_path": str(args.mart_path),
        "allow_unversioned_mart": bool(args.allow_unversioned_mart),
        "bq_project": args.bq_project if args.source == "bigquery" else None,
        "bq_dataset": args.bq_dataset if args.source == "bigquery" else None,
        "bq_table": args.bq_table if args.source == "bigquery" else None,
        "bq_location": args.bq_location if args.source == "bigquery" else None,
        "suffix": args.suffix,
        "smoke": bool(args.smoke),
        "split_periods": {
            "history": {"start": args.history_start, "end": args.history_end},
            "rank_label": {"start": args.rank_start, "end": args.rank_end},
            "test": {"start": args.test_start, "end": args.test_end},
        },
        "event_weights": event_weights,
        "raw_positive_counts": raw_summary,
        "retained_positive_counts": {
            "history": split_summary(history),
            "rank_label": split_summary(rank),
            "test": split_summary(test),
        },
        "retained_catalog": {
            "users": int(len(retained_users)),
            "items": int(len(retained_items)),
            "rank_label_users": int(rank["actor_id"].nunique()),
            "rank_label_items": int(rank["repo_id"].nunique()),
            "test_users": int(test["actor_id"].nunique()),
            "test_items": int(test["repo_id"].nunique()),
        },
        "filters": filter_summary,
        "sampling": sample_summary,
        "paths": {"canonical": str(paths.canonical), "summary": str(paths.canonical_summary)},
    }
    write_json(paths.canonical_summary, summary)

    print(f"wrote {paths.canonical}")
    print(f"wrote {paths.canonical_summary}")


if __name__ == "__main__":
    main()
