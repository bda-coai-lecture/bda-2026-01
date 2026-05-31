#!/usr/bin/env python
"""Backfill compact GitHub Archive metric marts without storing the large fact.

The normal platform sync keeps a user-repo-action fact table. That is useful for
BI drill-downs, but a 2025-09..2026-05 backfill can exceed BigQuery sandbox free
storage. This script writes only small aggregate mart tables.
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from google.cloud import bigquery


DEFAULT_PROJECT = "bda-coai"
DEFAULT_DATASET = "mart"
AGENT_SEED_REPOS = {
    1103012935: "openclaw/openclaw",
    1108837393: "code-yeongyu/oh-my-openagent",
}
AI_KEYWORDS = (
    "agent",
    "agents",
    "ai-agent",
    "ai-agents",
    "coding-agent",
    "claude",
    "claude-code",
    "codex",
    "cursor",
    "gemini",
    "llm",
    "mcp",
    "opencode",
    "openagent",
    "openclaw",
)
EXCLUDE_AGENT_TREND_KEYWORDS = (
    "stock",
    "trading",
    "finance",
    "financial",
    "crypto",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--location", default="US")
    parser.add_argument("--key-path", default=os.environ.get("GCP_KEY_PATH"))
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--metadata-source", choices=["bigquery", "sqlite"], default="bigquery")
    parser.add_argument("--metadata-table", default="repo_metadata")
    parser.add_argument("--metadata-db", type=Path, default=Path("data/repo_metadata.db"))
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def make_client(project: str, key_path: str | None) -> bigquery.Client:
    if key_path:
        return bigquery.Client.from_service_account_json(key_path, project=project)
    return bigquery.Client(project=project)


def ensure_dataset(client: bigquery.Client, project: str, dataset: str, location: str) -> None:
    dataset_id = f"{project}.{dataset}"
    ds = bigquery.Dataset(dataset_id)
    ds.location = location
    client.create_dataset(ds, exists_ok=True)
    ds = client.get_dataset(dataset_id)
    ds.default_table_expiration_ms = None
    ds.default_partition_expiration_ms = None
    try:
        client.update_dataset(
            ds,
            ["default_table_expiration_ms", "default_partition_expiration_ms"],
        )
    except Exception as exc:
        if "Billing has not been enabled" not in str(exc):
            raise
        ds.default_table_expiration_ms = 58 * 24 * 60 * 60 * 1000
        ds.default_partition_expiration_ms = 58 * 24 * 60 * 60 * 1000
        client.update_dataset(
            ds,
            ["default_table_expiration_ms", "default_partition_expiration_ms"],
        )


def iter_dates(start: date, end: date) -> list[date]:
    days = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def load_dataframe_table(client: bigquery.Client, table_id: str, df: pd.DataFrame) -> None:
    temp_table_id = f"{table_id}__load_{uuid.uuid4().hex}"
    table_name = table_id.rsplit(".", 1)[-1]
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = client.load_table_from_dataframe(
        df,
        temp_table_id,
        job_config=job_config,
        job_id_prefix=f"{table_name}_load_{uuid.uuid4().hex}_",
    )
    load_job.result()
    try:
        client.delete_table(table_id, not_found_ok=True)
        copy_config = bigquery.CopyJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
        )
        copy_job = client.copy_table(
            temp_table_id,
            table_id,
            job_config=copy_config,
            job_id_prefix=f"{table_name}_copy_{uuid.uuid4().hex}_",
        )
        copy_job.result()
    finally:
        client.delete_table(temp_table_id, not_found_ok=True)


EMPTY_TABLE_SCHEMAS = {
    "metrics_agent_trend_validation": [
        bigquery.SchemaField("model", "STRING"),
        bigquery.SchemaField("candidates", "INTEGER"),
        bigquery.SchemaField("spearman_next_score", "FLOAT"),
        bigquery.SchemaField("precision_at_20_next_top100", "FLOAT"),
        bigquery.SchemaField("ndcg_at_20", "FLOAT"),
        bigquery.SchemaField("avg_next_score_at_20", "FLOAT"),
    ],
}


def replace_empty_table(
    client: bigquery.Client,
    table_id: str,
    schema: list[bigquery.SchemaField],
) -> None:
    client.delete_table(table_id, not_found_ok=True)
    client.create_table(bigquery.Table(table_id, schema=schema))


def daily_query(day: date) -> str:
    suffix = day.strftime("%Y%m%d")
    iso = day.isoformat()
    return f"""
    WITH fact AS (
      SELECT
        CAST(actor.id AS INT64) AS user_id,
        CAST(repo.id AS INT64) AS repo_id,
        CAST(type AS STRING) AS action,
        COUNT(*) AS event_count
      FROM `githubarchive.day.{suffix}`
      WHERE actor.id IS NOT NULL
        AND repo.id IS NOT NULL
        AND type IS NOT NULL
      GROUP BY user_id, repo_id, action
    )
    SELECT
      DATE '{iso}' AS activity_date,
      COUNT(DISTINCT user_id) AS active_users,
      COUNT(DISTINCT repo_id) AS active_repos,
      SUM(event_count) AS total_events,
      COUNT(*) AS user_repo_action_rows,
      SAFE_DIVIDE(SUM(event_count), COUNT(DISTINCT user_id)) AS events_per_active_user,
      SUM(IF(action = 'PushEvent', event_count, 0)) AS push_events,
      SUM(IF(action = 'WatchEvent', event_count, 0)) AS watch_events,
      SUM(IF(action = 'ForkEvent', event_count, 0)) AS fork_events,
      SUM(IF(action = 'PullRequestEvent', event_count, 0)) AS pull_request_events,
      SUM(IF(action = 'IssuesEvent', event_count, 0)) AS issue_events,
      SUM(IF(action = 'IssueCommentEvent', event_count, 0)) AS issue_comment_events
    FROM fact
    """


def event_daily_query(day: date) -> str:
    suffix = day.strftime("%Y%m%d")
    iso = day.isoformat()
    return f"""
    WITH fact AS (
      SELECT
        CAST(actor.id AS INT64) AS user_id,
        CAST(repo.id AS INT64) AS repo_id,
        CAST(type AS STRING) AS action,
        COUNT(*) AS event_count
      FROM `githubarchive.day.{suffix}`
      WHERE actor.id IS NOT NULL
        AND repo.id IS NOT NULL
        AND type IS NOT NULL
      GROUP BY user_id, repo_id, action
    )
    SELECT
      DATE '{iso}' AS activity_date,
      action,
      COUNT(DISTINCT user_id) AS active_users,
      COUNT(DISTINCT repo_id) AS active_repos,
      SUM(event_count) AS total_events,
      COUNT(*) AS user_repo_action_rows
    FROM fact
    GROUP BY action
    """


def wildcard_table(start: date, end: date) -> tuple[str, list[bigquery.ScalarQueryParameter]]:
    return (
        "`githubarchive.day.202*`",
        [
            bigquery.ScalarQueryParameter("start_suffix", "STRING", start.strftime("%Y%m%d")[3:]),
            bigquery.ScalarQueryParameter("end_suffix", "STRING", end.strftime("%Y%m%d")[3:]),
        ],
    )


def query_df(
    client: bigquery.Client,
    sql: str,
    params: list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter] | None = None,
) -> pd.DataFrame:
    job_config = bigquery.QueryJobConfig(query_parameters=params or [])
    return client.query(sql, job_config=job_config).to_dataframe()


def build_daily_tables(
    client: bigquery.Client,
    start: date,
    end: date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    source, params = wildcard_table(start, end)
    daily_sql = f"""
    SELECT
      PARSE_DATE('%Y%m%d', CONCAT('202', _TABLE_SUFFIX)) AS activity_date,
      COUNT(DISTINCT CAST(actor.id AS INT64)) AS active_users,
      COUNT(DISTINCT CAST(repo.id AS INT64)) AS active_repos,
      COUNT(*) AS total_events,
      COUNT(DISTINCT CONCAT(CAST(actor.id AS STRING), '|', CAST(repo.id AS STRING), '|', type))
        AS user_repo_action_rows,
      SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT CAST(actor.id AS INT64))) AS events_per_active_user,
      SUM(IF(type = 'PushEvent', 1, 0)) AS push_events,
      SUM(IF(type = 'WatchEvent', 1, 0)) AS watch_events,
      SUM(IF(type = 'ForkEvent', 1, 0)) AS fork_events,
      SUM(IF(type = 'PullRequestEvent', 1, 0)) AS pull_request_events,
      SUM(IF(type = 'IssuesEvent', 1, 0)) AS issue_events,
      SUM(IF(type = 'IssueCommentEvent', 1, 0)) AS issue_comment_events
    FROM {source}
    WHERE _TABLE_SUFFIX BETWEEN @start_suffix AND @end_suffix
      AND actor.id IS NOT NULL
      AND repo.id IS NOT NULL
      AND type IS NOT NULL
    GROUP BY activity_date
    ORDER BY activity_date
    """
    event_sql = f"""
    SELECT
      PARSE_DATE('%Y%m%d', CONCAT('202', _TABLE_SUFFIX)) AS activity_date,
      CAST(type AS STRING) AS action,
      COUNT(DISTINCT CAST(actor.id AS INT64)) AS active_users,
      COUNT(DISTINCT CAST(repo.id AS INT64)) AS active_repos,
      COUNT(*) AS total_events,
      COUNT(DISTINCT CONCAT(CAST(actor.id AS STRING), '|', CAST(repo.id AS STRING), '|', type))
        AS user_repo_action_rows
    FROM {source}
    WHERE _TABLE_SUFFIX BETWEEN @start_suffix AND @end_suffix
      AND actor.id IS NOT NULL
      AND repo.id IS NOT NULL
      AND type IS NOT NULL
    GROUP BY activity_date, action
    ORDER BY activity_date, action
    """
    daily = query_df(client, daily_sql, params)
    event_daily = query_df(client, event_sql, params)
    print(
        f"BUILT daily rows={len(daily):,} event_type_rows={len(event_daily):,} "
        f"range={start}..{end}",
        flush=True,
    )
    return daily, event_daily


def build_weekly_table(client: bigquery.Client, start: date, end: date) -> pd.DataFrame:
    source, params = wildcard_table(start, end)
    sql = f"""
    SELECT
      DATE_TRUNC(PARSE_DATE('%Y%m%d', CONCAT('202', _TABLE_SUFFIX)), WEEK(MONDAY)) AS week_start,
      COUNT(DISTINCT CAST(actor.id AS INT64)) AS weekly_active_users,
      COUNT(DISTINCT CAST(repo.id AS INT64)) AS weekly_active_repos,
      COUNT(*) AS total_events,
      SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT CAST(actor.id AS INT64))) AS events_per_active_user
    FROM {source}
    WHERE _TABLE_SUFFIX BETWEEN @start_suffix AND @end_suffix
      AND actor.id IS NOT NULL
      AND repo.id IS NOT NULL
      AND type IS NOT NULL
    GROUP BY week_start
    ORDER BY week_start
    """
    return query_df(client, sql, params)


def build_user_segments(client: bigquery.Client, start: date, end: date) -> pd.DataFrame:
    source, params = wildcard_table(start, end)
    sql = f"""
    WITH user_period AS (
      SELECT
        CAST(actor.id AS INT64) AS user_id,
        COUNT(DISTINCT PARSE_DATE('%Y%m%d', CONCAT('202', _TABLE_SUFFIX))) AS active_days,
        COUNT(*) AS total_events,
        COUNT(DISTINCT CAST(repo.id AS INT64)) AS active_repos
      FROM {source}
      WHERE _TABLE_SUFFIX BETWEEN @start_suffix AND @end_suffix
        AND actor.id IS NOT NULL
        AND repo.id IS NOT NULL
        AND type IS NOT NULL
      GROUP BY user_id
    )
    SELECT
      CASE
        WHEN active_days = 1 THEN 'one_day'
        WHEN active_days BETWEEN 2 AND 4 THEN 'repeat_2d_4d'
        WHEN active_days BETWEEN 5 AND 14 THEN 'regular_5d_14d'
        ELSE 'power_15d_plus'
      END AS user_segment,
      COUNT(*) AS users,
      SUM(total_events) AS total_events,
      AVG(active_days) AS avg_active_days,
      AVG(active_repos) AS avg_active_repos,
      AVG(total_events) AS avg_total_events
    FROM user_period
    GROUP BY user_segment
    ORDER BY user_segment
    """
    return query_df(client, sql, params)


def build_retention(client: bigquery.Client, start: date, end: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    source, params = wildcard_table(start, end)
    weekly_sql = f"""
    WITH user_week AS (
      SELECT DISTINCT
        CAST(actor.id AS INT64) AS user_id,
        DATE_TRUNC(PARSE_DATE('%Y%m%d', CONCAT('202', _TABLE_SUFFIX)), WEEK(MONDAY)) AS week_start
      FROM {source}
      WHERE _TABLE_SUFFIX BETWEEN @start_suffix AND @end_suffix
        AND actor.id IS NOT NULL
        AND repo.id IS NOT NULL
        AND type IS NOT NULL
    ),
    first_week AS (
      SELECT user_id, MIN(week_start) AS cohort_week
      FROM user_week
      GROUP BY user_id
    ),
    cohort_activity AS (
      SELECT
        uw.user_id,
        fw.cohort_week,
        uw.week_start,
        DATE_DIFF(uw.week_start, fw.cohort_week, WEEK) AS weeks_since
      FROM user_week uw
      JOIN first_week fw USING (user_id)
    ),
    cohort_sizes AS (
      SELECT cohort_week, COUNT(*) AS cohort_users
      FROM first_week
      GROUP BY cohort_week
    )
    SELECT
      ca.cohort_week,
      ca.week_start,
      ca.weeks_since,
      cs.cohort_users,
      COUNT(DISTINCT ca.user_id) AS active_users,
      SAFE_DIVIDE(COUNT(DISTINCT ca.user_id), cs.cohort_users) AS retention_rate
    FROM cohort_activity ca
    JOIN cohort_sizes cs USING (cohort_week)
    GROUP BY ca.cohort_week, ca.week_start, ca.weeks_since, cs.cohort_users
    ORDER BY ca.cohort_week, ca.weeks_since
    """
    retention_weekly = query_df(client, weekly_sql, params)
    if retention_weekly.empty:
        return retention_weekly, pd.DataFrame()
    summary = (
        retention_weekly.pivot_table(
            index=["cohort_week", "cohort_users"],
            columns="weeks_since",
            values="retention_rate",
            fill_value=0.0,
        )
        .reset_index()
    )
    for week_num in range(4):
        if week_num not in summary.columns:
            summary[week_num] = 0.0
    summary = summary[["cohort_week", "cohort_users", 0, 1, 2, 3]].rename(
        columns={0: "w0_retention", 1: "w1_retention", 2: "w2_retention", 3: "w3_retention"}
    )
    return retention_weekly, summary


def load_repo_metadata(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame(
            columns=["repo_id", "repo_name", "description", "topics", "stargazers", "forks"]
        )
    conn = sqlite3.connect(str(db_path))
    try:
        return pd.read_sql_query(
            "SELECT repo_id, repo_name, description, topics, stargazers, forks FROM repo_metadata",
            conn,
        )
    finally:
        conn.close()


def load_repo_metadata_bq(
    client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
) -> pd.DataFrame:
    table_id = f"{project}.{dataset}.{table}"
    try:
        return client.query(
            f"""
            SELECT repo_id, repo_name, description, topics, stargazers, forks
            FROM `{table_id}`
            """
        ).to_dataframe()
    except Exception as exc:
        message = str(exc)
        if "Not found" in message or "NotFound" in message:
            return pd.DataFrame(
                columns=["repo_id", "repo_name", "description", "topics", "stargazers", "forks"]
            )
        raise


def metadata_keyword_mask(metadata: pd.DataFrame) -> pd.Series:
    if metadata.empty:
        return pd.Series(dtype=bool)
    text = (
        metadata["repo_name"].fillna("")
        + " "
        + metadata["description"].fillna("")
        + " "
        + metadata["topics"].fillna("")
    ).str.lower()
    pattern = (
        r"(?:^|[^a-z0-9])(?:"
        + "|".join(re.escape(keyword) for keyword in AI_KEYWORDS)
        + r")(?:[^a-z0-9]|$)"
    )
    exclude_pattern = (
        r"(?:^|[^a-z0-9])(?:"
        + "|".join(re.escape(keyword) for keyword in EXCLUDE_AGENT_TREND_KEYWORDS)
        + r")(?:[^a-z0-9]|$)"
    )
    return text.str.contains(pattern, regex=True) & ~text.str.contains(exclude_pattern, regex=True)


def add_repo_metadata(df: pd.DataFrame, metadata: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if metadata.empty:
        out["repo_name"] = out["repo_id"].astype(str)
        out["description"] = ""
        out["topics"] = ""
        out["stargazers"] = np.nan
        out["forks"] = np.nan
        return out
    out = out.merge(
        metadata[["repo_id", "repo_name", "description", "topics", "stargazers", "forks"]],
        on="repo_id",
        how="left",
    )
    for repo_id, repo_name in AGENT_SEED_REPOS.items():
        mask = out["repo_id"] == repo_id
        out.loc[mask & out["repo_name"].isna(), "repo_name"] = repo_name
    out["repo_name"] = out["repo_name"].fillna(out["repo_id"].astype(str))
    out["description"] = out["description"].fillna("")
    out["topics"] = out["topics"].fillna("")
    return out


def build_agent_trendy(
    client: bigquery.Client,
    start: date,
    end: date,
    metadata: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if metadata.empty:
        candidate_ids = set(AGENT_SEED_REPOS)
    else:
        candidate_ids = (
            set(metadata.loc[metadata_keyword_mask(metadata), "repo_id"].dropna().astype("int64"))
            | set(AGENT_SEED_REPOS)
        )
    if not candidate_ids:
        return pd.DataFrame(), pd.DataFrame()

    recent_start = end - timedelta(days=6)
    baseline_start = recent_start - timedelta(days=28)
    source, params = wildcard_table(baseline_start, end)
    params = [
        *params,
        bigquery.ArrayQueryParameter("repo_ids", "INT64", sorted(candidate_ids)),
        bigquery.ArrayQueryParameter("seed_repo_ids", "INT64", sorted(AGENT_SEED_REPOS)),
        bigquery.ScalarQueryParameter("recent_start", "DATE", recent_start),
        bigquery.ScalarQueryParameter("baseline_start", "DATE", baseline_start),
    ]
    sql = f"""
    WITH events AS (
      SELECT
        CAST(actor.id AS INT64) AS user_id,
        CAST(repo.id AS INT64) AS repo_id,
        CAST(type AS STRING) AS action,
        PARSE_DATE('%Y%m%d', CONCAT('202', _TABLE_SUFFIX)) AS activity_date,
        CASE
          WHEN type = 'WatchEvent' THEN 3.0
          WHEN type = 'ForkEvent' THEN 2.0
          WHEN type = 'PullRequestEvent' THEN 1.5
          WHEN type = 'IssuesEvent' THEN 1.0
          WHEN type = 'IssueCommentEvent' THEN 0.5
          WHEN type = 'PushEvent' THEN 0.2
          ELSE 0.1
        END AS weight
      FROM {source}
      WHERE _TABLE_SUFFIX BETWEEN @start_suffix AND @end_suffix
        AND CAST(repo.id AS INT64) IN UNNEST(@repo_ids)
        AND actor.id IS NOT NULL
        AND repo.id IS NOT NULL
        AND type IS NOT NULL
    ),
    scored AS (
      SELECT
        repo_id,
        action,
        IF(activity_date >= @recent_start, 'recent', 'baseline') AS window_name,
        COUNT(DISTINCT user_id) AS active_users,
        COUNT(*) AS events,
        SUM(weight) AS score
      FROM events
      GROUP BY repo_id, action, window_name
    ),
    repo_window AS (
      SELECT
        repo_id,
        SUM(IF(window_name = 'recent', score, 0)) AS recent_score,
        SUM(IF(window_name = 'baseline', score, 0)) AS baseline_score,
        SUM(IF(window_name = 'recent', active_users, 0)) AS recent_active_users_action_sum,
        SUM(IF(window_name = 'baseline', active_users, 0)) AS baseline_active_users_action_sum,
        SUM(IF(window_name = 'recent', events, 0)) AS recent_events,
        SUM(IF(window_name = 'baseline', events, 0)) AS baseline_events
      FROM scored
      GROUP BY repo_id
    ),
    repo_users AS (
      SELECT
        repo_id,
        COUNT(DISTINCT IF(activity_date >= @recent_start, user_id, NULL)) AS recent_active_users,
        COUNT(DISTINCT IF(activity_date < @recent_start, user_id, NULL)) AS baseline_active_users
      FROM events
      GROUP BY repo_id
    ),
    top_action AS (
      SELECT
        repo_id,
        ARRAY_AGG(IF(window_name = 'recent', action, NULL) IGNORE NULLS ORDER BY IF(window_name = 'recent', score, -1) DESC LIMIT 1)[SAFE_OFFSET(0)] AS recent_top_action,
        ARRAY_AGG(IF(window_name = 'baseline', action, NULL) IGNORE NULLS ORDER BY IF(window_name = 'baseline', score, -1) DESC LIMIT 1)[SAFE_OFFSET(0)] AS baseline_top_action
      FROM scored
      GROUP BY repo_id
    ),
    seed_users AS (
      SELECT DISTINCT user_id
      FROM events
      WHERE activity_date >= @recent_start
        AND repo_id IN UNNEST(@seed_repo_ids)
    ),
    affinity AS (
      SELECT e.repo_id, COUNT(DISTINCT e.user_id) AS recent_seed_users
      FROM events e
      JOIN seed_users s USING (user_id)
      WHERE e.activity_date >= @recent_start
      GROUP BY e.repo_id
    )
    SELECT
      rw.repo_id,
      rw.recent_score,
      rw.baseline_score,
      ru.recent_active_users,
      ru.baseline_active_users,
      rw.recent_events,
      rw.baseline_events,
      ta.recent_top_action,
      ta.baseline_top_action,
      IFNULL(a.recent_seed_users, 0) AS recent_seed_users
    FROM repo_window rw
    JOIN repo_users ru USING (repo_id)
    LEFT JOIN top_action ta USING (repo_id)
    LEFT JOIN affinity a USING (repo_id)
    WHERE ru.recent_active_users >= 10
      AND rw.recent_score >= 20
    """
    out = query_df(client, sql, params)
    if out.empty:
        return out, pd.DataFrame()
    out["recent_avg_score"] = out["recent_score"] / 7.0
    out["baseline_avg_score"] = out["baseline_score"] / 28.0
    out["growth_ratio"] = (out["recent_avg_score"] + 1.0) / (out["baseline_avg_score"] + 1.0)
    out["seed_affinity"] = out["recent_seed_users"] / out["recent_active_users"].clip(lower=1)
    out = out[(out["seed_affinity"] >= 0.005) | (out["repo_id"].isin(AGENT_SEED_REPOS))].copy()
    out["trend_score"] = (
        np.log1p(out["recent_active_users"])
        * np.log1p(out["recent_score"])
        * out["growth_ratio"].clip(upper=20.0)
        * (1.0 + out["seed_affinity"].clip(upper=1.0))
    )
    out = add_repo_metadata(out, metadata)
    out["seed_repo"] = out["repo_id"].map(AGENT_SEED_REPOS).fillna("")
    out["why_trendy"] = (
        "growth="
        + out["growth_ratio"].round(2).astype(str)
        + ", recent_users="
        + out["recent_active_users"].astype("int64").astype(str)
        + ", seed_affinity="
        + (out["seed_affinity"] * 100).round(1).astype(str)
        + "%, top_action="
        + out["recent_top_action"].fillna("")
    )
    cols = [
        "repo_id",
        "repo_name",
        "description",
        "topics",
        "seed_repo",
        "trend_score",
        "growth_ratio",
        "seed_affinity",
        "recent_seed_users",
        "recent_active_users",
        "baseline_active_users",
        "recent_score",
        "baseline_score",
        "recent_events",
        "baseline_events",
        "recent_top_action",
        "baseline_top_action",
        "stargazers",
        "forks",
        "why_trendy",
    ]
    return out.sort_values("trend_score", ascending=False).head(100)[cols], pd.DataFrame()


def main() -> None:
    args = parse_args()
    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()
    client = make_client(args.project, args.key_path)
    ensure_dataset(client, args.project, args.dataset, args.location)
    days = iter_dates(start, end)
    dataset_id = f"{args.project}.{args.dataset}"
    print(
        f"COMPACT_BACKFILL_PLAN range={start}..{end} days={len(days)} dataset={dataset_id} "
        f"metadata_source={args.metadata_source}",
        flush=True,
    )
    if args.dry_run:
        return

    daily, event_daily = build_daily_tables(client, start, end)
    weekly = build_weekly_table(client, start, end)
    user_segments = build_user_segments(client, start, end)
    retention_weekly, retention_summary = build_retention(client, start, end)
    metadata = (
        load_repo_metadata_bq(client, args.project, args.dataset, args.metadata_table)
        if args.metadata_source == "bigquery"
        else load_repo_metadata(args.metadata_db)
    )
    agent_trendy, agent_validation = build_agent_trendy(client, start, end, metadata)

    tables = {
        "metrics_daily": daily,
        "metrics_event_type_daily": event_daily,
        "metrics_weekly": weekly,
        "metrics_user_segments": user_segments,
        "metrics_retention_weekly": retention_weekly,
        "metrics_retention_summary": retention_summary,
        "metrics_agent_trendy_repos": agent_trendy,
        "metrics_agent_trend_validation": agent_validation,
    }
    for name, df in tables.items():
        table_id = f"{dataset_id}.{name}"
        if df.empty:
            if name in EMPTY_TABLE_SCHEMAS:
                replace_empty_table(client, table_id, EMPTY_TABLE_SCHEMAS[name])
                print(f"LOADED empty {table_id} schema_only=true", flush=True)
                continue
            client.delete_table(table_id, not_found_ok=True)
            print(f"SKIPPED empty {table_id}; deleted stale table if it existed", flush=True)
            continue
        load_dataframe_table(client, table_id, df)
        print(f"LOADED {table_id} rows={len(df):,}", flush=True)

    print(f"COMPACT_BACKFILL_DONE range={start}..{end} days={len(days)}")


if __name__ == "__main__":
    main()
