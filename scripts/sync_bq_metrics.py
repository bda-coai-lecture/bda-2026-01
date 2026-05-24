"""Sync GitHub Archive daily aggregates to BigQuery metric marts.

The script is intentionally boring and idempotent:

1. Plan the selected date range before touching BigQuery.
2. Backfill or refresh a bounded daily aggregate fact table in BigQuery.
3. Rebuild metric tables from the BigQuery fact table or legacy local parquet.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np
import duckdb
from google.cloud import bigquery


DEFAULT_PROJECT = "bda-coai"
DEFAULT_DATASET = "mart"
DEFAULT_FACT_TABLE = "fact_user_repo_activity"
SANDBOX_EXPIRATION_DAYS = 58
DEFAULT_MAX_DAYS = 90
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


@dataclass(frozen=True)
class TableNames:
    project: str
    dataset: str
    fact: str

    @property
    def dataset_id(self) -> str:
        return f"{self.project}.{self.dataset}"

    @property
    def fact_id(self) -> str:
        return f"{self.dataset_id}.{self.fact}"

    def table_id(self, name: str) -> str:
        return f"{self.dataset_id}.{name}"


def parse_day(path: Path) -> date:
    return datetime.strptime(path.stem, "%Y%m%d").date()


def iter_parquet_files(parquet_dir: Path, start: date | None, end: date | None) -> list[Path]:
    files = sorted(parquet_dir.glob("*.parquet"))
    selected = []
    for path in files:
        day = parse_day(path)
        if start and day < start:
            continue
        if end and day > end:
            continue
        selected.append(path)
    return selected


def iter_dates(start: date, end: date) -> list[date]:
    days = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def print_plan(files: list[Path], mode: str, max_days: int) -> None:
    total_bytes = sum(path.stat().st_size for path in files)
    first_day = parse_day(files[0]) if files else None
    last_day = parse_day(files[-1]) if files else None
    print(
        "PLAN "
        f"days={len(files)} range={first_day}..{last_day} "
        f"local_bytes={total_bytes / 1024 ** 3:.2f}GiB mode={mode} max_days={max_days}"
    )


def print_bq_plan(days: list[date], mode: str, max_days: int) -> None:
    first_day = days[0] if days else None
    last_day = days[-1] if days else None
    print(
        "PLAN "
        f"source=bigquery days={len(days)} range={first_day}..{last_day} "
        f"mode={mode} max_days={max_days}"
    )


def make_client(project: str, key_path: str | None) -> bigquery.Client:
    if key_path:
        return bigquery.Client.from_service_account_json(key_path, project=project)
    return bigquery.Client(project=project)


def bq_expiration(client: bigquery.Client) -> datetime:
    row = next(iter(client.query("SELECT CURRENT_TIMESTAMP() AS now_ts").result()))
    return row.now_ts + timedelta(days=SANDBOX_EXPIRATION_DAYS)


def ensure_dataset(client: bigquery.Client, names: TableNames, location: str) -> None:
    dataset = bigquery.Dataset(names.dataset_id)
    dataset.location = location
    dataset = client.create_dataset(dataset, exists_ok=True)
    dataset.default_table_expiration_ms = None
    dataset.default_partition_expiration_ms = None
    try:
        client.update_dataset(
            dataset,
            ["default_table_expiration_ms", "default_partition_expiration_ms"],
        )
    except Exception as exc:
        if "Billing has not been enabled" not in str(exc):
            raise
        expiration_ms = SANDBOX_EXPIRATION_DAYS * 24 * 60 * 60 * 1000
        dataset.default_table_expiration_ms = expiration_ms
        dataset.default_partition_expiration_ms = expiration_ms
        client.update_dataset(
            dataset,
            ["default_table_expiration_ms", "default_partition_expiration_ms"],
        )


def ensure_fact_table(client: bigquery.Client, names: TableNames) -> None:
    schema = [
        bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("repo_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("action", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("activity_date", "DATE", mode="REQUIRED"),
    ]
    table = bigquery.Table(names.fact_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field="activity_date")
    table.clustering_fields = ["action", "repo_id"]
    client.create_table(table, exists_ok=True)
    table = client.get_table(names.fact_id)
    table.expires = None
    try:
        client.update_table(table, ["expires"])
    except Exception as exc:
        if "Billing has not been enabled" not in str(exc):
            raise
        table.expires = bq_expiration(client)
        client.update_table(table, ["expires"])


def partition_exists(client: bigquery.Client, names: TableNames, activity_date: date) -> bool:
    sql = f"""
    SELECT 1
    FROM `{names.fact_id}`
    WHERE activity_date = @activity_date
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("activity_date", "DATE", activity_date)]
    )
    return next(iter(client.query(sql, job_config=job_config).result()), None) is not None


def load_one_day(
    client: bigquery.Client,
    names: TableNames,
    path: Path,
    mode: str,
    write_disposition: str | None = None,
) -> int:
    activity_date = parse_day(path)
    if mode == "skip-existing" and partition_exists(client, names, activity_date):
        print(f"SKIP {activity_date} already exists")
        return 0

    df = pd.read_parquet(path)
    df = df.rename(
        columns={
            "actor_id": "user_id",
            "repo_id": "repo_id",
            "type": "action",
            "cnt": "event_count",
        }
    )
    df["activity_date"] = activity_date
    df = df.dropna(subset=["user_id", "repo_id", "action", "event_count"])
    df = df.astype(
        {
            "user_id": "int64",
            "repo_id": "int64",
            "action": "string",
            "event_count": "int64",
        }
    )

    if mode == "replace-days":
        delete_sql = f"DELETE FROM `{names.fact_id}` WHERE activity_date = @activity_date"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("activity_date", "DATE", activity_date)
            ]
        )
        client.query(delete_sql, job_config=job_config).result()

    load_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("user_id", "INTEGER"),
            bigquery.SchemaField("repo_id", "INTEGER"),
            bigquery.SchemaField("action", "STRING"),
            bigquery.SchemaField("event_count", "INTEGER"),
            bigquery.SchemaField("activity_date", "DATE"),
        ],
        write_disposition=write_disposition or bigquery.WriteDisposition.WRITE_APPEND,
    )
    client.load_table_from_dataframe(df, names.fact_id, job_config=load_config).result()
    print(f"LOADED {activity_date} rows={len(df):,}")
    return len(df)


def load_one_day_from_public_bigquery(
    client: bigquery.Client,
    names: TableNames,
    activity_date: date,
    mode: str,
) -> int:
    if mode == "skip-existing" and partition_exists(client, names, activity_date):
        print(f"SKIP {activity_date} already exists")
        return 0

    if mode == "replace-days":
        delete_sql = f"DELETE FROM `{names.fact_id}` WHERE activity_date = @activity_date"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("activity_date", "DATE", activity_date)
            ]
        )
        client.query(delete_sql, job_config=job_config).result()

    date_str = activity_date.strftime("%Y%m%d")
    query = f"""
    SELECT
      CAST(actor.id AS INT64) AS user_id,
      CAST(repo.id AS INT64) AS repo_id,
      CAST(type AS STRING) AS action,
      COUNT(*) AS event_count,
      DATE '{activity_date.isoformat()}' AS activity_date
    FROM `githubarchive.day.{date_str}`
    WHERE actor.id IS NOT NULL
      AND repo.id IS NOT NULL
      AND type IS NOT NULL
    GROUP BY user_id, repo_id, action, activity_date
    """
    before_rows = client.get_table(names.fact_id).num_rows
    job_config = bigquery.QueryJobConfig(
        destination=names.fact_id,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.query(query, job_config=job_config)
    print(f"BQ_JOB {activity_date} {job.job_id}", flush=True)
    job.result()
    rows = max(client.get_table(names.fact_id).num_rows - before_rows, 0)
    print(f"LOADED {activity_date} rows={rows:,}")
    return rows


def read_activity_file(path: Path) -> pd.DataFrame:
    activity_date = parse_day(path)
    df = pd.read_parquet(path)
    df = df.rename(
        columns={
            "actor_id": "user_id",
            "repo_id": "repo_id",
            "type": "action",
            "cnt": "event_count",
        }
    )
    df["activity_date"] = pd.Timestamp(activity_date)
    df = df.dropna(subset=["user_id", "repo_id", "action", "event_count"])
    return df.astype(
        {
            "user_id": "int64",
            "repo_id": "int64",
            "action": "string",
            "event_count": "int64",
        }
    )


def load_activity_frames(files: list[Path]) -> pd.DataFrame:
    return pd.concat([read_activity_file(path) for path in files], ignore_index=True)


def load_repo_metadata(db_path: Path = Path("data/repo_metadata.db")) -> pd.DataFrame:
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


def load_repo_metadata_from_bq(client: bigquery.Client, names: TableNames) -> pd.DataFrame:
    table_id = names.table_id("repo_metadata")
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
    pattern = r"(?:^|[^a-z0-9])(?:" + "|".join(re.escape(keyword) for keyword in AI_KEYWORDS) + r")(?:[^a-z0-9]|$)"
    include = text.str.contains(pattern, regex=True)
    exclude_pattern = (
        r"(?:^|[^a-z0-9])(?:"
        + "|".join(re.escape(keyword) for keyword in EXCLUDE_AGENT_TREND_KEYWORDS)
        + r")(?:[^a-z0-9]|$)"
    )
    exclude = text.str.contains(exclude_pattern, regex=True)
    return include & ~exclude


def add_repo_metadata(df: pd.DataFrame, metadata: pd.DataFrame) -> pd.DataFrame:
    if metadata.empty:
        out = df.copy()
        out["repo_name"] = out["repo_id"].astype(str)
        out["description"] = ""
        out["topics"] = ""
        return out
    cols = ["repo_id", "repo_name", "description", "topics", "stargazers", "forks"]
    out = df.merge(metadata[cols], on="repo_id", how="left")
    for repo_id, repo_name in AGENT_SEED_REPOS.items():
        mask = out["repo_id"] == repo_id
        out.loc[mask & out["repo_name"].isna(), "repo_name"] = repo_name
    out["repo_name"] = out["repo_name"].fillna(out["repo_id"].astype(str))
    out["description"] = out["description"].fillna("")
    out["topics"] = out["topics"].fillna("")
    return out


def weighted_activity(df: pd.DataFrame) -> pd.DataFrame:
    weights = {
        "WatchEvent": 3.0,
        "ForkEvent": 2.0,
        "PullRequestEvent": 1.5,
        "IssuesEvent": 1.0,
        "IssueCommentEvent": 0.5,
        "PushEvent": 0.2,
    }
    out = df.copy()
    out["weighted_score"] = out["action"].map(weights).fillna(0.1) * out["event_count"]
    return out


def repo_window_stats(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "repo_id",
                f"{prefix}_score",
                f"{prefix}_active_users",
                f"{prefix}_events",
                f"{prefix}_top_action",
            ]
        )
    base = (
        df.groupby("repo_id")
        .agg(
            **{
                f"{prefix}_score": ("weighted_score", "sum"),
                f"{prefix}_active_users": ("user_id", "nunique"),
                f"{prefix}_events": ("event_count", "sum"),
            }
        )
        .reset_index()
    )
    action = (
        df.groupby(["repo_id", "action"])["weighted_score"]
        .sum()
        .reset_index()
        .sort_values(["repo_id", "weighted_score"], ascending=[True, False])
        .drop_duplicates("repo_id")
        .rename(columns={"action": f"{prefix}_top_action"})
        [["repo_id", f"{prefix}_top_action"]]
    )
    return base.merge(action, on="repo_id", how="left")


def seed_affinity_stats(df: pd.DataFrame, seed_repo_ids: set[int], prefix: str) -> pd.DataFrame:
    seed_users = set(df.loc[df["repo_id"].isin(seed_repo_ids), "user_id"].unique())
    if not seed_users:
        return pd.DataFrame(columns=["repo_id", f"{prefix}_seed_users"])
    seed_df = df[df["user_id"].isin(seed_users)]
    return (
        seed_df.groupby("repo_id")["user_id"]
        .nunique()
        .rename(f"{prefix}_seed_users")
        .reset_index()
    )


def make_agent_trendy_table(
    df: pd.DataFrame,
    metadata: pd.DataFrame,
    baseline_days: int,
    recent_days: int,
    top_n: int = 100,
) -> pd.DataFrame:
    max_day = df["activity_date"].max()
    recent_start = max_day - pd.Timedelta(days=recent_days - 1)
    baseline_start = recent_start - pd.Timedelta(days=baseline_days)
    baseline = df[(df["activity_date"] >= baseline_start) & (df["activity_date"] < recent_start)]
    recent = df[df["activity_date"] >= recent_start]

    seed_repo_ids = set(AGENT_SEED_REPOS)
    ai_repo_ids = set(metadata.loc[metadata_keyword_mask(metadata), "repo_id"].astype("int64"))
    candidate_ids = ai_repo_ids | seed_repo_ids
    baseline = baseline[baseline["repo_id"].isin(candidate_ids)]
    recent = recent[recent["repo_id"].isin(candidate_ids)]

    recent_stats = repo_window_stats(recent, "recent")
    baseline_stats = repo_window_stats(baseline, "baseline")
    affinity = seed_affinity_stats(recent, seed_repo_ids, "recent")

    out = recent_stats.merge(baseline_stats, on="repo_id", how="left").merge(
        affinity, on="repo_id", how="left"
    )
    fill_cols = [
        "baseline_score",
        "baseline_active_users",
        "baseline_events",
        "recent_seed_users",
    ]
    for col in fill_cols:
        out[col] = out[col].fillna(0)
    out["recent_avg_score"] = out["recent_score"] / recent_days
    out["baseline_avg_score"] = out["baseline_score"] / baseline_days
    out["growth_ratio"] = (out["recent_avg_score"] + 1.0) / (out["baseline_avg_score"] + 1.0)
    out["seed_affinity"] = out["recent_seed_users"] / out["recent_active_users"].clip(lower=1)

    out = out[
        (out["recent_active_users"] >= 10)
        & (out["recent_score"] >= 20)
        & ((out["seed_affinity"] >= 0.005) | (out["repo_id"].isin(seed_repo_ids)))
    ].copy()
    out["trend_score"] = (
        pd.Series(np.log1p(out["recent_active_users"]), index=out.index)
        * pd.Series(np.log1p(out["recent_score"]), index=out.index)
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
    return out.sort_values("trend_score", ascending=False).head(top_n)[cols].reset_index(drop=True)


def dcg_at_k(scores: list[float], k: int) -> float:
    import math

    return sum(math.log1p(max(float(score), 0.0)) / math.log2(i + 2) for i, score in enumerate(scores[:k]))


def evaluate_agent_trend(df: pd.DataFrame, metadata: pd.DataFrame) -> pd.DataFrame:
    min_day = df["activity_date"].min()
    baseline_end = min_day + pd.Timedelta(days=20)
    trend_end = baseline_end + pd.Timedelta(days=7)
    label_end = trend_end + pd.Timedelta(days=7)
    if df["activity_date"].max() < label_end:
        return pd.DataFrame()

    baseline = df[(df["activity_date"] >= min_day) & (df["activity_date"] <= baseline_end)]
    trend = df[(df["activity_date"] > baseline_end) & (df["activity_date"] <= trend_end)]
    label = df[(df["activity_date"] > trend_end) & (df["activity_date"] <= label_end)]

    seed_repo_ids = set(AGENT_SEED_REPOS)
    ai_repo_ids = set(metadata.loc[metadata_keyword_mask(metadata), "repo_id"].astype("int64"))
    candidate_ids = ai_repo_ids | seed_repo_ids
    baseline = baseline[baseline["repo_id"].isin(candidate_ids)]
    trend = trend[trend["repo_id"].isin(candidate_ids)]
    label = label[label["repo_id"].isin(candidate_ids)]

    features = repo_window_stats(trend, "trend").merge(
        repo_window_stats(baseline, "baseline"), on="repo_id", how="left"
    )
    features = features.merge(seed_affinity_stats(trend, seed_repo_ids, "trend"), on="repo_id", how="left")
    labels = repo_window_stats(label, "label")[["repo_id", "label_score", "label_active_users"]]
    out = features.merge(labels, on="repo_id", how="left")
    for col in ["baseline_score", "baseline_active_users", "trend_seed_users", "label_score", "label_active_users"]:
        out[col] = out[col].fillna(0)
    out["growth_ratio"] = ((out["trend_score"] / 7) + 1.0) / ((out["baseline_score"] / 21) + 1.0)
    out["seed_affinity"] = out["trend_seed_users"] / out["trend_active_users"].clip(lower=1)

    out = out[(out["trend_active_users"] >= 5) & (out["trend_score"] >= 10)].copy()
    if out.empty:
        return pd.DataFrame()

    out["agent_trend_score"] = (
        np.log1p(out["trend_active_users"])
        * np.log1p(out["trend_score"])
        * out["growth_ratio"].clip(upper=20.0)
        * (1.0 + out["seed_affinity"].clip(upper=1.0))
    )
    models = {
        "agent_trend_score": out["agent_trend_score"],
        "popularity_recent_score": out["trend_score"],
        "growth_only": out["growth_ratio"],
        "seed_affinity_only": out["seed_affinity"],
        "recent_active_users": out["trend_active_users"],
    }
    true_top = set(out.sort_values("label_score", ascending=False).head(100)["repo_id"])
    ideal_scores = out.sort_values("label_score", ascending=False)["label_score"].tolist()
    ideal_dcg = dcg_at_k(ideal_scores, 20) or 1.0
    rows = []
    for model_name, score in models.items():
        temp = out.assign(pred_score=score)
        ranked = temp.sort_values("pred_score", ascending=False)
        top20 = ranked.head(20)
        label_scores = top20["label_score"].tolist()
        rows.append(
            {
                "model": model_name,
                "candidates": len(out),
                "spearman_next_score": temp["pred_score"].rank().corr(temp["label_score"].rank()),
                "precision_at_20_next_top100": top20["repo_id"].isin(true_top).mean(),
                "ndcg_at_20": dcg_at_k(label_scores, 20) / ideal_dcg,
                "avg_next_score_at_20": top20["label_score"].mean(),
                "validation_baseline_start": min_day.date(),
                "validation_trend_start": (baseline_end + pd.Timedelta(days=1)).date(),
                "validation_label_start": (trend_end + pd.Timedelta(days=1)).date(),
            }
        )
    return pd.DataFrame(rows)


def build_local_metrics(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    daily_base = (
        df.groupby("activity_date")
        .agg(
            active_users=("user_id", "nunique"),
            active_repos=("repo_id", "nunique"),
            total_events=("event_count", "sum"),
            user_repo_action_rows=("event_count", "size"),
        )
        .reset_index()
    )
    daily_base["events_per_active_user"] = (
        daily_base["total_events"] / daily_base["active_users"]
    )

    action_pivot = (
        df.pivot_table(
            index="activity_date",
            columns="action",
            values="event_count",
            aggfunc="sum",
            fill_value=0,
        )
        .rename(
            columns={
                "PushEvent": "push_events",
                "WatchEvent": "watch_events",
                "ForkEvent": "fork_events",
                "PullRequestEvent": "pull_request_events",
                "IssuesEvent": "issue_events",
                "IssueCommentEvent": "issue_comment_events",
            }
        )
        .reset_index()
    )
    keep_action_cols = [
        "activity_date",
        "push_events",
        "watch_events",
        "fork_events",
        "pull_request_events",
        "issue_events",
        "issue_comment_events",
    ]
    for col in keep_action_cols:
        if col not in action_pivot.columns:
            action_pivot[col] = 0
    metrics_daily = daily_base.merge(action_pivot[keep_action_cols], on="activity_date")
    metrics_daily["activity_date"] = metrics_daily["activity_date"].dt.date

    metrics_event_type_daily = (
        df.groupby(["activity_date", "action"])
        .agg(
            active_users=("user_id", "nunique"),
            active_repos=("repo_id", "nunique"),
            total_events=("event_count", "sum"),
            user_repo_action_rows=("event_count", "size"),
        )
        .reset_index()
    )
    metrics_event_type_daily["activity_date"] = metrics_event_type_daily[
        "activity_date"
    ].dt.date

    weekly_df = df.copy()
    weekly_df["week_start"] = (
        weekly_df["activity_date"] - pd.to_timedelta(weekly_df["activity_date"].dt.weekday, unit="D")
    )
    metrics_weekly = (
        weekly_df.groupby("week_start")
        .agg(
            weekly_active_users=("user_id", "nunique"),
            weekly_active_repos=("repo_id", "nunique"),
            total_events=("event_count", "sum"),
        )
        .reset_index()
    )
    metrics_weekly["events_per_active_user"] = (
        metrics_weekly["total_events"] / metrics_weekly["weekly_active_users"]
    )
    metrics_weekly["week_start"] = metrics_weekly["week_start"].dt.date

    user_period = (
        df.groupby("user_id")
        .agg(
            active_days=("activity_date", "nunique"),
            total_events=("event_count", "sum"),
            active_repos=("repo_id", "nunique"),
        )
        .reset_index()
    )
    user_period["user_segment"] = pd.cut(
        user_period["active_days"],
        bins=[0, 1, 4, 14, float("inf")],
        labels=["one_day", "repeat_2d_4d", "regular_5d_14d", "power_15d_plus"],
        right=True,
    ).astype("string")
    metrics_user_segments = (
        user_period.groupby("user_segment", observed=True)
        .agg(
            users=("user_id", "size"),
            total_events=("total_events", "sum"),
            avg_active_days=("active_days", "mean"),
            avg_active_repos=("active_repos", "mean"),
            avg_total_events=("total_events", "mean"),
        )
        .reset_index()
    )

    user_week = weekly_df[["user_id", "week_start"]].drop_duplicates()
    first_week = user_week.groupby("user_id", as_index=False)["week_start"].min()
    first_week = first_week.rename(columns={"week_start": "cohort_week"})
    cohort_activity = user_week.merge(first_week, on="user_id")
    cohort_activity["weeks_since"] = (
        (cohort_activity["week_start"] - cohort_activity["cohort_week"]).dt.days // 7
    ).astype("int64")

    cohort_sizes = (
        first_week.groupby("cohort_week")
        .size()
        .rename("cohort_users")
        .reset_index()
    )
    retention_counts = (
        cohort_activity.groupby(["cohort_week", "week_start", "weeks_since"])
        .agg(active_users=("user_id", "nunique"))
        .reset_index()
        .merge(cohort_sizes, on="cohort_week")
    )
    retention_counts["retention_rate"] = (
        retention_counts["active_users"] / retention_counts["cohort_users"]
    )
    metrics_retention_weekly = retention_counts[
        ["cohort_week", "week_start", "weeks_since", "cohort_users", "active_users", "retention_rate"]
    ].copy()
    metrics_retention_weekly["cohort_week"] = metrics_retention_weekly["cohort_week"].dt.date
    metrics_retention_weekly["week_start"] = metrics_retention_weekly["week_start"].dt.date

    retention_summary = metrics_retention_weekly.pivot_table(
        index=["cohort_week", "cohort_users"],
        columns="weeks_since",
        values="retention_rate",
        fill_value=0.0,
    ).reset_index()
    for week_num in range(4):
        if week_num not in retention_summary.columns:
            retention_summary[week_num] = 0.0
    metrics_retention_summary = retention_summary[
        ["cohort_week", "cohort_users", 0, 1, 2, 3]
    ].rename(
        columns={
            0: "w0_retention",
            1: "w1_retention",
            2: "w2_retention",
            3: "w3_retention",
        }
    )

    metadata = load_repo_metadata()
    weighted_df = weighted_activity(df)
    agent_trendy = make_agent_trendy_table(weighted_df, metadata, baseline_days=28, recent_days=7)
    agent_validation = evaluate_agent_trend(weighted_df, metadata)

    return {
        "metrics_daily": metrics_daily,
        "metrics_event_type_daily": metrics_event_type_daily,
        "metrics_weekly": metrics_weekly,
        "metrics_user_segments": metrics_user_segments,
        "metrics_retention_weekly": metrics_retention_weekly,
        "metrics_retention_summary": metrics_retention_summary,
        "metrics_agent_trendy_repos": agent_trendy,
        "metrics_agent_trend_validation": agent_validation,
    }


def build_local_metrics_from_files(files: list[Path]) -> dict[str, pd.DataFrame]:
    """Build the same metric marts without holding the full fact frame in memory."""
    metadata = load_repo_metadata()
    seed_repo_ids = set(AGENT_SEED_REPOS)
    if metadata.empty:
        candidate_ids = seed_repo_ids
    else:
        candidate_ids = set(
            metadata.loc[metadata_keyword_mask(metadata), "repo_id"]
            .dropna()
            .astype("int64")
            .tolist()
        ) | seed_repo_ids

    daily_parts: list[pd.DataFrame] = []
    event_type_parts: list[pd.DataFrame] = []
    weekly_event_parts: list[pd.DataFrame] = []
    user_week_parts: list[pd.DataFrame] = []
    repo_week_parts: list[pd.DataFrame] = []
    user_day_parts: list[pd.DataFrame] = []
    user_repo_parts: list[pd.DataFrame] = []
    trend_parts: list[pd.DataFrame] = []

    for path in files:
        df = read_activity_file(path)
        df["week_start"] = (
            df["activity_date"] - pd.to_timedelta(df["activity_date"].dt.weekday, unit="D")
        )

        daily_base = (
            df.groupby("activity_date")
            .agg(
                active_users=("user_id", "nunique"),
                active_repos=("repo_id", "nunique"),
                total_events=("event_count", "sum"),
                user_repo_action_rows=("event_count", "size"),
            )
            .reset_index()
        )
        daily_base["events_per_active_user"] = (
            daily_base["total_events"] / daily_base["active_users"]
        )

        action_pivot = (
            df.pivot_table(
                index="activity_date",
                columns="action",
                values="event_count",
                aggfunc="sum",
                fill_value=0,
            )
            .rename(
                columns={
                    "PushEvent": "push_events",
                    "WatchEvent": "watch_events",
                    "ForkEvent": "fork_events",
                    "PullRequestEvent": "pull_request_events",
                    "IssuesEvent": "issue_events",
                    "IssueCommentEvent": "issue_comment_events",
                }
            )
            .reset_index()
        )
        keep_action_cols = [
            "activity_date",
            "push_events",
            "watch_events",
            "fork_events",
            "pull_request_events",
            "issue_events",
            "issue_comment_events",
        ]
        for col in keep_action_cols:
            if col not in action_pivot.columns:
                action_pivot[col] = 0
        daily_parts.append(daily_base.merge(action_pivot[keep_action_cols], on="activity_date"))

        event_type_parts.append(
            df.groupby(["activity_date", "action"])
            .agg(
                active_users=("user_id", "nunique"),
                active_repos=("repo_id", "nunique"),
                total_events=("event_count", "sum"),
                user_repo_action_rows=("event_count", "size"),
            )
            .reset_index()
        )
        weekly_event_parts.append(
            df.groupby("week_start")
            .agg(total_events=("event_count", "sum"))
            .reset_index()
        )
        user_week_parts.append(df[["user_id", "week_start"]].drop_duplicates())
        repo_week_parts.append(df[["repo_id", "week_start"]].drop_duplicates())
        user_day_parts.append(
            df.groupby(["user_id", "activity_date"])
            .agg(
                total_events=("event_count", "sum"),
                active_repos=("repo_id", "nunique"),
            )
            .reset_index()
        )
        user_repo_parts.append(df[["user_id", "repo_id"]].drop_duplicates())

        if candidate_ids:
            trend_df = df[df["repo_id"].isin(candidate_ids)].copy()
            if not trend_df.empty:
                trend_parts.append(weighted_activity(trend_df))

    metrics_daily = pd.concat(daily_parts, ignore_index=True)
    metrics_daily["activity_date"] = metrics_daily["activity_date"].dt.date

    metrics_event_type_daily = pd.concat(event_type_parts, ignore_index=True)
    metrics_event_type_daily["activity_date"] = metrics_event_type_daily["activity_date"].dt.date

    user_week = pd.concat(user_week_parts, ignore_index=True).drop_duplicates()
    repo_week = pd.concat(repo_week_parts, ignore_index=True).drop_duplicates()
    weekly_events = (
        pd.concat(weekly_event_parts, ignore_index=True)
        .groupby("week_start", as_index=False)["total_events"]
        .sum()
    )
    metrics_weekly = (
        user_week.groupby("week_start")
        .size()
        .rename("weekly_active_users")
        .reset_index()
        .merge(
            repo_week.groupby("week_start").size().rename("weekly_active_repos").reset_index(),
            on="week_start",
        )
        .merge(weekly_events, on="week_start")
    )
    metrics_weekly["events_per_active_user"] = (
        metrics_weekly["total_events"] / metrics_weekly["weekly_active_users"]
    )
    metrics_weekly["week_start"] = metrics_weekly["week_start"].dt.date

    user_day = pd.concat(user_day_parts, ignore_index=True)
    user_period = (
        user_day.groupby("user_id")
        .agg(
            active_days=("activity_date", "nunique"),
            total_events=("total_events", "sum"),
        )
        .reset_index()
        .merge(
            pd.concat(user_repo_parts, ignore_index=True)
            .drop_duplicates()
            .groupby("user_id")
            .size()
            .rename("active_repos")
            .reset_index(),
            on="user_id",
            how="left",
        )
    )
    user_period["user_segment"] = pd.cut(
        user_period["active_days"],
        bins=[0, 1, 4, 14, float("inf")],
        labels=["one_day", "repeat_2d_4d", "regular_5d_14d", "power_15d_plus"],
        right=True,
    ).astype("string")
    metrics_user_segments = (
        user_period.groupby("user_segment", observed=True)
        .agg(
            users=("user_id", "size"),
            total_events=("total_events", "sum"),
            avg_active_days=("active_days", "mean"),
            avg_active_repos=("active_repos", "mean"),
            avg_total_events=("total_events", "mean"),
        )
        .reset_index()
    )

    first_week = user_week.groupby("user_id", as_index=False)["week_start"].min()
    first_week = first_week.rename(columns={"week_start": "cohort_week"})
    cohort_activity = user_week.merge(first_week, on="user_id")
    cohort_activity["weeks_since"] = (
        (cohort_activity["week_start"] - cohort_activity["cohort_week"]).dt.days // 7
    ).astype("int64")
    cohort_sizes = (
        first_week.groupby("cohort_week").size().rename("cohort_users").reset_index()
    )
    retention_counts = (
        cohort_activity.groupby(["cohort_week", "week_start", "weeks_since"])
        .agg(active_users=("user_id", "nunique"))
        .reset_index()
        .merge(cohort_sizes, on="cohort_week")
    )
    retention_counts["retention_rate"] = (
        retention_counts["active_users"] / retention_counts["cohort_users"]
    )
    metrics_retention_weekly = retention_counts[
        ["cohort_week", "week_start", "weeks_since", "cohort_users", "active_users", "retention_rate"]
    ].copy()
    metrics_retention_weekly["cohort_week"] = metrics_retention_weekly["cohort_week"].dt.date
    metrics_retention_weekly["week_start"] = metrics_retention_weekly["week_start"].dt.date

    retention_summary = metrics_retention_weekly.pivot_table(
        index=["cohort_week", "cohort_users"],
        columns="weeks_since",
        values="retention_rate",
        fill_value=0.0,
    ).reset_index()
    for week_num in range(4):
        if week_num not in retention_summary.columns:
            retention_summary[week_num] = 0.0
    metrics_retention_summary = retention_summary[
        ["cohort_week", "cohort_users", 0, 1, 2, 3]
    ].rename(
        columns={
            0: "w0_retention",
            1: "w1_retention",
            2: "w2_retention",
            3: "w3_retention",
        }
    )

    if trend_parts:
        trend_activity = pd.concat(trend_parts, ignore_index=True)
        agent_trendy = make_agent_trendy_table(
            trend_activity,
            metadata,
            baseline_days=28,
            recent_days=7,
        )
        agent_validation = evaluate_agent_trend(trend_activity, metadata)
    else:
        agent_trendy = pd.DataFrame()
        agent_validation = pd.DataFrame()

    return {
        "metrics_daily": metrics_daily,
        "metrics_event_type_daily": metrics_event_type_daily,
        "metrics_weekly": metrics_weekly,
        "metrics_user_segments": metrics_user_segments,
        "metrics_retention_weekly": metrics_retention_weekly,
        "metrics_retention_summary": metrics_retention_summary,
        "metrics_agent_trendy_repos": agent_trendy,
        "metrics_agent_trend_validation": agent_validation,
    }


def load_dataframe_table(client: bigquery.Client, table_id: str, df: pd.DataFrame) -> None:
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()


def duckdb_parquet_source(files: list[Path]) -> str:
    file_list = ", ".join("'" + path.as_posix().replace("'", "''") + "'" for path in files)
    return f"""
    (
      SELECT
        CAST(actor_id AS BIGINT) AS user_id,
        CAST(repo_id AS BIGINT) AS repo_id,
        CAST(type AS VARCHAR) AS action,
        CAST(cnt AS BIGINT) AS event_count,
        CAST(strptime(regexp_extract(filename, '([0-9]{{8}})\\.parquet$', 1), '%Y%m%d') AS DATE)
          AS activity_date
      FROM read_parquet([{file_list}], filename = true)
      WHERE actor_id IS NOT NULL
        AND repo_id IS NOT NULL
        AND type IS NOT NULL
        AND cnt IS NOT NULL
    )
    """


def build_local_metrics_duckdb(files: list[Path]) -> dict[str, pd.DataFrame]:
    source = duckdb_parquet_source(files)
    Path("/tmp/duckdb-spill").mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=":memory:")
    con.execute("SET memory_limit = '8GB'")
    con.execute("SET temp_directory = '/tmp/duckdb-spill'")
    con.execute("SET threads = 1")
    con.execute("SET preserve_insertion_order = false")
    metadata = load_repo_metadata()
    seed_repo_ids = set(AGENT_SEED_REPOS)
    if metadata.empty:
        candidate_ids = seed_repo_ids
    else:
        candidate_ids = set(
            metadata.loc[metadata_keyword_mask(metadata), "repo_id"]
            .dropna()
            .astype("int64")
            .tolist()
        ) | seed_repo_ids
    candidate_sql = ", ".join(str(repo_id) for repo_id in sorted(candidate_ids)) or "-1"

    metrics_daily = con.sql(
        f"""
        WITH fact AS {source}
        SELECT
          activity_date,
          COUNT(DISTINCT user_id) AS active_users,
          COUNT(DISTINCT repo_id) AS active_repos,
          SUM(event_count) AS total_events,
          COUNT(*) AS user_repo_action_rows,
          SUM(event_count)::DOUBLE / COUNT(DISTINCT user_id) AS events_per_active_user,
          SUM(CASE WHEN action = 'PushEvent' THEN event_count ELSE 0 END) AS push_events,
          SUM(CASE WHEN action = 'WatchEvent' THEN event_count ELSE 0 END) AS watch_events,
          SUM(CASE WHEN action = 'ForkEvent' THEN event_count ELSE 0 END) AS fork_events,
          SUM(CASE WHEN action = 'PullRequestEvent' THEN event_count ELSE 0 END) AS pull_request_events,
          SUM(CASE WHEN action = 'IssuesEvent' THEN event_count ELSE 0 END) AS issue_events,
          SUM(CASE WHEN action = 'IssueCommentEvent' THEN event_count ELSE 0 END) AS issue_comment_events
        FROM fact
        GROUP BY activity_date
        ORDER BY activity_date
        """
    ).df()

    metrics_event_type_daily = con.sql(
        f"""
        WITH fact AS {source}
        SELECT
          activity_date,
          action,
          COUNT(DISTINCT user_id) AS active_users,
          COUNT(DISTINCT repo_id) AS active_repos,
          SUM(event_count) AS total_events,
          COUNT(*) AS user_repo_action_rows
        FROM fact
        GROUP BY activity_date, action
        ORDER BY activity_date, action
        """
    ).df()

    metrics_weekly = con.sql(
        f"""
        WITH fact AS {source}
        SELECT
          CAST(date_trunc('week', activity_date) AS DATE) AS week_start,
          COUNT(DISTINCT user_id) AS weekly_active_users,
          COUNT(DISTINCT repo_id) AS weekly_active_repos,
          SUM(event_count) AS total_events,
          SUM(event_count)::DOUBLE / COUNT(DISTINCT user_id) AS events_per_active_user
        FROM fact
        GROUP BY week_start
        ORDER BY week_start
        """
    ).df()

    metrics_user_segments = con.sql(
        f"""
        WITH fact AS {source},
        user_period AS (
          SELECT
            user_id,
            COUNT(DISTINCT activity_date) AS active_days,
            SUM(event_count) AS total_events,
            COUNT(DISTINCT repo_id) AS active_repos
          FROM fact
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
    ).df()

    metrics_retention_weekly = con.sql(
        f"""
        WITH fact AS {source},
        user_week AS (
          SELECT DISTINCT user_id, CAST(date_trunc('week', activity_date) AS DATE) AS week_start
          FROM fact
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
            CAST(date_diff('week', fw.cohort_week, uw.week_start) AS BIGINT) AS weeks_since
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
          COUNT(DISTINCT ca.user_id)::DOUBLE / cs.cohort_users AS retention_rate
        FROM cohort_activity ca
        JOIN cohort_sizes cs USING (cohort_week)
        GROUP BY ca.cohort_week, ca.week_start, ca.weeks_since, cs.cohort_users
        ORDER BY ca.cohort_week, ca.weeks_since
        """
    ).df()

    metrics_retention_summary = con.sql(
        f"""
        WITH retention AS (
          WITH fact AS {source},
          user_week AS (
            SELECT DISTINCT user_id, CAST(date_trunc('week', activity_date) AS DATE) AS week_start
            FROM fact
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
              CAST(date_diff('week', fw.cohort_week, uw.week_start) AS BIGINT) AS weeks_since
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
            ca.weeks_since,
            cs.cohort_users,
            COUNT(DISTINCT ca.user_id)::DOUBLE / cs.cohort_users AS retention_rate
          FROM cohort_activity ca
          JOIN cohort_sizes cs USING (cohort_week)
          GROUP BY ca.cohort_week, ca.weeks_since, cs.cohort_users
        )
        SELECT
          cohort_week,
          cohort_users,
          MAX(CASE WHEN weeks_since = 0 THEN retention_rate ELSE 0 END) AS w0_retention,
          MAX(CASE WHEN weeks_since = 1 THEN retention_rate ELSE 0 END) AS w1_retention,
          MAX(CASE WHEN weeks_since = 2 THEN retention_rate ELSE 0 END) AS w2_retention,
          MAX(CASE WHEN weeks_since = 3 THEN retention_rate ELSE 0 END) AS w3_retention
        FROM retention
        GROUP BY cohort_week, cohort_users
        ORDER BY cohort_week
        """
    ).df()

    trend_activity = con.sql(
        f"""
        WITH fact AS {source}
        SELECT user_id, repo_id, action, event_count, activity_date
        FROM fact
        WHERE repo_id IN ({candidate_sql})
        """
    ).df()
    if trend_activity.empty:
        agent_trendy = pd.DataFrame()
        agent_validation = pd.DataFrame()
    else:
        trend_activity["activity_date"] = pd.to_datetime(trend_activity["activity_date"])
        weighted_df = weighted_activity(trend_activity)
        agent_trendy = make_agent_trendy_table(
            weighted_df,
            metadata,
            baseline_days=28,
            recent_days=7,
        )
        agent_validation = evaluate_agent_trend(weighted_df, metadata)

    return {
        "metrics_daily": metrics_daily,
        "metrics_event_type_daily": metrics_event_type_daily,
        "metrics_weekly": metrics_weekly,
        "metrics_user_segments": metrics_user_segments,
        "metrics_retention_weekly": metrics_retention_weekly,
        "metrics_retention_summary": metrics_retention_summary,
        "metrics_agent_trendy_repos": agent_trendy,
        "metrics_agent_trend_validation": agent_validation,
    }


def rebuild_metrics(client: bigquery.Client, names: TableNames, files: list[Path]) -> None:
    for table_name in [
        "metrics_daily",
        "metrics_event_type_daily",
        "metrics_weekly",
        "metrics_user_segments",
        "metrics_retention_weekly",
        "metrics_retention_summary",
        "metrics_agent_trendy_repos",
        "metrics_agent_trend_validation",
    ]:
        client.delete_table(names.table_id(table_name), not_found_ok=True)

    metrics = build_local_metrics_duckdb(files)
    for table_name, metric_df in metrics.items():
        load_dataframe_table(client, names.table_id(table_name), metric_df)
        print(f"LOADED {table_name} rows={len(metric_df):,}")


def overwrite_query_table(client: bigquery.Client, table_id: str, sql: str) -> int:
    job_config = bigquery.QueryJobConfig(
        destination=table_id,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.query(sql, job_config=job_config)
    job.result()
    table = client.get_table(table_id)
    return table.num_rows


def rebuild_sql_metrics_from_bq(client: bigquery.Client, names: TableNames) -> None:
    metric_queries = {
        "metrics_daily": f"""
            SELECT
              activity_date,
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
            FROM `{names.fact_id}`
            GROUP BY activity_date
        """,
        "metrics_event_type_daily": f"""
            SELECT
              activity_date,
              action,
              COUNT(DISTINCT user_id) AS active_users,
              COUNT(DISTINCT repo_id) AS active_repos,
              SUM(event_count) AS total_events,
              COUNT(*) AS user_repo_action_rows
            FROM `{names.fact_id}`
            GROUP BY activity_date, action
        """,
        "metrics_weekly": f"""
            SELECT
              DATE_TRUNC(activity_date, WEEK(MONDAY)) AS week_start,
              COUNT(DISTINCT user_id) AS weekly_active_users,
              COUNT(DISTINCT repo_id) AS weekly_active_repos,
              SUM(event_count) AS total_events,
              SAFE_DIVIDE(SUM(event_count), COUNT(DISTINCT user_id)) AS events_per_active_user
            FROM `{names.fact_id}`
            GROUP BY week_start
        """,
        "metrics_user_segments": f"""
            WITH user_period AS (
              SELECT
                user_id,
                COUNT(DISTINCT activity_date) AS active_days,
                SUM(event_count) AS total_events,
                COUNT(DISTINCT repo_id) AS active_repos
              FROM `{names.fact_id}`
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
        """,
        "metrics_retention_weekly": f"""
            WITH user_week AS (
              SELECT DISTINCT user_id, DATE_TRUNC(activity_date, WEEK(MONDAY)) AS week_start
              FROM `{names.fact_id}`
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
        """,
        "metrics_retention_summary": f"""
            WITH user_week AS (
              SELECT DISTINCT user_id, DATE_TRUNC(activity_date, WEEK(MONDAY)) AS week_start
              FROM `{names.fact_id}`
            ),
            first_week AS (
              SELECT user_id, MIN(week_start) AS cohort_week
              FROM user_week
              GROUP BY user_id
            ),
            cohort_sizes AS (
              SELECT cohort_week, COUNT(*) AS cohort_users
              FROM first_week
              GROUP BY cohort_week
            ),
            retention AS (
              SELECT
                fw.cohort_week,
                DATE_DIFF(uw.week_start, fw.cohort_week, WEEK) AS weeks_since,
                COUNT(DISTINCT uw.user_id) AS active_users
              FROM user_week uw
              JOIN first_week fw USING (user_id)
              GROUP BY fw.cohort_week, uw.week_start, weeks_since
            )
            SELECT
              r.cohort_week,
              cs.cohort_users,
              MAX(IF(r.weeks_since = 0, SAFE_DIVIDE(r.active_users, cs.cohort_users), 0)) AS w0_retention,
              MAX(IF(r.weeks_since = 1, SAFE_DIVIDE(r.active_users, cs.cohort_users), 0)) AS w1_retention,
              MAX(IF(r.weeks_since = 2, SAFE_DIVIDE(r.active_users, cs.cohort_users), 0)) AS w2_retention,
              MAX(IF(r.weeks_since = 3, SAFE_DIVIDE(r.active_users, cs.cohort_users), 0)) AS w3_retention
            FROM retention r
            JOIN cohort_sizes cs USING (cohort_week)
            GROUP BY r.cohort_week, cs.cohort_users
        """,
    }

    for table_name in [
        "metrics_daily",
        "metrics_event_type_daily",
        "metrics_weekly",
        "metrics_user_segments",
        "metrics_retention_weekly",
        "metrics_retention_summary",
    ]:
        rows = overwrite_query_table(client, names.table_id(table_name), metric_queries[table_name])
        print(f"LOADED {table_name} rows={rows:,}")


def build_agent_metrics_from_bq(client: bigquery.Client, names: TableNames) -> dict[str, pd.DataFrame]:
    metadata = load_repo_metadata_from_bq(client, names)
    seed_repo_ids = set(AGENT_SEED_REPOS)
    if metadata.empty:
        candidate_ids = seed_repo_ids
    else:
        candidate_ids = set(
            metadata.loc[metadata_keyword_mask(metadata), "repo_id"]
            .dropna()
            .astype("int64")
            .tolist()
        ) | seed_repo_ids
    if not candidate_ids:
        return {
            "metrics_agent_trendy_repos": pd.DataFrame(),
            "metrics_agent_trend_validation": pd.DataFrame(),
        }

    sql = f"""
    SELECT user_id, repo_id, action, event_count, activity_date
    FROM `{names.fact_id}`
    WHERE repo_id IN UNNEST(@repo_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("repo_ids", "INT64", sorted(candidate_ids))
        ]
    )
    trend_activity = client.query(sql, job_config=job_config).to_dataframe()
    if trend_activity.empty:
        return {
            "metrics_agent_trendy_repos": pd.DataFrame(),
            "metrics_agent_trend_validation": pd.DataFrame(),
        }

    trend_activity["activity_date"] = pd.to_datetime(trend_activity["activity_date"])
    weighted_df = weighted_activity(trend_activity)
    return {
        "metrics_agent_trendy_repos": make_agent_trendy_table(
            weighted_df,
            metadata,
            baseline_days=28,
            recent_days=7,
        ),
        "metrics_agent_trend_validation": evaluate_agent_trend(weighted_df, metadata),
    }


def rebuild_metrics_from_bq(client: bigquery.Client, names: TableNames) -> None:
    for table_name in [
        "metrics_daily",
        "metrics_event_type_daily",
        "metrics_weekly",
        "metrics_user_segments",
        "metrics_retention_weekly",
        "metrics_retention_summary",
        "metrics_agent_trendy_repos",
        "metrics_agent_trend_validation",
    ]:
        client.delete_table(names.table_id(table_name), not_found_ok=True)

    rebuild_sql_metrics_from_bq(client, names)
    for table_name, metric_df in build_agent_metrics_from_bq(client, names).items():
        load_dataframe_table(client, names.table_id(table_name), metric_df)
        print(f"LOADED {table_name} rows={len(metric_df):,}")


def print_summary(client: bigquery.Client, names: TableNames) -> None:
    sql = f"""
    SELECT
      MIN(activity_date) AS min_date,
      MAX(activity_date) AS max_date,
      COUNT(*) AS days,
      SUM(total_events) AS events,
      MAX(active_users) AS max_daily_users,
      MAX(active_repos) AS max_daily_repos
    FROM `{names.table_id("metrics_daily")}`
    """
    row = next(iter(client.query(sql).result()))
    print(
        "METRICS "
        f"{row.min_date}..{row.max_date} days={row.days:,} "
        f"events={row.events:,} max_daily_users={row.max_daily_users:,} "
        f"max_daily_repos={row.max_daily_repos:,}"
    )

    metrics_sql = f"""
    SELECT activity_date, active_users, active_repos, total_events
    FROM `{names.table_id("metrics_daily")}`
    ORDER BY activity_date DESC
    LIMIT 7
    """
    print("LAST 7 DAILY METRICS")
    for metric in client.query(metrics_sql).result():
        print(
            f"  {metric.activity_date} "
            f"users={metric.active_users:,} repos={metric.active_repos:,} "
            f"events={metric.total_events:,}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--fact-table", default=DEFAULT_FACT_TABLE)
    parser.add_argument("--location", default="US")
    parser.add_argument(
        "--source",
        choices=["bigquery", "parquet"],
        default="bigquery",
        help="Read daily aggregates from GitHub Archive BigQuery tables or legacy local parquet.",
    )
    parser.add_argument("--parquet-dir", type=Path, default=Path("data/daily_agg"))
    parser.add_argument("--key-path", default=os.environ.get("GCP_KEY_PATH"))
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument(
        "--mode",
        choices=["replace-all", "replace-days", "append", "skip-existing"],
        default="replace-all",
    )
    parser.add_argument("--build-metrics", action="store_true")
    parser.add_argument("--skip-fact", action="store_true")
    parser.add_argument("--summary", action="store_true", default=True)
    parser.add_argument("--no-summary", action="store_false", dest="summary")
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--max-days", type=int, default=DEFAULT_MAX_DAYS)
    parser.add_argument("--allow-full-history", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start = datetime.strptime(args.start, "%Y-%m-%d").date() if args.start else None
    end = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else None
    names = TableNames(args.project, args.dataset, args.fact_table)
    files: list[Path] = []
    days: list[date] = []
    if args.source == "parquet":
        files = iter_parquet_files(args.parquet_dir, start, end)
        if not files:
            raise FileNotFoundError(f"No parquet files found in {args.parquet_dir}")
        print_plan(files, args.mode, args.max_days)
        selected_days = len(files)
    else:
        if not start or not end:
            raise SystemExit("--source bigquery requires explicit --start and --end.")
        days = iter_dates(start, end)
        print_bq_plan(days, args.mode, args.max_days)
        selected_days = len(days)

    if selected_days > args.max_days and not args.allow_full_history:
        raise SystemExit(
            f"Refusing to sync {selected_days} days. Pass --start/--end for a bounded window "
            f"or add --allow-full-history intentionally."
        )
    if args.plan_only:
        return

    client = make_client(args.project, args.key_path)

    ensure_dataset(client, names, args.location)
    if args.skip_fact:
        print(f"SKIP fact table sync; metric tables will be built from {args.source}.")
    else:
        if args.mode == "replace-all":
            client.delete_table(names.fact_id, not_found_ok=True)
        ensure_fact_table(client, names)

        print(f"SYNC source={args.source} days={selected_days} mode={args.mode} target={names.fact_id}")
        total_rows = 0
        if args.source == "parquet":
            for index, path in enumerate(files):
                write_disposition = None
                load_mode = args.mode
                if args.mode == "replace-all":
                    load_mode = "append"
                    write_disposition = (
                        bigquery.WriteDisposition.WRITE_TRUNCATE
                        if index == 0
                        else bigquery.WriteDisposition.WRITE_APPEND
                    )
                total_rows += load_one_day(client, names, path, load_mode, write_disposition)
        else:
            load_mode = "append" if args.mode == "replace-all" else args.mode
            for activity_date in days:
                total_rows += load_one_day_from_public_bigquery(
                    client,
                    names,
                    activity_date,
                    load_mode,
                )
        print(f"SYNCED rows={total_rows:,}")

    if args.build_metrics:
        if args.source == "parquet":
            rebuild_metrics(client, names, files)
        else:
            rebuild_metrics_from_bq(client, names)
    if args.summary:
        print_summary(client, names)


if __name__ == "__main__":
    main()
