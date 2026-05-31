"""Analytics-layer adapter for input drift: within-day distributions from daily_agg.

Drift is measured on the distribution *across users/repos within a day* (stable,
many samples) rather than daily scalar aggregates (trend/seasonality dominated).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

FEATURES = ["events_per_user", "events_per_repo", "repos_per_user"]


def day_distributions(
    path: str | Path, max_sample: int = 50000, rng: np.random.Generator | None = None
) -> dict[str, np.ndarray]:
    """Sampled log-scale within-day distributions for one daily_agg parquet."""
    if rng is None:
        rng = np.random.default_rng(0)
    df = pd.read_parquet(path, columns=["actor_id", "repo_id", "type", "cnt"])
    by_user = df.groupby("actor_id").agg(
        events=("cnt", "sum"), repos=("repo_id", "nunique")
    )
    by_repo = df.groupby("repo_id")["cnt"].sum()

    def _sample(values: np.ndarray) -> np.ndarray:
        values = np.log1p(values.astype(float))
        if values.size > max_sample:
            values = values[rng.choice(values.size, size=max_sample, replace=False)]
        return values

    return {
        "events_per_user": _sample(by_user["events"].to_numpy()),
        "events_per_repo": _sample(by_repo.to_numpy()),
        "repos_per_user": _sample(by_user["repos"].to_numpy()),
    }


def day_distributions_from_bq(
    client,
    table_id: str,
    activity_date: str,
    max_sample: int = 50000,
    rng: np.random.Generator | None = None,
) -> dict[str, np.ndarray]:
    """Sampled log-scale within-day distributions from a BigQuery fact table."""
    if rng is None:
        rng = np.random.default_rng(0)

    user_sql = f"""
    select
      user_id,
      sum(event_count) as events,
      count(distinct repo_id) as repos
    from `{table_id}`
    where activity_date = @activity_date
    group by user_id
    """
    repo_sql = f"""
    select
      repo_id,
      sum(event_count) as events
    from `{table_id}`
    where activity_date = @activity_date
    group by repo_id
    """
    try:
        from google.cloud import bigquery
    except ImportError as exc:  # pragma: no cover - exercised only without optional dep
        raise RuntimeError("google-cloud-bigquery is required for BigQuery drift input") from exc

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("activity_date", "DATE", activity_date)]
    )
    by_user = client.query(user_sql, job_config=job_config).to_dataframe()
    by_repo = client.query(repo_sql, job_config=job_config).to_dataframe()

    def _sample(values: np.ndarray) -> np.ndarray:
        values = np.log1p(values.astype(float))
        if values.size > max_sample:
            values = values[rng.choice(values.size, size=max_sample, replace=False)]
        return values

    return {
        "events_per_user": _sample(by_user["events"].to_numpy()),
        "events_per_repo": _sample(by_repo["events"].to_numpy()),
        "repos_per_user": _sample(by_user["repos"].to_numpy()),
    }
