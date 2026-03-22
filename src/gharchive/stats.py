"""Basic statistics from daily aggregation data."""

import numpy as np
import pandas as pd


def daily_active_users(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    """Count unique actors per day.

    Expects a DataFrame with 'actor_id' and a date column.
    Returns DataFrame with date and dau columns.
    """
    return (
        df.groupby(date_col)["actor_id"]
        .nunique()
        .reset_index()
        .rename(columns={"actor_id": "dau"})
        .sort_values(date_col)
    )


def weekly_cohort_retention(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    """Compute weekly cohort retention table.

    Assigns each user to a cohort (the week they first appeared),
    then calculates retention rate for each subsequent week.

    Returns a pivot DataFrame: rows = cohort week, columns = weeks since cohort,
    values = retention rate (0~1).
    """
    df = df.copy()
    df["week"] = df[date_col].dt.to_period("W-SAT").dt.start_time

    # cohort = first week each user appeared
    cohort = df.groupby("actor_id")["week"].min().rename("cohort")
    df = df.merge(cohort, on="actor_id")

    # unique users per cohort per week
    activity = df.groupby(["cohort", "week"])["actor_id"].nunique().reset_index(name="users")
    activity["weeks_since"] = ((activity["week"] - activity["cohort"]).dt.days // 7).astype(int)

    # cohort sizes
    cohort_sizes = activity[activity["weeks_since"] == 0].set_index("cohort")["users"]

    # pivot and divide by cohort size
    pivot = activity.pivot_table(index="cohort", columns="weeks_since", values="users", fill_value=0)
    retention = pivot.div(cohort_sizes, axis=0)

    retention.index = retention.index.strftime("%m/%d")
    retention.index.name = "cohort_week"
    return retention


def user_activity_summary(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    """Compute per-user activity summary.

    Returns DataFrame with: actor_id, active_days, total_events,
    first_seen, last_seen, top_event_type.
    """
    df = df.copy()
    df["date_d"] = df[date_col].dt.date

    agg = df.groupby("actor_id").agg(
        active_days=("date_d", "nunique"),
        total_events=("cnt", "sum"),
        first_seen=("date_d", "min"),
        last_seen=("date_d", "max"),
    )

    # top event type per user
    top_type = (
        df.groupby(["actor_id", "type"])["cnt"]
        .sum()
        .reset_index()
        .sort_values("cnt", ascending=False)
        .drop_duplicates("actor_id")
        .set_index("actor_id")["type"]
        .rename("top_event_type")
    )

    return agg.join(top_type).reset_index()


def daily_active_users_by_type(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    """Count unique actors per day per event type.

    Returns DataFrame with date, type, and dau columns.
    """
    return (
        df.groupby([date_col, "type"])["actor_id"]
        .nunique()
        .reset_index()
        .rename(columns={"actor_id": "dau"})
        .sort_values([date_col, "type"])
    )
