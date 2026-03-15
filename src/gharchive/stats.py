"""Basic statistics from daily aggregation data."""

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
