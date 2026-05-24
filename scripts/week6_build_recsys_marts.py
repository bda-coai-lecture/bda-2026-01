"""Build reproducible Week 6 recommendation batch marts from local parquet.

The goal is to stop rebuilding the same user/repo aggregates inside every
experiment script. These outputs are local parquet marts keyed by an explicit
window_end_date or snapshot_date, so training and evaluation can be recreated
from the same point-in-time inputs.

Usage:
    uv run python scripts/week6_build_recsys_marts.py --smoke
    uv run python scripts/week6_build_recsys_marts.py
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm


DATA_DIR = Path("data/daily_agg")
MART_DIR = Path("data/marts/week6")
DB_PATH = Path("data/repo_metadata.db")

EVENT_WEIGHTS = {
    "WatchEvent": 1.0,
    "ForkEvent": 2.0,
    "IssuesEvent": 0.5,
    "PullRequestEvent": 3.0,
    "IssueCommentEvent": 0.3,
    "PushEvent": 0.2,
}

EVENT_COUNT_COLUMNS = {
    "WatchEvent": "watch_cnt",
    "ForkEvent": "fork_cnt",
    "PullRequestEvent": "pr_cnt",
    "PushEvent": "push_cnt",
    "IssuesEvent": "issue_cnt",
    "IssueCommentEvent": "comment_cnt",
    "CommitCommentEvent": "comment_cnt",
}

PROFILE_EVENTS = [
    ("watch", "WatchEvent"),
    ("fork", "ForkEvent"),
    ("pr", "PullRequestEvent"),
    ("push", "PushEvent"),
    ("issue", "IssuesEvent"),
    ("comment", "IssueCommentEvent"),
]

REPO_POPULARITY_EVENTS = [
    "WatchEvent",
    "ForkEvent",
    "PullRequestEvent",
    "PushEvent",
    "IssuesEvent",
]


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def parse_day(path: Path) -> date:
    return datetime.strptime(path.stem, "%Y%m%d").date()


def load_daily_activity(data_dir: Path, start: date, end: date) -> pd.DataFrame:
    frames = []
    current = start
    while current <= end:
        path = data_dir / f"{current:%Y%m%d}.parquet"
        if path.exists():
            df = pd.read_parquet(path)
            df["activity_date"] = pd.Timestamp(current)
            frames.append(df)
        current += timedelta(days=1)
    if not frames:
        raise FileNotFoundError(f"No parquet files found in {data_dir} for {start}..{end}")

    out = pd.concat(frames, ignore_index=True)
    out = out.dropna(subset=["actor_id", "repo_id", "type", "cnt"])
    return out.astype(
        {
            "actor_id": "int64",
            "repo_id": "int64",
            "type": "string",
            "cnt": "int64",
        }
    )


def add_weighted_score(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["weighted_score"] = out["type"].map(EVENT_WEIGHTS).fillna(0.0) * out["cnt"]
    return out


def build_feedback(df: pd.DataFrame) -> pd.DataFrame:
    fb = (
        add_weighted_score(df)
        .groupby(["actor_id", "repo_id"], observed=True)["weighted_score"]
        .sum()
        .reset_index()
    )
    return fb[fb["weighted_score"] > 0].rename(columns={"weighted_score": "score"})


def build_user_repo_interaction_mart(df: pd.DataFrame, window_end_date: date) -> pd.DataFrame:
    scored = add_weighted_score(df)
    base = (
        scored.groupby(["actor_id", "repo_id"], observed=True)
        .agg(
            weighted_score=("weighted_score", "sum"),
            first_seen_at=("activity_date", "min"),
            last_seen_at=("activity_date", "max"),
            active_days=("activity_date", "nunique"),
        )
        .reset_index()
    )

    counts = (
        scored.assign(event_col=scored["type"].map(EVENT_COUNT_COLUMNS))
        .dropna(subset=["event_col"])
        .groupby(["actor_id", "repo_id", "event_col"], observed=True)["cnt"]
        .sum()
        .unstack(fill_value=0)
        .reset_index()
    )
    out = base.merge(counts, on=["actor_id", "repo_id"], how="left")
    for col in sorted(set(EVENT_COUNT_COLUMNS.values())):
        if col not in out:
            out[col] = 0
        out[col] = out[col].fillna(0).astype("int64")
    out.insert(0, "window_end_date", pd.Timestamp(window_end_date))
    return out.sort_values(["weighted_score", "actor_id", "repo_id"], ascending=[False, True, True])


def _score_by_actor(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="float64", index=pd.Index([], name="actor_id"))
    return add_weighted_score(df).groupby("actor_id", observed=True)["weighted_score"].sum()


def _entropy_from_counts(counts: pd.DataFrame) -> pd.Series:
    totals = counts.sum(axis=1).replace(0, np.nan)
    shares = counts.div(totals, axis=0).fillna(0)
    log_shares = np.log(shares.where(shares > 0, 1.0))
    return -(shares * log_shares).sum(axis=1)


def build_user_profile_mart(
    df: pd.DataFrame,
    window_end_date: date,
    recent_days: int,
) -> pd.DataFrame:
    scored = add_weighted_score(df)
    recent_start = pd.Timestamp(window_end_date - timedelta(days=recent_days - 1))
    prior_start = pd.Timestamp(window_end_date - timedelta(days=(recent_days * 2) - 1))
    recent = df[df["activity_date"] >= recent_start]
    prior = df[(df["activity_date"] >= prior_start) & (df["activity_date"] < recent_start)]

    out = (
        scored.groupby("actor_id", observed=True)
        .agg(
            total_score=("weighted_score", "sum"),
            unique_repos=("repo_id", "nunique"),
            active_days=("activity_date", "nunique"),
        )
        .reset_index()
    )

    counts = df.groupby(["actor_id", "type"], observed=True)["cnt"].sum().unstack(fill_value=0)
    total_events = counts.sum(axis=1).replace(0, np.nan)
    for prefix, event_type in PROFILE_EVENTS:
        value = counts.get(event_type, 0)
        if prefix == "comment":
            value = value + counts.get("CommitCommentEvent", 0)
        out = out.merge(
            (value / total_events).fillna(0).rename(f"{prefix}_share").reset_index(),
            on="actor_id",
            how="left",
        )

    entropy = _entropy_from_counts(counts).rename("event_entropy").reset_index()
    recent_score = _score_by_actor(recent).rename("recent_score")
    prior_score = _score_by_actor(prior).rename("prior_score")
    out = out.merge(entropy, on="actor_id", how="left")
    out = out.merge(recent_score.reset_index(), on="actor_id", how="left")
    out = out.merge(prior_score.reset_index(), on="actor_id", how="left")
    out[["recent_score", "prior_score"]] = out[["recent_score", "prior_score"]].fillna(0.0)
    out["recent_score_share"] = out["recent_score"] / out["total_score"].clip(lower=1e-6)
    out["score_growth_ratio"] = (out["recent_score"] - out["prior_score"]) / (
        out["prior_score"] + 1.0
    )
    out = out.drop(columns=["recent_score", "prior_score"])
    out.insert(0, "window_end_date", pd.Timestamp(window_end_date))
    return out.sort_values(["total_score", "actor_id"], ascending=[False, True])


def load_repo_metadata(db_path: Path) -> pd.DataFrame:
    columns = [
        "repo_id",
        "repo_name",
        "language",
        "topics",
        "stargazers",
        "forks",
        "archived",
        "http_status",
    ]
    if not db_path.exists():
        return pd.DataFrame(columns=columns)
    conn = sqlite3.connect(str(db_path))
    try:
        return pd.read_sql_query(
            """
            SELECT repo_id, repo_name, language, topics, stargazers, forks, archived, http_status
            FROM repo_metadata
            """,
            conn,
        )
    finally:
        conn.close()


def add_owner_org_columns(metadata: pd.DataFrame) -> pd.DataFrame:
    out = metadata.copy()
    repo_name = out["repo_name"].fillna("")
    owner = repo_name.str.split("/", n=1).str[0]
    out["owner_name"] = owner.where(owner != "", None)
    out["owner_id"] = pd.NA
    out["org_name"] = out["owner_name"]
    out["org_id"] = pd.NA
    return out


def _window_repo_stats(df: pd.DataFrame, snapshot: pd.Timestamp, days: int) -> pd.DataFrame:
    start = snapshot - pd.Timedelta(days=days - 1)
    part = add_weighted_score(df[df["activity_date"].between(start, snapshot)])
    if part.empty:
        return pd.DataFrame(columns=["repo_id"])

    base = (
        part.groupby("repo_id", observed=True)
        .agg(
            **{
                f"total_score_{days}d": ("weighted_score", "sum"),
                f"unique_users_{days}d": ("actor_id", "nunique"),
            }
        )
        .reset_index()
    )

    event_users = (
        part[part["type"].isin(REPO_POPULARITY_EVENTS)]
        .groupby(["repo_id", "type"], observed=True)["actor_id"]
        .nunique()
        .unstack(fill_value=0)
        .reset_index()
    )
    event_users = event_users.rename(
        columns={
            "WatchEvent": f"watch_users_{days}d",
            "ForkEvent": f"fork_users_{days}d",
            "PullRequestEvent": f"pr_users_{days}d",
            "PushEvent": f"push_users_{days}d",
            "IssuesEvent": f"issue_users_{days}d",
        }
    )
    comment = (
        part[part["type"].isin(["IssueCommentEvent", "CommitCommentEvent"])]
        .groupby("repo_id", observed=True)["actor_id"]
        .nunique()
        .rename(f"comment_users_{days}d")
        .reset_index()
    )
    return base.merge(event_users, on="repo_id", how="left").merge(comment, on="repo_id", how="left")


def build_repo_feature_mart(
    df: pd.DataFrame,
    metadata: pd.DataFrame,
    snapshot_date: date,
) -> pd.DataFrame:
    snapshot = pd.Timestamp(snapshot_date)
    repo_ids = pd.DataFrame({"repo_id": sorted(df["repo_id"].unique())})
    out = repo_ids
    for days in (7, 28, 42):
        out = out.merge(_window_repo_stats(df, snapshot, days), on="repo_id", how="left")

    for col in out.columns:
        if col != "repo_id":
            out[col] = out[col].fillna(0)

    out["score_growth_ratio"] = (out["total_score_7d"] - (out["total_score_28d"] / 4.0)) / (
        (out["total_score_28d"] / 4.0) + 1.0
    )
    out["user_growth_ratio"] = (out["unique_users_7d"] - (out["unique_users_28d"] / 4.0)) / (
        (out["unique_users_28d"] / 4.0) + 1.0
    )

    metadata = add_owner_org_columns(metadata)
    meta_cols = [
        "repo_id",
        "owner_id",
        "owner_name",
        "org_id",
        "org_name",
        "language",
        "topics",
        "stargazers",
        "forks",
        "archived",
    ]
    out = out.merge(metadata[meta_cols], on="repo_id", how="left")
    out = out.rename(columns={"stargazers": "stars"})
    out.insert(0, "snapshot_date", snapshot)
    return out.sort_values(["total_score_42d", "repo_id"], ascending=[False, True])


def _topic_set(value: object) -> set[str]:
    if not isinstance(value, str) or not value:
        return set()
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return set()
    if not isinstance(parsed, list):
        return set()
    return {str(topic).lower() for topic in parsed}


def build_repo_repo_related_mart(
    history_fb: pd.DataFrame,
    metadata: pd.DataFrame,
    window_end_date: date,
    max_users: int | None,
    max_items_per_user: int,
    related_top_k: int,
) -> pd.DataFrame:
    item_score = history_fb.groupby("repo_id", observed=True)["score"].sum().to_dict()
    pair_score: defaultdict[tuple[int, int], float] = defaultdict(float)
    pair_users: Counter[tuple[int, int]] = Counter()
    groups = history_fb.sort_values(["actor_id", "score"], ascending=[True, False]).groupby(
        "actor_id", observed=True
    )
    total_users = history_fb["actor_id"].nunique()
    if max_users:
        total_users = min(total_users, max_users)

    for i, (_, rows) in enumerate(tqdm(groups, total=total_users, desc="repo related pairs")):
        if max_users and i >= max_users:
            break
        rows = rows.head(max_items_per_user)
        items = rows["repo_id"].astype("int64").to_numpy()
        scores = np.log1p(rows["score"].astype(float).to_numpy())
        if len(items) < 2:
            continue
        for left in range(len(items) - 1):
            for right in range(left + 1, len(items)):
                a, b = int(items[left]), int(items[right])
                if a == b:
                    continue
                key = (a, b) if a < b else (b, a)
                pair_score[key] += float(math.sqrt(scores[left] * scores[right]))
                pair_users[key] += 1

    by_anchor: defaultdict[int, list[tuple[int, float, int]]] = defaultdict(list)
    for (a, b), score in pair_score.items():
        norm = math.sqrt(float(item_score.get(a, 0.0)) * float(item_score.get(b, 0.0))) + 1e-6
        cooc_score = score / norm
        users = pair_users[(a, b)]
        by_anchor[a].append((b, cooc_score, users))
        by_anchor[b].append((a, cooc_score, users))

    rows = []
    for anchor, related in by_anchor.items():
        related.sort(key=lambda value: value[1], reverse=True)
        for rank, (related_repo, cooc_score, users) in enumerate(related[:related_top_k], start=1):
            rows.append(
                {
                    "window_end_date": pd.Timestamp(window_end_date),
                    "anchor_repo_id": anchor,
                    "related_repo_id": related_repo,
                    "rank": rank,
                    "cooc_score": cooc_score,
                    "cooc_users": users,
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    meta = add_owner_org_columns(metadata).set_index("repo_id")
    owner = meta["owner_name"].to_dict()
    org = meta["org_name"].to_dict()
    language = meta["language"].to_dict()
    topics = meta["topics"].map(_topic_set).to_dict()
    out["same_owner"] = out["anchor_repo_id"].map(owner) == out["related_repo_id"].map(owner)
    out["same_org"] = out["anchor_repo_id"].map(org) == out["related_repo_id"].map(org)
    out["same_language"] = out["anchor_repo_id"].map(language) == out["related_repo_id"].map(language)
    out["topic_overlap"] = [
        len(topics.get(a, set()) & topics.get(b, set()))
        for a, b in zip(out["anchor_repo_id"], out["related_repo_id"], strict=False)
    ]
    return out.sort_values(["anchor_repo_id", "rank"])


def build_experiment_split_mart(
    splits: dict[str, pd.DataFrame],
    split_windows: dict[str, tuple[date, date]],
    experiment_id: str,
) -> pd.DataFrame:
    rows = []
    for split, df in splits.items():
        feedback = build_feedback(df)
        if feedback.empty:
            continue
        feedback = feedback.rename(columns={"score": "weighted_score"})
        start_date, end_date = split_windows[split]
        feedback.insert(0, "split_start_date", pd.Timestamp(start_date))
        feedback.insert(1, "split_end_date", pd.Timestamp(end_date))
        feedback.insert(0, "split", split)
        feedback.insert(0, "experiment_id", experiment_id)
        rows.append(feedback)
    if not rows:
        return pd.DataFrame(
            columns=[
                "experiment_id",
                "split",
                "split_start_date",
                "split_end_date",
                "actor_id",
                "repo_id",
                "weighted_score",
            ]
        )
    return pd.concat(rows, ignore_index=True).sort_values(
        ["experiment_id", "split", "actor_id", "repo_id"]
    )


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"saved {path} rows={len(df):,}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--metadata-db", type=Path, default=DB_PATH)
    parser.add_argument("--output-dir", type=Path, default=MART_DIR)
    parser.add_argument("--history-start", type=parse_date, default=date(2026, 3, 14))
    parser.add_argument("--history-end", type=parse_date, default=date(2026, 4, 24))
    parser.add_argument("--rank-label-start", type=parse_date, default=date(2026, 4, 25))
    parser.add_argument("--rank-label-end", type=parse_date, default=date(2026, 5, 1))
    parser.add_argument("--test-start", type=parse_date, default=date(2026, 5, 2))
    parser.add_argument("--test-end", type=parse_date, default=date(2026, 5, 8))
    parser.add_argument("--recent-days", type=int, default=14)
    parser.add_argument("--related-top-k", type=int, default=50)
    parser.add_argument("--related-max-users", type=int, default=300_000)
    parser.add_argument("--related-max-items-per-user", type=int, default=30)
    parser.add_argument("--experiment-id", type=str, default="week6_20260314_20260508")
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()

    if args.smoke:
        args.output_dir = args.output_dir / "smoke"
        args.history_start = date(2026, 4, 18)
        args.history_end = date(2026, 4, 24)
        args.rank_label_start = date(2026, 4, 25)
        args.rank_label_end = date(2026, 4, 26)
        args.test_start = date(2026, 4, 27)
        args.test_end = date(2026, 4, 28)
        args.related_top_k = 20
        args.related_max_users = 20_000
        args.related_max_items_per_user = 20

    started = time.time()
    print("1. load activity windows")
    history_df = load_daily_activity(args.data_dir, args.history_start, args.history_end)
    rank_df = load_daily_activity(args.data_dir, args.rank_label_start, args.rank_label_end)
    test_df = load_daily_activity(args.data_dir, args.test_start, args.test_end)
    all_df = pd.concat([history_df, rank_df, test_df], ignore_index=True)
    metadata = load_repo_metadata(args.metadata_db)

    print("2. build user-repo interaction mart")
    interaction = build_user_repo_interaction_mart(history_df, args.history_end)
    write_parquet(interaction, args.output_dir / "user_repo_interaction_mart.parquet")

    print("3. build user profile mart")
    profile = build_user_profile_mart(history_df, args.history_end, args.recent_days)
    write_parquet(profile, args.output_dir / "user_profile_mart.parquet")

    print("4. build repo feature mart")
    repo_features = build_repo_feature_mart(
        all_df[all_df["activity_date"] <= pd.Timestamp(args.history_end)],
        metadata,
        args.history_end,
    )
    write_parquet(repo_features, args.output_dir / "repo_feature_mart.parquet")

    print("5. build repo-repo related mart")
    history_fb = build_feedback(history_df)
    related = build_repo_repo_related_mart(
        history_fb,
        metadata,
        args.history_end,
        args.related_max_users,
        args.related_max_items_per_user,
        args.related_top_k,
    )
    write_parquet(related, args.output_dir / "repo_repo_related_mart.parquet")

    print("6. build experiment split mart")
    split = build_experiment_split_mart(
        {"history": history_df, "rank_label": rank_df, "test": test_df},
        {
            "history": (args.history_start, args.history_end),
            "rank_label": (args.rank_label_start, args.rank_label_end),
            "test": (args.test_start, args.test_end),
        },
        args.experiment_id,
    )
    write_parquet(split, args.output_dir / "experiment_split_mart.parquet")
    print(f"elapsed_min={round((time.time() - started) / 60, 2)}")


if __name__ == "__main__":
    main()
