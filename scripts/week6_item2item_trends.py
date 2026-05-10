"""Build item-to-item related repos and trendy repo artifacts for Week 6.

The item-to-item output is item2vec-style co-occurrence, not a neural model:
user histories are treated as short baskets and repo pairs are scored by
weighted co-occurrence normalized by item popularity.
"""

from __future__ import annotations

import argparse
import math
import time
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

from gharchive.loader import load_period

DATA_DIR = Path("data/daily_agg")
MODEL_DIR = Path("data/models/week6")

WEIGHTS = {
    "WatchEvent": 1.0,
    "ForkEvent": 2.0,
    "IssuesEvent": 0.5,
    "PullRequestEvent": 3.0,
    "IssueCommentEvent": 0.3,
    "PushEvent": 0.2,
}


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_feedback(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["score"] = out["type"].map(WEIGHTS).fillna(0) * out["cnt"]
    fb = out.groupby(["actor_id", "repo_id"], observed=True)["score"].sum().reset_index()
    return fb[fb["score"] > 0]


def retain_catalog(feedback: pd.DataFrame, min_item_users: int, max_items: int | None) -> pd.DataFrame:
    item_users = feedback.groupby("repo_id")["actor_id"].nunique()
    keep_items = set(item_users[item_users >= min_item_users].index)
    if max_items:
        keep_items = set(
            feedback[feedback["repo_id"].isin(keep_items)]
            .groupby("repo_id")["score"]
            .sum()
            .sort_values(ascending=False)
            .head(max_items)
            .index
        )
    return feedback[feedback["repo_id"].isin(keep_items)]


def build_trendy_repos(
    history_fb: pd.DataFrame,
    recent_fb: pd.DataFrame,
    prior_fb: pd.DataFrame,
    top_n: int,
) -> pd.DataFrame:
    total_score = history_fb.groupby("repo_id")["score"].sum()
    total_users = history_fb.groupby("repo_id")["actor_id"].nunique()
    recent_score = recent_fb.groupby("repo_id")["score"].sum()
    prior_score = prior_fb.groupby("repo_id")["score"].sum()
    recent_users = recent_fb.groupby("repo_id")["actor_id"].nunique()
    prior_users = prior_fb.groupby("repo_id")["actor_id"].nunique()

    idx = total_score.index
    out = pd.DataFrame(index=idx)
    out["repo_id"] = idx.astype("int64")
    out["total_score"] = total_score.reindex(idx).fillna(0).astype(float).to_numpy()
    out["total_users"] = total_users.reindex(idx).fillna(0).astype(float).to_numpy()
    out["recent_score"] = recent_score.reindex(idx).fillna(0).astype(float).to_numpy()
    out["prior_score"] = prior_score.reindex(idx).fillna(0).astype(float).to_numpy()
    out["recent_users"] = recent_users.reindex(idx).fillna(0).astype(float).to_numpy()
    out["prior_users"] = prior_users.reindex(idx).fillna(0).astype(float).to_numpy()
    out["growth_ratio"] = (out["recent_score"] - out["prior_score"]) / (out["prior_score"] + 1.0)
    out["user_growth_ratio"] = (out["recent_users"] - out["prior_users"]) / (out["prior_users"] + 1.0)
    out["recent_share"] = out["recent_score"] / (out["total_score"] + 1e-6)
    out["trend_score"] = (
        np.log1p(out["recent_score"])
        * np.log1p(out["recent_users"])
        * (1.0 + out["growth_ratio"].clip(lower=-0.5, upper=10.0))
        * (0.5 + out["recent_share"].clip(upper=1.0))
    )
    return out.sort_values("trend_score", ascending=False).head(top_n).reset_index(drop=True)


def build_item2item(
    history_fb: pd.DataFrame,
    max_users: int | None,
    max_items_per_user: int,
    related_top_k: int,
) -> pd.DataFrame:
    item_score = history_fb.groupby("repo_id")["score"].sum().to_dict()
    pair_score: defaultdict[tuple[int, int], float] = defaultdict(float)
    pair_users: Counter[tuple[int, int]] = Counter()

    user_groups = history_fb.sort_values(["actor_id", "score"], ascending=[True, False]).groupby(
        "actor_id", observed=True
    )
    total_users = history_fb["actor_id"].nunique()
    if max_users:
        total_users = min(total_users, max_users)

    for i, (_, rows) in enumerate(tqdm(user_groups, total=total_users, desc="item pairs")):
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
        normalized = score / norm
        users = pair_users[(a, b)]
        by_anchor[a].append((b, normalized, users))
        by_anchor[b].append((a, normalized, users))

    rows = []
    for anchor, related in by_anchor.items():
        related.sort(key=lambda x: x[1], reverse=True)
        for rank, (related_repo, score, users) in enumerate(related[:related_top_k], start=1):
            rows.append(
                {
                    "anchor_repo_id": anchor,
                    "rank": rank,
                    "related_repo_id": related_repo,
                    "score": score,
                    "cooc_users": users,
                    "anchor_score": float(item_score.get(anchor, 0.0)),
                    "related_score": float(item_score.get(related_repo, 0.0)),
                }
            )
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--history-start", type=parse_date, default=date(2026, 3, 14))
    parser.add_argument("--history-end", type=parse_date, default=date(2026, 4, 24))
    parser.add_argument("--recent-days", type=int, default=14)
    parser.add_argument("--min-item-users", type=int, default=3)
    parser.add_argument("--max-items", type=int, default=300_000)
    parser.add_argument("--max-users", type=int, default=300_000)
    parser.add_argument("--max-items-per-user", type=int, default=30)
    parser.add_argument("--related-top-k", type=int, default=50)
    parser.add_argument("--trendy-top-n", type=int, default=5000)
    parser.add_argument("--output-suffix", type=str, default="latest")
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    started = time.time()

    if args.smoke:
        args.max_items = 30_000
        args.max_users = 20_000
        args.max_items_per_user = 20
        args.related_top_k = 20
        args.trendy_top_n = 500
        args.output_suffix = "smoke_item2item"

    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    print("1. load history")
    history_df = load_period(DATA_DIR, args.history_start, args.history_end)
    recent_start = max(args.history_start, args.history_end - timedelta(days=args.recent_days - 1))
    prior_end = recent_start - timedelta(days=1)
    recent_df = load_period(DATA_DIR, recent_start, args.history_end)
    prior_df = (
        load_period(DATA_DIR, args.history_start, prior_end)
        if prior_end >= args.history_start
        else history_df.iloc[0:0].copy()
    )

    print("2. build feedback/catalog")
    history_fb = retain_catalog(build_feedback(history_df), args.min_item_users, args.max_items)
    keep_items = set(history_fb["repo_id"].unique())
    recent_fb = build_feedback(recent_df)
    prior_fb = build_feedback(prior_df) if len(prior_df) else history_fb.iloc[0:0].copy()
    recent_fb = recent_fb[recent_fb["repo_id"].isin(keep_items)]
    prior_fb = prior_fb[prior_fb["repo_id"].isin(keep_items)]
    print(
        f"   interactions={len(history_fb):,}, "
        f"users={history_fb.actor_id.nunique():,}, repos={history_fb.repo_id.nunique():,}"
    )

    print("3. trendy repos")
    trendy = build_trendy_repos(history_fb, recent_fb, prior_fb, args.trendy_top_n)
    trendy_path = MODEL_DIR / f"trendy_repos_{args.output_suffix}.parquet"
    trendy.to_parquet(trendy_path, index=False)

    print("4. item-to-item related repos")
    related = build_item2item(
        history_fb,
        args.max_users,
        args.max_items_per_user,
        args.related_top_k,
    )
    related_path = MODEL_DIR / f"item2item_related_{args.output_suffix}.parquet"
    related.to_parquet(related_path, index=False)

    cases = related[
        related["anchor_repo_id"].isin(trendy["repo_id"].head(200).astype("int64"))
    ].copy()
    cases_path = MODEL_DIR / f"week6_related_cases_{args.output_suffix}.parquet"
    cases.to_parquet(cases_path, index=False)

    print(f"saved: {trendy_path}")
    print(f"saved: {related_path}")
    print(f"saved: {cases_path}")
    print(f"elapsed_min: {round((time.time() - started) / 60, 2)}")


if __name__ == "__main__":
    main()
