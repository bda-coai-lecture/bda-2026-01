"""Summarize Week 6 per-user recommendation diagnostics.

Usage:
    uv run python scripts/week6_analyze_user_diagnostics.py \
      --suffix smoke_diagnostics
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

MODEL_DIR = Path("data/models/week6")
DEFAULT_SUFFIX = "related80_anchor20_full_als96_i12_lgbm63"


def dominant_event(df: pd.DataFrame) -> pd.Series:
    event_cols = [
        "user_watch_share",
        "user_pr_share",
        "user_fork_share",
        "user_push_share",
        "user_issue_share",
        "user_comment_share",
    ]
    labels = {col: col.replace("user_", "").replace("_share", "") for col in event_cols}
    values = df[event_cols].fillna(0)
    out = values.idxmax(axis=1).map(labels)
    out.loc[values.sum(axis=1) == 0] = "none"
    return out


def segment_table(df: pd.DataFrame, by: str) -> pd.DataFrame:
    out = (
        df.groupby(by, observed=True)
        .agg(
            users=("actor_id", "size"),
            ts_hit10=("ts_hit@10", "mean"),
            als_hit10=("als_hit@10", "mean"),
            ts_ndcg10=("ts_ndcg@10", "mean"),
            als_ndcg10=("als_ndcg@10", "mean"),
            resid_ndcg10=("ts_minus_als_ndcg@10", "mean"),
            ts_recall100=("ts_recall@100", "mean"),
            als_recall100=("als_recall@100", "mean"),
            resid_recall100=("ts_minus_als_recall@100", "mean"),
            top10_als_source=("top10_source_als_share", "mean"),
            top10_recent_source=("top10_source_recent_share", "mean"),
            watch_share=("user_watch_share", "mean"),
            push_share=("user_push_share", "mean"),
            pr_share=("user_pr_share", "mean"),
            recent_score_share=("user_recent_score_share", "mean"),
        )
        .sort_values(["users"], ascending=False)
    )
    if "top10_source_popular_share" in df:
        out["top10_popular_source"] = df.groupby(by, observed=True)[
            "top10_source_popular_share"
        ].mean()
    if "top10_source_related_share" in df:
        out["top10_related_source"] = df.groupby(by, observed=True)[
            "top10_source_related_share"
        ].mean()
    return out


def to_markdown(table: pd.DataFrame) -> str:
    out = table.reset_index()
    out = out.round(6)
    columns = [str(col) for col in out.columns]
    rows = ["| " + " | ".join(columns) + " |"]
    rows.append("| " + " | ".join(["---"] * len(columns)) + " |")
    for row in out.itertuples(index=False):
        rows.append("| " + " | ".join(str(value) for value in row) + " |")
    return "\n".join(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--suffix", default=DEFAULT_SUFFIX)
    parser.add_argument("--output-suffix", default=None)
    args = parser.parse_args()

    diagnostics_path = MODEL_DIR / f"week6_user_diagnostics_{args.suffix}.parquet"
    if not diagnostics_path.exists():
        raise SystemExit(f"missing diagnostics: {diagnostics_path}")

    df = pd.read_parquet(diagnostics_path)
    df["dominant_event"] = dominant_event(df)
    df["activity_bin"] = pd.qcut(
        df["log_user_total_score"].rank(method="first"),
        4,
        labels=["low", "mid_low", "mid_high", "high"],
    )
    df["recent_bin"] = pd.qcut(
        df["user_recent_score_share"].rank(method="first"),
        4,
        labels=["low", "mid_low", "mid_high", "high"],
    )

    output_suffix = args.output_suffix or args.suffix
    out_dir = MODEL_DIR / "diagnostics"
    out_dir.mkdir(parents=True, exist_ok=True)

    tables = {
        "overall": pd.DataFrame(
            [
                {
                    "users": len(df),
                    "ts_hit10": df["ts_hit@10"].mean(),
                    "als_hit10": df["als_hit@10"].mean(),
                    "ts_ndcg10": df["ts_ndcg@10"].mean(),
                    "als_ndcg10": df["als_ndcg@10"].mean(),
                    "resid_ndcg10": df["ts_minus_als_ndcg@10"].mean(),
                    "ts_recall100": df["ts_recall@100"].mean(),
                    "als_recall100": df["als_recall@100"].mean(),
                    "resid_recall100": df["ts_minus_als_recall@100"].mean(),
                }
            ]
        ),
        "by_dominant_event": segment_table(df, "dominant_event"),
        "by_activity_bin": segment_table(df, "activity_bin"),
        "by_recent_bin": segment_table(df, "recent_bin"),
    }

    for name, table in tables.items():
        path = out_dir / f"{output_suffix}_{name}.csv"
        table.to_csv(path)

    markdown_path = out_dir / f"{output_suffix}_summary.md"
    with markdown_path.open("w", encoding="utf-8") as f:
        f.write(f"# Week 6 user diagnostics: `{args.suffix}`\n\n")
        for name, table in tables.items():
            f.write(f"## {name}\n\n")
            f.write(to_markdown(table))
            f.write("\n\n")

    print(f"rows={len(df):,}")
    print(f"saved: {out_dir}")
    print(tables["overall"].round(6).to_string(index=False))


if __name__ == "__main__":
    main()
