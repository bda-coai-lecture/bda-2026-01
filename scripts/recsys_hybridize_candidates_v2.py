"""Build a V2 hybrid candidate cache from ALS plus related/recent/popular sources."""

from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

from recsys_v2_common import (
    SOURCE_CODE,
    SOURCE_HARD,
    SOURCE_POPULAR,
    SOURCE_RELATED,
    Paths,
    load_canonical,
    seen_by_user,
    write_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suffix", default="latest")
    parser.add_argument("--output-suffix", required=True)
    parser.add_argument("--canonical-path", type=Path, default=None)
    parser.add_argument("--candidate-path", type=Path, default=None)
    parser.add_argument("--mart-dir", type=Path, default=Path("data/marts/week6"))
    parser.add_argument("--candidate-k", type=int, default=300)
    parser.add_argument("--als-head", type=int, default=80)
    parser.add_argument("--related-candidate-cap", type=int, default=80)
    parser.add_argument("--related-top-per-anchor", type=int, default=10)
    parser.add_argument("--related-max-seen-anchors", type=int, default=20)
    parser.add_argument("--recent-candidate-cap", type=int, default=20)
    parser.add_argument("--popular-candidate-cap", type=int, default=20)
    return parser.parse_args()


def load_related(path: Path, valid_items: set[int], top_per_anchor: int) -> dict[int, list[tuple[int, float]]]:
    if top_per_anchor <= 0:
        return {}
    frame = pd.read_parquet(
        path,
        columns=["anchor_repo_id", "related_repo_id", "rank", "cooc_score"],
    )
    frame = frame[
        (frame["rank"] <= top_per_anchor)
        & frame["anchor_repo_id"].isin(valid_items)
        & frame["related_repo_id"].isin(valid_items)
    ]
    out: dict[int, list[tuple[int, float]]] = {}
    for anchor, part in frame.sort_values(["anchor_repo_id", "rank"]).groupby("anchor_repo_id", observed=True):
        out[int(anchor)] = [
            (int(row.related_repo_id), float(row.cooc_score))
            for row in part.itertuples(index=False)
        ]
    return out


def load_ranked_items(path: Path, column: str, valid_items: set[int]) -> list[int]:
    frame = pd.read_parquet(path, columns=["repo_id", column])
    frame = frame[frame["repo_id"].isin(valid_items)].copy()
    frame[column] = frame[column].fillna(0).astype(float)
    return frame.sort_values(column, ascending=False)["repo_id"].astype(int).tolist()


def top_history_anchors(history: pd.DataFrame, users: set[int], max_anchors: int) -> dict[int, list[int]]:
    if max_anchors <= 0:
        return {}
    frame = history[history["actor_id"].isin(users)].copy()
    frame = frame.sort_values(["actor_id", "score"], ascending=[True, False])
    return {
        int(uid): [int(repo_id) for repo_id in part["repo_id"].head(max_anchors)]
        for uid, part in frame.groupby("actor_id", observed=True)
    }


def append_candidate(
    out: list[dict[str, Any]],
    used: set[int],
    uid: int,
    repo_id: int,
    score: float,
    source_code: int,
    source_rank: int,
    source_counts: dict[str, int],
    source_name: str,
    candidate_k: int,
) -> bool:
    if len(out) >= candidate_k or repo_id in used:
        return False
    used.add(repo_id)
    out.append(
        {
            "actor_id": uid,
            "repo_id": repo_id,
            "candidate_rank": len(out),
            "raw_candidate_rank": len(out),
            "retrieval_score": score,
            "candidate_source_code": source_code,
            "source_rank": source_rank,
            "source_score": score,
        }
    )
    source_counts[source_name] += 1
    return True


def build_hybrid(args: argparse.Namespace) -> dict[str, Any]:
    base_paths = Paths(args.suffix)
    out_paths = Paths(args.output_suffix)
    canonical_path = args.canonical_path or base_paths.canonical
    candidate_path = args.candidate_path or base_paths.candidates

    history, rank, test = load_canonical(canonical_path)
    candidates = pd.read_parquet(candidate_path)
    if candidates.empty:
        raise RuntimeError(f"candidate parquet is empty: {candidate_path}")

    users = set(candidates["actor_id"].astype(int).unique())
    valid_items = set(candidates["repo_id"].astype(int).unique())
    valid_items.update(history["repo_id"].astype(int).unique())
    valid_items.update(rank["repo_id"].astype(int).unique())
    valid_items.update(test["repo_id"].astype(int).unique())

    history_seen = seen_by_user(history)
    anchors_by_user = top_history_anchors(history, users, args.related_max_seen_anchors)
    related = load_related(
        args.mart_dir / "repo_repo_related_mart.parquet",
        valid_items,
        args.related_top_per_anchor,
    )
    recent = load_ranked_items(args.mart_dir / "repo_feature_mart.parquet", "total_score_7d", valid_items)
    popular = load_ranked_items(args.mart_dir / "repo_feature_mart.parquet", "total_score_42d", valid_items)

    out_rows: list[dict[str, Any]] = []
    source_counts: dict[str, int] = defaultdict(int)
    grouped = candidates.sort_values(["actor_id", "candidate_rank"]).groupby("actor_id", observed=True)
    for uid, part in grouped:
        uid_int = int(uid)
        seen = history_seen.get(uid_int, set())
        used: set[int] = set()
        user_rows: list[dict[str, Any]] = []
        als_pairs = [
            (int(row.repo_id), float(row.retrieval_score))
            for row in part[["repo_id", "retrieval_score"]].itertuples(index=False)
            if int(row.repo_id) not in seen
        ]

        for source_rank, (repo_id, score) in enumerate(als_pairs[: args.als_head], start=1):
            append_candidate(
                user_rows,
                used,
                uid_int,
                repo_id,
                score,
                SOURCE_CODE[SOURCE_HARD],
                source_rank,
                source_counts,
                "als_head",
                args.candidate_k,
            )

        related_scores: dict[int, float] = defaultdict(float)
        for anchor in anchors_by_user.get(uid_int, []):
            for repo_id, score in related.get(anchor, []):
                if repo_id not in seen and repo_id not in used:
                    related_scores[repo_id] += score
        for source_rank, (repo_id, score) in enumerate(
            sorted(related_scores.items(), key=lambda item: item[1], reverse=True)[
                : args.related_candidate_cap
            ],
            start=1,
        ):
            append_candidate(
                user_rows,
                used,
                uid_int,
                repo_id,
                score,
                SOURCE_CODE[SOURCE_RELATED],
                source_rank,
                source_counts,
                "related",
                args.candidate_k,
            )

        recent_added = 0
        for rank_idx, repo_id in enumerate(recent):
            if recent_added >= args.recent_candidate_cap or len(user_rows) >= args.candidate_k:
                break
            if repo_id not in seen and repo_id not in used:
                if append_candidate(
                    user_rows,
                    used,
                    uid_int,
                    repo_id,
                    -0.001 * (rank_idx + 1),
                    SOURCE_CODE[SOURCE_POPULAR],
                    recent_added + 1,
                    source_counts,
                    "recent",
                    args.candidate_k,
                ):
                    recent_added += 1

        popular_added = 0
        for rank_idx, repo_id in enumerate(popular):
            if popular_added >= args.popular_candidate_cap or len(user_rows) >= args.candidate_k:
                break
            if repo_id not in seen and repo_id not in used:
                if append_candidate(
                    user_rows,
                    used,
                    uid_int,
                    repo_id,
                    -0.001 * (rank_idx + 1),
                    SOURCE_CODE[SOURCE_POPULAR],
                    popular_added + 1,
                    source_counts,
                    "popular",
                    args.candidate_k,
                ):
                    popular_added += 1

        for source_rank, (repo_id, score) in enumerate(als_pairs[args.als_head :], start=args.als_head + 1):
            append_candidate(
                user_rows,
                used,
                uid_int,
                repo_id,
                score,
                SOURCE_CODE[SOURCE_HARD],
                source_rank,
                source_counts,
                "als_tail",
                args.candidate_k,
            )
            if len(user_rows) >= args.candidate_k:
                break

        for rank_idx, row in enumerate(user_rows, start=1):
            row["candidate_rank"] = rank_idx
            row["raw_candidate_rank"] = rank_idx
        out_rows.extend(user_rows)

    output = pd.DataFrame(out_rows)
    output = output.astype(
        {
            "actor_id": "int64",
            "repo_id": "int64",
            "candidate_rank": "int32",
            "raw_candidate_rank": "int32",
            "retrieval_score": "float32",
            "candidate_source_code": "int8",
            "source_rank": "int32",
            "source_score": "float32",
        }
    )
    output_path = out_paths.candidates
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output[
        [
            "actor_id",
            "repo_id",
            "candidate_rank",
            "raw_candidate_rank",
            "retrieval_score",
            "candidate_source_code",
            "source_rank",
            "source_score",
        ]
    ].to_parquet(output_path, index=False)

    summary = {
        "suffix": args.output_suffix,
        "base_suffix": args.suffix,
        "canonical_path": str(canonical_path),
        "input_candidate_path": str(candidate_path),
        "output_candidate_path": str(output_path),
        "candidate_rows": int(len(output)),
        "candidate_users": int(output["actor_id"].nunique()),
        "candidate_k": int(args.candidate_k),
        "params": vars(args),
        "source_counts": dict(source_counts),
        "related_anchor_count": int(len(related)),
        "recent_pool_size": int(len(recent)),
        "popular_pool_size": int(len(popular)),
    }
    write_json(out_paths.retrieval_summary, summary)
    return summary


def main() -> None:
    summary = build_hybrid(parse_args())
    print(f"wrote {summary['output_candidate_path']}")
    print(f"wrote {Paths(summary['suffix']).retrieval_summary}")


if __name__ == "__main__":
    main()
