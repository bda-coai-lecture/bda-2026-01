"""Build V2 re-rank training rows from canonical positives and sampled negatives."""

from __future__ import annotations

import argparse
import heapq
from collections import Counter
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from recsys_v2_common import (
    FEATURE_COLUMNS,
    SOURCE_CODE,
    SOURCE_HARD,
    SOURCE_POPULAR,
    SOURCE_POSITIVE,
    SOURCE_RANDOM,
    SOURCE_RELATED,
    Paths,
    attach_features,
    empty_feedback,
    ensure_dirs,
    feature_stats,
    load_canonical,
    normalize_feedback,
    popularity_list,
    related_map_from_history,
    seen_by_user,
    write_json,
)

DEFAULT_SOURCE_MIX = {
    SOURCE_HARD: 0.40,
    SOURCE_POPULAR: 0.25,
    SOURCE_RELATED: 0.20,
    SOURCE_RANDOM: 0.15,
}

CANDIDATE_BASE_COLUMNS = ["actor_id", "repo_id", "candidate_rank", "retrieval_score"]
CANDIDATE_OPTIONAL_COLUMNS = ["candidate_source_code", "source_rank", "source_score"]
CANDIDATE_COLUMNS = [*CANDIDATE_BASE_COLUMNS, *CANDIDATE_OPTIONAL_COLUMNS]
CANONICAL_COLUMNS = ["split", "actor_id", "repo_id", "score"]


class RowBuffer:
    def __init__(self) -> None:
        self.actor_id: list[int] = []
        self.repo_id: list[int] = []
        self.label: list[int] = []
        self.source: list[str] = []
        self.rank_label_score: list[float] = []

    def append(
        self,
        actor_id: int,
        repo_id: int,
        label: int,
        source: str,
        rank_label_score: float = 0.0,
    ) -> None:
        self.actor_id.append(actor_id)
        self.repo_id.append(repo_id)
        self.label.append(label)
        self.source.append(source)
        self.rank_label_score.append(rank_label_score)

    def extend_frame(self, frame: pd.DataFrame) -> None:
        if frame.empty:
            return
        self.actor_id.extend(frame["actor_id"].astype("int64").tolist())
        self.repo_id.extend(frame["repo_id"].astype("int64").tolist())
        self.label.extend(frame["label"].astype("int8").tolist())
        self.source.extend(frame["source"].astype(str).tolist())
        self.rank_label_score.extend(frame["rank_label_score"].astype("float32").tolist())

    def __len__(self) -> int:
        return len(self.actor_id)

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "actor_id": np.asarray(self.actor_id, dtype=np.int64),
                "repo_id": np.asarray(self.repo_id, dtype=np.int64),
                "label": np.asarray(self.label, dtype=np.int8),
                "source": self.source,
                "rank_label_score": np.asarray(self.rank_label_score, dtype=np.float32),
            }
        )


class StreamingParquetWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.writer: pq.ParquetWriter | None = None

    def write_frame(self, frame: pd.DataFrame) -> None:
        table = pa.Table.from_pandas(frame, preserve_index=False)
        if self.writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.writer = pq.ParquetWriter(self.path, table.schema, compression="snappy")
        self.writer.write_table(table)

    def close(self) -> None:
        if self.writer is not None:
            self.writer.close()
            self.writer = None


def parse_mix(raw: str) -> dict[str, float]:
    aliases = {
        "hard": SOURCE_HARD,
        "retrieval_hard": SOURCE_HARD,
        "popular": SOURCE_POPULAR,
        "recent": SOURCE_POPULAR,
        "popular_recent": SOURCE_POPULAR,
        "related": SOURCE_RELATED,
        "source": SOURCE_RELATED,
        "related_source": SOURCE_RELATED,
        "random": SOURCE_RANDOM,
        "random_catalog": SOURCE_RANDOM,
    }
    out: dict[str, float] = {}
    for part in raw.split(","):
        if not part.strip():
            continue
        key, value = part.split("=", 1)
        source = aliases.get(key.strip())
        if source is None:
            raise argparse.ArgumentTypeError(f"unknown negative source in mix: {key}")
        out[source] = float(value)
    missing = set(DEFAULT_SOURCE_MIX) - set(out)
    if missing:
        raise argparse.ArgumentTypeError(f"negative mix missing sources: {sorted(missing)}")
    total = sum(out.values())
    if total <= 0:
        raise argparse.ArgumentTypeError("negative mix total must be positive")
    return {source: weight / total for source, weight in out.items()}


def per_source_targets(total: int, mix: dict[str, float]) -> dict[str, int]:
    exact = {source: total * weight for source, weight in mix.items()}
    targets = {source: int(np.floor(value)) for source, value in exact.items()}
    remainder = total - sum(targets.values())
    for source, _ in sorted(
        exact.items(), key=lambda kv: kv[1] - np.floor(kv[1]), reverse=True
    )[:remainder]:
        targets[source] += 1
    return targets


def effective_negative_mix(mix: dict[str, float], disable_related: bool) -> dict[str, float]:
    if not disable_related:
        return dict(mix)
    out = {source: weight for source, weight in mix.items() if source != SOURCE_RELATED}
    total = sum(out.values())
    if total <= 0:
        raise ValueError(
            "--disable-related-negatives requires non-related negative mix weight"
        )
    return {source: weight / total for source, weight in out.items()}


def resolve_io_paths(args: argparse.Namespace) -> tuple[Path, Path, Path, Path, str]:
    input_paths = Paths(args.suffix)
    output_suffix = args.output_suffix or args.suffix
    output_paths = Paths(output_suffix)
    canonical_path = Path(args.canonical_path) if args.canonical_path else input_paths.canonical
    candidate_path = Path(args.candidate_path) if args.candidate_path else input_paths.candidates
    return (
        canonical_path,
        candidate_path,
        output_paths.rerank_train,
        output_paths.rerank_summary,
        output_suffix,
    )


def iter_candidate_batches(path: Path, batch_size: int) -> Iterator[pd.DataFrame]:
    if not path.exists():
        raise FileNotFoundError(f"retrieval candidate cache not found: {path}")
    parquet = pq.ParquetFile(path)
    available_columns = set(parquet.schema_arrow.names)
    columns = [name for name in CANDIDATE_COLUMNS if name in available_columns]
    missing_base = sorted(set(CANDIDATE_BASE_COLUMNS) - available_columns)
    if missing_base:
        raise RuntimeError(f"candidate parquet missing required columns: {missing_base}")
    for batch in parquet.iter_batches(batch_size=batch_size, columns=columns):
        frame = batch.to_pandas()
        frame = frame.dropna(subset=["actor_id", "repo_id"])
        if frame.empty:
            continue
        for name in CANDIDATE_OPTIONAL_COLUMNS:
            if name not in frame.columns:
                if name == "candidate_source_code":
                    frame[name] = SOURCE_CODE[SOURCE_HARD]
                elif name == "source_rank":
                    frame[name] = frame["candidate_rank"]
                else:
                    frame[name] = frame["retrieval_score"]
        yield frame[CANDIDATE_COLUMNS].astype(
            {
                "actor_id": "int64",
                "repo_id": "int64",
                "candidate_rank": "float32",
                "retrieval_score": "float32",
                "candidate_source_code": "float32",
                "source_rank": "float32",
                "source_score": "float32",
            }
        )


def iter_canonical_batches(path: Path, batch_size: int) -> Iterator[pd.DataFrame]:
    if not path.exists():
        raise FileNotFoundError(f"canonical dataset not found: {path}")
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(batch_size=batch_size, columns=CANONICAL_COLUMNS):
        frame = batch.to_pandas()
        if frame.empty:
            continue
        yield frame


def load_split_from_canonical(
    path: Path,
    split: str,
    batch_size: int,
    actor_ids: set[int] | None = None,
) -> tuple[pd.DataFrame, dict[str, int]]:
    parts: list[pd.DataFrame] = []
    split_counts: Counter[str] = Counter()
    for batch in iter_canonical_batches(path, batch_size):
        split_values = batch["split"].astype(str)
        split_counts.update(split_values.value_counts().to_dict())
        mask = split_values == split
        if actor_ids is not None:
            mask &= batch["actor_id"].isin(actor_ids)
        if mask.any():
            parts.append(batch.loc[mask, ["actor_id", "repo_id", "score"]])

    if not parts:
        return empty_feedback(), dict(split_counts)
    return normalize_feedback(pd.concat(parts, ignore_index=True)), dict(split_counts)


def limited_canonical_requested(args: argparse.Namespace) -> bool:
    return args.max_train_users is not None or args.max_train_positives is not None


def unique_ordered(values: list[int], blocked: set[int]) -> list[int]:
    seen: set[int] = set()
    out: list[int] = []
    for value in values:
        item = int(value)
        if item in blocked or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def sample_from_pool(
    pool: list[int],
    count: int,
    blocked: set[int],
    rng: np.random.Generator,
) -> list[int]:
    if count <= 0 or not pool:
        return []
    if len(pool) <= max(count * 20, 1_000):
        choices = unique_ordered(pool, blocked)
        if len(choices) <= count:
            return choices
        idx = rng.choice(len(choices), size=count, replace=False)
        return [choices[int(i)] for i in idx]

    selected: list[int] = []
    seen: set[int] = set()
    attempts = 0
    max_attempts = max(count * 100, 1_000)
    while len(selected) < count and attempts < max_attempts:
        attempts += 1
        item = int(pool[int(rng.integers(0, len(pool)))])
        if item in blocked or item in seen:
            continue
        seen.add(item)
        selected.append(item)

    if len(selected) >= count:
        return selected

    for raw in pool:
        item = int(raw)
        if item in blocked or item in seen:
            continue
        seen.add(item)
        selected.append(item)
        if len(selected) >= count:
            break
    return selected


def related_pool_for_user(
    history_seen: dict[int, set[int]],
    related_by_item: dict[int, list[int]],
    max_seen_anchors: int,
) -> dict[int, list[int]]:
    pools: dict[int, list[int]] = {}
    for uid, seen_items in history_seen.items():
        rows: list[int] = []
        for anchor in list(seen_items)[:max_seen_anchors]:
            rows.extend(related_by_item.get(int(anchor), []))
        pools[int(uid)] = rows
    return pools


def make_positive_rows(rank: pd.DataFrame) -> pd.DataFrame:
    rows = rank[["actor_id", "repo_id", "score"]].copy()
    rows["actor_id"] = rows["actor_id"].astype("int64")
    rows["repo_id"] = rows["repo_id"].astype("int64")
    rows["label"] = np.int8(1)
    rows["source"] = SOURCE_POSITIVE
    rows["rank_label_score"] = rows["score"].astype("float32")
    return rows.drop(columns=["score"])


def select_train_rank(
    rank: pd.DataFrame,
    max_train_users: int | None,
    max_train_positives: int | None,
) -> tuple[pd.DataFrame, pd.DataFrame, int, int, int]:
    rank_users = np.sort(rank["actor_id"].astype("int64").unique())
    rank_users_before_limit = int(len(rank_users))
    if max_train_users is not None and max_train_users < 0:
        raise ValueError("--max-train-users must be non-negative")
    if max_train_positives is not None and max_train_positives < 0:
        raise ValueError("--max-train-positives must be non-negative")

    if max_train_users is None:
        selected_rank = rank.copy()
        selected_train_users = rank_users_before_limit
    else:
        selected_users = set(map(int, rank_users[:max_train_users]))
        selected_rank = rank[rank["actor_id"].isin(selected_users)].copy()
        selected_train_users = int(len(selected_users))

    train_rank = selected_rank.sort_values(["actor_id", "repo_id"], ascending=[True, True])
    if max_train_positives is not None:
        train_rank = train_rank.head(max_train_positives).copy()
    else:
        train_rank = train_rank.copy()
    return (
        selected_rank,
        train_rank,
        rank_users_before_limit,
        selected_train_users,
        int(len(selected_rank)),
    )


def select_limited_train_rank(
    rank: pd.DataFrame,
    max_train_users: int | None,
    max_train_positives: int | None,
) -> tuple[pd.DataFrame, pd.DataFrame, int, int, int]:
    rank_users = np.sort(rank["actor_id"].astype("int64").unique())
    rank_users_before_limit = int(len(rank_users))
    if max_train_users is not None and max_train_users < 0:
        raise ValueError("--max-train-users must be non-negative")
    if max_train_positives is not None and max_train_positives < 0:
        raise ValueError("--max-train-positives must be non-negative")

    if max_train_users is not None:
        selected_user_set = set(map(int, rank_users[:max_train_users]))
        user_limited_rank = rank[rank["actor_id"].isin(selected_user_set)].copy()
    else:
        user_limited_rank = rank.copy()

    ordered_rank = user_limited_rank.sort_values(
        ["actor_id", "repo_id"], ascending=[True, True]
    )
    if max_train_positives is not None:
        train_rank = ordered_rank.head(max_train_positives).copy()
    else:
        train_rank = ordered_rank.copy()

    selected_user_set = set(map(int, train_rank["actor_id"].astype("int64").unique()))
    rank_for_exclusion = rank[rank["actor_id"].isin(selected_user_set)].copy()
    selected_train_users = int(len(selected_user_set))
    return (
        rank_for_exclusion,
        train_rank,
        rank_users_before_limit,
        selected_train_users,
        int(len(rank_for_exclusion)),
    )


def load_limited_canonical(
    canonical_path: Path,
    batch_size: int,
    max_train_users: int | None,
    max_train_positives: int | None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    print("load rank labels", flush=True)
    rank, split_counts = load_split_from_canonical(
        canonical_path,
        split="rank_label",
        batch_size=batch_size,
    )
    (
        rank_for_exclusion,
        train_rank,
        rank_users_before_limit,
        selected_train_users,
        rank_positives_after_user_limit,
    ) = select_limited_train_rank(rank, max_train_users, max_train_positives)

    selected_users = set(map(int, rank_for_exclusion["actor_id"].astype("int64").unique()))
    print("load selected history", flush=True)
    history, history_split_counts = load_split_from_canonical(
        canonical_path,
        split="history",
        batch_size=batch_size,
        actor_ids=selected_users,
    )
    split_counts.update(history_split_counts)

    meta = {
        "canonical_load_mode": "limited_batch",
        "feature_scope": "selected_train_subset",
        "test_positive_pairs_available": int(split_counts.get("test", 0)),
        "rank_users_before_limit": rank_users_before_limit,
        "selected_train_users": selected_train_users,
        "rank_label_positive_pairs_after_user_limit": rank_positives_after_user_limit,
    }
    return history, rank_for_exclusion, train_rank, meta


def attach_candidate_metadata(
    rows: pd.DataFrame,
    candidate_path: Path,
    batch_size: int,
) -> pd.DataFrame:
    if rows.empty:
        out = rows.copy()
        out["retrieval_score"] = pd.Series(dtype="float32")
        out["candidate_rank"] = pd.Series(dtype="float32")
        out["candidate_source_code"] = pd.Series(dtype="float32")
        out["source_rank"] = pd.Series(dtype="float32")
        out["source_score"] = pd.Series(dtype="float32")
        return out

    row_pairs = set(map(tuple, rows[["actor_id", "repo_id"]].astype(int).to_numpy()))
    pair_users = set(rows["actor_id"].astype(int).unique())
    matched_parts: list[pd.DataFrame] = []

    for batch in iter_candidate_batches(candidate_path, batch_size=batch_size):
        batch = batch[batch["actor_id"].isin(pair_users)]
        if batch.empty:
            continue
        pairs = zip(batch["actor_id"].to_numpy(), batch["repo_id"].to_numpy())
        mask = np.fromiter(
            ((int(uid), int(repo_id)) in row_pairs for uid, repo_id in pairs),
            dtype=bool,
            count=len(batch),
        )
        if mask.any():
            matched_parts.append(batch.loc[mask, CANDIDATE_COLUMNS])

    if matched_parts:
        matched = pd.concat(matched_parts, ignore_index=True)
        matched = matched.sort_values(["actor_id", "repo_id", "candidate_rank"])
        matched = matched.drop_duplicates(["actor_id", "repo_id"], keep="first")
    else:
        matched = pd.DataFrame(columns=CANDIDATE_COLUMNS)

    out = rows.merge(matched, on=["actor_id", "repo_id"], how="left")
    out["retrieval_score"] = out["retrieval_score"].fillna(0).astype("float32")
    out["candidate_rank"] = out["candidate_rank"].fillna(0).astype("float32")
    fallback_source_code = out["source"].map(SOURCE_CODE).fillna(SOURCE_CODE[SOURCE_HARD])
    fallback_source_code = fallback_source_code.mask(
        out["source"] == SOURCE_POSITIVE,
        SOURCE_CODE[SOURCE_HARD],
    )
    out["candidate_source_code"] = out["candidate_source_code"].fillna(fallback_source_code).astype("float32")
    out["source_rank"] = out["source_rank"].fillna(0).astype("float32")
    out["source_score"] = out["source_score"].fillna(0).astype("float32")
    return out


def make_hard_negative_selections_from_candidates(
    candidate_path: Path,
    targets_by_user: dict[int, int],
    history_seen: dict[int, set[int]],
    train_positive: dict[int, set[int]],
    selected_by_user: dict[int, set[int]],
    max_hard_candidates_per_user: int,
    batch_size: int,
    max_catalog_items: int | None,
    metadata_pairs: set[tuple[int, int]] | None = None,
) -> tuple[
    dict[int, list[tuple[int, float, float, float, float, float]]],
    dict[tuple[int, int], tuple[float, float, float, float, float]],
    int,
    set[int],
]:
    selected_rows: dict[int, list[tuple[int, float, float, float, float, float]]] = {}
    selected_metadata: dict[tuple[int, int], tuple[float, float, float, float, float]] = {}
    candidate_catalog: set[int] = set()
    if not targets_by_user:
        return selected_rows, selected_metadata, 0, candidate_catalog

    print("scan candidates", flush=True)
    hard_users = set(targets_by_user)
    metadata_users = {uid for uid, _ in metadata_pairs} if metadata_pairs else set()
    effective_limits = {
        uid: min(max_hard_candidates_per_user, max(target * 3, target))
        for uid, target in targets_by_user.items()
    }
    heaps: dict[int, list[tuple[float, int, float, float, float, float]]] = {}

    for batch in iter_candidate_batches(candidate_path, batch_size=batch_size):
        if max_catalog_items is None:
            candidate_catalog.update(map(int, batch["repo_id"].unique()))
        batch = batch[batch["actor_id"].isin(hard_users | metadata_users)]
        if batch.empty:
            continue
        for row in batch.itertuples(index=False):
            uid = int(row.actor_id)
            repo_id = int(row.repo_id)
            pair = (uid, repo_id)
            rank = float(row.candidate_rank)
            score = float(row.retrieval_score)
            source_code = float(row.candidate_source_code)
            source_rank = float(row.source_rank)
            source_score = float(row.source_score)
            if metadata_pairs and pair in metadata_pairs and pair not in selected_metadata:
                selected_metadata[pair] = (score, rank, source_code, source_rank, source_score)
            if uid not in hard_users:
                continue
            if repo_id in history_seen.get(uid, set()) or repo_id in train_positive.get(uid, set()):
                continue
            limit = effective_limits[uid]
            if limit <= 0:
                continue
            heap = heaps.setdefault(uid, [])
            entry = (-rank, repo_id, score, source_code, source_rank, source_score)
            if len(heap) < limit:
                heapq.heappush(heap, entry)
            elif rank < -heap[0][0]:
                heapq.heapreplace(heap, entry)

    shortfall = 0
    for uid, target in targets_by_user.items():
        blocked = history_seen.get(uid, set()) | train_positive.get(uid, set()) | selected_by_user[uid]
        selected = 0
        seen_repos: set[int] = set()
        entries = sorted(heaps.get(uid, []), key=lambda item: (-item[0], item[1]))
        for neg_rank, repo_id, score, source_code, source_rank, source_score in entries:
            repo_id = int(repo_id)
            if repo_id in blocked or repo_id in seen_repos:
                continue
            seen_repos.add(repo_id)
            blocked.add(repo_id)
            selected_by_user[uid].add(repo_id)
            selected_rows.setdefault(uid, []).append(
                (
                    repo_id,
                    float(score),
                    float(-neg_rank),
                    float(source_code),
                    float(source_rank),
                    float(source_score),
                )
            )
            selected += 1
            if selected >= target:
                break
        if selected < target:
            shortfall += target - selected

    return selected_rows, selected_metadata, shortfall, candidate_catalog


def make_hard_negative_rows_from_candidates(
    candidate_path: Path,
    targets_by_user: dict[int, int],
    history_seen: dict[int, set[int]],
    train_positive: dict[int, set[int]],
    selected_by_user: dict[int, set[int]],
    max_hard_candidates_per_user: int,
    batch_size: int,
) -> tuple[pd.DataFrame, int, set[int]]:
    hard_by_user, _, shortfall, candidate_catalog = make_hard_negative_selections_from_candidates(
        candidate_path=candidate_path,
        targets_by_user=targets_by_user,
        history_seen=history_seen,
        train_positive=train_positive,
        selected_by_user=selected_by_user,
        max_hard_candidates_per_user=max_hard_candidates_per_user,
        batch_size=batch_size,
        max_catalog_items=None,
        metadata_pairs=None,
    )
    rows = RowBuffer()
    for uid in sorted(hard_by_user):
        for repo_id, *_ in hard_by_user[uid]:
            rows.append(uid, repo_id, 0, SOURCE_HARD)
    return rows.to_frame(), shortfall, candidate_catalog


def make_negative_rows(
    positives: pd.DataFrame,
    history: pd.DataFrame,
    rank: pd.DataFrame,
    candidate_path: Path,
    negatives_per_positive: int,
    mix: dict[str, float],
    seed: int,
    max_seen_anchors: int,
    max_hard_candidates_per_user: int,
    candidate_batch_size: int,
    disable_related_negatives: bool,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    rng = np.random.default_rng(seed)
    effective_mix = effective_negative_mix(mix, disable_related_negatives)
    rank_users = set(map(int, positives["actor_id"].unique()))
    print("build history_seen", flush=True)
    history_for_rank_users = history[history["actor_id"].isin(rank_users)].copy()
    history_seen = seen_by_user(history_for_rank_users)
    train_positive = seen_by_user(rank)
    if disable_related_negatives:
        print("skip related", flush=True)
        related_by_user: dict[int, list[int]] = {}
    else:
        print("build related", flush=True)
        related_by_user = related_pool_for_user(
            history_seen,
            related_map_from_history(
                history_for_rank_users,
                max_anchors_per_user=max_seen_anchors,
            ),
            max_seen_anchors=max_seen_anchors,
        )
    popular_pool = popularity_list(pd.concat([history, rank], ignore_index=True))
    catalog_base = set(history["repo_id"].astype(int)) | set(rank["repo_id"].astype(int))

    rows = RowBuffer()
    requested = 0
    shortfall = Counter()
    source_counts = Counter()
    positives_by_user = positives.groupby("actor_id", observed=True)["repo_id"].count()
    targets_by_user: dict[int, dict[str, int]] = {}
    selected_by_user: dict[int, set[int]] = {}

    for uid, positive_count in positives_by_user.items():
        uid = int(uid)
        target_total = int(positive_count) * negatives_per_positive
        targets = per_source_targets(target_total, effective_mix)
        targets_by_user[uid] = targets
        selected_by_user[uid] = set()
        requested += sum(targets.values())

    hard_targets = {
        uid: targets.get(SOURCE_HARD, 0)
        for uid, targets in targets_by_user.items()
        if targets.get(SOURCE_HARD, 0) > 0
    }
    hard_rows, hard_shortfall, candidate_catalog = make_hard_negative_rows_from_candidates(
        candidate_path=candidate_path,
        targets_by_user=hard_targets,
        history_seen=history_seen,
        train_positive=train_positive,
        selected_by_user=selected_by_user,
        max_hard_candidates_per_user=max_hard_candidates_per_user,
        batch_size=candidate_batch_size,
    )
    catalog = sorted(catalog_base | candidate_catalog)
    rows.extend_frame(hard_rows)
    source_counts[SOURCE_HARD] += len(hard_rows)
    if hard_shortfall:
        shortfall[SOURCE_HARD] += hard_shortfall

    print("build negatives", flush=True)
    for uid in targets_by_user:
        targets = targets_by_user[uid]
        blocked = set(map(int, history_seen.get(uid, set()))) | set(
            map(int, train_positive.get(uid, set()))
        )
        selected = selected_by_user[uid]
        source_pools = {
            SOURCE_POPULAR: popular_pool,
            SOURCE_RANDOM: catalog,
        }
        if not disable_related_negatives:
            source_pools[SOURCE_RELATED] = related_by_user.get(uid, [])

        for source, count in targets.items():
            if source == SOURCE_HARD:
                continue
            if count <= 0:
                continue
            current_blocked = blocked | selected
            sampled = sample_from_pool(source_pools[source], count, current_blocked, rng)
            if len(sampled) < count:
                shortfall[source] += count - len(sampled)
            for repo_id in sampled:
                selected.add(repo_id)
                rows.append(uid, int(repo_id), 0, source)
                source_counts[source] += 1

    negative_frame = rows.to_frame()
    summary = {
        "negative_semantics": "sampled unlabeled negatives",
        "negative_source_mix": effective_mix,
        "negative_source_mix_requested": mix,
        "related_negatives_disabled": disable_related_negatives,
        "negative_source_counts": dict(source_counts),
        "negative_source_shortfall": dict(shortfall),
        "negative_requested": requested,
        "negative_count": len(negative_frame),
        "max_hard_candidates_per_user": max_hard_candidates_per_user,
        "candidate_batch_size": candidate_batch_size,
    }
    return negative_frame, summary


def build_streaming_training_rows(
    train_rank: pd.DataFrame,
    history: pd.DataFrame,
    rank_for_exclusion: pd.DataFrame,
    candidate_path: Path,
    output_path: Path,
    negatives_per_positive: int,
    mix: dict[str, float],
    seed: int,
    max_seen_anchors: int,
    max_hard_candidates_per_user: int,
    candidate_batch_size: int,
    disable_related_negatives: bool,
    max_catalog_items: int | None,
    write_batch_users: int,
    max_rows_per_group: int,
) -> dict[str, Any]:
    if write_batch_users <= 0:
        raise ValueError("--write-batch-users must be positive")
    if max_rows_per_group <= 1:
        raise ValueError("--max-rows-per-group must be greater than 1")
    rng = np.random.default_rng(seed)
    effective_mix = effective_negative_mix(mix, disable_related_negatives)
    positives = make_positive_rows(train_rank)
    rank_users = set(map(int, positives["actor_id"].unique()))

    print("build history_seen", flush=True)
    history_for_rank_users = history[history["actor_id"].isin(rank_users)].copy()
    history_seen = seen_by_user(history_for_rank_users)
    train_positive = seen_by_user(rank_for_exclusion)
    if disable_related_negatives:
        print("skip related", flush=True)
        related_by_user: dict[int, list[int]] = {}
    else:
        print("build related", flush=True)
        related_by_user = related_pool_for_user(
            history_seen,
            related_map_from_history(
                history_for_rank_users,
                max_anchors_per_user=max_seen_anchors,
            ),
            max_seen_anchors=max_seen_anchors,
        )

    print("build item pools", flush=True)
    popular_pool = popularity_list(pd.concat([history, rank_for_exclusion], ignore_index=True))
    if max_catalog_items is not None:
        if max_catalog_items <= 0:
            raise ValueError("--max-catalog-items must be positive")
        popular_pool = popular_pool[:max_catalog_items]
        catalog_base = set(popular_pool)
    else:
        catalog_base = set(history["repo_id"].astype(int)) | set(
            rank_for_exclusion["repo_id"].astype(int)
        )

    positives_by_user = positives.groupby("actor_id", observed=True)["repo_id"].count()
    targets_by_user: dict[int, dict[str, int]] = {}
    selected_by_user: dict[int, set[int]] = {}
    requested = 0
    for uid, positive_count in positives_by_user.items():
        uid = int(uid)
        target_total = int(positive_count) * negatives_per_positive
        targets = per_source_targets(target_total, effective_mix)
        targets_by_user[uid] = targets
        selected_by_user[uid] = set()
        requested += sum(targets.values())

    hard_targets = {
        uid: targets.get(SOURCE_HARD, 0)
        for uid, targets in targets_by_user.items()
        if targets.get(SOURCE_HARD, 0) > 0
    }
    positive_pairs_for_metadata = set(
        map(tuple, positives[["actor_id", "repo_id"]].astype(int).to_numpy())
    )
    hard_by_user, candidate_metadata, hard_shortfall, candidate_catalog = make_hard_negative_selections_from_candidates(
        candidate_path=candidate_path,
        targets_by_user=hard_targets,
        history_seen=history_seen,
        train_positive=train_positive,
        selected_by_user=selected_by_user,
        max_hard_candidates_per_user=max_hard_candidates_per_user,
        batch_size=candidate_batch_size,
        max_catalog_items=max_catalog_items,
        metadata_pairs=positive_pairs_for_metadata,
    )
    catalog = sorted(catalog_base | candidate_catalog)

    stats = feature_stats(history, rank_for_exclusion)
    positives_by_user_frame = {
        int(uid): part
        for uid, part in positives.sort_values(["actor_id", "repo_id"]).groupby(
            "actor_id", observed=True
        )
    }
    users = sorted(positives_by_user_frame)
    source_counts = Counter()
    shortfall = Counter()
    if hard_shortfall:
        shortfall[SOURCE_HARD] += hard_shortfall
    positive_count = 0
    negative_count = 0
    rows_written = 0
    group_index = 0
    split_group_count = 0
    writer = StreamingParquetWriter(output_path)
    pending: list[pd.DataFrame] = []

    def flush_pending() -> None:
        nonlocal rows_written
        if not pending:
            return
        frame = pd.concat(pending, ignore_index=True)
        pending.clear()
        frame = attach_features(frame, stats)
        for col in FEATURE_COLUMNS:
            if col not in frame.columns:
                frame[col] = 0.0
        frame["label"] = frame["label"].astype("int8")
        frame["group_index"] = frame["group_index"].astype("int32")
        writer.write_frame(frame)
        rows_written += int(len(frame))

    print("write training rows", flush=True)
    try:
        for offset in range(0, len(users), write_batch_users):
            for uid in users[offset : offset + write_batch_users]:
                user_rows: list[dict[str, Any]] = []
                positive_part = positives_by_user_frame[uid]
                for row in positive_part.itertuples(index=False):
                    (
                        retrieval_score,
                        candidate_rank,
                        candidate_source_code,
                        source_rank,
                        source_score,
                    ) = candidate_metadata.get(
                        (uid, int(row.repo_id)),
                        (0.0, 0.0, float(SOURCE_CODE[SOURCE_HARD]), 0.0, 0.0),
                    )
                    user_rows.append(
                        {
                            "actor_id": uid,
                            "repo_id": int(row.repo_id),
                            "label": 1,
                            "source": SOURCE_POSITIVE,
                            "rank_label_score": float(row.rank_label_score),
                            "retrieval_score": float(retrieval_score),
                            "candidate_rank": float(candidate_rank),
                            "candidate_source_code": float(candidate_source_code),
                            "source_rank": float(source_rank),
                            "source_score": float(source_score),
                            "group_index": group_index,
                        }
                    )
                positive_count += int(len(positive_part))

                targets = targets_by_user[uid]
                selected = selected_by_user[uid]
                blocked = set(map(int, history_seen.get(uid, set()))) | set(
                    map(int, train_positive.get(uid, set()))
                )
                for (
                    repo_id,
                    retrieval_score,
                    candidate_rank,
                    candidate_source_code,
                    source_rank,
                    source_score,
                ) in hard_by_user.get(uid, []):
                    user_rows.append(
                        {
                            "actor_id": uid,
                            "repo_id": int(repo_id),
                            "label": 0,
                            "source": SOURCE_HARD,
                            "rank_label_score": 0.0,
                            "retrieval_score": float(retrieval_score),
                            "candidate_rank": float(candidate_rank),
                            "candidate_source_code": float(candidate_source_code),
                            "source_rank": float(source_rank),
                            "source_score": float(source_score),
                            "group_index": group_index,
                        }
                    )
                    source_counts[SOURCE_HARD] += 1
                    negative_count += 1

                source_pools = {
                    SOURCE_POPULAR: popular_pool,
                    SOURCE_RANDOM: catalog,
                }
                if not disable_related_negatives:
                    source_pools[SOURCE_RELATED] = related_by_user.get(uid, [])

                for source, count in targets.items():
                    if source == SOURCE_HARD or count <= 0:
                        continue
                    sampled = sample_from_pool(source_pools[source], count, blocked | selected, rng)
                    if len(sampled) < count:
                        shortfall[source] += count - len(sampled)
                    for repo_id in sampled:
                        selected.add(int(repo_id))
                        user_rows.append(
                            {
                                "actor_id": uid,
                                "repo_id": int(repo_id),
                                "label": 0,
                                "source": source,
                                "rank_label_score": 0.0,
                                "retrieval_score": 0.0,
                                "candidate_rank": 0.0,
                                "candidate_source_code": float(SOURCE_CODE[source]),
                                "source_rank": 0.0,
                                "source_score": 0.0,
                                "group_index": group_index,
                            }
                        )
                        source_counts[source] += 1
                        negative_count += 1

                user_frame = pd.DataFrame(user_rows)
                user_frame = user_frame.sort_values(
                    ["actor_id", "label", "source", "repo_id"],
                    ascending=[True, False, True, True],
                )
                if len(user_frame) <= max_rows_per_group:
                    user_frame["group_index"] = group_index
                    pending.append(user_frame)
                    group_index += 1
                    continue

                pos_frame = user_frame[user_frame["label"] == 1]
                neg_frame = user_frame[user_frame["label"] == 0]
                pos_chunk_size = max(1, max_rows_per_group // max(1, negatives_per_positive + 1))
                pos_chunks = [
                    pos_frame.iloc[start : start + pos_chunk_size]
                    for start in range(0, len(pos_frame), pos_chunk_size)
                ]
                if pos_chunks:
                    neg_boundaries = np.linspace(
                        0,
                        len(neg_frame),
                        num=len(pos_chunks) + 1,
                        dtype=int,
                    )
                    neg_splits = [
                        neg_frame.iloc[neg_boundaries[i] : neg_boundaries[i + 1]]
                        for i in range(len(pos_chunks))
                    ]
                else:
                    neg_splits = []
                for pos_chunk, neg_chunk in zip(pos_chunks, neg_splits, strict=False):
                    group_frame = pd.concat([pos_chunk, neg_chunk], ignore_index=True)
                    if len(group_frame) > max_rows_per_group:
                        raise RuntimeError(
                            f"group split still exceeds --max-rows-per-group for actor_id={uid}"
                        )
                    group_frame = group_frame.sort_values(
                        ["actor_id", "label", "source", "repo_id"],
                        ascending=[True, False, True, True],
                    )
                    group_frame["group_index"] = group_index
                    pending.append(group_frame)
                    group_index += 1
                    split_group_count += 1
            flush_pending()
    finally:
        writer.close()

    if rows_written == 0:
        raise RuntimeError("No rows were written to rerank training parquet.")

    return {
        "negative_semantics": "sampled unlabeled negatives",
        "negative_source_mix": effective_mix,
        "negative_source_mix_requested": mix,
        "related_negatives_disabled": disable_related_negatives,
        "negative_source_counts": dict(source_counts),
        "negative_source_shortfall": dict(shortfall),
        "negative_requested": requested,
        "negative_count": negative_count,
        "positive_count": positive_count,
        "rows": rows_written,
        "groups": group_index,
        "max_hard_candidates_per_user": max_hard_candidates_per_user,
        "candidate_batch_size": candidate_batch_size,
        "write_batch_users": write_batch_users,
        "max_rows_per_group": max_rows_per_group,
        "split_group_count": split_group_count,
        "max_catalog_items": max_catalog_items,
        "candidate_metadata_mode": "positive_and_hard_from_single_candidate_scan",
        "stream_write": True,
        "negative_intersects_history_seen": 0,
        "negative_intersects_rank_label_positive": 0,
    }


def build_rerank_data(args: argparse.Namespace) -> dict[str, Any]:
    ensure_dirs()
    (
        canonical_path,
        candidate_path,
        output_path,
        summary_path,
        output_suffix,
    ) = resolve_io_paths(args)
    if limited_canonical_requested(args):
        (
            history,
            rank_for_exclusion,
            train_rank,
            canonical_meta,
        ) = load_limited_canonical(
            canonical_path=canonical_path,
            batch_size=args.canonical_batch_size,
            max_train_users=args.max_train_users,
            max_train_positives=args.max_train_positives,
        )
        rank_users_before_limit = int(canonical_meta["rank_users_before_limit"])
        selected_train_users = int(canonical_meta["selected_train_users"])
        rank_positives_after_user_limit = int(
            canonical_meta["rank_label_positive_pairs_after_user_limit"]
        )
        test_positive_pairs_available = canonical_meta["test_positive_pairs_available"]
    else:
        print("load canonical", flush=True)
        history, rank, test = load_canonical(canonical_path)

        print("select users", flush=True)
        (
            rank_for_exclusion,
            train_rank,
            rank_users_before_limit,
            selected_train_users,
            rank_positives_after_user_limit,
        ) = select_train_rank(
            rank,
            args.max_train_users,
            args.max_train_positives,
        )
        canonical_meta = {
            "canonical_load_mode": "full_pandas",
            "feature_scope": "full_canonical_after_optional_limits",
        }
        test_positive_pairs_available = int(len(test))

    positives = make_positive_rows(train_rank)
    rank_pairs = set(map(tuple, rank_for_exclusion[["actor_id", "repo_id"]].astype(int).to_numpy()))
    if args.stream_write:
        row_summary = build_streaming_training_rows(
            train_rank=train_rank,
            history=history,
            rank_for_exclusion=rank_for_exclusion,
            candidate_path=candidate_path,
            output_path=output_path,
            negatives_per_positive=args.negatives_per_positive,
            mix=args.negative_mix,
            seed=args.seed,
            max_seen_anchors=args.related_max_seen_anchors,
            max_hard_candidates_per_user=args.max_hard_candidates_per_user,
            candidate_batch_size=args.candidate_batch_size,
            disable_related_negatives=args.disable_related_negatives,
            max_catalog_items=args.max_catalog_items,
            write_batch_users=args.write_batch_users,
            max_rows_per_group=args.max_rows_per_group,
        )
        positive_count = int(row_summary["positive_count"])
        negative_count = int(row_summary["negative_count"])
        rows_count = int(row_summary["rows"])
        groups_count = int(row_summary["groups"])
        included_rank_pairs = int(len(rank_pairs)) if args.max_train_positives is None else int(len(train_rank))
        negative_summary = row_summary
        invariant_negative_history = int(row_summary["negative_intersects_history_seen"])
        invariant_negative_rank = int(row_summary["negative_intersects_rank_label_positive"])
    else:
        negatives, negative_summary = make_negative_rows(
            positives=positives,
            history=history,
            rank=rank_for_exclusion,
            candidate_path=candidate_path,
            negatives_per_positive=args.negatives_per_positive,
            mix=args.negative_mix,
            seed=args.seed,
            max_seen_anchors=args.related_max_seen_anchors,
            max_hard_candidates_per_user=args.max_hard_candidates_per_user,
            candidate_batch_size=args.candidate_batch_size,
            disable_related_negatives=args.disable_related_negatives,
        )
        if args.max_catalog_items is not None:
            print("--max-catalog-items is ignored without --stream-write", flush=True)
        rows = pd.concat([positives, negatives], ignore_index=True)
        print("attach metadata/features", flush=True)
        rows = attach_candidate_metadata(
            rows,
            candidate_path=candidate_path,
            batch_size=args.candidate_batch_size,
        )
        rows = attach_features(rows, feature_stats(history, rank_for_exclusion))
        rows = rows.sort_values(["actor_id", "label", "source", "repo_id"], ascending=[True, False, True, True])
        rows["group_index"] = pd.factorize(rows["actor_id"], sort=True)[0].astype("int32")

        for col in FEATURE_COLUMNS:
            if col not in rows.columns:
                rows[col] = 0.0
        rows["label"] = rows["label"].astype("int8")
        print("write parquet", flush=True)
        rows.to_parquet(output_path, index=False)

        positive_pairs = set(
            map(tuple, rows.loc[rows["label"] == 1, ["actor_id", "repo_id"]].astype(int).to_numpy())
        )
        negative_pairs = set(
            map(tuple, rows.loc[rows["label"] == 0, ["actor_id", "repo_id"]].astype(int).to_numpy())
        )
        history_seen_pairs = set(
            map(tuple, history[["actor_id", "repo_id"]].astype(int).to_numpy())
        )
        positive_count = int(rows["label"].sum())
        negative_count = int((rows["label"] == 0).sum())
        rows_count = int(len(rows))
        groups_count = int(rows["actor_id"].nunique())
        included_rank_pairs = int(len(positive_pairs & rank_pairs))
        invariant_negative_history = int(len(negative_pairs & history_seen_pairs))
        invariant_negative_rank = int(len(negative_pairs & rank_pairs))
    if args.stream_write and args.max_train_positives is not None:
        included_rank_pairs = int(len(train_rank))
    rank_label_coverage_denominator = len(rank_pairs)
    if args.max_train_positives is not None:
        rank_label_coverage_denominator = len(train_rank)
    summary = {
        "suffix": args.suffix,
        "output_suffix": output_suffix,
        "canonical_path": str(canonical_path),
        "retrieval_candidates_path": str(candidate_path),
        "output_path": str(output_path),
        "feature_names": FEATURE_COLUMNS,
        **canonical_meta,
        "rows": rows_count,
        "groups": groups_count,
        "positive_count": positive_count,
        "negative_count": negative_count,
        "positive_rate": positive_count / rows_count if rows_count else 0.0,
        "positive_source": "rank_label",
        "max_train_users": args.max_train_users,
        "max_train_positives": args.max_train_positives,
        "selected_train_users": selected_train_users,
        "rank_users_before_limit": rank_users_before_limit,
        "rank_label_positive_pairs_after_user_limit": rank_positives_after_user_limit,
        "selected_train_positive_pairs": int(len(train_rank)),
        "train_positive_limit_applied": args.max_train_positives is not None,
        "expected_rank_label_positive_pairs": int(len(rank_pairs)),
        "included_rank_label_positive_pairs": included_rank_pairs,
        "rank_label_positive_coverage": (
            float(included_rank_pairs / rank_label_coverage_denominator)
            if rank_label_coverage_denominator
            else 0.0
        ),
        "negatives_per_positive": args.negatives_per_positive,
        "negative_policy": {
            "semantics": "sampled unlabeled negatives",
            "exclude_history_seen": True,
            "exclude_rank_label_positive": True,
            "exclude_test_positive": False,
        },
        "invariant_audit": {
            "negative_intersects_history_seen": invariant_negative_history,
            "negative_intersects_rank_label_positive": invariant_negative_rank,
            "test_rows_used_for_training": 0,
            "test_positive_pairs_loaded_for_exclusion": False,
            "test_positive_pairs_available": test_positive_pairs_available,
            "feature_columns_include_source": bool(any(name.startswith("source") for name in FEATURE_COLUMNS)),
            "feature_columns_include_rank_label_aggregate": bool(
                any("rank" in name and name not in {"candidate_rank"} for name in FEATURE_COLUMNS)
            ),
        },
        **negative_summary,
    }
    print("write summary", flush=True)
    write_json(summary_path, summary)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--suffix", default="latest")
    parser.add_argument("--canonical-path", type=Path, default=None)
    parser.add_argument("--candidate-path", type=Path, default=None)
    parser.add_argument("--output-suffix", default=None)
    parser.add_argument("--negatives-per-positive", type=int, default=20)
    parser.add_argument(
        "--negative-mix",
        type=parse_mix,
        default=dict(DEFAULT_SOURCE_MIX),
        help="Comma mix, e.g. hard=0.4,popular=0.25,related=0.2,random=0.15",
    )
    parser.add_argument("--related-max-seen-anchors", type=int, default=50)
    parser.add_argument("--disable-related-negatives", action="store_true")
    parser.add_argument("--max-hard-candidates-per-user", type=int, default=50)
    parser.add_argument("--candidate-batch-size", type=int, default=500_000)
    parser.add_argument("--canonical-batch-size", type=int, default=500_000)
    parser.add_argument("--max-train-users", type=int, default=None)
    parser.add_argument("--max-train-positives", type=int, default=None)
    parser.add_argument("--stream-write", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--write-batch-users", type=int, default=5_000)
    parser.add_argument(
        "--max-rows-per-group",
        type=int,
        default=10_000,
        help="Split very heavy users into multiple LightGBM query groups under this row limit.",
    )
    parser.add_argument(
        "--max-catalog-items",
        type=int,
        default=None,
        help="Limit popular/random negative pools to the top-N items without limiting users.",
    )
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main() -> None:
    summary = build_rerank_data(parse_args())
    print(
        "wrote {output_path} rows={rows:,} positives={positive_count:,} "
        "negatives={negative_count:,} positive_rate={positive_rate:.4f}".format(**summary)
    )


if __name__ == "__main__":
    main()
