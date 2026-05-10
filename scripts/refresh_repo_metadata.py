"""Refresh the local GitHub repo metadata cache.

The script uses local parquet activity to choose repo candidates, then updates
only missing or stale rows in data/repo_metadata.db.
"""

from __future__ import annotations

import argparse
import logging
import pickle
import sqlite3
from datetime import datetime
from pathlib import Path

import duckdb

from ghrec.metadata import fetch_and_cache_repos, get_github_token, init_db


DEFAULT_DB_PATH = Path("data/repo_metadata.db")
DEFAULT_PARQUET_DIR = Path("data/daily_agg")
DEFAULT_REPO_MAP = Path("data/models/repo_name_map.pkl")
DEFAULT_REPO_NAME_CACHE = Path("data/repo_name_lookup.db")
STALE_DAYS_BY_TIER = {
    "hot": 3,
    "warm": 7,
}


def parse_day(path: Path):
    return datetime.strptime(path.stem, "%Y%m%d").date()


def iter_parquet_files(parquet_dir: Path, start: str | None, end: str | None) -> list[Path]:
    start_day = datetime.strptime(start, "%Y-%m-%d").date() if start else None
    end_day = datetime.strptime(end, "%Y-%m-%d").date() if end else None
    files = []
    for path in sorted(parquet_dir.glob("*.parquet")):
        day = parse_day(path)
        if start_day and day < start_day:
            continue
        if end_day and day > end_day:
            continue
        files.append(path)
    return files


def parquet_source(files: list[Path]) -> str:
    file_list = ", ".join("'" + path.as_posix().replace("'", "''") + "'" for path in files)
    return f"read_parquet([{file_list}])"


def top_repo_ids(files: list[Path], top_n: int) -> list[int]:
    con = duckdb.connect(database=":memory:")
    con.execute("SET memory_limit = '2GB'")
    con.execute("SET threads = 1")
    rows = con.sql(
        f"""
        SELECT CAST(repo_id AS BIGINT) AS repo_id, SUM(cnt) AS events
        FROM {parquet_source(files)}
        WHERE repo_id IS NOT NULL
        GROUP BY repo_id
        ORDER BY events DESC
        LIMIT {int(top_n)}
        """
    ).fetchall()
    return [int(repo_id) for repo_id, _ in rows]


def init_repo_name_cache(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS repo_name_lookup (
            repo_id INTEGER PRIMARY KEY,
            repo_name TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS repo_name_lookup_miss (
            repo_id INTEGER PRIMARY KEY
        )
        """
    )
    conn.commit()
    return conn


def load_repo_name_cache(
    conn: sqlite3.Connection,
    repo_ids: set[int],
) -> tuple[dict[int, str], set[int]]:
    if not repo_ids:
        return {}, set()
    ids = [int(repo_id) for repo_id in repo_ids]
    placeholders = ",".join("?" for _ in ids)
    name_rows = conn.execute(
        f"SELECT repo_id, repo_name FROM repo_name_lookup WHERE repo_id IN ({placeholders})",
        ids,
    )
    miss_rows = conn.execute(
        f"SELECT repo_id FROM repo_name_lookup_miss WHERE repo_id IN ({placeholders})",
        ids,
    )
    return (
        {int(repo_id): str(repo_name) for repo_id, repo_name in name_rows},
        {int(repo_id) for (repo_id,) in miss_rows},
    )


def save_repo_name_cache(conn: sqlite3.Connection, repo_names: dict[int, str]) -> None:
    if not repo_names:
        return
    conn.executemany(
        "INSERT OR REPLACE INTO repo_name_lookup (repo_id, repo_name) VALUES (?, ?)",
        [(int(repo_id), str(repo_name)) for repo_id, repo_name in repo_names.items()],
    )
    conn.commit()


def save_repo_name_misses(conn: sqlite3.Connection, repo_ids: set[int]) -> None:
    if not repo_ids:
        return
    conn.executemany(
        "INSERT OR IGNORE INTO repo_name_lookup_miss (repo_id) VALUES (?)",
        [(int(repo_id),) for repo_id in repo_ids],
    )
    conn.commit()


def load_repo_name_map(path: Path, repo_ids: set[int]) -> dict[int, str]:
    if not path.exists():
        return {}
    with path.open("rb") as f:
        repo_name_map = pickle.load(f)
    out = {}
    for repo_id in repo_ids:
        repo_name = repo_name_map.get(repo_id)
        if repo_name:
            out[int(repo_id)] = str(repo_name)
    return out


def load_existing_repo_names(conn: sqlite3.Connection) -> dict[int, str]:
    return {
        int(repo_id): str(repo_name)
        for repo_id, repo_name in conn.execute(
            "SELECT repo_id, repo_name FROM repo_metadata WHERE repo_name IS NOT NULL"
        )
    }


def parse_repo_args(values: list[str] | None) -> dict[int, str]:
    repos = {}
    for value in values or []:
        if "=" not in value:
            raise ValueError(f"--repo must be formatted as repo_id=owner/name: {value}")
        repo_id, repo_name = value.split("=", 1)
        repos[int(repo_id)] = repo_name.strip()
    return repos


def load_repo_list(path: Path | None) -> dict[int, str]:
    if path is None:
        return {}
    if not path.exists():
        raise FileNotFoundError(path)
    repos = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "," in line:
            repo_id, repo_name = line.split(",", 1)
        elif "=" in line:
            repo_id, repo_name = line.split("=", 1)
        else:
            raise ValueError(f"Repo list line must be repo_id,owner/name: {line}")
        repos[int(repo_id)] = repo_name.strip()
    return repos


def cache_summary(conn: sqlite3.Connection) -> dict[str, object]:
    row = conn.execute(
        """
        SELECT
          COUNT(*) AS rows,
          SUM(CASE WHEN http_status = 200 THEN 1 ELSE 0 END) AS ok_rows,
          MIN(fetched_at) AS min_fetched_at,
          MAX(fetched_at) AS max_fetched_at
        FROM repo_metadata
        """
    ).fetchone()
    return {
        "rows": int(row[0] or 0),
        "ok_rows": int(row[1] or 0),
        "min_fetched_at": row[2],
        "max_fetched_at": row[3],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--parquet-dir", type=Path, default=DEFAULT_PARQUET_DIR)
    parser.add_argument("--repo-map-pkl", type=Path, default=DEFAULT_REPO_MAP)
    parser.add_argument("--repo-name-cache-db", type=Path, default=DEFAULT_REPO_NAME_CACHE)
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--top-n", type=int, default=500)
    parser.add_argument(
        "--cache-tier",
        choices=["hot", "warm", "custom"],
        default="warm",
        help="hot refreshes metadata older than 3 days; warm refreshes older than 7 days.",
    )
    parser.add_argument("--refresh-stale-days", type=int)
    parser.add_argument(
        "--repo",
        action="append",
        help="Explicit repo to cache, formatted as repo_id=owner/name. Repeatable.",
    )
    parser.add_argument(
        "--repo-list",
        type=Path,
        help="Text/CSV file with repo_id,owner/name or repo_id=owner/name lines.",
    )
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument("--max-fetch", type=int, default=200)
    parser.add_argument("--rate-limit-pause", type=float)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()
    files = iter_parquet_files(args.parquet_dir, args.start, args.end)
    explicit_repos = parse_repo_args(args.repo)
    explicit_repos.update(load_repo_list(args.repo_list))
    if not files and not explicit_repos:
        raise FileNotFoundError(f"No parquet files found in {args.parquet_dir}")

    conn = init_db(args.db_path)
    before = cache_summary(conn)
    repo_ids = set(top_repo_ids(files, args.top_n)) if files and args.top_n > 0 else set()
    repo_ids.update(explicit_repos)
    existing_repo_names = load_existing_repo_names(conn)
    repo_names = {
        int(repo_id): repo_name
        for repo_id, repo_name in existing_repo_names.items()
        if int(repo_id) in repo_ids
    }
    repo_name_cache_conn = init_repo_name_cache(args.repo_name_cache_db)
    cached_names, cached_misses = load_repo_name_cache(repo_name_cache_conn, repo_ids - set(repo_names))
    repo_names.update(cached_names)
    missing_from_lookup = repo_ids - set(repo_names) - cached_misses
    from_pickle = load_repo_name_map(args.repo_map_pkl, missing_from_lookup)
    save_repo_name_cache(repo_name_cache_conn, from_pickle)
    save_repo_name_misses(repo_name_cache_conn, missing_from_lookup - set(from_pickle))
    repo_names.update(from_pickle)
    repo_names.update(explicit_repos)

    missing_names = len(repo_ids - set(repo_names))
    refresh_stale_days = args.refresh_stale_days
    if refresh_stale_days is None:
        if args.cache_tier == "custom":
            raise ValueError("--cache-tier custom requires --refresh-stale-days")
        refresh_stale_days = STALE_DAYS_BY_TIER[args.cache_tier]
    token = get_github_token()
    pause = args.rate_limit_pause
    if pause is None:
        pause = 0.3 if token else 0.8

    logging.info(
        "PLAN files=%s top_n=%s explicit=%s tier=%s stale_days=%s mapped=%s missing_names=%s token=%s dry_run=%s",
        len(files),
        args.top_n,
        len(explicit_repos),
        args.cache_tier,
        refresh_stale_days,
        len(repo_names),
        missing_names,
        "set" if token else "not_set",
        args.dry_run,
    )
    logging.info("CACHE before=%s", before)

    fetch_and_cache_repos(
        conn,
        repo_names,
        token=token,
        rate_limit_pause=pause,
        refresh_stale_days=refresh_stale_days,
        force_refresh=args.force_refresh,
        max_fetch=args.max_fetch,
        dry_run=args.dry_run,
    )
    logging.info("CACHE after=%s", cache_summary(conn))


if __name__ == "__main__":
    main()
