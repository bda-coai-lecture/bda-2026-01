"""Refresh the local GitHub repo metadata cache.

The script uses local parquet activity to choose repo candidates, then updates
only missing or stale rows in data/repo_metadata.db.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import pickle
import sqlite3
from datetime import datetime, timedelta, timezone
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
DEFAULT_SAMPLE_K_PRIMES = [
    5,
    7,
    11,
    17,
    23,
    31,
    43,
    61,
    83,
    101,
    149,
    211,
    307,
    431,
    607,
    857,
    1201,
    1697,
    2381,
    3347,
    4703,
    6607,
    9283,
    13007,
    18223,
    25537,
    35753,
    50021,
]


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


def latest_file(files: list[Path]) -> Path | None:
    return max(files, key=parse_day) if files else None


def file_for_day(files: list[Path], day: str | None) -> Path | None:
    if day is None:
        return latest_file(files)
    target = datetime.strptime(day, "%Y-%m-%d").date()
    for path in files:
        if parse_day(path) == target:
            return path
    return None


def parse_prime_candidates(value: str) -> list[int]:
    primes = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        k = int(item)
        if not is_prime(k):
            raise ValueError(f"K must be a prime greater than 1: {k}")
        primes.append(k)
    if not primes:
        raise ValueError("--sample-k-primes must contain at least one K value")
    return sorted(set(primes))


def is_prime(value: int) -> bool:
    if value <= 1:
        return False
    if value <= 3:
        return True
    if value % 2 == 0 or value % 3 == 0:
        return False
    factor = 5
    while factor * factor <= value:
        if value % factor == 0 or value % (factor + 2) == 0:
            return False
        factor += 6
    return True


def stable_mod(value: int, seed: str, k: int) -> int:
    payload = f"{seed}:{int(value)}".encode("utf-8")
    digest = hashlib.blake2b(payload, digest_size=8).digest()
    return int.from_bytes(digest, "big") % int(k)


def daily_active_users(day_file: Path) -> list[int]:
    con = duckdb.connect(database=":memory:")
    con.execute("SET memory_limit = '2GB'")
    con.execute("SET threads = 1")
    rows = con.sql(
        f"""
        SELECT DISTINCT CAST(actor_id AS BIGINT) AS actor_id
        FROM read_parquet('{day_file.as_posix().replace("'", "''")}')
        WHERE actor_id IS NOT NULL
        """
    ).fetchall()
    return [int(actor_id) for (actor_id,) in rows]


def sampled_users(active_users: list[int], seed: str, k: int) -> list[int]:
    return [actor_id for actor_id in active_users if stable_mod(actor_id, seed, k) == 0]


def repo_ids_for_users(day_file: Path, actor_ids: list[int]) -> list[int]:
    if not actor_ids:
        return []
    con = duckdb.connect(database=":memory:")
    con.execute("SET memory_limit = '2GB'")
    con.execute("SET threads = 1")
    con.execute("CREATE TEMP TABLE sampled_users(actor_id BIGINT)")
    con.executemany(
        "INSERT INTO sampled_users VALUES (?)",
        [(int(actor_id),) for actor_id in actor_ids],
    )
    rows = con.sql(
        f"""
        SELECT CAST(d.repo_id AS BIGINT) AS repo_id, SUM(d.cnt) AS events
        FROM read_parquet('{day_file.as_posix().replace("'", "''")}') d
        JOIN sampled_users u(actor_id)
          ON CAST(d.actor_id AS BIGINT) = u.actor_id
        WHERE d.repo_id IS NOT NULL
        GROUP BY d.repo_id
        ORDER BY events DESC, repo_id
        """
    ).fetchall()
    return [int(repo_id) for repo_id, _ in rows]


def load_fresh_repo_ids(
    conn: sqlite3.Connection,
    refresh_stale_days: int | None,
    force_refresh: bool,
) -> set[int]:
    if force_refresh:
        return set()

    stale_before = None
    if refresh_stale_days is not None:
        stale_before = datetime.now(timezone.utc) - timedelta(days=refresh_stale_days)

    fresh = set()
    for repo_id, fetched_at in conn.execute("SELECT repo_id, fetched_at FROM repo_metadata"):
        if stale_before is None:
            fresh.add(int(repo_id))
            continue
        try:
            fetched = datetime.fromisoformat(str(fetched_at).replace("Z", "+00:00"))
        except ValueError:
            continue
        if fetched >= stale_before:
            fresh.add(int(repo_id))
    return fresh


def choose_systematic_sample(
    day_file: Path,
    seed: str,
    k_primes: list[int],
    top_ids: list[int],
    fresh_repo_ids: set[int],
    max_fetch: int | None,
    refresh_stale_days: int | None,
) -> tuple[list[int], dict[str, object]]:
    active_users = daily_active_users(day_file)
    top_set = set(top_ids)
    top_fetch_needed = len(top_set - fresh_repo_ids)
    refresh_days = max(int(refresh_stale_days or 1), 1)
    refresh_capacity = None if max_fetch is None else int(max_fetch) * refresh_days
    remaining_refresh_capacity = (
        None if refresh_capacity is None else max(refresh_capacity - top_fetch_needed, 0)
    )

    best_repo_ids: list[int] = []
    best_summary: dict[str, object] | None = None
    simulations = []
    for k in k_primes:
        users = sampled_users(active_users, seed, k)
        repo_ids = repo_ids_for_users(day_file, users)
        new_repo_ids = [repo_id for repo_id in repo_ids if repo_id not in top_set]
        fetch_needed = len(set(new_repo_ids) - fresh_repo_ids)
        summary = {
            "k": k,
            "dau": len(active_users),
            "sample_users": len(users),
            "sample_repos": len(repo_ids),
            "new_sample_repos": len(new_repo_ids),
            "sample_fetch_needed": fetch_needed,
            "top_fetch_needed": top_fetch_needed,
            "estimated_fetch_needed": top_fetch_needed + fetch_needed,
            "daily_max_fetch": max_fetch,
            "refresh_days": refresh_days,
            "refresh_capacity": refresh_capacity,
            "remaining_refresh_capacity_after_top": remaining_refresh_capacity,
        }
        simulations.append(summary)

        fits_budget = (
            remaining_refresh_capacity is None or fetch_needed <= remaining_refresh_capacity
        )
        if fits_budget and (
            best_summary is None
            or int(summary["sample_fetch_needed"]) > int(best_summary["sample_fetch_needed"])
        ):
            best_repo_ids = new_repo_ids
            best_summary = summary

    if best_summary is None:
        best_summary = min(simulations, key=lambda item: int(item["sample_fetch_needed"]))
        users = sampled_users(active_users, seed, int(best_summary["k"]))
        best_repo_ids = [
            repo_id for repo_id in repo_ids_for_users(day_file, users) if repo_id not in top_set
        ]

    best_summary["simulations"] = simulations
    return best_repo_ids, best_summary


def ordered_unique(values: list[int]) -> list[int]:
    seen = set()
    out = []
    for value in values:
        value = int(value)
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


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
    parser.add_argument("--top-n", type=int, default=1000)
    parser.add_argument("--systematic-sample", action="store_true")
    parser.add_argument(
        "--sample-date",
        help="Activity date for DAU systematic sampling. Defaults to the latest parquet day in the selected window.",
    )
    parser.add_argument("--sample-seed", default="bda-repo-metadata-v1")
    parser.add_argument(
        "--sample-k-prime",
        type=int,
        help="Fixed prime K. If omitted, K is chosen by simulation against --max-fetch.",
    )
    parser.add_argument(
        "--sample-k-primes",
        default=",".join(str(k) for k in DEFAULT_SAMPLE_K_PRIMES),
        help="Comma-separated prime K candidates for DAU systematic sampling.",
    )
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
    refresh_stale_days = args.refresh_stale_days
    if refresh_stale_days is None:
        if args.cache_tier == "custom":
            raise ValueError("--cache-tier custom requires --refresh-stale-days")
        refresh_stale_days = STALE_DAYS_BY_TIER[args.cache_tier]

    top_ids = top_repo_ids(files, args.top_n) if files and args.top_n > 0 else []
    repo_ids = ordered_unique(top_ids + list(explicit_repos))
    if args.systematic_sample:
        sample_file = file_for_day(files, args.sample_date)
        if sample_file is None:
            raise FileNotFoundError(f"No parquet file found for sample date: {args.sample_date}")
        sample_k_primes = (
            [args.sample_k_prime]
            if args.sample_k_prime is not None
            else parse_prime_candidates(args.sample_k_primes)
        )
        if any(not is_prime(k) for k in sample_k_primes):
            raise ValueError("--sample-k-prime must be a prime greater than 1")
        fresh_repo_ids = load_fresh_repo_ids(conn, refresh_stale_days, args.force_refresh)
        sample_ids, sample_summary = choose_systematic_sample(
            sample_file,
            args.sample_seed,
            sample_k_primes,
            top_ids,
            fresh_repo_ids,
            args.max_fetch,
            refresh_stale_days,
        )
        repo_ids = ordered_unique(repo_ids + sample_ids)
        logging.info(
            "SAMPLE day=%s seed=%s k=%s dau=%s sampled_users=%s sample_repos=%s new_sample_repos=%s "
            "top_fetch_needed=%s sample_fetch_needed=%s estimated_fetch_needed=%s "
            "daily_max_fetch=%s refresh_days=%s refresh_capacity=%s",
            parse_day(sample_file),
            args.sample_seed,
            sample_summary["k"],
            sample_summary["dau"],
            sample_summary["sample_users"],
            sample_summary["sample_repos"],
            sample_summary["new_sample_repos"],
            sample_summary["top_fetch_needed"],
            sample_summary["sample_fetch_needed"],
            sample_summary["estimated_fetch_needed"],
            args.max_fetch,
            sample_summary["refresh_days"],
            sample_summary["refresh_capacity"],
        )

    existing_repo_names = load_existing_repo_names(conn)
    repo_names = {
        int(repo_id): repo_name
        for repo_id, repo_name in existing_repo_names.items()
        if int(repo_id) in repo_ids
    }
    repo_name_cache_conn = init_repo_name_cache(args.repo_name_cache_db)
    repo_id_set = set(repo_ids)
    cached_names, cached_misses = load_repo_name_cache(repo_name_cache_conn, repo_id_set - set(repo_names))
    repo_names.update(cached_names)
    missing_from_lookup = repo_id_set - set(repo_names) - cached_misses
    from_pickle = load_repo_name_map(args.repo_map_pkl, missing_from_lookup)
    save_repo_name_cache(repo_name_cache_conn, from_pickle)
    save_repo_name_misses(repo_name_cache_conn, missing_from_lookup - set(from_pickle))
    repo_names.update(from_pickle)
    repo_names.update(explicit_repos)

    missing_names = len(repo_id_set - set(repo_names))
    repo_names = {repo_id: repo_names[repo_id] for repo_id in repo_ids if repo_id in repo_names}
    token = get_github_token()
    pause = args.rate_limit_pause
    if pause is None:
        pause = 0.3 if token else 0.8

    logging.info(
        "PLAN files=%s top_n=%s systematic_sample=%s candidates=%s explicit=%s tier=%s stale_days=%s "
        "mapped=%s missing_names=%s token=%s dry_run=%s",
        len(files),
        args.top_n,
        args.systematic_sample,
        len(repo_ids),
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
