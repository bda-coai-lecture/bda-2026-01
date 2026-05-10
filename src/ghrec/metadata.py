"""GitHub REST API metadata fetcher with SQLite caching."""

import json
import logging
import os
import sqlite3
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS repo_metadata (
    repo_id       INTEGER PRIMARY KEY,
    repo_name     TEXT NOT NULL,
    description   TEXT,
    language      TEXT,
    stargazers    INTEGER,
    forks         INTEGER,
    topics        TEXT,
    license_key   TEXT,
    created_at    TEXT,
    updated_at    TEXT,
    archived      INTEGER DEFAULT 0,
    fetched_at    TEXT NOT NULL,
    http_status   INTEGER DEFAULT 200
)
"""


def init_db(db_path: Path) -> sqlite3.Connection:
    """Create/open the metadata SQLite database."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute(_CREATE_TABLE)
    conn.commit()
    return conn


def get_github_token() -> str | None:
    """Return a GitHub token from env or GitHub CLI without logging the value."""
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        return token.strip()
    try:
        result = subprocess.run(
            ["gh", "auth", "token"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    token = result.stdout.strip()
    return token or None


def _parse_cached_at(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def fetch_repo_metadata(
    repo_name: str,
    token: str | None = None,
    session: requests.Session | None = None,
) -> dict:
    """Fetch metadata for a single repo from GitHub REST API."""
    session = session or requests.Session()
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    url = f"https://api.github.com/repos/{repo_name}"
    resp = session.get(url, headers=headers, timeout=15)

    if resp.status_code != 200:
        return {"http_status": resp.status_code}

    data = resp.json()
    return {
        "description": data.get("description"),
        "language": data.get("language"),
        "stargazers": data.get("stargazers_count", 0),
        "forks": data.get("forks_count", 0),
        "topics": json.dumps(data.get("topics", [])),
        "license_key": (data.get("license") or {}).get("spdx_id"),
        "created_at": data.get("created_at"),
        "updated_at": data.get("updated_at"),
        "archived": int(data.get("archived", False)),
        "http_status": 200,
    }


def fetch_and_cache_repos(
    conn: sqlite3.Connection,
    repo_names: dict[int, str],
    token: str | None = None,
    rate_limit_pause: float = 0.8,
    refresh_stale_days: int | None = None,
    force_refresh: bool = False,
    max_fetch: int | None = None,
    dry_run: bool = False,
) -> pd.DataFrame:
    """Fetch metadata for multiple repos, caching results in SQLite.

    repo_names: {repo_id: "owner/repo"} mapping.
    Skips repos already in the database.
    """
    existing = {
        int(row[0]): {
            "repo_name": row[1],
            "fetched_at": _parse_cached_at(row[2]),
            "http_status": int(row[3]) if row[3] is not None else None,
        }
        for row in conn.execute(
            "SELECT repo_id, repo_name, fetched_at, http_status FROM repo_metadata"
        )
    }

    stale_before = None
    if refresh_stale_days is not None:
        stale_before = datetime.now(timezone.utc) - timedelta(days=refresh_stale_days)

    missing = {}
    stale = {}
    for repo_id, repo_name in repo_names.items():
        repo_id = int(repo_id)
        cached = existing.get(repo_id)
        if cached is None:
            missing[repo_id] = str(repo_name)
            continue

        should_refresh = force_refresh
        if stale_before is not None:
            fetched_at = cached.get("fetched_at")
            should_refresh = should_refresh or fetched_at is None or fetched_at < stale_before
        if should_refresh:
            stale[repo_id] = str(repo_name)

    to_fetch = {**missing, **stale}

    if max_fetch is not None:
        to_fetch = dict(list(to_fetch.items())[:max_fetch])

    logger.info(f"{len(existing)} cached, {len(to_fetch)} to fetch")
    if dry_run:
        logger.info("Dry run. Skipping GitHub API calls.")
        return get_metadata_df(conn)

    session = requests.Session()
    fetched = 0

    for repo_id, repo_name in to_fetch.items():
        meta = fetch_repo_metadata(repo_name, token=token, session=session)
        now = datetime.now(timezone.utc).isoformat()

        conn.execute(
            """INSERT OR REPLACE INTO repo_metadata
               (repo_id, repo_name, description, language, stargazers, forks,
                topics, license_key, created_at, updated_at, archived,
                fetched_at, http_status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                int(repo_id),
                str(repo_name),
                meta.get("description"),
                meta.get("language"),
                int(meta["stargazers"]) if meta.get("stargazers") is not None else None,
                int(meta["forks"]) if meta.get("forks") is not None else None,
                meta.get("topics"),
                meta.get("license_key"),
                meta.get("created_at"),
                meta.get("updated_at"),
                int(meta.get("archived", 0)),
                now,
                int(meta.get("http_status", 200)),
            ),
        )
        fetched += 1

        if fetched % 50 == 0:
            conn.commit()
            logger.info(f"  fetched {fetched}/{len(to_fetch)}")

        if rate_limit_pause > 0:
            time.sleep(rate_limit_pause)

    conn.commit()
    logger.info(f"Done. Fetched {fetched} repos.")
    return get_metadata_df(conn)


def get_metadata_df(
    conn: sqlite3.Connection, repo_ids: list[int] | None = None
) -> pd.DataFrame:
    """Read cached metadata as a DataFrame."""
    query = "SELECT * FROM repo_metadata"
    if repo_ids:
        safe_ids = [int(rid) for rid in repo_ids]
        placeholders = ",".join("?" * len(safe_ids))
        query += f" WHERE repo_id IN ({placeholders})"
        df = pd.read_sql_query(query, conn, params=safe_ids)
    else:
        df = pd.read_sql_query(query, conn)
    return df
