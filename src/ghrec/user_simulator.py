"""Helpers for a local GitHub-user recommendation simulator."""

from __future__ import annotations

import json
import os
import pickle
import re
import sqlite3
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import requests

from ghrec.metadata import fetch_and_cache_repos, get_metadata_df, init_db


DEFAULT_API_URL = "http://localhost:8000"
DEFAULT_HISTORY_PATH = Path("data/marts/week6/user_repo_interaction_mart.parquet")
DEFAULT_CANONICAL_PATH = Path("data/features/recsys_v2/canonical_retrieval_rerank_v2_week7_full_20260502.parquet")
DEFAULT_REPO_METADATA_DB = Path("data/repo_metadata.db")
DEFAULT_REPO_NAME_MAP = Path("data/models/repo_name_map.pkl")
DEFAULT_USER_CACHE_DB = Path("data/github_user_cache.db")

EVENT_COLUMNS = ["watch_cnt", "fork_cnt", "pr_cnt", "push_cnt", "issue_cnt", "comment_cnt"]


@dataclass(frozen=True)
class GitHubUser:
    username: str
    actor_id: int
    html_url: str
    name: str | None = None
    public_repos: int | None = None
    followers: int | None = None
    fetched_at: str | None = None


def parse_github_username(value: str) -> str:
    """Extract a GitHub username from a login or profile URL."""
    text = value.strip()
    if not text:
        raise ValueError("GitHub username or URL is required")

    if text.startswith(("github.com/", "www.github.com/")):
        text = "https://" + text

    if "://" in text:
        parsed = urlparse(text)
        if parsed.netloc.lower() not in {"github.com", "www.github.com"}:
            raise ValueError("Only github.com profile URLs are supported")
        text = parsed.path.strip("/").split("/")[0]
    else:
        text = text.removeprefix("@").strip("/")

    if not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?", text):
        raise ValueError("Invalid GitHub username")
    return text


def github_token() -> str | None:
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        return token.strip()
    try:
        result = subprocess.run(
            ["gh", "auth", "token"],
            check=False,
            capture_output=True,
            text=True,
            timeout=8,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None


def init_user_cache(db_path: Path = DEFAULT_USER_CACHE_DB) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS github_users (
            username     TEXT PRIMARY KEY,
            actor_id     INTEGER NOT NULL,
            html_url     TEXT NOT NULL,
            name         TEXT,
            public_repos INTEGER,
            followers    INTEGER,
            fetched_at   TEXT NOT NULL,
            http_status  INTEGER NOT NULL DEFAULT 200
        )
        """
    )
    conn.commit()
    return conn


def _cached_user(conn: sqlite3.Connection, username: str, max_age_hours: int) -> GitHubUser | None:
    row = conn.execute(
        """
        SELECT username, actor_id, html_url, name, public_repos, followers, fetched_at
        FROM github_users
        WHERE lower(username) = lower(?) AND http_status = 200
        """,
        (username,),
    ).fetchone()
    if not row:
        return None
    fetched_at = datetime.fromisoformat(str(row[6]).replace("Z", "+00:00"))
    if fetched_at < datetime.now(UTC) - timedelta(hours=max_age_hours):
        return None
    return GitHubUser(
        username=str(row[0]),
        actor_id=int(row[1]),
        html_url=str(row[2]),
        name=row[3],
        public_repos=row[4],
        followers=row[5],
        fetched_at=str(row[6]),
    )


def resolve_github_user(
    username_or_url: str,
    db_path: Path = DEFAULT_USER_CACHE_DB,
    max_age_hours: int = 24,
    token: str | None = None,
    session: requests.Session | None = None,
) -> GitHubUser:
    """Resolve a GitHub login to the numeric id used by GHArchive actor_id."""
    username = parse_github_username(username_or_url)
    conn = init_user_cache(db_path)
    try:
        cached = _cached_user(conn, username, max_age_hours=max_age_hours)
        if cached:
            return cached

        session = session or requests.Session()
        headers = {"Accept": "application/vnd.github+json"}
        token = token if token is not None else github_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        resp = session.get(f"https://api.github.com/users/{username}", headers=headers, timeout=15)
        now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if resp.status_code != 200:
            conn.execute(
                """
                INSERT OR REPLACE INTO github_users
                (username, actor_id, html_url, fetched_at, http_status)
                VALUES (?, ?, ?, ?, ?)
                """,
                (username, 0, f"https://github.com/{username}", now, int(resp.status_code)),
            )
            conn.commit()
            raise LookupError(f"GitHub user lookup failed for {username}: HTTP {resp.status_code}")

        data = resp.json()
        user = GitHubUser(
            username=str(data["login"]),
            actor_id=int(data["id"]),
            html_url=str(data.get("html_url") or f"https://github.com/{username}"),
            name=data.get("name"),
            public_repos=data.get("public_repos"),
            followers=data.get("followers"),
            fetched_at=now,
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO github_users
            (username, actor_id, html_url, name, public_repos, followers, fetched_at, http_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 200)
            """,
            (
                user.username,
                user.actor_id,
                user.html_url,
                user.name,
                user.public_repos,
                user.followers,
                user.fetched_at,
            ),
        )
        conn.commit()
        return user
    finally:
        conn.close()


def load_repo_name_map(path: Path = DEFAULT_REPO_NAME_MAP) -> dict[int, str]:
    if not path.exists():
        return {}
    with path.open("rb") as f:
        raw = pickle.load(f)
    out: dict[int, str] = {}
    for key, value in raw.items():
        if pd.isna(key) or pd.isna(value):
            continue
        try:
            repo_id = int(key)
        except (TypeError, ValueError):
            continue
        repo_name = str(value)
        if repo_name:
            out[repo_id] = repo_name
    return out


def summarize_history(
    actor_id: int,
    history_path: Path = DEFAULT_HISTORY_PATH,
    canonical_path: Path = DEFAULT_CANONICAL_PATH,
    limit: int = 12,
) -> pd.DataFrame:
    """Return recent or model-history activity rows for one actor."""
    if history_path.exists():
        try:
            df = pd.read_parquet(history_path, filters=[("actor_id", "=", int(actor_id))])
        except Exception:
            df = pd.read_parquet(history_path)
            df = df[df["actor_id"] == int(actor_id)]
        if not df.empty:
            sort_cols = [col for col in ["last_seen_at", "weighted_score"] if col in df.columns]
            df = df.sort_values(sort_cols, ascending=False)
            return df.head(limit).reset_index(drop=True)

    if canonical_path.exists():
        df = pd.read_parquet(canonical_path, filters=[("actor_id", "=", int(actor_id)), ("split", "=", "history")])
        if not df.empty:
            df = df.rename(columns={"score": "weighted_score"})
            for col in EVENT_COLUMNS:
                df[col] = 0
            return df.sort_values("weighted_score", ascending=False).head(limit).reset_index(drop=True)

    return pd.DataFrame()


def event_label(row: pd.Series) -> str:
    counts = {col.replace("_cnt", ""): int(row.get(col, 0) or 0) for col in EVENT_COLUMNS}
    active = [f"{name} {count:,}" for name, count in counts.items() if count > 0]
    return ", ".join(active[:3]) if active else "model history"


def ensure_repo_metadata(
    repo_ids: list[int],
    repo_names: dict[int, str],
    db_path: Path = DEFAULT_REPO_METADATA_DB,
    max_fetch: int = 20,
) -> pd.DataFrame:
    """Read cached repo metadata and fetch a small missing set from GitHub."""
    repo_ids = [int(rid) for rid in repo_ids if rid is not None]
    if not repo_ids:
        return pd.DataFrame()

    conn = init_db(db_path)
    try:
        meta = get_metadata_df(conn, repo_ids=repo_ids)
        cached_ids = set(meta["repo_id"].astype(int)) if not meta.empty else set()
        missing = {
            int(rid): repo_names[int(rid)]
            for rid in repo_ids
            if int(rid) not in cached_ids and int(rid) in repo_names and "/" in repo_names[int(rid)]
        }
        if missing:
            fetch_and_cache_repos(
                conn,
                missing,
                token=github_token(),
                rate_limit_pause=0.1,
                max_fetch=max_fetch,
            )
            meta = get_metadata_df(conn, repo_ids=repo_ids)
        return meta
    finally:
        conn.close()


def metadata_lookup(meta: pd.DataFrame) -> dict[int, dict[str, Any]]:
    if meta.empty:
        return {}
    records = meta.to_dict(orient="records")
    return {int(row["repo_id"]): row for row in records}


def call_recommendation_api(
    actor_id: int,
    k: int,
    api_url: str = DEFAULT_API_URL,
    include_features: bool = False,
    include_sources: bool = True,
) -> dict[str, Any]:
    url = api_url.rstrip("/") + "/v1/recsys/recommendations"
    resp = requests.post(
        url,
        json={
            "actor_id": int(actor_id),
            "k": int(k),
            "bundle_id": None,
            "include_features": include_features,
            "include_sources": include_sources,
        },
        timeout=60,
    )
    if resp.status_code >= 400:
        try:
            detail = resp.json()
        except ValueError:
            detail = resp.text
        raise RuntimeError(f"Recommendation API failed: HTTP {resp.status_code} {detail}")
    return resp.json()


def healthcheck_api(api_url: str = DEFAULT_API_URL) -> dict[str, Any]:
    resp = requests.get(api_url.rstrip("/") + "/health", timeout=5)
    resp.raise_for_status()
    return resp.json()


def format_topics(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return value
    else:
        parsed = value
    if isinstance(parsed, list):
        return ", ".join(str(item) for item in parsed[:5])
    return str(parsed)
