"""Read-only FastAPI app for the Vercel recommendation frontend."""

from __future__ import annotations

import json
import os
import sqlite3
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from ghrec.api import recommend_for_actor
from ghrec.recsys_serving import ActorNotFoundError
from ghrec.user_simulator import resolve_github_user


DEFAULT_TRENDING_PATH = Path("data/models/week6/trendy_repos_latest.parquet")
DEFAULT_RELATED_PATH = Path("data/models/week6/item2item_related_latest.parquet")
DEFAULT_METADATA_DB = Path("data/repo_metadata.db")
DEFAULT_NAME_LOOKUP_DB = Path("data/repo_name_lookup.db")

SAMPLE_ITEMS = [
    {
        "repo_id": 10270250,
        "full_name": "facebook/react",
        "description": "The library for web and native user interfaces.",
        "language": "JavaScript",
        "stars": None,
        "forks": None,
        "topics": [],
        "url": "https://github.com/facebook/react",
    },
    {
        "repo_id": 65600975,
        "full_name": "pytorch/pytorch",
        "description": "Tensors and dynamic neural networks in Python.",
        "language": "Python",
        "stars": None,
        "forks": None,
        "topics": [],
        "url": "https://github.com/pytorch/pytorch",
    },
    {
        "repo_id": 21289110,
        "full_name": "microsoft/vscode",
        "description": "Visual Studio Code.",
        "language": "TypeScript",
        "stars": None,
        "forks": None,
        "topics": [],
        "url": "https://github.com/microsoft/vscode",
    },
]


def _csv_env(*names: str) -> list[str]:
    for name in names:
        value = os.environ.get(name)
        if value:
            return [part.strip() for part in value.split(",") if part.strip()]
    return []


def _path_env(name: str, default: Path) -> Path:
    return Path(os.environ.get(name, str(default)))


def _allowed_origins() -> list[str]:
    configured = _csv_env("GHREC_ALLOWED_ORIGINS", "RECSYS_ALLOWED_ORIGINS")
    if configured:
        return configured
    return [
        "http://localhost:3000",
        "http://localhost:3001",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:3001",
        "http://127.0.0.1:5173",
    ]


app = FastAPI(title="BDA Recsys Local API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins(),
    allow_origin_regex=os.environ.get("GHREC_ALLOWED_ORIGIN_REGEX", r"https://.*\.vercel\.app"),
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)


def main() -> None:
    """Run the local API with uvicorn."""
    import uvicorn

    uvicorn.run("ghrec.local_api:app", host="127.0.0.1", port=8001, reload=False)


def _connect(path: Path) -> sqlite3.Connection | None:
    if not path.exists():
        return None
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def _parse_topics(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed]


@lru_cache(maxsize=1)
def _metadata_by_repo_id() -> dict[int, dict[str, Any]]:
    db_path = _path_env("GHREC_REPO_METADATA_DB", DEFAULT_METADATA_DB)
    conn = _connect(db_path)
    if conn is None:
        return {}
    try:
        rows = conn.execute(
            """
            SELECT repo_id, repo_name, description, language, stargazers, forks, topics, fetched_at, http_status
            FROM repo_metadata
            """
        ).fetchall()
        return {
            int(row["repo_id"]): {
                "repo_id": int(row["repo_id"]),
                "full_name": row["repo_name"],
                "description": row["description"] if int(row["http_status"] or 0) == 200 else None,
                "language": row["language"] if int(row["http_status"] or 0) == 200 else None,
                "stars": row["stargazers"] if int(row["http_status"] or 0) == 200 else None,
                "forks": row["forks"] if int(row["http_status"] or 0) == 200 else None,
                "topics": _parse_topics(row["topics"]) if int(row["http_status"] or 0) == 200 else [],
                "cache": {
                    "source": "repo_metadata.sqlite",
                    "status": "cached" if int(row["http_status"] or 0) == 200 else "unavailable",
                    "fetched_at": row["fetched_at"],
                    "http_status": row["http_status"],
                },
            }
            for row in rows
        }
    finally:
        conn.close()


@lru_cache(maxsize=1)
def _name_lookup_by_repo_id() -> dict[int, str]:
    db_path = _path_env("GHREC_REPO_NAME_LOOKUP_DB", DEFAULT_NAME_LOOKUP_DB)
    conn = _connect(db_path)
    if conn is None:
        return {}
    try:
        rows = conn.execute("SELECT repo_id, repo_name FROM repo_name_lookup").fetchall()
        return {int(row["repo_id"]): str(row["repo_name"]) for row in rows}
    finally:
        conn.close()


@lru_cache(maxsize=1)
def _repo_id_by_name() -> dict[str, int]:
    out: dict[str, int] = {}
    for repo_id, meta in _metadata_by_repo_id().items():
        name = str(meta.get("full_name") or "").lower()
        if name:
            out[name] = int(repo_id)
    for repo_id, name in _name_lookup_by_repo_id().items():
        out.setdefault(str(name).lower(), int(repo_id))
    return out


def _repo_name(repo_id: int) -> str:
    meta = _metadata_by_repo_id().get(int(repo_id))
    if meta and meta.get("full_name"):
        return str(meta["full_name"])
    return _name_lookup_by_repo_id().get(int(repo_id), f"repo_{int(repo_id)}")


def _repo_meta(repo_id: int) -> dict[str, Any]:
    repo_id = int(repo_id)
    meta = dict(_metadata_by_repo_id().get(repo_id) or {})
    full_name = str(meta.get("full_name") or _repo_name(repo_id))
    meta.update(
        {
            "repo_id": repo_id,
            "full_name": full_name,
            "url": f"https://github.com/{full_name}" if "/" in full_name else None,
            "description": meta.get("description"),
            "language": meta.get("language"),
            "stars": meta.get("stars"),
            "forks": meta.get("forks"),
            "topics": meta.get("topics") or [],
            "cache": meta.get("cache")
            or {
                "source": "repo_metadata.sqlite",
                "status": "missing",
                "fetched_at": None,
                "http_status": None,
            },
        }
    )
    return meta


def _repo_item(
    repo_id: int,
    *,
    rank: int,
    score: float | None = None,
    reason: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    item = _repo_meta(int(repo_id))
    item.update({"rank": int(rank), "score": None if score is None else float(score)})
    if reason:
        item["reason"] = reason
    if extra:
        item.update(extra)
    return item


def _sample_response(limit: int, reason: str) -> list[dict[str, Any]]:
    return [
        {
            **item,
            "rank": idx,
            "score": float(limit - idx + 1),
            "reason": reason,
        }
        for idx, item in enumerate(SAMPLE_ITEMS[:limit], start=1)
    ]


def _score_from_row(row: Any, columns: set[str]) -> float | None:
    for name in ("trend_score", "score", "cooc_score", "related_score", "recent_score", "total_score"):
        if name in columns:
            value = getattr(row, name)
            if pd.notna(value):
                return float(value)
    return None


@lru_cache(maxsize=1)
def _trending_frame() -> pd.DataFrame:
    path = _path_env("GHREC_TRENDING_PATH", DEFAULT_TRENDING_PATH)
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


@lru_cache(maxsize=8)
def _related_for_anchor(anchor_repo_id: int) -> pd.DataFrame:
    path = _path_env("GHREC_RELATED_PATH", DEFAULT_RELATED_PATH)
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path, filters=[("anchor_repo_id", "=", int(anchor_repo_id))])
    except Exception:
        try:
            frame = pd.read_parquet(path)
            if "anchor_repo_id" not in frame.columns:
                return pd.DataFrame()
            return frame[frame["anchor_repo_id"].eq(int(anchor_repo_id))].copy()
        except Exception:
            return pd.DataFrame()


def _resolve_repo_id(owner: str, repo: str) -> int | None:
    full_name = f"{owner.strip()}/{repo.strip()}".lower()
    repo_id = _repo_id_by_name().get(full_name)
    return None if repo_id is None else int(repo_id)


@app.get("/health")
def health() -> dict[str, Any]:
    artifacts = {
        "trending": _path_env("GHREC_TRENDING_PATH", DEFAULT_TRENDING_PATH).exists(),
        "related": _path_env("GHREC_RELATED_PATH", DEFAULT_RELATED_PATH).exists(),
        "metadata_db": _path_env("GHREC_REPO_METADATA_DB", DEFAULT_METADATA_DB).exists(),
        "name_lookup_db": _path_env("GHREC_REPO_NAME_LOOKUP_DB", DEFAULT_NAME_LOOKUP_DB).exists(),
    }
    return {"status": "ok", "service": "recsys-local-api", "artifacts": artifacts}


@app.get("/api/trending")
def trending(
    limit: int = Query(default=20, ge=1, le=100),
    language: str | None = None,
) -> dict[str, Any]:
    frame = _trending_frame()
    warnings: list[str] = []
    if frame.empty or "repo_id" not in frame.columns:
        warnings.append("trending artifact is missing; sample data returned")
        return {"items": _sample_response(limit, "Sample fallback"), "metadata": {"warnings": warnings}}

    rows = frame.copy()
    if language:
        meta = _metadata_by_repo_id()
        keep = {
            repo_id
            for repo_id, values in meta.items()
            if str(values.get("language") or "").lower() == language.lower()
        }
        rows = rows[rows["repo_id"].isin(keep)]
    sort_col = "trend_score" if "trend_score" in rows.columns else "recent_score"
    if sort_col in rows.columns:
        rows = rows.sort_values(sort_col, ascending=False)
    rows = rows.head(limit)
    columns = set(rows.columns)
    items = [
        _repo_item(
            int(row.repo_id),
            rank=idx,
            score=_score_from_row(row, columns),
            reason="Recent activity growth",
            extra={
                "recent_score": float(row.recent_score) if "recent_score" in columns else None,
                "growth_ratio": float(row.growth_ratio) if "growth_ratio" in columns else None,
            },
        )
        for idx, row in enumerate(rows.itertuples(index=False), start=1)
    ]
    return {"items": items, "metadata": {"warnings": warnings, "count": len(items)}}


@app.get("/api/users/{user_id}/recommendations")
def user_recommendations(
    user_id: int,
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    actor_id = int(user_id)
    warnings: list[str] = []
    try:
        payload = recommend_for_actor(actor_id=actor_id, k=limit, include_features=False, include_sources=True)
    except ActorNotFoundError as exc:
        warnings.append(f"personalized serving failed: {exc}")
        return {
            "user": {"actor_id": int(actor_id)},
            "items": _sample_response(limit, "Cold-start fallback"),
            "metadata": {"cold_start": True, "warnings": warnings},
        }
    except HTTPException as exc:
        detail = exc.detail if isinstance(exc.detail, dict) else {}
        error = detail.get("error", {}) if isinstance(detail, dict) else {}
        if exc.status_code == 404 and error.get("code") == "actor_not_found":
            warnings.append(f"personalized serving failed: {error.get('message', 'actor not found')}")
            return {
                "user": {"actor_id": int(actor_id)},
                "items": _sample_response(limit, "Cold-start fallback"),
                "metadata": {"cold_start": True, "warnings": warnings},
            }
        raise HTTPException(status_code=503, detail=detail or {"message": str(exc)}) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "error": {
                    "code": "personalized_serving_unavailable",
                    "message": str(exc),
                }
            },
        ) from exc

    items = []
    for row in payload.get("items", []):
        source = row.get("candidate_source")
        reason = "Ranked from your local interaction history"
        if source == "related_source":
            reason = "Related to repositories in this user's history"
        items.append(
            _repo_item(
                int(row["repo_id"]),
                rank=int(row["rank"]),
                score=float(row["score"]),
                reason=reason,
                extra={
                    "candidate_source": source,
                    "candidate_rank": row.get("candidate_rank"),
                    "source_score": row.get("source_score"),
                },
            )
        )
    return {
        "user": {"actor_id": int(actor_id)},
        "bundle_id": payload.get("bundle_id"),
        "items": items,
        "metadata": {**payload.get("metadata", {}), "cold_start": False, "warnings": warnings},
    }


@app.get("/api/users/by-login/{username}/recommendations")
def user_recommendations_by_login(
    username: str,
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    try:
        user = resolve_github_user(username)
    except Exception as exc:
        return {
            "user": {"username": username},
            "items": _sample_response(limit, "GitHub user lookup fallback"),
            "metadata": {"cold_start": True, "warnings": [f"github user lookup failed: {exc}"]},
        }
    payload = user_recommendations(user_id=user.actor_id, limit=limit)
    payload["user"] = {
        "username": user.username,
        "actor_id": user.actor_id,
        "url": user.html_url,
        "name": user.name,
        "public_repos": user.public_repos,
        "followers": user.followers,
    }
    return payload


@app.get("/api/repos/{owner}/{repo}/related")
def related_repositories(
    owner: str,
    repo: str,
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    repo_id = _resolve_repo_id(owner, repo)
    if repo_id is None:
        warnings = ["repo is not in local lookup; sample fallback returned"]
        return {
            "anchor": {
                "repo_id": None,
                "full_name": f"{owner}/{repo}",
                "url": f"https://github.com/{owner}/{repo}",
            },
            "items": _sample_response(limit, "Sample fallback"),
            "metadata": {"warnings": warnings, "count": min(limit, len(SAMPLE_ITEMS))},
        }
    return related_repositories_by_id(repo_id, limit=limit)


@app.get("/api/repos/by-id/{repo_id}/related")
def related_repositories_by_id(
    repo_id: int,
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    frame = _related_for_anchor(int(repo_id))
    warnings: list[str] = []
    if frame.empty or "related_repo_id" not in frame.columns:
        warnings.append("related artifact has no rows for this repo; trending fallback returned")
        fallback = trending(limit=limit)["items"]
        return {"anchor": _repo_meta(repo_id), "items": fallback, "metadata": {"warnings": warnings}}

    sort_col = "rank" if "rank" in frame.columns else "score"
    rows = frame.sort_values(sort_col, ascending=(sort_col == "rank")).head(limit)
    columns = set(rows.columns)
    items = [
        _repo_item(
            int(row.related_repo_id),
            rank=idx,
            score=_score_from_row(row, columns),
            reason="Co-occurs with the anchor repository in user histories",
            extra={
                "source": "repo2repo_cooccurrence",
                "cooc_users": int(row.cooc_users) if "cooc_users" in columns and pd.notna(row.cooc_users) else None,
            },
        )
        for idx, row in enumerate(rows.itertuples(index=False), start=1)
    ]
    return {
        "anchor": _repo_meta(repo_id),
        "items": items,
        "metadata": {"warnings": warnings, "count": len(items)},
    }
