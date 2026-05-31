"""Contract tests for the read-only local API used by the Vercel frontend."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("httpx", reason="fastapi TestClient requires httpx")

from fastapi.testclient import TestClient  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ghrec import local_api  # noqa: E402


def _clear_caches() -> None:
    local_api._metadata_by_repo_id.cache_clear()
    local_api._name_lookup_by_repo_id.cache_clear()
    local_api._repo_id_by_name.cache_clear()
    local_api._trending_frame.cache_clear()
    local_api._related_for_anchor.cache_clear()


@pytest.fixture(autouse=True)
def clear_local_api_caches() -> None:
    _clear_caches()
    yield
    _clear_caches()


def test_health_contract() -> None:
    response = TestClient(local_api.app).get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["service"] == "recsys-local-api"
    assert set(payload["artifacts"]) == {
        "trending",
        "related",
        "metadata_db",
        "name_lookup_db",
    }


def test_trending_falls_back_when_artifact_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GHREC_TRENDING_PATH", "data/does-not-exist.parquet")
    _clear_caches()

    response = TestClient(local_api.app).get("/api/trending?limit=2")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["items"]) == 2
    assert payload["items"][0]["full_name"] == "facebook/react"
    assert payload["metadata"]["warnings"]


def test_user_recommendations_enrich_repo_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        local_api,
        "recommend_for_actor",
        lambda **kwargs: {
            "actor_id": kwargs["actor_id"],
            "bundle_id": "bundle-a",
            "items": [
                {
                    "repo_id": 1,
                    "rank": 1,
                    "score": 0.9,
                    "candidate_source": "related_source",
                    "candidate_rank": 7,
                    "source_score": 3.1,
                }
            ],
            "metadata": {"candidate_count": 12},
        },
    )
    monkeypatch.setattr(
        local_api,
        "_metadata_by_repo_id",
        lambda: {
            1: {
                "repo_id": 1,
                "full_name": "owner/repo",
                "description": "demo",
                "language": "Python",
                "stars": 10,
                "forks": 2,
                "topics": ["rec"],
            }
        },
    )

    response = TestClient(local_api.app).get("/api/users/42/recommendations?limit=1")

    assert response.status_code == 200
    payload: dict[str, Any] = response.json()
    assert payload["bundle_id"] == "bundle-a"
    assert payload["metadata"]["cold_start"] is False
    assert payload["items"][0]["full_name"] == "owner/repo"
    assert payload["items"][0]["candidate_source"] == "related_source"
    assert payload["items"][0]["cache"]["status"] == "missing"


def test_related_by_owner_repo_uses_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    import pandas as pd

    monkeypatch.setattr(local_api, "_repo_id_by_name", lambda: {"owner/anchor": 1})
    monkeypatch.setattr(local_api, "_metadata_by_repo_id", lambda: {})
    monkeypatch.setattr(local_api, "_name_lookup_by_repo_id", lambda: {1: "owner/anchor", 2: "owner/related"})
    monkeypatch.setattr(
        local_api,
        "_related_for_anchor",
        lambda repo_id: pd.DataFrame(
            [
                {
                    "anchor_repo_id": repo_id,
                    "related_repo_id": 2,
                    "rank": 1,
                    "score": 0.5,
                    "cooc_users": 3,
                }
            ]
        ),
    )

    response = TestClient(local_api.app).get("/api/repos/owner/anchor/related?limit=1")

    assert response.status_code == 200
    payload = response.json()
    assert payload["anchor"]["full_name"] == "owner/anchor"
    assert payload["items"][0]["full_name"] == "owner/related"
    assert payload["items"][0]["source"] == "repo2repo_cooccurrence"


def test_repo_meta_marks_non_200_cache_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        local_api,
        "_metadata_by_repo_id",
        lambda: {
            1: {
                "repo_id": 1,
                "full_name": "owner/deleted",
                "cache": {
                    "source": "repo_metadata.sqlite",
                    "status": "unavailable",
                    "fetched_at": "2026-05-31T00:00:00+00:00",
                    "http_status": 404,
                },
            }
        },
    )
    monkeypatch.setattr(local_api, "_name_lookup_by_repo_id", lambda: {})

    item = local_api._repo_meta(1)

    assert item["full_name"] == "owner/deleted"
    assert item["cache"]["status"] == "unavailable"
    assert item["cache"]["http_status"] == 404
