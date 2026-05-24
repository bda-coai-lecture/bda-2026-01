"""FastAPI response-shape contract tests for recommendation MLOps serving."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

pytest.importorskip("httpx", reason="fastapi TestClient requires httpx")

from fastapi.testclient import TestClient  # noqa: E402


api = pytest.importorskip("ghrec.api", reason="ghrec.api:app is not implemented yet")


ACTIVE_BUNDLE = {
    "bundle_id": "recsys_v2_20260502_hybrid_lgbm_n20",
    "status": "promoted",
    "created_at": "2026-05-24T00:00:00Z",
    "dataset_suffix": "retrieval_rerank_v2_week7_full_20260502",
    "candidate_suffix": (
        "retrieval_rerank_v2_week7_full_20260502_"
        "hybrid_als30_related140_recent30_pop0"
    ),
    "ranker_suffix": "retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel",
    "paths": {
        "canonical": "data/features/recsys_v2/canonical.parquet",
        "candidates": "data/features/recsys_v2/retrieval_candidates.parquet",
        "retrieval_summary": "data/models/recsys_v2/retrieval_summary.json",
        "ranker_model": "data/models/recsys_v2/ranker_lgbm.pkl",
        "ranker_summary": "data/models/recsys_v2/ranker_summary.json",
        "eval_metrics": "data/results/recsys_v2/eval_metrics.csv",
        "eval_summary": "data/results/recsys_v2/eval_summary.json",
    },
    "metrics": {
        "candidate.recall@100": 0.020926608608319477,
        "candidate.ndcg@100": 0.00609830458542892,
        "rerank.recall@100": 0.021772653333654638,
        "rerank.ndcg@100": 0.007082331681678839,
    },
    "promotion": {
        "promoted_at": "2026-05-24T00:00:00Z",
        "promoted_by": "pytest",
        "reason": "contract fixture",
    },
}


RECOMMENDATION = {
    "actor_id": 12345,
    "bundle_id": ACTIVE_BUNDLE["bundle_id"],
    "items": [
        {
            "repo_id": 987,
            "rank": 1,
            "score": 0.183,
            "candidate_rank": 42,
            "candidate_source": "related_source",
            "source_rank": 3,
            "source_score": 4.92,
        }
    ],
    "metadata": {
        "candidate_count": 300,
        "ranker": "lgbm_n20",
        "served_at": "2026-05-24T00:00:00Z",
    },
}


def _patch_first_existing(
    monkeypatch: pytest.MonkeyPatch,
    module: Any,
    names: tuple[str, ...],
    replacement: Callable[..., Any],
) -> str:
    for name in names:
        if hasattr(module, name):
            monkeypatch.setattr(module, name, replacement)
            return name
    pytest.skip(f"none of the expected test hooks exists: {', '.join(names)}")


def _app() -> Any:
    app = getattr(api, "app", None)
    if app is None:
        pytest.fail("ghrec.api must expose FastAPI app as `app`")
    return app


def test_expected_recsys_routes_exist() -> None:
    paths = {route.path for route in _app().routes}

    assert "/health" in paths
    assert "/v1/recsys/bundles/active" in paths
    assert "/v1/recsys/recommendations" in paths


def test_active_bundle_response_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_first_existing(
        monkeypatch,
        api,
        (
            "get_active_bundle",
            "load_active_bundle",
            "read_active_bundle",
        ),
        lambda *args, **kwargs: ACTIVE_BUNDLE,
    )

    response = TestClient(_app()).get("/v1/recsys/bundles/active")

    assert response.status_code == 200
    payload = response.json()
    assert payload["bundle_id"] == ACTIVE_BUNDLE["bundle_id"]
    assert payload["status"] == "promoted"
    assert payload["paths"]["ranker_model"].endswith(".pkl")
    assert payload["metrics"]["rerank.ndcg@100"] == pytest.approx(0.007082331681678839)
    assert payload["promotion"]["promoted_by"] == "pytest"
    for key in payload["metrics"]:
        assert key == key.lower()
        assert "." in key
        assert "@" in key


def test_recommendation_response_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_first_existing(
        monkeypatch,
        api,
        (
            "recommend_for_actor",
            "build_recommendations",
            "recommend",
        ),
        lambda *args, **kwargs: RECOMMENDATION,
    )

    response = TestClient(_app()).post(
        "/v1/recsys/recommendations",
        json={
            "actor_id": 12345,
            "k": 1,
            "bundle_id": None,
            "include_features": False,
            "include_sources": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["actor_id"] == 12345
    assert payload["bundle_id"] == ACTIVE_BUNDLE["bundle_id"]
    assert payload["metadata"]["candidate_count"] == 300
    assert payload["metadata"]["served_at"].endswith("Z")

    item = payload["items"][0]
    assert item == {
        "repo_id": 987,
        "rank": 1,
        "score": 0.183,
        "candidate_rank": 42,
        "candidate_source": "related_source",
        "source_rank": 3,
        "source_score": 4.92,
    }
