"""FastAPI app for the recommendation MLOps API."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from ghrec.mlops_registry import (
    ActiveBundleNotConfiguredError,
    ArtifactBundle,
    BundleNotFoundError,
    LocalBundleRegistry,
    RegistryError,
)
from ghrec.recsys_serving import (
    ActorNotFoundError,
    InvalidKError,
    RecommendationRequest,
    RecsysServingEngine,
    ServingError,
)


app = FastAPI(title="BDA Recsys MLOps API", version="0.1.0")


class RecommendBody(BaseModel):
    actor_id: int
    k: int = Field(default=100, ge=1, le=1000)
    bundle_id: str | None = None
    include_features: bool = False
    include_sources: bool = True


class PromoteBody(BaseModel):
    promoted_by: str = "local"
    reason: str | None = None


def error_response(exc: Exception, status_code: int) -> HTTPException:
    code = getattr(exc, "code", "internal_error")
    return HTTPException(
        status_code=status_code,
        detail={"error": {"code": code, "message": str(exc), "details": {}}},
    )


@lru_cache(maxsize=1)
def registry() -> LocalBundleRegistry:
    return LocalBundleRegistry()


@lru_cache(maxsize=8)
def serving_engine(bundle_id: str) -> RecsysServingEngine:
    bundle = registry().get_bundle(bundle_id)
    return RecsysServingEngine(bundle)


def resolve_bundle(bundle_id: str | None) -> ArtifactBundle:
    try:
        if bundle_id:
            return registry().get_bundle(bundle_id)
        return registry().active_bundle()
    except ActiveBundleNotConfiguredError as exc:
        raise error_response(exc, 503) from exc
    except BundleNotFoundError as exc:
        raise error_response(exc, 404) from exc


def get_active_bundle() -> dict[str, Any]:
    return registry().active_bundle().to_dict()


def recommend_for_actor(
    actor_id: int,
    k: int,
    bundle_id: str | None = None,
    include_features: bool = False,
    include_sources: bool = True,
) -> dict[str, Any]:
    bundle = resolve_bundle(bundle_id)
    return serving_engine(bundle.bundle_id).recommend(
        RecommendationRequest(
            actor_id=actor_id,
            k=k,
            include_features=include_features,
            include_sources=include_sources,
        )
    )


@app.get("/health")
def health() -> dict[str, Any]:
    try:
        active_id = registry().active_bundle_id()
    except ActiveBundleNotConfiguredError:
        active_id = None
    return {
        "status": "ok",
        "service": "recsys-api",
        "promoted_bundle_id": active_id,
    }


@app.get("/v1/recsys/bundles/active")
def active_bundle() -> dict[str, Any]:
    try:
        return get_active_bundle()
    except ActiveBundleNotConfiguredError as exc:
        raise error_response(exc, 503) from exc
    except BundleNotFoundError as exc:
        raise error_response(exc, 404) from exc


@app.get("/v1/recsys/bundles")
def list_bundles(
    status: str | None = None,
    limit: int = Query(default=20, ge=1, le=1000),
) -> list[dict[str, Any]]:
    return [bundle.to_dict() for bundle in registry().list_bundles(status=status, limit=limit)]


@app.post("/v1/recsys/bundles/{bundle_id}/promote")
def promote_bundle(bundle_id: str, body: PromoteBody) -> dict[str, Any]:
    try:
        promoted = registry().promote(bundle_id, promoted_by=body.promoted_by, reason=body.reason)
        serving_engine.cache_clear()
        return promoted.to_dict()
    except BundleNotFoundError as exc:
        raise error_response(exc, 404) from exc
    except RegistryError as exc:
        raise error_response(exc, 400) from exc


@app.post("/v1/recsys/recommendations")
def recommend(body: RecommendBody) -> dict[str, Any]:
    try:
        return recommend_for_actor(
            actor_id=body.actor_id,
            k=body.k,
            bundle_id=body.bundle_id,
            include_features=body.include_features,
            include_sources=body.include_sources,
        )
    except ActorNotFoundError as exc:
        raise error_response(exc, 404) from exc
    except InvalidKError as exc:
        raise error_response(exc, 422) from exc
    except ServingError as exc:
        raise error_response(exc, 500) from exc


@app.get("/v1/recsys/users/{actor_id}/candidates")
def explain_candidates(
    actor_id: int,
    bundle_id: str | None = None,
    limit: int = Query(default=300, ge=1, le=1000),
) -> dict[str, Any]:
    bundle = resolve_bundle(bundle_id)
    try:
        return serving_engine(bundle.bundle_id).explain_candidates(actor_id, limit=limit)
    except ActorNotFoundError as exc:
        raise error_response(exc, 404) from exc
    except InvalidKError as exc:
        raise error_response(exc, 422) from exc
    except ServingError as exc:
        raise error_response(exc, 500) from exc


@app.get("/v1/recsys/artifacts/exists")
def artifact_exists(path: str) -> dict[str, Any]:
    target = Path(path)
    return {"path": path, "exists": target.exists(), "is_file": target.is_file()}
