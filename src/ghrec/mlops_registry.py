"""Local registry for recommendation artifact bundles."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


DEFAULT_REGISTRY_DIR = Path("data/registry")
DEFAULT_BUNDLES_PATH = DEFAULT_REGISTRY_DIR / "recsys_bundles.json"
DEFAULT_PROMOTED_PATH = DEFAULT_REGISTRY_DIR / "recsys_promoted.json"

REQUIRED_PATH_KEYS = {
    "canonical",
    "candidates",
    "ranker_model",
    "ranker_summary",
    "eval_metrics",
    "eval_summary",
}


class RegistryError(RuntimeError):
    """Base registry error with API-friendly code."""

    code = "registry_error"


class BundleNotFoundError(RegistryError):
    code = "bundle_not_found"


class ActiveBundleNotConfiguredError(RegistryError):
    code = "active_bundle_not_configured"


class ArtifactMissingError(RegistryError):
    code = "artifact_missing"


@dataclass
class BundlePromotion:
    promoted_at: str | None = None
    promoted_by: str | None = None
    reason: str | None = None


@dataclass
class ArtifactBundle:
    bundle_id: str
    status: str
    created_at: str
    dataset_suffix: str
    candidate_suffix: str
    ranker_suffix: str
    paths: dict[str, str]
    metrics: dict[str, float] = field(default_factory=dict)
    promotion: BundlePromotion = field(default_factory=BundlePromotion)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ArtifactBundle":
        promotion = data.get("promotion") or {}
        return cls(
            bundle_id=str(data["bundle_id"]),
            status=str(data.get("status", "candidate")),
            created_at=str(data["created_at"]),
            dataset_suffix=str(data.get("dataset_suffix", "")),
            candidate_suffix=str(data.get("candidate_suffix", "")),
            ranker_suffix=str(data.get("ranker_suffix", "")),
            paths={str(k): str(v) for k, v in dict(data.get("paths", {})).items()},
            metrics={str(k): float(v) for k, v in dict(data.get("metrics", {})).items()},
            promotion=BundlePromotion(
                promoted_at=promotion.get("promoted_at"),
                promoted_by=promotion.get("promoted_by"),
                reason=promotion.get("reason"),
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "bundle_id": self.bundle_id,
            "status": self.status,
            "created_at": self.created_at,
            "dataset_suffix": self.dataset_suffix,
            "candidate_suffix": self.candidate_suffix,
            "ranker_suffix": self.ranker_suffix,
            "paths": dict(self.paths),
            "metrics": dict(self.metrics),
            "promotion": {
                "promoted_at": self.promotion.promoted_at,
                "promoted_by": self.promotion.promoted_by,
                "reason": self.promotion.reason,
            },
        }


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def normalize_metric_name(model: str, metric: str, k: int) -> str:
    model_key = model.strip().lower()
    if model_key in {"als retrieval", "retrieval", "candidate"}:
        prefix = "candidate"
    elif model_key in {"lgbm re-rank", "lgbm rerank", "rerank", "ranker"}:
        prefix = "rerank"
    else:
        prefix = model_key.replace(" ", "_").replace("-", "_")

    metric_key = metric.strip().lower()
    if metric_key == "unique_recommended":
        return f"{prefix}.unique_recommended@{k}"
    return f"{prefix}.{metric_key}@{k}"


def normalize_manifest_metrics(rows: list[dict[str, Any]]) -> dict[str, float]:
    """Convert eval CSV-style rows into stable manifest metric keys."""
    metrics: dict[str, float] = {}
    for row in rows:
        model = str(row["model"])
        k = int(row["k"])
        for metric in ["precision", "recall", "ndcg", "unique_recommended"]:
            if metric in row and row[metric] is not None:
                metrics[normalize_metric_name(model, metric, k)] = float(row[metric])
    return metrics


class LocalBundleRegistry:
    def __init__(
        self,
        bundles_path: Path = DEFAULT_BUNDLES_PATH,
        promoted_path: Path = DEFAULT_PROMOTED_PATH,
    ) -> None:
        self.bundles_path = bundles_path
        self.promoted_path = promoted_path

    def list_bundles(self, status: str | None = None, limit: int = 20) -> list[ArtifactBundle]:
        bundles = [ArtifactBundle.from_dict(row) for row in read_json(self.bundles_path, [])]
        if status:
            bundles = [bundle for bundle in bundles if bundle.status == status]
        bundles.sort(key=lambda bundle: bundle.created_at, reverse=True)
        return bundles[:limit]

    def get_bundle(self, bundle_id: str) -> ArtifactBundle:
        for bundle in self.list_bundles(status=None, limit=100000):
            if bundle.bundle_id == bundle_id:
                return bundle
        raise BundleNotFoundError(f"Bundle does not exist: {bundle_id}")

    def upsert_bundle(self, bundle: ArtifactBundle) -> ArtifactBundle:
        bundles = [ArtifactBundle.from_dict(row) for row in read_json(self.bundles_path, [])]
        replaced = False
        out = []
        for current in bundles:
            if current.bundle_id == bundle.bundle_id:
                out.append(bundle)
                replaced = True
            else:
                out.append(current)
        if not replaced:
            out.append(bundle)
        write_json(self.bundles_path, [row.to_dict() for row in out])
        return bundle

    def active_bundle_id(self) -> str:
        payload = read_json(self.promoted_path, {})
        bundle_id = payload.get("bundle_id")
        if not bundle_id:
            raise ActiveBundleNotConfiguredError("No active recommendation bundle is configured")
        return str(bundle_id)

    def active_bundle(self) -> ArtifactBundle:
        return self.get_bundle(self.active_bundle_id())

    def promote(self, bundle_id: str, promoted_by: str, reason: str | None = None) -> ArtifactBundle:
        bundle = self.get_bundle(bundle_id)
        validate_artifact_paths(bundle)
        bundle.status = "promoted"
        bundle.promotion = BundlePromotion(
            promoted_at=utc_now_iso(),
            promoted_by=promoted_by,
            reason=reason,
        )
        self.upsert_bundle(bundle)
        write_json(self.promoted_path, {"bundle_id": bundle.bundle_id})
        return bundle


def validate_artifact_paths(bundle: ArtifactBundle) -> None:
    missing_keys = sorted(REQUIRED_PATH_KEYS - set(bundle.paths))
    if missing_keys:
        raise ArtifactMissingError(f"Bundle is missing path keys: {missing_keys}")
    missing_files = sorted(str(path) for path in map(Path, bundle.paths.values()) if not path.exists())
    if missing_files:
        raise ArtifactMissingError(f"Bundle artifacts do not exist: {missing_files}")
