"""Serving helpers for the V2 recommendation pipeline."""

from __future__ import annotations

import json
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ghrec.mlops_registry import ArtifactBundle


SOURCE_HARD = "retrieval_hard"
SOURCE_POPULAR = "popular_recent"
SOURCE_RELATED = "related_source"
SOURCE_RANDOM = "random_catalog"
SOURCE_POSITIVE = "rank_label_positive"

SOURCE_CODE = {
    SOURCE_POSITIVE: 0,
    SOURCE_HARD: 1,
    SOURCE_POPULAR: 2,
    SOURCE_RELATED: 3,
    SOURCE_RANDOM: 4,
}
SOURCE_NAME = {value: key for key, value in SOURCE_CODE.items()}

LEGACY_FEATURE_COLUMNS = [
    "retrieval_score",
    "candidate_rank",
    "log_user_history_score",
    "log_user_history_repos",
    "log_item_history_score",
    "log_item_history_users",
    "user_item_history_seen",
    "user_history_score_share",
]

FEATURE_COLUMNS = [
    "retrieval_score",
    "candidate_rank",
    "candidate_source_code",
    "source_rank",
    "source_score",
    "log_user_history_score",
    "log_user_history_repos",
    "log_item_history_score",
    "log_item_history_users",
    "user_item_history_seen",
    "user_history_score_share",
]


class ServingError(RuntimeError):
    code = "serving_error"


class ActorNotFoundError(ServingError):
    code = "actor_not_found"


class InvalidKError(ServingError):
    code = "invalid_k"


@dataclass(frozen=True)
class RecommendationRequest:
    actor_id: int
    k: int = 100
    include_features: bool = False
    include_sources: bool = True


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def load_pickle(path: Path) -> Any:
    with path.open("rb") as f:
        return pickle.load(f)


def normalize_feedback(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            {
                "actor_id": pd.Series(dtype="int64"),
                "repo_id": pd.Series(dtype="int64"),
                "score": pd.Series(dtype="float32"),
            }
        )
    out = df.rename(columns={"weighted_score": "score"}).copy()
    out = out.dropna(subset=["actor_id", "repo_id", "score"])
    return out.astype({"actor_id": "int64", "repo_id": "int64", "score": "float32"})


def load_canonical(path: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_parquet(path)
    return (
        normalize_feedback(df[df["split"] == "history"]),
        normalize_feedback(df[df["split"] == "rank_label"]),
        normalize_feedback(df[df["split"] == "test"]),
    )


def seen_by_user(feedback: pd.DataFrame) -> dict[int, set[int]]:
    if feedback.empty:
        return {}
    return feedback.groupby("actor_id", observed=True)["repo_id"].apply(lambda s: set(map(int, s))).to_dict()


def feature_stats(history: pd.DataFrame) -> dict[str, Any]:
    user_hist = history.groupby("actor_id", observed=True).agg(
        user_history_score=("score", "sum"),
        user_history_repos=("repo_id", "nunique"),
    )
    item_hist = history.groupby("repo_id", observed=True).agg(
        item_history_score=("score", "sum"),
        item_history_users=("actor_id", "nunique"),
    )
    return {
        "user_hist": user_hist,
        "item_hist": item_hist,
        "max_user_history_score": float(user_hist["user_history_score"].max()) if len(user_hist) else 0.0,
        "history_seen": seen_by_user(history),
    }


def attach_features(rows: pd.DataFrame, stats: dict[str, Any]) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    out = rows.copy()
    out = out.merge(stats["user_hist"], left_on="actor_id", right_index=True, how="left")
    out = out.merge(stats["item_hist"], left_on="repo_id", right_index=True, how="left")
    fill_cols = [
        "user_history_score",
        "user_history_repos",
        "item_history_score",
        "item_history_users",
    ]
    out[fill_cols] = out[fill_cols].fillna(0)
    out["log_user_history_score"] = np.log1p(out["user_history_score"].astype(float))
    out["log_user_history_repos"] = np.log1p(out["user_history_repos"].astype(float))
    out["log_item_history_score"] = np.log1p(out["item_history_score"].astype(float))
    out["log_item_history_users"] = np.log1p(out["item_history_users"].astype(float))
    seen = stats["history_seen"]
    out["user_item_history_seen"] = [
        1.0 if int(r.repo_id) in seen.get(int(r.actor_id), set()) else 0.0
        for r in out[["actor_id", "repo_id"]].itertuples(index=False)
    ]
    out["user_history_score_share"] = (
        out["user_history_score"].astype(float) / (stats["max_user_history_score"] + 1e-6)
    )
    out["retrieval_score"] = out["retrieval_score"].fillna(0).astype("float32")
    out["candidate_rank"] = out["candidate_rank"].fillna(0).astype("float32")
    if "candidate_source_code" not in out.columns:
        out["candidate_source_code"] = SOURCE_CODE[SOURCE_HARD]
    if "source_rank" not in out.columns:
        out["source_rank"] = out["candidate_rank"]
    if "source_score" not in out.columns:
        out["source_score"] = out["retrieval_score"]
    out["candidate_source_code"] = out["candidate_source_code"].fillna(SOURCE_CODE[SOURCE_HARD]).astype("float32")
    out["source_rank"] = out["source_rank"].fillna(0).astype("float32")
    out["source_score"] = out["source_score"].fillna(0).astype("float32")
    return out


def predict_scores(model_payload: Any, features: pd.DataFrame, batch_size: int = 32768) -> np.ndarray:
    model = model_payload.get("model", model_payload) if isinstance(model_payload, dict) else model_payload
    if hasattr(model, "booster_"):
        chunks = [
            model.booster_.predict(features.iloc[start : start + batch_size])
            for start in range(0, len(features), batch_size)
        ]
    else:
        chunks = [
            model.predict(features.iloc[start : start + batch_size])
            for start in range(0, len(features), batch_size)
        ]
    if not chunks:
        return np.array([], dtype=np.float32)
    return np.concatenate(chunks).astype(np.float32, copy=False)


def feature_names_from_bundle(bundle: ArtifactBundle, model_payload: Any) -> list[str]:
    if isinstance(model_payload, dict) and model_payload.get("feature_names"):
        return list(model_payload["feature_names"])
    summary = read_json(Path(bundle.paths["ranker_summary"]))
    if summary.get("feature_names"):
        return list(summary["feature_names"])
    return LEGACY_FEATURE_COLUMNS


class RecsysServingEngine:
    def __init__(self, bundle: ArtifactBundle) -> None:
        self.bundle = bundle
        self._history: pd.DataFrame | None = None
        self._candidates: pd.DataFrame | None = None
        self._feature_stats: dict[str, Any] | None = None
        self._ranker: Any | None = None
        self._feature_names: list[str] | None = None

    @property
    def history(self) -> pd.DataFrame:
        if self._history is None:
            history, _, _ = load_canonical(Path(self.bundle.paths["canonical"]))
            self._history = history
        return self._history

    @property
    def candidates(self) -> pd.DataFrame:
        if self._candidates is None:
            self._candidates = pd.read_parquet(Path(self.bundle.paths["candidates"]))
        return self._candidates

    @property
    def stats(self) -> dict[str, Any]:
        if self._feature_stats is None:
            self._feature_stats = feature_stats(self.history)
        return self._feature_stats

    @property
    def ranker(self) -> Any:
        if self._ranker is None:
            self._ranker = load_pickle(Path(self.bundle.paths["ranker_model"]))
        return self._ranker

    @property
    def feature_names(self) -> list[str]:
        if self._feature_names is None:
            self._feature_names = feature_names_from_bundle(self.bundle, self.ranker)
        return self._feature_names

    def actor_candidates(self, actor_id: int, limit: int | None = None) -> pd.DataFrame:
        candidate_path = Path(self.bundle.paths["candidates"])
        rows = pd.read_parquet(
            candidate_path,
            filters=[("actor_id", "=", int(actor_id))],
        )
        if rows.empty:
            raise ActorNotFoundError(f"No candidates found for actor_id={actor_id}")
        rows = rows.sort_values("candidate_rank", ascending=True)
        if limit is not None:
            rows = rows.head(limit)
        return rows

    def recommend(self, request: RecommendationRequest) -> dict[str, Any]:
        if request.k <= 0 or request.k > 1000:
            raise InvalidKError("k must be between 1 and 1000")
        candidates = self.actor_candidates(request.actor_id)
        featured = attach_features(candidates, self.stats)
        missing = sorted(set(self.feature_names) - set(featured.columns))
        if missing:
            raise ServingError(f"Missing ranker features: {missing}")
        featured["score"] = predict_scores(self.ranker, featured[self.feature_names])
        ranked = featured.sort_values("score", ascending=False).head(request.k).reset_index(drop=True)

        items = []
        for idx, row in enumerate(ranked.itertuples(index=False), start=1):
            item = {
                "repo_id": int(row.repo_id),
                "rank": idx,
                "score": float(row.score),
                "candidate_rank": int(row.candidate_rank),
            }
            if request.include_sources:
                source_code = int(getattr(row, "candidate_source_code", SOURCE_CODE[SOURCE_HARD]))
                item.update(
                    {
                        "candidate_source": SOURCE_NAME.get(source_code, SOURCE_HARD),
                        "source_rank": int(getattr(row, "source_rank", row.candidate_rank)),
                        "source_score": float(getattr(row, "source_score", row.retrieval_score)),
                    }
                )
            if request.include_features:
                item["features"] = {
                    name: float(getattr(row, name))
                    for name in self.feature_names
                    if hasattr(row, name)
                }
            items.append(item)

        warnings = []
        if len(ranked) < request.k:
            warnings.append(f"Only {len(ranked)} candidates available for requested k={request.k}")
        return {
            "actor_id": int(request.actor_id),
            "bundle_id": self.bundle.bundle_id,
            "items": items,
            "metadata": {
                "candidate_count": int(len(candidates)),
                "ranker": self.bundle.ranker_suffix,
                "feature_names": list(self.feature_names),
                "warnings": warnings,
            },
        }

    def explain_candidates(self, actor_id: int, limit: int = 300) -> dict[str, Any]:
        if limit <= 0 or limit > 1000:
            raise InvalidKError("limit must be between 1 and 1000")
        rows = attach_features(self.actor_candidates(actor_id, limit=limit), self.stats)
        source_codes = rows["candidate_source_code"].astype(int)
        source_counts = {
            SOURCE_NAME.get(int(code), SOURCE_HARD): int(count)
            for code, count in source_codes.value_counts().sort_index().items()
        }
        candidates = []
        for row in rows.itertuples(index=False):
            source_code = int(getattr(row, "candidate_source_code", SOURCE_CODE[SOURCE_HARD]))
            candidates.append(
                {
                    "repo_id": int(row.repo_id),
                    "candidate_rank": int(row.candidate_rank),
                    "retrieval_score": float(row.retrieval_score),
                    "candidate_source": SOURCE_NAME.get(source_code, SOURCE_HARD),
                    "source_rank": int(getattr(row, "source_rank", row.candidate_rank)),
                    "source_score": float(getattr(row, "source_score", row.retrieval_score)),
                }
            )
        return {
            "actor_id": int(actor_id),
            "bundle_id": self.bundle.bundle_id,
            "candidates": candidates,
            "source_counts": source_counts,
        }
