# Recsys MLOps API Design

## Decision Boundary

Current production baseline stays fixed:

```text
candidate cache: retrieval_rerank_v2_week7_full_20260502_hybrid_als30_related140_recent30_pop0
ranker:          ranker_lgbm_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel.pkl
primary metric:  NDCG@100
```

Recent source-feature LGBM training did not beat this baseline. Source metadata should remain
in candidate artifacts for observability and future experiments, but it is not part of the
current production ranker contract.

## API Goals

1. Serve recommendations from a named artifact version.
2. Expose artifact, metric, and job metadata without opening local files manually.
3. Keep batch training/evaluation separate from low-latency serving.
4. Make metric names stable across CSV, JSON, MLflow, BigQuery, and API responses.
5. Support lecture/demo usage locally before adding managed infra.

Non-goals for the first API:

- No online training.
- No dashboard-only state.
- No automatic promotion based only on latest run.
- No ranker change unless an explicit model version is requested.

## Service Split

```text
Offline jobs
  build canonical dataset
  train retrieval
  hybridize candidates
  sample rerank data
  train reranker
  evaluate
  register artifact bundle

API service
  load promoted bundle
  recommend for actor_id
  inspect candidate sources
  expose metrics and run metadata
  trigger offline jobs later, behind explicit endpoints
```

First implementation can be a single FastAPI app, but it should preserve these boundaries in
module names and response contracts.

## Artifact Bundle Contract

An artifact bundle is the minimum deployable unit. It must be immutable after registration.

```json
{
  "bundle_id": "recsys_v2_20260502_hybrid_lgbm_n20",
  "status": "candidate|promoted|archived|failed",
  "created_at": "2026-05-24T00:00:00Z",
  "dataset_suffix": "retrieval_rerank_v2_week7_full_20260502",
  "candidate_suffix": "retrieval_rerank_v2_week7_full_20260502_hybrid_als30_related140_recent30_pop0",
  "ranker_suffix": "retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel",
  "paths": {
    "canonical": "data/features/recsys_v2/canonical_....parquet",
    "candidates": "data/features/recsys_v2/retrieval_candidates_....parquet",
    "retrieval_summary": "data/models/recsys_v2/retrieval_als_..._summary.json",
    "ranker_model": "data/models/recsys_v2/ranker_lgbm_....pkl",
    "ranker_summary": "data/models/recsys_v2/ranker_lgbm_..._summary.json",
    "eval_metrics": "data/results/recsys_v2/eval_metrics_....csv",
    "eval_summary": "data/results/recsys_v2/eval_..._summary.json"
  },
  "metrics": {
    "candidate.recall@100": 0.020927,
    "candidate.ndcg@100": 0.006098,
    "rerank.recall@100": 0.021773,
    "rerank.ndcg@100": 0.007082
  },
  "promotion": {
    "promoted_at": null,
    "promoted_by": null,
    "reason": null
  }
}
```

Registry storage for v1 can be a local JSON file:

```text
data/registry/recsys_bundles.json
data/registry/recsys_promoted.json
```

Later, the same schema can move to SQLite, BigQuery, MLflow model registry, or an object-store
manifest without changing API responses.

## Serving Endpoints

### Health

```http
GET /health
```

Response:

```json
{
  "status": "ok",
  "service": "recsys-api",
  "promoted_bundle_id": "recsys_v2_20260502_hybrid_lgbm_n20"
}
```

### Active Bundle

```http
GET /v1/recsys/bundles/active
```

Returns the promoted bundle manifest. This endpoint is the source of truth for what the API is
currently serving.

### List Bundles

```http
GET /v1/recsys/bundles?status=candidate&limit=20
```

Returns compact bundle records sorted by `created_at desc`.

### Promote Bundle

```http
POST /v1/recsys/bundles/{bundle_id}/promote
```

Request:

```json
{
  "promoted_by": "local",
  "reason": "Best NDCG@100 among policy-compatible runs"
}
```

Promotion rules for v1:

- Bundle must exist.
- Required artifact paths must exist.
- `rerank.ndcg@100` must be present.
- Promotion must be explicit; never auto-promote latest.

### Recommend For User

```http
POST /v1/recsys/recommendations
```

Request:

```json
{
  "actor_id": 12345,
  "k": 100,
  "bundle_id": null,
  "include_features": false,
  "include_sources": true
}
```

Response:

```json
{
  "actor_id": 12345,
  "bundle_id": "recsys_v2_20260502_hybrid_lgbm_n20",
  "items": [
    {
      "repo_id": 987,
      "rank": 1,
      "score": 0.183,
      "candidate_rank": 42,
      "candidate_source": "related_source",
      "source_rank": 3,
      "source_score": 4.92
    }
  ],
  "metadata": {
    "candidate_count": 300,
    "ranker": "lgbm_n20",
    "served_at": "2026-05-24T00:00:00Z"
  }
}
```

Behavior:

- Default `bundle_id=null` means active promoted bundle.
- Unknown `actor_id` returns `404` with `code=actor_not_found`.
- If a user has fewer than `k` candidates, return available rows and include a warning.
- Rerank features must use the ranker's stored `feature_names`; source features are only
  returned for inspection unless that ranker was trained with them.

### Explain User Candidates

```http
GET /v1/recsys/users/{actor_id}/candidates?bundle_id=...&limit=300
```

Returns candidate rows before reranking. This is mainly for MLOps/debugging:

```json
{
  "actor_id": 12345,
  "bundle_id": "recsys_v2_20260502_hybrid_lgbm_n20",
  "candidates": [
    {
      "repo_id": 987,
      "candidate_rank": 42,
      "retrieval_score": 0.31,
      "candidate_source": "related_source",
      "source_rank": 3,
      "source_score": 4.92
    }
  ],
  "source_counts": {
    "retrieval_hard": 30,
    "related_source": 140,
    "popular_recent": 30
  }
}
```

## Job Endpoints

Job endpoints should be optional in v1. If implemented, they enqueue or shell out to existing
scripts and return a job record. They should not block until the full training run finishes.

```http
POST /v1/recsys/jobs/evaluate
POST /v1/recsys/jobs/register-bundle
GET  /v1/recsys/jobs/{job_id}
```

Evaluation request:

```json
{
  "suffix": "retrieval_rerank_v2_week7_full_20260502_hybrid_als30_related140_recent30_pop0_lgbm_n20_t1",
  "canonical_path": "data/features/recsys_v2/canonical_retrieval_rerank_v2_week7_full_20260502.parquet",
  "candidate_path": "data/features/recsys_v2/retrieval_candidates_retrieval_rerank_v2_week7_full_20260502_hybrid_als30_related140_recent30_pop0.parquet",
  "ranker_path": "data/models/recsys_v2/ranker_lgbm_retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel.pkl",
  "k_values": [10, 50, 100, 200]
}
```

Register-bundle request:

```json
{
  "bundle_id": "recsys_v2_20260502_hybrid_lgbm_n20",
  "candidate_suffix": "retrieval_rerank_v2_week7_full_20260502_hybrid_als30_related140_recent30_pop0",
  "ranker_suffix": "retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel",
  "eval_suffix": "retrieval_rerank_v2_week7_full_20260502_hybrid_als30_related140_recent30_pop0_lgbm_n20_t1"
}
```

## Metric Naming

Use lowercase dotted names outside human-readable tables.

```text
candidate.precision@10
candidate.recall@10
candidate.ndcg@10
candidate.unique_recommended@10
rerank.precision@10
rerank.recall@10
rerank.ndcg@10
rerank.unique_recommended@10
```

Rules:

- `candidate.*` means the raw candidate ordering policy, usually `-candidate_rank`.
- `rerank.*` means final model score ordering.
- Metric suffix must include `@k` for top-k metrics.
- `eval_users` is a dimension/denominator, not a quality metric.
- Human tables may display `NDCG@100`, but persisted keys use `ndcg@100`.

## Error Contract

```json
{
  "error": {
    "code": "bundle_not_found",
    "message": "Bundle does not exist: abc",
    "details": {}
  }
}
```

Initial codes:

```text
bundle_not_found
active_bundle_not_configured
artifact_missing
actor_not_found
invalid_k
job_not_found
job_failed
```

## Implementation Plan

1. Add local registry helpers under `src/ghrec/mlops_registry.py`.
2. Add artifact loading and scoring helpers under `src/ghrec/recsys_serving.py`.
3. Add FastAPI app under `src/ghrec/api.py` or `app_recsys_api.py`.
4. Add a CLI script to register the current best bundle from existing summary files.
5. Add tests for manifest validation, metric normalization, and recommendation response shape.

Keep the first implementation read-only except `promote` and `register-bundle`. Training and
evaluation job orchestration can follow after the read path is stable.
