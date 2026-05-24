"""Contract tests for recommendation MLOps bundle metric normalization."""

import pytest


registry = pytest.importorskip(
    "ghrec.mlops_registry",
    reason="registry helpers are not implemented yet",
)


def test_normalize_manifest_metrics_from_eval_csv_rows() -> None:
    """CSV-style eval rows should become stable API/manifest metric keys."""
    normalize_manifest_metrics = getattr(registry, "normalize_manifest_metrics", None)
    if normalize_manifest_metrics is None:
        pytest.fail("ghrec.mlops_registry.normalize_manifest_metrics is required")

    raw_rows = [
        {
            "model": "ALS Retrieval",
            "k": 100,
            "eval_users": 126365,
            "precision": 0.0003687729988525304,
            "recall": 0.020926608608319477,
            "ndcg": 0.00609830458542892,
            "unique_recommended": 41200,
        },
        {
            "model": "LGBM Re-rank",
            "k": 100,
            "eval_users": 126365,
            "precision": 0.00037668658251889374,
            "recall": 0.021772653333654638,
            "ndcg": 0.007082331681678839,
            "unique_recommended": 37465,
        },
    ]

    metrics = normalize_manifest_metrics(raw_rows)

    assert metrics == {
        "candidate.precision@100": 0.0003687729988525304,
        "candidate.recall@100": 0.020926608608319477,
        "candidate.ndcg@100": 0.00609830458542892,
        "candidate.unique_recommended@100": 41200,
        "rerank.precision@100": 0.00037668658251889374,
        "rerank.recall@100": 0.021772653333654638,
        "rerank.ndcg@100": 0.007082331681678839,
        "rerank.unique_recommended@100": 37465,
    }
    assert "eval_users" not in metrics


def test_normalized_manifest_metric_keys_are_lowercase_dotted_topk_keys() -> None:
    normalize_manifest_metrics = getattr(registry, "normalize_manifest_metrics", None)
    if normalize_manifest_metrics is None:
        pytest.fail("ghrec.mlops_registry.normalize_manifest_metrics is required")

    metrics = normalize_manifest_metrics(
        [
            {
                "model": "LGBM Re-rank",
                "k": 10,
                "precision": 0.1,
                "recall": 0.2,
                "ndcg": 0.3,
                "unique_recommended": 4,
            }
        ]
    )

    assert set(metrics) == {
        "rerank.precision@10",
        "rerank.recall@10",
        "rerank.ndcg@10",
        "rerank.unique_recommended@10",
    }
    for key in metrics:
        assert key == key.lower()
        assert key.count(".") == 1
        assert "@" in key
        assert " " not in key
