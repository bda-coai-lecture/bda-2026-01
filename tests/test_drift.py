"""Unit tests for the input-drift core (ghrec.drift)."""

import numpy as np
import pytest

drift = pytest.importorskip("ghrec.drift")


def test_psi_zero_for_identical_distributions() -> None:
    props = [0.25, 0.25, 0.25, 0.25]
    assert drift.compute_psi(props, props) == pytest.approx(0.0, abs=1e-12)


def test_psi_positive_and_grows_with_divergence() -> None:
    ref = [0.25, 0.25, 0.25, 0.25]
    mild = [0.30, 0.25, 0.25, 0.20]
    severe = [0.70, 0.20, 0.05, 0.05]
    psi_mild = drift.compute_psi(ref, mild)
    psi_severe = drift.compute_psi(ref, severe)
    assert 0.0 < psi_mild < psi_severe


def test_build_reference_and_self_score_is_low() -> None:
    rng = np.random.default_rng(0)
    values = rng.normal(0.0, 1.0, size=5000)
    ref = drift.build_reference({"x": values}, layer="t", reference_window={}, n_bins=10)
    # Scoring the same population back should give near-zero PSI.
    psi = drift.score_window(ref, {"x": rng.normal(0.0, 1.0, size=5000)})["x"]
    assert psi < 0.05


def test_score_window_detects_shift() -> None:
    rng = np.random.default_rng(1)
    ref = drift.build_reference(
        {"x": rng.normal(0.0, 1.0, size=5000)}, layer="t", reference_window={}, n_bins=10
    )
    shifted = drift.score_window(ref, {"x": rng.normal(2.0, 1.0, size=5000)})["x"]
    assert shifted > 0.25  # large mean shift -> clearly elevated PSI


def test_reference_json_roundtrip(tmp_path) -> None:
    rng = np.random.default_rng(2)
    ref = drift.build_reference(
        {"x": rng.normal(size=1000)}, layer="t", reference_window={"start": "a"}, n_bins=8
    )
    path = tmp_path / "ref.json"
    ref.to_json(path)
    loaded = drift.DriftReference.from_json(path)
    assert loaded.layer == "t"
    assert loaded.features["x"].edges == ref.features["x"].edges
    assert loaded.features["x"].proportions == ref.features["x"].proportions


def test_calibrate_thresholds_orders_warn_below_alert() -> None:
    rng = np.random.default_rng(3)
    ref = drift.build_reference(
        {"x": rng.normal(size=4000)}, layer="t", reference_window={}, n_bins=10
    )
    windows = [{"x": rng.normal(size=2000)} for _ in range(60)]
    th = drift.calibrate_thresholds(ref, windows, warn_q=0.95, alert_q=0.99)
    assert th["x"]["warn"] <= th["x"]["alert"]
    assert th["x"]["n_windows"] == 60


def test_evaluate_classifies_against_thresholds() -> None:
    rng = np.random.default_rng(4)
    ref = drift.build_reference(
        {"x": rng.normal(size=4000)}, layer="t", reference_window={}, n_bins=10
    )
    thresholds = {"x": {"warn": 0.01, "alert": 0.05}}
    report = drift.evaluate(ref, {"x": rng.normal(3.0, 1.0, size=2000)}, thresholds)
    assert report["overall_status"] == "alert"
    assert report["features"][0]["feature"] == "x"
    assert report["features"][0]["status"] == "alert"


def test_evaluate_ok_when_no_drift() -> None:
    rng = np.random.default_rng(5)
    ref = drift.build_reference(
        {"x": rng.normal(size=4000)}, layer="t", reference_window={}, n_bins=10
    )
    thresholds = {"x": {"warn": 0.2, "alert": 0.5}}
    report = drift.evaluate(ref, {"x": rng.normal(size=2000)}, thresholds)
    assert report["overall_status"] == "ok"


def test_evaluate_zero_threshold_does_not_false_alert() -> None:
    """A degenerate feature with a zero noise floor must not alert at psi==0."""
    ref = drift.build_reference(
        {"x": np.zeros(1000)}, layer="t", reference_window={}, n_bins=10
    )
    thresholds = {"x": {"warn": 0.0, "alert": 0.0}}
    report = drift.evaluate(ref, {"x": np.zeros(500)}, thresholds)
    assert report["overall_status"] == "ok"
    assert report["features"][0]["status"] == "ok"


def test_recsys_adapter_drops_sparse_features() -> None:
    """feature_distributions drops features below min_finite."""
    recsys = pytest.importorskip("ghrec.drift_recsys")
    pd = pytest.importorskip("pandas")
    import tempfile

    df = pd.DataFrame(
        {
            "actor_id": np.arange(2000),
            "repo_id": np.arange(2000),
            "label": np.zeros(2000),
            "dense": np.random.default_rng(0).normal(size=2000),
            "sparse": [1.0] * 5 + [np.nan] * 1995,
        }
    )
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as fh:
        df.to_parquet(fh.name)
        dists = recsys.feature_distributions(
            fh.name, ["dense", "sparse"], max_sample=10000, min_finite=1000
        )
    assert "dense" in dists
    assert "sparse" not in dists
