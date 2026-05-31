"""Input-feature drift core: PSI, reference bins, backtest threshold calibration.

Layer-agnostic. Every drift target is reduced to a numeric matrix indexed by day
with one column per feature. A fixed reference window defines quantile bins and
reference proportions; per-feature PSI compares a current window against them.
Thresholds are calibrated from the empirical PSI distribution of normal day-to-day
variation (backtest), not textbook 0.1/0.25 cutoffs.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Sequence

import numpy as np

# Floor applied to every bin proportion so empty bins do not blow PSI up to inf.
PSI_EPSILON = 1e-4


@dataclass
class FeatureReference:
    """Quantile bin edges and reference proportions for one feature."""

    name: str
    edges: list[float]
    proportions: list[float]
    n_ref: int


@dataclass
class DriftReference:
    """Reference distribution for a whole layer."""

    layer: str
    created_at: str
    reference_window: dict[str, str]
    features: dict[str, FeatureReference] = field(default_factory=dict)

    def to_json(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "layer": self.layer,
            "created_at": self.created_at,
            "reference_window": self.reference_window,
            "features": {k: asdict(v) for k, v in self.features.items()},
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    @classmethod
    def from_json(cls, path: str | Path) -> "DriftReference":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        features = {
            k: FeatureReference(**v) for k, v in payload.get("features", {}).items()
        }
        return cls(
            layer=payload["layer"],
            created_at=payload["created_at"],
            reference_window=payload["reference_window"],
            features=features,
        )


def _quantile_edges(values: np.ndarray, n_bins: int) -> np.ndarray:
    """Quantile bin edges with open outer edges and deduped interior cuts.

    Interior cuts come from reference quantiles so bins are roughly equal-mass.
    Outer edges are -inf/+inf so future out-of-range values still land in a bin.
    """
    values = values[np.isfinite(values)]
    if values.size == 0:
        return np.array([-np.inf, np.inf])
    qs = np.linspace(0.0, 1.0, n_bins + 1)[1:-1]
    interior = np.unique(np.quantile(values, qs))
    return np.concatenate(([-np.inf], interior, [np.inf]))


def _proportions(values: np.ndarray, edges: np.ndarray) -> np.ndarray:
    """Smoothed bin proportions for `values` under fixed `edges`."""
    values = values[np.isfinite(values)]
    counts, _ = np.histogram(values, bins=edges)
    props = counts / max(counts.sum(), 1)
    props = np.clip(props, PSI_EPSILON, None)
    return props / props.sum()


def compute_psi(ref_props: Sequence[float], cur_props: Sequence[float]) -> float:
    """Population Stability Index between two aligned proportion vectors."""
    ref = np.clip(np.asarray(ref_props, dtype=float), PSI_EPSILON, None)
    cur = np.clip(np.asarray(cur_props, dtype=float), PSI_EPSILON, None)
    ref = ref / ref.sum()
    cur = cur / cur.sum()
    return float(np.sum((cur - ref) * np.log(cur / ref)))


def build_reference(
    matrix: dict[str, np.ndarray],
    layer: str,
    reference_window: dict[str, str],
    n_bins: int = 10,
) -> DriftReference:
    """Build per-feature quantile bins + reference proportions from a day matrix.

    `matrix` maps feature name -> 1-D array of reference-window daily values.
    """
    ref = DriftReference(
        layer=layer,
        created_at=datetime.now(UTC).isoformat(),
        reference_window=reference_window,
    )
    for name, values in matrix.items():
        values = np.asarray(values, dtype=float)
        edges = _quantile_edges(values, n_bins)
        props = _proportions(values, edges)
        ref.features[name] = FeatureReference(
            name=name,
            edges=edges.tolist(),
            proportions=props.tolist(),
            n_ref=int(np.isfinite(values).sum()),
        )
    return ref


def score_window(
    reference: DriftReference, window: dict[str, np.ndarray]
) -> dict[str, float]:
    """PSI per feature for a current window keyed the same way as the reference."""
    out: dict[str, float] = {}
    for name, fref in reference.features.items():
        if name not in window:
            continue
        edges = np.asarray(fref.edges, dtype=float)
        cur_props = _proportions(np.asarray(window[name], dtype=float), edges)
        out[name] = compute_psi(fref.proportions, cur_props)
    return out


def calibrate_thresholds(
    reference: DriftReference,
    backtest_windows: Sequence[dict[str, np.ndarray]],
    warn_q: float = 0.95,
    alert_q: float = 0.99,
) -> dict[str, dict[str, float]]:
    """Per-feature warn/alert PSI from the empirical null distribution.

    `backtest_windows` are normal-period current windows. The PSI each produces
    against the reference is the natural day-to-day variation; warn/alert are
    upper quantiles of that null.
    """
    samples: dict[str, list[float]] = {name: [] for name in reference.features}
    for window in backtest_windows:
        psis = score_window(reference, window)
        for name, value in psis.items():
            samples[name].append(value)

    thresholds: dict[str, dict[str, float]] = {}
    for name, values in samples.items():
        if not values:
            continue
        arr = np.asarray(values, dtype=float)
        thresholds[name] = {
            "warn": float(np.quantile(arr, warn_q)),
            "alert": float(np.quantile(arr, alert_q)),
            "n_windows": int(arr.size),
            "null_max": float(arr.max()),
            "null_median": float(np.median(arr)),
        }
    return thresholds


def evaluate(
    reference: DriftReference,
    window: dict[str, np.ndarray],
    thresholds: dict[str, dict[str, float]],
) -> dict:
    """Score a window and classify each feature against calibrated thresholds."""
    psis = score_window(reference, window)
    features = []
    worst = "ok"
    for name, psi in sorted(psis.items(), key=lambda kv: kv[1], reverse=True):
        th = thresholds.get(name, {})
        warn = th.get("warn")
        alert = th.get("alert")
        # Strict '>' so a degenerate feature whose noise floor is exactly 0
        # (constant within bins) does not false-alert at psi == 0.
        status = "ok"
        if alert is not None and psi > alert:
            status = "alert"
        elif warn is not None and psi > warn:
            status = "warn"
        if status == "alert" or (status == "warn" and worst != "alert"):
            worst = status
        features.append(
            {"feature": name, "psi": psi, "warn": warn, "alert": alert, "status": status}
        )
    return {
        "layer": reference.layer,
        "evaluated_at": datetime.now(UTC).isoformat(),
        "overall_status": worst,
        "features": features,
    }
