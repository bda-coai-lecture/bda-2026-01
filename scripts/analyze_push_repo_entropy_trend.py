#!/usr/bin/env python
"""Reproduce monthly entropy trend and single-breakpoint diagnostics.

Usage:
  uv run --with scipy python scripts/analyze_push_repo_entropy_trend.py
"""

from __future__ import annotations

import argparse
import csv
import math
from pathlib import Path

import numpy as np
from scipy.stats import kendalltau, theilslopes


DEFAULT_INPUT = Path("reports/push_repo_activation_entropy_monthly.csv")
METRICS = (
    ("entry_single", "avg_entropy"),
    ("entry_single", "low_entropy_share"),
    ("entry_single", "week_5_plus_return_share"),
    ("entry_single", "week_8_return_share"),
    ("entry_single", "entry_single_to_multi_share"),
    ("entry_multi", "avg_entropy"),
    ("entry_multi", "week_8_return_share"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    return parser.parse_args()


def read_series(path: Path) -> dict[tuple[str, str], list[float]]:
    rows = list(csv.DictReader(path.open(encoding="utf-8", newline="")))
    result: dict[tuple[str, str], list[float]] = {}
    for segment, metric in METRICS:
        selected = sorted(
            (
                row["cohort_month"],
                float(row[metric]),
            )
            for row in rows
            if row["entry_segment"] == segment and row[metric] != ""
        )
        result[(segment, metric)] = [value for _, value in selected]
    return result


def linear_rss(x: np.ndarray, y: np.ndarray) -> float:
    fitted = np.polyval(np.polyfit(x, y, 1), x)
    return float(np.sum((y - fitted) ** 2))


def bic(rss: float, n: int, parameters: int) -> float:
    return n * math.log(rss / n) + parameters * math.log(n)


def breakpoint_diagnostic(y: np.ndarray) -> tuple[int, float, float]:
    """Return best break-after index, piecewise-minus-linear BIC, linear BIC.

    Candidate models are two independent linear segments with at least three
    observations each. The piecewise model has four parameters versus two for
    a single linear trend.
    """
    x = np.arange(len(y), dtype=float)
    linear_bic = bic(linear_rss(x, y), len(y), 2)
    candidates: list[tuple[float, int]] = []
    for break_after in range(2, len(y) - 3):
        left = slice(0, break_after + 1)
        right = slice(break_after + 1, len(y))
        rss = linear_rss(x[left], y[left]) + linear_rss(x[right], y[right])
        candidates.append((bic(rss, len(y), 4), break_after))
    piecewise_bic, break_after = min(candidates)
    return break_after, piecewise_bic - linear_bic, linear_bic


def main() -> None:
    series = read_series(parse_args().input)
    for (segment, metric), values in series.items():
        y = np.asarray(values, dtype=float)
        x = np.arange(len(y), dtype=float)
        tau, p_value = kendalltau(x, y)
        slope, _, low, high = theilslopes(y, x, 0.95)
        print(
            f"{segment}.{metric}: "
            f"tau={tau:.3f} p={p_value:.4f} "
            f"theil_sen={slope:.6f}/month 95%CI=[{low:.6f},{high:.6f}]"
        )
        if metric in {"avg_entropy", "low_entropy_share"}:
            break_after, delta_bic, linear_bic = breakpoint_diagnostic(y)
            print(
                f"  breakpoint_candidate=after_index_{break_after} "
                f"piecewise_minus_linear_BIC={delta_bic:.2f} "
                f"linear_BIC={linear_bic:.2f}"
            )


if __name__ == "__main__":
    main()
