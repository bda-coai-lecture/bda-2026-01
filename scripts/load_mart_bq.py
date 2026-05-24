"""Compatibility wrapper for the safer BigQuery metric sync script.

Prefer using scripts/sync_bq_metrics.py directly. This wrapper preserves the
old entry point while defaulting to a bounded BigQuery fact and metric refresh.
"""

from __future__ import annotations

import sys

from sync_bq_metrics import main


DEFAULT_ARGS = [
    "--project",
    "bda-coai",
    "--dataset",
    "mart",
    "--source",
    "bigquery",
    "--start",
    "2026-04-04",
    "--end",
    "2026-05-08",
    "--max-days",
    "35",
    "--mode",
    "replace-all",
    "--build-metrics",
]


if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.argv.extend(DEFAULT_ARGS)
    main()
