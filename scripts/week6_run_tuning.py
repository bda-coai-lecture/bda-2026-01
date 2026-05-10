"""Run a small Week 6 recommender tuning queue with unique output suffixes."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

MODEL_DIR = Path("data/models/week6")
LOG_DIR = MODEL_DIR / "logs"


BASE_ARGS = [
    "--max-items",
    "300000",
    "--candidate-k",
    "300",
    "--hybrid-extra",
    "200",
    "--rank-users",
    "100000",
    "--eval-users",
    "10000000",
    "--qual-users",
    "300",
]


SCREEN_ARGS = [
    "--sample-ratio",
    "0.08",
    "--max-items",
    "120000",
    "--candidate-k",
    "220",
    "--hybrid-extra",
    "120",
    "--rank-users",
    "10000",
    "--eval-users",
    "8000",
    "--qual-users",
    "100",
]


EXPERIMENTS = [
    {
        "name": "screen_als64_i12_lgbm31",
        "args": SCREEN_ARGS
        + [
            "--factors",
            "64",
            "--iterations",
            "12",
            "--als-regularization",
            "0.01",
            "--lgbm-num-leaves",
            "31",
            "--lgbm-min-child-samples",
            "50",
        ],
    },
    {
        "name": "screen_als96_i12_lgbm63",
        "args": SCREEN_ARGS
        + [
            "--factors",
            "96",
            "--iterations",
            "12",
            "--als-regularization",
            "0.03",
            "--lgbm-num-leaves",
            "63",
            "--lgbm-min-child-samples",
            "50",
            "--lgbm-colsample",
            "0.85",
        ],
    },
    {
        "name": "screen_als64_i16_lgbm200",
        "args": SCREEN_ARGS
        + [
            "--factors",
            "64",
            "--iterations",
            "16",
            "--als-regularization",
            "0.05",
            "--lgbm-estimators",
            "200",
            "--lgbm-learning-rate",
            "0.035",
            "--lgbm-num-leaves",
            "31",
            "--lgbm-min-child-samples",
            "100",
            "--lgbm-colsample",
            "0.85",
        ],
    },
    {
        "name": "screen_bpr64_i80",
        "args": SCREEN_ARGS
        + [
            "--retrieval-model",
            "bpr",
            "--factors",
            "64",
            "--iterations",
            "80",
            "--als-regularization",
            "0.01",
            "--bpr-learning-rate",
            "0.01",
            "--lgbm-num-leaves",
            "31",
            "--lgbm-min-child-samples",
            "50",
        ],
    },
    {
        "name": "full_als96_i12_lgbm63",
        "args": BASE_ARGS
        + [
            "--factors",
            "96",
            "--iterations",
            "12",
            "--als-regularization",
            "0.03",
            "--lgbm-num-leaves",
            "63",
            "--lgbm-min-child-samples",
            "50",
            "--lgbm-colsample",
            "0.85",
        ],
    },
]


def run_one(name: str, args: list[str], dry_run: bool) -> dict:
    suffix = f"tune_{name}"
    cmd = [
        "uv",
        "run",
        "python",
        "scripts/week6_two_stage_v2.py",
        *args,
        "--output-suffix",
        suffix,
    ]
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{suffix}.log"
    started = time.time()
    if dry_run:
        print(" ".join(cmd))
        return {"name": name, "suffix": suffix, "dry_run": True, "cmd": cmd}

    print(f"\n=== {name} ===")
    print(" ".join(cmd))
    with log_path.open("w", encoding="utf-8") as log_file:
        proc = subprocess.run(cmd, stdout=log_file, stderr=subprocess.STDOUT, check=False)
    elapsed_min = round((time.time() - started) / 60, 2)
    return {
        "name": name,
        "suffix": suffix,
        "returncode": proc.returncode,
        "elapsed_min": elapsed_min,
        "log_path": str(log_path),
        "cmd": cmd,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only", nargs="*", default=None)
    args = parser.parse_args()

    selected = EXPERIMENTS
    if args.only:
        selected = [exp for exp in EXPERIMENTS if exp["name"] in set(args.only)]
        missing = sorted(set(args.only) - {exp["name"] for exp in selected})
        if missing:
            raise SystemExit(f"unknown experiments: {missing}")

    results = []
    for exp in selected:
        result = run_one(exp["name"], exp["args"], args.dry_run)
        results.append(result)
        if not args.dry_run and result.get("returncode") != 0:
            print(f"failed: {exp['name']} log={result['log_path']}", file=sys.stderr)
            break

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = MODEL_DIR / "week6_tuning_queue_summary.json"
    summary_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\nsummary: {summary_path}")


if __name__ == "__main__":
    main()

