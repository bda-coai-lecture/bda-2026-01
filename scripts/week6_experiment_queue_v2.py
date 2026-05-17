"""Run the Week 6 nDCG@100/200 improvement experiment queue.

This queue is intentionally a launcher only. Use ``--dry-run`` to inspect the
commands before running long experiments.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

MODEL_DIR = Path("data/models/week6")
LOG_DIR = MODEL_DIR / "logs"
TWO_STAGE_PATH = Path("scripts/week6_two_stage_v2.py")


SCREEN_ARGS = [
    "--sample-ratio",
    "0.06",
    "--max-items",
    "100000",
    "--candidate-k",
    "260",
    "--hybrid-extra",
    "160",
    "--rank-users",
    "9000",
    "--eval-users",
    "7000",
    "--qual-users",
    "120",
]


SCREEN_WIDE_ARGS = [
    "--sample-ratio",
    "0.06",
    "--max-items",
    "100000",
    "--candidate-k",
    "320",
    "--hybrid-extra",
    "220",
    "--rank-users",
    "9000",
    "--eval-users",
    "7000",
    "--qual-users",
    "120",
]


FULL_ARGS = [
    "--max-items",
    "300000",
    "--candidate-k",
    "360",
    "--hybrid-extra",
    "240",
    "--rank-users",
    "100000",
    "--eval-users",
    "10000000",
    "--qual-users",
    "300",
    "--save-user-diagnostics",
]


FULL_WIDE_ARGS = [
    "--max-items",
    "300000",
    "--candidate-k",
    "420",
    "--hybrid-extra",
    "280",
    "--rank-users",
    "100000",
    "--eval-users",
    "10000000",
    "--qual-users",
    "300",
    "--save-user-diagnostics",
]


@dataclass(frozen=True)
class Experiment:
    name: str
    tier: str
    hypothesis: str
    args: list[str]


EXPERIMENTS = [
    Experiment(
        name="screen_related120_anchor30_als128_a12_lgbm63",
        tier="screen",
        hypothesis="Increase related candidate recall and ALS capacity from the current best full run.",
        args=SCREEN_ARGS
        + [
            "--related-candidate-cap",
            "120",
            "--related-top-per-anchor",
            "12",
            "--related-max-seen-anchors",
            "30",
            "--factors",
            "128",
            "--iterations",
            "14",
            "--als-alpha",
            "1.2",
            "--als-regularization",
            "0.03",
            "--lgbm-estimators",
            "180",
            "--lgbm-learning-rate",
            "0.04",
            "--lgbm-num-leaves",
            "63",
            "--lgbm-min-child-samples",
            "60",
            "--lgbm-subsample",
            "0.9",
            "--lgbm-colsample",
            "0.85",
            "--lgbm-reg-l2",
            "0.1",
        ],
    ),
    Experiment(
        name="screen_related160_anchor40_als96_a16_lgbm127",
        tier="screen",
        hypothesis="Test a wider related pool with a larger ranker for nDCG@100 lift.",
        args=SCREEN_WIDE_ARGS
        + [
            "--related-candidate-cap",
            "160",
            "--related-top-per-anchor",
            "16",
            "--related-max-seen-anchors",
            "40",
            "--factors",
            "96",
            "--iterations",
            "16",
            "--als-alpha",
            "1.5",
            "--als-regularization",
            "0.05",
            "--lgbm-estimators",
            "240",
            "--lgbm-learning-rate",
            "0.03",
            "--lgbm-num-leaves",
            "127",
            "--lgbm-min-child-samples",
            "80",
            "--lgbm-subsample",
            "0.85",
            "--lgbm-colsample",
            "0.8",
            "--lgbm-reg-l2",
            "0.3",
        ],
    ),
    Experiment(
        name="screen_related80_anchor50_als160_a10_lgbm63",
        tier="screen",
        hypothesis="Keep related cap tighter but use more user anchors and higher ALS rank.",
        args=SCREEN_ARGS
        + [
            "--related-candidate-cap",
            "80",
            "--related-top-per-anchor",
            "20",
            "--related-max-seen-anchors",
            "50",
            "--factors",
            "160",
            "--iterations",
            "12",
            "--als-alpha",
            "1.0",
            "--als-regularization",
            "0.02",
            "--lgbm-estimators",
            "220",
            "--lgbm-learning-rate",
            "0.035",
            "--lgbm-num-leaves",
            "63",
            "--lgbm-min-child-samples",
            "40",
            "--lgbm-subsample",
            "0.95",
            "--lgbm-colsample",
            "0.9",
            "--lgbm-reg-l2",
            "0.05",
        ],
    ),
    Experiment(
        name="screen_related120_conservative_weights",
        tier="screen",
        hypothesis="Retry the best screening weight ablation with related candidates enabled.",
        args=SCREEN_ARGS
        + [
            "--use-marts",
            "never",
            "--related-candidate-cap",
            "120",
            "--related-top-per-anchor",
            "12",
            "--related-max-seen-anchors",
            "30",
            "--factors",
            "96",
            "--iterations",
            "14",
            "--als-alpha",
            "1.2",
            "--als-regularization",
            "0.03",
            "--lgbm-estimators",
            "200",
            "--lgbm-learning-rate",
            "0.04",
            "--lgbm-num-leaves",
            "63",
            "--lgbm-min-child-samples",
            "60",
            "--lgbm-subsample",
            "0.9",
            "--lgbm-colsample",
            "0.85",
            "--lgbm-reg-l2",
            "0.1",
            "--event-weight",
            "ForkEvent=1.5",
            "--event-weight",
            "PullRequestEvent=2.4",
            "--event-weight",
            "PushEvent=0.1",
        ],
    ),
    Experiment(
        name="screen_related120_explicit_interest_weights",
        tier="screen",
        hypothesis="Emphasize explicit watch/fork intent while retaining related candidate recall.",
        args=SCREEN_ARGS
        + [
            "--use-marts",
            "never",
            "--related-candidate-cap",
            "120",
            "--related-top-per-anchor",
            "12",
            "--related-max-seen-anchors",
            "30",
            "--factors",
            "96",
            "--iterations",
            "14",
            "--als-alpha",
            "1.2",
            "--als-regularization",
            "0.03",
            "--lgbm-estimators",
            "200",
            "--lgbm-learning-rate",
            "0.04",
            "--lgbm-num-leaves",
            "63",
            "--lgbm-min-child-samples",
            "70",
            "--lgbm-subsample",
            "0.9",
            "--lgbm-colsample",
            "0.85",
            "--lgbm-reg-l2",
            "0.2",
            "--event-weight",
            "WatchEvent=1.3",
            "--event-weight",
            "ForkEvent=2.4",
            "--event-weight",
            "IssuesEvent=0.35",
            "--event-weight",
            "IssueCommentEvent=0.2",
            "--event-weight",
            "PushEvent=0.1",
        ],
    ),
    Experiment(
        name="screen_related160_activity_balanced_weights",
        tier="screen",
        hypothesis="Balance activity-heavy events with stronger related recall to recover top-k quality.",
        args=SCREEN_WIDE_ARGS
        + [
            "--use-marts",
            "never",
            "--related-candidate-cap",
            "160",
            "--related-top-per-anchor",
            "16",
            "--related-max-seen-anchors",
            "40",
            "--factors",
            "128",
            "--iterations",
            "16",
            "--als-alpha",
            "1.5",
            "--als-regularization",
            "0.05",
            "--lgbm-estimators",
            "240",
            "--lgbm-learning-rate",
            "0.03",
            "--lgbm-num-leaves",
            "127",
            "--lgbm-min-child-samples",
            "100",
            "--lgbm-subsample",
            "0.85",
            "--lgbm-colsample",
            "0.8",
            "--lgbm-reg-l2",
            "0.5",
            "--event-weight",
            "IssuesEvent=0.7",
            "--event-weight",
            "IssueCommentEvent=0.5",
            "--event-weight",
            "PushEvent=0.35",
            "--event-weight",
            "PullRequestEvent=2.6",
        ],
    ),
    Experiment(
        name="full_related120_anchor30_als128_lgbm63",
        tier="full",
        hypothesis="Promote the moderate related/ALS capacity candidate if screens confirm lift.",
        args=FULL_ARGS
        + [
            "--related-candidate-cap",
            "120",
            "--related-top-per-anchor",
            "12",
            "--related-max-seen-anchors",
            "30",
            "--factors",
            "128",
            "--iterations",
            "14",
            "--als-alpha",
            "1.2",
            "--als-regularization",
            "0.03",
            "--lgbm-estimators",
            "220",
            "--lgbm-learning-rate",
            "0.035",
            "--lgbm-num-leaves",
            "63",
            "--lgbm-min-child-samples",
            "60",
            "--lgbm-subsample",
            "0.9",
            "--lgbm-colsample",
            "0.85",
            "--lgbm-reg-l2",
            "0.1",
        ],
    ),
    Experiment(
        name="full_related160_anchor40_als96_lgbm127",
        tier="full",
        hypothesis="Promote the high-recall related candidate for nDCG@100 and future @200 checks.",
        args=FULL_WIDE_ARGS
        + [
            "--related-candidate-cap",
            "160",
            "--related-top-per-anchor",
            "16",
            "--related-max-seen-anchors",
            "40",
            "--factors",
            "96",
            "--iterations",
            "16",
            "--als-alpha",
            "1.5",
            "--als-regularization",
            "0.05",
            "--lgbm-estimators",
            "260",
            "--lgbm-learning-rate",
            "0.03",
            "--lgbm-num-leaves",
            "127",
            "--lgbm-min-child-samples",
            "80",
            "--lgbm-subsample",
            "0.85",
            "--lgbm-colsample",
            "0.8",
            "--lgbm-reg-l2",
            "0.3",
        ],
    ),
]


def supports_k_values() -> bool:
    if not TWO_STAGE_PATH.exists():
        return False
    return "--k-values" in TWO_STAGE_PATH.read_text(encoding="utf-8")


def utc_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def build_command(exp: Experiment, run_id: str, k_values_supported: bool) -> tuple[list[str], str]:
    suffix = f"queue_v2_{run_id}_{exp.name}"
    cmd = [
        "uv",
        "run",
        "python",
        str(TWO_STAGE_PATH),
        *exp.args,
    ]
    if k_values_supported:
        cmd.extend(["--k-values", "10,50,100,200"])
    cmd.extend(["--output-suffix", suffix])
    return cmd, suffix


def run_one(exp: Experiment, cmd: list[str], suffix: str, dry_run: bool) -> dict:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{suffix}.log"
    started = time.time()

    if dry_run:
        print(" ".join(cmd))
        return {
            "name": exp.name,
            "tier": exp.tier,
            "suffix": suffix,
            "status": "dry_run",
            "log_path": str(log_path),
            "cmd": cmd,
            "hypothesis": exp.hypothesis,
        }

    print(f"\n=== {exp.name} ===")
    print(" ".join(cmd))
    with log_path.open("w", encoding="utf-8") as log_file:
        proc = subprocess.run(cmd, stdout=log_file, stderr=subprocess.STDOUT, check=False)

    return {
        "name": exp.name,
        "tier": exp.tier,
        "suffix": suffix,
        "status": "succeeded" if proc.returncode == 0 else "failed",
        "returncode": proc.returncode,
        "elapsed_min": round((time.time() - started) / 60, 2),
        "log_path": str(log_path),
        "cmd": cmd,
        "hypothesis": exp.hypothesis,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Print commands without running them.")
    parser.add_argument(
        "--only",
        nargs="*",
        default=None,
        help="Experiment names or tiers to run. Tiers: screen, full.",
    )
    parser.add_argument(
        "--continue-on-failure",
        action="store_true",
        help="Continue the queue if one experiment fails.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional suffix namespace. Defaults to a UTC timestamp.",
    )
    return parser.parse_args()


def select_experiments(only: list[str] | None) -> list[Experiment]:
    if not only:
        return list(EXPERIMENTS)

    wanted = set(only)
    selected = [exp for exp in EXPERIMENTS if exp.name in wanted or exp.tier in wanted]
    matched = {exp.name for exp in selected} | {exp.tier for exp in selected if exp.tier in wanted}
    missing = sorted(wanted - matched)
    if missing:
        available = [exp.name for exp in EXPERIMENTS] + ["screen", "full"]
        raise SystemExit(f"unknown --only values: {missing}; available: {available}")
    return selected


def write_summary(path: Path, payload: dict) -> None:
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    run_id = args.run_id or utc_run_id()
    k_values_supported = supports_k_values()
    selected = select_experiments(args.only)
    summary_path = MODEL_DIR / f"week6_experiment_queue_v2_summary_{run_id}.json"

    results = []
    payload = {
        "run_id": run_id,
        "dry_run": args.dry_run,
        "continue_on_failure": args.continue_on_failure,
        "two_stage_path": str(TWO_STAGE_PATH),
        "k_values_requested": [10, 50, 100, 200],
        "k_values_supported": k_values_supported,
        "evaluation_note": (
            "Passing --k-values 10,50,100,200 to week6_two_stage_v2.py."
            if k_values_supported
            else "week6_two_stage_v2.py has no --k-values option; queued commands will evaluate its built-in [10, 50, 100] only, so @200 is not measured by this run."
        ),
        "experiments": results,
    }

    for exp in selected:
        cmd, suffix = build_command(exp, run_id, k_values_supported)
        result = run_one(exp, cmd, suffix, args.dry_run)
        results.append(result)
        write_summary(summary_path, payload)
        if not args.dry_run and result.get("status") == "failed":
            print(f"failed: {exp.name} log={result['log_path']}", file=sys.stderr)
            if not args.continue_on_failure:
                break

    write_summary(summary_path, payload)
    print(f"\nsummary: {summary_path}")


if __name__ == "__main__":
    main()
