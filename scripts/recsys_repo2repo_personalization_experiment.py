"""Generate or run V2 related-source comparison commands.

This runner compares how repo-to-repo related candidates affect personalized
recommendation quality:

1. related off
2. rule-based repo_repo_related_mart
3. ML repo-to-repo related parquet used as repo_repo_related_mart

By default it prints the command sequence only. Pass --execute to run it.
"""

from __future__ import annotations

import argparse
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq


DEFAULT_MART_DIR = Path("data/marts/week6")
DEFAULT_TMP_ROOT = Path("data/tmp/recsys_repo2repo_personalization")
DEFAULT_BASE_SUFFIX = "retrieval_rerank_v2_week7_full_20260502"
DEFAULT_RANKER_SUFFIX = "retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel"
ML_RELATED_REQUIRED_COLUMNS = ("anchor_repo_id", "related_repo_id", "rank", "cooc_score")


@dataclass(frozen=True)
class Condition:
    name: str
    output_suffix: str
    mart_dir: Path
    related_candidate_cap: int
    related_top_per_anchor: int
    setup_steps: tuple[list[str], ...] = ()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-suffix",
        default=DEFAULT_BASE_SUFFIX,
        help=(
            "Canonical/retrieval base suffix. Defaults to the checked-in Week 7 V2 cache "
            f"({DEFAULT_BASE_SUFFIX}); override if using custom --canonical-path/--candidate-path."
        ),
    )
    parser.add_argument("--experiment-prefix", default="repo2repo_personalization")
    parser.add_argument("--canonical-path", type=Path, default=None)
    parser.add_argument("--candidate-path", type=Path, default=None)
    parser.add_argument("--ranker-path", type=Path, default=None)
    parser.add_argument(
        "--ranker-suffix",
        default=DEFAULT_RANKER_SUFFIX,
        help=(
            "Ranker suffix used when --ranker-path is omitted. Defaults to the Week 7 "
            f"LGBM ranker ({DEFAULT_RANKER_SUFFIX})."
        ),
    )
    parser.add_argument("--rule-mart-dir", type=Path, default=DEFAULT_MART_DIR)
    parser.add_argument(
        "--ml-related-path",
        type=Path,
        required=True,
        help=(
            "ML repo2repo parquet with anchor_repo_id, related_repo_id, rank, cooc_score. "
            "It is exposed to the V2 hybridizer as repo_repo_related_mart.parquet."
        ),
    )
    parser.add_argument("--tmp-root", type=Path, default=DEFAULT_TMP_ROOT)
    parser.add_argument("--candidate-k", type=int, default=300)
    parser.add_argument("--als-head", type=int, default=80)
    parser.add_argument("--related-candidate-cap", type=int, default=80)
    parser.add_argument("--related-top-per-anchor", type=int, default=10)
    parser.add_argument("--related-max-seen-anchors", type=int, default=20)
    parser.add_argument("--recent-candidate-cap", type=int, default=20)
    parser.add_argument("--popular-candidate-cap", type=int, default=20)
    parser.add_argument("--k-values", default="10,50,100,200")
    parser.add_argument("--device", default="cpu")
    parser.add_argument("--predict-batch-size", type=int, default=8192)
    parser.add_argument(
        "--copy-ml-mart",
        action="store_true",
        help="Copy parquet files into the temporary mart dir instead of using symlinks.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Run setup steps, hybridization, and eval. Default is dry-run command printing.",
    )
    args = parser.parse_args()
    validate_args(parser, args)
    return args


def quote_cmd(cmd: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in cmd)


def default_path(kind: str, suffix: str) -> Path:
    if kind == "canonical":
        return Path("data/features/recsys_v2") / f"canonical_{suffix}.parquet"
    if kind == "candidates":
        return Path("data/features/recsys_v2") / f"retrieval_candidates_{suffix}.parquet"
    if kind == "ranker":
        return Path("data/models/recsys_v2") / f"ranker_lgbm_{suffix}.pkl"
    raise ValueError(kind)


def validate_ml_related_path(parser: argparse.ArgumentParser, path: Path) -> None:
    if not path.exists():
        parser.error(f"--ml-related-path does not exist: {path}")
    try:
        columns = set(pq.read_schema(path).names)
    except Exception as exc:
        parser.error(f"--ml-related-path must be a readable parquet file: {path} ({exc})")
    missing = [column for column in ML_RELATED_REQUIRED_COLUMNS if column not in columns]
    if missing:
        parser.error(
            "--ml-related-path must be a mart-compatible parquet with columns "
            f"{', '.join(ML_RELATED_REQUIRED_COLUMNS)}: {path} (missing: {', '.join(missing)})"
        )


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    validate_ml_related_path(parser, args.ml_related_path)

    canonical_path = args.canonical_path or default_path("canonical", args.base_suffix)
    candidate_path = args.candidate_path or default_path("candidates", args.base_suffix)
    ranker_path = args.ranker_path or default_path("ranker", args.ranker_suffix)
    missing = [
        ("canonical", canonical_path),
        ("candidate", candidate_path),
        ("ranker", ranker_path),
        ("rule mart repo_feature", args.rule_mart_dir / "repo_feature_mart.parquet"),
        ("rule mart related", args.rule_mart_dir / "repo_repo_related_mart.parquet"),
    ]
    for label, path in missing:
        if not path.exists():
            parser.error(f"{label} path does not exist: {path}")


def materialize_file(src: Path, dst: Path, copy: bool) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() or dst.is_symlink():
        dst.unlink()
    if copy:
        shutil.copy2(src, dst)
    else:
        dst.symlink_to(src.resolve())


def prepare_ml_mart(args: argparse.Namespace, ml_mart_dir: Path) -> None:
    materialize_file(
        args.rule_mart_dir / "repo_feature_mart.parquet",
        ml_mart_dir / "repo_feature_mart.parquet",
        args.copy_ml_mart,
    )
    materialize_file(
        args.ml_related_path,
        ml_mart_dir / "repo_repo_related_mart.parquet",
        args.copy_ml_mart,
    )


def path_options(args: argparse.Namespace) -> list[str]:
    options: list[str] = []
    if args.canonical_path is not None:
        options.extend(["--canonical-path", str(args.canonical_path)])
    if args.candidate_path is not None:
        options.extend(["--candidate-path", str(args.candidate_path)])
    return options


def eval_path_options(args: argparse.Namespace, condition_suffix: str) -> list[str]:
    options: list[str] = []
    options.extend(["--canonical-path", str(args.canonical_path or default_path("canonical", args.base_suffix))])
    options.extend(["--candidate-path", str(default_path("candidates", condition_suffix))])
    options.extend(["--ranker-path", str(args.ranker_path or default_path("ranker", args.ranker_suffix))])
    return options


def build_conditions(args: argparse.Namespace) -> list[Condition]:
    ml_mart_dir = args.tmp_root / f"{args.experiment_prefix}_ml_mart"
    setup = (
        ["mkdir", "-p", str(ml_mart_dir)],
        [
            "ln" if not args.copy_ml_mart else "cp",
            "-sf" if not args.copy_ml_mart else "-f",
            str((args.rule_mart_dir / "repo_feature_mart.parquet").resolve()),
            str(ml_mart_dir / "repo_feature_mart.parquet"),
        ],
        [
            "ln" if not args.copy_ml_mart else "cp",
            "-sf" if not args.copy_ml_mart else "-f",
            str(args.ml_related_path.resolve()),
            str(ml_mart_dir / "repo_repo_related_mart.parquet"),
        ],
    )
    return [
        Condition(
            name="related_off",
            output_suffix=f"{args.experiment_prefix}_related_off",
            mart_dir=args.rule_mart_dir,
            related_candidate_cap=0,
            related_top_per_anchor=0,
        ),
        Condition(
            name="rule_related",
            output_suffix=f"{args.experiment_prefix}_rule_related",
            mart_dir=args.rule_mart_dir,
            related_candidate_cap=args.related_candidate_cap,
            related_top_per_anchor=args.related_top_per_anchor,
        ),
        Condition(
            name="ml_repo2repo_related",
            output_suffix=f"{args.experiment_prefix}_ml_related",
            mart_dir=ml_mart_dir,
            related_candidate_cap=args.related_candidate_cap,
            related_top_per_anchor=args.related_top_per_anchor,
            setup_steps=setup,
        ),
    ]


def hybridize_cmd(args: argparse.Namespace, condition: Condition) -> list[str]:
    return [
        "uv",
        "run",
        "python",
        "scripts/recsys_hybridize_candidates_v2.py",
        "--suffix",
        args.base_suffix,
        "--output-suffix",
        condition.output_suffix,
        *path_options(args),
        "--mart-dir",
        str(condition.mart_dir),
        "--candidate-k",
        str(args.candidate_k),
        "--als-head",
        str(args.als_head),
        "--related-candidate-cap",
        str(condition.related_candidate_cap),
        "--related-top-per-anchor",
        str(condition.related_top_per_anchor),
        "--related-max-seen-anchors",
        str(args.related_max_seen_anchors),
        "--recent-candidate-cap",
        str(args.recent_candidate_cap),
        "--popular-candidate-cap",
        str(args.popular_candidate_cap),
    ]


def eval_cmd(args: argparse.Namespace, condition: Condition) -> list[str]:
    return [
        "uv",
        "run",
        "python",
        "scripts/recsys_eval_v2.py",
        "--suffix",
        condition.output_suffix,
        *eval_path_options(args, condition.output_suffix),
        "--k-values",
        args.k_values,
        "--device",
        args.device,
        "--predict-batch-size",
        str(args.predict_batch_size),
    ]


def run_step(cmd: list[str]) -> None:
    print(f"$ {quote_cmd(cmd)}", flush=True)
    subprocess.run(cmd, check=True)


def main() -> None:
    args = parse_args()
    conditions = build_conditions(args)

    print("# V2 repo2repo personalization experiment")
    print(f"# mode: {'execute' if args.execute else 'dry-run'}")
    print(f"# base_suffix: {args.base_suffix}")
    print(f"# ranker_suffix: {args.ranker_suffix}")
    print(f"# ml_related_path: {args.ml_related_path}")
    print()

    for condition in conditions:
        print(f"## {condition.name}")
        if condition.setup_steps:
            for step in condition.setup_steps:
                print(quote_cmd(step))
        h_cmd = hybridize_cmd(args, condition)
        e_cmd = eval_cmd(args, condition)
        print(quote_cmd(h_cmd))
        print(quote_cmd(e_cmd))
        print()

        if args.execute:
            if condition.name == "ml_repo2repo_related":
                prepare_ml_mart(args, condition.mart_dir)
            run_step(h_cmd)
            run_step(e_cmd)

    print("# Compare output CSVs:")
    for condition in conditions:
        print(f"# - data/results/recsys_v2/eval_metrics_{condition.output_suffix}.csv")


if __name__ == "__main__":
    main()
