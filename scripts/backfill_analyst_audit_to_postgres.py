from __future__ import annotations

"""Backfill analyst-bot JSONL audit logs into Postgres/RDS.

Usage:
  ANALYST_AUDIT_DATABASE_URL=postgresql://... \
    uv run --with 'psycopg[binary]>=3.2,<4' \
      python scripts/backfill_analyst_audit_to_postgres.py \
      --state-dir logs/analyst-bot
"""

import argparse
import json
import os
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from slack_analyst_bot import (  # noqa: E402
    AUDIT_DATABASE_URL_ENV,
    AUDIT_SCHEMA_ENV,
    DEFAULT_AUDIT_SCHEMA,
    PostgresAuditSink,
    describe_database_url,
)


def iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"malformed JSONL at {path}:{lineno}: {exc}") from exc


def backfill_turns(sink: PostgresAuditSink, state_dir: Path, dry_run: bool) -> int:
    count = 0
    for path in sorted((state_dir / "audit").glob("*.jsonl")):
        for record in iter_jsonl(path):
            count += 1
            if not dry_run:
                sink.record_turn(record)
    return count


def backfill_traces(sink: PostgresAuditSink, state_dir: Path, dry_run: bool) -> int:
    count = 0
    for path in sorted((state_dir / "traces").glob("*.jsonl")):
        trace: list[str] = []
        for record in iter_jsonl(path):
            rtype = record.get("type")
            if rtype == "trace":
                trace.append(record.get("line") or "")
                continue
            if rtype == "summary":
                count += 1
                if not dry_run:
                    sink.record_trace(trace, record)
                trace = []
    return count


def backfill_feedback(sink: PostgresAuditSink, state_dir: Path, dry_run: bool) -> int:
    count = 0
    feedback_dir = state_dir / "feedback"
    if not feedback_dir.is_dir():
        return 0
    for path in sorted(feedback_dir.glob("*.jsonl")):
        for record in iter_jsonl(path):
            count += 1
            if not dry_run:
                sink.record_feedback(record)
    return count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill analyst-bot JSONL logs into Postgres/RDS.",
    )
    parser.add_argument(
        "--state-dir",
        default="logs/analyst-bot",
        help="JSONL state dir. Docker state can be copied/exported first.",
    )
    parser.add_argument(
        "--schema",
        default=os.environ.get(AUDIT_SCHEMA_ENV, DEFAULT_AUDIT_SCHEMA),
        help=f"Postgres schema. Default: env {AUDIT_SCHEMA_ENV} or {DEFAULT_AUDIT_SCHEMA}",
    )
    parser.add_argument("--dry-run", action="store_true", help="Count records without writing")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    database_url = os.environ.get(AUDIT_DATABASE_URL_ENV, "")
    if not database_url:
        raise SystemExit(f"{AUDIT_DATABASE_URL_ENV} is required")

    state_dir = Path(args.state_dir).expanduser()
    if not state_dir.is_dir():
        raise SystemExit(f"state dir does not exist: {state_dir}")

    sink = PostgresAuditSink(database_url, args.schema)
    print(f"target = {describe_database_url(database_url)} schema={sink.schema}")
    print(f"state  = {state_dir}")
    if args.dry_run:
        print("mode   = dry-run")

    turns = backfill_turns(sink, state_dir, args.dry_run)
    traces = backfill_traces(sink, state_dir, args.dry_run)
    feedback = backfill_feedback(sink, state_dir, args.dry_run)

    print(f"turns={turns} trace_groups={traces} feedback={feedback}")


if __name__ == "__main__":
    main()
