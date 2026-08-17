#!/usr/bin/env python
"""Prove a BigQuery service-account key is read-only, using only that key.

The Slack analyst bot hands `bq query` to a Claude session. `bq query` executes
DDL/DML (CREATE OR REPLACE, DROP, DELETE, MERGE) and no tool-permission rule can
constrain SQL, so read-only has to hold at the IAM layer. This script asserts it
empirically: two reads must succeed, three writes must be REFUSED, and the
per-query scan ceiling must actually fire.

Run it after minting the analyst key, and again after any IAM change:

  uv run --no-project --with google-cloud-bigquery \
    python scripts/verify_bq_readonly.py --key-path secrets/analyst-bq-key.json

Safety: the write probes can never damage real data even if the key turns out to
be writable. The CREATE probe targets a throwaway `__readonly_probe__` table (and
is dropped immediately if it lands), and the DELETE/INSERT probes carry
`WHERE FALSE`, so they touch zero rows while still requiring write permission at
job-creation time. `assert_probe_is_safe()` enforces that invariant before any
write SQL is sent.

Cost: reads are a few KB. The ceiling probe (check 6) is expected to be rejected
before execution and billed nothing; if the ceiling is misconfigured it degrades
to a single ~5 GiB scan (~$0.03) and is reported as a FAIL.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from google.cloud import bigquery


PROJECT_DIR = Path(__file__).resolve().parents[1]

# Throwaway relation for the CREATE probe. Never a real model name.
PROBE_TABLE = "__readonly_probe__"
# Sentinel the INSERT probe would carry if it ever landed a row (it must not).
INSERT_SENTINEL_DATE = "1900-01-01"
# Ceiling for check 6. Small enough that any real column scan blows through it.
CEILING_BYTES = 1024 * 1024


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--key-path",
        default=os.environ.get("ANALYST_BQ_KEY_PATH", "secrets/analyst-bq-key.json"),
    )
    parser.add_argument("--project", default=os.environ.get("BQ_PROJECT", "bda-coai"))
    parser.add_argument("--dataset", default=os.environ.get("BQ_DATASET", "mart"))
    parser.add_argument("--location", default=os.environ.get("BQ_LOCATION", "US"))
    parser.add_argument("--raw-day", default="20260701")
    return parser.parse_args()


def resolve_key(key_path: str) -> Path:
    path = Path(key_path)
    if not path.is_absolute():
        path = PROJECT_DIR / path
    if not path.is_file():
        raise SystemExit(f"key not found: {path}")
    return path


def make_client(key_path: Path, project: str, location: str) -> bigquery.Client:
    return bigquery.Client.from_service_account_json(
        str(key_path), project=project, location=location
    )


def key_identity(key_path: Path) -> str:
    payload = json.loads(key_path.read_text())
    return str(payload.get("client_email", "unknown"))


def error_code(exc: Exception) -> str:
    """Best-effort '<http status>:<reason>' for a BigQuery failure."""
    reason = ""
    errors = getattr(exc, "errors", None)
    if errors and isinstance(errors[0], dict):
        reason = str(errors[0].get("reason", ""))
    status = getattr(exc, "code", None)
    parts = [str(status) if status else "", reason or type(exc).__name__]
    return ":".join(part for part in parts if part)


def assert_probe_is_safe(sql: str) -> None:
    """Refuse to send write SQL that could touch real rows."""
    lowered = " ".join(sql.lower().split())
    if PROBE_TABLE in lowered or "where false" in lowered:
        return
    raise SystemExit(f"refusing to run unguarded write probe: {sql.strip()[:120]}")


def run_query(
    client: bigquery.Client,
    sql: str,
    location: str,
    dry_run: bool = False,
    max_bytes_billed: int | None = None,
) -> tuple[bool, str, int]:
    """Return (succeeded, error_code, bytes_processed)."""
    config = bigquery.QueryJobConfig(dry_run=dry_run, use_query_cache=False)
    if max_bytes_billed is not None:
        config.maximum_bytes_billed = max_bytes_billed
    try:
        job = client.query(sql, job_config=config, location=location)
        if not dry_run:
            job.result()
        return True, "", int(job.total_bytes_processed or 0)
    except Exception as exc:  # noqa: BLE001 - any failure is a datapoint here
        return False, error_code(exc), 0


def record(
    results: list[dict],
    name: str,
    expect: str,
    passed: bool,
    observed: str,
    detail: str = "",
) -> None:
    results.append(
        {
            "check": name,
            "expect": expect,
            "pass": passed,
            "observed": observed or "ok",
            "detail": detail,
        }
    )
    tag = "PASS" if passed else "FAIL"
    line = f"[{tag}] {name} (expect {expect}) observed={observed or 'ok'}"
    print(line if not detail else f"{line} -- {detail}")


def check_read_mart(client: bigquery.Client, args: argparse.Namespace, results: list[dict]) -> None:
    sql = (
        f"select activity_date, active_users "
        f"from `{args.project}.{args.dataset}.metrics_daily` "
        f"order by activity_date desc limit 1"
    )
    ok, code, _ = run_query(client, sql, args.location)
    record(results, "read_mart_select", "succeed", ok, code, f"{args.dataset}.metrics_daily")


def check_dry_run_public(
    client: bigquery.Client, args: argparse.Namespace, results: list[dict]
) -> None:
    sql = (
        "select type, count(*) as events "
        "from `githubarchive.day.20*` "
        f"where concat('20', _table_suffix) between '{args.raw_day}' and '{args.raw_day}' "
        "group by type"
    )
    ok, code, scanned = run_query(client, sql, args.location, dry_run=True)
    passed = ok and scanned > 0
    detail = f"bytes_processed={scanned}"
    if ok and scanned == 0:
        code = "dry_run_returned_zero_bytes"
    record(results, "dry_run_public_raw", "succeed", passed, code, detail)


def check_create_refused(
    client: bigquery.Client, args: argparse.Namespace, results: list[dict]
) -> None:
    fqn = f"{args.project}.{args.dataset}.{PROBE_TABLE}"
    sql = f"create or replace table `{fqn}` as select 1 as probe"
    assert_probe_is_safe(sql)
    ok, code, _ = run_query(client, sql, args.location)
    if not ok:
        record(results, "create_table_refused", "fail", True, code)
        return

    print()
    print("!!! WRITE SUCCEEDED: this key can CREATE OR REPLACE tables in "
          f"{args.project}.{args.dataset} -- it is NOT read-only !!!")
    drop_ok, drop_code, _ = run_query(client, f"drop table if exists `{fqn}`", args.location)
    cleanup = f"probe table dropped={drop_ok}" + (f" ({drop_code})" if drop_code else "")
    print(f"!!! cleanup: {cleanup}")
    print()
    record(results, "create_table_refused", "fail", False, "write_succeeded", cleanup)


def check_delete_refused(
    client: bigquery.Client, args: argparse.Namespace, results: list[dict]
) -> None:
    sql = (
        f"delete from `{args.project}.{args.dataset}.metrics_daily` where false"
    )
    assert_probe_is_safe(sql)
    ok, code, _ = run_query(client, sql, args.location)
    if not ok:
        record(results, "delete_refused", "fail", True, code)
        return
    print()
    print("!!! WRITE SUCCEEDED: this key can DELETE from "
          f"{args.dataset}.metrics_daily -- it is NOT read-only !!!")
    print("!!! no rows were removed (probe carried WHERE FALSE)")
    print()
    record(results, "delete_refused", "fail", False, "write_succeeded", "0 rows affected by design")


def check_insert_refused(
    client: bigquery.Client, args: argparse.Namespace, results: list[dict]
) -> None:
    table = f"{args.project}.{args.dataset}.metrics_daily"
    sql = (
        f"insert into `{table}` "
        "(activity_date, active_users, active_repos, total_events) "
        f"select date '{INSERT_SENTINEL_DATE}', 0, 0, 0 "
        "from unnest([1]) as probe where false"
    )
    assert_probe_is_safe(sql)
    ok, code, _ = run_query(client, sql, args.location)
    if not ok:
        record(results, "insert_refused", "fail", True, code)
        return

    print()
    print(f"!!! WRITE SUCCEEDED: this key can INSERT into {args.dataset}.metrics_daily "
          "-- it is NOT read-only !!!")
    leaked = -1
    verify = (
        f"select count(*) as rows_ from `{table}` "
        f"where activity_date = date '{INSERT_SENTINEL_DATE}'"
    )
    try:
        leaked = int(next(iter(client.query(verify, location=args.location).result())).rows_)
    except Exception as exc:  # noqa: BLE001
        print(f"!!! could not verify sentinel rows: {error_code(exc)}")
    if leaked > 0:
        print(f"!!! {leaked} sentinel row(s) present -- removing")
        run_query(
            client,
            f"delete from `{table}` where activity_date = date '{INSERT_SENTINEL_DATE}'",
            args.location,
        )
    else:
        print("!!! no rows were written (probe carried WHERE FALSE)")
    print()
    record(
        results,
        "insert_refused",
        "fail",
        False,
        "write_succeeded",
        f"sentinel_rows={leaked}",
    )


def check_cost_ceiling(
    client: bigquery.Client, args: argparse.Namespace, results: list[dict]
) -> None:
    sql = (
        "select count(distinct user_id) as users "
        f"from `{args.project}.{args.dataset}.fact_user_repo_activity`"
    )
    ok, code, _ = run_query(client, sql, args.location, max_bytes_billed=CEILING_BYTES)
    bit = "bytesBilledLimitExceeded" in code
    passed = (not ok) and bit
    detail = f"maximum_bytes_billed={CEILING_BYTES}"
    if ok:
        code = "query_ran_despite_ceiling"
        detail += " (ceiling did NOT bite -- a full column scan was billed)"
    record(results, "cost_ceiling_bites", "fail", passed, code, detail)


def main() -> None:
    args = parse_args()
    key_path = resolve_key(args.key_path)
    client = make_client(key_path, args.project, args.location)

    print(f"key: {key_path}")
    print(f"identity: {key_identity(key_path)}")
    print(f"project: {args.project}  dataset: {args.dataset}  location: {args.location}")
    print()

    results: list[dict] = []
    check_read_mart(client, args, results)
    check_dry_run_public(client, args, results)
    check_create_refused(client, args, results)
    check_delete_refused(client, args, results)
    check_insert_refused(client, args, results)
    check_cost_ceiling(client, args, results)

    failed = [row["check"] for row in results if not row["pass"]]
    report = {
        "identity": key_identity(key_path),
        "key_path": str(key_path),
        "project": args.project,
        "dataset": args.dataset,
        "checks": results,
        "failed": failed,
        "read_only": not failed,
    }
    print()
    print("BQ_READONLY_VERIFY " + json.dumps(report, ensure_ascii=False, sort_keys=True))

    if failed:
        raise SystemExit(f"not read-only: {len(failed)} check(s) failed: {', '.join(failed)}")


if __name__ == "__main__":
    main()
