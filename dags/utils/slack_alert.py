from __future__ import annotations

import json
import os
import urllib.request

SLACK_API_URL = "https://slack.com/api/chat.postMessage"
TOKEN_ENV = "SLACK_BOT_TOKEN"
CHANNEL = os.environ.get("SLACK_ALERT_CHANNEL", "C0B76SCRVB4")
MENTION_USER = os.environ.get("SLACK_ALERT_MENTION", "U0B76RDS60J")


def _post(text: str) -> None:
    token = os.environ.get(TOKEN_ENV)
    if not token:
        return
    payload = json.dumps({"channel": CHANNEL, "text": text}).encode("utf-8")
    req = urllib.request.Request(
        SLACK_API_URL,
        data=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        json.load(resp)


def notify_failure(context: dict) -> None:
    ti = context.get("task_instance")
    dag = context.get("dag")
    dag_id = getattr(dag, "dag_id", "?")
    task_id = getattr(ti, "task_id", "?")
    run_id = context.get("run_id", "?")
    exc = context.get("exception")
    log_url = getattr(ti, "log_url", "") if ti else ""
    mention = f"<@{MENTION_USER}> " if MENTION_USER else ""

    lines = [
        f":rotating_light: {mention}*Airflow 태스크 실패*",
        f"*DAG*: `{dag_id}`",
        f"*Task*: `{task_id}`",
        f"*Run*: `{run_id}`",
    ]
    if exc:
        lines.append(f"*Error*: `{str(exc)[:300]}`")
    if log_url:
        lines.append(f"<{log_url}|로그 보기>")

    _post("\n".join(lines))


def notify_drift(layer: str, report: dict) -> None:
    """Warn-only drift alert. Not a task failure — the task succeeded and found drift.

    `report` is the dict produced by ghrec.drift.evaluate(): it has overall_status
    ('ok'|'warn'|'alert') and a per-feature list with psi/warn/alert/status.
    """
    status = report.get("overall_status", "ok")
    if status == "ok":
        return

    icon = ":large_orange_diamond:" if status == "alert" else ":warning:"
    mention = f"<@{MENTION_USER}> " if (MENTION_USER and status == "alert") else ""
    flagged = [f for f in report.get("features", []) if f.get("status") != "ok"]

    lines = [
        f"{icon} {mention}*입력 드리프트 감지* (`{layer}`) — status: `{status}`",
        f"*Date*: `{report.get('evaluated_at', '?')}`",
    ]
    for f in flagged[:8]:
        thr = f.get("alert") if f.get("status") == "alert" else f.get("warn")
        lines.append(
            f"• `{f['feature']}` PSI=`{f['psi']:.4f}` "
            f"≥ {f['status']}=`{thr:.4f}`"
        )
    _post("\n".join(lines))


def notify_cost_guard(report: dict) -> None:
    mention = f"<@{MENTION_USER}> " if MENTION_USER else ""
    lines = [
        f":money_with_wings: {mention}*BigQuery 비용 가드 초과*",
        f"*Project*: `{report.get('project', '?')}`",
        f"*Window*: last `{report.get('lookback_hours', '?')}` hours",
        f"*Estimated*: `${report.get('estimated_usd', 0):.2f}` "
        f"> limit `${report.get('max_usd', 0):.2f}`",
        f"*Jobs*: `{report.get('jobs', 0)}`",
    ]
    for item in report.get("top_sources", [])[:5]:
        lines.append(
            f"• `{item.get('source_guess', '?')}` "
            f"${item.get('estimated_usd', 0):.2f} / jobs={item.get('jobs', 0)}"
        )
    _post("\n".join(lines))
