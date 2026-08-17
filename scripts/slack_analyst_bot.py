from __future__ import annotations

"""Slack Socket Mode listener that maps 1 Slack thread -> 1 Claude Code session.

Each thread drives the repo's `analysis` skill against GitHub Archive / BigQuery.
Turn 1 of a thread spawns `claude -p --session-id <uuid>`; every later turn spawns
`claude -p --resume <uuid>` with the same cwd, so context survives across messages
and across bot restarts (the session id is derived deterministically from the
Slack team/channel/thread triple).
"""

import argparse
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator
from urllib.parse import urlsplit

# This is a long-running daemon (Socket Mode connection stays open for days), so we
# use `logging` instead of bare print(): timestamps, levels and thread names matter
# when reconstructing what a session did hours after the fact.
LOG = logging.getLogger("slack_analyst_bot")

DEFAULT_REPO_DIR = "/Users/kakao/bda-2"
SKILL_RELPATH = ".claude/skills/analysis/SKILL.md"
DEFAULT_STATE_DIR = "logs/analyst-bot"
# The analyst's own read-only key. NOT `gcp-key.json`, which carries roles/owner --
# that one belongs to the operator and Airflow and must never reach the session.
DEFAULT_GCP_KEY = "secrets/analyst-bq-key.json"
# Dedicated gcloud config dir holding ONLY the analyst service account. `bq` reads
# the gcloud credential store rather than GOOGLE_APPLICATION_CREDENTIALS, so this is
# what bounds `bq`. Rebuild with:
#   CLOUDSDK_CONFIG=secrets/gcloud-analyst gcloud auth activate-service-account \
#     --key-file=secrets/analyst-bq-key.json
ANALYST_GCLOUD_CONFIG = "secrets/gcloud-analyst"
ANALYST_GCLOUD_CONFIG_ENV = "ANALYST_GCLOUD_CONFIG_DIR"
BQ_PROJECT = "bda-coai"

# --- Slack credentials ------------------------------------------------------
# Slack credentials are intentionally read from analyst-specific environment
# variable names even when the workspace reuses the same underlying Slack app as
# Airflow alerts. Keeping variable names separate makes the bot's required event
# scopes, app-level token, and reinstall risk explicit.
#
# Do not fall back to SLACK_BOT_TOKEN. That variable is reserved for Airflow alerting
# in this repo, and using it here makes a missing analyst token look like an event
# delivery bug instead of a startup configuration error.
BOT_TOKEN_ENV = "SLACK_ANALYST_BOT_TOKEN"
APP_TOKEN_ENV = "SLACK_ANALYST_APP_TOKEN"
METABASE_OPEN_ACTION_ID = "open_metabase_result"
METABASE_RUNTIME_CONFIG_FILENAME = "metabase-credentials-mcp.json"
AUDIT_DATABASE_URL_ENV = "ANALYST_AUDIT_DATABASE_URL"
AUDIT_SCHEMA_ENV = "ANALYST_AUDIT_SCHEMA"
DEFAULT_AUDIT_SCHEMA = "analyst_audit"


def bot_token() -> str | None:
    return os.environ.get(BOT_TOKEN_ENV) or None


def app_token() -> str | None:
    return os.environ.get(APP_TOKEN_ENV) or None

# --- Tool permissions -------------------------------------------------------
# NOTE: `--allowedTools` is only actually enforced under `--permission-mode manual`
# (or `plan`). Under `auto`/`dontAsk` a denied tool is merely logged and then
# auto-approved on retry. We therefore ALWAYS spawn with `manual`, which makes the
# allowlist binding and turns a blocked attempt into a `permission_denials[]` entry
# on the final result event rather than a hang.
#
# Two holes were found by live probing and are closed below. Do not widen these
# without re-probing:
#
#   1. `Bash(uv run:*)` was arbitrary code execution. `uv run python -c "..."` was
#      AUTO-APPROVED and printed gcp-key.json, with an empty permission_denials[].
#      Deny rules match the Bash *command string*, not what the child process does,
#      so one interpreter invocation defeats every deny below. Only literal `dbt`
#      invocations are allowed now.
#   2. The `Read` tool could read ANY absolute path on the machine and no permission
#      rule stopped it. Probed exhaustively: reading /etc/hosts succeeded with an
#      empty permission_denials[] under EVERY one of these --disallowedTools forms:
#          Read(/etc/**)   Read(//etc/**)   Read(**/hosts)   Read(/etc/*)
#      and an ALLOW rule `Read(<repo>/**)` confines nothing, because read-only tools
#      are permitted by default even under `manual`.
#
#      Earlier mitigation was to drop Read/Grep/Glob entirely and route file access
#      through Bash. For the lecture/demo build they are back for ergonomics, so the
#      real boundary is a container with narrow mounts plus filename-based deny rules
#      and outbound redaction.
#
# Verified as NOT bypasses (no need to defend again): shell chaining (`;`, `&&`, `|`)
# and command substitution are decomposed and refused by the matcher.
#
# Note gcp-key.json in the repo is a symlink to ~/Documents/gcp-key.json. The deny
# glob is filename-based (`**/gcp-key.json`) so it covers both paths.

# Tool set handed to the session.
#
# Read/Grep/Glob are INCLUDED by operator decision (2026-07-26) for analysis
# ergonomics, with the host-exposure tradeoff accepted knowingly. What that means
# concretely, so nobody rediscovers it the hard way:
#
#   * A session CAN read absolute paths outside the repo. Probed: reading
#     /etc/hosts succeeded and NO --disallowedTools form blocked it
#     (Read(/etc/**), Read(//etc/**), Read(**/hosts), Read(/etc/*) all failed).
#     Directory-prefix deny globs do not bind for Read.
#   * What DOES bind is a FILENAME glob -- Read(**/gcp-key.json) is enforced, and
#     it also covers the symlink target ~/Documents/gcp-key.json. So the deny list
#     below is written filename-first on purpose.
#   * Residual exposure: any file whose NAME does not match a deny entry is
#     readable, and the answer is posted to Slack. Containerising (see
#     docs/analyst_bot_docker.md) is what actually closes this.
SESSION_TOOLS = "Bash,Read,Grep,Glob,Write,Edit,Skill,TodoWrite"

METABASE_MCP_TOOLS = [
    # Same surface as the long-running Hamji bot. These are added only when the
    # operator explicitly enables Metabase MCP and provides METABASE_URL/API_KEY.
    "mcp__metabase",
    "mcp__metabase__search_content",
    "mcp__metabase__list_dashboards",
    "mcp__metabase__list_collections",
    "mcp__metabase__get_collection",
    "mcp__metabase__get_collection_items",
    "mcp__metabase__get_dashboard",
    "mcp__metabase__get_dashboard_cards",
    "mcp__metabase__create_dashboard",
    "mcp__metabase__create_card",
    "mcp__metabase__add_card_to_dashboard",
    "mcp__metabase__get_card",
    "mcp__metabase__execute_card",
]

BASE_ALLOWED_TOOLS = [
    # BigQuery. NOTE: `bq query` is NOT a read-only surface -- it executes DDL/DML
    # (CREATE OR REPLACE, DROP, DELETE, MERGE) and command-string matching cannot
    # constrain SQL. Read-only MUST be enforced at IAM: the service account should
    # hold roles/bigquery.dataViewer + jobUser and NOT dataEditor. Scan cost is
    # likewise bounded by BIGQUERYRC defaults for `bq` and BIGQUERY_MAXIMUM_BYTES_BILLED
    # for dbt/Python clients (see child_env), not by the tool allowlist.
    #
    # IAM only binds if `bq` actually runs AS that service account, which requires
    # CLOUDSDK_CONFIG (see child_env) -- exporting GOOGLE_APPLICATION_CREDENTIALS is
    # not enough and silently runs as the operator instead.
    #
    # Prefix matching means the subcommand must come FIRST: `bq query --project_id=X`
    # matches, `bq --project_id=X query` does NOT. The skill documents the former.
    "Bash(bq query:*)",
    "Bash(bq show:*)",
    "Bash(bq ls:*)",
    "Bash(bq head:*)",
    # dbt only. Never a bare interpreter -- see hole 1 above.
    "Bash(uv run --no-project --with dbt-bigquery dbt:*)",
    "Bash(uv run dbt:*)",
    # Repo reading / searching. Read/Grep/Glob are in SESSION_TOOLS; the Bash
    # equivalents stay because Bash is cwd-confined and is the safer path.
    "Read",
    "Grep",
    "Glob",
    "Bash(ls:*)",
    "Bash(cat:*)",
    "Bash(rg:*)",
    "Bash(grep:*)",
    "Bash(head:*)",
    "Bash(tail:*)",
    "Bash(wc:*)",
    "Bash(find:*)",
    "Bash(sed:*)",
    "Bash(git status:*)",
    "Bash(git diff:*)",
    "Bash(git log:*)",
    # The analyst may persist ad-hoc SQL, but only into the analyses folder.
    "Write(dbt/gharchive_metrics/analyses/**)",
    "Edit(dbt/gharchive_metrics/analyses/**)",
    # Skill invocation + planning.
    "Skill",
    "TodoWrite",
]

# `--disallowedTools` IS a hard deny in every permission mode, so this list is the
# real safety net: it holds even if the allowlist above is widened or bypassed.
DISALLOWED_TOOLS = [
    # Interpreters and package-runners: arbitrary code execution launders every
    # other deny rule in this list.
    "Bash(uv run python:*)",
    "Bash(uv run --with:*)",
    "Bash(uv run -c:*)",
    "Bash(uvx:*)",
    "Bash(python:*)",
    "Bash(python3:*)",
    "Bash(node:*)",
    "Bash(sh:*)",
    "Bash(bash:*)",
    "Bash(zsh:*)",
    "Bash(eval:*)",
    # Credential re-pointing. CLOUDSDK_CONFIG is forced in child_env so `bq` cannot
    # be moved off the read-only service account, but dbt reads GCP_KEY_PATH and
    # would happily switch to the operator's roles/owner key. The historical reports
    # under reports/ still show these exports in their "how to reproduce" sections,
    # and the session reads those reports -- so this is a live re-infection path.
    "Bash(export GCP_KEY_PATH:*)",
    "Bash(export GOOGLE_APPLICATION_CREDENTIALS:*)",
    "Bash(export CLOUDSDK_CONFIG:*)",
    # BigQuery mutation.
    "Bash(bq rm:*)",
    "Bash(bq cp:*)",
    "Bash(bq mk:*)",
    "Bash(bq load:*)",
    "Bash(bq insert:*)",
    "Bash(bq update:*)",
    # Repo / filesystem mutation and egress.
    "Bash(git push:*)",
    "Bash(git commit:*)",
    "Bash(rm:*)",
    "Bash(mv:*)",
    "Bash(cp:*)",
    "Bash(curl:*)",
    "Bash(wget:*)",
    "Bash(nc:*)",
    "Bash(ssh:*)",
    "Bash(scp:*)",
    "Bash(chmod:*)",
    "Bash(sudo:*)",
    # Credential files. These also bind for file-reading Bash commands and Grep.
    "Read(**/gcp-key.json)",
    "Read(**/.env)",
    "Read(**/.env.*)",
    "Read(**/.ssh/**)",
    "Read(**/.aws/**)",
    "Read(**/.config/gcloud/**)",
    # NOTE: do NOT deny `Read(**/.claude/**)` -- that glob also matches this repo's
    # .claude/skills/analysis/references/*, which the workflow requires reading.
    "Read(**/.claude.json)",
    "Read(**/*credentials*)",
    "Read(**/*.pem)",
    "Read(**/*.p12)",
    "Read(**/id_rsa*)",
    "Read(**/id_ed25519*)",
    # Credential FILENAMES. Filename globs are the only Read deny form proven to
    # bind (directory-prefix globs silently do not), so everything sensitive is
    # listed by name. `**/gcp-key.json` also covers the ~/Documents symlink target.
    "Read(**/*.pem)",
    "Read(**/*.p12)",
    "Read(**/*.key)",
    "Read(**/*.pfx)",
    "Read(**/id_rsa*)",
    "Read(**/id_ed25519*)",
    "Read(**/id_ecdsa*)",
    "Read(**/*credential*)",
    "Read(**/*credentials*)",
    "Read(**/.netrc)",
    "Read(**/.pgpass)",
    "Read(**/.htpasswd)",
    "Read(**/known_hosts)",
    "Read(**/authorized_keys)",
    "Read(**/*.kdbx)",
    "Read(**/*token*.json)",
    "Read(**/service-account*.json)",
    "Read(**/analyst-bq-key.json)",
    "Read(**/*.zsh_history)",
    "Read(**/*.bash_history)",
    "Read(**/metabase-credentials-mcp.json)",
    # Same names via the Bash file-reading commands. Bash is cwd-confined already,
    # but these hold for anything reachable inside the repo.
    "Bash(cat /Users/kakao/Documents:*)",
    "Bash(cat /secrets:*)",
    "Bash(head /secrets:*)",
    "Bash(tail /secrets:*)",
    "Bash(grep /secrets:*)",
    "Bash(rg /secrets:*)",
    "Bash(find /secrets:*)",
    "Bash(cat *credential*:*)",
    "Bash(head *credential*:*)",
    "Bash(tail *credential*:*)",
    "Bash(grep *credential*:*)",
    "Bash(rg *credential*:*)",
    "Bash(sed *credential*:*)",
    "Bash(cat /etc:*)",
    "Bash(cat /var:*)",
    "Bash(rg /Users/kakao/Documents:*)",
    "Bash(find /Users/kakao/Documents:*)",
]

# --- Intake contract --------------------------------------------------------
APPEND_SYSTEM_PROMPT = """\
You are answering a question that arrived in a Slack thread. Follow this contract exactly.

0. Stay inside this repository. Never read files outside it, and never quote the
   contents of any credential, key, token, or history file into your answer.
1. Invoke the repo's `analysis` skill (Skill tool, name: analysis) before doing any work,
   and follow its analysis procedure.
2. Internally classify the mode and fill in all 9 required inputs from that skill before
   querying. In the FINAL Slack answer, do NOT expose internal checklist labels such as
   "모드", "분석 모드", "인풋", "관측 grain", "세그먼트", "제외 조건", or "성공 지표".
   If an assumption materially changes interpretation, mention it in plain prose.
3. Run the required health checks before interpreting any number. In the FINAL Slack answer,
   do not include a validation checklist, health-check table, or "검증:" line unless the
   user explicitly asks for those details. Keep the detailed validation evidence in logs
   and saved SQL only.
4. NEVER run an unbounded scan over `githubarchive.day.20*` (or any wildcard that expands
   to the full history). Always pin an explicit date range / _TABLE_SUFFIX bound, and
   dry-run before a real query. Dry-run output, scan bytes, job ids, CLI commands, and
   health-check tables are operational evidence: keep them in logs / SQL files, not in
   the FINAL Slack answer.
5. Do not make causal claims ("X caused Y", "because of X") without a control group or an
   explicit counterfactual. Otherwise say "correlated with" and name the confounders.
6. Your FINAL message is posted verbatim into Slack. There is no human reading it
   first and no wrapper around it, so it must BE the answer, not an introduction to
   the answer. Assume a product owner will read it. It must:
   - sound like a helpful teammate in Slack, not like a technical audit note. Be warm,
     direct, and plain-spoken without being cute. Prefer Korean product language over
     database language: say "활동 계정" before "active actor"; move `actor.id`, distinct
     count, UTC, partition, and table names to the caveat unless the user asked for them.
     Use this final-answer glossary unless the user explicitly asks for schema terms:
     "active actor" -> "활동 계정", "actor" -> "계정", "actor.id" -> "계정 ID",
     "event" -> "활동 기록", "event type" -> "활동 종류", "UTC" -> "데이터 기준",
     "mart/raw/shard/partition/grain" -> avoid or explain in plain Korean.
   - if the user asks for a quick/simple answer ("간단히", "짧게", "quickly", "tl;dr"),
     answer in 2-4 short sentences plus one caveat line. Do not add trend tables,
     bot-rate breakdowns, or follow-up investigations unless they materially change
     the conclusion.
   - start with the conclusion itself. Do NOT open with a status line, a summary of
     what you did, or a hand-off sentence ("Analysis complete.", "Here is the answer
     that will be posted to Slack", "I left two SQL files"). Such a line is written
     to yourself and reads as leaked scaffolding to the person who asked.
   - contain no horizontal rules (`---`) and no preamble above the conclusion,
   - stay under ~2,500 characters. Keep conclusion, core numbers, date/filter basis,
     and caveat. Do not include process labels just because the
     analysis checklist used them internally,
   - use Slack mrkdwn (*bold*, `code`, • bullets) - never HTML or ###-headings,
   - do not include reproduction SQL paths or a "재현:" line. Saved SQL paths belong in
     logs/audit unless the user explicitly asks for reproducibility details,
   - never mention dry-run bytes, `bq`/`dbt` command lines, Claude turn counts, BigQuery
     GiB/cost accounting, MCP/tool names, or permission internals,
   - end with a one-line caveat about what the number does NOT show.
"""

METABASE_APPEND_PROMPT_TEMPLATE = """\

7. Metabase is available through MCP for visual follow-up. Use it only when a chart/card
   materially improves the answer (time series, breakdown, top-N, cohort/retention table)
   or when the user explicitly asks to show it in Metabase.
   - Prefer an existing relevant card/dashboard if it already answers the question.
   - If creating a new asset, first call list_collections and use a writable collection
     named "{collection_name}" when available. Do not modify unrelated existing
     dashboards unless the user explicitly asks for that dashboard.
   - Create a compact native SQL card, add it to a focused dashboard only when a single
     card is not enough, and name generated assets with a short Slack/demo prefix.
   - Do not tell the user to copy SQL into Metabase. Create the card directly. If the
     actual Metabase call fails, mention the failure in one short sentence and still
     answer from the validated numbers.
   - In the FINAL Slack answer, say only "Metabase 카드" or "Metabase 대시보드" with a
     clickable URL. Do not mention MCP, tool names, collection ids, or API details.
   - Link format: card {metabase_url}/question/{{id}}, dashboard {metabase_url}/dashboard/{{id}}.
"""

MAX_SLACK_CHUNK = 2800
MAX_FINAL_ANSWER_CHARS = 2500
TRACE_MAX_LINES = 40
UPDATE_INTERVAL_S = 1.5
CANCEL_ACTION_ID = "cancel_analyst_turn"
FEEDBACK_ACTION_PREFIX = "analyst_feedback_"
FEEDBACK_LABELS = {
    "helpful": "도움됨",
    "inaccurate": "부정확함",
    "needs_more_investigation": "추가 조사 필요",
}


# --- Small helpers ----------------------------------------------------------
def truncate_line(text: str, width: int = 160) -> str:
    flat = " ".join(str(text).split())
    if len(flat) <= width:
        return flat
    return flat[: width - 1] + "…"


def chunk_text(text: str, limit: int = MAX_SLACK_CHUNK) -> list[str]:
    """Split on line boundaries so no Slack post exceeds `limit` characters."""
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    current: list[str] = []
    size = 0
    for line in text.split("\n"):
        while len(line) > limit:
            if current:
                chunks.append("\n".join(current))
                current, size = [], 0
            chunks.append(line[:limit])
            line = line[limit:]
        if size + len(line) + 1 > limit and current:
            chunks.append("\n".join(current))
            current, size = [], 0
        current.append(line)
        size += len(line) + 1
    if current:
        chunks.append("\n".join(current))
    return [c for c in chunks if c.strip()]


LEAKED_PREAMBLE_PATTERNS = [
    re.compile(r"^\s*(?:아래|다음)(?:가|은|는)?\s*(?:Slack(?:에)?\s*)?(?:게시될\s*)?답변(?:입니다|이에요)?\.?\s*$", re.I),
    re.compile(r"^\s*(?:분석\s*)?(?:완료|완료했습니다|완료했어요)\.?\s*$", re.I),
    re.compile(r"^\s*here(?:'s| is)\s+(?:the\s+)?(?:slack\s+)?answer\.?\s*$", re.I),
    re.compile(r"^\s*analysis\s+complete\.?\s*$", re.I),
]

INTERNAL_METADATA_LINE_PATTERNS = [
    re.compile(r"^\s*(?:[-*]\s*)?(?:[*_`]+)?(?:분석\s*)?모드(?:[*_`]+)?\s*[:：]\s*.+$", re.I),
    re.compile(r"^\s*(?:[-*]\s*)?(?:[*_`]+)?분석\s*유형(?:[*_`]+)?\s*[:：]\s*.+$", re.I),
    re.compile(r"^\s*(?:[-*]\s*)?(?:[*_`]+)?(?:인풋|입력값?|요청\s*명세|분석\s*입력)(?:[*_`]+)?\s*[:：]?\s*$", re.I),
    re.compile(r"^\s*(?:[-*]\s*)?(?:[*_`]+)?(?:분석\s*대상|관측\s*grain|기준\s*시간|기준\s*시각|기간|세그먼트|제외\s*조건|성공\s*지표)(?:[*_`]+)?\s*[:：]\s*.+$", re.I),
    re.compile(r"^\s*(?:[-*]\s*)?(?:[*_`]+)?(?:mode|analysis mode|input checklist|observation grain|segments?|exclusions?|success metric)(?:[*_`]+)?\s*[:：]\s*.+$", re.I),
]

OPERATIONAL_DETAIL_LINE_PATTERNS = [
    re.compile(r"^\s*(?:[-*•]\s*)?.*(?:dry[-_ ]?run|드라이\s*런|bq\s+query|dbt\s+compile|maximum_bytes_billed).*$", re.I),
    re.compile(r"^\s*(?:[-*•]\s*)?.*(?:Query successfully validated|will process|bytes\s+(?:processed|billed)|bytesBilledLimitExceeded).*$", re.I),
    re.compile(r"^\s*(?:[-*•]\s*)?.*(?:예상\s*스캔|스캔량|과금|BigQuery\s+[0-9.,]+\s*GiB|Claude\s+\d+\s*턴).*$", re.I),
    re.compile(r"^\s*(?:[-*•]\s*)?.*(?:MCP|mcp__|tool_use|permission_denials|malformed_lines).*$", re.I),
    re.compile(r"^\s*(?:[-*•]\s*)?(?:검증|검증\s*요약|validation|health\s*checks?)\s*[:：].*$", re.I),
    re.compile(r"^\s*(?:[-*•]\s*)?(?:재현|재현\s*SQL|repro(?:duction)?|reproduce|sql\s*files?)\s*[:：].*$", re.I),
    re.compile(r"^\s*(?:[-*•]\s*)?.*dbt/gharchive_metrics/analyses/[^ \n`]+\.sql.*$", re.I),
]

METABASE_FAILURE_LINE_PATTERN = re.compile(
    r".*(?:metabase|메타베이스).*(?:mcp|도구|tool|api).*(?:실패|오류|권한|거부|failed|error|401|403|permission|denied).*",
    re.I,
)

METABASE_URL_PATTERN = re.compile(
    r"https?://[^\s>|)]+/(?:question|dashboard)/\d+(?:[^\s>|)]*)?",
    re.I,
)


def _korean_particle(base: str, particle: str | None) -> str:
    if particle in {"은", "는"}:
        return base + "은"
    if particle in {"이", "가"}:
        return base + "이"
    if particle in {"을", "를"}:
        return base + "을"
    if particle == "의":
        return base + "의"
    return base


def casualize_slack_terms(text: str) -> str:
    """Translate schema-ish metric terms into Slack-friendly product language."""
    lines: list[str] = []
    in_fence = False
    for raw_line in (text or "").split("\n"):
        line = raw_line
        stripped = line.strip()
        if stripped.startswith("```"):
            in_fence = not in_fence
            lines.append(line)
            continue
        if in_fence or METABASE_URL_PATTERN.search(line):
            lines.append(line)
            continue

        line = re.sub(
            r"\bactive\s+actors?\s*(은|는|이|가|을|를|의)?",
            lambda m: _korean_particle("활동 계정", m.group(1)),
            line,
            flags=re.I,
        )
        line = re.sub(r"`?actor\.id`?", "계정 ID", line, flags=re.I)
        line = re.sub(r"\bactor당\s*(?:events?|이벤트)", "계정당 활동 기록", line, flags=re.I)
        line = re.sub(
            r"\bactor\s*(은|는|이|가|을|를|의)?",
            lambda m: _korean_particle("계정", m.group(1)),
            line,
            flags=re.I,
        )
        line = re.sub(r"\bevent\s*types?(?=별|[^\w]|$)", "활동 종류", line, flags=re.I)
        line = re.sub(
            r"\bevents?\s*(은|는|이|가|을|를|의)?",
            lambda m: _korean_particle("활동 기록", m.group(1)),
            line,
            flags=re.I,
        )
        line = re.sub(
            r"이벤트\s*(은|는|이|가|을|를|의)?",
            lambda m: _korean_particle("활동 기록", m.group(1)),
            line,
        )
        line = re.sub(r"\bdistinct\s+count(?=[가-힣]|[^\w]|$)", "고유 수", line, flags=re.I)
        line = re.sub(r"\bUTC\b", "데이터 기준", line)
        line = re.sub(r"\bmart\b", "집계 데이터", line, flags=re.I)
        line = re.sub(r"\braw\b", "원천 데이터", line, flags=re.I)
        line = re.sub(r"\bshards?\b", "날짜별 데이터", line, flags=re.I)
        line = re.sub(r"\bpartitions?\b", "날짜 구간", line, flags=re.I)
        line = re.sub(r"\bgrain(?=[가-힣]|[^\w]|$)", "집계 단위", line, flags=re.I)
        lines.append(line)
    return "\n".join(lines).strip()


def strip_leaked_preamble(text: str) -> str:
    """Remove assistant-facing scaffolding that should not reach Slack."""
    lines = (text or "").replace("\r\n", "\n").split("\n")
    start = 0
    while start < len(lines):
        line = lines[start].strip()
        normalized = line.strip("*_` ")
        if not normalized:
            start += 1
            continue
        if re.fullmatch(r"-{3,}|_{3,}|\*{3,}", normalized):
            start += 1
            continue
        if any(pattern.match(normalized) for pattern in LEAKED_PREAMBLE_PATTERNS):
            start += 1
            continue
        break
    body_lines = [
        line
        for line in lines[start:]
        if not re.fullmatch(r"\s*(?:-{3,}|_{3,}|\*{3,})\s*", line)
    ]
    return "\n".join(body_lines).strip()


def strip_internal_metadata_lines(text: str) -> str:
    """Remove checklist labels that are useful internally but noisy in Slack."""
    kept: list[str] = []
    previous_blank = False
    for line in (text or "").split("\n"):
        normalized = line.strip().strip("*_` ")
        if any(pattern.match(normalized) for pattern in INTERNAL_METADATA_LINE_PATTERNS):
            continue
        blank = not line.strip()
        if blank and previous_blank:
            continue
        kept.append(line)
        previous_blank = blank
    return "\n".join(kept).strip()


def strip_operational_detail_lines(text: str) -> str:
    """Remove CLI/audit details that belong in logs, not in the Slack answer."""
    kept: list[str] = []
    previous_blank = False
    for line in (text or "").split("\n"):
        normalized = line.strip().strip("*_` ")
        cleaned_line = line.replace("Metabase MCP", "Metabase").replace("메타베이스 MCP", "메타베이스")
        if METABASE_URL_PATTERN.search(cleaned_line):
            kept.append(cleaned_line)
            previous_blank = False
            continue
        if METABASE_FAILURE_LINE_PATTERN.match(normalized):
            replacement = "Metabase 카드는 생성하지 못했습니다. 분석 결과는 검증된 숫자 기준으로 답했습니다."
            if not kept or kept[-1] != replacement:
                kept.append(replacement)
            previous_blank = False
            continue
        if any(pattern.match(normalized) for pattern in OPERATIONAL_DETAIL_LINE_PATTERNS):
            continue
        blank = not line.strip()
        if blank and previous_blank:
            continue
        kept.append(cleaned_line)
        previous_blank = blank
    return "\n".join(kept).strip()


def markdown_to_slack_mrkdwn(text: str) -> str:
    """Convert common GitHub Markdown emitted by Claude into Slack mrkdwn."""
    lines: list[str] = []
    in_fence = False
    for raw_line in (text or "").split("\n"):
        line = raw_line.rstrip()
        stripped = line.strip()
        if stripped.startswith("```"):
            in_fence = not in_fence
            lines.append(line)
            continue
        if in_fence:
            lines.append(line)
            continue

        heading = re.match(r"^\s{0,3}#{1,6}\s+(.+?)\s*#*\s*$", line)
        if heading:
            lines.append(f"*{heading.group(1).strip()}*")
            continue

        line = re.sub(r"\[([^\]]+)\]\((https?://[^)\s]+)\)", r"<\2|\1>", line)
        line = re.sub(r"\*\*([^*\n]+)\*\*", r"*\1*", line)
        line = re.sub(r"__([^_\n]+)__", r"*\1*", line)
        line = re.sub(r"^\s*[-*]\s+", "• ", line)
        lines.append(line)
    return "\n".join(lines).strip()


def split_paragraphs(text: str) -> list[str]:
    return [p.strip() for p in re.split(r"\n{2,}", text or "") if p.strip()]


def fit_text(text: str, limit: int, marker: str = " …") -> str:
    if len(text) <= limit:
        return text
    if limit <= len(marker):
        return text[:limit]
    return text[: limit - len(marker)].rstrip() + marker


def format_elapsed_ms(elapsed_ms: int) -> str:
    seconds = max(1, elapsed_ms // 1000)
    minutes, remainder = divmod(seconds, 60)
    if minutes == 0:
        return f"{remainder}초"
    if remainder == 0:
        return f"{minutes}분"
    return f"{minutes}분 {remainder}초"


def progress_elapsed_context(elapsed_ms: int, max_duration_ms: int | None = None) -> str:
    elapsed = f"{format_elapsed_ms(elapsed_ms)} 경과"
    if max_duration_ms and max_duration_ms > 0:
        return f"{elapsed} / 최대 {format_elapsed_ms(max_duration_ms)} · 같은 thread에서 이어서 질문하면 맥락을 이어갑니다."
    return f"{elapsed} · 같은 thread에서 이어서 질문하면 맥락을 이어갑니다."


def compact_slack_answer(text: str, limit: int = MAX_FINAL_ANSWER_CHARS) -> str:
    """Keep the answer Slack-sized while preserving the conclusion and caveat.

    The analysis skill still asks Claude to produce a compact final answer. This is a
    last-mile guardrail for the known failure mode where the final Slack post becomes
    a wall of intake and health-check tables.
    """
    normalized = text.strip()
    if len(normalized) <= limit:
        return normalized

    paragraphs = split_paragraphs(normalized)
    if not paragraphs:
        return fit_text(normalized, limit)

    caveat = ""
    for paragraph in reversed(paragraphs):
        if re.search(r"(한계|caveat|주의|인과|계측|결측|편향|지연)", paragraph, re.I):
            caveat = paragraph
            break

    selected: list[str] = []
    used = 0
    reserve = len(caveat) + 80 if caveat else 80
    for paragraph in paragraphs:
        if paragraph == caveat:
            continue
        candidate = fit_text(paragraph, 900 if not selected else 650)
        addition = len(candidate) + (2 if selected else 0)
        if used + addition + reserve > limit:
            continue
        selected.append(candidate)
        used += addition
        if len(selected) >= 4:
            break

    if caveat:
        selected.append(fit_text(caveat, 500))

    suffix = "상세 실행 내역은 로그에서 확인하세요."
    candidate = "\n\n".join(selected + [suffix]).strip()
    if len(candidate) <= limit:
        return candidate
    return fit_text(candidate, limit)


def format_final_answer_for_slack(text: str, limit: int = MAX_FINAL_ANSWER_CHARS) -> str:
    cleaned = strip_leaked_preamble(redact_sensitive_text(text))
    cleaned = strip_internal_metadata_lines(cleaned)
    cleaned = strip_operational_detail_lines(cleaned)
    mrkdwn = markdown_to_slack_mrkdwn(cleaned)
    mrkdwn = casualize_slack_terms(mrkdwn)
    return redact_sensitive_text(compact_slack_answer(mrkdwn, limit))


def extract_metabase_url(text: str) -> str | None:
    match = METABASE_URL_PATTERN.search(text or "")
    if not match:
        return None
    return match.group(0).rstrip(".,")


SECRET_REDACTIONS = [
    re.compile(r"xox[baprs]-[A-Za-z0-9-]+"),
    re.compile(r"xapp-[A-Za-z0-9-]+"),
    re.compile(r"\bsk-ant-[A-Za-z0-9_-]{10,}"),
    re.compile(r"\bmb_[A-Za-z0-9_-]{8,}"),
    re.compile(
        r"\b(?:METABASE_API_KEY|ANTHROPIC_API_KEY|ANTHROPIC_AUTH_TOKEN|CLAUDE_CODE_OAUTH_TOKEN|ANALYST_AUDIT_DATABASE_URL)\s*[:=]\s*['\"]?[^\s'\"`]+",
        re.I,
    ),
    re.compile(r"\bpostgres(?:ql)?://[^:\s/@]+:[^@\s]+@", re.I),
    re.compile(r"(?s)-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"AIza[0-9A-Za-z_-]{20,}"),
]


def redact_sensitive_text(text: str) -> str:
    redacted = text or ""
    for pattern in SECRET_REDACTIONS:
        redacted = pattern.sub("[REDACTED]", redacted)
    return redacted


def redact_sensitive_value(value):
    if isinstance(value, str):
        return redact_sensitive_text(value)
    if isinstance(value, list):
        return [redact_sensitive_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_sensitive_value(item) for item in value)
    if isinstance(value, dict):
        return {key: redact_sensitive_value(item) for key, item in value.items()}
    return value


SCHEMA_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
AUDIT_DB_TABLES = ("threads", "turns", "turn_usage", "trace_events", "feedback")


def validate_audit_schema(schema: str) -> str:
    name = (schema or DEFAULT_AUDIT_SCHEMA).strip()
    if not SCHEMA_NAME_RE.match(name):
        raise ValueError(
            f"invalid audit schema name {schema!r}; use letters, digits, and underscores only"
        )
    return name


def describe_database_url(url: str) -> str:
    if not url:
        return "disabled"
    try:
        parsed = urlsplit(url)
    except ValueError:
        return "<invalid url>"
    host = parsed.hostname or "<unknown-host>"
    port = f":{parsed.port}" if parsed.port else ""
    db = parsed.path.lstrip("/") or "<unknown-db>"
    user = parsed.username or "<unknown-user>"
    return f"{parsed.scheme or 'postgres'}://{user}@{host}{port}/{db}"


def trace_event_type(line: str) -> str:
    token = (line or "").strip().split(maxsplit=1)[0] if line else "trace"
    token = token.rstrip("[]:").lower()
    return token or "trace"


def slack_progress_line(raw_line: str) -> str | None:
    """Map raw Claude stream traces to user-facing Slack progress text.

    Raw traces are still kept in local logs/audit. Slack should show product-level
    state, not implementation details like JSON event names, tool IDs, or truncated
    command strings.
    """
    line = raw_line.strip()
    if not line:
        return None
    if line.startswith("init "):
        return "분석 환경을 준비했습니다."
    if line.startswith("rate-limit "):
        if "allowed_warning" in line:
            return None
        return "실행 가능 상태를 확인하고 있습니다."
    if line.startswith("thinking"):
        return "질문을 해석하고 분석 계획을 세우고 있습니다."
    if line.startswith("text "):
        # Assistant text often contains internal narration ("I'll start with...").
        # The final answer is posted separately after sanitation.
        return None
    if line.startswith("done "):
        return None

    if line.startswith("tool "):
        detail = line[5:]
        if detail.startswith("Skill"):
            return "분석 절차와 안전 규칙을 확인하고 있습니다."
        if detail.startswith("mcp__metabase"):
            return "Metabase에서 볼 수 있는 카드/대시보드를 준비하고 있습니다."
        if detail.startswith(("Read", "Grep", "Glob")):
            return "사용할 데이터 모델과 분석 문서를 확인하고 있습니다."
        if detail.startswith(("Write", "Edit")):
            return "계산 근거를 정리하고 있습니다."
        if detail.startswith("Bash"):
            lowered = detail.lower()
            if "bq query" in lowered and "--dry_run" in lowered:
                return "쿼리 실행 전 안전 조건을 확인하고 있습니다."
            if "bq query" in lowered:
                return "BigQuery에서 필요한 데이터를 조회하고 있습니다."
            if "dbt compile" in lowered:
                return "dbt 분석 SQL을 컴파일하고 있습니다."
            if "dbt ls" in lowered:
                return "dbt lineage를 확인하고 있습니다."
            if "git status" in lowered:
                return "저장소 상태를 확인하고 있습니다."
            if any(token in lowered for token in ("cat ", "sed ", "rg ", "grep ", "ls ")):
                return "분석에 필요한 파일과 정의를 확인하고 있습니다."
        return "필요한 근거를 수집하고 있습니다."

    if line.startswith("result[error]"):
        return "차단된 도구 호출을 안전한 방식으로 바꿔 재시도하고 있습니다."
    if line.startswith("result[ok]"):
        return "도구 결과를 확인하고 다음 단계로 진행 중입니다."

    return None


def encode_project_dir(repo_dir: str) -> str:
    """Claude Code encodes a project cwd by replacing BOTH '/' and '.' with '-'."""
    absolute = str(Path(repo_dir).resolve())
    return absolute.replace("/", "-").replace(".", "-")


def transcript_path(repo_dir: str, session_id: str) -> Path:
    return (
        Path.home()
        / ".claude"
        / "projects"
        / encode_project_dir(repo_dir)
        / f"{session_id}.jsonl"
    )


def session_id_for(team: str, channel: str, thread_ts: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"slack://{team}/{channel}/{thread_ts}"))


def gcp_key_path(repo_dir: str) -> Path:
    env = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env:
        return Path(env).expanduser()
    return Path(repo_dir) / DEFAULT_GCP_KEY


def analyst_gcloud_config_path(repo_dir: str) -> Path:
    env = os.environ.get(ANALYST_GCLOUD_CONFIG_ENV)
    if env:
        return Path(env).expanduser()
    return Path(repo_dir) / ANALYST_GCLOUD_CONFIG


def bigqueryrc_path(repo_dir: str) -> Path:
    return analyst_gcloud_config_path(repo_dir) / ".bigqueryrc"


def ensure_bigqueryrc(cfg: "Config") -> Path:
    """Write the rc file that supplies `bq`'s scan ceiling by default.

    Rewritten on every startup so `--max-scan-gib` cannot silently disagree with what
    the sessions actually get. Raising the ceiling is therefore an operator action
    (restart with a new value), which is the approval step.

    Honest limit: this sets a DEFAULT, not a cap. A session that passes an explicit
    `--maximum_bytes_billed` on the command line still wins, and no tool-permission
    pattern can catch that (deny matching sees command prefixes, not mid-command
    flags). The remaining defences are the skill's prose rule and the per-turn footer,
    which reports bytes actually billed and so makes an override visible after the
    fact. A project-level "query usage per user per day" quota is the only airtight
    ceiling and has to be set in the cloud console.
    """
    path = bigqueryrc_path(cfg.repo_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "# Generated by slack_analyst_bot.py on startup -- edits are overwritten.\n"
        "# Sets the default scan ceiling for every `bq query` in an analyst session.\n"
        "[query]\n"
        f"--maximum_bytes_billed={cfg.max_scan_bytes}\n",
        encoding="utf-8",
    )
    path.chmod(0o600)
    return path


def child_env(cfg: "Config") -> dict[str, str]:
    """Environment for the spawned `claude` process.

    The operator's shell may not have the BigQuery credentials exported. We inject
    them explicitly because the analysis skill runs `bq` and dbt inside the session:
    dbt's profile reads GCP_KEY_PATH and fails at PARSE time with
    "Env var required but not provided: 'GCP_KEY_PATH'" when it is absent, which
    surfaces to the user as an opaque dbt crash rather than a missing-credential error.
    """
    env = os.environ.copy()
    key = str(gcp_key_path(cfg.repo_dir))
    env.setdefault("GCP_KEY_PATH", key)
    env.setdefault("GOOGLE_APPLICATION_CREDENTIALS", key)
    # `bq` does NOT read GOOGLE_APPLICATION_CREDENTIALS -- it uses whatever account is
    # active in the gcloud credential store. Measured 2026-07-26: with the read-only
    # key exported, `select session_user()` still returned the operator's personal
    # account, i.e. the read-only boundary did not exist on the `bq` path at all.
    gcloud_cfg = analyst_gcloud_config_path(cfg.repo_dir)
    # Pointing CLOUDSDK_CONFIG at a dedicated config dir where only the analyst
    # service account is activated is what actually enforces it. Forced, not
    # setdefault: this is the security boundary, so the parent env must not win.
    env["CLOUDSDK_CONFIG"] = str(gcloud_cfg)
    # Scan ceiling, part 2. BIGQUERY_MAXIMUM_BYTES_BILLED above is read by the Python
    # client (so it binds dbt) but `bq` IGNORES it -- measured 2026-07-26: an 18.4 GB
    # raw sweep ran to completion under a 10 GiB env ceiling. `bq` honours the flag
    # `--maximum_bytes_billed`, and a [query] entry in the rc file supplies that flag
    # without the session having to pass it. Not airtight: an explicit flag on the
    # command line still overrides the rc default. See ensure_bigqueryrc.
    env["BIGQUERYRC"] = str(bigqueryrc_path(cfg.repo_dir))
    # Python/dbt client scan ceiling. `bq` ignores this env var, so the bq CLI path is
    # covered separately by BIGQUERYRC above. Force this value as well: a wider parent
    # shell env must not silently weaken the dbt/Python-client ceiling.
    env["BIGQUERY_MAXIMUM_BYTES_BILLED"] = str(cfg.max_scan_bytes)
    # Never leak the alerting app's Slack token into the analysis session.
    env.pop("SLACK_BOT_TOKEN", None)
    env.pop(BOT_TOKEN_ENV, None)
    env.pop(APP_TOKEN_ENV, None)
    # Audit DB credentials belong to the bot process only. The spawned analysis
    # session should be able to read data through BigQuery, not inspect or mutate
    # the Postgres/RDS audit store.
    env.pop(AUDIT_DATABASE_URL_ENV, None)
    # Metabase credentials are passed to the MCP server through its dedicated MCP
    # config, not as generic Claude child env. If MCP is disabled, strip a parent
    # shell key so a non-Metabase turn cannot leak it via stderr/final text.
    if not metabase_mcp_ready(cfg):
        env.pop("METABASE_API_KEY", None)
    return env


# --- Turn execution ---------------------------------------------------------
@dataclass
class Config:
    repo_dir: str = DEFAULT_REPO_DIR
    state_dir: str = DEFAULT_STATE_DIR
    headless: bool = False
    channel_allowlist: list[str] = field(default_factory=list)
    max_budget_usd: float = 5.0
    # 10 GiB. Applied as a bq CLI default via .bigqueryrc and as a Python/dbt client
    # ceiling via BIGQUERY_MAXIMUM_BYTES_BILLED. Not an airtight cloud quota: an
    # explicit bq flag can override the rc default. Raising it is an operator decision
    # -- restart with `--max-scan-gib N`.
    max_scan_bytes: int = 10 * 1024**3
    timeout: int = 900
    max_workers: int = 1
    model: str | None = None
    log_level: str = "INFO"
    enable_metabase_mcp: bool = False
    metabase_url: str = ""
    metabase_public_url: str = ""
    metabase_api_key: str = ""
    metabase_collection_name: str = "BDA 데이터 플랫폼"
    audit_database_url: str = ""
    audit_schema: str = DEFAULT_AUDIT_SCHEMA
    # Overridable only so --self-test can exercise the spawn/stream path without
    # dragging the whole analysis skill into a trivial prompt.
    append_system_prompt: str = APPEND_SYSTEM_PROMPT


@dataclass
class TurnResult:
    ok: bool = False
    session_id: str | None = None
    final_text: str = ""
    error: str | None = None
    cancelled: bool = False
    num_turns: int | None = None
    duration_ms: int | None = None
    total_cost_usd: float | None = None
    permission_denials: list[dict] = field(default_factory=list)
    bad_json_lines: int = 0
    trace: list[str] = field(default_factory=list)
    already_in_use: bool = False


def metabase_mcp_ready(cfg: Config) -> bool:
    return bool(cfg.enable_metabase_mcp and cfg.metabase_url and cfg.metabase_api_key)


def normalized_metabase_url(url: str) -> str:
    return (url or "").rstrip("/")


def metabase_slack_url_base(cfg: Config) -> str:
    return normalized_metabase_url(cfg.metabase_public_url or cfg.metabase_url)


def metabase_mcp_config_path(cfg: Config) -> Path:
    return Path(cfg.state_dir).expanduser() / "runtime" / METABASE_RUNTIME_CONFIG_FILENAME


def ensure_metabase_mcp_config(cfg: Config) -> Path | None:
    """Write a per-bot runtime MCP config without exposing the API key in argv/logs."""
    if not metabase_mcp_ready(cfg):
        return None
    path = metabase_mcp_config_path(cfg)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "mcpServers": {
            "metabase": {
                "type": "stdio",
                "command": "npx",
                "args": ["-y", "@easecloudio/mcp-metabase-server"],
                "env": {
                    "METABASE_URL": normalized_metabase_url(cfg.metabase_url),
                    "METABASE_API_KEY": cfg.metabase_api_key,
                },
            }
        }
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    path.chmod(0o600)
    return path


def allowed_tools(cfg: Config) -> list[str]:
    if not metabase_mcp_ready(cfg):
        return list(BASE_ALLOWED_TOOLS)
    return [*BASE_ALLOWED_TOOLS, *METABASE_MCP_TOOLS]


def effective_append_system_prompt(cfg: Config) -> str:
    if not metabase_mcp_ready(cfg):
        return cfg.append_system_prompt
    metabase_url = normalized_metabase_url(cfg.metabase_url)
    slack_url = metabase_slack_url_base(cfg)
    return (
        cfg.append_system_prompt
        + METABASE_APPEND_PROMPT_TEMPLATE.format(
            collection_name=cfg.metabase_collection_name,
            metabase_url=slack_url or metabase_url,
        )
    )


def rewrite_metabase_links_for_slack(text: str, cfg: Config) -> str:
    internal_url = normalized_metabase_url(cfg.metabase_url)
    public_url = metabase_slack_url_base(cfg)
    if not internal_url or not public_url or internal_url == public_url:
        return text
    return (text or "").replace(internal_url, public_url)


def build_command(cfg: Config, session_id: str, resume: bool) -> list[str]:
    mcp_config = ensure_metabase_mcp_config(cfg)
    cmd = [
        "claude",
        "-p",
        "--output-format",
        "stream-json",
        "--verbose",  # stream-json REQUIRES --verbose
        "--permission-mode",
        "manual",
        "--max-budget-usd",
        str(cfg.max_budget_usd),
        # Tool surface exposed to the analysis session. Read/Grep/Glob are included
        # for Slack UX ergonomics; Docker narrow mounts and outbound redaction are the
        # defence line for secrets.
        "--tools",
        SESSION_TOOLS,
        "--append-system-prompt",
        effective_append_system_prompt(cfg),
    ]
    if mcp_config is not None:
        cmd += ["--strict-mcp-config", "--mcp-config", str(mcp_config)]
    if cfg.model:
        cmd += ["--model", cfg.model]
    cmd += ["--resume" if resume else "--session-id", session_id]
    # These flags are variadic and would swallow a positional prompt, which is
    # exactly why the prompt goes in over stdin instead. Keep them LAST.
    cmd += ["--allowedTools", *allowed_tools(cfg)]
    cmd += ["--disallowedTools", *DISALLOWED_TOOLS]
    return cmd


def _describe_tool_use(block: dict) -> str:
    name = block.get("name", "?")
    tool_input = block.get("input") or {}
    if name == "Bash":
        detail = tool_input.get("command", "")
    elif name in ("Read", "Write", "Edit"):
        detail = tool_input.get("file_path", "")
    elif name == "Grep":
        detail = f"{tool_input.get('pattern', '')} {tool_input.get('path', '')}"
    elif name == "Skill":
        detail = tool_input.get("skill", "") or tool_input.get("name", "")
    elif name == "TodoWrite":
        todos = tool_input.get("todos") or []
        detail = f"{len(todos)} items"
    else:
        detail = json.dumps(tool_input, ensure_ascii=False)
    return truncate_line(f"{name}: {detail}", 140)


def run_turn(
    cfg: Config,
    session_id: str,
    prompt: str,
    *,
    resume: bool,
    cancel_event: threading.Event | None = None,
    on_trace=None,
    on_text=None,
) -> TurnResult:
    """Spawn one `claude -p` turn and parse its stream-json output.

    `on_trace(line)` is called for each compact progress line; `on_text(text)` for
    each assistant text block. Neither is allowed to raise fatally - callbacks are
    wrapped so a Slack outage cannot kill the turn.
    """
    result = TurnResult(session_id=session_id)
    try:
        cmd = build_command(cfg, session_id, resume)
    except OSError as exc:
        result.error = f"Metabase MCP 설정 파일 생성 실패: {exc}"
        return result

    def emit(line: str) -> None:
        result.trace.append(line)
        if on_trace is not None:
            try:
                on_trace(line)
            except Exception:  # noqa: BLE001 - a sink failure must not abort the turn
                LOG.exception("trace sink failed")

    if shutil.which("claude") is None:
        result.error = (
            "`claude` 실행 파일을 PATH에서 찾을 수 없습니다. "
            "Claude Code CLI를 설치하고 PATH에 추가하세요."
        )
        return result

    LOG.info(
        "spawn session=%s mode=%s cwd=%s",
        session_id,
        "resume" if resume else "create",
        cfg.repo_dir,
    )

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=cfg.repo_dir,  # --resume is scoped to the cwd's project dir
            env=child_env(cfg),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            start_new_session=True,  # own process group so we can kill the whole tree
        )
    except OSError as exc:
        result.error = f"claude 프로세스 시작 실패: {exc}"
        return result

    stderr_buf: list[str] = []

    def drain_stderr() -> None:
        assert proc.stderr is not None
        for line in proc.stderr:
            stderr_buf.append(line)
            if len(stderr_buf) > 200:
                del stderr_buf[:100]

    stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
    stderr_thread.start()

    timer_fired = threading.Event()
    cancel_fired = threading.Event()

    def kill_process_group() -> None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass

    def on_timeout() -> None:
        timer_fired.set()
        kill_process_group()

    timer = threading.Timer(cfg.timeout, on_timeout)
    timer.daemon = True
    timer.start()

    cancel_thread: threading.Thread | None = None
    cancel_watcher_stop = threading.Event()
    if cancel_event is not None:
        def watch_cancel() -> None:
            while not cancel_watcher_stop.is_set():
                if not cancel_event.wait(timeout=0.2):
                    continue
                if proc.poll() is None:
                    cancel_fired.set()
                    kill_process_group()
                return

        cancel_thread = threading.Thread(target=watch_cancel, daemon=True)
        cancel_thread.start()

    try:
        if proc.stdin is not None:
            try:
                proc.stdin.write(prompt)
            except (BrokenPipeError, OSError):
                pass
            finally:
                # Close stdin or the CLI warns after ~3s waiting for more input.
                try:
                    proc.stdin.close()
                except OSError:
                    pass

        assert proc.stdout is not None
        for raw in proc.stdout:
            raw = raw.strip()
            if not raw:
                continue
            try:
                event = json.loads(raw)
            except json.JSONDecodeError:
                result.bad_json_lines += 1
                LOG.debug("skipping malformed json line: %s", truncate_line(raw, 200))
                continue
            _handle_event(event, result, emit, on_text)

        proc.wait()
    finally:
        timer.cancel()
        cancel_watcher_stop.set()
        if cancel_thread is not None:
            cancel_thread.join(timeout=1)
        stderr_thread.join(timeout=2)
        try:
            if proc.poll() is None:
                kill_process_group()
        except (ProcessLookupError, PermissionError):
            pass

    stderr_text = "".join(stderr_buf)
    if "already in use" in stderr_text:
        result.already_in_use = True

    if timer_fired.is_set():
        result.ok = False
        result.error = f"타임아웃({cfg.timeout}s) 초과로 세션 프로세스를 종료했습니다."
        return result

    if cancel_fired.is_set():
        result.ok = False
        result.cancelled = True
        result.error = "사용자 요청으로 세션 프로세스를 종료했습니다."
        return result

    if proc.returncode != 0 and not result.ok:
        tail = "\n".join(stderr_text.strip().splitlines()[-12:])
        result.error = f"claude 종료 코드 {proc.returncode}\n{tail}".strip()
        return result

    if not result.ok and result.error is None:
        tail = "\n".join(stderr_text.strip().splitlines()[-12:])
        result.error = f"result 이벤트를 받지 못했습니다.\n{tail}".strip()
    return result


def _handle_event(event: dict, result: TurnResult, emit, on_text) -> None:
    etype = event.get("type")

    if etype == "system":
        if event.get("subtype") == "init":
            result.session_id = event.get("session_id") or result.session_id
            emit(
                f"init model={event.get('model', '?')} "
                f"tools={len(event.get('tools') or [])} "
                f"session={result.session_id}"
            )
        # `status` and `thinking_tokens` subtypes are noise - drop them.
        return

    if etype == "assistant":
        for block in (event.get("message") or {}).get("content") or []:
            btype = block.get("type")
            if btype == "text":
                text = block.get("text") or ""
                if text.strip():
                    emit(f"text {truncate_line(text, 140)}")
                    if on_text is not None:
                        try:
                            on_text(text)
                        except Exception:  # noqa: BLE001
                            LOG.exception("text sink failed")
            elif btype == "thinking":
                emit("thinking …")
            elif btype == "tool_use":
                emit(f"tool {_describe_tool_use(block)}")
        return

    if etype == "user":
        for block in (event.get("message") or {}).get("content") or []:
            if block.get("type") == "tool_result":
                flag = "error" if block.get("is_error") else "ok"
                content = block.get("content")
                if isinstance(content, list):
                    content = " ".join(
                        str(c.get("text", "")) for c in content if isinstance(c, dict)
                    )
                emit(f"result[{flag}] {truncate_line(content or '', 120)}")
        return

    if etype == "rate_limit_event":
        emit(f"rate-limit {truncate_line(json.dumps(event, ensure_ascii=False), 120)}")
        return

    if etype == "result":
        result.session_id = event.get("session_id") or result.session_id
        result.num_turns = event.get("num_turns")
        result.duration_ms = event.get("duration_ms")
        result.total_cost_usd = event.get("total_cost_usd")
        result.permission_denials = event.get("permission_denials") or []
        result.final_text = event.get("result") or ""
        subtype = event.get("subtype")
        result.ok = subtype == "success" and not event.get("is_error")
        if not result.ok:
            result.error = f"claude result subtype={subtype} is_error={event.get('is_error')}"
        emit(
            f"done subtype={subtype} turns={result.num_turns} "
            f"cost=${(result.total_cost_usd or 0):.4f}"
        )


# --- Session bookkeeping ----------------------------------------------------
@dataclass
class ScanUsage:
    bytes_billed: int = 0
    gib: float = 0.0
    usd: float = 0.0
    jobs: int = 0
    error: str | None = None


# BigQuery on-demand list price, US multi-region, 2026-07. First 1 TiB/month is
# free, which this deliberately ignores -- the number is meant to read as "what this
# question would cost if the free tier were already spent", not as an invoice line.
BQ_USD_PER_TIB = 6.25


def bq_usage_since(cfg: "Config", since: datetime) -> ScanUsage:
    """Bytes actually billed to the analyst service account since `since`.

    Runs in the bot process with the same read-only analyst key that the session
    uses. `JOBS_BY_USER` only exposes the authenticated principal's own jobs, so
    the service account only needs `bigquery.jobs.list` rather than `jobs.listAll`.

    Never raises -- a turn must still report its answer if billing lookup fails.
    """
    try:
        key = gcp_key_path(cfg.repo_dir)
        if not key.is_file():
            return ScanUsage(
                error=(
                    "BigQuery usage lookup disabled: analyst key is not mounted "
                    f"at {key}"
                )
            )

        from google.cloud import bigquery
        from google.oauth2 import service_account

        creds = service_account.Credentials.from_service_account_file(
            str(key), scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        client = bigquery.Client(credentials=creds, project=BQ_PROJECT)
        job = client.query(
            """
            select
              count(*) as jobs,
              ifnull(sum(total_bytes_billed), 0) as bytes_billed
            from `region-us`.INFORMATION_SCHEMA.JOBS_BY_USER
            where creation_time >= @since
              and job_type = 'QUERY'
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("since", "TIMESTAMP", since),
                ]
            ),
        )
        row = next(iter(job.result()), None)
        if row is None:
            return ScanUsage()
        bytes_billed = row.bytes_billed or 0
        gib = bytes_billed / 1024**3
        return ScanUsage(
            bytes_billed=bytes_billed,
            gib=gib,
            usd=bytes_billed / 1024**4 * BQ_USD_PER_TIB,
            jobs=row.jobs or 0,
        )
    except Exception as exc:  # noqa: BLE001 - accounting must never kill a turn
        LOG.warning("BigQuery usage lookup failed: %s", exc)
        return ScanUsage(error=str(exc))


@dataclass
class _ThreadQueueState:
    condition: threading.Condition
    next_ticket: int = 0
    serving_ticket: int = 0
    waiters: int = 0
    active: bool = False
    last_used: float = field(default_factory=time.monotonic)


class SessionRegistry:
    """Duplicate suppression, per-thread FIFO, and global turn concurrency.

    Two concurrent `--resume` calls for one Claude session can silently branch the
    transcript. A plain `threading.Lock` prevents overlap but does not guarantee FIFO
    order, and one Slack storm can still start an unbounded number of Claude workers.
    This registry makes both constraints explicit.
    """

    def __init__(self, max_concurrent_turns: int = 3) -> None:
        if max_concurrent_turns < 1:
            raise ValueError("max_concurrent_turns must be >= 1")
        self._registry_lock = threading.Lock()
        self._queues: dict[tuple[str, str], _ThreadQueueState] = {}
        self._seen_events: set[str] = set()
        self._seen_order: list[str] = []
        self._global_slots = threading.BoundedSemaphore(max_concurrent_turns)

    @contextmanager
    def claim(self, channel: str, thread_ts: str) -> Iterator[None]:
        key = (channel, thread_ts)
        with self._registry_lock:
            state = self._queues.get(key)
            if state is None:
                state = _ThreadQueueState(condition=threading.Condition(self._registry_lock))
                self._queues[key] = state
            ticket = state.next_ticket
            state.next_ticket += 1
            state.waiters += 1
            while ticket != state.serving_ticket:
                state.condition.wait()
            state.waiters -= 1
            state.active = True
            state.last_used = time.monotonic()

        acquired = False
        try:
            self._global_slots.acquire()
            acquired = True
            yield
        finally:
            if acquired:
                self._global_slots.release()
            with self._registry_lock:
                current = self._queues.get(key)
                if current is state:
                    state.active = False
                    state.serving_ticket += 1
                    state.last_used = time.monotonic()
                    state.condition.notify_all()
                    if (
                        state.waiters == 0
                        and not state.active
                        and state.serving_ticket == state.next_ticket
                    ):
                        self._queues.pop(key, None)

    def is_duplicate(self, event_key: str | None) -> bool:
        if not event_key:
            return False
        with self._registry_lock:
            if event_key in self._seen_events:
                return True
            self._seen_events.add(event_key)
            self._seen_order.append(event_key)
            if len(self._seen_order) > 2000:
                for stale in self._seen_order[:1000]:
                    self._seen_events.discard(stale)
                del self._seen_order[:1000]
            return False

    def known_thread(self, channel: str, thread_ts: str) -> bool:
        with self._registry_lock:
            return (channel, thread_ts) in self._queues


@dataclass
class ActiveTurn:
    cancel_event: threading.Event
    channel: str
    thread_ts: str
    pending_ts: str | None
    requester: str
    started_at: datetime
    slack_trace: object | None = None
    cancel_requested_by: str | None = None
    cancel_requested_at: str | None = None


class ActiveTurnRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._turns: dict[str, ActiveTurn] = {}

    def register(self, run_id: str, turn: ActiveTurn) -> None:
        with self._lock:
            self._turns[run_id] = turn

    def finish(self, run_id: str) -> None:
        with self._lock:
            self._turns.pop(run_id, None)

    def request_cancel(self, run_id: str, user: str) -> ActiveTurn | None:
        with self._lock:
            turn = self._turns.get(run_id)
            if turn is None:
                return None
            turn.cancel_requested_by = user
            turn.cancel_requested_at = datetime.now(timezone.utc).isoformat()
            turn.cancel_event.set()
            return turn

    def request_cancel_all(self, user: str) -> list[ActiveTurn]:
        with self._lock:
            turns = list(self._turns.values())
            now = datetime.now(timezone.utc).isoformat()
            for turn in turns:
                if not turn.cancel_requested_by:
                    turn.cancel_requested_by = user
                    turn.cancel_requested_at = now
                turn.cancel_event.set()
            return turns


def safe_state_key(*parts: str) -> str:
    return "_".join(parts).replace("/", "_").replace(":", "_").replace(".", "_")


class PostgresAuditSink:
    """Optional Postgres/RDS audit sink.

    The sink opens short-lived connections instead of keeping one forever: Slack turns
    are sparse, and reconnecting cleanly is simpler than nursing a stale pooler socket.
    """

    def __init__(self, database_url: str, schema: str) -> None:
        self.database_url = database_url
        self.schema = validate_audit_schema(schema)
        self._lock = threading.Lock()

    def _connect(self):
        import psycopg

        return psycopg.connect(self.database_url, connect_timeout=5)

    def _table(self, name: str):
        from psycopg import sql

        return sql.Identifier(self.schema, name)

    def _upsert_thread(self, cur, record: dict) -> int:
        from psycopg import sql

        cur.execute(
            sql.SQL(
                """
                insert into {} (
                  team_id, channel_id, thread_ts, session_id, first_seen_at, last_seen_at
                )
                values (%s, %s, %s, %s, %s, %s)
                on conflict (team_id, channel_id, thread_ts)
                do update set
                  session_id = excluded.session_id,
                  last_seen_at = excluded.last_seen_at
                returning thread_id
                """
            ).format(self._table("threads")),
            (
                record.get("team"),
                record.get("channel"),
                record.get("thread_ts"),
                record.get("session_id"),
                record.get("created_at"),
                record.get("completed_at") or record.get("created_at"),
            ),
        )
        row = cur.fetchone()
        return int(row[0])

    def record_turn(self, record: dict) -> None:
        from psycopg import sql
        from psycopg.types.json import Jsonb

        payload = redact_sensitive_value(dict(record))
        with self._lock, self._connect() as conn, conn.cursor() as cur:
            thread_id = self._upsert_thread(cur, payload)
            cur.execute(
                sql.SQL(
                    """
                    insert into {} (
                      audit_id, thread_id, status, requester, question, answer,
                      slack_message_ts, started_at, completed_at, duration_ms,
                      resume, session_id, error, raw_record
                    )
                    values (
                      %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s,
                      %s, %s, %s, %s
                    )
                    on conflict (audit_id)
                    do update set
                      thread_id = excluded.thread_id,
                      status = excluded.status,
                      requester = excluded.requester,
                      question = excluded.question,
                      answer = excluded.answer,
                      slack_message_ts = excluded.slack_message_ts,
                      completed_at = excluded.completed_at,
                      duration_ms = excluded.duration_ms,
                      resume = excluded.resume,
                      session_id = excluded.session_id,
                      error = excluded.error,
                      raw_record = excluded.raw_record
                    """
                ).format(self._table("turns")),
                (
                    payload.get("audit_id"),
                    thread_id,
                    payload.get("status"),
                    payload.get("requester"),
                    payload.get("question"),
                    payload.get("answer"),
                    payload.get("answer_message_ts"),
                    payload.get("created_at"),
                    payload.get("completed_at"),
                    payload.get("duration_ms"),
                    bool(payload.get("resume")),
                    payload.get("session_id"),
                    payload.get("error"),
                    Jsonb(payload),
                ),
            )
            cur.execute(
                sql.SQL(
                    """
                    insert into {} (
                      audit_id, claude_turns, claude_duration_ms,
                      claude_cost_usd_equivalent, bq_jobs, bq_billed_bytes,
                      bq_gib, bq_usd, bq_error, permission_denials, bad_json_lines
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    on conflict (audit_id)
                    do update set
                      claude_turns = excluded.claude_turns,
                      claude_duration_ms = excluded.claude_duration_ms,
                      claude_cost_usd_equivalent = excluded.claude_cost_usd_equivalent,
                      bq_jobs = excluded.bq_jobs,
                      bq_billed_bytes = excluded.bq_billed_bytes,
                      bq_gib = excluded.bq_gib,
                      bq_usd = excluded.bq_usd,
                      bq_error = excluded.bq_error,
                      permission_denials = excluded.permission_denials,
                      bad_json_lines = excluded.bad_json_lines
                    """
                ).format(self._table("turn_usage")),
                (
                    payload.get("audit_id"),
                    payload.get("claude_turns"),
                    payload.get("claude_duration_ms"),
                    payload.get("claude_cost_usd_equivalent"),
                    payload.get("bq_jobs"),
                    payload.get("bq_billed_bytes"),
                    payload.get("bq_gib"),
                    payload.get("bq_usd"),
                    payload.get("bq_error"),
                    Jsonb(payload.get("permission_denials") or []),
                    payload.get("bad_json_lines") or 0,
                ),
            )

    def record_trace(self, trace: list[str], summary: dict) -> None:
        from psycopg import sql
        from psycopg.types.json import Jsonb

        payload = redact_sensitive_value(dict(summary))
        audit_id = payload.get("audit_id")
        if not audit_id:
            return
        rows = [
            (
                audit_id,
                seq,
                datetime.now(timezone.utc).isoformat(),
                trace_event_type(line),
                redact_sensitive_text(line),
                Jsonb({"raw_line": redact_sensitive_text(line)}),
            )
            for seq, line in enumerate(trace, start=1)
        ]
        rows.append(
            (
                audit_id,
                len(rows) + 1,
                datetime.now(timezone.utc).isoformat(),
                "summary",
                None,
                Jsonb(payload),
            )
        )
        with self._lock, self._connect() as conn, conn.cursor() as cur:
            cur.executemany(
                sql.SQL(
                    """
                    insert into {} (audit_id, seq, event_ts, event_type, text, payload)
                    values (%s, %s, %s, %s, %s, %s)
                    on conflict (audit_id, seq)
                    do update set
                      event_ts = excluded.event_ts,
                      event_type = excluded.event_type,
                      text = excluded.text,
                      payload = excluded.payload
                    """
                ).format(self._table("trace_events")),
                rows,
            )

    def record_feedback(self, record: dict) -> None:
        from psycopg import sql

        payload = redact_sensitive_value(dict(record))
        with self._lock, self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    insert into {} (
                      audit_id, feedback_type, user_id, channel_id, thread_ts, created_at
                    )
                    values (%s, %s, %s, %s, %s, %s)
                    """
                ).format(self._table("feedback")),
                (
                    payload.get("audit_id"),
                    payload.get("feedback_type"),
                    payload.get("user"),
                    payload.get("channel"),
                    payload.get("thread_ts"),
                    payload.get("ts"),
                ),
            )


class BotAuditLogger:
    """Append-only local audit and feedback logs.

    This intentionally lives under `logs/` by default, which is already gitignored.
    Records are operational evidence, not source artifacts.
    """

    def __init__(
        self,
        state_dir: str,
        database_url: str = "",
        database_schema: str = DEFAULT_AUDIT_SCHEMA,
    ) -> None:
        self.state_dir = Path(state_dir).expanduser()
        self.db_sink: PostgresAuditSink | None = None
        if database_url:
            self.db_sink = PostgresAuditSink(database_url, database_schema)

    def _date_key(self, dt: datetime) -> str:
        kst = dt.astimezone(timezone(timedelta(hours=9)))
        return kst.date().isoformat()

    def _append_jsonl(self, relpath: str, record: dict) -> None:
        path = self.state_dir / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

    def record_turn(self, **record) -> None:
        completed_at = record.get("completed_at")
        if isinstance(completed_at, datetime):
            dt = completed_at
        elif isinstance(completed_at, str):
            try:
                dt = datetime.fromisoformat(completed_at)
            except ValueError:
                dt = datetime.now(timezone.utc)
        else:
            dt = datetime.now(timezone.utc)
        payload = redact_sensitive_value(dict(record))
        self._append_jsonl(f"audit/{self._date_key(dt)}.jsonl", payload)
        if self.db_sink is not None:
            try:
                self.db_sink.record_turn(payload)
            except Exception:  # noqa: BLE001 - DB must not hide Slack delivery
                LOG.exception("failed to write turn audit to Postgres")

    def record_trace(self, channel: str, thread_ts: str, trace: list[str], summary: dict) -> None:
        key = safe_state_key(channel, thread_ts)
        for line in trace:
            self._append_jsonl(
                f"traces/{key}.jsonl",
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "type": "trace",
                    "line": redact_sensitive_text(line),
                },
            )
        summary_record = redact_sensitive_value({
            "ts": datetime.now(timezone.utc).isoformat(),
            "type": "summary",
            **summary,
        })
        self._append_jsonl(f"traces/{key}.jsonl", summary_record)
        if self.db_sink is not None:
            try:
                self.db_sink.record_trace(trace, summary_record)
            except Exception:  # noqa: BLE001
                LOG.exception("failed to write trace audit to Postgres")

    def record_feedback(
        self,
        *,
        audit_id: str,
        feedback_type: str,
        user: str,
        channel: str,
        thread_ts: str,
    ) -> None:
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "audit_id": audit_id,
            "feedback_type": feedback_type,
            "user": user,
            "channel": channel,
            "thread_ts": thread_ts,
        }
        payload = redact_sensitive_value(record)
        self._append_jsonl(f"feedback/{self._date_key(datetime.now(timezone.utc))}.jsonl", payload)
        if self.db_sink is not None:
            try:
                self.db_sink.record_feedback(payload)
            except Exception:  # noqa: BLE001
                LOG.exception("failed to write feedback audit to Postgres")


def section_block(text: str) -> dict:
    return {
        "type": "section",
        # Slack may otherwise collapse longer bot-authored sections behind "Show more".
        # `expand` is supported by section blocks and is specifically useful for AI-style
        # answers where the whole response should be visible without an extra click.
        "expand": True,
        "text": {"type": "mrkdwn", "text": text[:MAX_SLACK_CHUNK]},
    }


def context_block(text: str) -> dict:
    return {
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": text[:MAX_SLACK_CHUNK]}],
    }


def cancellation_requested_payload(elapsed_ms: int, requested_by: str | None) -> tuple[str, list[dict]]:
    actor = f"<@{requested_by}> 요청으로 " if requested_by else ""
    elapsed = format_elapsed_ms(elapsed_ms)
    text = f":octagonal_sign: *분석 중단 요청됨*\n{actor}현재 실행 중인 작업을 중단하고 있습니다."
    blocks = [
        section_block(text),
        context_block(f"{elapsed} 경과 · 실행 중인 분석 작업에 종료 신호를 보냈습니다."),
    ]
    return text, blocks


def cancelled_payload(
    elapsed_ms: int,
    requested_by: str | None,
    scan: ScanUsage | None = None,
) -> tuple[str, list[dict]]:
    actor = f"<@{requested_by}> 요청으로 " if requested_by else ""
    elapsed = format_elapsed_ms(elapsed_ms)
    text = f":white_check_mark: *분석 중단됨*\n{actor}분석을 멈췄습니다. 부분 결과는 게시하지 않았습니다."
    details = f"{elapsed} 경과"
    blocks = [
        section_block(text),
        context_block(f"{details} · 이미 시작된 작업의 세부 내역은 실행 로그에 남깁니다."),
    ]
    return text, blocks


def user_facing_error(error: str | None, permission_denials: list[dict] | None = None) -> str:
    text = error or ""
    lowered = text.lower()
    if "타임아웃" in text or "timed out" in lowered or "timeout" in lowered:
        return "분석 시간이 제한을 넘어 작업을 중단했습니다. 범위를 더 좁혀 다시 요청하세요."
    if "bytesbilledlimitexceeded" in lowered or "maximum_bytes_billed" in lowered:
        return "요청 범위가 실행 상한을 넘어 중단했습니다. 기간이나 범위를 줄여 다시 요청하세요."
    if "permission" in lowered or "denied" in lowered or permission_denials:
        return "권한 제한에 걸려 이 요청을 완료하지 못했습니다. 원시 오류는 로컬 audit 로그에 남겼습니다."
    if "claude" in lowered and ("401" in lowered or "oauth" in lowered or "auth" in lowered):
        return "Claude 인증 상태를 확인해야 합니다. 터미널에서 로그인 상태를 점검하세요."
    if "already in use" in lowered:
        return "같은 thread의 이전 작업이 아직 정리되지 않았습니다. 잠시 뒤 다시 요청하세요."
    return "분석 중 오류가 발생했습니다. 원시 오류는 로컬 audit 로그에 남겼습니다."


def error_payload(message: str) -> tuple[str, list[dict]]:
    text = f":rotating_light: *분석 실패*\n{message}"
    blocks = [
        section_block(text),
        context_block("필요하면 질문 범위와 기간을 좁혀 같은 thread에서 다시 요청하세요."),
    ]
    return text, blocks


# --- Slack plumbing ---------------------------------------------------------
class SlackTrace:
    """Placeholder message + throttled `chat.update` live progress trace."""

    def __init__(
        self,
        client,
        channel: str,
        thread_ts: str,
        header: str,
        *,
        cancel_action_value: str | None = None,
        max_duration_ms: int | None = None,
    ) -> None:
        self._client = client
        self._channel = channel
        self._thread_ts = thread_ts
        self._header = header
        self._cancel_action_value = cancel_action_value
        self._max_duration_ms = max_duration_ms
        self._started_at_monotonic = time.monotonic()
        self._lines: list[str] = []
        self._ts: str | None = None
        self._last_update = 0.0
        self._last_activity = time.monotonic()
        self._closed = False
        self._heartbeat_stop = threading.Event()
        self._override_blocks: list[dict] | None = None
        self._last_error: str | None = None
        self._lock = threading.Lock()
        self._start()
        self._heartbeat = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat.start()

    @property
    def ts(self) -> str | None:
        return self._ts

    @property
    def last_error(self) -> str | None:
        return self._last_error

    def _start(self) -> None:
        try:
            text = f"{self._header}\n질문을 접수했습니다."
            resp = self._client.chat_postMessage(
                channel=self._channel,
                thread_ts=self._thread_ts,
                text=self._message_text(text),
                blocks=self._blocks(text),
            )
            self._ts = resp.get("ts")
            self._last_error = None
        except Exception:  # noqa: BLE001 - Slack must never kill a turn
            self._last_error = "failed to post placeholder message"
            LOG.exception("failed to post placeholder message")

    def _render(self) -> str:
        if self._closed:
            return self._header[:MAX_SLACK_CHUNK]
        lines = self._lines[-4:]
        if not lines:
            return f"{self._header}\n질문을 정리하고 분석 범위를 잡고 있습니다."[:MAX_SLACK_CHUNK]
        current = lines[-1]
        previous = lines[:-1]
        text = f"{self._header}\n{current}"
        if previous:
            body = "\n".join(f"• {line}" for line in previous)
            text += f"\n\n*진행상황*\n{body}"
        return text[:MAX_SLACK_CHUNK]

    def _elapsed_ms(self) -> int:
        return int((time.monotonic() - self._started_at_monotonic) * 1000)

    def _progress_context(self) -> str:
        return progress_elapsed_context(self._elapsed_ms(), self._max_duration_ms)

    def _message_text(self, text: str) -> str:
        if self._closed or self._override_blocks is not None:
            return text[:MAX_SLACK_CHUNK]
        return f"{text}\n{self._progress_context()}"[:MAX_SLACK_CHUNK]

    def _blocks(self, text: str) -> list[dict] | None:
        if self._override_blocks is not None:
            return self._override_blocks
        section = section_block(text)
        if self._closed:
            return [section]
        blocks = [
            section,
            context_block(self._progress_context()),
        ]
        if self._cancel_action_value:
            blocks.append(
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "중단하기", "emoji": True},
                            "style": "danger",
                            "action_id": CANCEL_ACTION_ID,
                            "value": self._cancel_action_value,
                        }
                    ],
                },
            )
        return blocks

    def add(self, line: str) -> None:
        friendly = slack_progress_line(line)
        if friendly is None:
            return
        with self._lock:
            if self._closed:
                return
            self._last_activity = time.monotonic()
            self._lines = [existing for existing in self._lines if existing != friendly]
            self._lines.append(friendly)
            self._lines = self._lines[-4:]
            now = time.monotonic()
            if now - self._last_update < UPDATE_INTERVAL_S:
                return  # coalesce: the next update carries everything anyway
            self._last_update = now
            self._flush_locked()

    def finish(self, footer: str, *, blocks: list[dict] | None = None) -> bool:
        """Collapse the live trace into a single summary line.

        The per-tool trace earns its keep while the turn is running -- a turn takes
        minutes and silence reads as a crash. Once the answer is posted it is just a
        40-line command log sitting above the answer, so the message is overwritten
        rather than appended to.
        """
        with self._lock:
            self._closed = True
            self._heartbeat_stop.set()
            self._lines = []
            self._header = footer
            self._cancel_action_value = None
            self._override_blocks = blocks
            delivered = self._flush_locked()
        self._heartbeat.join(timeout=1)
        return delivered

    def replace_and_stop_progress(self, text: str, *, blocks: list[dict] | None = None) -> bool:
        """Replace the live progress message and stop progress/heartbeat rewrites.

        Used immediately after a cancel button click so the visible "중단 요청됨"
        state is not overwritten by a late tool trace or heartbeat before the child
        process exits and the final cancelled state is rendered.
        """
        with self._lock:
            self._closed = True
            self._heartbeat_stop.set()
            self._lines = []
            self._header = text
            self._cancel_action_value = None
            self._override_blocks = blocks
            delivered = self._flush_locked()
        self._heartbeat.join(timeout=1)
        return delivered

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.wait(timeout=10):
            with self._lock:
                if self._closed:
                    return
                if time.monotonic() - self._last_activity >= 30:
                    line = "데이터 조회가 길어져 기다리는 중입니다."
                    if not self._lines or self._lines[-1] != line:
                        self._lines.append(line)
                self._last_update = 0
                self._flush_locked()

    def _flush_locked(self) -> bool:
        if not self._ts:
            self._last_error = "Slack progress message ts is missing"
            return False
        text = self._render()
        try:
            self._client.chat_update(
                channel=self._channel,
                ts=self._ts,
                text=self._message_text(text),
                blocks=self._blocks(text),
            )
            self._last_error = None
            return True
        except Exception:  # noqa: BLE001
            self._last_error = "chat_update failed"
            LOG.exception("chat_update failed")
            return False


def feedback_blocks(
    text: str,
    audit_id: str,
    footer: str | None = None,
    *,
    metabase_url: str | None = None,
) -> list[dict]:
    blocks = [
        section_block(text),
    ]
    if footer:
        blocks.append(context_block(footer))
    if metabase_url:
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Metabase 열기", "emoji": True},
                        "action_id": METABASE_OPEN_ACTION_ID,
                        "url": metabase_url,
                    }
                ],
            }
        )
    blocks.append(
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": FEEDBACK_LABELS["helpful"], "emoji": True},
                    "style": "primary",
                    "action_id": f"{FEEDBACK_ACTION_PREFIX}helpful",
                    "value": audit_id,
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": FEEDBACK_LABELS["inaccurate"], "emoji": True},
                    "style": "danger",
                    "action_id": f"{FEEDBACK_ACTION_PREFIX}inaccurate",
                    "value": audit_id,
                },
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": FEEDBACK_LABELS["needs_more_investigation"],
                        "emoji": True,
                    },
                    "action_id": f"{FEEDBACK_ACTION_PREFIX}needs_more_investigation",
                    "value": audit_id,
                },
            ],
        },
    )
    return blocks


def feedback_recorded_blocks(blocks: list[dict], feedback_type: str, user: str | None) -> list[dict]:
    """Return answer blocks with feedback buttons replaced by a visible status."""
    label = FEEDBACK_LABELS.get(feedback_type, feedback_type)
    actor = f" · <@{user}>" if user else ""
    kept: list[dict] = []
    for block in blocks:
        if block.get("type") == "actions":
            elements = block.get("elements") or []
            if any(str(element.get("action_id", "")).startswith(FEEDBACK_ACTION_PREFIX) for element in elements):
                continue
        if block.get("type") == "context":
            rendered = str(block.get("elements", ""))
            if "피드백 기록됨:" in rendered:
                continue
        kept.append(block)
    kept.append(context_block(f"피드백 기록됨: {label}{actor}"))
    return kept


def post_chunks(
    client,
    channel: str,
    thread_ts: str,
    text: str,
    *,
    final_blocks: list[dict] | None = None,
) -> list[str]:
    posted_ts: list[str] = []
    chunks = chunk_text(text)
    for index, chunk in enumerate(chunks):
        kwargs = {}
        if final_blocks is not None and index == len(chunks) - 1:
            kwargs["blocks"] = final_blocks
        try:
            resp = client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=chunk,
                **kwargs,
            )
            if resp and resp.get("ts"):
                posted_ts.append(resp["ts"])
        except Exception:  # noqa: BLE001
            LOG.exception("chat_postMessage failed")
        time.sleep(1.05)  # Slack allows roughly 1 msg/sec per channel
    return posted_ts


MENTION_RE = re.compile(r"<@[UW][A-Z0-9]+>")


def clean_text(text: str, bot_user_id: str | None = None) -> str:
    raw = text or ""
    if bot_user_id:
        raw = raw.replace(f"<@{bot_user_id}>", "")
    else:
        raw = MENTION_RE.sub("", raw)
    return raw.strip()


def build_prompt(user: str, channel: str, thread_ts: str, question: str) -> str:
    header = (
        "[Slack context]\n"
        f"- requester: <@{user}>\n"
        f"- channel: {channel}\n"
        f"- thread_ts: {thread_ts}\n"
        "- This conversation is thread-scoped: earlier turns in this session are the "
        "same Slack thread, and your final message is posted back into it.\n\n"
        "[Question]\n"
    )
    return header + question


def handle_turn(
    cfg: Config,
    registry: SessionRegistry,
    active_turns: ActiveTurnRegistry,
    audit_logger: BotAuditLogger,
    client,
    *,
    team: str,
    channel: str,
    thread_ts: str,
    user: str,
    question: str,
) -> None:
    tag = f"[{channel}/{thread_ts}]"
    with registry.claim(channel, thread_ts):
        audit_id = str(uuid.uuid4())
        run_id = str(uuid.uuid4())
        cancel_event = threading.Event()
        turn_started_at = datetime.now(timezone.utc)
        session_id = session_id_for(team, channel, thread_ts)
        exists = transcript_path(cfg.repo_dir, session_id).exists()
        header = ":bar_chart: *분석 중*"

        slack_trace = SlackTrace(
            client,
            channel,
            thread_ts,
            header,
            cancel_action_value=run_id,
            max_duration_ms=cfg.timeout * 1000,
        )
        active_turn = ActiveTurn(
            cancel_event=cancel_event,
            channel=channel,
            thread_ts=thread_ts,
            pending_ts=slack_trace.ts,
            requester=user,
            started_at=turn_started_at,
            slack_trace=slack_trace,
        )
        active_turns.register(
            run_id,
            active_turn,
        )
        chunks_of_text: list[str] = []

        def on_trace(line: str) -> None:
            LOG.info("%s %s", tag, line)
            slack_trace.add(line)

        def on_text(text: str) -> None:
            chunks_of_text.append(text)

        prompt = build_prompt(user, channel, thread_ts, question)
        result = TurnResult(session_id=session_id)
        try:
            try:
                result = run_turn(
                    cfg,
                    session_id,
                    prompt,
                    resume=exists,
                    cancel_event=cancel_event,
                    on_trace=on_trace,
                    on_text=on_text,
                )
            except Exception as exc:  # noqa: BLE001
                LOG.exception("%s claude turn crashed", tag)
                result.ok = False
                result.error = f"claude turn crashed: {exc}"

            if not exists and result.already_in_use:
                LOG.warning("%s session already in use; falling back to --resume", tag)
                slack_trace.add("세션이 이미 존재하여 --resume 으로 재시도합니다")
                try:
                    result = run_turn(
                        cfg,
                        session_id,
                        prompt,
                        resume=True,
                        cancel_event=cancel_event,
                        on_trace=on_trace,
                        on_text=on_text,
                    )
                except Exception as exc:  # noqa: BLE001
                    LOG.exception("%s claude resume turn crashed", tag)
                    result.ok = False
                    result.error = f"claude resume turn crashed: {exc}"
        finally:
            active_turns.finish(run_id)

        # Two different kinds of "cost" that must not be added together: Claude runs
        # on the operator's subscription (the CLI reports an API-equivalent estimate
        # and consumes a rolling rate-limit window -- no invoice), while BigQuery
        # bytes are actually billed. Summing them would overstate real spend ~30x.
        scan = bq_usage_since(cfg, turn_started_at)
        bq_footer = (
            f"BigQuery {scan.gib:.2f} GiB · ${scan.usd:.2f} · {scan.jobs}건"
            if scan.error is None
            else f"BigQuery 집계 실패 ({truncate_line(scan.error, 90)})"
        )
        audit_footer = (
            f"Claude {result.num_turns or 0}턴 · "
            f"{(result.duration_ms or 0) / 1000:.0f}초 · "
            f"환산 ${(result.total_cost_usd or 0):.2f} (구독, 청구 없음)"
            f"  |  {bq_footer}"
        )
        slack_footer = (
            f"{(result.duration_ms or 0) / 1000:.0f}초 · "
            "상세 실행 내역은 로그에 저장됨"
        )
        if result.bad_json_lines:
            audit_footer += f" · malformed_lines={result.bad_json_lines}"

        trace_summary = {
            "audit_id": audit_id,
            "ok": result.ok,
            "cancelled": result.cancelled,
            "footer": audit_footer,
            "trace_line_count": len(result.trace),
        }

        def record_trace_audit() -> None:
            try:
                audit_logger.record_trace(
                    channel,
                    thread_ts,
                    result.trace,
                    trace_summary,
                )
            except Exception:  # noqa: BLE001 - audit must not hide the answer
                LOG.exception("failed to record trace audit")

        audit_common = {
            "audit_id": audit_id,
            "created_at": turn_started_at.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "duration_ms": int((datetime.now(timezone.utc) - turn_started_at).total_seconds() * 1000),
            "team": team,
            "channel": channel,
            "thread_ts": thread_ts,
            "requester": user,
            "question": question,
            "session_id": session_id,
            "resume": exists,
            "claude_turns": result.num_turns,
            "claude_duration_ms": result.duration_ms,
            "claude_cost_usd_equivalent": result.total_cost_usd,
            "bq_billed_bytes": scan.bytes_billed,
            "bq_gib": scan.gib,
            "bq_usd": scan.usd,
            "bq_jobs": scan.jobs,
            "bq_error": scan.error,
            "permission_denials": result.permission_denials,
            "bad_json_lines": result.bad_json_lines,
        }

        if result.cancelled:
            elapsed_ms = int((datetime.now(timezone.utc) - turn_started_at).total_seconds() * 1000)
            text, blocks = cancelled_payload(
                elapsed_ms,
                active_turn.cancel_requested_by,
                scan,
            )
            slack_trace.finish(text, blocks=blocks)
            try:
                audit_logger.record_turn(
                    **audit_common,
                    status="cancelled",
                    error=result.error or "cancelled",
                )
            except Exception:  # noqa: BLE001
                LOG.exception("failed to record cancelled turn audit")
            record_trace_audit()
            return

        if not result.ok:
            message = user_facing_error(result.error, result.permission_denials)
            text, blocks = error_payload(message)
            slack_trace.finish(text, blocks=blocks)
            try:
                audit_logger.record_turn(
                    **audit_common,
                    status="error",
                    error=result.error or "unknown error",
                )
            except Exception:  # noqa: BLE001
                LOG.exception("failed to record failed turn audit")
            record_trace_audit()
            return

        raw_body = result.final_text.strip() or "\n\n".join(chunks_of_text).strip() or "_(빈 응답)_"
        body = format_final_answer_for_slack(raw_body)
        body = rewrite_metabase_links_for_slack(body, cfg)
        final_blocks = feedback_blocks(
            body,
            audit_id,
            slack_footer,
            metabase_url=extract_metabase_url(body),
        )
        delivered = slack_trace.finish(body, blocks=final_blocks)
        if delivered and slack_trace.ts:
            answer_ts = [slack_trace.ts]
        else:
            LOG.warning(
                "%s final answer chat.update failed; falling back to chat.postMessage (%s)",
                tag,
                slack_trace.last_error or "unknown error",
            )
            answer_ts = post_chunks(
                client,
                channel,
                thread_ts,
                body,
                final_blocks=final_blocks,
            )
        if not answer_ts:
            try:
                audit_logger.record_turn(
                    **audit_common,
                    status="delivery_error",
                    answer=body,
                    error=slack_trace.last_error or "final answer was not delivered to Slack",
                )
            except Exception:  # noqa: BLE001
                LOG.exception("failed to record delivery error audit")
            record_trace_audit()
            return
        try:
            audit_logger.record_turn(
                **audit_common,
                status="success",
                answer=body,
                answer_message_ts=answer_ts[-1] if answer_ts else None,
            )
        except Exception:  # noqa: BLE001
            LOG.exception("failed to record successful turn audit")
        record_trace_audit()


# --- Preflight --------------------------------------------------------------
def audit_database_status(cfg: Config) -> tuple[str, list[str]]:
    if not cfg.audit_database_url:
        return "disabled", []
    blockers: list[str] = []
    try:
        schema = validate_audit_schema(cfg.audit_schema)
    except ValueError as exc:
        return "blocked (invalid schema)", [str(exc)]
    try:
        import psycopg  # noqa: F401
    except ImportError:
        return (
            "blocked (psycopg missing)",
            ["ANALYST_AUDIT_DATABASE_URL 이 설정됐지만 psycopg 를 import 할 수 없습니다."],
        )

    try:
        sink = PostgresAuditSink(cfg.audit_database_url, schema)
        with sink._connect() as conn, conn.cursor() as cur:
            cur.execute("select 1")
            missing: list[str] = []
            for table in AUDIT_DB_TABLES:
                cur.execute("select to_regclass(%s)", (f"{schema}.{table}",))
                if cur.fetchone()[0] is None:
                    missing.append(table)
            if missing:
                blockers.append(
                    "audit DB schema/table 이 없습니다. "
                    f"scripts/analyst_audit_schema.sql 을 audit DB에서 실행하세요. missing={missing}"
                )
                return f"blocked ({schema} missing tables: {', '.join(missing)})", blockers
    except Exception as exc:  # noqa: BLE001
        return (
            "blocked (connection failed)",
            [f"audit DB 연결 확인 실패: {truncate_line(str(exc), 180)}"],
        )
    return f"configured ({describe_database_url(cfg.audit_database_url)}, schema={schema})", []


def preflight(cfg: Config) -> tuple[list[str], dict]:
    """Return (blockers, resolved-config-summary). Never raises."""
    blockers: list[str] = []
    repo = Path(cfg.repo_dir)
    skill = repo / SKILL_RELPATH
    key = gcp_key_path(cfg.repo_dir)
    claude_bin = shutil.which("claude")
    npx_bin = shutil.which("npx")

    if not bot_token():
        blockers.append(f"{BOT_TOKEN_ENV} 이 설정되지 않았습니다 (xoxb- 토큰).")
    if not app_token():
        blockers.append(f"{APP_TOKEN_ENV} 이 설정되지 않았습니다 (xapp- 앱 레벨 토큰).")
    if os.environ.get("SLACK_BOT_TOKEN") and not bot_token():
        blockers.append(
            "SLACK_BOT_TOKEN 이 설정되어 있지만 이 봇은 사용하지 않습니다. "
            f"분석 봇은 이벤트 수신용 설정을 명확히 하기 위해 {BOT_TOKEN_ENV} 와 "
            f"{APP_TOKEN_ENV} 를 별도로 요구합니다."
        )
    if claude_bin is None:
        blockers.append("`claude` 실행 파일을 PATH에서 찾을 수 없습니다.")
    if cfg.max_workers < 1:
        blockers.append("--max-workers 는 1 이상이어야 합니다.")
    if not repo.is_dir():
        blockers.append(f"repo dir 이 존재하지 않습니다: {repo}")
    elif not skill.is_file():
        blockers.append(f"analysis 스킬이 없습니다: {skill}")
    if not (key.is_file() and os.access(key, os.R_OK)):
        blockers.append(f"GCP 키를 읽을 수 없습니다: {key}")

    audit_db_status, audit_db_blockers = audit_database_status(cfg)
    blockers.extend(audit_db_blockers)

    metabase_status = "disabled"
    metabase_config_file = ""
    if cfg.enable_metabase_mcp:
        missing_metabase = []
        if not cfg.metabase_url:
            missing_metabase.append("METABASE_URL")
        if not cfg.metabase_api_key:
            missing_metabase.append("METABASE_API_KEY")
        if npx_bin is None:
            missing_metabase.append("npx")
        if missing_metabase:
            metabase_status = f"blocked ({', '.join(missing_metabase)} missing)"
            blockers.append(
                "Metabase MCP가 활성화됐지만 필요한 설정이 없습니다: "
                + ", ".join(missing_metabase)
            )
        else:
            try:
                mcp_config = ensure_metabase_mcp_config(cfg)
                metabase_config_file = str(mcp_config) if mcp_config else ""
                metabase_status = "configured (smoke not run)"
            except OSError as exc:
                metabase_status = "blocked (config write failed)"
                blockers.append(f"Metabase MCP 설정 파일을 쓸 수 없습니다: {exc}")

    # `bq` ignores GOOGLE_APPLICATION_CREDENTIALS and uses the gcloud credential
    # store, so an unbuilt or stale CLOUDSDK_CONFIG silently falls back to whatever
    # account the operator happens to be logged in as -- roles/owner, in practice.
    # That failure is invisible at runtime, so it has to block startup.
    try:
        rc = ensure_bigqueryrc(cfg)
        scan_ceiling = f"{cfg.max_scan_bytes // 1024**3} GiB via {rc.name}"
    except OSError as exc:
        scan_ceiling = "<write failed>"
        blockers.append(f"{bigqueryrc_path(cfg.repo_dir)} 를 쓸 수 없습니다: {exc}")

    gcloud_cfg = analyst_gcloud_config_path(cfg.repo_dir)
    bq_identity = "<unchecked>"
    if not gcloud_cfg.is_dir():
        blockers.append(
            f"분석 봇 전용 gcloud 설정이 없습니다: {gcloud_cfg}\n"
            f"  CLOUDSDK_CONFIG={gcloud_cfg} gcloud auth activate-service-account "
            f"--key-file={key}"
        )
    else:
        try:
            proc = subprocess.run(
                ["gcloud", "auth", "list", "--format=value(account)"],
                env={**os.environ, "CLOUDSDK_CONFIG": str(gcloud_cfg)},
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )
            accounts = [a for a in proc.stdout.split() if a]
            bq_identity = accounts[0] if accounts else "<none>"
            if len(accounts) != 1 or not bq_identity.startswith("bda-analyst-ro@"):
                blockers.append(
                    f"{gcloud_cfg} 의 활성 계정이 읽기 전용 SA 하나가 아닙니다: {accounts}. "
                    "이 상태로 돌리면 bq 가 운영자 권한으로 실행됩니다."
                )
        except (OSError, subprocess.SubprocessError) as exc:  # pragma: no cover
            blockers.append(f"gcloud 계정 확인 실패: {exc}")

    try:
        audit_schema_summary = validate_audit_schema(cfg.audit_schema)
    except ValueError:
        audit_schema_summary = f"invalid ({cfg.audit_schema})"

    summary = {
        "repo_dir": str(repo),
        "state_dir": str(Path(cfg.state_dir).expanduser()),
        "mode": "headless" if cfg.headless else "attached",
        "channel_allowlist": cfg.channel_allowlist or ["<all>"],
        "max_budget_usd": cfg.max_budget_usd,
        "max_scan_gib": cfg.max_scan_bytes // 1024**3,
        "timeout_s": cfg.timeout,
        "max_workers": cfg.max_workers,
        "bq_usage_footer": (
            "per-turn exact enough for demo (single worker)"
            if cfg.max_workers == 1
            else "may include concurrent analyst jobs; use --max-workers 1 for exact demo footer"
        ),
        "model": cfg.model or "<cli default>",
        "permission_mode": "manual",
        "allowed_tools": len(allowed_tools(cfg)),
        "disallowed_tools": len(DISALLOWED_TOOLS),
        "claude_bin": claude_bin or "<not found>",
        "npx_bin": npx_bin or "<not found>",
        "metabase_mcp": metabase_status,
        "metabase_url": normalized_metabase_url(cfg.metabase_url) or "MISSING",
        "metabase_public_url": metabase_slack_url_base(cfg) or "MISSING",
        "metabase_api_key": "set" if cfg.metabase_api_key else "MISSING",
        "metabase_collection": cfg.metabase_collection_name,
        "metabase_mcp_config": metabase_config_file or "<not written>",
        "audit_database": audit_db_status,
        "audit_schema": audit_schema_summary,
        "project_transcript_dir": str(
            Path.home() / ".claude" / "projects" / encode_project_dir(cfg.repo_dir)
        ),
        "gcp_key": str(key),
        "gcloud_config": str(gcloud_cfg),
        "bq_identity": bq_identity,
        "bq_scan_ceiling": scan_ceiling,
        f"{BOT_TOKEN_ENV.lower()}": "set" if bot_token() else "MISSING",
        f"{APP_TOKEN_ENV.lower()}": "set" if app_token() else "MISSING",
        "alerting_app_token_present": bool(os.environ.get("SLACK_BOT_TOKEN")),
    }
    return blockers, summary


APP_TOKEN_HELP = f"""\
Slack 자격증명이 설정되지 않았습니다.

이 레포는 기존 "Airflow Alerts" 앱(bot_id B0B76RVQA66)을 분석 봇과 공유합니다.
알림용 토큰과 분석 봇용 토큰은 환경변수만 분리합니다.

--- 1. 봇 스코프 추가 (OAuth & Permissions) --------------------------------
  https://api.slack.com/apps → 해당 앱 → OAuth & Permissions
  → Scopes → Bot Token Scopes → Add an OAuth Scope 를 항목마다 반복

  필수: app_mentions:read, channels:history, groups:history,
        im:history, mpim:history, chat:write
  권장: chat:write.public, channels:read, users:read,
        reactions:write, files:write

--- 2. 이벤트 구독 (Event Subscriptions) -----------------------------------
  Enable Events 켜기 → Subscribe to bot events → Add Bot User Event
    app_mention, message.channels, message.groups, message.im, message.mpim
  ※ Socket Mode 가 켜져 있으면 Request URL 은 요구되지 않습니다.

--- 3. Interactivity & Shortcuts --------------------------------------------
  Interactivity 를 켜야 진행 메시지의 `중단하기`와 답변 피드백 버튼이 동작합니다.
  Socket Mode 를 쓰므로 Request URL 은 요구되지 않습니다.

--- 4. Socket Mode + 앱 레벨 토큰 ------------------------------------------
  Socket Mode → Enable Socket Mode 켜기
  Basic Information → App-Level Tokens → Generate Token and Scopes
  → 스코프 `connections:write` → Generate → xapp- 토큰 복사

--- 5. 재설치 --------------------------------------------------------------
  OAuth & Permissions → Reinstall to Workspace (새 스코프 승인)

  주의: 재설치로 xoxb- 토큰이 회전하면 .env 의 SLACK_BOT_TOKEN 이 낡아
  Airflow 알림이 조용히 멎습니다. 재설치 직후 auth.test 로 확인하고,
  바뀌었으면 SLACK_BOT_TOKEN 과 {BOT_TOKEN_ENV} 양쪽에 반영하세요.

--- 6. 환경변수 ------------------------------------------------------------
  .env 에 기록하거나 export:
    {BOT_TOKEN_ENV}=xoxb-...
    {APP_TOKEN_ENV}=xapp-...

--- 7. 채널 초대 -----------------------------------------------------------
  대상 채널에서  /invite @<봇 이름>
  (chat:write.public 이 있으면 공개 채널은 초대 없이도 게시 가능)

일괄 적용 대안: config/slack_analyst_app_manifest.yaml
"""


# --- Self test --------------------------------------------------------------
def self_test(cfg: Config) -> int:
    logging.getLogger().setLevel(logging.INFO)
    cfg.append_system_prompt = (
        "Self-test mode: answer directly and briefly. Do not use any tools or skills."
    )
    team, channel, thread_ts = "T_SELFTEST", "C_SELFTEST", "1700000000.000100"
    session_id = session_id_for(team, channel, thread_ts)
    path = transcript_path(cfg.repo_dir, session_id)

    print(f"[self-test] repo_dir      = {cfg.repo_dir}")
    print(f"[self-test] session_id    = {session_id}")
    print(f"[self-test] transcript    = {path}")
    print(f"[self-test] exists before = {path.exists()}")

    def trace(prefix: str):
        def _t(line: str) -> None:
            print(f"[{prefix}] {line}")

        return _t

    exists = path.exists()
    print("\n=== TURN 1 ===")
    r1 = run_turn(
        cfg,
        session_id,
        "Reply with exactly: SELFTEST OK",
        resume=exists,
        on_trace=trace("turn1"),
    )
    if not exists and r1.already_in_use:
        print("[self-test] session id already in use -> retrying with --resume")
        r1 = run_turn(
            cfg,
            session_id,
            "Reply with exactly: SELFTEST OK",
            resume=True,
            on_trace=trace("turn1r"),
        )
    print(f"[self-test] turn1 ok={r1.ok} session_id={r1.session_id}")
    print(f"[self-test] turn1 result={r1.final_text.strip()!r}")
    if r1.error:
        print(f"[self-test] turn1 error={r1.error}")
    if not r1.ok:
        return 1

    print(f"[self-test] transcript exists after turn1 = {path.exists()}")

    print("\n=== TURN 2 (resume) ===")
    r2 = run_turn(
        cfg,
        session_id,
        "What exact text did you reply with in your previous message? Answer in one line.",
        resume=True,
        on_trace=trace("turn2"),
    )
    print(f"[self-test] turn2 ok={r2.ok} session_id={r2.session_id}")
    print(f"[self-test] turn2 result={r2.final_text.strip()!r}")
    if r2.error:
        print(f"[self-test] turn2 error={r2.error}")
    if not r2.ok:
        return 1

    same = r1.session_id == r2.session_id == session_id
    print(f"\n[self-test] session_id identical across turns: {same}")
    print(f"[self-test] turn1 cost=${(r1.total_cost_usd or 0):.4f} "
          f"turn2 cost=${(r2.total_cost_usd or 0):.4f}")
    print("[self-test] PASS" if same else "[self-test] FAIL")
    return 0 if same else 1


# --- Listener ---------------------------------------------------------------
def start_listener(cfg: Config) -> None:
    try:
        from slack_bolt import App
        from slack_bolt.adapter.socket_mode import SocketModeHandler
    except ImportError as exc:
        raise SystemExit(
            "slack-bolt 를 찾을 수 없습니다. 다음으로 실행하세요:\n"
            "  uv run --with slack-bolt python scripts/slack_analyst_bot.py"
        ) from exc

    blockers, summary = preflight(cfg)
    if not app_token() or not bot_token():
        raise SystemExit(APP_TOKEN_HELP)
    if blockers:
        raise SystemExit("사전 점검 실패:\n" + "\n".join(f"  - {b}" for b in blockers))

    LOG.info("resolved config: %s", json.dumps(summary, ensure_ascii=False))

    app = App(token=bot_token())
    bot_user_id = app.client.auth_test()["user_id"]
    LOG.info("connected as bot_user_id=%s", bot_user_id)

    registry = SessionRegistry(cfg.max_workers)
    active_turns = ActiveTurnRegistry()
    audit_logger = BotAuditLogger(
        cfg.state_dir,
        database_url=cfg.audit_database_url,
        database_schema=cfg.audit_schema,
    )
    previous_signal_handlers: dict[int, object] = {}

    def request_shutdown(signum, _frame) -> None:  # noqa: ANN001
        signame = signal.Signals(signum).name
        turns = active_turns.request_cancel_all("system")
        LOG.warning("received %s; requested cancellation for %d active turn(s)", signame, len(turns))
        raise KeyboardInterrupt

    for signum in (signal.SIGTERM, signal.SIGINT):
        previous_signal_handlers[signum] = signal.getsignal(signum)
        signal.signal(signum, request_shutdown)

    def allowed_channel(channel: str) -> bool:
        return channel.startswith("D") or not cfg.channel_allowlist or channel in cfg.channel_allowlist

    def dispatch(event: dict, client) -> None:
        channel = event.get("channel")
        if not channel or not allowed_channel(channel):
            return
        if event.get("bot_id") or event.get("subtype"):
            return
        user = event.get("user")
        if not user or user == bot_user_id:
            return
        event_key = event.get("client_msg_id") or event.get("event_ts")
        if registry.is_duplicate(event_key):
            LOG.debug("duplicate delivery ignored: %s", event_key)
            return

        thread_ts = event.get("thread_ts") or event.get("ts")
        team = event.get("team") or "unknown"
        question = clean_text(event.get("text", ""), bot_user_id)
        if not question:
            try:
                client.chat_postMessage(
                    channel=channel,
                    thread_ts=thread_ts,
                    text="질문 본문을 멘션 뒤에 함께 적어주세요. 예: `@분석봇 지난 7일 active actor 변화를 봐줘`",
                )
            except Exception:  # noqa: BLE001
                LOG.exception("failed to post empty-question guide")
            return

        # Slack redelivers un-acked events, so the handler returns immediately and
        # the real work happens on a worker thread.
        worker = threading.Thread(
            target=_safe_handle,
            args=(
                cfg,
                registry,
                active_turns,
                audit_logger,
                client,
                team,
                channel,
                thread_ts,
                user,
                question,
            ),
            name=f"turn-{channel}-{thread_ts}",
            daemon=False,
        )
        worker.start()

    @app.action(CANCEL_ACTION_ID)
    def on_cancel(action, body, client, ack=None):  # noqa: ANN001
        if ack is not None:
            ack()
        run_id = action.get("value") if isinstance(action, dict) else None
        user = (body.get("user") or {}).get("id") if isinstance(body, dict) else None
        channel = (body.get("channel") or {}).get("id") if isinstance(body, dict) else None
        if not run_id or not user:
            LOG.warning("cancel action missing run_id/user")
            return
        turn = active_turns.request_cancel(run_id, user)
        if turn is None:
            if channel:
                try:
                    client.chat_postEphemeral(
                        channel=channel,
                        user=user,
                        text="이미 완료됐거나 중단된 작업입니다.",
                    )
                except Exception:  # noqa: BLE001
                    LOG.exception("failed to post stale cancel notice")
            return
        if turn.pending_ts:
            try:
                elapsed_ms = int((datetime.now(timezone.utc) - turn.started_at).total_seconds() * 1000)
                text, blocks = cancellation_requested_payload(elapsed_ms, user)
                if hasattr(turn.slack_trace, "replace_and_stop_progress"):
                    turn.slack_trace.replace_and_stop_progress(text, blocks=blocks)
                else:
                    client.chat_update(
                        channel=turn.channel,
                        ts=turn.pending_ts,
                        text=text,
                        blocks=blocks,
                    )
            except Exception:  # noqa: BLE001
                LOG.exception("failed to update cancellation request message")

    @app.action(re.compile(f"^{FEEDBACK_ACTION_PREFIX}"))
    def on_feedback(action, body, client, ack=None):  # noqa: ANN001
        if ack is not None:
            ack()
        if not isinstance(action, dict) or not isinstance(body, dict):
            return
        action_id = action.get("action_id") or ""
        audit_id = action.get("value") or ""
        feedback_type = action_id.removeprefix(FEEDBACK_ACTION_PREFIX)
        user = (body.get("user") or {}).get("id")
        channel = (body.get("channel") or {}).get("id")
        message = body.get("message") or {}
        thread_ts = message.get("thread_ts") or message.get("ts")
        message_ts = message.get("ts")
        if not audit_id or not feedback_type or not user or not channel or not thread_ts:
            LOG.warning("feedback action missing required fields")
            return
        try:
            audit_logger.record_feedback(
                audit_id=audit_id,
                feedback_type=feedback_type,
                user=user,
                channel=channel,
                thread_ts=thread_ts,
            )
            if message_ts:
                client.chat_update(
                    channel=channel,
                    ts=message_ts,
                    text=message.get("text") or "피드백을 기록했습니다.",
                    blocks=feedback_recorded_blocks(message.get("blocks") or [], feedback_type, user),
                )
            else:
                client.chat_postEphemeral(
                    channel=channel,
                    user=user,
                    text="피드백을 기록했습니다.",
                )
        except Exception:  # noqa: BLE001
            LOG.exception("failed to record feedback")

    @app.action(METABASE_OPEN_ACTION_ID)
    def on_open_metabase(ack=None):  # noqa: ANN001
        if ack is not None:
            ack()

    @app.event("app_mention")
    def on_mention(event, client, ack=None):  # noqa: ANN001
        dispatch(event, client)

    @app.event("message")
    def on_message(event, client):  # noqa: ANN001
        # Registration is required or Bolt logs "Unhandled request" for every message
        # in every subscribed channel.
        #
        # Public/private channel replies still require an @-mention; otherwise once a
        # thread was owned EVERY human message in it would spawn a paid Claude turn.
        # Direct messages are different: the user intentionally talks to the bot, so
        # plain DM text is accepted for the 1-person lecture/demo workflow.
        if event.get("channel_type") == "im":
            dispatch(event, client)
            return
        if LOG.isEnabledFor(logging.DEBUG):
            thread_ts = event.get("thread_ts")
            if thread_ts and registry.known_thread(event.get("channel", ""), thread_ts):
                LOG.debug(
                    "thread reply without a mention ignored (channel=%s thread=%s)",
                    event.get("channel"),
                    thread_ts,
                )

    try:
        LOG.info(
            "starting Socket Mode listener (%s mode)",
            "headless" if cfg.headless else "attached",
        )
        SocketModeHandler(app, app_token()).start()
    except KeyboardInterrupt:
        active_turns.request_cancel_all("system")
        LOG.warning("Socket Mode listener stopped")
    finally:
        for signum, previous in previous_signal_handlers.items():
            signal.signal(signum, previous)


def _safe_handle(  # noqa: ANN001
    cfg,
    registry,
    active_turns,
    audit_logger,
    client,
    team,
    channel,
    thread_ts,
    user,
    question,
) -> None:
    try:
        handle_turn(
            cfg,
            registry,
            active_turns,
            audit_logger,
            client,
            team=team,
            channel=channel,
            thread_ts=thread_ts,
            user=user,
            question=question,
        )
    except Exception:  # noqa: BLE001 - one thread's failure must never kill the daemon
        LOG.exception("unhandled error while handling %s/%s", channel, thread_ts)
        try:
            client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=":rotating_light: *내부 오류로 이 요청을 처리하지 못했습니다.* 로그를 확인하세요.",
            )
        except Exception:  # noqa: BLE001
            LOG.exception("failed to report internal error to Slack")


# --- CLI --------------------------------------------------------------------
def bool_from_env(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Slack Socket Mode bot: 1 thread = 1 Claude Code analysis session.",
    )
    parser.add_argument("--headless", action="store_true", help="TTY 없는 실행 모드 표시")
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="프로세스 로그 레벨. Docker/headless에서도 기본 INFO 로그를 남긴다",
    )
    parser.add_argument("--repo-dir", default=DEFAULT_REPO_DIR, help="세션 cwd (레포 루트)")
    parser.add_argument(
        "--state-dir",
        default=DEFAULT_STATE_DIR,
        help="audit/trace/feedback JSONL 저장 경로 (기본: logs/analyst-bot)",
    )
    parser.add_argument("--channel-allowlist", default="", help="쉼표 구분 채널 ID (빈 값이면 전체)")
    parser.add_argument("--max-budget-usd", type=float, default=5.0, help="턴당 Claude 하드 지출 상한")
    parser.add_argument(
        "--max-scan-gib",
        type=int,
        default=10,
        help=(
            "BigQuery 쿼리당 스캔 상한(GiB). bq는 .bigqueryrc 기본값, dbt/Python은 "
            "BIGQUERY_MAXIMUM_BYTES_BILLED로 적용. 기본 10. 명시적 bq flag나 프로젝트 quota와 "
            "다르므로, 초과가 정당한 분석이면 운영자가 이 값을 올려 재기동하는 것이 승인 절차다"
        ),
    )
    parser.add_argument("--timeout", type=int, default=900, help="턴당 타임아웃(초)")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=1,
        help="동시에 실행할 Slack 분석 턴 수. 기본 1은 BigQuery 사용량 푸터가 섞이지 않게 하기 위한 데모 권장값",
    )
    parser.add_argument("--model", default=None, help="claude --model 패스스루")
    parser.add_argument(
        "--enable-metabase-mcp",
        dest="enable_metabase_mcp",
        action="store_true",
        default=None,
        help="METABASE_URL/METABASE_API_KEY로 Metabase MCP를 Claude 세션에 주입",
    )
    parser.add_argument(
        "--disable-metabase-mcp",
        dest="enable_metabase_mcp",
        action="store_false",
        help="환경변수 ANALYST_ENABLE_METABASE_MCP=1 이 있어도 Metabase MCP를 끔",
    )
    parser.add_argument(
        "--metabase-url",
        default=None,
        help="Metabase MCP 접속용 base URL. 미지정 시 METABASE_URL 환경변수 사용",
    )
    parser.add_argument(
        "--metabase-public-url",
        default=None,
        help="Slack 링크용 Metabase base URL. 미지정 시 METABASE_PUBLIC_URL/ANALYST_METABASE_PUBLIC_URL 또는 --metabase-url 사용",
    )
    parser.add_argument(
        "--metabase-collection-name",
        default=None,
        help="봇이 생성 카드/대시보드를 우선 저장할 Metabase 컬렉션 이름",
    )
    parser.add_argument(
        "--audit-schema",
        default=None,
        help=f"Postgres/RDS audit schema 이름. 기본: {DEFAULT_AUDIT_SCHEMA}",
    )
    parser.add_argument("--dry-run", action="store_true", help="사전 점검 후 설정만 출력하고 종료")
    parser.add_argument("--self-test", action="store_true", help="Slack 없이 세션 생성/재개 검증")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-7s %(threadName)s %(message)s",
    )

    cfg = Config(
        repo_dir=str(Path(args.repo_dir).expanduser()),
        state_dir=str(Path(args.state_dir).expanduser()),
        headless=args.headless,
        channel_allowlist=[c.strip() for c in args.channel_allowlist.split(",") if c.strip()],
        max_budget_usd=args.max_budget_usd,
        max_scan_bytes=args.max_scan_gib * 1024**3,
        timeout=args.timeout,
        max_workers=args.max_workers,
        model=args.model,
        log_level=args.log_level,
        enable_metabase_mcp=(
            args.enable_metabase_mcp
            if args.enable_metabase_mcp is not None
            else bool_from_env("ANALYST_ENABLE_METABASE_MCP", False)
        ),
        metabase_url=args.metabase_url or os.environ.get("METABASE_URL", ""),
        metabase_public_url=(
            args.metabase_public_url
            or os.environ.get("METABASE_PUBLIC_URL", "")
            or os.environ.get("ANALYST_METABASE_PUBLIC_URL", "")
        ),
        metabase_api_key=os.environ.get("METABASE_API_KEY", ""),
        metabase_collection_name=(
            args.metabase_collection_name
            or os.environ.get("ANALYST_METABASE_COLLECTION_NAME", "BDA 데이터 플랫폼")
        ),
        audit_database_url=os.environ.get(AUDIT_DATABASE_URL_ENV, ""),
        audit_schema=args.audit_schema or os.environ.get(AUDIT_SCHEMA_ENV, DEFAULT_AUDIT_SCHEMA),
    )

    if args.dry_run:
        blockers, summary = preflight(cfg)
        print("=== resolved config ===")
        for key, value in summary.items():
            print(f"{key:24} = {value}")
        print(f"\n=== session tool set (--tools) ===\n  {SESSION_TOOLS}")
        if "Read" in SESSION_TOOLS.split(","):
            print("  주의: Read 포함. 세션이 레포 밖 절대경로를 읽을 수 있습니다.")
            print("        디렉터리 단위 deny 는 Read 에 적용되지 않으며, 파일명 글롭만 유효합니다.")
            if str(Path(cfg.repo_dir)) == "/app" or os.environ.get(ANALYST_GCLOUD_CONFIG_ENV):
                print("        Docker 실행에서는 좁은 마운트 목록이 실제 파일 격리 경계입니다.")
            else:
                print("        호스트 실행의 완전 차단은 어렵고 Docker 좁은 마운트가 필요합니다.")
        print("\n=== allowed tools (enforced because permission-mode=manual) ===")
        for tool in allowed_tools(cfg):
            print(f"  + {tool}")
        print("\n=== disallowed tools (hard deny in every mode) ===")
        for tool in DISALLOWED_TOOLS:
            print(f"  - {tool}")
        print("\n=== preflight ===")
        if blockers:
            for blocker in blockers:
                print(f"  BLOCKER: {blocker}")
            if not app_token() or not bot_token():
                print()
                print(APP_TOKEN_HELP)
            print("dry-run: 위 항목을 해결해야 실제 연결이 가능합니다 (dry-run 자체는 정상 종료).")
        else:
            print("  모든 사전 점검 통과.")
        raise SystemExit(0)

    if args.self_test:
        raise SystemExit(self_test(cfg))

    start_listener(cfg)


if __name__ == "__main__":
    main()
