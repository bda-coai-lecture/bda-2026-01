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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

# This is a long-running daemon (Socket Mode connection stays open for days), so we
# use `logging` instead of bare print(): timestamps, levels and thread names matter
# when reconstructing what a session did hours after the fact.
LOG = logging.getLogger("slack_analyst_bot")

DEFAULT_REPO_DIR = "/Users/kakao/bda-2"
SKILL_RELPATH = ".claude/skills/analysis/SKILL.md"
# The analyst's own read-only key. NOT `gcp-key.json`, which carries roles/owner --
# that one belongs to the operator and Airflow and must never reach the session.
DEFAULT_GCP_KEY = "secrets/analyst-bq-key.json"
# Dedicated gcloud config dir holding ONLY the analyst service account. `bq` reads
# the gcloud credential store rather than GOOGLE_APPLICATION_CREDENTIALS, so this is
# what bounds `bq`. Rebuild with:
#   CLOUDSDK_CONFIG=secrets/gcloud-analyst gcloud auth activate-service-account \
#     --key-file=secrets/analyst-bq-key.json
ANALYST_GCLOUD_CONFIG = "secrets/gcloud-analyst"
# Operator key. Used ONLY by the bot process for privileged accounting
# (INFORMATION_SCHEMA.JOBS), never injected into the session. See bq_usage_since.
DEFAULT_OPERATOR_KEY = "gcp-key.json"
BQ_PROJECT = "bda-coai"
ANALYST_SA_PREFIX = "bda-analyst-ro@"

# --- Slack credentials ------------------------------------------------------
# The analyst bot uses its OWN Slack app, deliberately separate from the existing
# "Airflow Alerts" app that dags/utils/slack_alert.py posts through.
#
# Why separate: the alerting app only needs `chat:write` (outbound). This bot must
# RECEIVE events, which needs extra bot scopes plus an app-level token, and adding
# those to the alerting app requires a reinstall that can rotate its xoxb token --
# silently breaking Airflow alerts, since slack_alert._post() swallows failures.
#
# We intentionally do NOT fall back to SLACK_BOT_TOKEN. Doing so would connect with
# the alerting app's identity, which lacks app_mentions:read/channels:history, and
# fail in a confusing way at event-delivery time instead of at startup.
BOT_TOKEN_ENV = "SLACK_ANALYST_BOT_TOKEN"
APP_TOKEN_ENV = "SLACK_ANALYST_APP_TOKEN"


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
#      Fix: drop Read/Grep/Glob from the tool set entirely via `--tools` (see
#      SESSION_TOOLS). All file access then goes through Bash, which IS confined to
#      the cwd by default -- verified: `cat /etc/hosts` is denied while
#      `cat dbt/gharchive_metrics/dbt_project.yml` succeeds.
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
#     docs/analyst_bot_docker.md, currently unused) is what actually closes this.
SESSION_TOOLS = "Bash,Read,Grep,Glob,Write,Edit,Skill,TodoWrite"

ALLOWED_TOOLS = [
    # BigQuery. NOTE: `bq query` is NOT a read-only surface -- it executes DDL/DML
    # (CREATE OR REPLACE, DROP, DELETE, MERGE) and command-string matching cannot
    # constrain SQL. Read-only MUST be enforced at IAM: the service account should
    # hold roles/bigquery.dataViewer + jobUser and NOT dataEditor. Scan cost is
    # likewise bounded by BIGQUERY_MAXIMUM_BYTES_BILLED (see child_env), not here.
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
    "Read(**/*.zsh_history)",
    "Read(**/*.bash_history)",
    # Same names via the Bash file-reading commands. Bash is cwd-confined already,
    # but these hold for anything reachable inside the repo.
    "Bash(cat /Users/kakao/Documents:*)",
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
   and follow its report format.
2. State the mode classification and fill in all 9 required inputs from that skill. If an
   input is missing from the user's message, state the assumption you made instead of
   silently guessing.
3. Always include the health-check table before interpreting any number.
4. NEVER run an unbounded scan over `githubarchive.day.20*` (or any wildcard that expands
   to the full history). Always pin an explicit date range / _TABLE_SUFFIX bound, and
   dry-run before a real query.
5. Do not make causal claims ("X caused Y", "because of X") without a control group or an
   explicit counterfactual. Otherwise say "correlated with" and name the confounders.
6. Your FINAL message is posted verbatim into Slack. There is no human reading it
   first and no wrapper around it, so it must BE the answer, not an introduction to
   the answer. It must:
   - start with the conclusion itself. Do NOT open with a status line, a summary of
     what you did, or a hand-off sentence ("Analysis complete.", "Here is the answer
     that will be posted to Slack", "I left two SQL files"). Such a line is written
     to yourself and reads as leaked scaffolding to the person who asked.
   - contain no horizontal rules (`---`) and no preamble above the conclusion,
   - stay under ~2,500 characters. If the inputs and health checks do not fit,
     compress them to one line each rather than dropping the conclusion's support,
   - use Slack mrkdwn (*bold*, `code`, • bullets) - never HTML or ###-headings,
   - reference SQL by file path (e.g. `dbt/gharchive_metrics/analyses/foo.sql`) instead of
     pasting full query text,
   - end with a one-line caveat about what the number does NOT show.
"""

MAX_SLACK_CHUNK = 2800
TRACE_MAX_LINES = 40
UPDATE_INTERVAL_S = 1.5


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


def bigqueryrc_path(repo_dir: str) -> Path:
    return Path(repo_dir) / ANALYST_GCLOUD_CONFIG / ".bigqueryrc"


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
    # Pointing CLOUDSDK_CONFIG at a dedicated config dir where only the analyst
    # service account is activated is what actually enforces it. Forced, not
    # setdefault: this is the security boundary, so the parent env must not win.
    env["CLOUDSDK_CONFIG"] = str(Path(cfg.repo_dir) / ANALYST_GCLOUD_CONFIG)
    # Scan ceiling, part 2. BIGQUERY_MAXIMUM_BYTES_BILLED above is read by the Python
    # client (so it binds dbt) but `bq` IGNORES it -- measured 2026-07-26: an 18.4 GB
    # raw sweep ran to completion under a 10 GiB env ceiling. `bq` honours the flag
    # `--maximum_bytes_billed`, and a [query] entry in the rc file supplies that flag
    # without the session having to pass it. Not airtight: an explicit flag on the
    # command line still overrides the rc default. See ensure_bigqueryrc.
    env["BIGQUERYRC"] = str(bigqueryrc_path(cfg.repo_dir))
    # Hard scan ceiling. `--max-budget-usd` caps Claude API spend only; nothing in the
    # tool allowlist can constrain a SQL scan, so BigQuery cost is bounded here. `bq`
    # reads this and fails the job outright when the estimate exceeds it, which is a
    # real guardrail rather than the prose rule in the skill.
    env.setdefault("BIGQUERY_MAXIMUM_BYTES_BILLED", str(cfg.max_scan_bytes))
    # Never leak the alerting app's Slack token into the analysis session.
    env.pop("SLACK_BOT_TOKEN", None)
    env.pop(BOT_TOKEN_ENV, None)
    env.pop(APP_TOKEN_ENV, None)
    return env


# --- Turn execution ---------------------------------------------------------
@dataclass
class Config:
    repo_dir: str = DEFAULT_REPO_DIR
    headless: bool = False
    channel_allowlist: list[str] = field(default_factory=list)
    max_budget_usd: float = 5.0
    # BigQuery on-demand is ~$6.25/TiB, so 200 GiB is roughly $1.25 per query worst case.
    # 10 GiB. BigQuery enforces this per job via BIGQUERY_MAXIMUM_BYTES_BILLED, so a
    # query over the line fails outright rather than being talked past. Sized from
    # measurement: a 41-day raw shard sweep billed 4.39 GiB, so ordinary mart work and
    # bounded raw checks fit, while a broad raw sweep does not. Raising it is an
    # operator decision -- restart with `--max-scan-gib N`. Was 200 GiB, which let a
    # full raw sweep through on 2% of budget and was not a defence line in any sense.
    max_scan_bytes: int = 10 * 1024**3
    timeout: int = 900
    model: str | None = None
    # Overridable only so --self-test can exercise the spawn/stream path without
    # dragging the whole analysis skill into a trivial prompt.
    append_system_prompt: str = APPEND_SYSTEM_PROMPT


@dataclass
class TurnResult:
    ok: bool = False
    session_id: str | None = None
    final_text: str = ""
    error: str | None = None
    num_turns: int | None = None
    duration_ms: int | None = None
    total_cost_usd: float | None = None
    permission_denials: list[dict] = field(default_factory=list)
    bad_json_lines: int = 0
    trace: list[str] = field(default_factory=list)
    already_in_use: bool = False


def build_command(cfg: Config, session_id: str, resume: bool) -> list[str]:
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
        # Removes Read/Grep/Glob from the session. Required: the Read tool cannot be
        # confined to the repo by any permission rule (see hole 2 above).
        "--tools",
        SESSION_TOOLS,
        "--append-system-prompt",
        cfg.append_system_prompt,
    ]
    if cfg.model:
        cmd += ["--model", cfg.model]
    cmd += ["--resume" if resume else "--session-id", session_id]
    # These flags are variadic and would swallow a positional prompt, which is
    # exactly why the prompt goes in over stdin instead. Keep them LAST.
    cmd += ["--allowedTools", *ALLOWED_TOOLS]
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
    on_trace=None,
    on_text=None,
) -> TurnResult:
    """Spawn one `claude -p` turn and parse its stream-json output.

    `on_trace(line)` is called for each compact progress line; `on_text(text)` for
    each assistant text block. Neither is allowed to raise fatally - callbacks are
    wrapped so a Slack outage cannot kill the turn.
    """
    result = TurnResult(session_id=session_id)
    cmd = build_command(cfg, session_id, resume)

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

    def on_timeout() -> None:
        timer_fired.set()
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass

    timer = threading.Timer(cfg.timeout, on_timeout)
    timer.daemon = True
    timer.start()

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
        stderr_thread.join(timeout=2)
        try:
            if proc.poll() is None:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass

    stderr_text = "".join(stderr_buf)
    if "already in use" in stderr_text:
        result.already_in_use = True

    if timer_fired.is_set():
        result.ok = False
        result.error = f"타임아웃({cfg.timeout}s) 초과로 세션 프로세스를 종료했습니다."
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

    Runs in the BOT process with the operator key, never in the session: reading
    INFORMATION_SCHEMA needs bigquery.jobs.listAll, and granting that to the
    read-only account would widen exactly the surface we spent the day narrowing.
    Accounting is a privileged operation and belongs on the privileged side.

    Never raises -- a turn must still report its answer if billing lookup fails.
    """
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account

        key = Path(cfg.repo_dir) / DEFAULT_OPERATOR_KEY
        creds = service_account.Credentials.from_service_account_file(
            str(key), scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        client = bigquery.Client(credentials=creds, project=BQ_PROJECT)
        job = client.query(
            """
            select
              count(*) as jobs,
              ifnull(sum(total_bytes_billed), 0) as bytes_billed
            from `region-us`.INFORMATION_SCHEMA.JOBS
            where creation_time >= @since
              and job_type = 'QUERY'
              and user_email like @account
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("since", "TIMESTAMP", since),
                    bigquery.ScalarQueryParameter(
                        "account", "STRING", f"{ANALYST_SA_PREFIX}%"
                    ),
                ]
            ),
        )
        row = next(iter(job.result()), None)
        if row is None:
            return ScanUsage()
        gib = (row.bytes_billed or 0) / 1024**3
        return ScanUsage(
            gib=gib,
            usd=(row.bytes_billed or 0) / 1024**4 * BQ_USD_PER_TIB,
            jobs=row.jobs or 0,
        )
    except Exception as exc:  # noqa: BLE001 - accounting must never kill a turn
        LOG.warning("BigQuery usage lookup failed: %s", exc)
        return ScanUsage(error=str(exc))


class SessionRegistry:
    """Per-(channel, thread_ts) locks. Two concurrent `--resume` on one session id
    do NOT error - they silently branch the transcript and lose a turn - so
    serialization per thread is mandatory."""

    def __init__(self) -> None:
        self._registry_lock = threading.Lock()
        self._locks: dict[tuple[str, str], threading.Lock] = {}
        self._seen_events: set[str] = set()
        self._seen_order: list[str] = []

    def lock_for(self, channel: str, thread_ts: str) -> threading.Lock:
        key = (channel, thread_ts)
        with self._registry_lock:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
            return lock

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
            return (channel, thread_ts) in self._locks


# --- Slack plumbing ---------------------------------------------------------
class SlackTrace:
    """Placeholder message + throttled `chat.update` live progress trace."""

    def __init__(self, client, channel: str, thread_ts: str, header: str) -> None:
        self._client = client
        self._channel = channel
        self._thread_ts = thread_ts
        self._header = header
        self._lines: list[str] = []
        self._ts: str | None = None
        self._last_update = 0.0
        self._lock = threading.Lock()
        self._start()

    def _start(self) -> None:
        try:
            resp = self._client.chat_postMessage(
                channel=self._channel,
                thread_ts=self._thread_ts,
                text=f"{self._header}\n_분석을 시작합니다…_",
            )
            self._ts = resp.get("ts")
        except Exception:  # noqa: BLE001 - Slack must never kill a turn
            LOG.exception("failed to post placeholder message")

    def _render(self) -> str:
        lines = self._lines[-TRACE_MAX_LINES:]
        if not lines:
            # After finish() the header IS the message; before the first tool call
            # there is nothing to show yet.
            return self._header[:MAX_SLACK_CHUNK]
        body = "\n".join(f"• {line}" for line in lines)
        text = f"{self._header}\n{body}"
        return text[:MAX_SLACK_CHUNK]

    def add(self, line: str) -> None:
        with self._lock:
            self._lines.append(line)
            now = time.monotonic()
            if now - self._last_update < UPDATE_INTERVAL_S:
                return  # coalesce: the next update carries everything anyway
            self._last_update = now
            self._flush_locked()

    def finish(self, footer: str) -> None:
        """Collapse the live trace into a single summary line.

        The per-tool trace earns its keep while the turn is running -- a turn takes
        minutes and silence reads as a crash. Once the answer is posted it is just a
        40-line command log sitting above the answer, so the message is overwritten
        rather than appended to.
        """
        with self._lock:
            self._lines = []
            self._header = footer
            self._flush_locked()

    def _flush_locked(self) -> None:
        if not self._ts:
            return
        try:
            self._client.chat_update(
                channel=self._channel, ts=self._ts, text=self._render()
            )
        except Exception:  # noqa: BLE001
            LOG.exception("chat_update failed")


def post_chunks(client, channel: str, thread_ts: str, text: str) -> None:
    for chunk in chunk_text(text):
        try:
            client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=chunk)
        except Exception:  # noqa: BLE001
            LOG.exception("chat_postMessage failed")
        time.sleep(1.05)  # Slack allows roughly 1 msg/sec per channel


MENTION_RE = re.compile(r"<@[UW][A-Z0-9]+>")


def clean_text(text: str) -> str:
    return MENTION_RE.sub("", text or "").strip()


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
    client,
    *,
    team: str,
    channel: str,
    thread_ts: str,
    user: str,
    question: str,
) -> None:
    lock = registry.lock_for(channel, thread_ts)
    tag = f"[{channel}/{thread_ts}]"
    with lock:
        turn_started_at = datetime.now(timezone.utc)
        session_id = session_id_for(team, channel, thread_ts)
        exists = transcript_path(cfg.repo_dir, session_id).exists()
        header = f":bar_chart: *분석 진행 중* — `{session_id[:8]}`"

        slack_trace = SlackTrace(client, channel, thread_ts, header)
        chunks_of_text: list[str] = []

        def on_trace(line: str) -> None:
            if not cfg.headless:
                LOG.info("%s %s", tag, line)
            slack_trace.add(line)

        def on_text(text: str) -> None:
            chunks_of_text.append(text)

        prompt = build_prompt(user, channel, thread_ts, question)
        result = run_turn(
            cfg,
            session_id,
            prompt,
            resume=exists,
            on_trace=on_trace,
            on_text=on_text,
        )

        if not exists and result.already_in_use:
            LOG.warning("%s session already in use; falling back to --resume", tag)
            slack_trace.add("세션이 이미 존재하여 --resume 으로 재시도합니다")
            result = run_turn(
                cfg,
                session_id,
                prompt,
                resume=True,
                on_trace=on_trace,
                on_text=on_text,
            )

        # Two different kinds of "cost" that must not be added together: Claude runs
        # on the operator's subscription (the CLI reports an API-equivalent estimate
        # and consumes a rolling rate-limit window -- no invoice), while BigQuery
        # bytes are actually billed. Summing them would overstate real spend ~30x.
        scan = bq_usage_since(cfg, turn_started_at)
        footer = (
            f"Claude {result.num_turns or 0}턴 · "
            f"{(result.duration_ms or 0) / 1000:.0f}초 · "
            f"환산 ${(result.total_cost_usd or 0):.2f} (구독, 청구 없음)"
            f"  |  BigQuery {scan.gib:.2f} GiB · ${scan.usd:.2f} (실지출, {scan.jobs}건)"
        )
        if result.bad_json_lines:
            footer += f" · malformed_lines={result.bad_json_lines}"
        slack_trace.finish(footer)

        if not result.ok:
            post_chunks(
                client,
                channel,
                thread_ts,
                f":rotating_light: *분석 실패*\n```{(result.error or '알 수 없는 오류')[:2000]}```",
            )
            return

        body = result.final_text.strip() or "_(빈 응답)_"
        post_chunks(client, channel, thread_ts, body)

        if result.permission_denials:
            lines = [":lock: *권한 차단된 도구 호출*"]
            for denial in result.permission_denials[:8]:
                detail = json.dumps(denial.get("tool_input") or {}, ensure_ascii=False)
                lines.append(f"• `{denial.get('tool_name', '?')}` — {truncate_line(detail, 120)}")
            post_chunks(client, channel, thread_ts, "\n".join(lines))

        post_chunks(client, channel, thread_ts, f":receipt: `{footer}`")


# --- Preflight --------------------------------------------------------------
def preflight(cfg: Config) -> tuple[list[str], dict]:
    """Return (blockers, resolved-config-summary). Never raises."""
    blockers: list[str] = []
    repo = Path(cfg.repo_dir)
    skill = repo / SKILL_RELPATH
    key = gcp_key_path(cfg.repo_dir)
    claude_bin = shutil.which("claude")

    if not bot_token():
        blockers.append(f"{BOT_TOKEN_ENV} 이 설정되지 않았습니다 (xoxb- 토큰).")
    if not app_token():
        blockers.append(f"{APP_TOKEN_ENV} 이 설정되지 않았습니다 (xapp- 앱 레벨 토큰).")
    if os.environ.get("SLACK_BOT_TOKEN") and not bot_token():
        blockers.append(
            "SLACK_BOT_TOKEN 이 설정되어 있지만 이 봇은 사용하지 않습니다. "
            "그 토큰은 Airflow 알림 전용 앱(chat:write 만 보유)이라 이벤트를 받을 수 없습니다. "
            f"분석 봇 전용 앱을 따로 만들어 {BOT_TOKEN_ENV} 로 설정하세요."
        )
    if claude_bin is None:
        blockers.append("`claude` 실행 파일을 PATH에서 찾을 수 없습니다.")
    if not repo.is_dir():
        blockers.append(f"repo dir 이 존재하지 않습니다: {repo}")
    elif not skill.is_file():
        blockers.append(f"analysis 스킬이 없습니다: {skill}")
    if not (key.is_file() and os.access(key, os.R_OK)):
        blockers.append(f"GCP 키를 읽을 수 없습니다: {key}")

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

    gcloud_cfg = repo / ANALYST_GCLOUD_CONFIG
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

    summary = {
        "repo_dir": str(repo),
        "mode": "headless" if cfg.headless else "attached",
        "channel_allowlist": cfg.channel_allowlist or ["<all>"],
        "max_budget_usd": cfg.max_budget_usd,
        "max_scan_gib": cfg.max_scan_bytes // 1024**3,
        "timeout_s": cfg.timeout,
        "model": cfg.model or "<cli default>",
        "permission_mode": "manual",
        "allowed_tools": len(ALLOWED_TOOLS),
        "disallowed_tools": len(DISALLOWED_TOOLS),
        "claude_bin": claude_bin or "<not found>",
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

--- 3. Socket Mode + 앱 레벨 토큰 ------------------------------------------
  Socket Mode → Enable Socket Mode 켜기
  Basic Information → App-Level Tokens → Generate Token and Scopes
  → 스코프 `connections:write` → Generate → xapp- 토큰 복사

--- 4. 재설치 --------------------------------------------------------------
  OAuth & Permissions → Reinstall to Workspace (새 스코프 승인)

  주의: 재설치로 xoxb- 토큰이 회전하면 .env 의 SLACK_BOT_TOKEN 이 낡아
  Airflow 알림이 조용히 멎습니다. 재설치 직후 auth.test 로 확인하고,
  바뀌었으면 SLACK_BOT_TOKEN 과 {BOT_TOKEN_ENV} 양쪽에 반영하세요.

--- 5. 환경변수 ------------------------------------------------------------
  .env 에 기록하거나 export:
    {BOT_TOKEN_ENV}=xoxb-...
    {APP_TOKEN_ENV}=xapp-...

--- 6. 채널 초대 -----------------------------------------------------------
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

    registry = SessionRegistry()

    def allowed_channel(channel: str) -> bool:
        return not cfg.channel_allowlist or channel in cfg.channel_allowlist

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
        question = clean_text(event.get("text", ""))
        if not question:
            return

        # Slack redelivers un-acked events, so the handler returns immediately and
        # the real work happens on a worker thread.
        worker = threading.Thread(
            target=_safe_handle,
            args=(cfg, registry, client, team, channel, thread_ts, user, question),
            name=f"turn-{channel}-{thread_ts}",
            daemon=True,
        )
        worker.start()

    @app.event("app_mention")
    def on_mention(event, client, ack=None):  # noqa: ANN001
        dispatch(event, client)

    @app.event("message")
    def on_message(event, client):  # noqa: ANN001
        # Registration is required or Bolt logs "Unhandled request" for every message
        # in every subscribed channel.
        #
        # Deliberately a no-op for turn dispatch: a follow-up must @-mention the bot,
        # which arrives as an `app_mention` event and is handled above. Without that
        # rule, once a thread was owned EVERY human message in it -- including two
        # people talking to each other -- would spawn a paid Claude turn. Multi-turn
        # still works: `app_mention` fires for mentions inside threads too, and the
        # session id is derived from thread_ts, so context carries across turns.
        if LOG.isEnabledFor(logging.DEBUG):
            thread_ts = event.get("thread_ts")
            if thread_ts and registry.known_thread(event.get("channel", ""), thread_ts):
                LOG.debug(
                    "thread reply without a mention ignored (channel=%s thread=%s)",
                    event.get("channel"),
                    thread_ts,
                )

    LOG.info(
        "starting Socket Mode listener (%s mode)",
        "headless" if cfg.headless else "attached",
    )
    SocketModeHandler(app, app_token()).start()


def _safe_handle(cfg, registry, client, team, channel, thread_ts, user, question) -> None:  # noqa: ANN001
    try:
        handle_turn(
            cfg,
            registry,
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
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Slack Socket Mode bot: 1 thread = 1 Claude Code analysis session.",
    )
    parser.add_argument("--headless", action="store_true", help="터미널 실시간 출력 끄기")
    parser.add_argument("--repo-dir", default=DEFAULT_REPO_DIR, help="세션 cwd (레포 루트)")
    parser.add_argument("--channel-allowlist", default="", help="쉼표 구분 채널 ID (빈 값이면 전체)")
    parser.add_argument("--max-budget-usd", type=float, default=5.0, help="턴당 Claude 하드 지출 상한")
    parser.add_argument(
        "--max-scan-gib",
        type=int,
        default=10,
        help=(
            "BigQuery 쿼리당 스캔 상한(GiB). BIGQUERY_MAXIMUM_BYTES_BILLED 로 강제. "
            "기본 10. 세션은 스스로 못 올리므로, 초과가 정당한 분석이면 "
            "운영자가 이 값을 올려 재기동하는 것이 승인 절차다"
        ),
    )
    parser.add_argument("--timeout", type=int, default=900, help="턴당 타임아웃(초)")
    parser.add_argument("--model", default=None, help="claude --model 패스스루")
    parser.add_argument("--dry-run", action="store_true", help="사전 점검 후 설정만 출력하고 종료")
    parser.add_argument("--self-test", action="store_true", help="Slack 없이 세션 생성/재개 검증")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.WARNING if args.headless else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(threadName)s %(message)s",
    )

    cfg = Config(
        repo_dir=str(Path(args.repo_dir).expanduser()),
        headless=args.headless,
        channel_allowlist=[c.strip() for c in args.channel_allowlist.split(",") if c.strip()],
        max_budget_usd=args.max_budget_usd,
        max_scan_bytes=args.max_scan_gib * 1024**3,
        timeout=args.timeout,
        model=args.model,
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
            print("        완전 차단은 컨테이너 실행이 필요합니다 (docs/analyst_bot_docker.md).")
        print("\n=== allowed tools (enforced because permission-mode=manual) ===")
        for tool in ALLOWED_TOOLS:
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
