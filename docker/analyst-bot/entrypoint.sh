#!/usr/bin/env bash
# Entrypoint for the containerised Slack analyst bot.
#
# Everything here is preflight: fail fast, in one shot, with messages that name
# the exact fix. The bot's own preflight() also checks some of this, but it only
# runs after slack-bolt import and it reports in Korean prose; container startup
# failures need to be greppable in `docker compose logs`.
#
# Any extra arguments are passed straight through to the bot, e.g.
#   docker compose run --rm analyst-bot --dry-run
#   docker compose run --rm analyst-bot --self-test
set -euo pipefail

REPO_DIR="${ANALYST_REPO_DIR:-/app}"
BOT_SCRIPT="${REPO_DIR}/scripts/slack_analyst_bot.py"
SKILL_FILE="${REPO_DIR}/.claude/skills/analysis/SKILL.md"
KEY_PATH="${GCP_KEY_PATH:-/secrets/analyst-bq-key.json}"
ANALYSES_DIR="${REPO_DIR}/dbt/gharchive_metrics/analyses"
TARGET_DIR="${REPO_DIR}/dbt/gharchive_metrics/target"
STATE_DIR="${ANALYST_STATE_DIR:-/home/analyst/state}"
export ANALYST_GCLOUD_CONFIG_DIR="${ANALYST_GCLOUD_CONFIG_DIR:-${CLOUDSDK_CONFIG:-/home/analyst/.config/gcloud}}"
export CLOUDSDK_CONFIG="${ANALYST_GCLOUD_CONFIG_DIR}"

# Defensive: this service must never carry the Airflow alerting app's token or
# the operator's GitHub tokens. docker-compose.yml enumerates variables instead
# of using env_file for exactly this reason; if someone re-adds `env_file: .env`
# (which holds SLACK_BOT_TOKEN, GITHUB_TOKEN, GH_TOKEN), these unsets still hold.
unset SLACK_BOT_TOKEN GITHUB_TOKEN GH_TOKEN SLACK_ALERT_CHANNEL SLACK_ALERT_MENTION

ERRORS=()
WARNINGS=()
err() { ERRORS+=("$1"); }
warn() { WARNINGS+=("$1"); }

log() { printf '[entrypoint] %s\n' "$1"; }

mask() {
    # Show enough of a token to identify it, never enough to use it.
    local value="${1:-}"
    if [[ -z "${value}" ]]; then printf 'MISSING'; else printf '%s…(%d chars)' "${value:0:9}" "${#value}"; fi
}

# --- 1. Tooling on PATH -----------------------------------------------------
if ! command -v claude >/dev/null 2>&1; then
    err "\`claude\` is not on PATH. The image should provide it via
       npm install -g @anthropic-ai/claude-code -- rebuild the image
       (docker compose build analyst-bot)."
fi
if ! command -v bq >/dev/null 2>&1; then
    err "\`bq\` is not on PATH. The image should provide it via the
       google-cloud-cli package -- rebuild the image."
fi

PY="${ANALYST_PYTHON:-/usr/local/bin/python3}"
if [[ ! -x "${PY}" ]]; then
    PY="$(command -v python3 || true)"
fi
if [[ -z "${PY}" ]]; then
    err "No python3 interpreter found in the image."
elif ! "${PY}" -c 'import slack_bolt' >/dev/null 2>&1; then
    err "slack_bolt is not importable by ${PY}. Rebuild the image
       (the Dockerfile installs it with \`uv pip install --system\`)."
fi

# --- 2. Project mount -------------------------------------------------------
if [[ ! -d "${REPO_DIR}" ]]; then
    err "Project directory ${REPO_DIR} does not exist. Mount the repo at
       ${REPO_DIR} (docker-compose.yml: \`- ./:${REPO_DIR}:ro\`)."
elif [[ ! -f "${BOT_SCRIPT}" ]]; then
    err "${BOT_SCRIPT} not found. ${REPO_DIR} is mounted but does not look like
       the bda-2 repo -- check the bind mount source in docker-compose.yml."
elif [[ ! -f "${SKILL_FILE}" ]]; then
    err "analysis skill missing: ${SKILL_FILE}
       The bot refuses to start without it (every turn invokes the skill).
       Confirm .claude/ is inside the mounted repo and not excluded."
fi

# The analyst is allowed to persist ad-hoc SQL here and nowhere else. This is a
# warning, not a blocker: the bot answers questions fine without it, it just
# cannot save queries. On Linux hosts see docs/analyst_bot_docker.md (uid).
if [[ -d "${ANALYSES_DIR}" && ! -w "${ANALYSES_DIR}" ]]; then
    warn "${ANALYSES_DIR} is not writable by uid $(id -u). Write/Edit into
       dbt/gharchive_metrics/analyses/ will fail. Mount it read-write."
fi
if [[ -d "${TARGET_DIR}" && ! -w "${TARGET_DIR}" ]]; then
    warn "${TARGET_DIR} is not writable by uid $(id -u). \`dbt compile\` will
       fail, which breaks the skill's dry-run step. It should be a named volume."
fi
if ! mkdir -p "${STATE_DIR}" >/dev/null 2>&1 || [[ ! -w "${STATE_DIR}" ]]; then
    err "State directory ${STATE_DIR} is not writable by uid $(id -u).
       Mount a persistent writable volume there; audit/trace/feedback logs use it."
fi
if ! mkdir -p "${ANALYST_GCLOUD_CONFIG_DIR}" >/dev/null 2>&1 || [[ ! -w "${ANALYST_GCLOUD_CONFIG_DIR}" ]]; then
    err "Gcloud config directory ${ANALYST_GCLOUD_CONFIG_DIR} is not writable by uid $(id -u).
       The bot writes .bigqueryrc there and gcloud stores the bq service-account auth there."
fi

# --- 3. BigQuery service-account key ----------------------------------------
if [[ -d "${KEY_PATH}" ]]; then
    err "${KEY_PATH} is a DIRECTORY. docker created it because the host path in
       the bind mount does not exist. Fix on the host:
         mkdir -p secrets && cp <read-only-sa-key>.json secrets/analyst-bq-key.json
         chmod 600 secrets/analyst-bq-key.json
       then: docker compose down analyst-bot && docker compose up -d analyst-bot
       Do NOT point this at ./gcp-key.json -- that symlinks to the operator's
       broad key in ~/Documents."
elif [[ ! -f "${KEY_PATH}" ]]; then
    err "BigQuery key not found at ${KEY_PATH}. Place a service-account key with
       roles/bigquery.dataViewer + roles/bigquery.jobUser (NOT dataEditor) at
       ./secrets/analyst-bq-key.json on the host."
elif [[ ! -r "${KEY_PATH}" ]]; then
    err "BigQuery key ${KEY_PATH} exists but is not readable by uid $(id -u).
       On the host: chmod 644 secrets/analyst-bq-key.json (it is mounted :ro)."
elif ! "${PY}" -c 'import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if d.get("type")=="service_account" else 1)' "${KEY_PATH}" >/dev/null 2>&1; then
    err "${KEY_PATH} is not a JSON service-account key (expected \"type\":
       \"service_account\"). An OAuth client secret or an authorized_user file
       will not work for dbt's keyfile profile."
fi
export GCP_KEY_PATH="${KEY_PATH}"
export GOOGLE_APPLICATION_CREDENTIALS="${KEY_PATH}"

# --- 4. Slack credentials ---------------------------------------------------
# The bot deliberately does not fall back to SLACK_BOT_TOKEN (alerting app).
if [[ -z "${SLACK_ANALYST_BOT_TOKEN:-}" ]]; then
    err "SLACK_ANALYST_BOT_TOKEN is not set (xoxb- token of the analyst bot's OWN
       Slack app -- not the Airflow Alerts app). Run
       \`docker compose run --rm analyst-bot --dry-run\` for the full app setup
       walkthrough printed by the bot."
fi
if [[ -z "${SLACK_ANALYST_APP_TOKEN:-}" ]]; then
    err "SLACK_ANALYST_APP_TOKEN is not set (xapp- app-level token with scope
       connections:write; required for Socket Mode)."
fi

# --- 5. Optional Metabase MCP -----------------------------------------------
if [[ "${ANALYST_ENABLE_METABASE_MCP:-0}" == "1" ]]; then
    if ! command -v npx >/dev/null 2>&1; then
        err "\`npx\` is not on PATH. Metabase MCP runs via
           npx -y @easecloudio/mcp-metabase-server -- rebuild the image."
    fi
    if [[ -z "${METABASE_URL:-}" ]]; then
        err "ANALYST_ENABLE_METABASE_MCP=1 but METABASE_URL is not set.
           In docker compose this should usually be http://metabase:3000."
    fi
    if [[ -z "${METABASE_API_KEY:-}" ]]; then
        err "ANALYST_ENABLE_METABASE_MCP=1 but METABASE_API_KEY is not set.
           Create a Metabase API key and pass it through .env. Do not use
           METABASE_EMAIL/PASSWORD for the MCP server."
    fi
    if [[ -z "${ANALYST_METABASE_PUBLIC_URL:-}" ]]; then
        warn "ANALYST_ENABLE_METABASE_MCP=1 but ANALYST_METABASE_PUBLIC_URL is not set.
       Slack links will fall back to METABASE_URL, which may be a Docker-internal
       URL such as http://metabase:3000."
    fi
fi

# --- 6. Anthropic credentials ----------------------------------------------
# The operator's HOST login does not transfer on its own: subscription OAuth
# lands in the macOS Keychain and ~/.claude.json, neither of which is (or should
# be) mounted here. But the container can hold its own credential -- ~/.claude is
# a named volume, so an in-container login persists and refreshes in place.
CLAUDE_CREDS="${CLAUDE_CONFIG_DIR:-${HOME}/.claude}/.credentials.json"
if [[ -z "${ANTHROPIC_API_KEY:-}" \
   && -z "${ANTHROPIC_AUTH_TOKEN:-}" \
   && -z "${CLAUDE_CODE_OAUTH_TOKEN:-}" \
   && ! -f "${CLAUDE_CREDS}" ]]; then
    err "No Anthropic credential. Pick ONE:

       (1) OAuth inside this container -- RECOMMENDED. The credential stays in
           the analyst-claude volume and refreshes itself; no secret on the host.
           Run once, follow the printed link, paste the code back:
             docker compose run --rm -it --entrypoint claude analyst-bot auth login
           Then: docker compose up -d analyst-bot

       (2) Subscription long-lived token. On the HOST run \`claude setup-token\`,
           then put the result in .env as CLAUDE_CODE_OAUTH_TOKEN=...

       (3) ANTHROPIC_API_KEY=... in .env (Anthropic Console billing).

       A host \`claude auth login\` alone does NOT reach this container.
       See docs/analyst_bot_docker.md."
fi

# --- 7. Report and bail -----------------------------------------------------
if ((${#WARNINGS[@]})); then
    printf '\n'
    for w in "${WARNINGS[@]}"; do printf '[entrypoint] WARNING: %s\n' "${w}"; done
fi
if ((${#ERRORS[@]})); then
    printf '\n[entrypoint] startup blocked by %d problem(s):\n\n' "${#ERRORS[@]}" >&2
    for e in "${ERRORS[@]}"; do printf '  * %s\n\n' "${e}" >&2; done
    exit 1
fi

# --- 8. Activate the service account for the bq CLI ------------------------
# bq does NOT read GOOGLE_APPLICATION_CREDENTIALS; it uses the gcloud credential
# store (CLOUDSDK_CONFIG). Without this every `bq query` in a session fails with
# "There was a problem refreshing your current auth tokens".
if [[ "${ANALYST_SKIP_GCLOUD_AUTH:-0}" != "1" ]]; then
    if ! gcloud auth activate-service-account --key-file="${KEY_PATH}" --quiet >/dev/null 2>&1; then
        printf '\n[entrypoint] FATAL: `gcloud auth activate-service-account` failed for %s.\n' "${KEY_PATH}" >&2
        printf '  Re-run manually for the real error:\n' >&2
        printf '    docker compose run --rm --entrypoint bash analyst-bot -c "gcloud auth activate-service-account --key-file=%s"\n' "${KEY_PATH}" >&2
        printf '  Set ANALYST_SKIP_GCLOUD_AUTH=1 to start anyway (bq will not work).\n' >&2
        exit 1
    fi
    if [[ -n "${DBT_BIGQUERY_PROJECT:-}" ]]; then
        gcloud config set project "${DBT_BIGQUERY_PROJECT}" --quiet >/dev/null 2>&1 || true
    fi
fi

# --- 9. Build the bot command ----------------------------------------------
# --repo-dir must be the canonical container path: `claude --resume` is scoped to
# the project dir derived from cwd, and the bot maps a session's transcript to
# ~/.claude/projects/<cwd with / and . replaced by ->. For /app that is
# ~/.claude/projects/-app, which is why ~/.claude is a named volume.
# Docker can run in headless mode and still keep INFO logs; `--log-level` controls
# logging separately from `--headless`.
ARGS=(
    --repo-dir "${REPO_DIR}"
    --state-dir "${STATE_DIR}"
    --log-level "${ANALYST_LOG_LEVEL:-INFO}"
    --max-budget-usd "${ANALYST_MAX_BUDGET_USD:-5}"
    --max-scan-gib "${ANALYST_MAX_SCAN_GIB:-10}"
    --timeout "${ANALYST_TURN_TIMEOUT_S:-900}"
    --max-workers "${ANALYST_MAX_WORKERS:-1}"
)
if [[ -n "${ANALYST_CHANNEL_ALLOWLIST:-}" ]]; then
    ARGS+=(--channel-allowlist "${ANALYST_CHANNEL_ALLOWLIST}")
fi
if [[ "${ANALYST_HEADLESS:-0}" == "1" ]]; then
    ARGS+=(--headless)
fi
if [[ "${ANALYST_ENABLE_METABASE_MCP:-0}" == "1" ]]; then
    ARGS+=(--enable-metabase-mcp)
fi
if [[ -n "${METABASE_URL:-}" ]]; then
    ARGS+=(--metabase-url "${METABASE_URL}")
fi
if [[ -n "${ANALYST_METABASE_PUBLIC_URL:-}" ]]; then
    ARGS+=(--metabase-public-url "${ANALYST_METABASE_PUBLIC_URL}")
fi
if [[ -n "${ANALYST_METABASE_COLLECTION_NAME:-}" ]]; then
    ARGS+=(--metabase-collection-name "${ANALYST_METABASE_COLLECTION_NAME}")
fi

# User-supplied flags come last so they win in argparse.
ARGS+=("$@")

log "user=$(id -un)($(id -u):$(id -g))  cwd=${REPO_DIR}"
log "claude=$(command -v claude) ($(claude --version 2>/dev/null | head -1))"
log "bq=$(command -v bq)  python=${PY}"
log "bq_key=${KEY_PATH}  gcloud_config=${ANALYST_GCLOUD_CONFIG_DIR}"
log "state_dir=${STATE_DIR}  claude_transcripts=${CLAUDE_CONFIG_DIR:-${HOME}/.claude}/projects"
log "slack_bot_token=$(mask "${SLACK_ANALYST_BOT_TOKEN:-}")  slack_app_token=$(mask "${SLACK_ANALYST_APP_TOKEN:-}")"
if [[ "${ANALYST_ENABLE_METABASE_MCP:-0}" == "1" ]]; then
    if [[ -n "${METABASE_API_KEY:-}" ]]; then
        log "metabase_mcp=enabled  metabase_url=${METABASE_URL:-MISSING}  metabase_public_url=${ANALYST_METABASE_PUBLIC_URL:-MISSING}  metabase_api_key=set"
    else
        log "metabase_mcp=enabled  metabase_url=${METABASE_URL:-MISSING}  metabase_public_url=${ANALYST_METABASE_PUBLIC_URL:-MISSING}  metabase_api_key=MISSING"
    fi
else
    log "metabase_mcp=disabled"
fi
if [[ -f "${CLAUDE_CREDS}" ]]; then
    log "anthropic_auth=in-container OAuth (${CLAUDE_CREDS})"
elif [[ -n "${CLAUDE_CODE_OAUTH_TOKEN:-}" ]]; then
    log "anthropic_auth=CLAUDE_CODE_OAUTH_TOKEN $(mask "${CLAUDE_CODE_OAUTH_TOKEN}")"
elif [[ -n "${ANTHROPIC_API_KEY:-}" ]]; then
    log "anthropic_auth=ANTHROPIC_API_KEY $(mask "${ANTHROPIC_API_KEY}")"
else
    log "anthropic_auth=ANTHROPIC_AUTH_TOKEN (base_url=${ANTHROPIC_BASE_URL:-default})"
fi
log "exec: ${PY} ${BOT_SCRIPT} ${ARGS[*]}"

exec "${PY}" "${BOT_SCRIPT}" "${ARGS[@]}"
