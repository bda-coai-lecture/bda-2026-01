# Slack analyst bot (scripts/slack_analyst_bot.py) packaged as a container.
#
# WHY THIS IMAGE EXISTS
# ---------------------
# The bot spawns `claude -p` sessions on behalf of anyone in the Slack workspace.
# Live probing (documented in scripts/slack_analyst_bot.py, "hole 2") showed the
# Read tool can open ANY absolute path and that no --disallowedTools glob stops
# it. On the host that means ~/.ssh, ~/.aws, ~/.claude.json and
# ~/Documents/gcp-key.json are reachable. The container -- not the permission
# list -- is the containment boundary: the only host paths inside it are the
# project and ONE narrowly-scoped BigQuery service-account key.
#
# Therefore: mount nothing else. See docs/analyst_bot_docker.md.

# Base gives Python 3.13 (pyproject requires >=3.13,<3.14) + uv, same family as
# docker/api/Dockerfile.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

# Pinned the way docker/airflow/Dockerfile pins its base image.
ARG NODE_VERSION=22.11.0
# Deliberately NOT pinned by default: the bot passes `--tools`,
# `--max-budget-usd` and `--permission-mode manual`, which only exist in recent
# Claude Code releases. Build once, read `claude --version` from the build log,
# then pin here (e.g. --build-arg CLAUDE_CODE_VERSION=2.0.14) so the image is
# reproducible.
ARG CLAUDE_CODE_VERSION=latest
ARG ANALYST_UID=10001
ARG ANALYST_GID=10001

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    DEBIAN_FRONTEND=noninteractive

# --- OS packages ------------------------------------------------------------
# git + ripgrep: the session's Bash allowlist uses `git status/diff/log` and `rg`.
# python3 (Debian 3.11): the gcloud/bq CLI is not validated against 3.13, so it
#   gets its own interpreter via CLOUDSDK_PYTHON while the bot runs on 3.13.
# xz-utils: needed to unpack the Node tarball below.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gnupg \
        git \
        ripgrep \
        xz-utils \
        python3 \
    && rm -rf /var/lib/apt/lists/*

# --- bq / gcloud (slim: base CLI only, no extra components) -----------------
RUN set -eux; \
    curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg; \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
        > /etc/apt/sources.list.d/google-cloud-sdk.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends google-cloud-cli; \
    rm -rf /var/lib/apt/lists/*; \
    CLOUDSDK_PYTHON=/usr/bin/python3 bq version; \
    CLOUDSDK_PYTHON=/usr/bin/python3 gcloud --version

# --- Node.js (for the Claude Code CLI) --------------------------------------
# Arch detected at build time so this also builds on Apple Silicon without
# relying on BuildKit-only TARGETARCH.
RUN set -eux; \
    dpkg_arch="$(dpkg --print-architecture)"; \
    case "${dpkg_arch}" in \
        amd64) node_arch=x64 ;; \
        arm64) node_arch=arm64 ;; \
        *) echo "unsupported architecture: ${dpkg_arch}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-${node_arch}.tar.xz" \
        -o /tmp/node.tar.xz; \
    tar -xJf /tmp/node.tar.xz -C /usr/local --strip-components=1 --no-same-owner; \
    rm /tmp/node.tar.xz; \
    node --version; \
    npm --version

# --- Claude Code CLI --------------------------------------------------------
RUN npm install -g --no-fund --no-audit "@anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}" \
    && npm cache clean --force \
    && claude --version

# --- Python deps for the bot process itself ---------------------------------
# Only slack-bolt (pulls slack-sdk). The analysis session gets dbt through
# `uv run --no-project --with dbt-bigquery dbt ...`, which is what the skill and
# the bot's tool allowlist expect -- a globally installed dbt would not be
# reachable through that allowlist.
RUN uv pip install --system --no-cache \
    "slack-bolt>=1.21,<2" \
    "slack-sdk>=3.33,<4" \
    "google-cloud-bigquery>=3.25,<4"

# --- Unprivileged user ------------------------------------------------------
# Owns exactly the writable paths it needs: its home (uv cache, gcloud config,
# dbt logs, ~/.claude session transcripts). /app stays root-owned and is
# expected to be a read-only bind mount.
RUN set -eux; \
    groupadd -g "${ANALYST_GID}" analyst; \
    useradd -m -u "${ANALYST_UID}" -g "${ANALYST_GID}" -s /bin/bash analyst; \
    mkdir -p /home/analyst/.claude/projects \
             /home/analyst/.cache/uv \
             /home/analyst/.config/gcloud \
             /home/analyst/dbt-logs \
             /home/analyst/state; \
    chown -R analyst:analyst /home/analyst; \
    chmod 0700 /home/analyst; \
    mkdir -p /app /secrets; \
    # docker initialises a fresh named volume from the image path it is mounted
    # over, ownership included. Without this the analyst-dbt-target volume comes
    # up root-owned and `dbt compile` dies with EACCES (verified). /app itself is
    # a read-only bind mount at runtime, so this stub only matters for that copy.
    mkdir -p /app/dbt/gharchive_metrics/target; \
    chown -R analyst:analyst /app/dbt; \
    # The bind-mounted repo is owned by the host user, not by uid 10001, so git
    # would refuse it as "dubious ownership" and break `git status/diff/log`.
    git config --system --add safe.directory /app

ENV HOME=/home/analyst \
    CLAUDE_CONFIG_DIR=/home/analyst/.claude \
    UV_CACHE_DIR=/home/analyst/.cache/uv \
    CLOUDSDK_PYTHON=/usr/bin/python3 \
    CLOUDSDK_CONFIG=/home/analyst/.config/gcloud \
    CLOUDSDK_CORE_DISABLE_PROMPTS=1 \
    GCP_KEY_PATH=/secrets/analyst-bq-key.json \
    GOOGLE_APPLICATION_CREDENTIALS=/secrets/analyst-bq-key.json \
    # dbt: keep logs out of the read-only repo, and no phone-home / no
    # .user.yml write into the read-only profiles dir.
    DBT_LOG_PATH=/home/analyst/dbt-logs \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false \
    DO_NOT_TRACK=1 \
    # git must not try to take optional index locks on a read-only worktree.
    GIT_OPTIONAL_LOCKS=0 \
    # No self-update (global npm dir is root-owned) and no non-essential egress.
    DISABLE_AUTOUPDATER=1 \
    DISABLE_TELEMETRY=1 \
    DISABLE_ERROR_REPORTING=1 \
    CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1

COPY docker/analyst-bot/entrypoint.sh /usr/local/bin/analyst-bot-entrypoint
RUN chmod 0755 /usr/local/bin/analyst-bot-entrypoint

# cwd is load-bearing: `claude --resume` is scoped to the project dir derived
# from cwd, and the bot derives transcript paths from it. Keep it /app forever.
WORKDIR /app

USER analyst

ENTRYPOINT ["/usr/local/bin/analyst-bot-entrypoint"]
