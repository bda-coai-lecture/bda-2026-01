# Slack 분석 봇 컨테이너 운영 런북

> 2026-08-01 기준 `docker-compose.yml`에 `analyst-bot` 서비스가 등록되어 있다.
> 호스트 실행은 빠른 시연용이고, Docker 실행은 Read/Grep/Glob의 레포 밖 읽기 위험을
> 좁은 마운트로 줄이는 운영 경로다. BigQuery는 전용 read-only 서비스 계정으로 제한한다.

최종 수정: 2026-08-02

대상: `scripts/slack_analyst_bot.py` (Slack 스레드 1개 = Claude Code 세션 1개)
관련 파일: `docker/analyst-bot/Dockerfile`, `docker/analyst-bot/entrypoint.sh`,
`docker-compose.yml`의 `analyst-bot` 서비스.

## 왜 컨테이너에 넣는가

이 봇은 워크스페이스의 **누구나** 던진 질문으로 `claude -p` 세션을 띄운다.
실측 결과 `Read` 도구는 호스트의 **임의 절대경로**를 읽을 수 있고 이를 막는 권한 규칙이 없다.
(`Read(/etc/**)`, `Read(//etc/**)`, `Read(**/hosts)`, `Read(/etc/*)` 전부 우회됨.
allow 규칙을 레포로 좁혀도 read-only 도구는 기본 허용이라 아무것도 못 막는다.
근거 주석은 `scripts/slack_analyst_bot.py`의 "hole 2".)

호스트에서 그 의미는 `~/.ssh`, `~/.aws`, `~/.claude.json`, `~/Documents/gcp-key.json`이
전부 사정거리 안이라는 뜻이다. 따라서 **격리 경계는 권한 목록이 아니라 컨테이너다.**
컨테이너 안에는 프로젝트와 **좁은 권한의 BigQuery 키 1개**만 존재해야 하고,
그 상태에서는 무제한 `Read`도 볼 것이 없다.

이 설계의 유일한 규칙: **마운트를 늘리지 말 것.**

## 1회 준비

### 1) 읽기 전용 BigQuery 서비스 계정 키

```bash
mkdir -p secrets
cp <read-only-sa-key>.json secrets/analyst-bq-key.json
chmod 644 secrets/analyst-bq-key.json   # 컨테이너는 uid 10001로 :ro 마운트를 읽는다
```

- 키에 필요한 역할은 `roles/bigquery.dataViewer` + `roles/bigquery.jobUser` +
  `projects/bda-coai/roles/analystBotJobMetadataViewer`이고
  **`dataEditor`는 주지 않는다.** `bq query`는 DDL/DML을 실행할 수 있고 명령 문자열
  매칭으로 SQL을 제한할 수 없으므로 읽기 전용은 IAM에서만 강제된다.
  `analystBotJobMetadataViewer`는 `bigquery.jobs.list` 하나만 가진 custom role이며,
  봇이 `INFORMATION_SCHEMA.JOBS_BY_USER`로 자기 BigQuery 사용량을 audit에 남기기 위해 필요하다.
  (해당 키의 IAM 발급 절차는 별도 런북에서 다룬다. 여기서는 경로만 소비한다.)
- **`./gcp-key.json`을 마운트하지 않는다.** 그것은 `~/Documents/gcp-key.json`으로 가는
  심볼릭 링크이며 운영자의 광범위 키다.
- `secrets/`는 `.gitignore`에 추가했다. 기존 `gcp-key.json` 패턴은 파일명 기반이라
  `secrets/analyst-bq-key.json`을 덮지 않았고, 다른 어떤 규칙도 덮지 않았다.

### 2) 환경변수

`.env` (compose 보간 대상)에 아래를 추가한다. **`analyst-bot`은 `env_file`을 쓰지 않는다.**
`.env`에는 Airflow 알림 앱의 `SLACK_BOT_TOKEN`과 `GITHUB_TOKEN`/`GH_TOKEN`이 있고
그것들은 이 서비스에 들어가면 안 되므로, `docker-compose.yml`에서 변수를 하나씩 열거한다.

```bash
SLACK_ANALYST_BOT_TOKEN=xoxb-...     # 분석 봇용 Bot User OAuth Token
SLACK_ANALYST_APP_TOKEN=xapp-...     # connections:write, Socket Mode
# Claude 인증은 아래 "Anthropic 인증" 절 중 하나를 선택한다.
# 권장: 컨테이너 안에서 `claude auth login` 1회 실행.
# 대안: CLAUDE_CODE_OAUTH_TOKEN=... 또는 ANTHROPIC_API_KEY=...
# 선택
ANALYST_CHANNEL_ALLOWLIST=C0123ABCD,C0456EFGH
ANALYST_MAX_BUDGET_USD=5
ANALYST_MAX_SCAN_GIB=10
ANALYST_TURN_TIMEOUT_S=900
ANALYST_MAX_WORKERS=1
ANALYST_HEADLESS=1
ANALYST_LOG_LEVEL=INFO

# 선택: Metabase 카드/대시보드 자동 생성
ANALYST_ENABLE_METABASE_MCP=1
ANALYST_METABASE_INTERNAL_URL=http://metabase:3000  # MCP 접속용 컨테이너 내부 URL
ANALYST_METABASE_PUBLIC_URL=http://localhost:3001   # Slack 링크용 브라우저 URL
METABASE_API_KEY=mb_...
ANALYST_METABASE_COLLECTION_NAME=BDA 데이터 플랫폼

# 선택: 별도 Postgres/RDS audit 테이블 저장
ANALYST_AUDIT_DATABASE_URL=postgresql://...
ANALYST_AUDIT_SCHEMA=analyst_audit
```

Slack 앱 자체를 아직 안 만들었다면 전체 절차는 봇이 직접 출력한다:

```bash
docker compose run --rm analyst-bot --dry-run
```

Metabase MCP는 기본 꺼짐이다. 켜려면 `METABASE_API_KEY`가 필요하다.
봇은 `@easecloudio/mcp-metabase-server`를 `npx`로 실행하고, Claude 세션에는
필요할 때 카드/대시보드를 만들 수 있는 Metabase 도구만 추가한다. 최종 Slack 답변에는
카드/대시보드 URL과 버튼만 보이고, MCP/tool/collection id/API key 같은 세부값은 숨긴다.
API key는 CLI 인자로 넘기지 않고 runtime MCP config에만 기록한다. config 파일명은
`metabase-credentials-mcp.json`이고, 파일 권한은 0600이며, 세션 Read 차단 목록에도 들어간다.

Docker Compose로 띄운 봇에서 Metabase에 붙을 때는 `localhost:3001`을 쓰지 않는다.
컨테이너 내부의 `localhost`는 봇 컨테이너 자신이므로 MCP 접속 URL은
`http://metabase:3000`이어야 한다. Slack에 노출할 링크만 브라우저용
`http://localhost:3001`로 둔다.

Postgres/RDS audit을 켤 때는 먼저 `scripts/analyst_audit_schema.sql`을 audit DB에서 실행한다.
세부 설계와 backfill 절차는 `docs/analyst_bot_postgres_audit.md`, 강의용 데이터 흐름 설명은
`docs/analyst_bot_feedback_loop_lecture.html`을 본다.

## Anthropic 인증 — 반드시 읽을 것

**컨테이너 안의 CLI는 운영자의 호스트 로그인을 쓸 수 없다.**
Claude 구독(Pro/Max) OAuth 로그인은 macOS Keychain과 `~/.claude.json`에 저장되고,
그 둘은 의도적으로 마운트하지 않는다(마운트하면 이 문서의 존재 이유가 사라진다).
즉 **구독 로그인은 컨테이너로 이전되지 않는다.**

권장 경로는 컨테이너 안에서 OAuth 로그인을 한 번 수행하는 것이다.

```bash
docker compose run --rm -it --entrypoint claude analyst-bot auth login
```

로그인 결과는 `analyst-claude` named volume의 `/home/analyst/.claude/.credentials.json`에
저장되고, 이후 `docker compose up -d analyst-bot`에서 재사용된다.

대안:

- `CLAUDE_CODE_OAUTH_TOKEN=...`: 호스트에서 `claude setup-token`으로 만든 장기 토큰을 `.env`에 둔다.
- `ANTHROPIC_API_KEY=...`: Anthropic Console API 키. 사용량은 구독이 아니라 API 크레딧에서 빠진다.
- 게이트웨이/프록시를 쓴다면 `ANTHROPIC_AUTH_TOKEN` + `ANTHROPIC_BASE_URL`도 전달된다.

자격증명이 하나도 없으면 entrypoint가 즉시 실패하며 위 선택지를 출력한다.
호스트 `~/.claude` 디렉터리 전체를 마운트하지 않는다. 꼭 파일 기반으로 넘겨야 한다면
`~/.claude` 전체가 아니라 필요한 credential 파일 하나만 별도로 검토한다.

## 빌드와 실행

```bash
docker compose build analyst-bot
docker compose up -d analyst-bot
docker compose logs -f analyst-bot
docker compose restart analyst-bot     # 스레드 컨텍스트는 유지된다 (아래 볼륨 참고)
docker compose stop analyst-bot
```

`restart: unless-stopped` — 레포의 다른 장기 실행 서비스와 같은 정책이다.
`SIGTERM`/`SIGINT`가 오면 봇은 실행 중인 turn의 cancel event를 세팅하고 child process group을 정리할 기회를 준다.
Compose에는 `init: true`, `stop_grace_period: 30s`를 둔다.

이미지 재현성을 위해 `CLAUDE_CODE_VERSION`은 첫 빌드 로그의 `claude --version`을 보고 고정한다:

```bash
docker compose build --build-arg CLAUDE_CODE_VERSION=<빌드된 버전> analyst-bot
```

## 이미지 구성

| 구성 | 내용 |
|---|---|
| base | `ghcr.io/astral-sh/uv:python3.13-bookworm-slim` (pyproject `>=3.13,<3.14`, uv 포함) |
| Node.js | `ARG NODE_VERSION=22.11.0` 공식 tarball, arch 자동 판별(amd64/arm64) |
| Claude Code | `npm install -g @anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}` |
| bq / gcloud | apt `google-cloud-cli` (추가 component 없음). `CLOUDSDK_PYTHON=/usr/bin/python3` |
| 기타 | `git`, `ripgrep` (세션 Bash 허용목록이 `rg`, `git status/diff/log`를 씀) |
| Python 패키지 | `slack-bolt`만. dbt는 세션이 `uv run --no-project --with dbt-bigquery dbt`로 받는다 |
| 사용자 | `analyst` uid 10001 / gid 10001, `USER analyst`. `/app`은 root 소유(읽기 전용 마운트) |

`gcloud`가 Python 3.13에서 검증되지 않았기 때문에 Debian `python3`(3.11)를 별도로 깔고
`CLOUDSDK_PYTHON`으로 그것만 쓰게 했다. 봇 본체는 3.13에서 돈다.

## 마운트 결정과 근거

| 마운트 | 모드 | 이유 |
|---|---|---|
| 선택 마운트 → `/app` | **ro** | 전체 레포를 마운트하지 않는다. `.claude`, `dbt`, `docs`, `scripts`, `reports`, `config`, `dags`, `AGENTS.md` 등 필요한 경로만 읽기 전용으로 연다. `/app/secrets`가 생기지 않게 하는 것이 핵심이다 |
| `./dbt/gharchive_metrics/analyses` | rw | 분석가가 임시 SQL을 남기는 **유일한** 호스트 노출 쓰기 지점 (봇 allowlist의 `Write/Edit`도 이 경로만 허용) |
| `analyst-dbt-target` (named) | rw | `dbt compile`이 `target/`에 쓰고, 스킬은 `target/compiled/...`를 dry-run한다. 정본 경로에 있어야 하지만 호스트 레포를 더럽힐 필요는 없다 |
| `./secrets/analyst-bq-key.json` → `/secrets/analyst-bq-key.json` | **ro** | 좁은 권한 BQ 키 1개. `/app` 아래에는 노출하지 않는다 |
| `analyst-claude` → `/home/analyst/.claude` | rw | Claude 세션 transcript 영속화 |
| `analyst-gcloud` → `/home/analyst/.config/gcloud` | rw | `gcloud auth activate-service-account`와 `.bigqueryrc` 저장. 봇의 `ANALYST_GCLOUD_CONFIG_DIR`와 같은 경로 |
| `analyst-state` → `/home/analyst/state` | rw | audit/trace/feedback JSONL 저장. 봇은 `--state-dir /home/analyst/state`로 뜬다 |
| `analyst-uv-cache` → `/home/analyst/.cache/uv` | rw | uv 캐시 (`uv run --with dbt-bigquery` 재다운로드 방지) |

**필요 경로 ro + 쓰기 경로 carve-out을 택했다.** 유일한 위험은 dbt였는데
쓰기 지점이 셋뿐이고 전부 처리했다:

1. `target/` → named volume (위).
2. dbt 로그 → 레포 대신 `DBT_LOG_PATH=/home/analyst/dbt-logs` (마운트 불필요).
3. `dbt/profiles/.user.yml`(익명 통계용) → `DO_NOT_TRACK=1`,
   `DBT_SEND_ANONYMOUS_USAGE_STATS=false`로 아예 쓰지 않게 했다.

추가로 read-only 워크트리 때문에 필요한 두 가지:

- `git config --system --add safe.directory /app` — 마운트된 파일 소유자가
  uid 10001이 아니므로, 없으면 git이 "dubious ownership"으로 `git status/diff/log`를 거부한다.
- `GIT_OPTIONAL_LOCKS=0` — read-only 워크트리에서 index lock 시도를 하지 않게 한다.

### 절대 마운트하지 않는 것

호스트 `~/.claude`, `~/.ssh`, `~/.aws`, `~/.config/gcloud`, `~/Documents`,
`./gcp-key.json`, `/var/run/docker.sock`.
docker socket은 컨테이너 격리를 무의미하게 만들고, 나머지는 이 문서 첫 절의 위협 그대로다.

### uid 주의 (Linux 호스트)

macOS Docker Desktop은 bind mount의 uid를 호스트 사용자로 매핑해주므로
uid 10001이 `analyses/`에 그대로 쓸 수 있다. **Linux에서는 아니다.** 그 경우

```bash
setfacl -m u:10001:rwx dbt/gharchive_metrics/analyses    # 또는
# docker-compose.yml에 임시로  user: "$(id -u):$(id -g)"  를 추가
```

entrypoint는 `analyses/`가 쓰기 불가면 **경고**만 하고 기동한다(분석 답변 자체는 가능하고
SQL 저장만 실패하므로). `target/`이 쓰기 불가면 `dbt compile`이 깨지므로 같은 경고가 뜬다.

## 세션 영속성

봇은 `(team, channel, thread_ts)`에서 session id를 결정론적으로 만들고,
transcript 경로는 `~/.claude/projects/<인코딩된 cwd>/<session_id>.jsonl`이다.
인코딩은 `/`와 **`.` 둘 다** `-`로 바꾼다. cwd가 `/app`이므로 결과는

```
/home/analyst/.claude/projects/-app/<uuid>.jsonl
```

- 그래서 **cwd는 영구히 `/app`**이다. 바꾸면 인코딩된 디렉터리가 바뀌고 기존 스레드의
  `--resume`이 전부 첫 턴으로 되돌아간다. entrypoint가 `--repo-dir /app`을 항상 넘긴다.
- 그래서 named volume은 `~/.claude` **전체**를 덮는다(`projects/`만이 아니라
  `.credentials.json`, `shell-snapshots/`, `statsig/`도 여기 있다).
- `docker compose restart`는 스레드 컨텍스트를 유지한다. `docker compose down -v`는 **날린다.**

```bash
docker compose exec analyst-bot ls /home/analyst/.claude/projects/-app | head
```

## 로깅 — headless와 log level을 분리했다

Docker 기본값은 `ANALYST_HEADLESS=1`, `ANALYST_LOG_LEVEL=INFO`다.
`--headless`는 이제 실행 모드 표시만 하고, 로그 억제는 `--log-level`이 담당한다.
따라서 detached/headless로 떠도 `docker compose logs -f analyst-bot`에 기동 로그와 턴 trace가 남는다.
정말 조용히 돌리려면 `.env`에 `ANALYST_LOG_LEVEL=WARNING`을 넣는다.

## 플래그 전달

entrypoint가 `ANALYST_*` 환경변수를 CLI flag로 변환한다.
기본으로 `--repo-dir /app`, `--state-dir /home/analyst/state`,
`--max-scan-gib 10`, `--max-workers 1`, `--log-level INFO`가 전달된다.
`ANALYST_ENABLE_METABASE_MCP=1`이면 `--enable-metabase-mcp`, MCP 접속 URL, Slack 링크용 public URL,
컬렉션 이름도 전달된다.
argparse는 뒤에 온 값이 이기므로 임시 실행은 이렇게 한다:

```bash
docker compose run --rm analyst-bot --dry-run                  # 사전 점검 + 설정 출력
docker compose run --rm analyst-bot --self-test                # Slack 없이 세션 생성/재개 검증
docker compose run --rm analyst-bot --model claude-opus-4-6 --timeout 1800
docker compose run --rm --entrypoint bash analyst-bot          # 셸로 들어가기
```

## entrypoint가 검사하는 것

전부 모아서 한 번에 보고하고, 하나라도 걸리면 기동하지 않는다.

1. `claude`, `bq`가 PATH에 있는지, `slack_bolt`가 import되는지
2. `/app`이 마운트되어 있고 `scripts/slack_analyst_bot.py`와
   `.claude/skills/analysis/SKILL.md`가 있는지
3. BQ 키가 **파일**이고 읽을 수 있고 `"type": "service_account"`인지
   (호스트 경로가 없으면 docker가 **디렉터리**를 만들어버리므로 그 경우를 따로 안내한다)
4. `SLACK_ANALYST_BOT_TOKEN` / `SLACK_ANALYST_APP_TOKEN` 존재
5. `ANALYST_ENABLE_METABASE_MCP=1`이면 `npx`, `METABASE_URL`, `METABASE_API_KEY` 존재
6. Anthropic 자격증명 존재 (없으면 위 "Anthropic 인증" 안내와 함께 실패)
7. `analyses/`, `target/` 쓰기 가능 여부 → 경고
8. `/home/analyst/state`, `/home/analyst/.config/gcloud` 쓰기 가능 여부 → 실패 시 기동 차단

그다음 `gcloud auth activate-service-account --key-file=$GCP_KEY_PATH`를 실행한다.
**`bq`는 `GOOGLE_APPLICATION_CREDENTIALS`를 읽지 않고** gcloud 자격증명 저장소를 쓰기 때문에,
이걸 안 하면 세션의 모든 `bq query`가 auth 오류로 죽는다. 실패 시 기동을 막고
재현 명령을 출력한다(`ANALYST_SKIP_GCLOUD_AUTH=1`로 우회 가능, 그 경우 bq는 못 쓴다).

또한 `SLACK_BOT_TOKEN`, `GITHUB_TOKEN`, `GH_TOKEN`, `SLACK_ALERT_*`를 방어적으로 `unset`한다.
누군가 나중에 `env_file: .env`를 다시 붙여도 알림 앱 토큰과 GitHub 토큰이 세션에 새지 않는다.

## Read/Grep/Glob과 secrets 경계

현재 봇의 `SESSION_TOOLS`에는 `Read,Grep,Glob`이 포함된다. 그래서 Docker 서비스는
전체 레포를 `/app`에 마운트하지 않는다. 특히 `secrets/`는 `/app` 아래에 두지 않고,
BigQuery 키만 `/secrets/analyst-bq-key.json`으로 별도 read-only 마운트한다.
코드에도 `analyst-bq-key.json`과 `/secrets` file-reading 명령 deny, 최종 Slack 답변 redaction을 추가했다.
`docker-compose.yml`은 예외적으로 `/app/docker-compose.yml`에 read-only 마운트한다.
이 파일이 없으면 컨테이너 안 `git status`가 compose 파일을 삭제된 파일로 오진하고,
봇이 Metabase 스택 상태를 잘못 판단할 수 있다.
이 마운트 경계를 바꿀 때도 `DISALLOWED_TOOLS`의 `Read(**/gcp-key.json)`류 규칙은
그대로 둔다. 마운트된 BQ 키 자체를 세션이 출력하는 것은 여전히 막을 이유가 있다.

## 트러블슈팅

| 증상 | 원인 / 조치 |
|---|---|
| `startup blocked by N problem(s)` | 출력된 항목 그대로 고친다. 전부 조치 문구 포함 |
| `/secrets/analyst-bq-key.json is a DIRECTORY` | 호스트에 키 파일이 없어 docker가 디렉터리를 생성함. 빈 디렉터리 지우고 키를 놓고 `down` → `up -d` |
| 세션이 매번 첫 턴처럼 굴다 | `analyst-claude` 볼륨이 사라졌거나 cwd가 `/app`이 아님. `ls /home/analyst/.claude/projects` 확인 |
| `bq` auth 오류 | entrypoint의 `activate-service-account`가 건너뛰어졌거나 키 권한 부족(`jobUser` 확인) |
| dbt가 `Env var required but not provided: 'GCP_KEY_PATH'` | `GCP_KEY_PATH`가 세션에 없다. compose environment 확인 |
| `dbt compile` permission denied | `analyst-dbt-target` 볼륨이 uid 10001 소유가 아님. `docker compose down && docker volume rm bda-2_analyst-dbt-target` 후 재기동 |
| 로그가 조용하다 | `ANALYST_LOG_LEVEL`이 `WARNING` 이상인지 확인. Docker 기본은 headless + INFO 로그 |
| Slack에 답이 안 온다 | 채널에 봇 초대(`/invite`), `ANALYST_CHANNEL_ALLOWLIST` 확인, Socket Mode/이벤트 구독 확인 |
| Metabase 카드가 안 만들어진다 | `ANALYST_ENABLE_METABASE_MCP=1`, `METABASE_API_KEY`, `ANALYST_METABASE_INTERNAL_URL` 확인. compose 내부 URL은 보통 `http://metabase:3000` |
| Slack의 Metabase 버튼이 안 열린다 | `ANALYST_METABASE_PUBLIC_URL` 확인. Docker 내부 URL(`http://metabase:3000`)이 Slack에 노출되면 브라우저에서 열리지 않는다 |
| 카드 생성 요청이 10분 이상 걸린다 | 현재 프롬프트는 카드 생성만 요청해도 분석 스킬과 건강성 검증을 다시 탄다. 로그에서 `bq` 120초 timeout, Metabase `execute_query` 500, `mcp__metabase__create_card` 시각을 확인한다. 이미 검증된 SQL/기존 카드 링크 요청에는 fast path 코드가 아직 없다 |

## 검증 상태 (2026-08-02)

확인한 것:

- `uv run python -m py_compile scripts/slack_analyst_bot.py tests/test_slack_analyst_bot.py` 통과.
- `uv run pytest tests/test_slack_analyst_bot.py` → 29 passed.
- `bash -n docker/analyst-bot/entrypoint.sh` 통과.
- `docker compose config --services` 통과, 서비스 목록에 `analyst-bot` 포함.
- Metabase MCP on/off 명령 구성, `--strict-mcp-config`, `--mcp-config` 생성, API key argv 비노출, internal/public URL 분리 단위 테스트 통과.
- `docker compose config` 렌더 기준으로 `analyst-bot`은 전체 레포가 아니라 필요한 경로만 `/app`에 마운트하고,
  BQ 키는 `/secrets/analyst-bq-key.json`으로 별도 마운트한다. `docker-compose.yml`은
  컨테이너 내부 `git status` 오진 방지를 위해 read-only로 별도 마운트한다.
- `docker compose build analyst-bot` 통과. 봇 프로세스 이미지에 `google-cloud-bigquery` 포함.
- 컨테이너 격리 점검 통과: `/app/secrets`와 `/app/gcp-key.json` 없음, `/app` root는 쓰기 불가,
  `/home/analyst/state`, `/home/analyst/.config/gcloud`, `analyses/`, `target/`만 쓰기 가능.
- 컨테이너 안 Claude OAuth 로그인 통과. `/home/analyst/.claude/.credentials.json`이
  `analyst-claude` named volume에 저장됨을 확인했다.
- 실제 OAuth credential로 `docker compose run --rm analyst-bot --dry-run` 통과.
  Docker 경로에서 `repo_dir=/app`, `state_dir=/home/analyst/state`,
  `gcloud_config=/home/analyst/.config/gcloud`, `bq_identity=bda-analyst-ro@...`,
  `bq_scan_ceiling=10 GiB via .bigqueryrc`를 확인했다.
- 실제 OAuth credential로 `docker compose run --rm analyst-bot --self-test` 통과.
  session id가 두 턴에서 동일했고 `--resume`이 동작했다.
- `docker compose up -d analyst-bot` 통과. 로그에서 headless Socket Mode 연결과
  `Bolt app is running!`을 확인했다.
- Slack end-to-end canary 1회 통과. 실제 멘션을 받아 BigQuery 조회 후 답변을 게시했다.
  이 canary에서 답변 톤이 기술적으로 읽히는 문제가 드러나, 이후 최종 답변 프롬프트와
  캐주얼 용어 치환을 보강했다.
- 컨테이너 안에서 `bq query --project_id=bda-coai --use_legacy_sql=false "select session_user()"` 실행 시
  `bda-analyst-ro@...`로 실행되는 것을 확인했다.
- 컨테이너 안에서 `uv run --no-project --with dbt-bigquery dbt compile ... --select metrics_daily` 통과.
- compile된 `metrics_daily.sql`을 `bq --dry_run`으로 검증해 bq CLI 경로가 동작하는 것을 확인했다
  (예상 스캔이 10 GiB를 넘는 쿼리는 실행하지 않는다).
- Metabase API key를 로컬 Metabase에서 생성해 `.env`에만 저장하고, 값이 argv/log에 노출되지 않는 상태로
  `analyst-bot`을 재기동했다. 로그에서 `metabase_mcp=enabled`, MCP runtime config 0600,
  컨테이너 내부 `http://metabase:3000/api/health` 200을 확인했다.
- Metabase MCP 실제 카드 생성 canary 통과. `BDA 데이터 플랫폼` 컬렉션에 native SQL 카드가 생성되고
  Slack 답변에는 public URL(`http://localhost:3001/question/...`)이 노출됨을 확인했다.

확인하지 못한 것:

- 톤/용어 보강 후 Slack end-to-end 턴 재확인은 아직 남아 있다.
- 이미 검증된 SQL/기존 카드 링크 요청을 재분석 없이 처리하는 fast path는 아직 없다.
- `docker build --check`는 호스트 buildx 버전에 따라 사용 불가할 수 있다.
