# Slack 분석 봇 컨테이너 운영 런북

> **현재 사용하지 않음 (2026-07-26 결정).** 이번 회차는 호스트에서 직접 실행한다.
> 이 문서와 `docker/analyst-bot/`은 나중에 격리가 필요해질 때 쓰기 위해 보존한다.
> 호스트 실행이 안전한 근거: `SESSION_TOOLS`가 Read/Grep/Glob을 제거해 파일 접근이
> cwd에 갇힌 Bash로만 가능하고, BigQuery는 전용 read-only 서비스 계정으로 제한된다.
> `docker-compose.yml`에는 `analyst-bot` 서비스가 등록되어 있지 않다.

최종 수정: 2026-07-26

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

- 키에 필요한 역할은 `roles/bigquery.dataViewer` + `roles/bigquery.jobUser`이고
  **`dataEditor`는 주지 않는다.** `bq query`는 DDL/DML을 실행할 수 있고 명령 문자열
  매칭으로 SQL을 제한할 수 없으므로 읽기 전용은 IAM에서만 강제된다.
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
SLACK_ANALYST_BOT_TOKEN=xoxb-...     # 분석 봇 전용 앱 (Airflow Alerts 앱 아님)
SLACK_ANALYST_APP_TOKEN=xapp-...     # connections:write, Socket Mode
ANTHROPIC_API_KEY=sk-ant-...         # 아래 "Anthropic 인증" 절 필독
# 선택
ANALYST_CHANNEL_ALLOWLIST=C0123ABCD,C0456EFGH
ANALYST_MAX_BUDGET_USD=5
ANALYST_MAX_SCAN_GIB=200
ANALYST_TURN_TIMEOUT_S=900
ANALYST_HEADLESS=0
```

Slack 앱 자체를 아직 안 만들었다면 전체 절차는 봇이 직접 출력한다:

```bash
docker compose run --rm analyst-bot --dry-run
```

## Anthropic 인증 — 반드시 읽을 것

**컨테이너 안의 CLI는 운영자의 호스트 로그인을 쓸 수 없다.**
Claude 구독(Pro/Max) OAuth 로그인은 macOS Keychain과 `~/.claude.json`에 저장되고,
그 둘은 의도적으로 마운트하지 않는다(마운트하면 이 문서의 존재 이유가 사라진다).
즉 **구독 로그인은 컨테이너로 이전되지 않는다.**

- 구현된 경로: **`ANTHROPIC_API_KEY`** (Console API 키). 사용량은 구독이 아니라 API 크레딧에서 빠진다.
- 게이트웨이/프록시를 쓴다면 `ANTHROPIC_AUTH_TOKEN` + `ANTHROPIC_BASE_URL`도 전달된다.
- 자격증명이 하나도 없으면 entrypoint가 즉시 실패하며 위 내용을 출력한다.
- **대안(미구현):** 호스트에서 `claude setup-token`으로 만든 자격증명 파일을
  `~/.claude/.credentials.json`에 넣어 컨테이너의
  `/home/analyst/.claude/.credentials.json`으로 `:ro` 마운트하는 방법이 있다.
  entrypoint는 이 파일이 있으면 인증이 있다고 인정한다. 다만 (a) 토큰 만료 시
  갱신을 컨테이너가 파일에 쓸 수 없어 조용히 죽고, (b) 호스트 `~/.claude` 트리를
  건드리는 마운트를 하나 더 만드는 것이므로 채택하지 않았다. 쓰려면
  `~/.claude` **디렉터리 전체가 아니라 그 파일 하나만** 마운트해야 한다.

## 빌드와 실행

```bash
docker compose build analyst-bot
docker compose up -d analyst-bot
docker compose logs -f analyst-bot
docker compose restart analyst-bot     # 스레드 컨텍스트는 유지된다 (아래 볼륨 참고)
docker compose stop analyst-bot
```

`restart: unless-stopped` — 레포의 다른 장기 실행 서비스와 같은 정책이다.

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
| `./:/app` | **ro** | dbt 모델·docs·`.claude/skills/**`·`AGENTS.md` 읽기용. 쓰기 필요 경로만 아래에서 뚫는다 |
| `./dbt/gharchive_metrics/analyses` | rw | 분석가가 임시 SQL을 남기는 **유일한** 호스트 노출 쓰기 지점 (봇 allowlist의 `Write/Edit`도 이 경로만 허용) |
| `analyst-dbt-target` (named) | rw | `dbt compile`이 `target/`에 쓰고, 스킬은 `target/compiled/...`를 dry-run한다. 정본 경로에 있어야 하지만 호스트 레포를 더럽힐 필요는 없다 |
| `./secrets/analyst-bq-key.json` | **ro** | 좁은 권한 BQ 키 1개 |
| `analyst-claude` → `/home/analyst/.claude` | rw | Claude 세션 transcript 영속화 |
| `analyst-uv-cache` → `/home/analyst/.cache/uv` | rw | uv 캐시 (`uv run --with dbt-bigquery` 재다운로드 방지) |

**레포 전체 rw 대신 "ro + 쓰기 경로 carve-out"을 택했다.** 유일한 위험은 dbt였는데
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

## 로깅 — `--headless`를 기본으로 두지 않았다

컨테이너에 대화형 터미널이 없으므로 `--headless`가 자연스러워 보이지만,
봇의 현재 구현에서 `--headless`는

- 루트 로그 레벨을 `WARNING`으로 내리고 (`main()`의 `logging.basicConfig`),
- 턴별 trace 라인(`on_trace`의 `LOG.info`)을 아예 건너뛴다.

즉 `docker compose logs -f analyst-bot`에 기동 이후 사실상 아무것도 안 남는다.
로그 레벨을 되살리는 환경변수나 플래그는 봇에 없고, 이 작업에서 봇 코드는 고치지 않았다.

봇의 non-headless("attached") 모드는 **TTY를 요구하지 않는다** — 로깅 레벨만 다르다.
그래서 기본값을 `ANALYST_HEADLESS=0`(= `--headless` 미전달)로 두어 INFO 로그와
턴 trace가 `docker compose logs`에 보이게 했다. `PYTHONUNBUFFERED=1`로 버퍼링도 껐다.

정말 조용히 돌리려면 `.env`에 `ANALYST_HEADLESS=1`을 넣는다(로그는 거의 사라진다).
근본 해결은 봇에 `--log-level` 옵션을 추가하는 것이고, 그건 이 작업 범위 밖이다.

## 플래그 전달

`command:`가 기본 플래그를 들고 있고, entrypoint가 `--repo-dir /app` 뒤에 이어 붙인다.
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
5. Anthropic 자격증명 존재 (없으면 위 "Anthropic 인증" 안내와 함께 실패)
6. `analyses/`, `target/` 쓰기 가능 여부 → 경고

그다음 `gcloud auth activate-service-account --key-file=$GCP_KEY_PATH`를 실행한다.
**`bq`는 `GOOGLE_APPLICATION_CREDENTIALS`를 읽지 않고** gcloud 자격증명 저장소를 쓰기 때문에,
이걸 안 하면 세션의 모든 `bq query`가 auth 오류로 죽는다. 실패 시 기동을 막고
재현 명령을 출력한다(`ANALYST_SKIP_GCLOUD_AUTH=1`로 우회 가능, 그 경우 bq는 못 쓴다).

또한 `SLACK_BOT_TOKEN`, `GITHUB_TOKEN`, `GH_TOKEN`, `SLACK_ALERT_*`를 방어적으로 `unset`한다.
누군가 나중에 `env_file: .env`를 다시 붙여도 알림 앱 토큰과 GitHub 토큰이 세션에 새지 않는다.

## 컨테이너에서는 `Read`/`Grep`/`Glob`을 되살릴 수 있다

봇은 호스트 안전 때문에 세션 도구 집합에서 이 셋을 빼고 있다:

```python
# scripts/slack_analyst_bot.py
SESSION_TOOLS = "Bash,Write,Edit,Skill,TodoWrite"
```

이유는 하나뿐이다 — 호스트에서 `Read`가 레포 밖 절대경로를 읽을 수 있고 막을 수 없다.
**컨테이너에서는 그 전제가 사라진다.** 마운트가 프로젝트와 BQ 키뿐이므로 무제한 `Read`가
도달할 수 있는 민감 파일이 없다. 따라서 컨테이너 전용 배포에서는

```python
SESSION_TOOLS = "Read,Grep,Glob,Bash,Write,Edit,Skill,TodoWrite"
```

로 되돌리는 것이 합리적이다. 얻는 것: 파일 읽기/검색이 `cat`/`rg` Bash 우회 없이
정상 도구로 돌아가고, `APPEND_SYSTEM_PROMPT`의 0번 항목(도구 없음 안내)도 필요 없어진다.

**이번 작업에서는 봇 파이썬 코드를 고치지 않았다.** 같은 스크립트가 호스트에서도
실행되기 때문에 되살리려면 컨테이너 여부로 분기해야 하고(예: 환경변수 게이트),
그건 코드 변경 승인이 필요한 별도 결정이다. 되살릴 때도 `DISALLOWED_TOOLS`의
`Read(**/gcp-key.json)`류 규칙은 그대로 두는 것이 좋다 — 마운트된 BQ 키 자체를
세션이 출력하는 것은 여전히 막을 이유가 있다.

## 트러블슈팅

| 증상 | 원인 / 조치 |
|---|---|
| `startup blocked by N problem(s)` | 출력된 항목 그대로 고친다. 전부 조치 문구 포함 |
| `/secrets/analyst-bq-key.json is a DIRECTORY` | 호스트에 키 파일이 없어 docker가 디렉터리를 생성함. 빈 디렉터리 지우고 키를 놓고 `down` → `up -d` |
| 세션이 매번 첫 턴처럼 굴다 | `analyst-claude` 볼륨이 사라졌거나 cwd가 `/app`이 아님. `ls /home/analyst/.claude/projects` 확인 |
| `bq` auth 오류 | entrypoint의 `activate-service-account`가 건너뛰어졌거나 키 권한 부족(`jobUser` 확인) |
| dbt가 `Env var required but not provided: 'GCP_KEY_PATH'` | `GCP_KEY_PATH`가 세션에 없다. compose environment 확인 |
| `dbt compile` permission denied | `analyst-dbt-target` 볼륨이 uid 10001 소유가 아님. `docker compose down && docker volume rm bda-2_analyst-dbt-target` 후 재기동 |
| 로그가 조용하다 | `ANALYST_HEADLESS=1`이 켜져 있다. 위 로깅 절 참고 |
| Slack에 답이 안 온다 | 채널에 봇 초대(`/invite`), `ANALYST_CHANNEL_ALLOWLIST` 확인, Socket Mode/이벤트 구독 확인 |

## 검증 상태 (2026-07-26)

확인한 것:

- `docker compose config` 통과. `analyst-bot`이 의도한 마운트/환경변수로 렌더되고,
  렌더 결과에 `SLACK_BOT_TOKEN`/`GITHUB_TOKEN`/`GH_TOKEN`이 **없다**(airflow 서비스에는 보인다).
  컨테이너 안 `env`에도 그 셋이 없음을 실제로 확인했다.
- 기존 서비스 / `x-airflow-common` 앵커 / 기존 볼륨은 수정하지 않았다(추가만).
- **이미지 빌드 성공.** arm64, 82초, 1.44 GB.
  Python 3.13.11 / Node v22.11.0 / Claude Code **2.1.220** / bq 2.1.35 / Google Cloud SDK 577.0.0.
- entrypoint 사전 점검: 자격증명이 전혀 없을 때 5개 문제를 한 번에 출력하고 exit 1.
  잘못된 키로는 `gcloud auth activate-service-account` 실패를 잡아내고 기동을 막는다.
- 컨테이너 안에서 봇 `--dry-run`이 정상 동작. `project_transcript_dir`이
  `/home/analyst/.claude/projects/-app`으로, `alerting_app_token_present = False`로 나온다.
- 마운트 실측: `/app/README.md` 쓰기 → `Read-only file system`(의도),
  `analyses/` 쓰기 OK, `target/` 볼륨 쓰기 OK, `~/.claude`·uv 캐시 쓰기 OK,
  `git status`/`git log`가 read-only 워크트리에서 동작(safe.directory 적용됨).
- `uv run --no-project --with dbt-bigquery dbt parse --project-dir ... --profiles-dir ...`
  이 컨테이너 안에서 **성공**했다(dbt 1.12.0, bigquery adapter 등록, `target/perf_info.json` 기록,
  로그는 `/home/analyst/dbt-logs/dbt.log`). 읽기 전용 레포 + 쓰기 경로 carve-out 설계가
  dbt를 깨뜨리지 않는다는 근거다.
- 빌드 중 발견해 고친 실제 버그: `analyst-dbt-target` 볼륨이 root 소유로 생성되어
  uid 10001이 쓸 수 없었다(`dbt compile`이 EACCES로 죽는다). Dockerfile에서
  `/app/dbt/gharchive_metrics/target`을 미리 만들고 `analyst` 소유로 바꿔,
  docker가 새 볼륨을 초기화할 때 소유권을 물려받게 했다. **기존에 만들어진 볼륨이 있으면
  `docker volume rm bda-2_analyst-dbt-target` 후 재기동해야 이 수정이 적용된다.**

확인하지 못한 것:

- **Slack end-to-end 턴을 돌리지 않았다.** 분석 전용 Slack 앱 토큰과 읽기 전용 BQ 키가
  아직 없어서, 검증은 형식이 맞는 더미 키/토큰으로 했다(`--dry-run`까지).
  실제 자격증명이 준비되면 `docker compose run --rm analyst-bot --self-test`로
  세션 생성 → `--resume` 재개까지 먼저 확인하는 것을 권한다.
- `bq query` 실제 실행(권한·스캔 상한 동작)도 실제 키가 필요하다.
- `docker build --check`는 사용할 수 없었다(호스트 buildx v0.8.2, 해당 기능은 더 최신 필요).
  대신 실제 빌드가 성공했으므로 그보다 강한 검증이다.
