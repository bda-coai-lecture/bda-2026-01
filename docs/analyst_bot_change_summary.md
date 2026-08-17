# Slack 분석 봇 변경 요약

작성: 2026-08-02

이 문서는 Slack 분석 봇을 운영 가능한 형태로 다듬은 변경 내용을 세 덩어리로 정리한다.
상세 운영 절차는 `docs/analyst_bot_handoff.md`, 짧은 시연 절차는
`docs/analyst_bot_demo_runbook.md`, 컨테이너 운영은 `docs/analyst_bot_docker.md`를 본다.

## 1. hamji 워크플로우 적용과 Slack UX 변경

이번 작업의 출발점은 오래 운영된 Slack/Bolt 봇인 `hamji-portable-20260801`의 패턴이었다.
그 구현을 그대로 옮기지는 않았고, BDA 2의 단일 Python 러너에 맞게 필요한 운영 습관만 가져왔다.

핵심 판단은 Slack을 실행 로그 창이 아니라 의사결정 화면으로 보자는 것이다. 그래서 Slack에는
결론, 핵심 숫자, 기준일/기간, 한계만 남기고, 검증 과정과 재현 경로는 로컬 로그로 분리했다.

바뀐 Slack 표면:

- 최종 답변은 결론으로 바로 시작한다.
- 답변은 기술 감사 노트가 아니라 Slack에서 동료가 알려주는 말투에 가깝게 쓴다. `active actor` 같은 지표명은 필요한 경우에만 뒤에 붙이고, 먼저 `활동 계정`처럼 읽히는 말을 쓴다.
- `분석 완료`, `아래가 답변입니다`, `모드: 활동량`, `인풋`, `관측 grain`, `---` 같은 내부 라벨을 제거한다.
- `dry run`, `bq query`, `dbt compile`, `Claude N턴`, `BigQuery N GiB` 같은 운영 세부값을 Slack에 노출하지 않는다.
- 사용자가 `간단히`라고 하면 2-4문장과 한계 한 줄로 끝낸다. 비교표나 추가 진단은 결론이 바뀔 때만 붙인다.
- 진행 중에는 raw trace 대신 사람이 읽을 수 있는 상태 문구를 보여준다.
- 오래 걸리는 작업에는 elapsed와 최대 시간을 보여주고, 30초 이상 조용하면 heartbeat를 남긴다.
- 진행 메시지에 `중단하기` 버튼을 붙였다. 누르면 “중단 요청됨”을 먼저 보여주고, 작업 종료 후 “중단됨”으로 정리한다.
- 최종 답변에는 피드백 버튼을 붙여 audit 로그와 연결한다.
- Metabase가 켜져 있을 때는 카드/대시보드 URL과 `Metabase 열기` 버튼만 보여준다. MCP, API key, collection id 같은 내부값은 숨긴다.

작업 중 대화에서 정리된 문장은 “분석자용 근거는 로그, Slack은 의사결정 화면”이었다.
이 기준으로 답변 후처리, 진행 메시지, audit 저장 위치를 계속 조정했다.

## 2. 기술 변경사항, Docker 제외

Docker를 빼고 보면 변경은 크게 네 층이다.

첫째, 최종 답변 후처리 계층을 만들었다. Claude가 분석 절차를 성실히 따르다 보면
검증표, 재현 SQL, dry-run 정보까지 답변에 섞어 쓰는 경우가 있다. 이 내용은 분석자에게는 필요하지만
질문자에게는 소음이다. 그래서 게시 직전에 `format_final_answer_for_slack()`을 거치게 했다.

후처리에서 하는 일:

- 메타 서문 제거
- 내부 체크리스트 라벨 제거
- 운영 상세 라인 제거
- GitHub Markdown을 Slack mrkdwn으로 변환
- 토큰, API key, private key 패턴 redaction
- 최종 답변 2,500자 guardrail 적용
- Metabase internal URL을 public URL로 치환

둘째, 실행 제어를 강화했다. Slack thread 하나가 Claude session 하나가 되기 때문에,
같은 thread에서 동시에 두 턴이 실행되면 transcript가 갈라질 수 있다. 이를 막기 위해
스레드별 FIFO queue를 두고, 전체 동시 실행 수는 `--max-workers`로 제한했다.

관련 변경:

- `(team, channel, thread_ts)`에서 session id를 결정론적으로 생성
- 첫 턴은 `--session-id`, 이후 턴은 `--resume`
- 같은 thread는 순서대로만 실행
- 기본 `--max-workers 1`로 데모 중 BigQuery 사용량 집계가 섞이지 않게 함
- cancel event를 통해 실행 중 child process group을 종료

셋째, 운영 증거를 audit/trace/feedback으로 분리했다. Slack에는 짧은 답만 남기고,
실행 근거는 `logs/analyst-bot/` 아래 JSONL로 저장한다.

저장되는 것:

- 질문, requester, channel/thread, session id
- 성공/실패/중단/delivery error 상태
- Claude turn 수와 duration, API 환산 cost
- BigQuery billed GiB/USD/job 수
- permission denial, malformed JSON line 수
- Slack에는 숨긴 raw trace
- 답변 피드백

넷째, 권한과 비용 경계를 명확히 했다. 봇 세션은 read-only BigQuery 서비스 계정을 쓰고,
운영자 키는 세션에 넣지 않는다. `bq`는 `GOOGLE_APPLICATION_CREDENTIALS`가 아니라
gcloud credential store를 보므로, `child_env()`에서 `CLOUDSDK_CONFIG`와 `BIGQUERYRC`를 강제로 주입한다.

중요한 제한:

- `--max-scan-gib`는 `.bigqueryrc` 기본값으로 들어가지만 프로젝트 quota처럼 airtight한 하드 상한은 아니다.
- 명령줄에서 `--maximum_bytes_billed`를 직접 크게 주면 rc 기본값을 이길 수 있다.
- 그래서 진짜 하드 상한은 BigQuery 콘솔의 사용자당 일일 쿼리 사용량 할당량으로 막아야 한다.
- 현재 이 콘솔 quota 설정은 아직 남아 있다.

Metabase MCP도 선택 기능으로 붙였다. `ANALYST_ENABLE_METABASE_MCP=1`, `METABASE_URL`,
`METABASE_API_KEY`가 있을 때만 `--strict-mcp-config --mcp-config`를 Claude에 넘긴다.
API key는 argv, Slack 답변, audit에 남기지 않고 runtime config 파일에만 쓴다.

이 과정에서 우리 대화는 계속 “어디까지 Slack에 보여줄 것인가”와 “어디를 로그로 보낼 것인가”로 수렴했다.
예를 들어 `검증:`과 `재현:` 라인은 분석자로서는 유용하지만 PO 화면에서는 제거했고,
대신 audit/trace에서 확인 가능하게 남겼다.

검증:

- `uv run pytest tests/test_slack_analyst_bot.py` → 29 passed
- 답변 후처리, Metabase MCP config, 중단 버튼, 피드백 버튼, delivery error, Slack raw error 은닉을 테스트에 포함
- `py_compile`과 `bash -n docker/analyst-bot/entrypoint.sh` 통과

## 3. Docker 컨테이너화

Docker 전환의 이유는 편의가 아니라 격리다. 현재 세션 도구에는 `Read`, `Grep`, `Glob`이 포함되어 있고,
실측 결과 `Read`는 호스트의 임의 절대경로를 읽을 수 있었다. 디렉터리 단위 deny 규칙으로는 막히지 않았고,
파일명 글롭만 일부 동작했다.

따라서 실제 경계는 Claude permission list가 아니라 컨테이너 마운트다. 컨테이너 안에는 필요한 레포 조각과
좁은 권한의 BigQuery 키 하나만 넣는다.

컨테이너 설계:

- `analyst` uid 10001 비root 사용자로 실행
- `/app`에는 필요한 경로만 읽기 전용으로 mount
- `/app/secrets`, `/app/gcp-key.json`은 없음
- BigQuery read-only 키는 `/secrets/analyst-bq-key.json`으로 별도 read-only mount
- ad-hoc SQL 쓰기는 `dbt/gharchive_metrics/analyses/`만 허용
- dbt `target/`, Claude transcript, gcloud config, audit state, uv cache는 named volume 사용
- `gcloud auth activate-service-account`를 entrypoint에서 매번 수행
- Docker 기본은 headless지만 `ANALYST_LOG_LEVEL=INFO`라 `docker compose logs`에 진행 로그가 남음

검증한 것:

- `docker compose build analyst-bot` 통과
- 더미 Anthropic env로 `docker compose run --rm analyst-bot --dry-run` 통과
- dry-run에서 `repo_dir=/app`, `state_dir=/home/analyst/state`,
  `gcloud_config=/home/analyst/.config/gcloud`, `bq_identity=bda-analyst-ro@...`,
  `bq_scan_ceiling=10 GiB via .bigqueryrc` 확인
- 컨테이너 안에서 `/app/secrets`와 `/app/gcp-key.json`이 없는 것 확인
- `/app` root는 쓰기 불가, state/gcloud/analyses/target만 쓰기 가능 확인
- 컨테이너 안에서 `bq query ... "select session_user()"` 실행 시 `bda-analyst-ro@...` 확인
- 컨테이너 안에서 `dbt compile --select metrics_daily` 통과
- compile된 SQL을 `bq --dry_run`으로 검증해 bq CLI 경로가 동작하는 것 확인
- 컨테이너 안 Claude OAuth 로그인 성공. credential은 `analyst-claude` named volume에 저장됨
- 실제 OAuth credential로 `docker compose run --rm analyst-bot --dry-run` 통과
- `docker compose run --rm analyst-bot --self-test` 통과. session id가 두 턴에서 동일했고 `--resume`이 동작함
- `docker compose up -d analyst-bot` 후 headless Socket Mode 연결 확인. 로그에 `Bolt app is running!` 표시
- Slack end-to-end canary 1회 통과. 컨테이너가 실제 멘션을 받아 BigQuery 조회 후 답변을 게시했다.
  다만 답변이 `active actor`, `actor.id`, `UTC`, 토요일 비교표까지 포함해 너무 기술적으로 읽혔고,
  그 결과를 보고 용어 치환과 `간단히` 답변 규칙을 보강했다.

남은 것:

- 톤/용어 보강 후 Slack end-to-end canary 재확인
- `METABASE_API_KEY` 추가 후 Metabase 카드 생성 canary
- BigQuery hard quota 콘솔 설정

Claude 인증은 호스트 로그인이 자동으로 넘어오지 않는다. 호스트 `~/.claude`나 macOS Keychain을
마운트하면 컨테이너 격리 목적이 깨지기 때문에, 컨테이너 안에서 한 번 OAuth 로그인하는 방식을 쓴다.

```bash
docker compose run --rm -it --entrypoint claude analyst-bot auth login
```

로그인 결과는 `analyst-claude` named volume에 저장되고, 이후 컨테이너 재시작에도 유지된다.

## 4. 현재 판단

현재 상태는 “컨테이너 headless 실행과 첫 Slack canary는 통과, 말투 보강 후 재확인만 남음”이다.
Slack 수신, BigQuery 조회, 마운트, 로깅, Claude OAuth, self-test는 확인했다.
막혀 있던 것은 권한 설계 문제가 아니라 컨테이너 내부 Claude 인증 절차였고, 지금은 해소됐다.

다음 루프:

```bash
docker compose logs -f analyst-bot
```

Slack에서 짧은 mart 질문을 던져서 최종 확인한다. 기준은 단순하다:
Slack에는 결론과 한계만 보이고, dry run/검증/재현/비용 상세는 로그에만 있어야 한다.
