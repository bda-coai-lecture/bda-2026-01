# 분석 자동화 핸드오프

작성: 2026-07-26

Slack 스레드에서 받은 데이터 질문을 GitHub Archive/BigQuery로 분석해 답하는 봇과,
그 봇이 따르는 분석 절차를 구축한 기록이다. 이 문서만 읽고 이어받을 수 있게 썼다.

## 1. 지금 당장 돌리는 방법

```bash
cd /Users/kakao/bda-2
set -a && source ./.env && set +a
uv run --with slack-bolt --with google-cloud-bigquery python scripts/slack_analyst_bot.py
```

`⚡️ Bolt app is running!` 이 뜨면 붙은 것이다. 채널에서 `@<봇> 질문` 으로 호출한다.
**후속 질문도 멘션이 필요하다.** 멘션 없는 스레드 답글은 무시된다(비용 방어).

붙기 전에 설정을 점검만 하려면:

```bash
uv run --with slack-bolt --with google-cloud-bigquery python scripts/slack_analyst_bot.py --dry-run   # 설정 출력 후 종료
uv run --with slack-bolt --with google-cloud-bigquery python scripts/slack_analyst_bot.py --self-test # 세션 생성→재개 검증
```

주요 플래그: `--channel-allowlist`(쉼표 구분 채널 ID, 비우면 전체) ·
`--max-budget-usd`(기본 5, **달러 상한이 아니다** — 아래 2절 끝의 비용 구조 참조. 턴당 작업량 상한으로만 유효) ·
`--max-scan-gib`(**기본 10**, 쿼리당 BigQuery 스캔 상한. `.bigqueryrc`로 적용된다 —
환경변수만으로는 `bq`에 안 먹는다, 6절. 초과하면 세션이 멈추고 운영자가 이 값을 올려 재기동하는 것이 승인 절차다) ·
`--timeout`(기본 900초) · `--headless`(로그 최소화, **현재 비권장** — 아래 5절)

## 2. 검증된 현재 상태

전부 2026-07-26에 실제로 실행해 확인한 것이다. 추측 없음.

| 항목 | 상태 | 근거 |
|---|---|---|
| Socket Mode 연결 | 성립 | `⚡️ Bolt app is running!`, `bot_user_id=U0B74PR3478` |
| Slack 봇 스코프 | 적용 완료 | `app_mentions:read`, `channels:history`, `groups:history`, `im:history`, `mpim:history`, `chat:write`, `chat:write.public`, `chat:write.customize`, `reactions:write`, `files:write`, `channels:read`, `users:read` |
| **Slack 이벤트 수신** | **성립** | 매니페스트를 `apps.manifest.export`로 읽었더니 `settings.event_subscriptions` 블록 자체가 없고 `socket_mode_enabled: false`였다. `apps.manifest.update`로 둘 다 수정 → 멘션 이벤트 수신 시작 |
| 알림용 토큰 회전 | **일어나지 않음** | 재설치 후 `SLACK_BOT_TOKEN` `auth.test` → `ok: true`. 매니페스트 수정도 `permissions_updated:false`(스코프 무변화)라 재설치·회전 없었고 양쪽 `auth.test` `ok:true` |
| BigQuery read-only (Python 클라이언트) | 6/6 PASS | `scripts/verify_bq_readonly.py`. 단 그중 `cost_ceiling_bites`는 **Python 클라이언트 경로만** 검증한다 — 세션은 `bq`를 쓴다(6절) |
| 쿼리당 스캔 상한 (`bq` 경로) | **성립** | `secrets/gcloud-analyst/.bigqueryrc`의 `[query] --maximum_bytes_billed`. 기동할 때마다 다시 쓰고 `child_env()`가 `BIGQUERYRC`를 주입. preflight에 `bq_scan_ceiling = 10 GiB via .bigqueryrc` |
| 상한 초과 시 승인 게이트 | 의도대로 정지 | 제약 세션에서 dry run 17.17 GiB가 나오자 **실행하지 않고** "승인 필요: 예상 17.17 GiB (상한 10 GiB)"로 멈췄다. 기간을 쪼개 우회하지도 않았다. 실제 과금 0 GiB |
| BigQuery read-only (`bq` 경로) | **성립** | 세션 안에서 `select session_user()` → `bda-analyst-ro@`. `CLOUDSDK_CONFIG` 주입 후 |
| preflight 신원 확인 | 통과 | 기동 로그에 `bq_identity = bda-analyst-ro@bda-coai.iam.gserviceaccount.com` |
| 세션 내 쓰기 차단 | 의도대로 실패 | `create schema` → `bigquery.datasets.create denied` |
| 자격증명 재지정 차단 | 차단됨 | `export GCP_KEY_PATH=...` → deny |
| 도구 허용 목록 (`bq query`) | 통과 | 스킬과 references 3개의 호출을 `bq query --project_id=X` 형태로 교체한 뒤. 교체 전에는 전부 차단됐다(6절) |
| 세션 생성→재개 | PASS | `--self-test`, session_id 동일 + 이전 턴 기억 |
| dbt compile 호환 | 정상 | read-only 키로 compile + dry run 성공 |
| dbt run 차단 | 의도대로 실패 | `Permission bigquery.tables.create denied` |
| **실전 분석 턴 완주** | **성공** | 24턴. dry run → 본쿼리 → 재현용 SQL 2개 작성까지 진행. 답변은 raw↔mart 41일 전수 대조로 diff 0 확인, 급감 시작일이 07-06이 아니라 **07-09**임을 정정, 06-18~06-24 저빈도 6개 type 결손을 추가로 찾아냈다 |
| 사용량/실지출 분리 푸터 | 구현 완료 | 턴 종료 시 `Claude 24턴 · 96초 · 환산 $0.94 (구독, 청구 없음)  \|  BigQuery 4.39 GiB · $0.03 (실지출, 24건)` |
| 진행 추적 한 줄 요약 | 구현 완료 | 도구 40줄짜리 진행 메시지를 턴 종료 시 위 푸터 한 줄로 덮어쓴다 |

수정 전 첫 실전 턴은 쿼리 4건이 전부 차단돼 봇이 기존 리포트만 읽고 답했다(18턴). 위 24턴은 그 뒤 재실행이다.

**아직 미검증**: 채점 문제집 8문제(`docs/analysis_testset.md`) 통과 여부, 여러 사람이 동시에 쓰는 상황,
응답 시간·재질문율 같은 효과 지표. 전부 측정 전이다.

### 비용 구조 — 두 종류의 돈을 섞지 말 것

봇은 `claude -p` 로컬 CLI를 띄우고, 이는 **운영자 Max 구독으로 돈다.**

- 로그의 `cost=$0.94` 같은 값은 **API 환산 추정치이지 실제 청구가 아니다.**
  같은 로그에 `rate_limit_event, rateLimitType:"five_hour"`가 찍힌다 —
  과금이 아니라 **5시간 창 사용량**으로 계산된다는 뜻이다.
- **실지출은 BigQuery 스캔뿐이다.** 24턴 실전 턴 기준 **4.39 GiB ≈ $0.03**.
- 따라서 `--max-budget-usd`는 달러 상한이 아니라 **턴당 작업량 상한**으로만 유효하다.

둘을 합산하면 실지출을 **30배쯤 과대평가**한다. 리포트나 보고에 숫자를 옮길 때 반드시 분리해서 적을 것.

턴이 끝나면 푸터가 둘을 나눠 찍는다. BigQuery 쪽 숫자는 추정이 아니라 **실측**이다 —
봇 프로세스가 **운영자 키로** `INFORMATION_SCHEMA.JOBS`를 조회해 해당 턴 구간의
`total_bytes_billed`를 합산한다. 세션 계정은 `bigquery.jobs.list`가 없어 자기 지출을 못 읽는다.
**집계는 실행 주체가 아니라 권한 있는 쪽에서 한다** — 경계를 좁게 잡았으면 계측은 밖에서 해야 한다.

## 3. 자격증명 인벤토리

| 변수 / 파일 | 용도 | 주의 |
|---|---|---|
| `SLACK_ANALYST_BOT_TOKEN` (.env) | 분석 봇 `xoxb-` | 알림 앱과 **같은 앱**(bot_id `B0B76RVQA66`) |
| `SLACK_ANALYST_APP_TOKEN` (.env) | Socket Mode `xapp-` | `connections:write`. app_id `A0B6RBYRZ4P` |
| `SLACK_BOT_TOKEN` (.env) | Airflow 알림 | 같은 앱이라 위 스코프를 **함께 얻었다** — 아래 경고 |
| `secrets/analyst-bq-key.json` | 봇 BigQuery | `bda-analyst-ro@bda-coai...`, mode 600, gitignore |
| `secrets/gcloud-analyst/` | `bq` 경로 전용 gcloud 설정 | 읽기 전용 SA만 활성화. `child_env()`가 `CLOUDSDK_CONFIG`로 **강제 주입**. 재생성 명령은 6절 |
| `gcp-key.json` (심볼릭 링크) | 운영자·Airflow | `dane-gcp@...`, **`roles/owner` 보유**. 봇은 쓰지 않는다 |
| Anthropic | 호스트 로그인 | `dane@clobe.com`, Max 구독 |

**경고 — 앱을 공유한 대가.** 알림용 `SLACK_BOT_TOKEN`이 분석 봇 스코프를 전부 물려받았다.
원래 `chat:write,incoming-webhook`만 있던 토큰이 지금은 채널 히스토리 읽기와
파일 업로드까지 할 수 있다. Airflow 컨테이너에 들어가는 토큰의 피해 범위가 커졌다.
분리하려면 별도 앱을 만들어야 한다(`config/slack_analyst_app_manifest.yaml`에 절차 있음).

**경고 — 노출된 토큰.** 구축 과정에서 `xoxb-`와 `xapp-` 토큰이 작업 기록에 평문으로 남았다.
운영 투입 전에 양쪽 다 회전시키는 것을 권한다. 회전하면 `.env` 갱신 + Airflow 재시작이 필요하다.

## 4. 구조

상세 설계는 `docs/analyst_bot_design.html`에 있다. 요약만.

```
Slack 멘션 → 스레드 키 (channel, thread_ts)
           → session_id = uuid5("slack://team/channel/thread_ts")   ← 결정론적
           → 턴 1: claude -p --session-id / 턴 2+: --resume         ← cwd 고정 필수
           → analysis 스킬 (모드 7종 → 인풋 9종 → 건강성 → dry run → 분석)
           → 진행 추적(chat.update 1.5초 스로틀) + 최종 답변 게시
```

**절대 건드리면 안 되는 세 가지.** 전부 실측으로 확인한 함정이다.

1. **`--session-id`는 생성 전용.** 재사용하면 `already in use`로 exit 1. 2턴부터 `--resume`.
2. **`--resume`은 cwd에 묶인다.** 다른 디렉터리에서 부르면 세션을 못 찾는다.
3. **같은 세션 동시 실행은 에러 없이 갈라진다.** transcript가 분기되고 한쪽 턴이 유실된다.
   그래서 `(channel, thread_ts)`별 락이 있다. Bolt 기본 `concurrency=10`이라 방치하면 반드시 발생.

### 파일 지도

| 경로 | 역할 |
|---|---|
| `scripts/slack_analyst_bot.py` | 러너 (약 1,000줄) |
| `.claude/skills/analysis/` | 분석 절차 정본 (SKILL.md + references 5종) |
| `docs/analysis_workflow_review.md` | 외부 워크플로우 적용 판정 근거 |
| `docs/analysis_testset.md` | 채점 문제집 8개 + 실측 기준선 |
| `docs/analyst_bot_testcases.md` | 봇 러너 TC — Slack·세션·권한·비용·출력 계약 |
| `docs/analyst_bot_design.html` | 설계 문서 |
| `docs/analyst_bot_overview.html` | 강의용 개요 (아키텍처 · 절차 · 가치 · 난점) |
| `docs/analyst_bq_readonly.md` | BigQuery 권한 런북 |
| `scripts/verify_bq_readonly.py` | read-only 검증 (쓰기가 실패해야 PASS) |
| `config/slack_analyst_app_manifest.yaml` | Slack 앱 매니페스트 (스코프 일괄 적용용) |
| `docker/analyst-bot/`, `docs/analyst_bot_docker.md` | **현재 미사용.** 격리 필요해질 때 |
| `reports/20260726_*.md` | 이번에 수행한 분석 2건 |
| `dbt/gharchive_metrics/analyses/non_push_drop_*.sql` | 실전 턴이 남긴 재현용 SQL 2건 (raw 일별 시계열 · raw↔mart 대조) |

## 5. 미해결 — 우선순위 순

번호는 이번에 새로 드러난 1·2·6번이 끼어들면서 밀렸다. 다른 문서의 옛 번호와 대조하지 말 것.

### 높음

**1. 답변에 메타 서문이 유출된다.** 최종 텍스트 앞에 "아래가 Slack에 게시될 답변입니다"와 `---`가
붙은 채로 그대로 게시됐다. 또 서문에 `**분석 완료.**`처럼 **Slack mrkdwn이 아닌 마크다운**을 썼다.
게시 직전 단계에서 서문·구분선을 잘라내거나, 스킬이 최종 블록만 내놓도록 형식을 못박아야 한다.

**2. 답변이 너무 길다.** 인풋 9종과 건강성 10항목을 전부 펼쳐 스레드가 벽이 된다.
절차를 지켰다는 증거는 남기되 스레드에는 요약만 올리고 상세는 접거나 리포트로 넘기는 형태가 필요하다.

**3. Read가 레포 밖을 읽을 수 있다.** 운영자 결정으로 Read/Grep/Glob을 복구했고, 그 대가다.
실측: `Read(/etc/**)`, `Read(//etc/**)`, `Read(**/hosts)`, `Read(/etc/*)` **네 형태 모두 차단 실패**.
디렉터리 단위 deny는 Read에 적용되지 않는다. **파일명 글롭만 유효**하므로
`Read(**/gcp-key.json)`, `Read(**/id_rsa*)` 등 65개를 이름 기준으로 깔아뒀다.
이름이 안 걸리는 파일은 읽히고, 답변은 Slack에 게시된다.
프롬프트로도 막고 있지만 그건 소프트 방어다(`/etc/hosts` 프로브에서 `denials=0`, 모델이 스스로 거절).
**진짜로 닫으려면 컨테이너 실행이 필요하다** — `docker/analyst-bot/`이 빌드까지 끝나 있다.

**4. 동시 세션 상한이 없다.** 이벤트마다 스레드를 띄우므로 질문 N개 = 사용량 N배
(달러가 아니라 5시간 창 사용량이다 — 2절 비용 구조). `ThreadPoolExecutor(max_workers=3)` 정도로 묶어야 한다.

**5. 재시작하면 진행 중 스레드 소유권을 잃는다.** 소유권이 메모리 dict(`_locks`)에만 있다.
transcript 파일 존재 여부로 판정하도록 바꿔야 한다. 현재는 멘션이 필수라 체감 영향은 작다.

**6. 스캔 상한이 하드 상한이 아니다 — 프로젝트 수준 할당량이 미설정이다.**
`.bigqueryrc`는 **기본값**을 정할 뿐 상한이 아니다. 세션이 명령줄에 `--maximum_bytes_billed`를
직접 붙이면 그쪽이 이긴다. 도구 권한 deny는 **명령 접두만** 보므로 명령 중간의 플래그를 못 잡는다.
현재 남은 방어는 (a) 스킬의 금지 규칙, (b) 턴 종료 시 실제 과금 바이트 보고 —
둘 다 우회를 **막지는 못하고 사후에 드러나게만** 한다.
**진짜로 airtight한 것은 프로젝트 수준의 "사용자당 일일 쿼리 사용량" 할당량뿐이고,
이건 클라우드 콘솔에서 설정해야 한다. 아직 미설정이다.**

### 중간

**7. 같은 스레드 연속 질문의 응답 순서가 뒤집힐 수 있다.** `threading.Lock`은 FIFO가 아니다.
스레드별 큐 + 전용 소비자 스레드로 교체 필요.

**8. `--headless`가 로그를 다 끈다.** 루트 레벨을 `WARNING`으로 낮추고 턴별 트레이스도 막는다.
헤드리스로 갈 때 `--log-level`을 먼저 추가해야 한다. 지금은 attached 모드로 쓸 것.

**9. `_locks` 무한 증가.** 스레드 키마다 락이 쌓이고 제거되지 않는다. 5번을 먼저 고쳐야 한다.

**10. `bq query`의 DDL/DML은 도구 권한으로 못 막는다.** IAM으로 해결했지만,
`Bash(uv run ... dbt:*)`가 `compile`과 `build`를 구분 못 하는 구조 자체는 남아 있다.
같은 한계가 6번(명령 중간 플래그)에서 다시 나타난다 — **접두 매칭은 인자를 못 본다.**

### 데이터 쪽 후속 과제

**11. `dim_push_automation_actor`가 자동화를 거의 못 잡는다.**
등재 20,992명 중 `explicit_bot`(로그인이 `[bot]`으로 끝남)이 20,706명(98.6%),
`machine_rate_suspect`는 **305명(1.5%)**. 분당 100건 문턱이 너무 높아 거의 발화하지 않는다.
사실상 "자기 이름에 `[bot]`을 붙인 계정 목록"이다.
→ **일당 기준 velocity로 재설계 필요.** 이것 때문에 리포트 결론 하나를 철회했다(12번).

**12. 요일별 역전의 원인은 미규명.** `reports/20260726_weekday_push_divergence.md`.
주말에 actor는 26% 줄고 event는 8.7% 늘며 1인당 event가 47% 오른다(raw 대조로 확인된 사실).
최초에 "자동화 봇 가설 반증"이라 결론했으나 11번 때문에 **철회**했다.
검정 도구가 무효였으므로 원인은 여전히 열려 있다.

**13. non-Push 이벤트 급감 — 시작일은 07-06이 아니라 07-09.**
`reports/20260726_actor_decline_diagnosis.md`(리포트는 히스토리라 07-06 서술이 남아 있다).
실전 턴에서 41일 전수 대조로 **시작일을 07-09로 정정**했고, 06-18~06-24 구간의
저빈도 6개 type 결손을 추가로 찾았다.
우리 파이프라인은 무혐의다 — raw↔mart 41일 전수 대조에서 diff 0.
손실이 `githubarchive.day.*` 안에 이미 존재한다. PushEvent는 반대로 행당 event가
3.57→6.90으로 뛰는데 그 패킹도 raw에 이미 있다. 상류(GH Archive 수집) 손실이 1순위 의심이나
우리 데이터로는 증명 불가.
→ **`total_events` 단독 모니터링을 폐기하고 Push/non-Push를 분리**해야 한다.
15주간 합계가 3.44~3.98M 밴드에 머물면서 양쪽 움직임을 다 가렸다.

**14. mart 커버리지가 fact보다 짧다.** `metrics_daily` 등은 2025-09-01부터, fact는 2025-05-01부터.
백필 경계이지 버그가 아니다. 다만 상시 존재하므로 `mart 0행 = 데이터 없음` 추론은 계속 틀린다.
건강성 검증표에 항목으로 넣어뒀다.

## 6. 시간을 잡아먹은 함정들

다시 밟지 않도록.

- **`dbt`는 전역 설치가 없다.** `uv run --no-project --with dbt-bigquery dbt ...`
- **`GCP_KEY_PATH`가 없으면 dbt가 파싱 단계에서 죽는다.** `Env var required but not provided`.
  봇은 자식 프로세스에 이걸 주입한다(`child_env`).
- **`bq`는 `GOOGLE_APPLICATION_CREDENTIALS`를 안 읽는다.** gcloud 자격증명 저장소를 쓴다.
  2026-07-26 실측: 읽기 전용 키를 export하고 `select session_user()`를 돌렸더니
  **운영자 개인 계정이 나왔다.** 즉 `bq` 경로에는 읽기 전용 경계가 아예 없었다.
  → 해결: `secrets/gcloud-analyst/`에 읽기 전용 SA만 활성화해 두고
  `child_env()`가 `CLOUDSDK_CONFIG`를 **강제 주입**한다(setdefault 아님).
  preflight가 활성 계정이 `bda-analyst-ro@` 하나인지 확인하고 아니면 기동을 막는다.
  설정 디렉터리를 다시 만들려면:
  `CLOUDSDK_CONFIG=secrets/gcloud-analyst gcloud auth activate-service-account --key-file=secrets/analyst-bq-key.json`
- **도구 허용 패턴은 접두 매칭이라 서브커맨드가 먼저 와야 한다.**
  `Bash(bq query:*)`는 `bq query --project_id=X`에 걸리지만
  `bq --project_id=X query`에는 **안 걸린다.** 스킬 문서가 후자를 쓰고 있어서
  첫 실전 턴의 쿼리가 전부 차단됐다(4회). 스킬의 `bq` 호출을 전부 전자로 고쳤다.
- **스킬 0단계가 운영자 키를 export하라고 지시하고 있었다.** `gcp-key.json`은 `roles/owner`다.
  봇이 읽기 전용 키를 주입해도 세션이 첫 명령으로 갈아탔다. 지금은 "이미 주입돼 있으니
  다시 export하지 말라"로 바뀌었고, `export GCP_KEY_PATH` / `GOOGLE_APPLICATION_CREDENTIALS` /
  `CLOUDSDK_CONFIG`는 deny에 올렸다. **`reports/`의 재현 방법에 아직 옛 export가 남아 있다** —
  세션이 그 리포트를 읽으므로 재감염 경로다. deny가 그걸 막는다.
- **`--allowedTools`는 `manual` 모드에서만 구속력이 있다.** `auto`/`dontAsk`에서는
  거부가 기록된 뒤 재시도가 자동 승인된다. `--disallowedTools`는 모든 모드에서 하드 거부.
- **`--allowedTools` 등 variadic 플래그는 positional 프롬프트를 삼킨다.** 프롬프트는 stdin으로.
- **`stream-json`은 `--verbose`가 필수다.** 없으면 exit 1.
- **`uv run python -c`는 모든 deny를 무력화한다.** deny는 Bash 명령 문자열만 본다.
  인터프리터는 금지목록에 있다. 넓히지 말 것.
- **리텐션 계열 mart가 정의가 다른 채로 5개 공존한다.** 같은 주에 W1이 23.8% vs 45.6%.
  `metrics_user_retention_weekly`는 `metrics_user_lifecycle_weekly`의 `select *` 별칭이다.
- **`roles/bigquery.user`는 읽기 전용이 아니다.** `datasets.create`를 포함해
  봇이 만든 데이터셋의 OWNER가 된다. `jobUser`가 맞는 role이다.
- **`gcloud`는 `GOOGLE_APPLICATION_CREDENTIALS`를 무시한다.** 별도로 인증된 계정을 쓴다.
- **`BIGQUERY_MAXIMUM_BYTES_BILLED`는 `bq`에 안 먹는다.** 그 환경변수는 **Python 클라이언트만** 읽고
  `bq` CLI는 완전히 무시한다. 2026-07-26 실측: 10 GiB 상한을 env로 걸고 dry run 18.4 GB짜리
  raw 쿼리를 돌렸더니 **그대로 완주했다**(exit 0, 결과 반환).
  `verify_bq_readonly.py`의 `cost_ceiling_bites`가 PASS였던 것은 그 스크립트가 Python 클라이언트를
  쓰기 때문이고, **세션은 `bq`를 쓴다.** 즉 **세션의 스캔 상한은 처음부터 작동한 적이 없었다** —
  200 GiB든 10 GiB든 무의미했다. 검증이 PASS인데 방어선은 없는, 가장 나쁜 조합이었다.
  → 해결: `bq`는 `--maximum_bytes_billed` **플래그는** 듣고, rc 파일 `[query]` 섹션에 적어두면
  세션이 플래그를 안 붙여도 적용된다. 봇이 기동할 때마다 `secrets/gcloud-analyst/.bigqueryrc`를
  현재 상한으로 다시 쓰고 `child_env()`가 `BIGQUERYRC`를 주입한다.
  `BIGQUERY_MAXIMUM_BYTES_BILLED`도 **계속 주입한다** — dbt는 Python 클라이언트를 쓰므로 그쪽엔 유효하다.
  **같은 상한을 두 경로에 걸려면 서로 다른 장치가 필요하다.** 한쪽만 걸어놓고 걸렸다고 믿기 쉽다.
  (완전하지 않다 — 5절 6번)
- **로그의 `cost=$…`를 청구액으로 읽지 말 것.** API 환산 추정치다. 실지출은 BigQuery 스캔뿐이고
  둘을 더하면 30배쯤 과대평가된다. 상세는 2절 끝 「비용 구조」.
- **세션 계정으로는 BigQuery 실지출을 못 읽는다.** `INFORMATION_SCHEMA.JOBS` 조회에
  `bigquery.jobs.list`가 필요한데 읽기 전용 SA에는 없다(실측 확인). 실지출 표시는
  봇 프로세스가 **운영자 키로** 따로 조회해 처리한다(2절, 구현 완료).

## 7. 다음에 할 일

첫 실전 턴, 스캔 상한(`.bigqueryrc`), 사용량/실지출 분리 푸터, 진행 추적 한 줄 요약까지는 끝났다(2절).
아래는 그 다음이다.

1. **프로젝트 수준 일일 쿼리 사용량 할당량을 건다(5절 6번).** 지금 스캔 상한은 rc 기본값이라
   세션이 플래그로 덮을 수 있다. 클라우드 콘솔에서 설정해야 하고, **이것만이 하드 상한이다.**
2. **답변 형식을 고친다(5절 1·2번).** 메타 서문·`---` 유출과 마크다운 혼용을 잘라내고, 스레드 길이를 줄인다.
   실전 턴에서 사용자에게 그대로 보인 문제다.
3. 테스트셋 8문제를 순서대로 통과시킨다(`docs/analysis_testset.md`). 채점 기준이 함정 회피다.
4. 미해결 4·5번(동시 세션 상한, 스레드 소유권)을 고친다. 사람이 여럿 쓰기 시작하면 바로 문제가 된다.
5. 11번(자동화 판별 재설계)을 처리한다. 이게 12번의 선행 조건이다.
6. 13번 후속 — 급감 시작일이 07-09로 정정됐으므로 상류(GH Archive 수집) 쪽 확인은 그 날짜 기준으로 다시 잡는다.
7. 토큰 회전.
