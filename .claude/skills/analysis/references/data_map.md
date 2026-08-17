# 데이터 맵 — 무엇이 어디에 있고, 어디에 함정이 있나

숫자를 인용하기 전에 여기서 grain과 함정을 확인한다.
정본은 항상 모델 SQL이다. 이 표와 SQL이 다르면 SQL이 맞다.

## 0. 좌표

| 항목 | 값 |
|---|---|
| BigQuery project | `bda-coai` |
| dataset | `mart` |
| location | `US` |
| dbt project | `dbt/gharchive_metrics` |
| dbt profiles | `dbt/profiles` |
| 원천 | `githubarchive.day.20*` (공개, UTC 일자별 shard, 스캔 과금) |
| 보유 범위 | 2025-05-01 ~ 최근 (2026-07-26 기준 2026-07-25) |
| 적재 | Airflow `gharchive_dbt_metrics`, 매일 09:30 KST, 최근 3일 replace-days |

## 1. 계층

```
githubarchive.day.20*                     원천. _TABLE_SUFFIX 없이 절대 조회 금지
  └─ fact_user_repo_activity              dbt 소유 적재 경계 (incremental, insert_overwrite)
       └─ stg_user_repo_activity          null 제거 + week_start/month_start 파생 (view)
            └─ metrics_*                  대시보드용 mart (table)
  └─ int_push_automation_actor_day        raw 직접 스캔. 분당 velocity 증거
       └─ dim_push_automation_actor       자동화 actor 차원
```

`raw_githubarchive_events_90d` / `fact_user_repo_activity_from_raw_reference`는
lineage 확인용 reference view다. **분석 소스로 쓰지 않는다.** `var()` 범위대로 raw를 다시 스캔한다.

## 2. fact / staging

### `fact_user_repo_activity`

| 항목 | 값 |
|---|---|
| grain | `activity_date × user_id × repo_id × action` |
| 컬럼 | `activity_date`, `user_id`, `repo_id`, `action`, `event_count` |
| partition | `activity_date` (day) |
| cluster | `user_id`, `repo_id`, `action` |
| 적재 | `insert_overwrite`, `raw_start_date`~`raw_end_date` shard만 스캔 |

**함정**
- `user_id`는 `actor.id`다. 조직이 아니다. A가 조직 B에서 이벤트를 내면 활성 actor는 A다. `org`는 저장하지 않는다.
- `event_count`는 **이벤트 수**다. 커밋 수가 아니다. PushEvent 1건에 커밋이 여러 개일 수 있다.
- raw `count(*)` 와 비교할 대상은 fact row count가 아니라 `sum(event_count)`다.
- `payload`가 없다. PR이 열렸는지 머지됐는지, push에 커밋이 몇 개인지 fact로는 알 수 없다.
- 시각(hour)이 없다. `activity_date`뿐이다. 요일은 파생 가능, **시간대는 계측 공백**이다.

### `stg_user_repo_activity`

fact와 동일 grain에 `week_start`(월요일 시작), `month_start` 추가. null row 제거. view라 비용은 fact와 같다.

## 3. mart — 이름이 닮았고 정의가 다른 것들

**이 절이 이 문서의 핵심이다.** 리텐션 계열은 5개가 서로 다른 정의로 공존한다.

| 모델 | cohort/기준 | 실제 의미 | 언제 쓰나 |
|---|---|---|---|
| `metrics_retention_weekly` | `min(week_start)` per actor | **최초 관측 코호트 리텐션** (long) | "신규 유저가 남는가" |
| `metrics_retention_summary` | 위와 동일 | 같은 정의의 W0~W3 wide | 요약 표 |
| `metrics_cohort_retention_weekly_heatmap` | actor가 활동한 **모든** 주 | **활동주 롤링 코호트** (W1~W12 wide) | "이번 주 활동한 사람이 이후에도 오는가" |
| `metrics_user_lifecycle_weekly` | 직전 주 대비 | **직전 기간 라이프사이클** (active/existing/churned) | "주간 유지·이탈 회계" |
| `metrics_user_retention_weekly` | — | **`metrics_user_lifecycle_weekly`의 별칭.** `select *` 뿐 | 쓰지 말 것. 원본을 직접 참조 |

monthly 계열(`metrics_cohort_retention_monthly_heatmap`, `metrics_user_lifecycle_monthly`,
`metrics_user_retention_monthly`)도 같은 구도다.

**규칙: "리텐션"이라는 단어가 나오면 위 표에서 어느 정의인지 먼저 고르고, 답변에 모델명을 적는다.**

`metrics_user_lifecycle_weekly`의 `returning_or_historical_unknown_users` 컬럼명은 좌측 절단을 정직하게 드러낸 것이다.
직전 주에 없던 actor가 (a) 진짜 신규인지 (b) 보유 범위 이전부터 있던 복귀자인지 구분할 수 없다.

## 4. mart — 활동량

| 모델 | grain | 주요 컬럼 |
|---|---|---|
| `metrics_daily` | `activity_date` | `active_users`, `active_repos`, `total_events`, event type별 `*_events` |
| `metrics_event_type_daily` | `activity_date × action` | `active_users`, `active_repos`, `total_events` |
| `metrics_weekly` | `week_start` | `weekly_active_users`, `weekly_active_repos`, `total_events` |
| `metrics_user_segments` | `user_segment` | 보유 **전 구간** 누적 actor 세그먼트 |

**함정**
- `metrics_daily.active_users`는 actor 수다. 사람 수가 아니고 봇을 포함한다.
- `metrics_event_type_daily`의 `active_users`를 event type끼리 더하면 **중복 계산**이다. 한 actor가 여러 type을 낸다.
- `metrics_user_segments`는 기간 파라미터가 없다. fact 전 구간(2025-05-01~) 기준이라 기간별 비교에 못 쓴다.

## 5. mart — push 에피소드 (라이프사이클)

| 모델 | grain | 역할 |
|---|---|---|
| `fct_push_repo_episode` | `repo_id × episode_start_date` | 28일 무활동 규칙으로 분절한 repo push 에피소드 |
| `metrics_push_repo_episode_started_monthly` | `cohort_month` | 시작 코호트 월별 요약 |
| `metrics_push_repo_episode_started_30d_monthly` | `cohort_month` | 30일 고정창 생존 (우측 절단 통제판) |
| `metrics_push_repo_episode_churned_monthly` | `churn_month` | 이탈 확정 에피소드 월별 요약 |
| `dim_push_automation_actor` | `user_id` | 자동화 의심 actor. **아래 경고 필독** |
| `int_push_automation_actor_day` | `activity_date × user_id` | 분당 push/repo velocity 최대값 |

`fct_push_repo_episode` 핵심 컬럼:

- `is_churn_observable` — `last_push_date + 28일 <= data_through_date` 인가
- `is_provisional` — 위의 반대. 아직 이탈 판정 불가
- `is_entry_window_complete` — 진입 7일 창이 관측 범위 안에 들어왔나
- `entry_actor_count` / `lifetime_actor_count` — 진입주 / 생애 고유 actor 수
- `is_collaborative_at_entry` — `entry_actor_count >= 2`
- `normalized_entropy` — 주별 push 분포의 Shannon 엔트로피 / ln(주 수). 활동이 고르게 퍼졌는가
- `data_through_date` — **as-of 날짜.** 리포트에 반드시 인용

**함정**
- 좌측 절단 방어: `episode_start_date >= source_start_date + 28일` 인 에피소드만 남긴다. 2025-05-29 이전 시작은 없다.
- `is_provisional` 에피소드를 이탈률 분모에 넣으면 이탈률이 과소 추정된다.
- **자동화 actor를 제외하지 않는다.** `dim_push_automation_actor`가 있는데도 이 모델은 안 쓴다. 순수 사람 push를 원하면 직접 anti-join 해야 한다.

### ★ `dim_push_automation_actor`는 "봇 목록"이 아니다

2026-07-26 확인. 등재된 20,992명의 구성이 이렇다.

| 판정 근거 | 수 | 비중 |
|---|---|---|
| `explicit_bot` (로그인이 `[bot]`으로 끝남) | 20,706 | **98.6%** |
| `machine_rate_suspect` (분당 100 push 또는 100 repo 이상) | 305 | 1.5% |

**사실상 "자기 이름에 `[bot]`을 붙인 계정 목록"이다.** 속도 규칙은 분당 문턱이 너무 높아 거의 발화하지 않는다.

그 결과 **이 dim을 제외해도 "사람만"이 되지 않는다.** 같은 날 확인된 수치로,
하루 100건 이상 push하는 actor는 2,037명 → 10,146명으로 +398% 늘어 push event의 73%를 차지하는데
dim에 잡힌 것은 극히 일부다.

**규칙**
- "자동화 제외"라고 쓰지 말고 **"`[bot]` 접미사 계정 제외"**라고 쓴다.
- 이 dim을 근거로 "자동화가 원인이 아니다"라고 결론하지 않는다. 검정력이 없다.
- 진짜 자동화 통제가 필요하면 일당 기준 velocity를 직접 계산한다.
- `total_push_events`는 push 이벤트 수다. 커밋 수가 아니다.

## 6. 계측 공백 — fact로 답할 수 없는 것

| 질문 유형 | fact | raw로 가능? | 필요 필드 |
|---|---|---|---|
| 시간대(hour)별 활동 | 없음 | 가능 | `created_at` |
| 요일별 활동 | **가능** (`activity_date`에서 파생) | — | — |
| 커밋 수 | 없음 (event 수만) | 가능 | `payload` JSON의 `size` / `distinct_size` |
| PR 상태(open/merge/close) | 없음 (`type`만) | 가능 | `payload` JSON의 `action`, `merged` |
| repo 언어/스타 | 없음 | 불가 | GitHub REST API (`data/repo_metadata.db`) |
| 조직(org) 귀속 | 없음 | 가능 | `org` RECORD |
| actor 로그인명 | 없음 | 가능 | `actor.login` (`int_push_automation_actor_day`에 일부 있음) |
| 사람 vs 봇 | 부분 (`dim_push_automation_actor`) | 부분 | 완전한 판별은 불가 |

**"없다"로 끝내지 않는다.** `fact에는 없음 / raw에서 N GB 스캔하면 가능`까지 적는다.
raw payload는 STRING이므로 `json_value(payload, '$.size')`로 뽑는다. payload 컬럼은 크다. 반드시 날짜와 `type`을 좁히고 dry run한다.

## 7. 자주 쓰는 명령

**자격 증명은 봇/실행 환경이 이미 주입한다. 직접 export 하지 않는다.**
dbt profile은 `GCP_KEY_PATH`를 요구하지만, 값이 비어 있으면 환경 설정 문제로 보고하고 멈춘다.
특히 `gcp-key.json`은 운영자 키일 수 있으므로 그 경로로 덮어쓰지 않는다.

```bash
# lineage 검색
uv run --no-project --with dbt-bigquery dbt ls \
  --project-dir dbt/gharchive_metrics --profiles-dir dbt/profiles \
  --select "fqn:*retention*"

# 상하류 확인
uv run --no-project --with dbt-bigquery dbt ls \
  --project-dir dbt/gharchive_metrics --profiles-dir dbt/profiles \
  --select "+metrics_retention_weekly+"

# 렌더링 SQL 확인
uv run --no-project --with dbt-bigquery dbt compile \
  --project-dir dbt/gharchive_metrics --profiles-dir dbt/profiles \
  --select 모델명

# dry run (compile된 SQL을 쏜다)
bq query --project_id=bda-coai --use_legacy_sql=false --dry_run \
  < dbt/gharchive_metrics/target/compiled/gharchive_metrics/analyses/파일.sql

# 선택 빌드
uv run --no-project --with dbt-bigquery dbt build \
  --project-dir dbt/gharchive_metrics --profiles-dir dbt/profiles \
  --select "+모델명"

# 최근 스캔 비용 확인
uv run --no-project --with google-cloud-bigquery \
  python scripts/check_bigquery_cost_guard.py --project bda-coai --lookback-hours 2 --max-usd 3
```

`dbt`는 전역 설치되어 있지 않다. 항상 `uv run --no-project --with dbt-bigquery`로 실행한다.
