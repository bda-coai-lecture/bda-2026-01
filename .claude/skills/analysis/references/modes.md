# 모드별 실행 플레이북

질문을 하나의 모드로 분류하면 필요한 검증과 산출물이 정해진다.
모드를 두 개 이상 걸치면 **분해해서 순서대로** 처리한다. 섞지 않는다.

## 1. 활동량 분석

**질문 형태** — 얼마나 쓰였나 / 추세가 늘고 있나 / 어느 세그먼트가 많이 쓰나

**기본 소스** — `metrics_daily`, `metrics_event_type_daily`, `metrics_weekly`

**산출물**
- 일별 active actor 수 (라인)
- 일별 event 수 (라인)
- event type별 분해
- 데이터 건강성 표

**필수 확인**
- 최신 1~2일이 shard 미마감/지연 도착으로 낮게 나오지 않는가 → 잘라내거나 점선 처리
- actor 수(사람 아님)와 event 수를 섞어 말하지 않는가
- `metrics_event_type_daily.active_users`를 type끼리 더하지 않았는가 (중복)
- 자동화 actor 포함 여부를 적었는가

**금지**
- 누적 actor 수(stock)와 당일 신규(flow)를 같은 축에 그리기
- row 수를 actor 수처럼 말하기

## 2. 라이프사이클 단계 분석

원문의 "퍼널"을 대체한다. **전환율이라고 부르지 않는다.** 제품 퍼널이 아니다.

**질문 형태** — 어디서 떨어지나 / 얼마나 살아남나 / 협업으로 넘어가나

**기본 소스** — `fct_push_repo_episode`, `metrics_push_repo_episode_*`, `metrics_retention_weekly`

**repo 단계 구조**

```
1단계  에피소드 시작 (28일 무활동 후 첫 push)
2단계  진입주(7일) 협업 진입          is_collaborative_at_entry
3단계  30일 도달                      reached_day_30_share
4단계  28일 무활동 이탈 확정          is_churn_observable / churn_date
```

**필수 확인**
- `is_provisional` 에피소드를 이탈률 분모에 넣지 않았는가
- `is_entry_window_complete`가 false인 에피소드를 협업률 분모에서 뺐는가
- 좌측 절단: `episode_start_date >= 2025-05-29` 인 것만 존재한다
- **as-of 날짜(`data_through_date`)를 인용했는가** — 안 적은 이탈률은 무효
- **단계 보존: 뒤 단계의 unique 수가 앞 단계보다 크면 FAIL이다.** 발견이 아니라 정의 오류나 join fan-out이다. 원인을 찾기 전에 결과를 보고하지 않는다

**우측 절단이 걱정되면** `metrics_push_repo_episode_started_30d_monthly`를 쓴다.
30일 고정창이라 코호트 간 비교가 공정하다.

## 3. 전후 비교

**질문 형태** — 그 시점 이후로 달라졌나

**기본 소스** — `metrics_daily` + 요일 분해

**산출물**
- 변경 전 N일 vs 변경 후 N일 (같은 길이)
- 일별 추세선
- 요일별 분해 (GitHub 데이터는 주말 효과가 크다)
- baseline: 같은 지표의 작년 동기 또는 직전 동일 길이 구간

**필수 확인**
- 두 구간의 길이와 요일 구성이 같은가 (7의 배수로 잡는다)
- 계절성/요일 효과와 구분되는가
- 대상 밖 세그먼트도 같이 움직였는가 (같이 움직였으면 전역 요인)

**표현 규칙**
```
권장: 출시 후 14일 동안 일평균 active actor가 A에서 B로 관측됐습니다. 같은 기간 전체 actor도 C% 움직였습니다.
금지: 출시 때문에 늘었습니다.
```

**우리 데이터 특성** — GitHub Archive에는 "출시"가 없다. 전후 비교의 기준점은 대개
외부 사건(모델 출시, 정책 변경, 연휴)이고, 우리는 그 사건을 관측하지 못한다.
기준점의 출처를 반드시 적는다.

## 4. 세그먼트 비교

원문의 "AB 테스트 분석"을 대체한다. **우리 데이터에 variant 배정은 존재하지 않는다.**

**질문 형태** — A 집단과 B 집단의 패턴이 다른가

**기본 소스** — `stg_user_repo_activity`에서 actor 세그먼트 직접 구성, `metrics_user_segments`

**첫 문장에 반드시 쓸 것**
> 이건 무작위 배정 실험이 아니라 관측 세그먼트 비교입니다. 집단 간 차이를 처치 효과로 읽을 수 없습니다.

**필수 확인**
- 세그먼트 정의가 **결과 변수를 포함하지 않는가** (활동량으로 나눠놓고 활동량을 비교하면 동어반복)
- 세그먼트 배정 시점과 측정 시점이 분리되어 있는가 (배정 기간 / 측정 기간을 따로 적는다)
- 세그먼트 크기가 비교 가능한가. 한쪽이 극단적으로 작으면 방향성만 말한다
- 자동화 actor가 한쪽 세그먼트에 몰려 있지 않은가 — **push 다수 세그먼트에서 특히 위험**
- **몇 개를 훑었는가.** 지표·세그먼트·event type을 여러 개 보고 그중 움직인 것만 보고하지 않는다. 몇 개를 봤는지 적고, **사전에 고른 지표인지 결과를 본 뒤 고른 지표인지** 구분한다. 12개 event type을 훑어 1개가 튀는 것은 우연으로도 흔히 일어난다

**금지**
- 해시로 임의 그룹을 만들어 AB처럼 보고하기
- "유의하다" — 우리는 사전 가설도 표본 설계도 없다. 효과 크기와 표본 수만 말한다
- "차이 없다" / "효과 없다" — 표본이 작을 때는 **"판단 불가(검정력 부족)"**이 정확한 표현이다. 얼마나 더 필요한지 함께 적는다

## 5. 리텐션 / 코호트

**질문 형태** — 다시 돌아오나 / 신규가 남나

**기본 소스** — 정의를 먼저 고른다. `references/data_map.md` 3절 참조

| 원하는 것 | 모델 |
|---|---|
| 신규(최초 관측) actor가 남는가 | `metrics_retention_weekly` |
| 이번 주 활동자가 이후에도 오는가 | `metrics_cohort_retention_weekly_heatmap` |
| 주간 유지·이탈 회계 | `metrics_user_lifecycle_weekly` |

**필수 확인 — 좌측 절단**
보유 범위는 2025-05-01부터다. 그 이전부터 활동하던 actor가 첫 주에 전부 "신규"로 잡힌다.
**최초 시점 근처 코호트(최소 첫 4주)는 신규 코호트로 쓰지 않는다.**

**필수 확인 — 우측 절단**
W4 리텐션을 보려면 코호트 시작 후 5주가 지나 있어야 한다.
관측이 덜 된 코호트를 완전 관측 코호트와 같은 표에 넣지 않는다. 넣으려면 빈칸으로 둔다.

**W 정의 고정**
`week_start`는 `date_trunc(activity_date, week(monday))`. 월요일 시작이다.
`weeks_since = date_diff(week_start, cohort_week, week)`. W0는 코호트 주 자신이라 정의상 100%다.

## 6. 데이터 품질 진단

**질문 형태** — 이 수치를 믿어도 되나 / mart랑 raw가 왜 다르나

**절차**
1. 같은 기간·같은 필터로 fact와 raw를 각각 집계
2. raw `count(*)` vs fact `sum(event_count)` — **fact row count와 비교하지 않는다**
3. 다르면 순서대로 추적: 기간 경계 → null 제외(`actor.id`/`repo.id`/`type`) → 집계 grain → 지연 도착 → 적재 창(최근 3일 replace-days)

```sql
-- 1일 정합성 확인. raw는 반드시 _TABLE_SUFFIX로 좁힌다
select 'raw' as src, count(*) as events, count(distinct actor.id) as actors
from `githubarchive.day.20*`
where concat('20', _table_suffix) between '20260701' and '20260701'
  and actor.id is not null and repo.id is not null and type is not null
union all
select 'fact', sum(event_count), count(distinct user_id)
from `bda-coai.mart.fact_user_repo_activity`
where activity_date = '2026-07-01'
```

**필수 확인**
- 적재 창이 최근 3일이므로 그보다 오래된 지연 도착은 fact에 반영되지 않는다
- fact는 `insert_overwrite`다. 재적재 범위 밖의 과거 파티션은 그대로 남는다

## 7. 계측 공백 판정

**질문 형태** — 애초에 이 데이터가 있나

**절차**
1. `references/data_map.md` 6절에서 확인
2. fact에 있으면 → 해당 모드로 넘어간다
3. fact에 없고 raw에 있으면 → **비용 견적을 먼저 낸다**
   - 필요한 최소 날짜 범위와 `type` 필터를 정한다
   - dry run으로 bytes를 잰다
   - 견적을 사용자에게 보고하고 승인을 받은 뒤 실행한다
4. raw에도 없으면 → 없다고 말하고, 무엇을 계측해야 답할 수 있는지 적는다

**보고 형식**
```
fact에는 없습니다. githubarchive.day의 payload에서 뽑을 수 있습니다.
2026-07-01~2026-07-07, type='PushEvent'로 좁히면 dry run 기준 N GB (약 $X)입니다.
진행할까요?
```

`payload`는 STRING이고 크다. 날짜와 `type`을 좁히지 않은 payload 스캔은 실행하지 않는다.
