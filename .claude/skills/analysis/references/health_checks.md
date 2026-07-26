# 건강성 검증

본 분석 범위로 확장하기 **전에** 1일(필요하면 7일) 범위에서 채운다.
`PASS` / `WARN` / `FAIL`을 붙이고, WARN 이상이 하나라도 있으면 결론의 강도를 낮춘다.

## 검증표 (리포트에 그대로 붙여넣는다)

| 검증 | 확인 질문 | 결과 | 근거 |
|---|---|---|---|
| shard 마감 | 최신 `activity_date`가 완결된 UTC 일자인가 | | |
| **mart 커버리지** | **분석 기간이 mart에 실제로 담겨 있나 (fact보다 짧을 수 있다)** | | |
| 적재 지연 | 최근 3일 replace-days 창 밖의 지연 도착이 있나 | | |
| 좌측 절단 | 2025-05-01 이전 이력 부재가 결론에 영향을 주나 | | |
| 우측 절단 | 28일 이탈창 / N주 코호트가 미완성인 구간이 섞였나 | | |
| 자동화 혼입 | `dim_push_automation_actor` 제외/포함을 결정하고 적었나 | | |
| grain 중복 | 선언한 grain에서 실제로 unique한가 | | |
| 단계 보존 | (단계 분석 시) 뒤 단계 unique 수 ≤ 앞 단계 unique 수인가 | | |
| 합계 보존 | 필터·조인 전후 `sum(event_count)` 변화가 설명되나 | | |
| 분모 정의 | 분모에서 제외한 row가 의도한 것뿐인가 | | |
| raw 정합 | (필요 시) raw `count(*)` vs fact `sum(event_count)` | | |
| 스캔 예산 | dry run bytes가 예산 안인가 | | |

## 쿼리 모음

raw·fact 쿼리는 날짜를 좁혀 놓았다. 날짜만 바꿔 쓴다.
mart 쿼리는 전체 스캔이지만 mart가 작아서 문제되지 않는다.

### shard 마감 + 최근 추세 급락 확인

최근 7일이 갑자기 낮으면 실제 감소가 아니라 미마감일 가능성이 높다.

```sql
select activity_date, active_users, total_events
from `bda-coai.mart.metrics_daily`
order by activity_date desc
limit 10
```

`metrics_daily`는 mart table이라 스캔이 작다. 이 확인에 fact를 직접 스캔하지 않는다.

### mart 커버리지 = fact 커버리지인가 ★

**mart가 fact보다 짧은 기간만 담고 있을 수 있다.** 2026-07-26 확인 시점에
`metrics_daily`는 2025-09-01부터인데 fact는 2025-05-01부터였다. 2025-06-15를 조회하면
mart는 0행, fact는 1,148,747행이었다. mart를 믿고 "그 기간은 데이터가 없다"고 답하면 틀린다.

```sql
select 'metrics_daily' as m, cast(min(activity_date) as string) as min_d, cast(max(activity_date) as string) as max_d
from `bda-coai.mart.metrics_daily`
union all
select 'fact', cast(min(activity_date) as string), cast(max(activity_date) as string)
from `bda-coai.mart.fact_user_repo_activity`
```

분석 기간이 mart 커버리지 밖이면 fact로 내려가고, mart 갱신이 필요하다는 사실을 리포트에 적는다.

### grain 중복

```sql
select count(*) as rows_, count(distinct format('%t|%t|%t|%t',
         activity_date, user_id, repo_id, action)) as distinct_grain
from `bda-coai.mart.fact_user_repo_activity`
where activity_date = '2026-07-01'
```

`rows_ = distinct_grain` 이어야 한다.

### null / 범위

```sql
select
  count(*) as rows_,
  countif(user_id is null) as null_user,
  countif(repo_id is null) as null_repo,
  countif(action is null) as null_action,
  countif(event_count < 1) as bad_event_count,
  min(activity_date) as min_d,
  max(activity_date) as max_d
from `bda-coai.mart.fact_user_repo_activity`
where activity_date between '2026-07-01' and '2026-07-07'
```

fact에는 `not_null` 테스트가 걸려 있어 보통 0이다. 0이 아니면 적재가 깨진 것이다.

### raw ↔ fact 정합 (1일)

```sql
select 'raw' as src, count(*) as events, count(distinct actor.id) as actors
from `githubarchive.day.20*`
where concat('20', _table_suffix) between '20260701' and '20260701'
  and actor.id is not null and repo.id is not null and type is not null
union all
select 'fact', sum(event_count), count(distinct user_id)
from `bda-coai.mart.fact_user_repo_activity`
where activity_date = '2026-07-01'
```

**raw `count(*)`와 비교할 대상은 fact `sum(event_count)`다.** row count가 아니다.

### 자동화 actor 영향 크기

제외 여부를 결정하기 전에 규모를 본다.

```sql
with push as (
  select user_id, sum(event_count) as push_events
  from `bda-coai.mart.fact_user_repo_activity`
  where activity_date between '2026-07-01' and '2026-07-07'
    and action = 'PushEvent'
  group by user_id
)
select
  countif(a.user_id is not null) as automation_actors,
  count(*) as all_actors,
  safe_divide(countif(a.user_id is not null), count(*)) as automation_actor_share,
  safe_divide(sum(if(a.user_id is not null, p.push_events, 0)), sum(p.push_events)) as automation_event_share
from push as p
left join `bda-coai.mart.dim_push_automation_actor` as a using (user_id)
```

actor 비중은 작은데 event 비중이 크면, actor 지표는 영향이 적고 event 지표는 크게 흔들린다.
둘을 나눠서 보고한다.

### 좌측 절단 확인

```sql
select cohort_week, cohort_users
from `bda-coai.mart.metrics_retention_weekly`
where weeks_since = 0
order by cohort_week
limit 6
```

첫 코호트가 비정상적으로 크면 그건 신규가 아니라 **기존 actor가 한꺼번에 잡힌 것**이다. 제외한다.

### 우측 절단 확인 (에피소드)

코호트별로 봐야 어느 구간을 빼야 하는지 알 수 있다. 전체 합계 하나로는 아무것도 못 고른다.

```sql
select
  cohort_month,
  max(data_through_date) as as_of_date,
  count(*) as episodes,
  countif(is_provisional) as provisional,
  round(safe_divide(countif(is_provisional), count(*)) * 100, 2) as provisional_pct
from `bda-coai.mart.fct_push_repo_episode`
where cohort_month >= date_sub(current_date(), interval 6 month)
group by cohort_month
order by cohort_month
```

`provisional_pct`가 높은 최근 코호트를 이탈률 비교에 넣지 않는다.
2026-07-26 확인 시점 전체 평균은 9.65%, as-of는 2026-07-18이었다.

### dry run

```bash
export GOOGLE_APPLICATION_CREDENTIALS=gcp-key.json
bq query --project_id=bda-coai --use_legacy_sql=false --dry_run < 쿼리.sql
```

출력의 bytes를 GB로 환산해 보고한다. 온디맨드 단가는 약 $6.25/TiB
(`scripts/check_bigquery_cost_guard.py` 기본값 기준).

## 예산 기준

| 상황 | 목표 |
|---|---|
| mart 조회 | 수백 MB 이하. 넘으면 파티션 조건을 빠뜨린 것 |
| fact 조회 (1~7일) | 수 GB 이하 |
| raw 조회 (payload 없이, 1일, type 필터) | 수십 GB. **승인 후 실행** |
| raw payload 조회 | 날짜와 type을 좁히고 dry run 먼저. **항상 승인 후 실행** |

예산을 넘기면 실행 전에 컬럼·날짜·event type·엔티티를 줄이거나 join 전에 pre-aggregate한다.
