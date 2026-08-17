-- Question: 최근 완료된 1일의 active actor 수 (Metabase 임시 카드용)
-- Mode: 활동량
-- Grain: activity_date × actor (지표는 activity_date 1행)
-- Time basis / timezone: activity_date, UTC. 진행 중인 당일(current_date UTC) 제외
-- Validation range: 2026-07-19 ~ 2026-08-01 (14일 추세) + 2026-08-01 단일일 정합
-- Analysis range: 2026-08-01 (as-of 2026-08-02)
-- Segments: 없음 (전체 actor)
-- Exclusions: 진행 중인 당일 제외. automation_actor(dim_push_automation_actor) 포함(제외하지 않음)
-- Metric definition: active_actors = count(distinct user_id) = metrics_daily.active_users
-- Guardrail: total_events (actor가 낮은데 event가 정상이면 미마감이 아니라 주말 패턴 신호)
-- Expected output grain: 1행
-- Scan budget: mart 조회, 목표 수백 MB 이하 / 실측 dry run 23.8 MB (fact 1파티션 정합 쿼리)
-- Metabase card: 1023 (collection 5 "BDA 데이터 플랫폼")

-- 1) 본 지표 (Metabase 카드 1023과 동일 SQL)
select
  active_users as active_actors,
  activity_date as as_of_date
from `bda-coai.mart.metrics_daily`
where activity_date = (
  select max(activity_date)
  from `bda-coai.mart.metrics_daily`
  where activity_date < current_date('UTC')
)
;

-- 2) 건강성 검증 ------------------------------------------------------------
-- 2-1) mart 커버리지: 2025-05-01 ~ 2026-08-01, 458일 연속 (누락 없음) → PASS
select cast(min(activity_date) as string) as min_d,
       cast(max(activity_date) as string) as max_d,
       count(*) as n_days
from `bda-coai.mart.metrics_daily`
;

-- 2-2) shard 마감 / 우측 절단: 최신일 2026-08-01, total_events 4,071,556 는
--      직전 14일 범위(3.88M~4.07M) 안 → 부분 적재 아님 → PASS
select cast(activity_date as string) as d, active_users, active_repos, total_events
from `bda-coai.mart.metrics_daily`
where activity_date >= date_sub(date '2026-08-01', interval 13 day)
order by activity_date desc
;

-- 2-3) mart ↔ fact 정합 + grain 유일성
--      결과: actors 305,409 = metrics_daily.active_users,
--            events 4,071,556 = total_events,
--            rows_ 547,715 = distinct_grain → PASS
select count(distinct user_id) as actors,
       sum(event_count) as events,
       count(*) as rows_,
       count(distinct format('%t|%t|%t', user_id, repo_id, action)) as distinct_grain
from `bda-coai.mart.fact_user_repo_activity`
where activity_date = date '2026-08-01'
;

-- 2-4) 자동화 혼입 규모: [bot] 접미사 계정 1,387명 = DAU의 0.45% (제외하지 않음)
with d as (
  select distinct user_id
  from `bda-coai.mart.fact_user_repo_activity`
  where activity_date = date '2026-08-01'
)
select count(*) as all_actors,
       countif(a.user_id is not null) as bot_suffix_actors,
       round(safe_divide(countif(a.user_id is not null), count(*)) * 100, 2) as bot_actor_pct
from d
left join `bda-coai.mart.dim_push_automation_actor` a using (user_id)
;
