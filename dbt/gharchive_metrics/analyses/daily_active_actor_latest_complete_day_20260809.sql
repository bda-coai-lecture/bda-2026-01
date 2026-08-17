-- Question: 최근 완료된 1일의 active actor 수 (Slack 간단 질의, as-of 2026-08-09)
-- Mode: 활동량
-- Grain: activity_date × actor (지표 출력은 activity_date 1행)
-- Time basis / timezone: activity_date, UTC. 진행 중인 당일(current_date UTC) 제외
-- Validation range: 최근 16일 추세 + 최신 완료일 단일 파티션 정합
-- Analysis range: 최신 완료일 (as-of 2026-08-09)
-- Segments: 없음 (전체 actor)
-- Exclusions: 진행 중인 당일 제외. automation_actor(dim_push_automation_actor) 포함(제외하지 않음)
-- Metric definition: active_actors = count(distinct user_id) = metrics_daily.active_users
-- Guardrail: events_per_actor (actor가 낮은데 event/actor가 정상이면 미마감이 아니라 주말 패턴 신호)
-- Expected output grain: 1행
-- Scan budget: mart 조회, 목표 수백 MB 이하 / 실측 dry run 14.9 KB(추세) + 32.1 MB(fact 1파티션) → PASS
-- 선행 분석: daily_active_actor_latest_complete_day_20260802.sql (Metabase card 1023, 재사용)
--
-- 결과 (as-of 2026-08-09):
--   최신 완료일 2026-08-08(토), active_actors = 380,346, total_events = 4,012,620, events/actor 10.55
--
-- 건강성 검증표
--   shard 마감      PASS  최신일 2026-08-08, 진행 중 당일(08-09) 제외. events 4.01M은 직전 16일 범위(2.49M~4.08M) 상단
--   mart 커버리지   PASS  2025-05-01 ~ 2026-08-08, 465일 = distinct 465일 (누락 없음)
--   적재 지연       WARN  08-02~08-07 구간이 actors 242k~300k로 낮음. replace-days(최근 3일) 창 밖 구간이라
--                         지연 도착/부분 적재 가능성을 raw 대조 없이는 배제 못 함. 08-08 자체는 정상 수준
--   좌측 절단       N/A   단일일 활동량 지표, 이력 의존 없음
--   우측 절단       PASS  진행 중 당일 제외. 코호트/이탈창 미사용
--   자동화 혼입     기록  [bot] 접미사 계정 1,496명 = DAU의 0.39%. 제외하지 않음
--   grain 중복      PASS  rows_ 735,607 = distinct_grain 735,607
--   합계 보존       PASS  fact distinct user_id 380,346 = metrics_daily.active_users
--                         fact sum(event_count) 4,012,620 = metrics_daily.total_events
--   분모 정의       PASS  null_user 0, bad_event_count 0
--   스캔 예산       PASS  총 ~32 MB (상한 10 GiB)

-- 1) 본 지표
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
-- 2-1) mart 커버리지 + shard 마감 + 우측 절단 (최근 16일 추세)
select
  cast(activity_date as string) as d,
  active_users as active_actors,
  active_repos,
  total_events,
  round(safe_divide(total_events, active_users), 2) as events_per_actor
from `bda-coai.mart.metrics_daily`
where activity_date >= date_sub(current_date('UTC'), interval 16 day)
order by activity_date desc
;

-- 2-2) mart 커버리지 경계
select cast(min(activity_date) as string) as min_d,
       cast(max(activity_date) as string) as max_d,
       count(*) as n_days
from `bda-coai.mart.metrics_daily`
;

-- 2-3) mart ↔ fact 정합 + grain 유일성 + null (최신 완료일 1파티션)
select count(distinct user_id) as actors,
       sum(event_count) as events,
       count(*) as rows_,
       count(distinct format('%t|%t|%t', user_id, repo_id, action)) as distinct_grain,
       countif(user_id is null) as null_user,
       countif(event_count < 1) as bad_event_count
from `bda-coai.mart.fact_user_repo_activity`
where activity_date = date '2026-08-08'
;

-- 2-4) 자동화 혼입 규모: [bot] 접미사 계정 비중 (제외하지 않음)
with d as (
  select distinct user_id
  from `bda-coai.mart.fact_user_repo_activity`
  where activity_date = date '2026-08-08'
)
select count(*) as all_actors,
       countif(a.user_id is not null) as bot_suffix_actors,
       round(safe_divide(countif(a.user_id is not null), count(*)) * 100, 2) as bot_actor_pct
from d
left join `bda-coai.mart.dim_push_automation_actor` a using (user_id)
;
