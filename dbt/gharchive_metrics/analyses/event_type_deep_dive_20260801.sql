-- Question: 활동을 event type(action)별로 쪼개면 최근 트렌드가 어떻게 보이나
-- Mode: 활동량 (event type 세그먼트 분해)
-- Grain: action x 7일 기간 버킷 (원천 grain은 activity_date x action)
-- Time basis / timezone: activity_date, UTC. as-of = mart 최신 완결일 2026-08-01
-- Validation range: 최근 60일 (event_type_health_20260801.sql 에서 합계 보존/grain 중복 PASS)
-- Analysis range: 최근 7일 / 직전 7일 / 8주 전 7일
--   p_recent 2026-07-26~2026-08-01, p_prev 2026-07-19~2026-07-25, p_base 2026-06-07~2026-06-13
-- Segments: action 16종 전체
-- Exclusions: 미완결 최신일(2026-08-02) 제외. automation_actor 미제외(포함)
-- Metric definition: events = sum(total_events)  (기간 합, 정확)
--   avg_daily_actors = avg(active_users)  = actor-day 일평균. 기간 내 distinct actor가 아니다.
--   (action별 active_users를 기간/타입에 걸쳐 더하면 중복 계산이므로 일평균으로만 본다)
-- Guardrail: events_per_actor_day = sum(total_events) / sum(active_users)
-- Expected output grain: action 1행 x 16
-- Note: mart table만 조회.

with params as (
  select
    date '2026-08-01' as as_of,
    date '2026-07-26' as recent_start,
    date '2026-07-19' as prev_start,
    date '2026-07-25' as prev_end,
    date '2026-06-07' as base_start,
    date '2026-06-13' as base_end
),

filtered_event_type_daily as (
  select e.activity_date, e.action, e.total_events, e.active_users
  from `bda-coai.mart.metrics_event_type_daily` as e, params as p
  where e.activity_date between p.base_start and p.as_of
),

aggregated_by_action as (
  select
    f.action,
    sum(if(f.activity_date between p.recent_start and p.as_of, f.total_events, 0)) as ev_recent,
    sum(if(f.activity_date between p.prev_start and p.prev_end, f.total_events, 0)) as ev_prev,
    sum(if(f.activity_date between p.base_start and p.base_end, f.total_events, 0)) as ev_base,
    round(avg(if(f.activity_date between p.recent_start and p.as_of, f.active_users, null)), 0) as act_recent,
    round(avg(if(f.activity_date between p.prev_start and p.prev_end, f.active_users, null)), 0) as act_prev,
    round(avg(if(f.activity_date between p.base_start and p.base_end, f.active_users, null)), 0) as act_base
  from filtered_event_type_daily as f, params as p
  group by f.action
),

final as (
  select
    action,
    ev_recent,
    round(safe_divide(ev_recent, sum(ev_recent) over ()) * 100, 1) as ev_recent_share_pct,
    round(safe_divide(ev_recent - ev_prev, ev_prev) * 100, 1) as ev_wow_pct,
    round(safe_divide(ev_recent - ev_base, ev_base) * 100, 1) as ev_vs_8w_pct,
    act_recent,
    act_prev,
    act_base,
    round(safe_divide(act_recent - act_prev, act_prev) * 100, 1) as act_wow_pct,
    round(safe_divide(act_recent - act_base, act_base) * 100, 1) as act_vs_8w_pct,
    round(safe_divide(ev_recent, act_recent * 7), 2) as events_per_actor_day_recent,
    round(safe_divide(ev_base, act_base * 7), 2) as events_per_actor_day_base
  from aggregated_by_action
)

select *
from final
order by ev_recent desc
