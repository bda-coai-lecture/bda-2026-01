-- Question: push / non-push 구성 변화가 언제부터, 어떤 모양으로 진행됐나
-- Mode: 활동량 (event type 구성 추세)
-- Grain: activity_date
-- Time basis / timezone: activity_date, UTC
-- Validation range: event_type_health_20260801.sql 에서 합계 보존 PASS
-- Analysis range: 2026-06-01 ~ 2026-08-01 (mart 최신 완결일까지)
-- Segments: PushEvent vs non-Push 전체
-- Exclusions: 미완결 최신일(2026-08-02) 제외. automation_actor 미제외(포함)
-- Metric definition: push_events / non_push_events = sum(total_events) by action 분기
--   non_push_types = 그 날 1건 이상 관측된 non-push action 수
-- Expected output grain: activity_date 1행
-- Note: mart table만 조회. 선행 raw 확인은 non_push_drop_raw_daily_series.sql 참고.

with filtered as (
  select activity_date, action, total_events, active_users
  from `bda-coai.mart.metrics_event_type_daily`
  where activity_date between date '2026-06-01' and date '2026-08-01'
),

aggregated_daily as (
  select
    activity_date,
    sum(if(action = 'PushEvent', total_events, 0)) as push_events,
    sum(if(action <> 'PushEvent', total_events, 0)) as non_push_events,
    max(if(action = 'PushEvent', active_users, null)) as push_actors,
    countif(action <> 'PushEvent' and total_events > 0) as non_push_types
  from filtered
  group by activity_date
),

final as (
  select
    activity_date,
    format_date('%a', activity_date) as dow,
    push_events,
    non_push_events,
    round(safe_divide(push_events, push_events + non_push_events) * 100, 1) as push_share_pct,
    push_actors,
    round(safe_divide(push_events, push_actors), 2) as push_events_per_actor,
    non_push_types
  from aggregated_daily
)

select *
from final
order by activity_date
