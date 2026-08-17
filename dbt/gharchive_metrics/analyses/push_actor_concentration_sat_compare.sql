-- Question: push 계정당 이벤트 수가 2배 이상 오른 게 소수 고volume 계정 집중 때문인가
-- Mode: 세그먼트 비교 (일일 push volume 구간별)
-- Grain: activity_date x volume_bucket (원천 grain은 activity_date x user_id)
-- Time basis / timezone: activity_date, UTC
-- Validation range: 두 날짜 모두 raw 대조 완료 (2026-08-01은 raw=fact 정확히 일치)
-- Analysis range: 2026-06-06(Sat) vs 2026-08-01(Sat)
--   ※ 요일 효과가 크므로 같은 토요일끼리 비교해 통제한다
-- Segments: 일일 push event 수 구간 (1-9 / 10-99 / 100-999 / 1000+)
-- Exclusions: action='PushEvent'만. automation_actor 미제외(포함)
--   dim_push_automation_actor는 사실상 [bot] 접미사 목록이라 검정력이 없어 velocity로 직접 본다
-- Metric definition: bucket별 actor 수, push event 합, 각 날짜 내 event 점유율
-- Expected output grain: activity_date x volume_bucket
-- Note: partition pruning으로 2일만 스캔.

with filtered_push as (
  select activity_date, user_id, sum(event_count) as push_events
  from `bda-coai.mart.fact_user_repo_activity`
  where activity_date in (date '2026-06-06', date '2026-08-01')
    and action = 'PushEvent'
  group by activity_date, user_id
),

bucketed as (
  select
    activity_date,
    case
      when push_events >= 1000 then 'd_1000plus'
      when push_events >= 100 then 'c_100_999'
      when push_events >= 10 then 'b_10_99'
      else 'a_1_9'
    end as volume_bucket,
    user_id,
    push_events
  from filtered_push
),

aggregated as (
  select
    activity_date,
    volume_bucket,
    count(distinct user_id) as actors,
    sum(push_events) as push_events
  from bucketed
  group by activity_date, volume_bucket
),

final as (
  select
    activity_date,
    volume_bucket,
    actors,
    round(safe_divide(actors, sum(actors) over (partition by activity_date)) * 100, 2) as actor_share_pct,
    push_events,
    round(safe_divide(push_events, sum(push_events) over (partition by activity_date)) * 100, 2) as event_share_pct
  from aggregated
)

select *
from final
order by activity_date, volume_bucket
