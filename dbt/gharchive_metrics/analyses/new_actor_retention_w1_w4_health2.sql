-- Question: shard 마감(최근 일별 추세) + 자동화([bot] 접미사) actor 혼입 규모
-- Grain: activity_date (마감), 1행 (자동화 비중)
-- Time basis / timezone: activity_date (UTC)
-- Validation range: 최근 10일 / 2026-06-15~2026-06-21 (완결된 주)
-- Analysis range: n/a (검증 전용)
-- Segments: 없음
-- Exclusions: 없음
-- Metric definition: active_users/total_events 일별, dim_push_automation_actor 매칭 actor 비중
-- Expected output grain: 검증 항목별 1행
select
  'daily' as check_name,
  cast(activity_date as string) as v1,
  cast(active_users as string) as v2,
  cast(total_events as string) as v3
from `bda-coai.mart.metrics_daily`
where activity_date >= date_sub(date '2026-07-25', interval 9 day)

union all
select
  'bot_share_week_20260615',
  cast(countif(a.user_id is not null) as string),
  cast(count(*) as string),
  cast(round(safe_divide(countif(a.user_id is not null), count(*)) * 100, 2) as string)
from (
  select distinct user_id
  from `bda-coai.mart.fact_user_repo_activity`
  where activity_date between '2026-06-15' and '2026-06-21'
) as f
left join `bda-coai.mart.dim_push_automation_actor` as a using (user_id)
order by check_name, v1
