with user_period as (
  select
    user_id,
    count(distinct activity_date) as active_days,
    sum(event_count) as total_events,
    count(distinct repo_id) as active_repos
  from {{ ref('stg_user_repo_activity') }}
  group by user_id
),

segmented as (
  select
    *,
    case
      when active_days = 1 then 'one_day'
      when active_days between 2 and 4 then 'repeat_2d_4d'
      when active_days between 5 and 14 then 'regular_5d_14d'
      else 'power_15d_plus'
    end as user_segment
  from user_period
)

select
  user_segment,
  count(*) as users,
  sum(total_events) as total_events,
  avg(active_days) as avg_active_days,
  avg(active_repos) as avg_active_repos,
  avg(total_events) as avg_total_events
from segmented
group by user_segment
