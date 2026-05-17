select
  week_start,
  count(distinct user_id) as weekly_active_users,
  count(distinct repo_id) as weekly_active_repos,
  sum(event_count) as total_events,
  safe_divide(sum(event_count), count(distinct user_id)) as events_per_active_user
from {{ ref('stg_user_repo_activity') }}
group by week_start
