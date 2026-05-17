select
  activity_date,
  action,
  count(distinct user_id) as active_users,
  count(distinct repo_id) as active_repos,
  sum(event_count) as total_events,
  count(*) as user_repo_action_rows
from {{ ref('stg_user_repo_activity') }}
group by activity_date, action
