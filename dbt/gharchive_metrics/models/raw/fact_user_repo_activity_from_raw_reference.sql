select
  user_id,
  repo_id,
  action,
  count(*) as event_count,
  activity_date
from {{ ref('raw_githubarchive_events_90d') }}
group by user_id, repo_id, action, activity_date
