select
  cast(user_id as int64) as user_id,
  cast(repo_id as int64) as repo_id,
  cast(action as string) as action,
  cast(event_count as int64) as event_count,
  cast(activity_date as date) as activity_date,
  date_trunc(cast(activity_date as date), week(monday)) as week_start,
  date_trunc(cast(activity_date as date), month) as month_start
from {{ ref('fact_user_repo_activity') }}
where user_id is not null
  and repo_id is not null
  and action is not null
  and event_count is not null
  and activity_date is not null
