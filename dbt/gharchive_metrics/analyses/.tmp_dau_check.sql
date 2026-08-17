select cast(activity_date as string) as d, active_users, active_repos, total_events,
       round(safe_divide(total_events, active_users), 2) as events_per_actor
from `bda-coai.mart.metrics_daily`
where activity_date >= date_sub(date '2026-08-01', interval 13 day)
  and activity_date < current_date('UTC')
order by activity_date desc
