select
  activity_date,
  count(distinct user_id) as active_users,
  count(distinct repo_id) as active_repos,
  sum(event_count) as total_events,
  count(*) as user_repo_action_rows,
  safe_divide(sum(event_count), count(distinct user_id)) as events_per_active_user,
  sum(if(action = 'PushEvent', event_count, 0)) as push_events,
  sum(if(action = 'WatchEvent', event_count, 0)) as watch_events,
  sum(if(action = 'ForkEvent', event_count, 0)) as fork_events,
  sum(if(action = 'PullRequestEvent', event_count, 0)) as pull_request_events,
  sum(if(action = 'IssuesEvent', event_count, 0)) as issue_events,
  sum(if(action = 'IssueCommentEvent', event_count, 0)) as issue_comment_events
from {{ ref('stg_user_repo_activity') }}
group by activity_date
