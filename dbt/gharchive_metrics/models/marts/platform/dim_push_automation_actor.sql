{{
  config(
    materialized='table',
    cluster_by=["user_id"]
  )
}}

with actor_days as (
  select *
  from {{ ref('int_push_automation_actor_day') }}
),

actor_flags as (
  select
    user_id,
    array_agg(
      latest_login ignore nulls
      order by activity_date desc, latest_login desc
      limit 1
    )[safe_offset(0)] as latest_login,
    logical_or(explicit_bot) as explicit_bot,
    logical_or(machine_rate_suspect) as machine_rate_suspect,
    max(max_push_events_in_minute) as max_push_events_in_minute,
    max(max_repos_in_minute) as max_repos_in_minute,
    sum(observed_push_events) as observed_push_events,
    min(activity_date) as observed_from,
    max(activity_date) as observed_through
  from actor_days
  group by user_id
)

select
  user_id,
  latest_login,
  explicit_bot,
  machine_rate_suspect,
  case
    when explicit_bot and machine_rate_suspect then 'explicit_bot_and_machine_rate'
    when explicit_bot then 'explicit_bot'
    else 'machine_rate'
  end as flag_reason,
  max_push_events_in_minute,
  max_repos_in_minute,
  observed_push_events,
  observed_from,
  observed_through
from actor_flags
where explicit_bot or machine_rate_suspect
