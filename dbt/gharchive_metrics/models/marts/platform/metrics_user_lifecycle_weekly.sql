with user_week as (
  select distinct
    user_id,
    week_start
  from {{ ref('stg_user_repo_activity') }}
),

date_bounds as (
  select
    min(week_start) as min_period_start,
    max(week_start) as max_period_start,
    max(activity_date) as max_activity_date
  from {{ ref('stg_user_repo_activity') }}
),

periods as (
  select
    period_start,
    date_add(period_start, interval 7 day) as next_period_start,
    date_add(period_start, interval 6 day) as period_end,
    max_activity_date
  from date_bounds,
  unnest(generate_date_array(min_period_start, max_period_start, interval 7 day)) as period_start
),

active_users as (
  select
    week_start,
    count(distinct user_id) as active_users
  from user_week
  group by week_start
),

existing_users as (
  select
    current_period.week_start,
    count(distinct current_period.user_id) as existing_users
  from user_week as current_period
  join user_week as previous_period
    on current_period.user_id = previous_period.user_id
    and previous_period.week_start = date_sub(current_period.week_start, interval 7 day)
  group by current_period.week_start
),

returning_users as (
  select
    current_period.week_start,
    count(distinct current_period.user_id) as returning_or_historical_unknown_users
  from user_week as current_period
  left join user_week as previous_period
    on current_period.user_id = previous_period.user_id
    and previous_period.week_start = date_sub(current_period.week_start, interval 7 day)
  where previous_period.user_id is null
  group by current_period.week_start
),

churned_users as (
  select
    date_add(previous_period.week_start, interval 7 day) as week_start,
    count(distinct previous_period.user_id) as churned_users
  from user_week as previous_period
  left join user_week as current_period
    on previous_period.user_id = current_period.user_id
    and current_period.week_start = date_add(previous_period.week_start, interval 7 day)
  where current_period.user_id is null
  group by week_start
)

select
  periods.period_start as week_start,
  periods.period_end,
  date_sub(periods.period_start, interval 7 day) as previous_week_start,
  periods.max_activity_date >= periods.period_end as is_complete_period,
  date_sub(periods.period_start, interval 7 day) >= date_bounds.min_period_start as has_complete_previous_period,
  coalesce(active_users.active_users, 0) as active_users,
  coalesce(previous_active_users.active_users, 0) as previous_active_users,
  coalesce(existing_users.existing_users, 0) as existing_users,
  coalesce(returning_users.returning_or_historical_unknown_users, 0) as returning_or_historical_unknown_users,
  coalesce(churned_users.churned_users, 0) as churned_users,
  safe_divide(coalesce(existing_users.existing_users, 0), previous_active_users.active_users) as retention_rate,
  safe_divide(coalesce(churned_users.churned_users, 0), previous_active_users.active_users) as churn_rate,
  safe_divide(coalesce(returning_users.returning_or_historical_unknown_users, 0), active_users.active_users) as returning_or_historical_unknown_share
from periods
cross join date_bounds
left join active_users
  on periods.period_start = active_users.week_start
left join active_users as previous_active_users
  on previous_active_users.week_start = date_sub(periods.period_start, interval 7 day)
left join existing_users
  on periods.period_start = existing_users.week_start
left join returning_users
  on periods.period_start = returning_users.week_start
left join churned_users
  on periods.period_start = churned_users.week_start
