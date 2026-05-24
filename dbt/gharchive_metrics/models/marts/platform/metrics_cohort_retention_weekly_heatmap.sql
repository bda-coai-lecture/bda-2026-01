with user_week as (
  select distinct
    user_id,
    week_start
  from {{ ref('stg_user_repo_activity') }}
),

cohort_activity as (
  select
    cohort_week.week_start as cohort_week,
    activity_week.week_start,
    date_diff(activity_week.week_start, cohort_week.week_start, week) as weeks_since,
    cohort_week.user_id
  from user_week as cohort_week
  join user_week as activity_week
    on cohort_week.user_id = activity_week.user_id
    and activity_week.week_start >= cohort_week.week_start
    and activity_week.week_start <= date_add(cohort_week.week_start, interval 12 week)
),

cohort_sizes as (
  select
    week_start as cohort_week,
    count(distinct user_id) as cohort_users
  from user_week
  group by week_start
),

retention as (
  select
    cohort_activity.cohort_week,
    cohort_activity.weeks_since,
    cohort_sizes.cohort_users,
    count(distinct cohort_activity.user_id) as active_users,
    safe_divide(count(distinct cohort_activity.user_id), cohort_sizes.cohort_users) as retention_rate
  from cohort_activity
  join cohort_sizes using (cohort_week)
  where cohort_activity.weeks_since between 0 and 12
  group by
    cohort_activity.cohort_week,
    cohort_activity.weeks_since,
    cohort_sizes.cohort_users
)

select
  cohort_week,
  max(cohort_users) as cohort_users,
  max(weeks_since) as max_observed_weeks_since,
  max(if(weeks_since = 0, active_users, null)) as w0_active_users,
  max(if(weeks_since = 0, retention_rate, null)) as w0_retention,
  max(if(weeks_since = 1, retention_rate, null)) as w1_retention,
  max(if(weeks_since = 2, retention_rate, null)) as w2_retention,
  max(if(weeks_since = 3, retention_rate, null)) as w3_retention,
  max(if(weeks_since = 4, retention_rate, null)) as w4_retention,
  max(if(weeks_since = 5, retention_rate, null)) as w5_retention,
  max(if(weeks_since = 6, retention_rate, null)) as w6_retention,
  max(if(weeks_since = 7, retention_rate, null)) as w7_retention,
  max(if(weeks_since = 8, retention_rate, null)) as w8_retention,
  max(if(weeks_since = 9, retention_rate, null)) as w9_retention,
  max(if(weeks_since = 10, retention_rate, null)) as w10_retention,
  max(if(weeks_since = 11, retention_rate, null)) as w11_retention,
  max(if(weeks_since = 12, retention_rate, null)) as w12_retention
from retention
group by cohort_week
