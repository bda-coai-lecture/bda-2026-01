with user_month as (
  select distinct
    user_id,
    month_start
  from {{ ref('stg_user_repo_activity') }}
),

cohort_activity as (
  select
    cohort_month.month_start as cohort_month,
    activity_month.month_start,
    date_diff(activity_month.month_start, cohort_month.month_start, month) as months_since,
    cohort_month.user_id
  from user_month as cohort_month
  join user_month as activity_month
    on cohort_month.user_id = activity_month.user_id
    and activity_month.month_start >= cohort_month.month_start
    and activity_month.month_start <= date_add(cohort_month.month_start, interval 12 month)
),

cohort_sizes as (
  select
    month_start as cohort_month,
    count(distinct user_id) as cohort_users
  from user_month
  group by month_start
),

retention as (
  select
    cohort_activity.cohort_month,
    cohort_activity.months_since,
    cohort_sizes.cohort_users,
    count(distinct cohort_activity.user_id) as active_users,
    safe_divide(count(distinct cohort_activity.user_id), cohort_sizes.cohort_users) as retention_rate
  from cohort_activity
  join cohort_sizes using (cohort_month)
  where cohort_activity.months_since between 0 and 12
  group by
    cohort_activity.cohort_month,
    cohort_activity.months_since,
    cohort_sizes.cohort_users
)

select
  cohort_month,
  max(cohort_users) as cohort_users,
  max(months_since) as max_observed_months_since,
  max(if(months_since = 0, active_users, null)) as m0_active_users,
  max(if(months_since = 0, retention_rate, null)) as m0_retention,
  max(if(months_since = 1, retention_rate, null)) as m1_retention,
  max(if(months_since = 2, retention_rate, null)) as m2_retention,
  max(if(months_since = 3, retention_rate, null)) as m3_retention,
  max(if(months_since = 4, retention_rate, null)) as m4_retention,
  max(if(months_since = 5, retention_rate, null)) as m5_retention,
  max(if(months_since = 6, retention_rate, null)) as m6_retention,
  max(if(months_since = 7, retention_rate, null)) as m7_retention,
  max(if(months_since = 8, retention_rate, null)) as m8_retention,
  max(if(months_since = 9, retention_rate, null)) as m9_retention,
  max(if(months_since = 10, retention_rate, null)) as m10_retention,
  max(if(months_since = 11, retention_rate, null)) as m11_retention,
  max(if(months_since = 12, retention_rate, null)) as m12_retention
from retention
group by cohort_month
