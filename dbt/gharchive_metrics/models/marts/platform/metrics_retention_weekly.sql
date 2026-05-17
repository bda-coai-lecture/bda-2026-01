with user_week as (
  select distinct
    user_id,
    week_start
  from {{ ref('stg_user_repo_activity') }}
),

first_week as (
  select
    user_id,
    min(week_start) as cohort_week
  from user_week
  group by user_id
),

cohort_activity as (
  select
    user_week.user_id,
    first_week.cohort_week,
    user_week.week_start,
    date_diff(user_week.week_start, first_week.cohort_week, week) as weeks_since
  from user_week
  join first_week using (user_id)
),

cohort_sizes as (
  select
    cohort_week,
    count(*) as cohort_users
  from first_week
  group by cohort_week
)

select
  cohort_activity.cohort_week,
  cohort_activity.week_start,
  cohort_activity.weeks_since,
  cohort_sizes.cohort_users,
  count(distinct cohort_activity.user_id) as active_users,
  safe_divide(count(distinct cohort_activity.user_id), cohort_sizes.cohort_users) as retention_rate
from cohort_activity
join cohort_sizes using (cohort_week)
group by
  cohort_activity.cohort_week,
  cohort_activity.week_start,
  cohort_activity.weeks_since,
  cohort_sizes.cohort_users
