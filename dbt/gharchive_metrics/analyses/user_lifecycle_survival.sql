-- Question: What is the observed actor lifetime, dormancy gap, and exact-week future survival baseline?
-- Grain: actor x Monday-starting UTC week; final output is metric x horizon.
-- Time basis / timezone: fact activity_date, UTC calendar weeks starting Monday.
-- Validation range: 2026-05-24 to 2026-05-30.
-- Analysis range: complete weeks within analysis_start_date to analysis_end_date.
-- Segments: first-observed weekly cohorts; horizon in (4, 12, 26 weeks).
-- Exclusions: null keys are excluded upstream; partial boundary weeks are excluded.
-- Metric definition: survival_h is activity in the exact h-th week after first observed week.
-- Expected output grain: metric_name x horizon_weeks (null for lifetime/gap metrics).

with params as (
  select
    date('{{ var("analysis_start_date", "2025-05-05") }}') as analysis_start_date,
    date('{{ var("analysis_end_date", "2026-07-12") }}') as analysis_end_date
),

filtered_activity as (
  select distinct
    user_id,
    date_trunc(activity_date, week(monday)) as week_start
  from {{ ref('fact_user_repo_activity') }}
  cross join params
  where activity_date between analysis_start_date and analysis_end_date
),

sequenced_activity as (
  select
    user_id,
    week_start,
    lag(week_start) over (partition by user_id order by week_start) as previous_active_week
  from filtered_activity
),

user_lifetime as (
  select
    user_id,
    min(week_start) as first_week,
    max(week_start) as last_week,
    count(*) as active_weeks,
    date_diff(max(week_start), min(week_start), week) + 1 as observed_span_weeks,
    max(
      if(
        previous_active_week is null,
        0,
        date_diff(week_start, previous_active_week, week) - 1
      )
    ) as max_inactive_gap_weeks
  from sequenced_activity
  group by user_id
),

horizons as (
  select horizon_weeks
  from unnest([4, 12, 26]) as horizon_weeks
),

eligible_cohort_users as (
  select
    user_lifetime.user_id,
    user_lifetime.first_week,
    horizons.horizon_weeks
  from user_lifetime
  cross join horizons
  cross join params
  where date_add(user_lifetime.first_week, interval horizons.horizon_weeks week)
        <= params.analysis_end_date
),

survival_summary as (
  select
    eligible.horizon_weeks,
    count(*) as eligible_users,
    countif(future.user_id is not null) as surviving_users,
    safe_divide(countif(future.user_id is not null), count(*)) as survival_rate
  from eligible_cohort_users as eligible
  left join filtered_activity as future
    on future.user_id = eligible.user_id
   and future.week_start =
       date_add(eligible.first_week, interval eligible.horizon_weeks week)
  group by eligible.horizon_weeks
),

lifetime_quantiles as (
  select
    count(*) as users,
    approx_quantiles(observed_span_weeks, 100)[offset(50)] as span_p50_weeks,
    approx_quantiles(observed_span_weeks, 100)[offset(75)] as span_p75_weeks,
    approx_quantiles(observed_span_weeks, 100)[offset(90)] as span_p90_weeks,
    approx_quantiles(max_inactive_gap_weeks, 100)[offset(50)] as gap_p50_weeks,
    approx_quantiles(max_inactive_gap_weeks, 100)[offset(75)] as gap_p75_weeks,
    approx_quantiles(max_inactive_gap_weeks, 100)[offset(90)] as gap_p90_weeks,
    avg(active_weeks) as mean_active_weeks,
    avg(observed_span_weeks) as mean_span_weeks
  from user_lifetime
),

final as (
  select
    'exact_week_survival_rate' as metric_name,
    horizon_weeks,
    survival_rate as metric_value,
    eligible_users as denominator_users,
    surviving_users as numerator_users
  from survival_summary

  union all

  select 'observed_span_p50_weeks', null, cast(span_p50_weeks as float64), users, null
  from lifetime_quantiles
  union all
  select 'observed_span_p75_weeks', null, cast(span_p75_weeks as float64), users, null
  from lifetime_quantiles
  union all
  select 'observed_span_p90_weeks', null, cast(span_p90_weeks as float64), users, null
  from lifetime_quantiles
  union all
  select 'max_inactive_gap_p50_weeks', null, cast(gap_p50_weeks as float64), users, null
  from lifetime_quantiles
  union all
  select 'max_inactive_gap_p75_weeks', null, cast(gap_p75_weeks as float64), users, null
  from lifetime_quantiles
  union all
  select 'max_inactive_gap_p90_weeks', null, cast(gap_p90_weeks as float64), users, null
  from lifetime_quantiles
  union all
  select 'mean_active_weeks', null, mean_active_weeks, users, null
  from lifetime_quantiles
  union all
  select 'mean_observed_span_weeks', null, mean_span_weeks, users, null
  from lifetime_quantiles
)

select *
from final
order by metric_name, horizon_weeks
