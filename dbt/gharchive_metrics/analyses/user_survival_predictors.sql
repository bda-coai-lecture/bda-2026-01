-- Question: Which pre-cutoff actor activity patterns are associated with subsequent 12-week survival?
-- Grain: calendar cutoff week x predictor x fixed feature bucket
-- Time basis / timezone: fact_user_repo_activity.activity_date (UTC calendar date); Sunday cutoffs
-- Validation range: one cutoff from var('validation_cutoff', '2026-04-19')
-- Analysis range: weekly cutoffs from var('cohort_start_date', '2026-02-01') through var('cohort_end_date', '2026-04-19')
-- Segments: prior-28-day events, active days/weeks, action diversity, repository count, and weekly regularity
-- Exclusions: null keys are excluded upstream; actors require >=1 event in the 28-day feature window
-- Metric definition: features use cutoff-27..cutoff only; outcomes use cutoff+1..cutoff+84 only
-- Expected output grain: one row per cutoff_date x predictor x feature_bucket

with params as (
  select
    date('{{ var("cohort_start_date", "2026-02-01") }}') as cohort_start_date,
    date('{{ var("cohort_end_date", "2026-04-19") }}') as cohort_end_date,
    date('{{ var("validation_cutoff", "2026-04-19") }}') as validation_cutoff
),

cutoffs as (
  select
    cutoff_date,
    case
      when cutoff_date = validation_cutoff then 'forward_validation'
      else 'development'
    end as evaluation_split
  from params,
  unnest(generate_date_array(cohort_start_date, cohort_end_date, interval 7 day)) as cutoff_date
),

filtered_activity as (
  select
    activity_date,
    user_id,
    repo_id,
    action,
    event_count
  from {{ ref('fact_user_repo_activity') }}
  cross join params
  where activity_date between date_sub(cohort_start_date, interval 27 day)
                          and date_add(cohort_end_date, interval 84 day)
),

feature_daily as (
  select
    c.cutoff_date,
    c.evaluation_split,
    a.user_id,
    a.activity_date,
    sum(a.event_count) as daily_events,
    count(distinct a.repo_id) as daily_repos,
    count(distinct a.action) as daily_actions
  from cutoffs c
  join filtered_activity a
    on a.activity_date between date_sub(c.cutoff_date, interval 27 day) and c.cutoff_date
  group by c.cutoff_date, c.evaluation_split, a.user_id, a.activity_date
),

feature_actor as (
  select
    cutoff_date,
    evaluation_split,
    user_id,
    sum(daily_events) as events_28d,
    count(*) as active_days_28d,
    count(distinct div(date_diff(cutoff_date, activity_date, day), 7)) as active_weeks_28d,
    stddev_pop(daily_events) as daily_event_stddev,
    avg(daily_events) as daily_event_mean
  from feature_daily
  group by cutoff_date, evaluation_split, user_id
),

feature_actor_dimensions as (
  select
    c.cutoff_date,
    a.user_id,
    count(distinct a.repo_id) as repos_28d,
    count(distinct a.action) as actions_28d
  from cutoffs c
  join filtered_activity a
    on a.activity_date between date_sub(c.cutoff_date, interval 27 day) and c.cutoff_date
  group by c.cutoff_date, a.user_id
),

future_actor as (
  select
    c.cutoff_date,
    a.user_id,
    count(distinct date_diff(a.activity_date, c.cutoff_date, week)) as future_active_weeks_12w,
    countif(a.activity_date between date_add(c.cutoff_date, interval 57 day)
                                    and date_add(c.cutoff_date, interval 84 day)) > 0
      as survived_to_weeks_9_12
  from cutoffs c
  join filtered_activity a
    on a.activity_date between date_add(c.cutoff_date, interval 1 day)
                           and date_add(c.cutoff_date, interval 84 day)
  group by c.cutoff_date, a.user_id
),

actor_dataset as (
  select
    f.cutoff_date,
    f.evaluation_split,
    f.user_id,
    f.events_28d,
    f.active_days_28d,
    f.active_weeks_28d,
    d.repos_28d,
    d.actions_28d,
    safe_divide(f.daily_event_stddev, f.daily_event_mean) as active_day_event_cv,
    coalesce(o.future_active_weeks_12w, 0) as future_active_weeks_12w,
    coalesce(o.survived_to_weeks_9_12, false) as survived_to_weeks_9_12
  from feature_actor f
  join feature_actor_dimensions d
    using (cutoff_date, user_id)
  left join future_actor o
    using (cutoff_date, user_id)
),

bucketed as (
  select
    a.cutoff_date,
    a.evaluation_split,
    a.user_id,
    a.future_active_weeks_12w,
    a.survived_to_weeks_9_12,
    bucket.predictor,
    bucket.feature_bucket,
    bucket.bucket_order
  from actor_dataset a
  cross join unnest([
    struct(
      'events_28d' as predictor,
      case when events_28d = 1 then '01: 1'
           when events_28d <= 4 then '02: 2-4'
           when events_28d <= 19 then '03: 5-19'
           when events_28d <= 99 then '04: 20-99'
           else '05: 100+' end as feature_bucket,
      case when events_28d = 1 then 1 when events_28d <= 4 then 2
           when events_28d <= 19 then 3 when events_28d <= 99 then 4 else 5 end as bucket_order),
    struct(
      'active_days_28d',
      case when active_days_28d = 1 then '01: 1'
           when active_days_28d <= 3 then '02: 2-3'
           when active_days_28d <= 7 then '03: 4-7'
           when active_days_28d <= 14 then '04: 8-14'
           else '05: 15-28' end,
      case when active_days_28d = 1 then 1 when active_days_28d <= 3 then 2
           when active_days_28d <= 7 then 3 when active_days_28d <= 14 then 4 else 5 end),
    struct(
      'active_weeks_28d',
      concat('0', cast(active_weeks_28d as string), ': ', cast(active_weeks_28d as string), ' week(s)'),
      cast(active_weeks_28d as int64)),
    struct(
      'repos_28d',
      case when repos_28d = 1 then '01: 1'
           when repos_28d = 2 then '02: 2'
           when repos_28d <= 5 then '03: 3-5'
           when repos_28d <= 10 then '04: 6-10'
           else '05: 11+' end,
      case when repos_28d = 1 then 1 when repos_28d = 2 then 2
           when repos_28d <= 5 then 3 when repos_28d <= 10 then 4 else 5 end),
    struct(
      'actions_28d',
      case when actions_28d = 1 then '01: 1'
           when actions_28d = 2 then '02: 2'
           when actions_28d <= 4 then '03: 3-4'
           else '04: 5+' end,
      case when actions_28d = 1 then 1 when actions_28d = 2 then 2
           when actions_28d <= 4 then 3 else 4 end),
    struct(
      'active_day_event_cv',
      case when active_days_28d = 1 then '00: single active day'
           when active_day_event_cv < 0.5 then '01: <0.5 (steady)'
           when active_day_event_cv < 1.0 then '02: 0.5-1.0'
           else '03: 1.0+ (bursty)' end,
      case when active_days_28d = 1 then 0 when active_day_event_cv < 0.5 then 1
           when active_day_event_cv < 1.0 then 2 else 3 end)
  ]) as bucket
),

aggregated as (
  select
    cutoff_date,
    evaluation_split,
    predictor,
    feature_bucket,
    bucket_order,
    count(*) as actors,
    avg(cast(future_active_weeks_12w > 0 as int64)) as any_12w_reactivation_rate,
    avg(cast(survived_to_weeks_9_12 as int64)) as late_12w_survival_rate,
    avg(future_active_weeks_12w) as avg_future_active_weeks
  from bucketed
  group by cutoff_date, evaluation_split, predictor, feature_bucket, bucket_order
),

final as (
  select
    *,
    safe_divide(
      late_12w_survival_rate,
      first_value(late_12w_survival_rate) over (
        partition by cutoff_date, predictor order by bucket_order
      )
    ) as late_survival_ratio_vs_lowest_bucket
  from aggregated
)

select *
from final
order by cutoff_date, predictor, bucket_order
