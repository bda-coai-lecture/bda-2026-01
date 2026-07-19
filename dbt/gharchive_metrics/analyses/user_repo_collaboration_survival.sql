-- Question: Are repository breadth, shared-repository exposure, collaborator breadth,
--           and repository continuity observed before a landmark associated with later actor activity?
-- Grain: landmark cohort week x feature x feature bucket
-- Time basis / timezone: activity_date, UTC; weeks start Monday
-- Validation range: 2025-12-01 landmark only (override vars)
-- Analysis range: 2025-07-07 through 2026-01-12 landmark weeks (every four weeks)
-- Segments: active repo count, shared repo exposure, distinct co-actor breadth, repo continuity
-- Exclusions: upstream null exclusions; outcomes require complete 26-week follow-up
-- Metric definition: features use landmark week and prior 3 weeks only; survival is any activity
--                    in the exact 12th or 26th week after the landmark week
-- Expected output grain: one row per cohort_week x feature_name x feature_bucket

{% set cohort_start_date = var('cohort_start_date', '2025-07-07') %}
{% set cohort_end_date = var('cohort_end_date', '2026-01-12') %}
{% set data_end_date = var('data_end_date', '2026-07-18') %}

with params as (
  select
    date('{{ cohort_start_date }}') as cohort_start_date,
    date('{{ cohort_end_date }}') as cohort_end_date,
    date('{{ data_end_date }}') as data_end_date
),

cohort_weeks as (
  select cohort_week
  from params,
  unnest(
    generate_date_array(cohort_start_date, cohort_end_date, interval 4 week)
  ) as cohort_week
  where date_add(cohort_week, interval 26 week) <= data_end_date
),

filtered_activity as (
  select
    activity_date,
    date_trunc(activity_date, week(monday)) as activity_week,
    user_id,
    repo_id
  from {{ ref('fact_user_repo_activity') }}
  cross join params
  where activity_date between date_sub(cohort_start_date, interval 3 week)
                          and data_end_date
  group by activity_date, activity_week, user_id, repo_id
),

observation_activity as (
  select
    cohort.cohort_week,
    activity.activity_date,
    activity.activity_week,
    activity.user_id,
    activity.repo_id
  from cohort_weeks as cohort
  join filtered_activity as activity
    on activity.activity_date between date_sub(cohort.cohort_week, interval 3 week)
                                  and date_add(cohort.cohort_week, interval 6 day)
),

landmark_actors as (
  -- Requiring landmark-week activity makes this a repeated landmark prediction,
  -- rather than a first-ever-appearance cohort with unknown left history.
  select distinct
    cohort_week,
    user_id
  from observation_activity
  where activity_week = cohort_week
),

repo_observation as (
  select
    cohort_week,
    repo_id,
    count(distinct user_id) as repo_actor_count
  from observation_activity
  group by cohort_week, repo_id
),

actor_repo_observation as (
  select
    activity.cohort_week,
    activity.user_id,
    activity.repo_id,
    count(distinct activity.activity_week) as active_weeks_on_repo,
    max(repo.repo_actor_count) as repo_actor_count
  from observation_activity as activity
  join landmark_actors as landmark
    using (cohort_week, user_id)
  join repo_observation as repo
    using (cohort_week, repo_id)
  group by activity.cohort_week, activity.user_id, activity.repo_id
),

actor_intensity as (
  select
    activity.cohort_week,
    activity.user_id,
    count(distinct activity.activity_week) as active_weeks_4w,
    count(distinct activity.activity_date) as active_days_4w
  from observation_activity as activity
  join landmark_actors as landmark
    using (cohort_week, user_id)
  group by activity.cohort_week, activity.user_id
),

actor_features as (
  select
    actor_repo.cohort_week,
    actor_repo.user_id,
    any_value(intensity.active_weeks_4w) as active_weeks_4w,
    any_value(intensity.active_days_4w) as active_days_4w,
    count(*) as active_repos_4w,
    countif(repo_actor_count >= 2) as shared_repos_4w,
    -- Sum is an exposure breadth: the same co-actor encountered in two repos can count twice.
    sum(repo_actor_count - 1) as collaborator_exposures_4w,
    countif(active_weeks_on_repo >= 2) as continuing_repos_4w
  from actor_repo_observation as actor_repo
  join actor_intensity as intensity
    using (cohort_week, user_id)
  group by actor_repo.cohort_week, actor_repo.user_id
),

future_activity as (
  select
    cohort.cohort_week,
    activity.user_id,
    logical_or(
      activity.activity_date between date_add(cohort.cohort_week, interval 12 week)
                                 and date_add(cohort.cohort_week, interval 90 day)
    ) as active_w12,
    logical_or(
      activity.activity_date between date_add(cohort.cohort_week, interval 26 week)
                                 and date_add(cohort.cohort_week, interval 188 day)
    ) as active_w26
  from cohort_weeks as cohort
  join filtered_activity as activity
    on activity.activity_date between date_add(cohort.cohort_week, interval 12 week)
                                  and date_add(cohort.cohort_week, interval 188 day)
  group by cohort.cohort_week, activity.user_id
),

actor_outcomes as (
  select
    feature.*,
    coalesce(future.active_w12, false) as active_w12,
    coalesce(future.active_w26, false) as active_w26
  from actor_features as feature
  left join future_activity as future
    using (cohort_week, user_id)
),

long_features as (
  select
    actor.cohort_week,
    actor.user_id,
    actor.active_w12,
    actor.active_w26,
    feature.feature_name,
    feature.feature_bucket
  from actor_outcomes as actor
  cross join unnest([
    struct(
      'active_repos_4w' as feature_name,
      case
        when active_repos_4w = 1 then '01: 1'
        when active_repos_4w between 2 and 4 then '02: 2-4'
        when active_repos_4w between 5 and 9 then '03: 5-9'
        else '04: 10+'
      end as feature_bucket
    ),
    struct(
      'shared_repo_exposure_4w' as feature_name,
      case when shared_repos_4w = 0 then '01: none' else '02: 1+' end as feature_bucket
    ),
    struct(
      'collaborator_exposures_4w' as feature_name,
      case
        when collaborator_exposures_4w = 0 then '01: 0'
        when collaborator_exposures_4w between 1 and 2 then '02: 1-2'
        when collaborator_exposures_4w between 3 and 9 then '03: 3-9'
        else '04: 10+'
      end as feature_bucket
    ),
    struct(
      'continuing_repos_4w' as feature_name,
      case
        when continuing_repos_4w = 0 then '01: 0'
        when continuing_repos_4w = 1 then '02: 1'
        else '03: 2+'
      end as feature_bucket
    ),
    struct(
      -- This matched-stratum view compares repo breadth among actors with the
      -- same number of active weeks, reducing (not eliminating) activity-volume confounding.
      'active_weeks_x_repo_breadth_4w' as feature_name,
      concat(
        format('%02d', active_weeks_4w),
        ': ',
        cast(active_weeks_4w as string),
        ' active week(s) / ',
        if(active_repos_4w = 1, '1 repo', '2+ repos')
      ) as feature_bucket
    )
  ]) as feature
),

final as (
  select
    cohort_week,
    feature_name,
    feature_bucket,
    count(*) as actors,
    countif(active_w12) as active_actors_w12,
    safe_divide(countif(active_w12), count(*)) as survival_rate_w12,
    countif(active_w26) as active_actors_w26,
    safe_divide(countif(active_w26), count(*)) as survival_rate_w26
  from long_features
  group by cohort_week, feature_name, feature_bucket
)

select *
from final
order by cohort_week, feature_name, feature_bucket
