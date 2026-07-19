-- Question: Which prior Push activity and repository patterns are associated with 28-day developer churn and resurrection?
-- Grain: Sunday cutoff x predictor x feature bucket.
-- Time basis / timezone: fact activity_date, UTC.
-- Validation range: one Sunday cutoff, 2026-04-19 by default.
-- Analysis range: weekly cutoffs from 2026-02-01 through 2026-04-19 by default.
-- Segments: prior-28-day active days/weeks/events, repo breadth, repeated repos, shared-repo exposure.
-- Exclusions: non-Push events; actors without Push in the final 7 days before cutoff.
-- Metric definition: churn = no Push in cutoff+1..28; resurrection = any Push in cutoff+29..84 among churned actors.
-- Expected output grain: cutoff_date x predictor x feature_bucket.

{% set cohort_start_date = var('cohort_start_date', '2026-02-01') %}
{% set cohort_end_date = var('cohort_end_date', '2026-04-19') %}
{% set exclude_push_automation = var('exclude_push_automation', false) %}

with params as (
  select
    date('{{ cohort_start_date }}') as cohort_start_date,
    date('{{ cohort_end_date }}') as cohort_end_date
),

requested_cutoffs as (
  select cutoff_date
  from params,
  unnest(generate_date_array(cohort_start_date, cohort_end_date, interval 7 day)) as cutoff_date
),

push_activity as (
  select
    activity_date,
    user_id,
    repo_id,
    sum(event_count) as push_events
  from {{ ref('fact_user_repo_activity') }} as activity
  cross join params
  where activity_date between date_sub(cohort_start_date, interval 27 day)
                          and date_add(cohort_end_date, interval 84 day)
    and action = 'PushEvent'
    {% if exclude_push_automation %}
    and not exists (
      select 1
      from {{ ref('dim_push_automation_actor') }} as automation
      where automation.user_id = activity.user_id
    )
    {% endif %}
  group by activity_date, user_id, repo_id
),

source_bounds as (
  select max(activity_date) as data_through_date
  from push_activity
),

cutoffs as (
  select cutoff_date
  from requested_cutoffs
  cross join source_bounds
  where date_add(cutoff_date, interval 84 day) <= data_through_date
),

observation_activity as (
  select
    cutoff.cutoff_date,
    activity.activity_date,
    activity.user_id,
    activity.repo_id,
    activity.push_events,
    div(date_diff(cutoff.cutoff_date, activity.activity_date, day), 7) as recency_week_bin
  from cutoffs as cutoff
  join push_activity as activity
    on activity.activity_date between date_sub(cutoff.cutoff_date, interval 27 day)
                                  and cutoff.cutoff_date
),

actor_repo_features as (
  select
    cutoff_date,
    user_id,
    repo_id,
    count(distinct recency_week_bin) as active_weeks_on_repo,
    sum(push_events) as repo_push_events
  from observation_activity
  group by cutoff_date, user_id, repo_id
),

repo_features as (
  select
    cutoff_date,
    repo_id,
    count(distinct user_id) as repo_actors_28d
  from observation_activity
  group by cutoff_date, repo_id
),

actor_features as (
  select
    activity.cutoff_date,
    activity.user_id,
    count(distinct activity.activity_date) as active_days_28d,
    count(distinct activity.recency_week_bin) as active_weeks_28d,
    count(distinct activity.repo_id) as repos_28d,
    sum(activity.push_events) as push_events_28d
  from observation_activity as activity
  group by activity.cutoff_date, activity.user_id
  having max(activity.activity_date) >= date_sub(activity.cutoff_date, interval 6 day)
),

actor_repo_rollup as (
  select
    actor_repo.cutoff_date,
    actor_repo.user_id,
    countif(actor_repo.active_weeks_on_repo >= 2) as repeated_repos_28d,
    countif(repo.repo_actors_28d >= 2) as shared_repos_28d
  from actor_repo_features as actor_repo
  join repo_features as repo
    using (cutoff_date, repo_id)
  group by actor_repo.cutoff_date, actor_repo.user_id
),

future_activity as (
  select
    cutoff.cutoff_date,
    activity.user_id,
    logical_or(
      activity.activity_date between date_add(cutoff.cutoff_date, interval 1 day)
                                 and date_add(cutoff.cutoff_date, interval 28 day)
    ) as pushed_next_28d,
    logical_or(
      activity.activity_date between date_add(cutoff.cutoff_date, interval 29 day)
                                 and date_add(cutoff.cutoff_date, interval 84 day)
    ) as pushed_days_29_84
  from cutoffs as cutoff
  join push_activity as activity
    on activity.activity_date between date_add(cutoff.cutoff_date, interval 1 day)
                                  and date_add(cutoff.cutoff_date, interval 84 day)
  group by cutoff.cutoff_date, activity.user_id
),

actor_dataset as (
  select
    feature.cutoff_date,
    feature.user_id,
    feature.active_days_28d,
    feature.active_weeks_28d,
    feature.repos_28d,
    feature.push_events_28d,
    repo.repeated_repos_28d,
    repo.shared_repos_28d,
    not coalesce(future.pushed_next_28d, false) as churned_28d,
    not coalesce(future.pushed_next_28d, false)
      and coalesce(future.pushed_days_29_84, false) as resurrected_days_29_84
  from actor_features as feature
  join actor_repo_rollup as repo
    using (cutoff_date, user_id)
  left join future_activity as future
    using (cutoff_date, user_id)
),

long_features as (
  select
    actor.cutoff_date,
    actor.user_id,
    actor.churned_28d,
    actor.resurrected_days_29_84,
    feature.predictor,
    feature.feature_bucket,
    feature.bucket_order
  from actor_dataset as actor
  cross join unnest([
    struct(
      'active_weeks_28d' as predictor,
      format('%d week(s)', actor.active_weeks_28d) as feature_bucket,
      actor.active_weeks_28d as bucket_order
    ),
    struct(
      'repos_28d',
      case
        when actor.repos_28d = 1 then '1 repo'
        when actor.repos_28d between 2 and 4 then '2-4 repos'
        when actor.repos_28d between 5 and 9 then '5-9 repos'
        else '10+ repos'
      end,
      case
        when actor.repos_28d = 1 then 1
        when actor.repos_28d between 2 and 4 then 2
        when actor.repos_28d between 5 and 9 then 3
        else 4
      end
    ),
    struct(
      'repeated_repos_28d',
      case
        when actor.repeated_repos_28d = 0 then '0 repeated repos'
        when actor.repeated_repos_28d = 1 then '1 repeated repo'
        else '2+ repeated repos'
      end,
      case
        when actor.repeated_repos_28d = 0 then 1
        when actor.repeated_repos_28d = 1 then 2
        else 3
      end
    ),
    struct(
      'shared_repos_28d',
      if(actor.shared_repos_28d = 0, '0 shared repos', '1+ shared repos'),
      if(actor.shared_repos_28d = 0, 1, 2)
    ),
    struct(
      'active_weeks_x_repo_breadth',
      concat(
        cast(actor.active_weeks_28d as string),
        ' week(s) | ',
        if(actor.repos_28d = 1, '1 repo', '2+ repos')
      ),
      actor.active_weeks_28d * 10 + if(actor.repos_28d = 1, 1, 2)
    ),
    struct(
      'push_events_28d',
      case
        when actor.push_events_28d = 1 then '1 push'
        when actor.push_events_28d between 2 and 4 then '2-4 pushes'
        when actor.push_events_28d between 5 and 19 then '5-19 pushes'
        when actor.push_events_28d between 20 and 99 then '20-99 pushes'
        else '100+ pushes'
      end,
      case
        when actor.push_events_28d = 1 then 1
        when actor.push_events_28d between 2 and 4 then 2
        when actor.push_events_28d between 5 and 19 then 3
        when actor.push_events_28d between 20 and 99 then 4
        else 5
      end
    )
  ]) as feature
),

final as (
  select
    cutoff_date,
    predictor,
    feature_bucket,
    bucket_order,
    count(*) as developer_landmarks,
    countif(churned_28d) as churned_developer_landmarks,
    safe_divide(countif(churned_28d), count(*)) as churn_rate_28d,
    countif(resurrected_days_29_84) as resurrected_developer_landmarks,
    safe_divide(
      countif(resurrected_days_29_84),
      countif(churned_28d)
    ) as resurrection_rate_days_29_84
  from long_features
  group by cutoff_date, predictor, feature_bucket, bucket_order
)

select *
from final
order by cutoff_date, predictor, bucket_order
