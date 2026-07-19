-- Question: How did the temporal concentration and later reactivation of Push repo activation episodes change each month?
-- Grain: activation cohort month x entry-week actor segment
-- Time basis / timezone: activity_date, UTC; eight 7-day bins anchored on activation_date
-- Validation range: 2025-08 and 2026-04 cohorts checked against bounded source windows
-- Analysis range: source 2025-05-01 through 2026-06-25; complete cohorts 2025-08 through 2026-04
-- Segments: one actor vs multiple actors during the entry week (first seven days)
-- Exclusions: non-Push events; left-censored months before 2025-08; right-censored months after 2026-04
-- Metric definition: activation = Push after 90 complete inactive days; entropy = Shannon entropy of eight weekly PushEvent shares / ln(8)
-- Expected output grain: one row per activation cohort month x entry segment

{% set source_start_date = var('entropy_source_start_date', '2025-05-01') %}
{% set source_end_date = var('entropy_source_end_date', '2026-06-25') %}
{% set cohort_start_month = var('entropy_cohort_start_month', '2025-08-01') %}
{% set cohort_end_month = var('entropy_cohort_end_month', '2026-04-30') %}

with push_base as (
  select
    activity_date,
    repo_id,
    user_id,
    event_count
  from {{ ref('fact_user_repo_activity') }}
  where activity_date between date('{{ source_start_date }}') and date('{{ source_end_date }}')
    and action = 'PushEvent'
),

repo_days as (
  select
    activity_date,
    repo_id,
    sum(event_count) as pushes
  from push_base
  group by activity_date, repo_id
),

repo_days_lagged as (
  select
    *,
    lag(activity_date) over (
      partition by repo_id
      order by activity_date
    ) as previous_push_date
  from repo_days
),

activation_episodes as (
  select
    repo_id,
    activity_date as activation_date,
    date_trunc(activity_date, month) as cohort_month
  from repo_days_lagged
  where activity_date between date('{{ cohort_start_month }}') and date('{{ cohort_end_month }}')
    and (
      previous_push_date is null
      or date_diff(activity_date, previous_push_date, day) > 90
    )
),

episode_activity as (
  select
    episode.cohort_month,
    episode.repo_id,
    episode.activation_date,
    activity.activity_date,
    activity.user_id,
    activity.event_count,
    div(date_diff(activity.activity_date, episode.activation_date, day), 7) as week_index
  from activation_episodes as episode
  join push_base as activity
    on activity.repo_id = episode.repo_id
    and activity.activity_date between episode.activation_date
                                   and date_add(episode.activation_date, interval 55 day)
),

weekly as (
  select
    cohort_month,
    repo_id,
    activation_date,
    week_index,
    sum(event_count) as pushes
  from episode_activity
  group by cohort_month, repo_id, activation_date, week_index
),

episode_totals as (
  select
    cohort_month,
    repo_id,
    activation_date,
    sum(pushes) as total_push_events,
    count(*) as active_weeks,
    max(week_index) as last_active_week
  from weekly
  group by cohort_month, repo_id, activation_date
),

actor_counts as (
  select
    cohort_month,
    repo_id,
    activation_date,
    count(distinct if(week_index = 0, user_id, null)) as entry_week_actors,
    count(distinct user_id) as actors_8w
  from episode_activity
  group by cohort_month, repo_id, activation_date
),

entropy as (
  select
    weekly.cohort_month,
    weekly.repo_id,
    weekly.activation_date,
    -sum(
      safe_divide(weekly.pushes, totals.total_push_events)
      * ln(safe_divide(weekly.pushes, totals.total_push_events))
    ) / ln(8) as normalized_entropy
  from weekly
  join episode_totals as totals
    using (cohort_month, repo_id, activation_date)
  group by weekly.cohort_month, weekly.repo_id, weekly.activation_date
),

episode_metrics as (
  select
    totals.*,
    actors.entry_week_actors,
    actors.actors_8w,
    entropy.normalized_entropy,
    if(actors.entry_week_actors = 1, 'entry_single', 'entry_multi') as entry_segment
  from episode_totals as totals
  join actor_counts as actors
    using (cohort_month, repo_id, activation_date)
  join entropy
    using (cohort_month, repo_id, activation_date)
)

select
  cohort_month,
  entry_segment,
  count(*) as activation_episodes,
  avg(normalized_entropy) as avg_entropy,
  approx_quantiles(normalized_entropy, 100)[offset(50)] as median_entropy,
  avg(active_weeks) as avg_active_weeks,
  safe_divide(countif(normalized_entropy <= 0.25), count(*)) as low_entropy_share,
  safe_divide(countif(last_active_week >= 4), count(*)) as week_5_plus_return_share,
  safe_divide(countif(last_active_week = 7), count(*)) as week_8_return_share,
  approx_quantiles(total_push_events, 100)[offset(50)] as median_push_events,
  safe_divide(
    countif(entry_week_actors = 1 and actors_8w >= 2),
    countif(entry_week_actors = 1)
  ) as entry_single_to_multi_share
from episode_metrics
group by cohort_month, entry_segment
order by cohort_month, entry_segment
