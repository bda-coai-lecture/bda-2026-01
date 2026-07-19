-- Question: How did Push actor and repository composition change in every complete month?
-- Grain: calendar month
-- Time basis / timezone: activity_date, UTC
-- Validation range: 2026-06-24 through 2026-06-30
-- Analysis range: 2025-05-01 through 2026-06-30
-- Segments: one-repo vs multi-repo actor; single-actor vs multi-actor repo
-- Exclusions: non-Push events; null keys and nonpositive event_count are excluded upstream
-- Metric definition: low intensity = one repo/actor, one active day, and at most two PushEvents in the month
-- Expected output grain: one row per calendar month

{% set analysis_start_date = var('analysis_start_date', '2025-05-01') %}
{% set analysis_end_date = var('analysis_end_date', '2026-06-30') %}

with push_activity as (
  select
    activity_date,
    date_trunc(activity_date, month) as month_start,
    user_id,
    repo_id,
    event_count
  from {{ ref('fact_user_repo_activity') }}
  where activity_date between date('{{ analysis_start_date }}') and date('{{ analysis_end_date }}')
    and action = 'PushEvent'
),

actor_month as (
  select
    month_start,
    user_id,
    count(distinct repo_id) as push_repos,
    count(distinct activity_date) as push_days,
    sum(event_count) as push_events
  from push_activity
  group by month_start, user_id
),

actor_metrics as (
  select
    month_start,
    count(*) as push_actors,
    countif(push_repos = 1) as one_repo_actors,
    countif(push_repos >= 2) as multi_repo_actors,
    safe_divide(countif(push_repos = 1), count(*)) as one_repo_actor_share,
    safe_divide(
      countif(push_repos = 1 and push_days = 1 and push_events <= 2),
      count(*)
    ) as low_intensity_one_repo_actor_share
  from actor_month
  group by month_start
),

repo_month as (
  select
    month_start,
    repo_id,
    count(distinct user_id) as actors,
    count(distinct activity_date) as push_days,
    sum(event_count) as push_events
  from push_activity
  group by month_start, repo_id
),

repo_metrics as (
  select
    month_start,
    count(*) as push_repos,
    countif(actors = 1) as single_actor_repos,
    countif(actors >= 2) as multi_actor_repos,
    safe_divide(countif(actors = 1), count(*)) as single_actor_repo_share,
    safe_divide(
      countif(actors = 1 and push_days = 1 and push_events <= 2),
      count(*)
    ) as low_intensity_single_actor_repo_share,
    approx_quantiles(push_events, 100)[offset(50)] as median_push_events
  from repo_month
  group by month_start
),

event_metrics as (
  select
    month_start,
    sum(event_count) as push_events,
    safe_divide(sum(event_count), count(distinct activity_date)) as push_events_per_day
  from push_activity
  group by month_start
)

select *
from actor_metrics
join repo_metrics using (month_start)
join event_metrics using (month_start)
order by month_start
