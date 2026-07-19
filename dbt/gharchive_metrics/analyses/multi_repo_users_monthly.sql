-- Question: How has multi-repository actor activity changed over time?
-- Grain: calendar month
-- Time basis / timezone: activity_date, UTC
-- Validation range: 2026-06-24 through 2026-06-30
-- Analysis range: parameterized; intended for complete calendar months
-- Segments: active repositories per actor-month: 1, 2-4, 5-9, 10+
-- Exclusions: null actor/repo/action/event_count/date are excluded upstream
-- Metric definition: multi-repo actor = actor active in 2+ distinct repositories in a calendar month
-- Expected output grain: one row per calendar month

{% set analysis_start_date = var('analysis_start_date', '2025-05-01') %}
{% set analysis_end_date = var('analysis_end_date', '2026-06-30') %}

with params as (
  select
    date('{{ analysis_start_date }}') as start_date,
    date('{{ analysis_end_date }}') as end_date
),

filtered_activity as (
  select
    date_trunc(activity_date, month) as month_start,
    user_id,
    repo_id,
    event_count
  from {{ ref('fact_user_repo_activity') }}
  cross join params
  where activity_date between start_date and end_date
),

actor_month as (
  select
    month_start,
    user_id,
    count(distinct repo_id) as active_repos,
    sum(event_count) as total_events
  from filtered_activity
  group by month_start, user_id
),

monthly as (
  select
    month_start,
    count(*) as active_actors,
    countif(active_repos >= 2) as multi_repo_actors,
    countif(active_repos = 1) as one_repo_actors,
    countif(active_repos between 2 and 4) as actors_2_4_repos,
    countif(active_repos between 5 and 9) as actors_5_9_repos,
    countif(active_repos >= 10) as actors_10_plus_repos,
    safe_divide(countif(active_repos >= 2), count(*)) as multi_repo_actor_share,
    avg(active_repos) as avg_repos_per_actor,
    approx_quantiles(active_repos, 100)[offset(50)] as median_repos_per_actor,
    sum(total_events) as total_events,
    sum(if(active_repos >= 2, total_events, 0)) as multi_repo_actor_events
  from actor_month
  group by month_start
),

final as (
  select
    *,
    safe_divide(multi_repo_actor_events, total_events) as multi_repo_event_share,
    safe_divide(
      multi_repo_actors - lag(multi_repo_actors) over (order by month_start),
      lag(multi_repo_actors) over (order by month_start)
    ) as multi_repo_actor_mom_change
  from monthly
)

select *
from final
order by month_start
