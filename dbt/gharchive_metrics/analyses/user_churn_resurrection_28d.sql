-- Question: Under a 28-day inactivity rule, how often do actors churn and later resurrect?
-- Grain: final output is one row per lifecycle metric; intermediate grain is actor activity day / episode.
-- Time basis / timezone: fact activity_date, UTC.
-- Validation range: parameterized; validate on a bounded source window before full execution.
-- Analysis range: 2025-05-01 through 2026-07-18 by default.
-- Segments: first churn, resurrection within 28/84/180 days, repeated churn episodes.
-- Exclusions: null keys excluded upstream; churn and resurrection horizons without full follow-up are excluded.
-- Metric definition: churn occurs after 28 consecutive inactive days; any later activity is resurrection.
-- Expected output grain: metric_name.

{% set analysis_start_date = var('analysis_start_date', '2025-05-01') %}
{% set analysis_end_date = var('analysis_end_date', '2026-07-18') %}
{% set activity_scope = var('activity_scope', 'all') %}
{% set exclude_push_automation = var('exclude_push_automation', false) %}

with params as (
  select
    date('{{ analysis_start_date }}') as analysis_start_date,
    date('{{ analysis_end_date }}') as analysis_end_date
),

actor_days as (
  select distinct
    user_id,
    activity_date
  from {{ ref('fact_user_repo_activity') }} as activity
  cross join params
  where activity_date between analysis_start_date and analysis_end_date
    {% if activity_scope == 'push' %}
    and action = 'PushEvent'
    {% endif %}
    {% if exclude_push_automation %}
    and not exists (
      select 1
      from {{ ref('dim_push_automation_actor') }} as automation
      where automation.user_id = activity.user_id
    )
    {% endif %}
),

sequenced_days as (
  select
    user_id,
    activity_date,
    lag(activity_date) over (
      partition by user_id
      order by activity_date
    ) as previous_activity_date,
    lead(activity_date) over (
      partition by user_id
      order by activity_date
    ) as next_activity_date
  from actor_days
),

episode_starts as (
  select
    *,
    countif(
      previous_activity_date is null
      or date_diff(activity_date, previous_activity_date, day) > 28
    ) over (
      partition by user_id
      order by activity_date
      rows between unbounded preceding and current row
    ) as episode_number
  from sequenced_days
),

episodes as (
  select
    user_id,
    episode_number,
    min(activity_date) as episode_start_date,
    max(activity_date) as episode_last_activity_date
  from episode_starts
  group by user_id, episode_number
),

episode_lifecycle as (
  select
    episode.*,
    date_add(episode_last_activity_date, interval 28 day) as churn_date,
    lead(episode_start_date) over (
      partition by user_id
      order by episode_number
    ) as resurrection_date
  from episodes as episode
),

observable_episodes as (
  select
    lifecycle.*,
    resurrection_date is not null as observed_resurrection,
    date_diff(resurrection_date, churn_date, day) as days_churned_before_resurrection
  from episode_lifecycle as lifecycle
  cross join params
  where churn_date <= analysis_end_date
),

actor_summary as (
  select
    user_id,
    min(episode_start_date) as first_seen_date,
    min(churn_date) as first_churn_date,
    min(if(episode_number = 1, resurrection_date, null)) as first_resurrection_date,
    count(*) as observable_churn_episodes,
    countif(observed_resurrection) as observed_resurrection_episodes
  from observable_episodes
  group by user_id
),

first_churn as (
  select
    lifecycle.user_id,
    lifecycle.episode_start_date as first_seen_date,
    lifecycle.churn_date as first_churn_date,
    lifecycle.resurrection_date as first_resurrection_date,
    date_diff(lifecycle.churn_date, lifecycle.episode_start_date, day) as days_to_first_churn,
    date_diff(lifecycle.resurrection_date, lifecycle.churn_date, day) as days_to_first_resurrection
  from episode_lifecycle as lifecycle
  cross join params
  where lifecycle.episode_number = 1
    and lifecycle.churn_date <= analysis_end_date
),

population as (
  select
    count(distinct user_id) as observed_actors,
    count(distinct if(
      activity_date <= date_sub(analysis_end_date, interval 28 day),
      user_id,
      null
    )) as actors_with_minimum_churn_followup
  from actor_days
  cross join params
),

metric_rows as (
  select
    'observed_actors' as metric_name,
    cast(observed_actors as float64) as metric_value,
    observed_actors as denominator,
    observed_actors as numerator
  from population

  union all

  select
    'actors_with_observed_first_churn',
    cast(count(*) as float64),
    population.actors_with_minimum_churn_followup,
    count(*)
  from first_churn
  cross join population
  group by population.actors_with_minimum_churn_followup

  union all

  select
    'first_churn_rate_among_followup_eligible',
    safe_divide(count(*), population.actors_with_minimum_churn_followup),
    population.actors_with_minimum_churn_followup,
    count(*)
  from first_churn
  cross join population
  group by population.actors_with_minimum_churn_followup

  union all

  select
    'first_churn_resurrected_within_28d',
    safe_divide(
      countif(
        first_resurrection_date > first_churn_date
        and first_resurrection_date <= date_add(first_churn_date, interval 28 day)
      ),
      count(*)
    ),
    count(*),
    countif(
      first_resurrection_date > first_churn_date
      and first_resurrection_date <= date_add(first_churn_date, interval 28 day)
    )
  from first_churn
  cross join params
  where date_add(first_churn_date, interval 28 day) <= analysis_end_date

  union all

  select
    'first_churn_resurrected_within_84d',
    safe_divide(
      countif(
        first_resurrection_date > first_churn_date
        and first_resurrection_date <= date_add(first_churn_date, interval 84 day)
      ),
      count(*)
    ),
    count(*),
    countif(
      first_resurrection_date > first_churn_date
      and first_resurrection_date <= date_add(first_churn_date, interval 84 day)
    )
  from first_churn
  cross join params
  where date_add(first_churn_date, interval 84 day) <= analysis_end_date

  union all

  select
    'first_churn_resurrected_within_180d',
    safe_divide(
      countif(
        first_resurrection_date > first_churn_date
        and first_resurrection_date <= date_add(first_churn_date, interval 180 day)
      ),
      count(*)
    ),
    count(*),
    countif(
      first_resurrection_date > first_churn_date
      and first_resurrection_date <= date_add(first_churn_date, interval 180 day)
    )
  from first_churn
  cross join params
  where date_add(first_churn_date, interval 180 day) <= analysis_end_date

  union all

  select
    'days_to_first_churn_p50',
    cast(approx_quantiles(days_to_first_churn, 100)[offset(50)] as float64),
    count(*),
    null
  from first_churn

  union all

  select
    'days_to_first_churn_p75',
    cast(approx_quantiles(days_to_first_churn, 100)[offset(75)] as float64),
    count(*),
    null
  from first_churn

  union all

  select
    'days_churned_before_first_resurrection_p50',
    cast(approx_quantiles(days_to_first_resurrection, 100)[offset(50)] as float64),
    countif(first_resurrection_date is not null),
    null
  from first_churn

  union all

  select
    'actors_with_multiple_observable_churn_episodes',
    safe_divide(countif(observable_churn_episodes >= 2), count(*)),
    count(*),
    countif(observable_churn_episodes >= 2)
  from actor_summary
)

select *
from metric_rows
order by metric_name
