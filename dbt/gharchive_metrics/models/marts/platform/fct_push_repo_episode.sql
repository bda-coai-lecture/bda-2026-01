{{
  config(
    materialized='table',
    partition_by={
      "field": "episode_start_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["repo_id", "is_collaborative_at_entry"]
  )
}}

with push_actor_days as (
  select
    activity_date,
    repo_id,
    user_id,
    sum(event_count) as push_events
  from {{ ref('fact_user_repo_activity') }}
  where action = 'PushEvent'
  group by activity_date, repo_id, user_id
),

data_bounds as (
  select
    min(activity_date) as source_start_date,
    max(activity_date) as data_through_date
  from push_actor_days
),

repo_days as (
  select
    activity_date,
    repo_id,
    sum(push_events) as push_events
  from push_actor_days
  group by activity_date, repo_id
),

sequenced_repo_days as (
  select
    *,
    lag(activity_date) over (
      partition by repo_id
      order by activity_date
    ) as previous_push_date
  from repo_days
),

numbered_repo_days as (
  select
    *,
    countif(
      previous_push_date is null
      or date_diff(activity_date, previous_push_date, day) > 28
    ) over (
      partition by repo_id
      order by activity_date
      rows between unbounded preceding and current row
    ) as episode_number
  from sequenced_repo_days
),

episode_days as (
  select
    repo_id,
    episode_number,
    min(activity_date) over (
      partition by repo_id, episode_number
    ) as episode_start_date,
    activity_date,
    previous_push_date,
    push_events
  from numbered_repo_days
),

eligible_episode_days as (
  select episode.*
  from episode_days as episode
  cross join data_bounds
  where episode_start_date >= date_add(source_start_date, interval 28 day)
),

episode_actor_days as (
  select
    episode.repo_id,
    episode.episode_number,
    episode.episode_start_date,
    actor_day.activity_date,
    actor_day.user_id,
    actor_day.push_events,
    div(
      date_diff(actor_day.activity_date, episode.episode_start_date, day),
      7
    ) as week_index
  from eligible_episode_days as episode
  join push_actor_days as actor_day
    using (repo_id, activity_date)
),

episode_core as (
  select
    repo_id,
    episode_number,
    episode_start_date,
    max(activity_date) as last_push_date,
    count(distinct activity_date) as push_days,
    count(distinct user_id) as lifetime_actor_count,
    count(distinct if(
      activity_date <= date_add(episode_start_date, interval 6 day),
      user_id,
      null
    )) as entry_actor_count,
    sum(push_events) as total_push_events,
    count(distinct week_index) as active_weeks
  from episode_actor_days
  group by repo_id, episode_number, episode_start_date
),

episode_lifecycle as (
  select
    core.*,
    lag(last_push_date) over (
      partition by repo_id
      order by episode_start_date
    ) as previous_episode_last_push_date,
    lead(episode_start_date) over (
      partition by repo_id
      order by episode_start_date
    ) as next_episode_start_date
  from episode_core as core
),

actor_first_push as (
  select
    repo_id,
    episode_number,
    episode_start_date,
    user_id,
    min(activity_date) as actor_first_push_date
  from episode_actor_days
  group by repo_id, episode_number, episode_start_date, user_id
),

episode_collaboration as (
  select
    repo_id,
    episode_number,
    episode_start_date,
    array_agg(
      actor_first_push_date
      order by actor_first_push_date, user_id
    )[safe_offset(1)] as first_collaboration_date
  from actor_first_push
  group by repo_id, episode_number, episode_start_date
),

weekly_pushes as (
  select
    repo_id,
    episode_number,
    episode_start_date,
    week_index,
    sum(push_events) as weekly_push_events
  from episode_actor_days
  group by repo_id, episode_number, episode_start_date, week_index
),

episode_entropy as (
  select
    weekly.repo_id,
    weekly.episode_number,
    weekly.episode_start_date,
    -sum(
      safe_divide(weekly.weekly_push_events, core.total_push_events)
      * ln(safe_divide(weekly.weekly_push_events, core.total_push_events))
    ) as entropy_nats
  from weekly_pushes as weekly
  join episode_core as core
    using (repo_id, episode_number, episode_start_date)
  group by weekly.repo_id, weekly.episode_number, weekly.episode_start_date
),

final as (
  select
    to_hex(md5(concat(
      cast(core.repo_id as string),
      '|',
      cast(core.episode_start_date as string)
    ))) as episode_id,
    core.repo_id,
    core.episode_start_date,
    date_trunc(core.episode_start_date, month) as cohort_month,
    core.previous_episode_last_push_date,
    date_diff(
      core.episode_start_date,
      core.previous_episode_last_push_date,
      day
    ) as days_since_previous_episode_push,
    core.next_episode_start_date,
    core.last_push_date,
    date_add(core.last_push_date, interval 28 day) as churn_date,
    date_add(core.last_push_date, interval 28 day) <= bounds.data_through_date
      as is_churn_observable,
    if(
      date_add(core.last_push_date, interval 28 day) <= bounds.data_through_date,
      'churned',
      'active_as_of'
    ) as episode_status,
    date_diff(core.last_push_date, core.episode_start_date, day) + 1
      as episode_duration_days,
    core.entry_actor_count,
    core.lifetime_actor_count,
    core.entry_actor_count >= 2 as is_collaborative_at_entry,
    core.entry_actor_count = 1 and core.lifetime_actor_count >= 2
      as transitioned_to_collaboration,
    core.lifetime_actor_count >= 2 as is_ever_collaborative,
    collaboration.first_collaboration_date,
    date_add(core.episode_start_date, interval 6 day) <= bounds.data_through_date
      as is_entry_window_complete,
    core.push_days,
    core.total_push_events,
    core.active_weeks,
    div(date_diff(core.last_push_date, core.episode_start_date, day), 7) + 1
      as entropy_calendar_weeks,
    entropy.entropy_nats,
    case
      when div(date_diff(core.last_push_date, core.episode_start_date, day), 7) + 1 = 1
        then 0.0
      else safe_divide(
        entropy.entropy_nats,
        ln(div(date_diff(core.last_push_date, core.episode_start_date, day), 7) + 1)
      )
    end as normalized_entropy,
    bounds.source_start_date,
    bounds.data_through_date,
    not (
      date_add(core.last_push_date, interval 28 day) <= bounds.data_through_date
    ) as is_provisional
  from episode_lifecycle as core
  join episode_entropy as entropy
    using (repo_id, episode_number, episode_start_date)
  join episode_collaboration as collaboration
    using (repo_id, episode_number, episode_start_date)
  cross join data_bounds as bounds
)

select *
from final
