{{
  config(
    materialized='table',
    partition_by={
      "field": "cohort_month",
      "data_type": "date",
      "granularity": "month"
    }
  )
}}

with eligible_episodes as (
  select
    episode_id,
    repo_id,
    episode_start_date,
    cohort_month,
    last_push_date,
    next_episode_start_date,
    data_through_date
  from {{ ref('fct_push_repo_episode') }}
  where date_add(episode_start_date, interval 29 day) <= data_through_date
),

push_days_in_fixed_window as (
  select
    episode.episode_id,
    episode.cohort_month,
    episode.episode_start_date,
    max(episode.last_push_date) as episode_last_push_date,
    max(activity.activity_date) as last_push_date_in_30d
  from eligible_episodes as episode
  join {{ ref('fact_user_repo_activity') }} as activity
    on activity.repo_id = episode.repo_id
   and activity.action = 'PushEvent'
   and activity.activity_date between episode.episode_start_date
                                  and date_add(episode.episode_start_date, interval 29 day)
   and (
     episode.next_episode_start_date is null
     or activity.activity_date < episode.next_episode_start_date
   )
  group by episode.episode_id, episode.cohort_month, episode.episode_start_date
),

monthly as (
  select
    cohort_month,
    count(*) as episode_count_with_complete_30d_window,
    avg(
      date_diff(last_push_date_in_30d, episode_start_date, day) + 1
    ) as avg_observed_duration_days_30d,
    countif(
      episode_last_push_date >= date_add(episode_start_date, interval 29 day)
    ) as reached_day_30_count,
    safe_divide(
      countif(episode_last_push_date >= date_add(episode_start_date, interval 29 day)),
      count(*)
    ) as reached_day_30_share
  from push_days_in_fixed_window
  group by cohort_month
)

select *
from monthly
