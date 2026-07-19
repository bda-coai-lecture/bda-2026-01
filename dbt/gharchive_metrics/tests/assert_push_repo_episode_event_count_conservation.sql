with push_repo_days as (
  select
    activity_date,
    repo_id,
    sum(event_count) as push_events
  from {{ ref('fact_user_repo_activity') }}
  where action = 'PushEvent'
  group by activity_date, repo_id
),

data_bounds as (
  select min(activity_date) as source_start_date
  from push_repo_days
),

sequenced as (
  select
    *,
    lag(activity_date) over (
      partition by repo_id
      order by activity_date
    ) as previous_push_date
  from push_repo_days
),

numbered as (
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
  from sequenced
),

eligible_source as (
  select sum(push_events) as push_events
  from (
    select
      *,
      min(activity_date) over (
        partition by repo_id, episode_number
      ) as episode_start_date
    from numbered
  )
  cross join data_bounds
  where episode_start_date >= date_add(source_start_date, interval 28 day)
),

episode_mart as (
  select sum(total_push_events) as push_events
  from {{ ref('fct_push_repo_episode') }}
)

select
  source.push_events as source_push_events,
  mart.push_events as mart_push_events
from eligible_source as source
cross join episode_mart as mart
where source.push_events != mart.push_events
