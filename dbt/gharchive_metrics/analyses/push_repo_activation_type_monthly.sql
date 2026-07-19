-- Question: What mutually exclusive activation type does each Push repository episode follow?
-- Grain: activation cohort month x project type
-- Time basis / timezone: activity_date, UTC; first seven days and following eight weeks
-- Validation range: 2025-08 through 2026-04 complete cohorts
-- Analysis range: source 2025-05-01 through 2026-06-25
-- Segments: early ended, solo sustained, solo-to-collaboration, collaborative from entry
-- Exclusions: non-Push events; left- and right-censored cohort months
-- Metric definition: activation = Push after 90 complete inactive days
-- Expected output grain: one row per activation cohort month x project type

with push_base as (
  select
    activity_date,
    repo_id,
    user_id
  from {{ ref('fact_user_repo_activity') }}
  where activity_date between date('2025-05-01') and date('2026-06-25')
    and action = 'PushEvent'
),

repo_days as (
  select activity_date, repo_id
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
  where activity_date between date('2025-08-01') and date('2026-04-30')
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
    activity.user_id,
    div(date_diff(activity.activity_date, episode.activation_date, day), 7) as week_index
  from activation_episodes as episode
  join push_base as activity
    on activity.repo_id = episode.repo_id
    and activity.activity_date between episode.activation_date
                                   and date_add(episode.activation_date, interval 55 day)
),

episode_metrics as (
  select
    cohort_month,
    repo_id,
    activation_date,
    count(distinct if(week_index = 0, user_id, null)) as entry_actors,
    count(distinct user_id) as actors_8w,
    max(week_index) as last_active_week
  from episode_activity
  group by cohort_month, repo_id, activation_date
),

classified as (
  select
    *,
    case
      when entry_actors >= 2 then '처음부터 협업'
      when actors_8w >= 2 then '혼자 시작 후 협업'
      when last_active_week >= 4 then '혼자 시작해 지속'
      else '초기 종료형'
    end as project_type
  from episode_metrics
)

select
  cohort_month,
  project_type,
  count(*) as projects,
  safe_divide(
    count(*),
    sum(count(*)) over (partition by cohort_month)
  ) as project_share
from classified
group by cohort_month, project_type
order by cohort_month, project_type
