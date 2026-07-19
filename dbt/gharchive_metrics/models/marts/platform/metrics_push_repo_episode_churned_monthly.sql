{{
  config(
    materialized='table',
    partition_by={
      "field": "churn_month",
      "data_type": "date",
      "granularity": "month"
    }
  )
}}

with churned_episodes as (
  select
    *,
    date_trunc(churn_date, month) as churn_month
  from {{ ref('fct_push_repo_episode') }}
  where is_churn_observable
),

monthly_counts as (
  select
    churn_month,
    max(data_through_date) as as_of_date,
    count(*) as churned_episode_count,
    avg(episode_duration_days) as avg_episode_duration_days,
    avg(normalized_entropy) as avg_normalized_entropy,
    countif(is_collaborative_at_entry) as collaborative_at_entry_count,
    countif(transitioned_to_collaboration) as solo_to_collaboration_count,
    countif(not is_ever_collaborative) as never_collaborative_count
  from churned_episodes
  group by churn_month
),

monthly_medians as (
  select distinct
    churn_month,
    percentile_cont(episode_duration_days, 0.5) over (
      partition by churn_month
    ) as median_episode_duration_days,
    percentile_cont(normalized_entropy, 0.5) over (
      partition by churn_month
    ) as median_normalized_entropy
  from churned_episodes
)

select
  counts.*,
  medians.median_episode_duration_days,
  medians.median_normalized_entropy
from monthly_counts as counts
join monthly_medians as medians
  using (churn_month)
