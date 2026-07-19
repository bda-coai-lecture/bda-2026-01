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

with episodes as (
  select *
  from {{ ref('fct_push_repo_episode') }}
),

monthly_counts as (
  select
    cohort_month,
    max(data_through_date) as as_of_date,
    count(*) as episode_count,
    countif(is_provisional) as provisional_episode_count,
    safe_divide(countif(is_provisional), count(*)) as provisional_episode_share,
    countif(is_collaborative_at_entry and is_entry_window_complete)
      as collaborative_at_entry_count,
    safe_divide(
      countif(is_collaborative_at_entry and is_entry_window_complete),
      countif(is_entry_window_complete)
    ) as collaborative_at_entry_share,
    countif(entry_actor_count = 1) as solo_entry_count,
    countif(transitioned_to_collaboration) as solo_to_collaboration_count,
    safe_divide(
      countif(transitioned_to_collaboration),
      countif(entry_actor_count = 1)
    ) as solo_to_collaboration_share_as_of,
    avg(normalized_entropy) as avg_normalized_entropy_as_of,
    avg(if(is_churn_observable, normalized_entropy, null))
      as finalized_avg_normalized_entropy,
    countif(is_churn_observable) as finalized_episode_count
  from episodes
  group by cohort_month
),

monthly_medians as (
  select distinct
    cohort_month,
    percentile_cont(normalized_entropy, 0.5) over (
      partition by cohort_month
    ) as median_normalized_entropy_as_of,
    percentile_cont(
      if(is_churn_observable, normalized_entropy, null),
      0.5 ignore nulls
    ) over (
      partition by cohort_month
    ) as finalized_median_normalized_entropy
  from episodes
)

select
  counts.*,
  medians.median_normalized_entropy_as_of,
  medians.finalized_median_normalized_entropy
from monthly_counts as counts
join monthly_medians as medians
  using (cohort_month)
