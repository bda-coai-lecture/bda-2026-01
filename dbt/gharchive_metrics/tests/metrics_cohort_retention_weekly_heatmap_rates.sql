select *
from {{ ref('metrics_cohort_retention_weekly_heatmap') }}
where active_users <= 0
  or exists (
    select 1
    from unnest([
      w1, w2, w3, w4, w5, w6, w7, w8, w9, w10, w11, w12
    ]) as rate
    where rate < 0 or rate > 1
  )
