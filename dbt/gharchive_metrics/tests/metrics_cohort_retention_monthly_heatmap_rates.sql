select *
from {{ ref('metrics_cohort_retention_monthly_heatmap') }}
where active_users <= 0
  or exists (
    select 1
    from unnest([
      m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12
    ]) as rate
    where rate < 0 or rate > 1
  )
