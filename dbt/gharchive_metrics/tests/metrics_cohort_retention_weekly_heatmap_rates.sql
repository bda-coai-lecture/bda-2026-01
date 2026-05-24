select *
from {{ ref('metrics_cohort_retention_weekly_heatmap') }}
where cohort_users <= 0
  or w0_active_users != cohort_users
  or w0_retention != 1.0
  or exists (
    select 1
    from unnest([
      w0_retention, w1_retention, w2_retention, w3_retention, w4_retention,
      w5_retention, w6_retention, w7_retention, w8_retention, w9_retention,
      w10_retention, w11_retention, w12_retention
    ]) as rate
    where rate < 0 or rate > 1
  )
