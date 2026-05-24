select *
from {{ ref('metrics_cohort_retention_monthly_heatmap') }}
where cohort_users <= 0
  or m0_active_users != cohort_users
  or m0_retention != 1.0
  or exists (
    select 1
    from unnest([
      m0_retention, m1_retention, m2_retention, m3_retention, m4_retention,
      m5_retention, m6_retention, m7_retention, m8_retention, m9_retention,
      m10_retention, m11_retention, m12_retention
    ]) as rate
    where rate < 0 or rate > 1
  )
