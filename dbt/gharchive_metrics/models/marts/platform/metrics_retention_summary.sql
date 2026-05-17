select
  cohort_week,
  max(cohort_users) as cohort_users,
  max(if(weeks_since = 0, retention_rate, 0.0)) as w0_retention,
  max(if(weeks_since = 1, retention_rate, 0.0)) as w1_retention,
  max(if(weeks_since = 2, retention_rate, 0.0)) as w2_retention,
  max(if(weeks_since = 3, retention_rate, 0.0)) as w3_retention
from {{ ref('metrics_retention_weekly') }}
group by cohort_week
