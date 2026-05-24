with monthly_active_users as (
  select
    month_start,
    count(distinct user_id) as monthly_active_users
  from {{ ref('stg_user_repo_activity') }}
  group by month_start
)

select
  heatmap.month_start,
  heatmap.active_users as heatmap_active_users,
  monthly_active_users.monthly_active_users
from {{ ref('metrics_cohort_retention_monthly_heatmap') }} as heatmap
full outer join monthly_active_users
  using (month_start)
where coalesce(heatmap.active_users, -1) != coalesce(monthly_active_users.monthly_active_users, -1)
