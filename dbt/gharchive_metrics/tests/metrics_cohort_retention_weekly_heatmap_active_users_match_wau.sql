select
  heatmap.week_start,
  heatmap.active_users as heatmap_active_users,
  weekly.weekly_active_users
from {{ ref('metrics_cohort_retention_weekly_heatmap') }} as heatmap
full outer join {{ ref('metrics_weekly') }} as weekly
  using (week_start)
where coalesce(heatmap.active_users, -1) != coalesce(weekly.weekly_active_users, -1)
