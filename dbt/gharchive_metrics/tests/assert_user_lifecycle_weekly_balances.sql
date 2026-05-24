select *
from {{ ref('metrics_user_lifecycle_weekly') }}
where has_complete_previous_period
  and (
    active_users != existing_users + returning_or_historical_unknown_users
    or previous_active_users != existing_users + churned_users
    or retention_rate < 0
    or retention_rate > 1
    or churn_rate < 0
    or churn_rate > 1
    or returning_or_historical_unknown_share < 0
    or returning_or_historical_unknown_share > 1
  )
