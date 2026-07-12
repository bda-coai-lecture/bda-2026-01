-- Hygiene: event_count is COUNT(*) of raw events at the grain, so it must be a
-- positive integer. A null or < 1 value means the aggregation produced a phantom
-- row. Returns offending rows; passes empty.
select
  user_id,
  repo_id,
  action,
  activity_date,
  event_count
from {{ ref('fact_user_repo_activity') }}
where event_count is null or event_count < 1
