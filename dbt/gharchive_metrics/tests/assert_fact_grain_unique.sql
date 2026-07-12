-- Grain uniqueness: fact_user_repo_activity is declared at one row per
-- (user_id, repo_id, action, activity_date). An incremental insert_overwrite that
-- double-writes a partition would silently duplicate rows and inflate every
-- downstream metric. Returns any grain key with more than one row; passes empty.
select
  user_id,
  repo_id,
  action,
  activity_date,
  count(*) as rows_at_grain
from {{ ref('fact_user_repo_activity') }}
group by user_id, repo_id, action, activity_date
having count(*) > 1
