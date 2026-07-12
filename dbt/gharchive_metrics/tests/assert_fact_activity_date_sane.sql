-- Date sanity: activity_date is derived from the sharded table suffix via
-- parse_date('%Y%m%d', concat('20', _table_suffix)). A suffix/parse bug would
-- emit absurd partitions (e.g. year 2002, or future dates). Guard against dates
-- before the project floor or in the future. Returns offending dates; passes empty.
select distinct activity_date
from {{ ref('fact_user_repo_activity') }}
where activity_date < date('2025-01-01')
   or activity_date > current_date()
