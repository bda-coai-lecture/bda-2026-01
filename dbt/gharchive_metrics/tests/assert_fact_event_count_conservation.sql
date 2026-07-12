-- Conservation: within the raw build window, fact must preserve every source
-- event. For each date, SUM(event_count) in fact_user_repo_activity must equal
-- the raw GitHub Archive event count (raw_githubarchive_events_90d, same filter).
-- Any mismatch = our raw->fact transform dropped or duplicated events. Returns
-- offending dates; passes when empty.
{%- set start = var('raw_start_date') -%}
{%- set end = var('raw_end_date') -%}

with fact as (
  select activity_date, sum(event_count) as n
  from {{ ref('fact_user_repo_activity') }}
  where activity_date between date('{{ start }}') and date('{{ end }}')
  group by activity_date
),
raw as (
  select activity_date, count(*) as n
  from {{ ref('raw_githubarchive_events_90d') }}
  group by activity_date
)
select
  coalesce(f.activity_date, r.activity_date) as activity_date,
  coalesce(f.n, 0) as fact_events,
  coalesce(r.n, 0) as raw_events
from fact f
full outer join raw r using (activity_date)
where coalesce(f.n, 0) != coalesce(r.n, 0)
