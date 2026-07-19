-- Question: What share of Push activity is covered by the static language snapshot?
-- Grain: calendar month (UTC)
-- Time basis / timezone: GitHub Archive daily shard / UTC
-- Validation range: 2026-07-17 through 2026-07-17
-- Analysis range: 2025-01-01 through 2026-06-30
-- Segments: matched versus unmatched repository name
-- Exclusions: non-PushEvent events; the incomplete current month
-- Metric definition: matched PushEvent and active-repo shares
-- Expected output grain: month

with language_repos as (
  select distinct lower(repo_name) as repo_name
  from `bigquery-public-data.github_repos.languages`
),

monthly_repo_push as (
  select
    date_trunc(parse_date('%Y%m%d', concat('20', _table_suffix)), month) as activity_month,
    lower(repo.name) as repo_name,
    count(*) as push_events
  from `githubarchive.day.20*`
  where concat('20', _table_suffix) between '20250101' and '20260630'
    and type = 'PushEvent'
    and repo.name is not null
  group by activity_month, repo_name
),

coverage as (
  select
    p.*,
    l.repo_name is not null as is_matched
  from monthly_repo_push as p
  left join language_repos as l using (repo_name)
)

select
  activity_month,
  sum(push_events) as push_events,
  sum(if(is_matched, push_events, 0)) as matched_push_events,
  safe_divide(sum(if(is_matched, push_events, 0)), sum(push_events)) as push_event_coverage,
  count(*) as active_repos,
  countif(is_matched) as matched_active_repos,
  safe_divide(countif(is_matched), count(*)) as active_repo_coverage
from coverage
group by activity_month
order by activity_month;
