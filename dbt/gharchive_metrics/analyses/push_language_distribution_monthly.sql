-- Question: Has the language mix of GitHub Push activity changed since 2025?
-- Grain: calendar month (UTC) x repository primary language
-- Time basis / timezone: GitHub Archive daily shard / UTC
-- Validation range: 2026-07-17 through 2026-07-17
-- Analysis range: 2025-01-01 through 2026-06-30
-- Segments: primary language from bigquery-public-data.github_repos.languages
-- Exclusions: non-PushEvent events; the incomplete current month; language-unmatched repos
-- Metric definition: PushEvent count, active repo count, and
--   each metric's share among repositories matched to the static language snapshot
-- Expected output grain: month x language
--
-- Important limitation: github_repos.languages was last modified in 2022-11.
-- This query therefore measures activity-mix changes within a legacy repository
-- cohort that can be matched to that snapshot, not the contemporaneous language
-- distribution of all GitHub repositories.

with repo_language_bytes as (
  select
    lower(repo_name) as repo_name,
    lang.name as language_name,
    lang.bytes as language_bytes
  from `bigquery-public-data.github_repos.languages`,
    unnest(language) as lang
  where lang.name is not null
),

primary_language as (
  select repo_name, language_name
  from repo_language_bytes
  qualify row_number() over (
    partition by repo_name
    order by language_bytes desc, language_name
  ) = 1
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

monthly_totals as (
  select
    activity_month,
    sum(push_events) as all_push_events,
    count(*) as all_active_repos
  from monthly_repo_push
  group by activity_month
),

matched as (
  select
    p.activity_month,
    l.language_name,
    p.repo_name,
    p.push_events
  from monthly_repo_push as p
  join primary_language as l using (repo_name)
),

monthly_language as (
  select
    activity_month,
    language_name,
    sum(push_events) as push_events,
    count(*) as active_repos
  from matched
  group by activity_month, language_name
),

monthly_matched_totals as (
  select
    activity_month,
    sum(push_events) as matched_push_events,
    count(distinct repo_name) as matched_active_repos
  from matched
  group by activity_month
)

select
  l.activity_month,
  language_name,
  push_events,
  active_repos,
  safe_divide(push_events, matched_push_events) as push_event_share,
  safe_divide(active_repos, matched_active_repos) as active_repo_share,
  safe_divide(matched_push_events, all_push_events) as push_event_coverage,
  safe_divide(matched_active_repos, all_active_repos) as active_repo_coverage
from monthly_language as l
join monthly_matched_totals as m using (activity_month)
join monthly_totals as t using (activity_month)
qualify row_number() over (
  partition by l.activity_month
  order by push_events desc, language_name
) <= 25
order by l.activity_month, push_events desc;
