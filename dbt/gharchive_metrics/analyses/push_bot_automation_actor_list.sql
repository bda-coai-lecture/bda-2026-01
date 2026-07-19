-- Question: Which Push actors should be excluded as explicit bots or high-confidence machine-rate automation?
-- Grain: actor.id.
-- Time basis / timezone: raw GitHub Archive created_at, UTC.
-- Validation range: 2026-07-18.
-- Analysis range: 2025-05-01 through 2026-07-18 by default.
-- Segments: login ending [bot]; 100+ PushEvents or 100+ distinct repos in one minute.
-- Exclusions: non-Push events; null actor IDs.
-- Metric definition: remove explicit bots in the primary view; add machine-rate actors in sensitivity analysis.
-- Expected output grain: one row per flagged user_id.

{% set analysis_start_date = var('analysis_start_date', '2025-05-01') %}
{% set analysis_end_date = var('analysis_end_date', '2026-07-18') %}

with push_events as (
  select
    cast(actor.id as int64) as user_id,
    cast(actor.login as string) as login,
    cast(repo.id as int64) as repo_id,
    timestamp_trunc(created_at, minute) as minute_start
  from `githubarchive.day.20*`
  where concat('20', _table_suffix)
        between replace('{{ analysis_start_date }}', '-', '')
            and replace('{{ analysis_end_date }}', '-', '')
    and type = 'PushEvent'
    and actor.id is not null
),

actor_minute as (
  select
    user_id,
    any_value(login having max minute_start) as latest_login_in_minute,
    minute_start,
    count(*) as push_events_in_minute,
    count(distinct repo_id) as repos_in_minute
  from push_events
  group by user_id, minute_start
),

actor_flags as (
  select
    user_id,
    any_value(latest_login_in_minute having max minute_start) as latest_login,
    logical_or(
      regexp_contains(lower(latest_login_in_minute), r'[[]bot[]]$')
    ) as explicit_bot,
    max(push_events_in_minute) >= 100
      or max(repos_in_minute) >= 100 as machine_rate_suspect,
    max(push_events_in_minute) as max_push_events_in_minute,
    max(repos_in_minute) as max_repos_in_minute,
    sum(push_events_in_minute) as observed_push_events
  from actor_minute
  group by user_id
)

select
  user_id,
  latest_login,
  explicit_bot,
  machine_rate_suspect,
  case
    when explicit_bot and machine_rate_suspect then 'explicit_bot_and_machine_rate'
    when explicit_bot then 'explicit_bot'
    else 'machine_rate'
  end as flag_reason,
  max_push_events_in_minute,
  max_repos_in_minute,
  observed_push_events,
  date('{{ analysis_start_date }}') as observed_from,
  date('{{ analysis_end_date }}') as observed_through
from actor_flags
where explicit_bot or machine_rate_suspect
