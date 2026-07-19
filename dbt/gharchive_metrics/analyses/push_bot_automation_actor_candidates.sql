-- Question: Which Push actors are explicit bots or show clearly machine-scale per-minute Push behavior?
-- Grain: final output is one summary row; actor_flags CTE is one row per actor.id.
-- Time basis / timezone: raw GitHub Archive created_at, UTC.
-- Validation range: 2026-07-18.
-- Analysis range: 2025-05-01 through 2026-07-18 by default.
-- Segments: login ending [bot]; 100+ PushEvents or 100+ distinct repos in one minute.
-- Exclusions: non-Push events; null actor IDs.
-- Metric definition: explicit bot is high precision; behavior automation is a sensitivity-analysis flag.
-- Expected output grain: one summary row.

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
    sum(push_events_in_minute) as push_events
  from actor_minute
  group by user_id
),

final as (
  select
    count(*) as push_actors,
    countif(explicit_bot) as explicit_bot_actors,
    countif(machine_rate_suspect) as machine_rate_suspect_actors,
    countif(explicit_bot and machine_rate_suspect) as overlapping_flag_actors,
    countif(machine_rate_suspect and not explicit_bot) as behavior_only_actors,
    sum(push_events) as push_events,
    sum(if(explicit_bot, push_events, 0)) as explicit_bot_push_events,
    sum(if(machine_rate_suspect, push_events, 0)) as machine_rate_suspect_push_events,
    safe_divide(countif(explicit_bot), count(*)) as explicit_bot_actor_share,
    safe_divide(sum(if(explicit_bot, push_events, 0)), sum(push_events))
      as explicit_bot_push_event_share,
    safe_divide(
      sum(if(explicit_bot or machine_rate_suspect, push_events, 0)),
      sum(push_events)
    ) as combined_flag_push_event_share
  from actor_flags
)

select *
from final
