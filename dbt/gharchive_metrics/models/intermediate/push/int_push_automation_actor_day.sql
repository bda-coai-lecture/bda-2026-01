{{
  config(
    materialized='incremental',
    partition_by={
      "field": "activity_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["user_id"],
    incremental_strategy='insert_overwrite',
    on_schema_change='sync_all_columns'
  )
}}

{% set analysis_start_date = var('analysis_start_date', '2025-05-01') %}
{% set analysis_end_date = var('analysis_end_date', '2026-07-18') %}

with push_events as (
  select
    parse_date('%Y%m%d', concat('20', _table_suffix)) as activity_date,
    cast(actor.id as int64) as user_id,
    cast(actor.login as string) as login,
    cast(repo.id as int64) as repo_id,
    created_at,
    timestamp_trunc(created_at, minute) as minute_start
  from `githubarchive.day.20*`
  where concat('20', _table_suffix)
        between replace('{{ analysis_start_date }}', '-', '')
            and replace('{{ analysis_end_date }}', '-', '')
    and type = 'PushEvent'
    and actor.id is not null
),

actor_minutes as (
  select
    activity_date,
    user_id,
    minute_start,
    array_agg(
      login ignore nulls
      order by created_at desc, login desc
      limit 1
    )[safe_offset(0)] as latest_login_in_minute,
    coalesce(
      logical_or(regexp_contains(lower(login), r'[[]bot[]]$')),
      false
    ) as explicit_bot_in_minute,
    count(*) as push_events_in_minute,
    count(distinct repo_id) as repos_in_minute
  from push_events
  group by activity_date, user_id, minute_start
),

actor_days as (
  select
    activity_date,
    user_id,
    array_agg(
      latest_login_in_minute ignore nulls
      order by minute_start desc, latest_login_in_minute desc
      limit 1
    )[safe_offset(0)] as latest_login,
    logical_or(explicit_bot_in_minute) as explicit_bot,
    max(push_events_in_minute) >= 100
      or max(repos_in_minute) >= 100 as machine_rate_suspect,
    max(push_events_in_minute) as max_push_events_in_minute,
    max(repos_in_minute) as max_repos_in_minute,
    sum(push_events_in_minute) as observed_push_events
  from actor_minutes
  group by activity_date, user_id
)

select
  to_hex(md5(concat(
    cast(activity_date as string),
    '|',
    cast(user_id as string)
  ))) as actor_day_id,
  activity_date,
  user_id,
  latest_login,
  explicit_bot,
  machine_rate_suspect,
  max_push_events_in_minute,
  max_repos_in_minute,
  observed_push_events
from actor_days
