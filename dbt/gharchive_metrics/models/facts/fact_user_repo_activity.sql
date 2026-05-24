{{
  config(
    materialized='incremental',
    partition_by={
      "field": "activity_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["user_id", "repo_id", "action"],
    incremental_strategy='insert_overwrite',
    on_schema_change='sync_all_columns'
  )
}}

{%- set raw_start_date = var('raw_start_date') -%}
{%- set raw_end_date = var('raw_end_date') -%}

select
  cast(actor.id as int64) as user_id,
  cast(repo.id as int64) as repo_id,
  cast(type as string) as action,
  count(*) as event_count,
  parse_date('%Y%m%d', concat('20', _table_suffix)) as activity_date
from `githubarchive.day.20*`
where concat('20', _table_suffix) between replace('{{ raw_start_date }}', '-', '')
                                      and replace('{{ raw_end_date }}', '-', '')
  and actor.id is not null
  and repo.id is not null
  and type is not null
group by user_id, repo_id, action, activity_date
