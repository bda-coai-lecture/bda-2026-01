-- Question: Do Push activity levels differ by day of week, and does the answer flip depending on whether we count actors or events?
-- Grain: input activity_date x user_id x repo_id x action; output day-of-week (7 rows)
-- Time basis / timezone: activity_date, UTC (GitHub Archive daily shard date)
-- Validation range: 2026-07-12 single day (grain uniqueness, null keys, event_count >= 1)
-- Analysis range: 2026-06-15 through 2026-07-12 (4 complete Mon-Sun weeks, 4 of each weekday)
-- Segments: automation actors (dim_push_automation_actor) vs all actors
-- Exclusions: non-Push events; automation actors reported separately, not dropped
-- Metric definition: actors = count(distinct user_id) per day, averaged over the 4 occurrences of each weekday;
--                    events = sum(event_count) per day, averaged the same way. event_count is PushEvent count, NOT commit count.
-- Expected output grain: one row per day of week
--
-- Why 4 whole weeks: any window that is not a multiple of 7 gives each weekday a different
-- number of occurrences and biases the averages. The window ends 2026-07-12, well clear of
-- the unclosed latest UTC shard.
--
-- Cost: dry run 1,114,316,405 bytes (1.06 GB) on 2026-07-26.

{% set analysis_start_date = var('weekday_start_date', '2026-06-15') %}
{% set analysis_end_date = var('weekday_end_date', '2026-07-12') %}

with params as (
  select
    date('{{ analysis_start_date }}') as start_date,
    date('{{ analysis_end_date }}') as end_date
),

filtered_push as (
  select
    activity.activity_date,
    activity.user_id,
    activity.event_count,
    automation.user_id is not null as is_automation
  from {{ ref('fact_user_repo_activity') }} as activity
  cross join params
  left join {{ ref('dim_push_automation_actor') }} as automation
    using (user_id)
  where activity.activity_date between params.start_date and params.end_date
    and activity.action = 'PushEvent'
),

aggregated_daily as (
  select
    activity_date,
    count(distinct user_id) as actors_all,
    count(distinct if(not is_automation, user_id, null)) as actors_human,
    sum(event_count) as events_all,
    sum(if(not is_automation, event_count, 0)) as events_human
  from filtered_push
  group by activity_date
),

final as (
  select
    format_date('%a', activity_date) as day_of_week,
    cast(format_date('%u', activity_date) as int64) as day_of_week_index,
    count(*) as observed_days,
    round(avg(actors_all)) as avg_actors_all,
    round(avg(actors_human)) as avg_actors_human,
    round(avg(events_all)) as avg_events_all,
    round(avg(events_human)) as avg_events_human,
    round(safe_divide(avg(events_human), avg(actors_human)), 2) as human_events_per_actor
  from aggregated_daily
  group by day_of_week, day_of_week_index
)

select *
from final
order by day_of_week_index
