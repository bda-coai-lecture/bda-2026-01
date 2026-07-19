-- Question: Do recent CreateEvent and DeleteEvent add 28-day Push churn signal beyond Push frequency and repo breadth?
-- Grain: Sunday cutoff x comparison type x comparison group.
-- Time basis / timezone: fact activity_date, UTC.
-- Validation range: one Sunday cutoff, 2026-04-19 by default.
-- Analysis range: weekly cutoffs from 2026-02-01 through 2026-04-19 by default.
-- Segments: no Create/Delete, Create only, Delete only, both; matched by Push-active weeks and repo breadth.
-- Exclusions: event types other than Push/Create/Delete; actors without Push in the final 7 days before cutoff.
-- Metric definition: churn = no Push in cutoff+1..28; resurrection = any Push in cutoff+29..84 among churned actors.
-- Expected output grain: cutoff_date x comparison_type x comparison_group.

{% set cohort_start_date = var('cohort_start_date', '2026-02-01') %}
{% set cohort_end_date = var('cohort_end_date', '2026-04-19') %}
{% set exclude_push_automation = var('exclude_push_automation', false) %}

with params as (
  select
    date('{{ cohort_start_date }}') as cohort_start_date,
    date('{{ cohort_end_date }}') as cohort_end_date
),

requested_cutoffs as (
  select cutoff_date
  from params,
  unnest(generate_date_array(cohort_start_date, cohort_end_date, interval 7 day)) as cutoff_date
),

filtered_activity as (
  select
    activity_date,
    user_id,
    repo_id,
    action,
    sum(event_count) as events
  from {{ ref('fact_user_repo_activity') }} as activity
  cross join params
  where activity_date between date_sub(cohort_start_date, interval 27 day)
                          and date_add(cohort_end_date, interval 84 day)
    and action in ('PushEvent', 'CreateEvent', 'DeleteEvent')
    {% if exclude_push_automation %}
    and not exists (
      select 1
      from {{ ref('dim_push_automation_actor') }} as automation
      where automation.user_id = activity.user_id
    )
    {% endif %}
  group by activity_date, user_id, repo_id, action
),

source_bounds as (
  select max(activity_date) as data_through_date
  from filtered_activity
),

cutoffs as (
  select cutoff_date
  from requested_cutoffs
  cross join source_bounds
  where date_add(cutoff_date, interval 84 day) <= data_through_date
),

observation_activity as (
  select
    cutoff.cutoff_date,
    activity.activity_date,
    activity.user_id,
    activity.repo_id,
    activity.action,
    activity.events,
    div(date_diff(cutoff.cutoff_date, activity.activity_date, day), 7) as recency_week_bin
  from cutoffs as cutoff
  join filtered_activity as activity
    on activity.activity_date between date_sub(cutoff.cutoff_date, interval 27 day)
                                  and cutoff.cutoff_date
),

actor_features as (
  select
    cutoff_date,
    user_id,
    count(distinct if(action = 'PushEvent', recency_week_bin, null)) as push_active_weeks_28d,
    count(distinct if(action = 'PushEvent', repo_id, null)) as push_repos_28d,
    countif(action = 'CreateEvent') > 0 as had_create_28d,
    countif(action = 'DeleteEvent') > 0 as had_delete_28d,
    count(distinct if(action = 'CreateEvent', repo_id, null)) as create_repos_28d,
    count(distinct if(action = 'DeleteEvent', repo_id, null)) as delete_repos_28d
  from observation_activity
  group by cutoff_date, user_id
  having max(if(action = 'PushEvent', activity_date, null))
         >= date_sub(cutoff_date, interval 6 day)
),

future_push as (
  select
    cutoff.cutoff_date,
    activity.user_id,
    logical_or(
      activity.activity_date between date_add(cutoff.cutoff_date, interval 1 day)
                                 and date_add(cutoff.cutoff_date, interval 28 day)
    ) as pushed_next_28d,
    logical_or(
      activity.activity_date between date_add(cutoff.cutoff_date, interval 29 day)
                                 and date_add(cutoff.cutoff_date, interval 84 day)
    ) as pushed_days_29_84
  from cutoffs as cutoff
  join filtered_activity as activity
    on activity.action = 'PushEvent'
   and activity.activity_date between date_add(cutoff.cutoff_date, interval 1 day)
                                  and date_add(cutoff.cutoff_date, interval 84 day)
  group by cutoff.cutoff_date, activity.user_id
),

actor_dataset as (
  select
    feature.*,
    case
      when not had_create_28d and not had_delete_28d then 'no create/delete'
      when had_create_28d and not had_delete_28d then 'create only'
      when not had_create_28d and had_delete_28d then 'delete only'
      else 'create + delete'
    end as create_delete_group,
    not coalesce(future.pushed_next_28d, false) as churned_28d,
    not coalesce(future.pushed_next_28d, false)
      and coalesce(future.pushed_days_29_84, false) as resurrected_days_29_84
  from actor_features as feature
  left join future_push as future
    using (cutoff_date, user_id)
),

long_comparisons as (
  select
    actor.cutoff_date,
    actor.user_id,
    actor.churned_28d,
    actor.resurrected_days_29_84,
    comparison.comparison_type,
    comparison.comparison_group,
    comparison.group_order
  from actor_dataset as actor
  cross join unnest([
    struct(
      'create_delete_overall' as comparison_type,
      actor.create_delete_group as comparison_group,
      case actor.create_delete_group
        when 'no create/delete' then 1
        when 'create only' then 2
        when 'delete only' then 3
        else 4
      end as group_order
    ),
    struct(
      'active_weeks_x_create_delete',
      concat(
        cast(actor.push_active_weeks_28d as string),
        ' push week(s) | ',
        actor.create_delete_group
      ),
      actor.push_active_weeks_28d * 10
        + case actor.create_delete_group
            when 'no create/delete' then 1
            when 'create only' then 2
            when 'delete only' then 3
            else 4
          end
    ),
    struct(
      'active_weeks_repo_x_create_delete',
      concat(
        cast(actor.push_active_weeks_28d as string),
        ' push week(s) | ',
        if(actor.push_repos_28d = 1, '1 repo', '2+ repos'),
        ' | ',
        actor.create_delete_group
      ),
      actor.push_active_weeks_28d * 100
        + if(actor.push_repos_28d = 1, 10, 20)
        + case actor.create_delete_group
            when 'no create/delete' then 1
            when 'create only' then 2
            when 'delete only' then 3
            else 4
          end
    )
  ]) as comparison
),

final as (
  select
    cutoff_date,
    comparison_type,
    comparison_group,
    group_order,
    count(*) as developer_landmarks,
    countif(churned_28d) as churned_developer_landmarks,
    safe_divide(countif(churned_28d), count(*)) as churn_rate_28d,
    countif(resurrected_days_29_84) as resurrected_developer_landmarks,
    safe_divide(
      countif(resurrected_days_29_84),
      countif(churned_28d)
    ) as resurrection_rate_days_29_84
  from long_comparisons
  group by cutoff_date, comparison_type, comparison_group, group_order
)

select *
from final
order by cutoff_date, comparison_type, group_order
