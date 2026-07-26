-- Question: 2026-07-06 이후 non-Push 이벤트 급감이 mart(적재/모델) 문제인가 상류(GH Archive) 문제인가
-- Grain: activity_date x source(raw|fact) x action
-- Time basis / timezone: raw = _TABLE_SUFFIX (UTC 일자 shard), fact = activity_date (UTC)
-- Validation range: 2026-07-02 (급감 전 1일), 2026-07-21 (급감 후 1일)
-- Analysis range: 위 2일 (raw 스캔 비용 통제. 추세 자체는 mart로 확인)
-- Segments: action (PushEvent vs non-Push 전 type)
-- Exclusions: actor.id / repo.id / type null 제외 (fact 적재 조건과 동일). automation_actor: 포함(제외하지 않음)
-- Metric definition: raw = count(*), fact = sum(event_count)  ※ fact row count가 아니다
-- Expected output grain: (activity_date, action) 당 raw_events, fact_events, diff

{% set d1 = var('probe_date_pre', '20260702') %}
{% set d2 = var('probe_date_post', '20260721') %}

with raw_events as (
  select
    parse_date('%Y%m%d', concat('20', _table_suffix)) as activity_date,
    type as action,
    count(*) as raw_events
  from `githubarchive.day.20*`
  where _table_suffix in ('{{ d1[2:] }}', '{{ d2[2:] }}')
    and actor.id is not null
    and repo.id is not null
    and type is not null
  group by activity_date, action
),

fact_events as (
  select
    activity_date,
    action,
    sum(event_count) as fact_events
  from `bda-coai.mart.fact_user_repo_activity`
  where activity_date in (
    parse_date('%Y%m%d', '{{ d1 }}'),
    parse_date('%Y%m%d', '{{ d2 }}')
  )
  group by activity_date, action
),

final as (
  select
    coalesce(r.activity_date, f.activity_date) as activity_date,
    coalesce(r.action, f.action) as action,
    coalesce(r.raw_events, 0) as raw_events,
    coalesce(f.fact_events, 0) as fact_events,
    coalesce(f.fact_events, 0) - coalesce(r.raw_events, 0) as fact_minus_raw
  from raw_events as r
  full outer join fact_events as f
    on r.activity_date = f.activity_date
   and r.action = f.action
)

select * from final
order by activity_date, raw_events desc
