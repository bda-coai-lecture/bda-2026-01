-- Question: metrics_event_type_daily로 event type별 심층분석을 해도 되나 (선행 건강성 검증)
-- Mode: 데이터 품질 진단 (활동량 분석의 선행 단계)
-- Grain: activity_date x action
-- Time basis / timezone: activity_date, UTC
-- Validation range: 최근 60일
-- Analysis range: 동일
-- Segments: 없음
-- Exclusions: 없음 (검증 단계라 필터 최소화)
-- Metric definition: 합계 보존 = sum(metrics_event_type_daily.total_events) vs metrics_daily.total_events
--                    grain 중복 = count(*) vs count(distinct activity_date||action)
-- Expected output grain: section별 행
-- Note: mart table만 조회. raw/fact 스캔 없음.

with coverage as (
  select
    'coverage' as section,
    'metrics_event_type_daily' as k,
    cast(min(activity_date) as string) as v1,
    cast(max(activity_date) as string) as v2,
    cast(count(*) as string) as v3,
    cast(count(distinct action) as string) as v4
  from `bda-coai.mart.metrics_event_type_daily`

  union all

  select
    'coverage',
    'metrics_daily',
    cast(min(activity_date) as string),
    cast(max(activity_date) as string),
    cast(count(*) as string),
    null
  from `bda-coai.mart.metrics_daily`
),

grain_dupe as (
  select
    'grain_dupe' as section,
    'rows vs distinct(date,action)' as k,
    cast(count(*) as string) as v1,
    cast(count(distinct format('%t|%t', activity_date, action)) as string) as v2,
    cast(countif(action is null) as string) as v3,
    cast(countif(total_events < 1) as string) as v4
  from `bda-coai.mart.metrics_event_type_daily`
  where activity_date >= date_sub(current_date(), interval 60 day)
),

sum_preserve as (
  select
    'sum_preserve' as section,
    cast(m.activity_date as string) as k,
    cast(e.etd_events as string) as v1,
    cast(m.total_events as string) as v2,
    cast(e.etd_events - m.total_events as string) as v3,
    cast(e.action_rows as string) as v4
  from `bda-coai.mart.metrics_daily` as m
  left join (
    select activity_date, sum(total_events) as etd_events, count(*) as action_rows
    from `bda-coai.mart.metrics_event_type_daily`
    group by activity_date
  ) as e using (activity_date)
  where m.activity_date >= date_sub(current_date(), interval 10 day)
)

select * from coverage
union all select * from grain_dupe
union all select * from sum_preserve
order by section, k desc
