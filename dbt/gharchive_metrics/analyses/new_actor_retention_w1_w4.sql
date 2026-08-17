-- Question: 처음 활동한 actor가 1주 후 / 4주(≈1개월) 후에도 활동하는가
-- Grain: cohort_week (최초 관측 주)
-- Time basis / timezone: week_start, cohort_week = min(week_start) per user_id, UTC 월요일 시작
-- Validation range: mart 전 구간 커버리지 + 최근 10일 마감 (별도 파일 *_health.sql, *_health2.sql)
-- Analysis range: cohort_week 2025-11-03 ~ 2026-07-06 (W1), 2025-11-03 ~ 2026-06-15 (W4)
-- Segments: 없음 (전체 actor)
-- Exclusions: 2025-09-01~2025-10-27 코호트 = mart 좌측 절단 번인 구간 제외,
--             W1/W4 관측주가 미완결인 코호트 제외, automation_actor 제외하지 않음(mart 정의상 포함)
-- Metric definition: retention_rate = (코호트 actor 중 weeks_since=n 주에 1회 이상 활동한 distinct actor) / cohort_users
-- Expected output grain: cohort_week 1행
with params as (
  select
    date '2025-11-03' as cohort_start,
    date '2026-07-06' as w1_cohort_end,   -- W1 관측주(2026-07-13)가 완결된 마지막 코호트
    date '2026-06-15' as w4_cohort_end    -- W4 관측주(2026-07-13)가 완결된 마지막 코호트
),

filtered_retention as (
  select r.*
  from `bda-coai.mart.metrics_retention_weekly` as r
  cross join params as p
  where r.cohort_week between p.cohort_start and p.w1_cohort_end
    and r.weeks_since in (0, 1, 4)
),

aggregated_cohort as (
  select
    cohort_week,
    max(cohort_users) as cohort_users,
    max(if(weeks_since = 0, retention_rate, null)) as w0_rate,
    max(if(weeks_since = 1, retention_rate, null)) as w1_rate,
    max(if(weeks_since = 1, active_users, null)) as w1_users,
    max(if(weeks_since = 4, retention_rate, null)) as w4_rate,
    max(if(weeks_since = 4, active_users, null)) as w4_users
  from filtered_retention
  group by cohort_week
),

final as (
  select
    a.cohort_week,
    a.cohort_users,
    round(a.w0_rate * 100, 2) as w0_pct,
    a.w1_users,
    round(a.w1_rate * 100, 2) as w1_pct,
    if(a.cohort_week <= p.w4_cohort_end, a.w4_users, null) as w4_users,
    if(a.cohort_week <= p.w4_cohort_end, round(a.w4_rate * 100, 2), null) as w4_pct
  from aggregated_cohort as a
  cross join params as p
)

select * from final
order by cohort_week
