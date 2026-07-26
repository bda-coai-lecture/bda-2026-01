-- Question: 처음 활동한 actor가 1주 후 / 4주(≈1개월) 후에도 활동하는가 (코호트 가중 통합값)
-- Grain: 1행 (W1 pooled), 1행 (W4 pooled), 1행 (최근 8코호트 W1)
-- Time basis / timezone: week_start, cohort_week = min(week_start) per user_id, UTC 월요일 시작
-- Validation range: new_actor_retention_w1_w4_health.sql / _health2.sql
-- Analysis range: cohort_week 2025-11-03 ~ 2026-07-06 (W1), 2025-11-03 ~ 2026-06-15 (W4)
-- Segments: 없음 (전체 actor)
-- Exclusions: 2025-09-01~2025-10-27 코호트(mart 좌측 절단 번인) 제외, 관측주 미완결 코호트 제외,
--             automation_actor 제외하지 않음(mart 정의상 포함)
-- Metric definition: pooled_rate = sum(weeks_since=n active_users) / sum(cohort_users)
-- Expected output grain: 검증 항목별 1행
with params as (
  select
    date '2025-11-03' as cohort_start,
    date '2026-07-06' as w1_cohort_end,
    date '2026-06-15' as w4_cohort_end
),

filtered_retention as (
  select r.cohort_week, r.weeks_since, r.cohort_users, r.active_users
  from `bda-coai.mart.metrics_retention_weekly` as r
  cross join params as p
  where r.cohort_week between p.cohort_start and p.w1_cohort_end
    and r.weeks_since in (0, 1, 4)
),

aggregated_pooled as (
  select
    'w1_pooled_20251103_20260706' as metric,
    sum(if(f.weeks_since = 0, f.cohort_users, 0)) as denominator,
    sum(if(f.weeks_since = 1, f.active_users, 0)) as numerator
  from filtered_retention as f

  union all

  select
    'w4_pooled_20251103_20260615',
    sum(if(f.weeks_since = 0, f.cohort_users, 0)),
    sum(if(f.weeks_since = 4, f.active_users, 0))
  from filtered_retention as f
  cross join params as p
  where f.cohort_week <= p.w4_cohort_end

  union all

  select
    'w1_pooled_last8_20260518_20260706',
    sum(if(f.weeks_since = 0, f.cohort_users, 0)),
    sum(if(f.weeks_since = 1, f.active_users, 0))
  from filtered_retention as f
  where f.cohort_week >= date '2026-05-18'

  union all

  select
    'w4_pooled_last8_20260427_20260615',
    sum(if(f.weeks_since = 0, f.cohort_users, 0)),
    sum(if(f.weeks_since = 4, f.active_users, 0))
  from filtered_retention as f
  where f.cohort_week between date '2026-04-27' and date '2026-06-15'
),

final as (
  select
    metric,
    denominator,
    numerator,
    round(safe_divide(numerator, denominator) * 100, 2) as pooled_pct
  from aggregated_pooled
)

select * from final
order by metric
