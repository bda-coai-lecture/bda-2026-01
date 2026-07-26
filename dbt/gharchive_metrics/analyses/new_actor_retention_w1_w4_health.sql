-- Question: metrics_retention_weekly 커버리지/grain/좌측절단 건강성 검증
-- Grain: 소스별 1행 (coverage), cohort_week (좌측 절단)
-- Time basis / timezone: week_start, cohort_week (UTC, 월요일 시작)
-- Validation range: mart 전 구간 (mart table이라 스캔 소량)
-- Analysis range: n/a (검증 전용)
-- Segments: 없음
-- Exclusions: 없음 (automation_actor 포함 — 규모는 별도 확인)
-- Metric definition: coverage min/max, rows vs distinct grain, weeks_since=0 코호트 크기
-- Expected output grain: 검증 항목별 1행
select
  'coverage_retention_weekly' as check_name,
  cast(min(cohort_week) as string) as v1,
  cast(max(cohort_week) as string) as v2,
  cast(max(week_start) as string) as v3,
  cast(count(*) as string) as v4,
  cast(count(distinct format('%t|%t', cohort_week, week_start)) as string) as v5
from `bda-coai.mart.metrics_retention_weekly`

-- fact 커버리지는 파티션 메타데이터로 확인한다 (컬럼 min/max는 4.4 GiB 스캔이라 낭비)
union all
select
  'coverage_fact',
  min(partition_id),
  max(partition_id),
  null, null, null
from `bda-coai.mart.INFORMATION_SCHEMA.PARTITIONS`
where table_name = 'fact_user_repo_activity'
  and partition_id != '__NULL__'

union all
select
  'coverage_metrics_daily',
  cast(min(activity_date) as string),
  cast(max(activity_date) as string),
  null, null, null
from `bda-coai.mart.metrics_daily`

union all
select
  'cohort_size',
  cast(cohort_week as string),
  cast(cohort_users as string),
  null, null, null
from `bda-coai.mart.metrics_retention_weekly`
where weeks_since = 0
order by check_name, v1
