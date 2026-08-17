-- Question: 2026-08-01 event type 심층분석을 Metabase 임시 카드로 올린다
-- Mode: 활동량 (event type 세그먼트 분해) — 카드화용 쿼리 모음
-- Grain: 카드별로 아래 각 블록 헤더에 표기
-- Time basis / timezone: activity_date, UTC. as-of = mart 최신 완결일 2026-08-01
-- Validation range: event_type_health_20260801.sql 에서 합계 보존 / grain 중복 / 커버리지 PASS
--                   daily_active_actor_raw_reconcile_20260801.sql 에서 raw=fact 정확히 일치
-- Analysis range: 각 블록 헤더 참조
-- Segments: action 16종 / push volume 구간
-- Exclusions: 미완결 최신일(2026-08-02) 제외. automation_actor 미제외(포함)
-- Metric definition: 각 블록 헤더 참조
-- Expected output grain: 각 블록 헤더 참조
--
-- 이 파일은 "Metabase native question 4개"의 본문이다. 한 번에 실행하는 스크립트가 아니다.
-- 카드별로 블록 하나를 그대로 복사해 native SQL question 으로 저장한다.
-- 대시보드에는 붙이지 않는다(임시 카드). 컬렉션: "BDA 데이터 플랫폼".
--
-- 시각화 원칙 (dataviz)
--   * 이중축 금지. 카드 2는 두 시리즈가 같은 단위(활동 기록 수)라 축 하나로 둔다.
--   * 카드 1은 단일 시리즈라 범례 없이 제목이 지표를 설명한다.
--   * 카드 3은 크기 비교라 가로 막대(row)로 둔다.
--   * 값 라벨을 모든 점에 찍지 않는다 (graph.show_values = false).


-- ============================================================================
-- 카드 1 | 임시 - 푸시 비중 추이 (일별, %)
--   display: line   |  grain: activity_date  |  range: 2026-06-01 ~ 2026-08-01
--   metric: push_share_pct = sum(PushEvent total_events) / sum(total_events) * 100
--   y축 80~100 고정. 6월 초 ~84% → 2026-08-01 95.6%
-- ============================================================================
SELECT
  activity_date,
  ROUND(
    SAFE_DIVIDE(
      SUM(IF(action = 'PushEvent', total_events, 0)),
      SUM(total_events)
    ) * 100, 1) AS push_share_pct
FROM `bda-coai.mart.metrics_event_type_daily`
WHERE activity_date BETWEEN DATE '2026-06-01' AND DATE '2026-08-01'
GROUP BY activity_date
ORDER BY activity_date;


-- ============================================================================
-- 카드 2 | 임시 - 푸시 vs 푸시 외 활동 기록 (일별)
--   display: line (시리즈 2개, 단일 y축)  |  grain: activity_date
--   range: 2026-06-01 ~ 2026-08-01
--   metric: push_events / non_push_events = sum(total_events) by action 분기
--   총량은 375만~407만으로 거의 평평한데 푸시 외가 60만 → 18만으로 축소
-- ============================================================================
SELECT
  activity_date,
  SUM(IF(action = 'PushEvent', total_events, 0)) AS push_events,
  SUM(IF(action <> 'PushEvent', total_events, 0)) AS non_push_events
FROM `bda-coai.mart.metrics_event_type_daily`
WHERE activity_date BETWEEN DATE '2026-06-01' AND DATE '2026-08-01'
GROUP BY activity_date
ORDER BY activity_date;


-- ============================================================================
-- 카드 3 | 임시 - 활동 종류별 증감률 (최근 7일 vs 8주 전 7일, %)
--   display: row (가로 막대)  |  grain: action
--   range: 최근 7일 2026-07-26~2026-08-01 vs 8주 전 2026-06-07~2026-06-13
--   metric: change_pct = (ev_recent - ev_base) / ev_base * 100
--   PushEvent만 +19.1%, 나머지 15종은 -57.6% ~ -92.0%
-- ============================================================================
WITH aggregated AS (
  SELECT
    action,
    SUM(IF(activity_date BETWEEN DATE '2026-07-26' AND DATE '2026-08-01', total_events, 0)) AS ev_recent,
    SUM(IF(activity_date BETWEEN DATE '2026-06-07' AND DATE '2026-06-13', total_events, 0)) AS ev_base
  FROM `bda-coai.mart.metrics_event_type_daily`
  WHERE activity_date BETWEEN DATE '2026-06-07' AND DATE '2026-08-01'
  GROUP BY action
)
SELECT
  action,
  ev_recent,
  ev_base,
  ROUND(SAFE_DIVIDE(ev_recent - ev_base, ev_base) * 100, 1) AS change_pct
FROM aggregated
WHERE ev_base > 0
ORDER BY change_pct DESC;


-- ============================================================================
-- 카드 4 | 임시 - 푸시 계정 집중도 (토요일 비교: 06-06 vs 08-01)
--   display: table  |  grain: activity_date x volume_bucket
--   range: 2026-06-06(Sat) vs 2026-08-01(Sat) — 요일 효과 통제
--   metric: bucket별 actor 수, push event 합, 날짜 내 event 점유율
--   하루 100~999회 푸시 계정 1,577 → 13,229 (+739%), 푸시 기록의 15% → 70%
--   ※ dim_push_automation_actor는 사실상 [bot] 접미사 목록이라 검정력이 없어 velocity로 직접 본다
-- ============================================================================
WITH filtered_push AS (
  SELECT activity_date, user_id, SUM(event_count) AS push_events
  FROM `bda-coai.mart.fact_user_repo_activity`
  WHERE activity_date IN (DATE '2026-06-06', DATE '2026-08-01')
    AND action = 'PushEvent'
  GROUP BY activity_date, user_id
),

bucketed AS (
  SELECT
    activity_date,
    CASE
      WHEN push_events >= 1000 THEN '4. 1000회+'
      WHEN push_events >= 100 THEN '3. 100-999회'
      WHEN push_events >= 10 THEN '2. 10-99회'
      ELSE '1. 1-9회'
    END AS volume_bucket,
    user_id,
    push_events
  FROM filtered_push
),

aggregated AS (
  SELECT
    activity_date,
    volume_bucket,
    COUNT(DISTINCT user_id) AS actors,
    SUM(push_events) AS push_events
  FROM bucketed
  GROUP BY activity_date, volume_bucket
)

SELECT
  activity_date,
  volume_bucket,
  actors,
  push_events,
  ROUND(SAFE_DIVIDE(push_events, SUM(push_events) OVER (PARTITION BY activity_date)) * 100, 1) AS event_share_pct
FROM aggregated
ORDER BY activity_date, volume_bucket;
