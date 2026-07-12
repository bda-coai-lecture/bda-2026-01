-- Metabase 대시보드 카드 정의 (native queries)
-- 데이터소스: BigQuery `bda-coai.github_ai_analysis`
-- 정본 뷰: ai_pr_metrics_monthly (합산 기준, 월 갱신 시 자동 반영)
-- data_quality='full'만 기본 표시(2026-05·06 부분수집 제외)

-- ─────────────────────────────────────────────
-- 카드 1: AI PR 비중 장기추세 (라인차트) — ym x축, ai_pct/ai_pct_bot_only y축
SELECT ym,
       ai_pct        AS `AI%_합산(실제)`,
       ai_pct_bot_only AS `AI%_봇계정만(하한)`
FROM `bda-coai.github_ai_analysis.ai_pr_metrics_monthly`
WHERE data_quality = 'full'
ORDER BY ym;

-- ─────────────────────────────────────────────
-- 카드 2: 툴별 점유 추세 (스택 영역/막대) — ym x축, 툴 시리즈
SELECT ym, copilot AS Copilot, codex AS Codex, claude AS Claude,
       cursor AS Cursor, devin AS Devin, jules AS Jules
FROM `bda-coai.github_ai_analysis.ai_pr_metrics_monthly`
WHERE data_quality = 'full'
ORDER BY ym;

-- ─────────────────────────────────────────────
-- 카드 3: 도입 폭 — 활성 레포 수(막대) + 도입레포 내 AI 비중(라인, 콤보차트)
SELECT ym,
       active_repos                 AS `AI 도입 레포 수`,
       active_repo_ai_pct_botonly   AS `도입레포 내 AI%(하한)`
FROM `bda-coai.github_ai_analysis.ai_pr_metrics_monthly`
WHERE data_quality = 'full'
ORDER BY ym;

-- ─────────────────────────────────────────────
-- 카드 4: KPI 넘버 — 최신 확정월(full) 헤드라인
SELECT
  ym                                   AS `기준월`,
  ROUND(ai_pct,1)                      AS `전역 AI PR %`,
  ai_pr                                AS `AI PR 수(표본)`,
  active_repos                         AS `도입 레포 수`,
  ROUND(active_repo_ai_pct_botonly,0)  AS `도입레포 내 AI%`
FROM `bda-coai.github_ai_analysis.ai_pr_metrics_monthly`
WHERE data_quality = 'full'
ORDER BY ym DESC
LIMIT 1;

-- ─────────────────────────────────────────────
-- 카드 5: 3파전 스냅샷 — 최신 확정월 툴 점유 (파이/막대)
SELECT tool, prs FROM (
  SELECT 'Copilot' AS tool, copilot AS prs, ym FROM `bda-coai.github_ai_analysis.ai_pr_metrics_monthly`
  UNION ALL SELECT 'Codex', codex, ym FROM `bda-coai.github_ai_analysis.ai_pr_metrics_monthly`
  UNION ALL SELECT 'Claude', claude, ym FROM `bda-coai.github_ai_analysis.ai_pr_metrics_monthly`
  UNION ALL SELECT 'Cursor', cursor, ym FROM `bda-coai.github_ai_analysis.ai_pr_metrics_monthly`
  UNION ALL SELECT 'Devin', devin, ym FROM `bda-coai.github_ai_analysis.ai_pr_metrics_monthly`
  UNION ALL SELECT 'Jules', jules, ym FROM `bda-coai.github_ai_analysis.ai_pr_metrics_monthly`
) WHERE ym = (SELECT MAX(ym) FROM `bda-coai.github_ai_analysis.ai_pr_metrics_monthly` WHERE data_quality='full')
ORDER BY prs DESC;
