#!/usr/bin/env bash
# 마트에 새 달 1개월치(9·10·11일) append — 합산(봇+브랜치) 기준.
# 사용: ./refresh_mart.sh 2026-08
# 비용: payload 스캔 3일치 ~10GB = 정가 $0.06, 무료구간 내 실제 $0.
set -euo pipefail
[ $# -eq 1 ] || { echo "usage: $0 YYYY-MM"; exit 1; }
Y=${1%-*}; M=${1#*-}; YD=${Y:3:1}                 # 2026 -> '6'
SUF="'${YD}${M}09','${YD}${M}10','${YD}${M}11'"
DS="bda-coai.github_ai_analysis"; PROJ="bda-coai"
AILIST="'Copilot','Claude','claude[bot]','google-labs-jules[bot]','devin-ai-integration[bot]','cursor[bot]','Codex'"
BR="'codex','cursor','claude','devin','copilot','jules'"
YM="CONCAT('202',SUBSTR(_TABLE_SUFFIX,1,1),'-',SUBSTR(_TABLE_SUFFIX,2,2))"
DT="PARSE_DATE('%Y%m%d', CONCAT('202',_TABLE_SUFFIX))"
TOOL="CASE actor.login WHEN 'Copilot' THEN 'copilot' WHEN 'Claude' THEN 'claude' WHEN 'claude[bot]' THEN 'claude' WHEN 'google-labs-jules[bot]' THEN 'jules' WHEN 'devin-ai-integration[bot]' THEN 'devin' WHEN 'cursor[bot]' THEN 'cursor' WHEN 'Codex' THEN 'codex' END"
BREXPR="LOWER(REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(payload,'\$.pull_request.head.ref'), r'^([^/]+)/'))"
q(){ bq --project_id=$PROJ query --use_legacy_sql=false "$1"; }

# 멱등: 같은 달 재실행 대비 먼저 삭제
for T in ai_pr_events ai_pr_repo_active ai_pr_monthly_raw ai_pr_monthly_combined ai_branch_monthly; do q "DELETE FROM \`$DS.$T\` WHERE ym='$1'"; done

# 1) 봇전용 원본 3종 (기존 유지, actor.login만 = 저렴)
q "INSERT INTO \`$DS.ai_pr_events\` SELECT $DT dt,$YM ym,actor.login login,$TOOL tool,repo.name repo
   FROM \`githubarchive.day.202*\` WHERE _TABLE_SUFFIX IN ($SUF) AND type='PullRequestEvent' AND actor.login IN ($AILIST)"
q "INSERT INTO \`$DS.ai_pr_repo_active\` SELECT ym,repo,COUNT(*) total_pr,COUNTIF(is_ai) ai_pr FROM(
     SELECT $YM ym,repo.name repo,actor.login IN ($AILIST) is_ai FROM \`githubarchive.day.202*\`
     WHERE _TABLE_SUFFIX IN ($SUF) AND type='PullRequestEvent') GROUP BY ym,repo HAVING ai_pr>0"
q "INSERT INTO \`$DS.ai_pr_monthly_raw\` SELECT $YM ym,COUNT(*) total_pr,COUNTIF(actor.login IN ($AILIST)) ai_pr,
     COUNT(DISTINCT repo.name) repos_total,COUNT(DISTINCT IF(actor.login IN ($AILIST),repo.name,NULL)) repos_ai
   FROM \`githubarchive.day.202*\` WHERE _TABLE_SUFFIX IN ($SUF) AND type='PullRequestEvent' GROUP BY ym"

# 2) 합산(봇+브랜치) = 대시보드 정본 (payload 스캔)
q "INSERT INTO \`$DS.ai_pr_monthly_combined\`
   WITH e AS (SELECT $YM ym, actor.login login, $BREXPR br
     FROM \`githubarchive.day.202*\` WHERE _TABLE_SUFFIX IN ($SUF) AND type='PullRequestEvent')
   SELECT ym, COUNT(*) total_pr,
     COUNTIF(login IN ($AILIST)) bot_ai,
     COUNTIF(login NOT IN ($AILIST) AND br IN ($BR)) recovered,
     COUNTIF(login IN ($AILIST) OR br IN ($BR)) combined,
     ROUND(100*COUNTIF(login IN ($AILIST) OR br IN ($BR))/COUNT(*),2) combined_pct,
     COUNTIF(login='Copilot' OR (login NOT IN ($AILIST) AND br='copilot')) copilot,
     COUNTIF(login='Codex' OR (login NOT IN ($AILIST) AND br='codex')) codex,
     COUNTIF(login IN ('Claude','claude[bot]') OR (login NOT IN ($AILIST) AND br='claude')) claude,
     COUNTIF(login='cursor[bot]' OR (login NOT IN ($AILIST) AND br='cursor')) cursor,
     COUNTIF(login='devin-ai-integration[bot]' OR (login NOT IN ($AILIST) AND br='devin')) devin,
     COUNTIF(login='google-labs-jules[bot]' OR (login NOT IN ($AILIST) AND br='jules')) jules
   FROM e GROUP BY ym"

# 3) CreateEvent 기반 AI 브랜치 지표 = PR결손 보강 (PullRequestEvent 언더수집 대비)
q "INSERT INTO \`$DS.ai_branch_monthly\`
   WITH c AS (SELECT $YM ym, JSON_EXTRACT_SCALAR(payload,'\$.ref_type') rt,
       LOWER(REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(payload,'\$.ref'), r'^([^/]+)/')) br
     FROM \`githubarchive.day.202*\` WHERE _TABLE_SUFFIX IN ($SUF) AND type='CreateEvent')
   SELECT ym, COUNTIF(rt='branch') branch_creates,
     COUNTIF(rt='branch' AND br IN ($BR)) ai_branches,
     ROUND(100*COUNTIF(rt='branch' AND br IN ($BR))/COUNTIF(rt='branch'),2) ai_branch_pct,
     COUNTIF(br='codex') codex, COUNTIF(br='claude') claude, COUNTIF(br='copilot') copilot,
     COUNTIF(br='cursor') cursor, COUNTIF(br='devin') devin, COUNTIF(br='jules') jules
   FROM c GROUP BY ym"
echo "appended $1 to mart (combined + branch보강). 뷰 ai_pr_metrics_monthly 자동 반영됨."
