#!/usr/bin/env bash
set -euo pipefail
DIR="/Users/kakao/bda-2/data/ai_trends"
mkdir -p "$DIR"
PROJ="bda-coai"

# 36-day sample: 9/10/11 of each month, 2025-06..2026-06 (2025-10 skipped = broken archive)
# _TABLE_SUFFIX of githubarchive.day.202* strips "202" prefix -> 5 chars, e.g. 20250609 -> "50609"
SUF="'50609','50610','50611','50709','50710','50711','50809','50810','50811','50909','50910','50911','51109','51110','51111','51209','51210','51211','60109','60110','60111','60209','60210','60211','60309','60310','60311','60409','60410','60411','60509','60510','60511','60609','60610','60611'"
AILIST="'Copilot','Claude','claude[bot]','google-labs-jules[bot]','devin-ai-integration[bot]','cursor[bot]','Codex'"
YM="CONCAT('202',SUBSTR(_TABLE_SUFFIX,1,1),'-',SUBSTR(_TABLE_SUFFIX,2,2))"
DT="CONCAT('202',SUBSTR(_TABLE_SUFFIX,1,1),'-',SUBSTR(_TABLE_SUFFIX,2,2),'-',SUBSTR(_TABLE_SUFFIX,4,2))"
TOOL="CASE actor.login WHEN 'Copilot' THEN 'copilot' WHEN 'Claude' THEN 'claude' WHEN 'claude[bot]' THEN 'claude' WHEN 'google-labs-jules[bot]' THEN 'jules' WHEN 'devin-ai-integration[bot]' THEN 'devin' WHEN 'cursor[bot]' THEN 'cursor' WHEN 'Codex' THEN 'codex' END"
Q(){ bq --project_id="$PROJ" query --use_legacy_sql=false --format=csv --max_rows=100000000 "$1"; }

# 1) events cache (AI bot PR events, row-level)
Q "SELECT $DT AS dt, $YM AS ym, actor.login AS login, $TOOL AS tool, repo.name AS repo
FROM \`githubarchive.day.202*\`
WHERE _TABLE_SUFFIX IN ($SUF) AND type='PullRequestEvent' AND actor.login IN ($AILIST)" > "$DIR/events.csv" &
P1=$!

# 2) repo_active cache (repos with >=1 AI PR: total vs ai per month)
Q "SELECT ym, repo, COUNT(*) AS total_pr, COUNTIF(is_ai) AS ai_pr FROM (
  SELECT $YM AS ym, repo.name AS repo, actor.login IN ($AILIST) AS is_ai
  FROM \`githubarchive.day.202*\` WHERE _TABLE_SUFFIX IN ($SUF) AND type='PullRequestEvent')
GROUP BY ym, repo HAVING ai_pr > 0" > "$DIR/repo_active.csv" &
P2=$!

# 3) monthly global denominator + repo counts
Q "SELECT $YM AS ym, COUNT(*) AS total_pr,
  COUNTIF(actor.login IN ($AILIST)) AS ai_pr,
  COUNT(DISTINCT repo.name) AS repos_total,
  COUNT(DISTINCT IF(actor.login IN ($AILIST), repo.name, NULL)) AS repos_ai
FROM \`githubarchive.day.202*\` WHERE _TABLE_SUFFIX IN ($SUF) AND type='PullRequestEvent'
GROUP BY ym ORDER BY ym" > "$DIR/monthly_global.csv" &
P3=$!

wait $P1 $P2 $P3
echo "=== done ==="
wc -l "$DIR"/events.csv "$DIR"/repo_active.csv "$DIR"/monthly_global.csv
