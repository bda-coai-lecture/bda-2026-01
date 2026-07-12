#!/usr/bin/env python
"""(Re)build the small precomputed marts that back the DAU/MAU decline-diagnosis
dashboard cards on 'GitHub Core Metrics' (dashboard 4).

Why: the diagnosis cards used to scan `metrics_daily` and the large
`fact_user_repo_activity` on every dashboard refresh. These marts pre-aggregate
everything into a handful of tiny tables (hundreds of bytes), so dashboard loads
read almost nothing from BigQuery — the heavy scan happens once here instead.

Refresh cadence: append-only source, so a daily run is plenty. Schedule via
cron or an Airflow DAG. Run locally:  uv run python scripts/build_diag_marts.py
"""
from __future__ import annotations
import os
from google.cloud import bigquery

PROJECT = os.environ.get("BQ_PROJECT", "bda-coai")
DATASET = os.environ.get("BQ_DATASET", "mart")
SOCIAL = "'WatchEvent','ForkEvent','PullRequestEvent','IssuesEvent','IssueCommentEvent'"
HERO = ("'codecrafters-io/build-your-own-x','public-apis/public-apis',"
        "'anthropics/skills','karpathy/autoresearch'")

# name -> DDL. Each is CREATE OR REPLACE so the script is idempotent.
TABLES: dict[str, str] = {
    # cards 970 (index) + 971 (raw social counts)
    "diag_event_type_monthly": f"""
CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.diag_event_type_monthly` AS
WITH m AS (
  SELECT DATE_TRUNC(activity_date, MONTH) AS month,
    SUM(push_events) push, SUM(watch_events) watch, SUM(fork_events) fork,
    SUM(pull_request_events) pr, SUM(issue_events) issue, SUM(issue_comment_events) issue_comment
  FROM `{PROJECT}.{DATASET}.metrics_daily` GROUP BY month),
b AS (SELECT push bp, watch bw, fork bf, pr bpr, issue bi FROM m WHERE month = DATE '2025-05-01')
SELECT m.month, push, watch, fork, pr, issue, issue_comment,
  ROUND(100*push/bp) push_idx, ROUND(100*watch/bw) star_idx, ROUND(100*fork/bf) fork_idx,
  ROUND(100*pr/bpr) pr_idx, ROUND(100*issue/bi) issue_idx
FROM m CROSS JOIN b
""",
    # card 972 — the only full fact scan; runs once here, not on every refresh
    "diag_user_population_monthly": f"""
CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.diag_user_population_monthly` AS
WITH u AS (
  SELECT DATE_TRUNC(activity_date, MONTH) AS month, user_id,
    MAX(action='PushEvent') did_push,
    MAX(action IN ({SOCIAL})) did_social
  FROM `{PROJECT}.{DATASET}.fact_user_repo_activity` GROUP BY month, user_id)
SELECT month,
  COUNT(DISTINCT IF(did_push, user_id, NULL)) push_users,
  COUNT(DISTINCT IF(did_social AND NOT did_push, user_id, NULL)) social_only_users
FROM u GROUP BY month
""",
    # card 973
    "diag_social_coverage_weekly": f"""
CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.diag_social_coverage_weekly` AS
SELECT DATE_TRUNC(activity_date, WEEK) AS week,
  ROUND(100*SUM(watch_events+fork_events+pull_request_events+issue_events+issue_comment_events)
        /SUM(total_events),2) social_event_pct
FROM `{PROJECT}.{DATASET}.metrics_daily` WHERE activity_date >= '2025-06-01' GROUP BY week
""",
    # card 974
    "diag_activation_monthly": f"""
CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.diag_activation_monthly` AS
SELECT DATE_TRUNC(activity_date, MONTH) AS month,
  ROUND(AVG(active_users)) dau_avg, ROUND(AVG(events_per_active_user),2) events_per_user
FROM `{PROJECT}.{DATASET}.metrics_daily` GROUP BY month
""",
    # card 975 — WatchEvent is a cluster key, so this scan is cheap
    "diag_hero_repo_stars_monthly": f"""
CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.diag_hero_repo_stars_monthly` AS
SELECT DATE_TRUNC(f.activity_date, MONTH) AS month, r.repo_name AS repo, SUM(f.event_count) stars
FROM `{PROJECT}.{DATASET}.fact_user_repo_activity` f
JOIN `{PROJECT}.{DATASET}.repo_metadata` r USING (repo_id)
WHERE f.action='WatchEvent' AND r.repo_name IN ({HERO})
GROUP BY month, repo
""",
}


def main() -> None:
    client = bigquery.Client(project=PROJECT)
    total_gb = 0.0
    for name, ddl in TABLES.items():
        job = client.query(ddl)
        job.result()
        gb = (job.total_bytes_processed or 0) / 1e9
        total_gb += gb
        print(f"  built {name:<32} scanned {gb:6.3f} GB")
    print(f"DONE. rebuilt {len(TABLES)} diag marts. total scanned ~{total_gb:.3f} GB")


if __name__ == "__main__":
    main()
