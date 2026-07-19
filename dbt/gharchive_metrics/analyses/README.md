# dbt analyses

일회성 또는 탐색용 BigQuery SQL을 두는 경로다. 저장소의 전체 실행 원칙은 [`docs/bigquery_dbt_analysis_workflow.md`](../../../docs/bigquery_dbt_analysis_workflow.md)를 따른다.

분석 파일은 질문과 출력 grain을 파일 상단에 기록하고, 기간을 `var()`로 받는다.

```sql
-- Question:
-- Grain:
-- Time basis / timezone: activity_date / UTC
-- Validation range:
-- Analysis range:
-- Segments:
-- Exclusions:
-- Metric definition:
-- Expected output grain:

{% set start_date = var('analysis_start_date', '2026-07-01') %}
{% set end_date = var('analysis_end_date', '2026-07-01') %}

with filtered_activity as (
    select
        activity_date,
        user_id,
        repo_id,
        action,
        event_count
    from {{ ref('stg_user_repo_activity') }}
    where activity_date between date('{{ start_date }}') and date('{{ end_date }}')
),

aggregated_daily as (
    select
        activity_date,
        approx_count_distinct(user_id) as active_actors,
        approx_count_distinct(repo_id) as active_repos,
        sum(event_count) as events
    from filtered_activity
    group by activity_date
)

select
    activity_date,
    active_actors,
    active_repos,
    events
from aggregated_daily
order by activity_date
```

먼저 1일 범위로 compile하고 compiled SQL을 BigQuery dry run한 뒤 필요한 만큼만 기간을 확장한다.
