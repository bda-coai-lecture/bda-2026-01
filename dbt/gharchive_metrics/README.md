# gharchive_metrics dbt Project

Week 7 데이터 핸들링용 dbt project다. GitHub Archive BigQuery public table에서 백필한 `mart.fact_user_repo_activity`를 source로 보고, dashboard용 metric mart와 dbt Semantic Layer 정의를 만든다.

## Local Commands

```bash
GCP_KEY_PATH=/path/to/gcp-key.json \
DBT_BIGQUERY_PROJECT=bda-coai \
DBT_BIGQUERY_DATASET=mart \
uv run --with dbt-bigquery dbt build \
  --project-dir dbt/gharchive_metrics \
  --profiles-dir dbt/profiles
```

## Airflow

Airflow에서는 `dags/gharchive_dbt_metrics.py`가 GitHub Archive BigQuery public table을 일별로 집계해 `fact_user_repo_activity`에 먼저 적재한 뒤, Cosmos `DbtTaskGroup`으로 dbt model/test를 실행한다.

## Boundary

- dbt가 담당: core platform metrics, retention mart, semantic model/metrics YAML
- Python이 당분간 담당: SQLite repo metadata가 필요한 AI agent trendy repo enrichment
