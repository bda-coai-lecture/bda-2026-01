# gharchive_metrics dbt Project

Week 7 데이터 핸들링용 dbt project다. GitHub Archive BigQuery daily sharded table에서 `fact_user_repo_activity`를 incremental fact로 만들고, dashboard용 metric mart와 dbt Semantic Layer 정의를 만든다.

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

Airflow에서는 Cosmos `DbtTaskGroup`으로 dbt model/test를 실행한다. `fact_user_repo_activity`는 dbt incremental model이 소유하며, `raw_start_date`~`raw_end_date` 범위의 GitHub Archive daily shard만 스캔해 해당 `activity_date` partition을 overwrite한다.

## Migration Note

기존 `mart.fact_user_repo_activity`가 unpartitioned table이면 `insert_overwrite` incremental 전략을 바로 적용할 수 없다. 한 번은 partitioned table로 마이그레이션해야 한다. 현재 BigQuery 메타데이터 기준 기존 테이블은 `timePartitioning = null`, clustering은 `action, repo_id`다.

권장 순서:

1. 짧은 날짜 범위로 `raw_start_date`/`raw_end_date`를 잡아 dry run 또는 compile로 SQL을 확인한다.
2. 비용을 확인한 뒤, maintenance window에서 `fact_user_repo_activity`를 partitioned table로 재생성한다.
3. 이후 일별 실행은 하루 또는 며칠 범위만 overwrite한다.

## Boundary

- dbt가 담당: incremental fact, core platform metrics, retention mart, semantic model/metrics YAML
- Python이 당분간 담당: SQLite repo metadata가 필요한 AI agent trendy repo enrichment
