# Week 7 Platform Notebooks

Week 7부터는 1~6주차의 데이터 핸들링/추천 실험 산출물을 운영 가능한 데이터 플랫폼 흐름으로 고정한다.

예정 notebook:

| # | 노트북 | 내용 |
|---|---|---|
| 00 | mart_design | notebook 분석 결과를 mart grain과 schema로 정리 |
| 01 | dbt_metric_build | BigQuery fact를 source로 두고 dbt metric mart 생성 |
| 02 | semantic_layer | dbt semantic model과 metric YAML 정의 |
| 03 | airflow_cosmos_walkthrough | Cosmos로 dbt project를 Airflow task group에 연결 |
| 04 | metabase_dashboard_review | BigQuery metric mart와 dashboard 확인 |

실제 실행 로직은 `scripts/`, `dbt/gharchive_metrics`, `dags/`, `docs/data_platform_local.md`를 기준으로 두고, 이 디렉터리의 notebook은 강의용 해설과 작은 실습을 담당한다.
