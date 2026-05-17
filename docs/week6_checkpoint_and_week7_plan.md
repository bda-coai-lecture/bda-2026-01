# Week 6 체크포인트와 Week 7 진행안

최종 수정: 2026-05-12

이 문서는 1~6주차까지의 강의 과정을 한 번 닫고, 다음 주차부터 어떤 흐름으로 이어갈지 정리한다. 현재 과정은 크게 두 축으로 완성된 상태다.

## 1~6주차 완료 범위

### A. 데이터 핸들링 트랙

GitHub Archive 원천 로그를 분석 가능한 형태로 바꾸는 흐름을 다뤘다.

| 주제 | 산출물 | 핵심 메시지 |
|---|---|---|
| BigQuery 연결과 비용 확인 | `notebooks/gharchive/00_setup.ipynb`, `01_extract_daily_agg.ipynb` | 큰 로그는 먼저 dry run과 집계 단위로 다룬다. |
| 저장 포맷 비교 | `02_storage_formats.ipynb` | 분석용 데이터는 CSV/JSON보다 Parquet가 유리하다. |
| 기초 제품 지표 | `03_dau.ipynb` | DAU, event count, active repo 같은 기본 metric을 만든다. |
| 기간 확장 | `04_extract_week5.ipynb` | 실험 기간이 바뀌면 추출 범위와 재현 기준도 같이 관리한다. |
| retention/activity 분석 | `05_retention_activity.ipynb`, `06_activity_deep_dive.ipynb` | cohort, retention, 활동성 segment로 유저 행동을 본다. |
| 데이터 품질 점검 | `07_data_quality.ipynb` | row count, null, 중복, 날짜 범위 검증을 강의 흐름에 포함한다. |

### B. 추천 시스템 트랙

집계된 user-repo interaction을 추천 문제로 바꾸고, baseline부터 two-stage 구조까지 확장했다.

| 주제 | 산출물 | 핵심 메시지 |
|---|---|---|
| 추천용 EDA | `notebooks/ghrec/00_eda.ipynb` | event log를 유저-아이템 문제로 재정의한다. |
| 인기 추천 baseline | `01_most_popular.ipynb`, `02_popularity_prediction.ipynb` | baseline 없이 모델 성능을 말하지 않는다. |
| repo metadata | `03_repo_metadata.ipynb`, `src/ghrec/metadata.py` | 외부 API는 SQLite cache로 안정화한다. |
| user-item matrix | `04_user_item_matrix.ipynb` | sparse matrix가 추천 모델의 기본 입력이다. |
| ALS/BPR retrieval | `05_als_vs_popularity.ipynb`, `06_embedding_exploration.ipynb` | 협업 필터링은 후보 생성과 유사 item 탐색에 강하다. |
| two-stage 추천 | `07_two_stage.ipynb`, `scripts/week6_two_stage_v2.py` | ALS retrieval 뒤에 LGBM ranker로 재정렬한다. |
| FAISS 서빙 | `08_faiss_benchmark.ipynb` | offline 모델을 빠른 검색 구조로 연결한다. |
| neural retrieval | `09_two_tower.ipynb`, `scripts/train_two_tower_10pct.py` | metadata를 넣은 Two-Tower와 ALS를 비교한다. |

## Week 6 기준선

Week 6 추천 실험은 `docs/week6_recsys_handoff.md`를 기준 문서로 둔다.

| 항목 | 현재 기준 |
|---|---|
| 데이터 기간 | `2026-02-15` ~ `2026-05-08` |
| history | `2026-03-14` ~ `2026-04-24` |
| rank-label | `2026-04-25` ~ `2026-05-01` |
| test | `2026-05-02` ~ `2026-05-08` |
| 최고 run | `related80_anchor20_full_als96_i12_lgbm63` |
| 최고 성능 | `Two-Stage/Fallback NDCG@10 = 0.016029`, `Recall@100 = 0.074161` |
| 핵심 결론 | weak segment는 ranker보다 후보 recall 병목이 컸고, item-to-item related source가 가장 크게 개선했다. |

여기까지를 추천 모델링 파트의 1차 완료선으로 둔다. 이후 추천 실험은 "강의 본편"이 아니라 심화/확장 실험으로 분리한다.

## 다음 주차 방향

Week 7은 데이터 핸들링 트랙으로 진행한다. 핵심은 notebook에서 만든 지표를 dbt project로 옮기고, semantic layer에 지표 정의를 고정한 뒤, Cosmos로 Airflow에 연결하는 것이다.

### Week 7 제목

데이터 제품화: dbt Metric Mart, Semantic Layer, Airflow Cosmos

### 학습 목표

- BigQuery fact table을 dbt source로 선언한다.
- SQL model, `ref`, source freshness/test, schema test의 역할을 구분한다.
- DAU, WAU, retention 같은 지표 mart를 dbt model로 만든다.
- dbt Semantic Layer에 metric 이름과 집계 방식을 YAML로 정의한다.
- Cosmos로 dbt project를 Airflow task group으로 렌더링한다.
- Metabase dashboard가 직접 계산식이 아니라 검증된 mart를 보게 만든다.

### 강의 흐름 초안

| 순서 | 내용 | 실습 파일 |
|---:|---|---|
| 1 | 1~6주차 산출물 복습: parquet와 metric table | `docs/data_platform_local.md` |
| 2 | fact table grain 정리: `user_id, repo_id, action, activity_date` | `scripts/sync_bq_metrics.py` |
| 3 | dbt project 구조와 source/ref/test | `dbt/gharchive_metrics` |
| 4 | metric mart SQL 작성 | `dbt/gharchive_metrics/models/marts/platform` |
| 5 | Semantic Layer에 지표 정의 | `dbt/gharchive_metrics/models/semantic/platform_activity.yml` |
| 6 | Cosmos로 dbt를 Airflow task group에 연결 | `dags/gharchive_dbt_metrics.py` |
| 7 | Docker Compose로 Airflow/Metabase 실행 | `docker-compose.yml`, `docker/airflow/Dockerfile` |
| 8 | BigQuery metric mart와 dashboard 확인 | `docs/data_platform_local.md`, `scripts/setup_metabase_dashboard.py` |

### Week 7에서 새로 만들면 좋은 자료

- `notebooks/platform/00_mart_design.ipynb`
- `notebooks/platform/01_dbt_metric_build.ipynb`
- `notebooks/platform/02_semantic_layer.ipynb`
- `notebooks/platform/03_airflow_cosmos_walkthrough.ipynb`
- `notebooks/platform/04_metabase_dashboard_review.ipynb`

notebook은 강의 설명용으로 얇게 두고, 실제 실행 로직은 `scripts/`, `dbt/gharchive_metrics`, `dags/`를 재사용한다.

## 경계 규칙

- 1~6주차 자료는 추천 실험의 기준선으로 유지한다.
- Week 7부터는 "모델 성능 개선"보다 "dbt 지표 정의, 반복 실행, 관찰, 대시보드화"에 초점을 둔다.
- 추천 성능 수치를 다시 갱신하더라도 Week 6 문서에는 기준선 변경 사유를 명시한다.
- 새 주차 자료는 기존 추천 notebook 번호와 섞지 않고 `notebooks/platform/` 아래에 둔다.
