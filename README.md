# BDA 2기 — GitHub 추천 시스템

GitHub Archive(BigQuery) 데이터를 활용한 **repo 추천 시스템** 구축 프로젝트.  
데이터 추출 → EDA → Popularity baseline → 행렬분해(ALS) → Two-Stage(ALS+LGBM) → Two-Tower(Neural) → FAISS 서빙 → Streamlit 대시보드까지 full pipeline을 다룹니다.

로컬 Airflow + Metabase + BigQuery metric mart 운영 노트는 [docs/data_platform_local.md](docs/data_platform_local.md)에 정리되어 있습니다.
추천 실험 MLflow 기록/비교 방법은 [docs/mlflow_recsys_tracking.html](docs/mlflow_recsys_tracking.html)에 분리되어 있습니다.

1~6주차 강의 경계와 다음 주차 진행안은 [docs/week6_checkpoint_and_week7_plan.md](docs/week6_checkpoint_and_week7_plan.md)에 정리되어 있습니다.

## 환경 설정

### 1. uv 설치

```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### 2. 프로젝트 세팅

```bash
git clone <repo-url> && cd bda-2
uv sync
```

### 3. GCP 서비스 계정 키

BigQuery 쿼리를 위해 GCP 서비스 계정 키(JSON)가 필요합니다.

1. [GCP Console](https://console.cloud.google.com/) → IAM → 서비스 계정 → 키 생성
2. 필요 권한: `BigQuery Job User` + `BigQuery Data Viewer`
3. 프로젝트 루트에 `gcp-key.json`으로 저장하거나 환경변수 설정:

```bash
export GCP_KEY_PATH="/path/to/your/gcp-key.json"
```

### 4. GitHub 토큰 (선택)

repo 메타데이터 수집 시 GitHub REST API를 사용합니다.

```bash
gh auth login
```

## 프로젝트 구조

```
bda-2/
├── pyproject.toml
├── uv.lock
├── app_reco.py                     # Streamlit 정성평가 대시보드
├── src/
│   ├── gharchive/                  # 데이터 핸들링
│   │   ├── client.py               # BigQuery 클라이언트 + 로거
│   │   ├── extract.py              # 일별 집계 추출
│   │   ├── loader.py               # Parquet 로더
│   │   └── transform.py            # 타입 최적화
│   └── ghrec/                      # 추천
│       ├── recommend.py            # popularity scoring, top-N
│       ├── evaluate.py             # NDCG, precision@K, diversity
│       ├── inference.py            # 추천 inference + 병렬 평가
│       └── metadata.py             # GitHub REST API + SQLite 캐시
├── scripts/
│   ├── eval_full.py                # 전체 데이터 평가 (ALS vs Two-Stage)
│   ├── sync_bq_metrics.py          # parquet → BigQuery metric mart 동기화
│   ├── refresh_repo_metadata.py    # GitHub repo metadata cache 갱신
│   ├── setup_metabase_dashboard.py # Metabase 질문/대시보드 자동 구성
│   └── train_two_tower.py          # Two-Tower 학습 + ALS 비교
├── dags/
│   ├── gharchive_platform_metrics.py # Airflow metric/metadata DAG
│   └── gharchive_dbt_metrics.py      # Cosmos dbt metric DAG
├── dbt/
│   ├── gharchive_metrics/         # Week 7 dbt metric/semantic layer project
│   └── profiles/profiles.yml      # secret 없는 BigQuery profile template
├── docker/
│   └── airflow/Dockerfile          # Airflow 3.2.1 + uv image
├── docs/
│   ├── data_platform_local.md      # 로컬 데이터 플랫폼 운영 문서
│   ├── mlflow_recsys_tracking.html # 추천 실험 MLflow tracking 문서
│   ├── mlflow_recsys_tracking.md   # 추천 실험 MLflow tracking 문서
│   └── week6_checkpoint_and_week7_plan.md # 1~6주차 완료선 + Week 7 진행안
├── notebooks/
│   ├── gharchive/                  # 데이터 파이프라인
│   ├── ghrec/                      # 추천 모델
│   └── platform/                   # Week 7 데이터 제품화
└── data/                           # gitignore 대상
    ├── daily_agg/                  # 추출된 parquet (20260101~20260516)
    ├── repo_metadata.db            # GitHub 메타데이터 SQLite 캐시
    └── models/                     # 학습된 모델 아티팩트
```

## 노트북

현재 1~6주차는 `gharchive` 데이터 핸들링과 `ghrec` 추천 시스템 두 축으로 닫고, 다음 주차부터는 dbt metric mart, semantic layer, Airflow Cosmos, BI dashboard 중심의 데이터 제품화 흐름으로 진행합니다.

### gharchive (데이터 파이프라인)

| # | 노트북 | 내용 |
|---|---|---|
| 00 | setup | BigQuery 클라이언트 설정 |
| 01 | extract_daily_agg | dry run 비용 확인 → 28일분 parquet 추출 |
| 02 | storage_formats | JSON/CSV/Parquet 포맷 비교 벤치마크 |
| 03 | dau | DAU 트렌드 분석 |
| 04 | extract_week5 | 5주차 데이터 추가 추출 (3/15~3/21) |
| 05 | retention_activity | Weekly retention + 유저 활동성 분석 |
| 06 | activity_deep_dive | 활동 패턴 심층 분석 |
| 07 | data_quality | 데이터 건전성 검증 |

### ghrec (추천 시스템)

| # | 노트북 | 내용 | 핵심 결과 |
|---|---|---|---|
| 00 | eda | GitHub Archive EDA | |
| 01 | most_popular | Star vs 가중점수 Top-N | |
| 02 | popularity_prediction | 3주 train/1주 test 평가 | NDCG, Precision@K |
| 03 | repo_metadata | GitHub REST API 메타 수집 | SQLite 캐싱 |
| 04 | user_item_matrix | Sparse matrix 구축 | Dense vs Sparse 비교 |
| 05 | als_vs_popularity | ALS 행렬분해 vs Popularity | ALS 68x 더 다양한 추천 |
| 06 | embedding_exploration | ALS/BPR 임베딩 탐색, 케이스 스터디 | 유사 repo, t-SNE |
| **07** | **two_stage** | **ALS retrieval + LGBM re-rank** | **NDCG@10 +28% vs ALS** |
| **08** | **faiss_benchmark** | **FAISS ANN 벤치마크** | **54ms → 3ms (17x)** |
| **09** | **two_tower** | **Two-Tower (PyTorch) vs ALS** | **NDCG@50 2.7x vs ALS** |

## Two-Stage 추천 구조

```
유저 → [ALS Retrieval] → candidate 400개 → [LGBM Re-rank] → Top-K 추천
              │                              │
        collaborative signal           + metadata features
        (행렬분해 임베딩)              (stars, forks, language,
                                       popularity, user activity)
```

### 전체 데이터 평가 (436K users, n=400)

| Model | K=10 NDCG | K=50 NDCG | K=100 NDCG |
|---|---|---|---|
| Popularity | 0.00016 | 0.00042 | 0.00084 |
| ALS | 0.00117 | 0.00164 | 0.00178 |
| **Two-Stage** | **0.00150** | **0.00206** | **0.00220** |

### Retrieval 모델 비교 (5% sample)

| Model | K=50 NDCG | 특징 |
|---|---|---|
| ALS | 0.00013 | interaction only |
| **Two-Tower** | **0.00035** | + language, stars, forks |

### FAISS 서빙 속도

| Method | Latency | Recall@200 |
|---|---|---|
| Brute-force (sklearn) | 54ms | 100% |
| **FAISS FlatIP** | **3ms** | **100%** |

## Streamlit 대시보드

Repo-to-Repo 추천 정성 평가 도구.

```bash
uv run streamlit run app_reco.py
```

- repo 이름 검색 (stars 순 정렬) 또는 ID 직접 입력
- Two-Stage (ALS → LGBM) / ALS Only 추천 방식 선택
- GitHub API → SQLite 자동 캐싱으로 메타데이터 표시
- FAISS FlatIP으로 3ms 검색

## 로컬 데이터 플랫폼

Airflow 3.2.1로 metadata cache refresh와 BigQuery metric mart sync를 오케스트레이션하고, Metabase에서 기초 지표, AI agent trendy repo, OSS Signal 운영 대시보드를 확인합니다.
Week 7부터는 `dbt/gharchive_metrics`에서 core metric mart와 semantic layer를 정의하고, `dags/gharchive_dbt_metrics.py`에서 Cosmos로 dbt model/test를 Airflow task group으로 실행합니다.

```bash
docker compose up airflow-apiserver airflow-scheduler airflow-dag-processor metabase
```

- Airflow: `http://localhost:8080`
- Metabase: `http://localhost:3001`
- OSS Signal 운영 대시보드: `http://localhost:3001/dashboard/4`
- 운영 문서: [docs/data_platform_local.md](docs/data_platform_local.md)

dbt 단독 실행:

```bash
GCP_KEY_PATH=/path/to/gcp-key.json \
DBT_BIGQUERY_PROJECT=bda-coai \
DBT_BIGQUERY_DATASET=mart \
uv run --with dbt-bigquery dbt build \
  --project-dir dbt/gharchive_metrics \
  --profiles-dir dbt/profiles
```

## 스크립트

```bash
# 최근 35일 parquet에서 aggregate metric만 BigQuery에 업로드
GCP_KEY_PATH=/path/to/gcp-key.json \
uv run python scripts/sync_bq_metrics.py \
  --project bda-coai \
  --dataset mart \
  --parquet-dir data/daily_agg \
  --start 2026-04-12 \
  --end 2026-05-16 \
  --max-days 35 \
  --mode replace-all \
  --skip-fact \
  --build-metrics

# repo metadata cache 갱신
uv run python scripts/refresh_repo_metadata.py \
  --start 2026-04-12 \
  --end 2026-05-16 \
  --top-n 1000 \
  --systematic-sample \
  --cache-tier warm \
  --max-fetch 4500 \
  --rate-limit-pause 0.2

# Week 6 추천 feature cache 생성
uv run python scripts/week6_build_recsys_features.py \
  --use-marts always \
  --output-suffix airflow_light

# Week 7 neural re-rank 비교 smoke
uv run python scripts/week6_neural_rankers.py \
  --smoke \
  --rankers lgbm,fm,deepwide,deepfm,dlrm \
  --epochs 1 \
  --torch-threads 1 \
  --device cpu

# feature-rich Two-Tower 학습 + ALS 비교
OMP_NUM_THREADS=1 uv run python scripts/train_two_tower_week6_full_v2.py
```

## 데이터

| 파일 | 설명 |
|---|---|
| `data/daily_agg/*.parquet` | BigQuery 일별 집계 (20260101~20260516, 136일) |
| `data/repo_metadata.db` | GitHub 메타데이터 SQLite 캐시 |
| `data/models/als_twostage.pkl` | ALS 모델 (64 factors) |
| `data/models/lgbm_ranker.txt` | LGBM LambdaRank re-rank model |
| `data/models/two_tower.pt` | Two-Tower PyTorch 모델 |
| `data/models/index_mappings.pkl` | user/item index 매핑 |
| `data/models/repo_name_map.pkl` | repo_id → repo_name (11.8M) |

## 주요 의존성

| 패키지 | 용도 |
|---|---|
| google-cloud-bigquery | BigQuery 쿼리 |
| pandas, pyarrow | 데이터 처리 |
| implicit | ALS/BPR 행렬분해 |
| lightgbm | LambdaRank re-rank |
| torch | Two-Tower 모델 |
| faiss-cpu | ANN 검색 |
| scikit-learn | 평가 메트릭 |
| streamlit | 대시보드 |
| matplotlib | 시각화 |
