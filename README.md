# BDA 2기 — GitHub Archive 분석

GitHub Archive(BigQuery) 데이터를 일별 집계로 추출하고, popularity 기반 추천 baseline을 구축하는 실습 프로젝트.

## 환경 설정

### 1. uv 설치

[uv](https://docs.astral.sh/uv/)는 Python 패키지/프로젝트 매니저입니다.

```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### 2. 프로젝트 세팅

```bash
git clone <repo-url> && cd bda-2

# 의존성 설치 (Python 3.13 + venv 자동 생성)
uv sync
```

`uv sync` 한 번이면 끝입니다. `uv.lock`이 있으므로 모든 사람이 동일한 버전으로 설치됩니다.

### 3. GCP 서비스 계정 키

BigQuery 쿼리를 위해 GCP 서비스 계정 키(JSON)가 필요합니다.

1. [GCP Console](https://console.cloud.google.com/) → IAM → 서비스 계정 → 키 생성
2. 필요 권한: `BigQuery Job User` + `BigQuery Data Viewer`
3. 환경변수 `GCP_KEY_PATH`에 키 파일 경로를 설정하거나, 노트북과 같은 디렉토리에 `gcp-key.json`으로 저장

```bash
export GCP_KEY_PATH="/path/to/your/gcp-key.json"
```

### 4. GitHub 토큰 (선택)

repo 메타데이터 수집 시 GitHub REST API를 사용합니다. `gh` CLI가 로그인되어 있으면 자동으로 토큰을 가져옵니다.

```bash
gh auth login
```

토큰 없이도 동작하지만, rate limit이 60회/시간으로 제한됩니다 (토큰 있으면 5,000회/시간).

## 프로젝트 구조

```
bda-2/
├── pyproject.toml
├── uv.lock
├── src/
│   ├── gharchive/                  # 데이터 핸들링
│   │   ├── client.py               # BigQuery 클라이언트 + 로거
│   │   ├── extract.py              # 일별 집계 추출
│   │   └── transform.py            # 타입 최적화
│   └── ghrec/                      # 추천
│       ├── recommend.py            # popularity scoring, top-N
│       ├── evaluate.py             # NDCG, precision@K, diversity
│       ├── inference.py            # 추천 inference + 병렬 평가
│       └── metadata.py             # GitHub REST API + SQLite 캐시
├── notebooks/
│   ├── gharchive/                  # 데이터 핸들링 노트북
│   │   ├── 01_extract_daily_agg.ipynb
│   │   └── 02_storage_formats.ipynb
│   └── ghrec/                      # 추천 노트북
│       ├── 01_most_popular.ipynb
│       ├── 02_popularity_prediction.ipynb
│       ├── 03_repo_metadata.ipynb
│       ├── 04_user_item_matrix.ipynb
│       └── 05_als_vs_popularity.ipynb
└── data/                           # gitignore 대상
    ├── daily_agg/                  # 추출된 parquet 파일
    └── repo_metadata.db            # GitHub 메타데이터 SQLite 캐시
```

## 데이터

| 파일 | 설명 |
|---|---|
| `data/daily_agg/*.parquet` | BigQuery에서 추출한 일별 집계 (actor_id, repo_id, type, cnt) |
| `data/repo_metadata.db` | GitHub REST API 응답을 캐싱한 SQLite DB |

두 파일 모두 `data/` 아래라 git에 포함되지 않습니다. 노트북을 순서대로 실행하면 자동 생성됩니다.

### repo_metadata.db 스키마

```sql
CREATE TABLE repo_metadata (
    repo_id       INTEGER PRIMARY KEY,
    repo_name     TEXT NOT NULL,
    description   TEXT,
    language      TEXT,
    stargazers    INTEGER,
    forks         INTEGER,
    topics        TEXT,           -- JSON array string
    license_key   TEXT,
    created_at    TEXT,
    updated_at    TEXT,
    archived      INTEGER DEFAULT 0,
    fetched_at    TEXT NOT NULL,
    http_status   INTEGER DEFAULT 200
);
```

한 번 수집한 repo는 SQLite에 캐싱되어 재호출하지 않습니다.

## 노트북 실행

```bash
# Jupyter 실행 (uv 가상환경 내에서)
uv run jupyter lab

# 또는 VS Code에서 .ipynb 직접 열기 (커널: .venv/bin/python 선택)
```

### gharchive (데이터 핸들링)

1. **01_extract_daily_agg** — dry run으로 비용 확인 → 28일분 데이터 parquet 저장
2. **02_storage_formats** — JSON/CSV/Parquet 포맷 비교 + dtype별 크기/속도 벤치마크

### ghrec (추천)

1. **01_most_popular** — Star 기준 vs 가중 점수 기준 Top-N 비교
2. **02_popularity_prediction** — 3주 train / 1주 test split, K별 평가 (NDCG, Precision@K)
3. **03_repo_metadata** — GitHub REST API로 메타데이터 수집, SQLite 캐싱, 예측 결과 해석
4. **04_user_item_matrix** — Feedback → User×Item matrix, Dense vs Sparse 메모리 비교
5. **05_als_vs_popularity** — ALS 행렬분해 vs Popularity baseline 성능 비교 (멀티프로세싱 평가)

## 주요 의존성

| 패키지 | 용도 |
|---|---|
| google-cloud-bigquery | BigQuery 쿼리 실행 |
| pyarrow | Parquet 읽기/쓰기 |
| pandas | 데이터 처리 |
| requests | GitHub REST API 호출 |
| scipy | Sparse matrix |
| implicit | ALS 행렬분해 |
| matplotlib | 시각화 |
| tqdm | 진행률 표시 |
| ipykernel | Jupyter 커널 |
