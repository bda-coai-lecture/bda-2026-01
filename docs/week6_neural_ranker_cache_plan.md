# Week 6 Neural Ranker 캐시 실행 계획

생성일: 2026-05-09 KST

## 목표

FM, Deep&Wide, DeepFM 실험에서 가장 비싼 구간인 데이터 로딩, ALS 학습, candidate retrieval, feature matrix 생성을 한 번만 수행하고, 이후에는 캐시를 재사용해서 모델 학습/평가만 반복한다.

## 구현 내용

대상 파일:

- `scripts/week6_neural_rankers.py`

추가 옵션:

| 옵션 | 설명 |
|---|---|
| `--write-feature-cache` | retrieval/context/rank feature matrix/eval 후보를 pickle 캐시로 저장 |
| `--reuse-feature-cache` | 저장된 feature cache를 읽고 데이터 로딩, ALS, retrieval, feature 생성을 건너뜀 |
| `--feature-cache-path PATH` | 캐시 경로 직접 지정 |

기본 캐시 경로:

```bash
data/models/week6/week6_ranker_compare_<suffix>_features.pkl
```

## 검증한 실행

### 1. 캐시 생성 smoke

```bash
OMP_NUM_THREADS=1 uv run python scripts/week6_neural_rankers.py \
  --smoke \
  --device cpu \
  --torch-threads 1 \
  --output-suffix cache_smoke \
  --write-feature-cache
```

결과:

- 캐시 생성 성공
- 캐시: `data/models/week6/week6_ranker_compare_cache_smoke_features.pkl`
- metrics: `data/models/week6/week6_ranker_compare_cache_smoke_metrics.csv`
- summary: `data/models/week6/week6_ranker_compare_cache_smoke_summary.json`

### 2. 캐시 재사용 smoke

```bash
OMP_NUM_THREADS=1 uv run python scripts/week6_neural_rankers.py \
  --smoke \
  --device cpu \
  --torch-threads 1 \
  --output-suffix cache_smoke_reuse \
  --reuse-feature-cache \
  --feature-cache-path data/models/week6/week6_ranker_compare_cache_smoke_features.pkl
```

결과:

- 데이터 로딩, ALS 학습, retrieval, feature 생성을 건너뛰고 바로 학습/평가로 진입
- smoke 기준 약 2초 내 완료
- metrics: `data/models/week6/week6_ranker_compare_cache_smoke_reuse_metrics.csv`
- summary: `data/models/week6/week6_ranker_compare_cache_smoke_reuse_summary.json`

## 다음 실행 순서

### 1. mid-scale feature cache 생성

```bash
OMP_NUM_THREADS=1 uv run python scripts/week6_neural_rankers.py \
  --history-start 2026-03-28 \
  --history-end 2026-04-24 \
  --rank-start 2026-04-25 \
  --rank-end 2026-05-01 \
  --test-start 2026-05-02 \
  --test-end 2026-05-08 \
  --max-items 100000 \
  --candidate-k 200 \
  --hybrid-extra 100 \
  --rank-users 30000 \
  --eval-users 30000 \
  --factors 64 \
  --iterations 12 \
  --epochs 1 \
  --batch-size 4096 \
  --predict-batch-size 32768 \
  --device cpu \
  --torch-threads 1 \
  --lgbm-estimators 120 \
  --output-suffix hist28_mid_cache \
  --write-feature-cache
```

### 2. 같은 cache로 neural 학습 반복

```bash
OMP_NUM_THREADS=1 uv run python scripts/week6_neural_rankers.py \
  --device cpu \
  --torch-threads 1 \
  --epochs 3 \
  --batch-size 4096 \
  --predict-batch-size 32768 \
  --lgbm-estimators 120 \
  --output-suffix hist28_mid_e3 \
  --reuse-feature-cache \
  --feature-cache-path data/models/week6/week6_ranker_compare_hist28_mid_cache_features.pkl
```

## 주의

- feature cache는 ALS 모델, 후보 목록, context, rank feature matrix를 포함하므로 크기가 커질 수 있다.
- `--reuse-feature-cache`로 돌릴 때 split/filter/retrieval 관련 옵션은 캐시에 저장된 값을 사용한다. 학습 관련 옵션인 `--epochs`, `--batch-size`, `--lr`, `--hidden-dims`, `--fm-factors`, `--dropout` 위주로 바꿔서 반복 실험하면 된다.
- full-scale 실행은 mid-scale 캐시 크기와 소요 시간을 확인한 뒤 진행한다.
