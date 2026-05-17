# MLflow 추천 운영 모니터링

최종 수정: 2026-05-17

이 문서는 추천 모델 실험과 운영 후보 평가를 MLflow에 어떻게 남길지 정리한다. MLflow의 강점은 여러 실험과 백테스트 run을 같은 형식으로 기록하고 비교하는 데 있다. 운영에서는 실시간 모니터링 도구라기보다, 어떤 모델/데이터/파라미터 조합을 선택했는지 남기는 모델 운영 장부로 사용한다.

## 한 장 요약

- tracking URI: `sqlite:///mlflow.db`
- backend store: repo root의 `mlflow.db`
- artifact store: 기본 MLflow artifact 경로
- 비교 질문:
  - candidate 단계에서 어떤 방식이 정답 repo를 더 잘 끌어오는가?
  - 같은 candidate set에서 어떤 re-ranker가 순서를 더 잘 매기는가?
  - 시간이 지나도 각 모델의 성능 차이가 유지되는가?
- UI 실행:

```bash
uv run mlflow ui --backend-store-uri sqlite:///mlflow.db --port 5000
```

- UI: `http://localhost:5000`
- 학습 스크립트는 기본적으로 MLflow에 기록한다. 기록을 끄고 싶을 때만 `--no-mlflow`를 붙인다.

## MLflow를 어디까지 쓸까

MLflow는 기본적으로 실험 추적 도구다. 다양한 모델, feature, split, hyperparameter를 바꿔가며 백테스트한 결과를 run 단위로 남기고 비교하는 데 가장 잘 맞는다. 추천시스템에서는 ALS factor, candidate 수, related 후보 cap, LGBM 설정, neural ranker 구조를 바꾼 결과를 같은 테이블에서 보는 용도다.

운영에서도 쓸 수 있지만 역할은 제한적으로 보는 게 맞다. MLflow는 production 모델이 어떤 run에서 나왔는지, offline NDCG/Recall이 얼마였는지, 어떤 feature snapshot으로 학습했는지를 남기는 기준점이다. 반대로 실시간 latency, CTR 하락 알림, online drift 감지는 Prometheus/Grafana/Datadog/Evidently/WhyLabs 같은 모니터링 도구가 더 적합하다.

이 저장소에서는 MLflow를 다음 정도로만 사용한다.

- 실험/백테스트 run의 config, metric, artifact 위치를 남긴다.
- 운영 후보 모델의 offline 성능과 재현 조건을 확인한다.
- 강의에서는 현업에서 볼 수 있는 기본 experiment tracker 예시로 소개한다.
- 상세 해석과 수업용 비교표는 CSV/summary JSON, 필요하면 DuckDB/Streamlit 대시보드를 source of truth로 둔다.

## 대안 도구

MLflow가 불편하다면 대안은 충분히 있다. 특히 UI에서 실험을 훑고, 차트를 만들고, 리포트를 공유하는 경험은 W&B, Neptune, Comet 쪽이 더 편한 경우가 많다.

| 도구 | 성격 | 이 프로젝트에서의 판단 |
|---|---|---|
| W&B | 제품화된 experiment dashboard, chart/report/sweep/artifact/registry 제공 | 공유와 시각화가 중요하면 MLflow보다 편할 가능성이 높다 |
| Neptune | experiment tracking에 집중한 SaaS | 많은 run과 metric을 테이블/차트로 관리할 때 적합 |
| Comet | W&B와 비슷한 experiment management 플랫폼 | 팀 단위 실험 비교와 리포트가 필요할 때 후보 |
| ClearML | experiment tracking + self-host + job execution에 가까운 MLOps 플랫폼 | 수업용으로는 다소 무겁지만 자체 서버/실행 관리까지 원하면 후보 |
| CSV/JSON + DuckDB + Streamlit | 가장 직접적인 custom dashboard | 현재 BDA 추천 실험처럼 offline metric 해석이 핵심이면 가장 읽기 쉽다 |

결론적으로 이 문서의 포지션은 “MLflow가 유일한 정답”이 아니다. MLflow는 표준적인 실험 장부로 남기고, 수업과 분석에서 정말 필요한 비교 화면은 artifact-first 방식으로 별도 구성하는 쪽이 더 실용적이다.

## 운영 뷰

| 운영 질문 | MLflow에서 볼 것 | 판단 기준 |
|---|---|---|
| candidate 단계에서 어떤 방식이 정답 repo를 더 잘 끌어오나? | `recsys-two-stage`, `recsys-retrieval` | `core_recall_at_100`, `core_unique_at_100`, 모델별 recall/unique |
| 같은 candidate set에서 어떤 re-ranker가 순서를 더 잘 매기나? | `recsys-rerank` | `core_ndcg_at_10`, `core_ndcg_at_100`, 모델별 NDCG/Recall artifact |
| 시간이 지나도 성능이 유지되나? | 동일 설정의 날짜별 run | `exp_data_test_end` 기준으로 `core_*` 추세 비교 |

리서치와 운영 후보 평가는 분리한다. 논문 모델 설명, 구조 비교, 실패 원인 분석은 `docs/week7_recsys_deep_models.md`와 `docs/week7_deep_models_visual.html`에 둔다. MLflow 화면에서는 candidate recall, re-rank NDCG, 시간별 drift를 먼저 본다.

## Experiment 구조

| experiment | 운영 역할 | 대표 스크립트 |
|---|---|---|
| `recsys-two-stage` | candidate + 운영 대표 two-stage 성능. ALS/Fallback과 Two-Stage/Fallback을 같은 run에서 비교 | `scripts/week6_two_stage_v2.py` |
| `recsys-rerank` | 동일 candidate set 위에서 LGBM/FM/Deep&Wide/DeepFM/DLRM re-rank 성능 비교 | `scripts/week6_neural_rankers.py` |
| `recsys-retrieval` | candidate 생성 모델 비교. ALS와 Two-Tower의 정확도/다양성 trade-off 확인 | `scripts/train_two_tower_week6_full_v2.py` |

## 최신 2026-05-16 run

데이터 split:

| 구간 | 기간 |
|---|---|
| history | `2026-03-22` ~ `2026-05-02` |
| rank-label | `2026-05-03` ~ `2026-05-09` |
| test | `2026-05-10` ~ `2026-05-16` |

공통 설정:

- `use_marts=always`
- `max_items=100000`
- `candidate_k=120`
- `hybrid_extra=80`
- `related_candidate_cap=30`
- `related_top_per_anchor=10`
- `related_max_seen_anchors=10`
- `factors=48`
- `iterations=4`
- `eval_users=3000`
- ranker feature cache: `data/features/week6/ranker_features_airflow_20260516.parquet`

| experiment | suffix | run id | elapsed | 핵심 산출물 |
|---|---|---|---:|---|
| `recsys-two-stage` | `airflow_20260516_lgbm_eval` | `b4e1a76a92ed46608ca747b3d7482d31` | 8.81분 | `week6_two_stage_airflow_20260516_lgbm_eval_metrics.csv` |
| `recsys-rerank` | `airflow_20260516_neural_compare` | `4138bb93fe0846fba689518f260ddb06` | 10.14분 | `week6_ranker_compare_airflow_20260516_neural_compare_metrics.csv` |
| `recsys-retrieval` | `airflow_20260516_two_tower` | `646383e71b2e4ee495ecac57484a3c94` | 17.67분 | `two_tower_airflow_20260516_two_tower_metrics.csv` |

## UI에서 봐야 할 컬럼

MLflow run table에는 전체 args를 다 펼치기보다 비교에 필요한 값을 `exp_*`, `core_*` 중심으로 남긴다.

추천해서 볼 param:

| prefix | 의미 |
|---|---|
| `exp_data_*` | history/rank/test 날짜, catalog 크기, rank/eval user 수 |
| `exp_candidate_*` | candidate 수, related 후보 cap, anchor 설정 |
| `exp_ranker_*` | ranker 종류, epoch, batch size, learning rate, hidden dims |
| `exp_run_*` | mart 사용 여부, feature cache 사용 여부, parquet 경로 |
| `cache_*` | feature cache를 재사용한 경우 원본 cache의 주요 설정 |

기본 화면 metric:

| metric | 운영 의미 |
|---|---|
| `core_recall_at_100` | candidate/retrieval이 정답을 충분히 끌어오는지 보는 1차 지표 |
| `core_ndcg_at_10` | 실제 상단 추천 품질. re-rank 의사결정의 대표 지표 |
| `core_ndcg_at_100` | Top 100 전체 정렬 품질 |
| `core_unique_at_100` | 추천 다양성. 인기 repo 쏠림 확인 |
| `elapsed_min` | 운영 비용과 재실행 가능성 |

상세 비교 metric:

| metric | 의미 |
|---|---|
| `core_ndcg_at_10` | primary model의 top-10 ranking 품질 |
| `core_ndcg_at_100` | primary model의 top-100 ranking 품질 |
| `core_recall_at_100` | primary model이 test 정답을 얼마나 포함했는지 |
| `core_unique_at_100` | primary model의 추천 다양성 |
| `{model}_ndcg_at_100` | 개별 모델의 NDCG@100 |
| `{model}_recall_at_100` | 개별 모델의 Recall@100 |

## 최신 run 재실행

LGBM two-stage:

```bash
OMP_NUM_THREADS=1 uv run python scripts/week6_two_stage_v2.py \
  --history-start 2026-03-22 \
  --history-end 2026-05-02 \
  --rank-start 2026-05-03 \
  --rank-end 2026-05-09 \
  --test-start 2026-05-10 \
  --test-end 2026-05-16 \
  --use-marts always \
  --max-items 100000 \
  --candidate-k 120 \
  --hybrid-extra 80 \
  --rank-users 10000 \
  --eval-users 3000 \
  --factors 48 \
  --iterations 4 \
  --als-regularization 0.03 \
  --related-candidate-cap 30 \
  --related-top-per-anchor 10 \
  --related-max-seen-anchors 10 \
  --chunk-size 1000 \
  --ranker-feature-parquet data/features/week6/ranker_features_airflow_20260516.parquet \
  --ranker-feature-summary data/features/week6/ranker_features_airflow_20260516_summary.json \
  --output-suffix airflow_20260516_lgbm_eval
```

Neural re-rank 비교:

```bash
OMP_NUM_THREADS=1 uv run python scripts/week6_neural_rankers.py \
  --history-start 2026-03-22 \
  --history-end 2026-05-02 \
  --rank-start 2026-05-03 \
  --rank-end 2026-05-09 \
  --test-start 2026-05-10 \
  --test-end 2026-05-16 \
  --use-marts always \
  --max-items 100000 \
  --candidate-k 120 \
  --hybrid-extra 80 \
  --rank-users 10000 \
  --eval-users 3000 \
  --factors 48 \
  --iterations 4 \
  --related-candidate-cap 30 \
  --related-top-per-anchor 10 \
  --related-max-seen-anchors 10 \
  --chunk-size 1000 \
  --ranker-feature-parquet data/features/week6/ranker_features_airflow_20260516.parquet \
  --ranker-feature-summary data/features/week6/ranker_features_airflow_20260516_summary.json \
  --output-suffix airflow_20260516_neural_compare \
  --rankers lgbm,fm,deepwide,deepfm,dlrm \
  --epochs 3 \
  --batch-size 8192 \
  --predict-batch-size 32768 \
  --torch-threads 1
```

Two-Tower retrieval:

```bash
OMP_NUM_THREADS=1 uv run python scripts/train_two_tower_week6_full_v2.py \
  --suffix airflow_20260516_lgbm_eval \
  --output-suffix airflow_20260516_two_tower \
  --epochs 3 \
  --batch-size 4096 \
  --eval-users 3000 \
  --embed-dim 64
```

## Backfill

이미 생성된 Week 6/7 artifact를 MLflow로 다시 적재할 때 사용한다.

```bash
uv run python scripts/backfill_mlflow_from_week6_artifacts.py
```

이 스크립트는 기존 metrics/summary/model artifact를 읽어 MLflow run으로 적재한다. 대형 ALS/model cache는 복사하지 않고, 비교에 필요한 핵심 CSV/JSON 중심으로 남긴다.

## 운영 원칙

- 실험 성능표는 CSV/summary JSON을 source of truth로 둔다.
- MLflow는 run 비교, 재현성 확인, artifact 탐색, 운영 후보 모델의 선택 근거 기록용으로 사용한다.
- 실시간 서비스 모니터링과 알림은 MLflow의 주 역할이 아니다.
- 대형 feature/model artifact를 MLflow에 중복 저장하지 않는다.
- `exp_*` param과 `core_*` metric 이름은 바꾸지 않는다. UI 비교 컬럼이 깨진다.
- 날짜 split, feature parquet, mart 사용 여부는 반드시 MLflow param으로 남긴다.
