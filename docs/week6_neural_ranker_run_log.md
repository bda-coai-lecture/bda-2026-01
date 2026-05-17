# Week 6 Neural Ranker 실행 로그

생성일: 2026-05-09 KST

## 완료된 실행

| 실행 | suffix | 범위 | 소요 시간 | 산출물 |
|---|---:|---|---:|---|
| 스모크 | `smoke_neural` | smoke 기본값, CPU | 1.72분 | `data/models/week6/week6_ranker_compare_smoke_neural_*` |
| hist28 mini | `hist28_mini_neural` | hist28 날짜, `max_items=50k`, `rank_users=5k`, `eval_users=10k`, `candidate_k=120`, `hybrid_extra=80`, CPU, 1 epoch | 3.19분 | `data/models/week6/week6_ranker_compare_hist28_mini_neural_*` |
| hist28 중간 캐시 생성 | `hist28_mid_cache` | 2026-03-28~2026-04-24 history, 2026-04-25~2026-05-01 rank, 2026-05-02~2026-05-08 test, `max_items=100k`, `rank_users=30k`, `eval_users=30k`, `candidate_k=200`, `hybrid_extra=100`, CPU, 1 epoch, feature cache 저장 | 8.08분 | `data/models/week6/week6_ranker_compare_hist28_mid_cache_*` |
| hist28 중간 캐시 재사용 | `hist28_mid_e3` | `hist28_mid_cache` feature cache 재사용, FM/Deep&Wide/DeepFM 3 epochs 재학습/평가 | 3.00분 | `data/models/week6/week6_ranker_compare_hist28_mid_e3_*` |
| Week 7 full-scale cache 생성 | `week7_full_500k_u100k_e3` | 2026-03-14~2026-04-24 history, 2026-04-25~2026-05-01 rank, 2026-05-02~2026-05-08 test, `sample_ratio=1.0`, `max_items=500k`, `rank_users=100k`, `eval_users=100k`, `candidate_k=300`, `hybrid_extra=200`, feature cache 저장 | 약 23.7분 후 중단 | `data/models/week6/week6_ranker_compare_week7_full_500k_u100k_e3_features.pkl` |
| Week 7 full-scale cache 재사용 | `week7_full_500k_u100k_e3_reuse_b32768` | 위 feature cache 재사용, LGBM/FM/Deep&Wide/DeepFM/DLRM, 3 epochs, CPU, `batch_size=32768`, MLflow logging | 26.06분 | `data/models/week6/week6_ranker_compare_week7_full_500k_u100k_e3_reuse_b32768_*` |

## 주요 결과

`hist28_mid_e3` 기준 NDCG:

| 모델 | NDCG@10 | NDCG@50 | NDCG@100 |
|---|---:|---:|---:|
| Popularity | 0.00417 | 0.01167 | 0.01477 |
| ALS/Fallback | 0.00952 | 0.01571 | 0.01908 |
| Two-Stage/LGBM | 0.01455 | 0.02229 | 0.02574 |
| FM | 0.01255 | 0.02023 | 0.02396 |
| Deep&Wide | 0.01342 | 0.02101 | 0.02479 |
| DeepFM | 0.01316 | 0.02084 | 0.02451 |

현재 중간 스케일에서는 Two-Stage/LGBM이 NDCG@10/50/100 모두 1위다. Deep&Wide와 DeepFM은 ALS보다 높고 LGBM보다는 낮다.

## Week 7 full-scale 결과

Week 7 강의 기준 지표는 `NDCG@100`으로 본다.

실제 데이터 규모:

| 항목 | 값 |
|---|---:|
| history interactions | 4,040,529 |
| history users | 1,968,877 |
| history repos | 451,113 |
| rank rows | 4,622,500 |
| rank users | 9,245 |
| positive labels | 11,095 |
| eval users | 100,000 |

`week7_full_500k_u100k_e3_reuse_b32768` 기준:

| 모델 | NDCG@10 | NDCG@50 | NDCG@100 | Recall@100 | Unique@100 |
|---|---:|---:|---:|---:|---:|
| Popularity | 0.002615 | 0.006879 | 0.008764 | 0.037714 | 127 |
| ALS/Fallback | 0.006693 | 0.010729 | 0.012831 | 0.044381 | 12,075 |
| Two-Stage/LGBM | 0.010599 | 0.016095 | 0.018799 | 0.060113 | 14,040 |
| FM | 0.008365 | 0.013490 | 0.016664 | 0.058042 | 16,046 |
| Deep&Wide | 0.009035 | 0.014398 | 0.017342 | 0.058727 | 14,588 |
| DeepFM | 0.008776 | 0.014205 | 0.017341 | 0.059643 | 14,214 |
| DLRM | 0.009227 | 0.014772 | 0.017625 | 0.059748 | 15,028 |

요약:

- NDCG@100 기준 `Two-Stage/LGBM`이 1위다.
- neural ranker 중에서는 `DLRM`이 1위다.
- `Deep&Wide`와 `DeepFM`은 거의 동률이다.
- 모든 reranker가 `ALS/Fallback`보다 높다.
- Two-Tower는 ALS 대체보다 후보 생성 다양화 역할로 해석한다.

## 중단한 실행

| 실행 | 범위 | 중단 전 실행 시간 | 중단 사유 |
|---|---|---:|---|
| hist28 full neural | hist28 날짜, `max_items=300k`, `rank_users=100k`, 전체 eval, MPS, 3 epochs | 약 28.7분 | `build_feature_context`에서 pandas/GC 메모리 압박 발생. 모델 학습 전 physical footprint가 약 42GB까지 증가해서 중단. |
| hist28 100k/eval30k neural | hist28 날짜, `max_items=100k`, `rank_users=30k`, `eval_users=30k`, MPS, 3 epochs | 약 11.2분 | 같은 context builder 메모리 압박으로 완료 전 중단. |
| hist28 100k/eval30k CPU | hist28 날짜, `max_items=100k`, `rank_users=30k`, `eval_users=30k`, CPU, 2 epochs | 약 5.1분 | 큰 CPU batch 학습이 실험 루프 용도로 너무 느려서 완료 전 중단. |
| Week 7 full-scale 첫 실행 | `week7_full_500k_u100k_e3`, CPU, `batch_size=8192` | 23.67분 | feature cache 생성은 완료했지만 neural 학습이 너무 느려져 중단. cache는 재사용 가능. |
| Week 7 full-scale cache 재사용, 큰 batch | `week7_full_500k_u100k_e3_reuse_b65536`, CPU, `batch_size=65536` | 짧음 | PyTorch segfault. 이후 `batch_size=32768`, `torch_threads=1`로 낮춰 완료. |

## 메모

- 완료된 실행의 `summary.json`에는 `elapsed_min`이 들어 있다.
- `scripts/week6_neural_rankers.py`는 `week6_two_stage_v2.py`의 무거운 per-user profile 객체 생성을 피하도록 light feature context를 사용한다.
- full-scale 실행을 제대로 돌리려면 candidate feature/rank data를 먼저 캐싱해야 한다. 그래야 neural 학습이 중간에 끊겨도 ALS retrieval과 feature matrix 생성을 반복하지 않는다.
- cache 재사용 실행의 실제 데이터 범위는 cache 생성 실행의 `summary.json`을 기준으로 본다. 스크립트는 이후 실행부터 `feature_cache_args`도 summary에 남긴다.
- 16GB 로컬 Mac에서는 Docker Desktop, Airflow, Metabase를 중지한 뒤 full-scale run을 돌리는 것이 안정적이었다. 완료 run의 최대 RSS는 약 9.7GB였다.
- MLflow는 현재 `sqlite:///mlflow.db` backend로 기록된다. 예전 filesystem backend(`mlruns`) 경고를 피하고, 로컬에서 run 검색/비교를 안정적으로 하기 위한 설정이다.
