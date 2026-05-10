# Week 6 Neural Ranker 실행 로그

생성일: 2026-05-09 KST

## 완료된 실행

| 실행 | suffix | 범위 | 소요 시간 | 산출물 |
|---|---:|---|---:|---|
| 스모크 | `smoke_neural` | smoke 기본값, CPU | 1.72분 | `data/models/week6/week6_ranker_compare_smoke_neural_*` |
| hist28 mini | `hist28_mini_neural` | hist28 날짜, `max_items=50k`, `rank_users=5k`, `eval_users=10k`, `candidate_k=120`, `hybrid_extra=80`, CPU, 1 epoch | 3.19분 | `data/models/week6/week6_ranker_compare_hist28_mini_neural_*` |
| hist28 중간 캐시 생성 | `hist28_mid_cache` | 2026-03-28~2026-04-24 history, 2026-04-25~2026-05-01 rank, 2026-05-02~2026-05-08 test, `max_items=100k`, `rank_users=30k`, `eval_users=30k`, `candidate_k=200`, `hybrid_extra=100`, CPU, 1 epoch, feature cache 저장 | 8.08분 | `data/models/week6/week6_ranker_compare_hist28_mid_cache_*` |
| hist28 중간 캐시 재사용 | `hist28_mid_e3` | `hist28_mid_cache` feature cache 재사용, FM/Deep&Wide/DeepFM 3 epochs 재학습/평가 | 3.00분 | `data/models/week6/week6_ranker_compare_hist28_mid_e3_*` |

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

## 중단한 실행

| 실행 | 범위 | 중단 전 실행 시간 | 중단 사유 |
|---|---|---:|---|
| hist28 full neural | hist28 날짜, `max_items=300k`, `rank_users=100k`, 전체 eval, MPS, 3 epochs | 약 28.7분 | `build_feature_context`에서 pandas/GC 메모리 압박 발생. 모델 학습 전 physical footprint가 약 42GB까지 증가해서 중단. |
| hist28 100k/eval30k neural | hist28 날짜, `max_items=100k`, `rank_users=30k`, `eval_users=30k`, MPS, 3 epochs | 약 11.2분 | 같은 context builder 메모리 압박으로 완료 전 중단. |
| hist28 100k/eval30k CPU | hist28 날짜, `max_items=100k`, `rank_users=30k`, `eval_users=30k`, CPU, 2 epochs | 약 5.1분 | 큰 CPU batch 학습이 실험 루프 용도로 너무 느려서 완료 전 중단. |

## 메모

- 완료된 실행의 `summary.json`에는 `elapsed_min`이 들어 있다.
- `scripts/week6_neural_rankers.py`는 `week6_two_stage_v2.py`의 무거운 per-user profile 객체 생성을 피하도록 light feature context를 사용한다.
- full-scale 실행을 제대로 돌리려면 candidate feature/rank data를 먼저 캐싱해야 한다. 그래야 neural 학습이 중간에 끊겨도 ALS retrieval과 feature matrix 생성을 반복하지 않는다.
- cache 재사용 실행의 실제 데이터 범위는 cache 생성 실행의 `summary.json`을 기준으로 본다. 스크립트는 이후 실행부터 `feature_cache_args`도 summary에 남긴다.
