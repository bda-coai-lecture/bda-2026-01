# Week 7 추천시스템 심화: Two-Tower, DeepFM, DLRM

최종 수정: 2026-05-13

## 핵심 결론

Week 7의 핵심 메시지는 "neural retrieval이 ALS를 바로 대체한다"가 아니라, retrieval과 re-rank의 역할을 분리해서 보는 것이다.

- ALS는 여전히 강한 collaborative retrieval baseline이다.
- Two-Tower v2는 feature를 추가해도 단독 retrieval 성능은 ALS보다 낮았다.
- 대신 Two-Tower는 추천 다양성이 압도적으로 커서 candidate 생성 다양화 용도로 해석하는 것이 적절하다.
- 최종 구조는 `ALS / Popularity / Two-Tower candidate 생성 -> LGBM / DeepFM / DLRM re-rank`이 자연스럽다.
- 같은 candidate set re-rank 비교에서는 NDCG@100 기준 `LGBM > DLRM > Deep&Wide ~= DeepFM > FM > ALS/Fallback > Popularity` 순서가 나왔다.

| 모델 | 추천 파이프라인 역할 | 강점 | 주의점 |
|---|---|---|---|
| ALS | retrieval | 빠르고 단순한 collaborative filtering 기준선 | interaction 외 feature를 직접 쓰기 어렵다. |
| Two-Tower | retrieval | user/item side feature를 embedding에 넣고 ANN search로 확장 가능 | negative sampling, embedding 품질, index 운영이 필요하다. |
| LGBM LambdaRank | re-rank | tabular feature와 작은 데이터에서도 강한 기준선 | neural feature interaction을 직접 학습하지는 않는다. |
| DeepFM | re-rank | FM interaction과 deep network를 함께 사용 | LGBM보다 좋아지려면 데이터/튜닝/feature가 충분해야 한다. |
| DLRM | re-rank | feature field embedding과 pairwise interaction을 명시적으로 학습 | sparse/dense feature schema 설계가 중요하다. |

## 최종 실험 결과

기준 지표는 강의 메시지와 맞춰 `NDCG@100`으로 본다.

### Two-Tower v2 retrieval

Two-Tower v2는 user/repo feature를 추가했지만 NDCG@10 기준 ALS `0.00532` 대비 `0.00473`으로 낮았다. 다만 unique recommended@100은 ALS `14.5k` 대비 Two-Tower `285.8k`로 훨씬 높아, ALS 대체재보다는 다양성 보강 candidate 생성기로 해석하는 것이 적절하다.

산출물:

- `scripts/train_two_tower_week6_full_v2.py`
- `data/models/week6/two_tower_week6_full_v2_e5_metrics.csv`
- `data/models/week6/two_tower_week6_full_v2_e5_summary.json`
- `data/models/week6/two_tower_week6_full_v2_e5.pt`

### Full-scale re-rank 비교

실행 suffix: `week7_full_500k_u100k_e3_reuse_b32768`

실험 설정:

- history: 2026-03-14 ~ 2026-04-24
- rank: 2026-04-25 ~ 2026-05-01
- test: 2026-05-02 ~ 2026-05-08
- `sample_ratio=1.0`
- `max_items=500000`이지만 실제 history repos가 `451,113`개라 item cap에는 걸리지 않았다.
- `candidate_k=300`, `hybrid_extra=200`
- `rank_users=100000`, 실제 rank users `9,245`
- `eval_users=100000`
- neural re-rank models `epochs=3`, `batch_size=32768`, CPU

실제 데이터 규모:

| 항목 | 값 |
|---|---:|
| history interactions | 4,040,529 |
| history users | 1,968,877 |
| history repos | 451,113 |
| rank rows | 4,622,500 |
| positive labels | 11,095 |
| positive rate | 0.002400 |
| eval users | 100,000 |

결과:

| 모델 | NDCG@10 | NDCG@50 | NDCG@100 | Recall@100 | Unique@100 |
|---|---:|---:|---:|---:|---:|
| Popularity | 0.002615 | 0.006879 | 0.008764 | 0.037714 | 127 |
| ALS/Fallback | 0.006693 | 0.010729 | 0.012831 | 0.044381 | 12,075 |
| Two-Stage/LGBM | 0.010599 | 0.016095 | 0.018799 | 0.060113 | 14,040 |
| FM | 0.008365 | 0.013490 | 0.016664 | 0.058042 | 16,046 |
| Deep&Wide | 0.009035 | 0.014398 | 0.017342 | 0.058727 | 14,588 |
| DeepFM | 0.008776 | 0.014205 | 0.017341 | 0.059643 | 14,214 |
| DLRM | 0.009227 | 0.014772 | 0.017625 | 0.059748 | 15,028 |

해석:

- NDCG@100 기준 LGBM이 가장 높다.
- neural re-rank 모델 중에서는 DLRM이 가장 높다.
- Deep&Wide와 DeepFM은 거의 동률이다.
- 모든 re-rank 모델이 ALS/Fallback보다 높아서 Week 7의 성능 개선 포인트는 retrieval 교체보다 re-rank에 있다.
- Two-Tower는 단독 성능보다 candidate 다양성 측면에서 의미가 크다.

산출물:

- `data/models/week6/week6_ranker_compare_week7_full_500k_u100k_e3_reuse_b32768_metrics.csv`
- `data/models/week6/week6_ranker_compare_week7_full_500k_u100k_e3_reuse_b32768_summary.json`
- `data/models/week6/lgbm_ranker_compare_week7_full_500k_u100k_e3_reuse_b32768.txt`
- `data/models/week6/fm_ranker_week7_full_500k_u100k_e3_reuse_b32768.pt`
- `data/models/week6/deepwide_ranker_week7_full_500k_u100k_e3_reuse_b32768.pt`
- `data/models/week6/deepfm_ranker_week7_full_500k_u100k_e3_reuse_b32768.pt`
- `data/models/week6/dlrm_ranker_week7_full_500k_u100k_e3_reuse_b32768.pt`

MLflow:

- tracking URI: `sqlite:///mlflow.db`
- experiment: `bda-week7-recsys-re-rank`
- 추천 실험 비교용 핵심 설정은 `exp_*` param으로 별도 기록한다. 전체 args도 함께 남기되, 강의/온보딩에서는 `exp_data_*`, `exp_candidate_*`, `exp_ranker_*`, `primary_*` metric을 중심으로 보면 된다.

## 강의 흐름

1. Week 6 기준선 복습
   - `ALS retrieval -> LGBM re-rank`
   - 기준 문서: `docs/week6_recsys_handoff.md`

2. Two-Tower를 ALS 대신 쓸 수 있는지 확인
   - 실습: `notebooks/ghrec/09_two_tower.ipynb`
   - 실행: `OMP_NUM_THREADS=1 uv run python scripts/train_two_tower_10pct.py`
   - 메시지: ALS는 interaction-only, Two-Tower는 metadata/side feature를 함께 쓴다.

3. 같은 candidate set에서 re-ranker 비교
   - 실습: `scripts/week6_neural_rankers.py`
   - 비교 모델: `LGBM`, `FM`, `Deep&Wide`, `DeepFM`, `DLRM`
   - smoke 실행:

```bash
uv run python scripts/week6_neural_rankers.py \
  --smoke \
  --output-suffix week7_dlrm_smoke \
  --epochs 1 \
  --torch-threads 1 \
  --device cpu
```

4. production 추천 구조와 연결
   - retrieval embedding model과 ANN index
   - re-ranker model과 feature schema/config
   - feature store와 online serving
   - 강의 메시지: 실무에서는 retrieval, re-rank, feature, serving 책임을 분리한다.

## Smoke 결과

`week7_dlrm_smoke`는 1% sample, 1 epoch라 성능 결론용이 아니라 실행 검증용이다.

| model | NDCG@10 | NDCG@50 | NDCG@100 |
|---|---:|---:|---:|
| ALS/Fallback | 0.002062 | 0.002432 | 0.003276 |
| Two-Stage/LGBM | 0.010118 | 0.010645 | 0.010645 |
| DeepFM | 0.000289 | 0.000772 | 0.000943 |
| DLRM | 0.012973 | 0.013487 | 0.013487 |

강의에서는 이 결과를 "DLRM 코드 경로가 정상 동작한다"는 확인으로만 쓰고, 모델 우열은 중간 스케일 이상의 재실험으로 판단한다.

## 다음 작업 candidate

- `notebooks/ghrec/10_neural_retrieval_vs_als.ipynb`: ALS vs Two-Tower retrieval 비교 설명용 노트북
- `notebooks/ghrec/11_deep_rankers.ipynb`: LGBM vs DeepFM vs DLRM 비교 설명용 노트북
- 강의 자료에서는 `NDCG@100`을 메인 지표로 고정하고, @10은 참고 지표로만 사용한다.
