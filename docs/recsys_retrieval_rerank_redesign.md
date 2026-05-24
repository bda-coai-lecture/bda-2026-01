# Retrieval / Re-rank 데이터 재설계

최종 수정: 2026-05-22

## 문제 정의

이번 추가 실험의 목적은 기존 Week 6/7 자료를 대체하지 않고, 추천 파이프라인의 데이터 설계를 새로 분리해 검증하는 것이다.

현재 확인한 문제는 세 가지다.

1. 초기 노트북 `notebooks/ghrec/07_two_stage.ipynb`는 ranker 학습 label과 최종 평가 label이 모두 `test_gt`에서 온다. 즉 test-period label leakage가 있다.
2. 최신 스크립트 계열은 `history -> rank_label -> test` split으로 개선됐지만, re-rank 학습 row는 retrieval/hybrid 후보 안에 들어온 positive만 사용한다. 그래서 rank-label positive가 충분해도 실제 학습 positive가 매우 작아질 수 있다. 2026-05-16 run 문서에는 `rank labels=361,371`, `rank rows=163,400`, `positive labels=877`로 기록돼 있다.
3. retrieval과 re-rank 비교 실험의 positive 원천이 명확히 하나로 고정돼 있지 않아, 모델별 성능 차이가 데이터 구성 차이인지 모델 차이인지 해석이 흐려진다.

따라서 새 실험은 다음 원칙으로 간다.

- candidate generation 모델과 re-ranker는 같은 train positive split을 사용한다.
- 이전 주차와 같은 test window를 유지해 성능 비교가 가능하게 한다.
- 기존 노트북/스크립트는 건드리지 않고 새 스크립트/문서/산출물 suffix를 추가한다.
- re-ranker 학습 positive는 retrieval candidate 안에 우연히 들어온 positive로 제한하지 않는다.
- train/test temporal split은 평가를 위한 것이고, train split 내부에서 retrieval positive와 rerank positive를 임의로 다른 기간으로 나누지 않는다.

## 기존 구현 판정

### 초기 노트북

`notebooks/ghrec/07_two_stage.ipynb`

- `TRAIN=2026-03-01..2026-03-28`, `TEST=2026-03-29..2026-04-03`
- ALS retrieval은 train으로 학습한다.
- LGBM ranker 학습 label도 `test_gt`를 사용한다.
- 최종 평가도 같은 `test_gt`를 사용한다.
- 결론: 강의 개념 설명용으로는 남길 수 있지만, 성능 비교 기준으로 쓰면 안 된다.

### Week 6/7 스크립트

`scripts/week6_two_stage_v2.py`, `scripts/week6_neural_rankers.py`

- 기본 split은 개선돼 있다.
  - history: `2026-03-14..2026-04-24`
  - rank-label: `2026-04-25..2026-05-01`
  - test: `2026-05-02..2026-05-08`
- 최신 자동화 run은 다음 split도 쓴다.
  - history: `2026-03-22..2026-05-02`
  - rank-label: `2026-05-03..2026-05-09`
  - test: `2026-05-10..2026-05-16`
- ranker 학습은 `retrieval/hybrid candidates`에 feature를 만들고, `repo_id in rank_labels[user]`로 label을 붙인다.
- `add_label_only_candidates()`는 ranker train 후보에 rank-label positive를 강제로 추가할 수 있다. 평가 후보에는 넣지 않는다.
- neural ranker 비교도 같은 candidate-conditioned rank data를 쓴다. 그래서 retrieval이 positive를 놓치면 neural ranker 학습 positive도 같이 줄어든다.

### Mart 주의점

현재 `data/marts/week6/experiment_split_mart.parquet`에는 `split_start_date`, `split_end_date` 컬럼이 없는 구버전일 수 있다. 코드가 날짜 컬럼이 있을 때만 검증하면 `--use-marts always`와 날짜 args가 달라도 오래된 mart를 조용히 읽을 수 있다.

새 실험 전에는 split mart를 재생성하거나, 새 loader에서 split window 검증을 강제한다.

## 새 실험의 canonical dataset

이 실험에서는 하나의 positive dataset을 명시한다.

```text
canonical_positive_interactions
= experiment_split_mart.parquet 또는 daily_agg에서 동일 event weight로 만든
  (split, actor_id, repo_id, score)
```

split별 역할:

| split | 역할 |
|---|---|
| `history` | user context, seen item, warm user/item filter, feature seed |
| `rank_label` | V2 train positive. candidate generator와 re-ranker가 같이 쓰는 학습 label |
| `test` | 최종 평가 positive |

비교 기준 split은 지난 주차 full-scale 결과와 맞춘다.

| split | 기간 |
|---|---|
| history | `2026-03-14` ~ `2026-04-24` |
| rank-label | `2026-04-25` ~ `2026-05-01` |
| test | `2026-05-02` ~ `2026-05-08` |

추가로 최신 자동화 split은 별도 run으로만 비교한다.

| split | 기간 |
|---|---|
| history | `2026-03-22` ~ `2026-05-02` |
| rank-label | `2026-05-03` ~ `2026-05-09` |
| test | `2026-05-10` ~ `2026-05-16` |

## Candidate generation / re-rank 학습 설계

V2의 핵심은 `rank_label`을 하나의 train positive split으로 고정하는 것이다.

| 구성요소 | 학습 positive | negative |
|---|---|---|
| ALS / BPR retrieval | `rank_label` user-repo positives | ALS confidence 또는 BPR pairwise negative |
| Two-Tower retrieval | `rank_label` user-repo positives | in-batch + sampled negatives |
| LGBM / neural re-ranker | `rank_label` user-repo positives | mixed negative sampling |

`history`는 retrieval 학습 positive를 따로 만들기 위한 split이 아니다. V2에서는 user profile, seen item, warm-start filter, feature seed로만 쓴다. 필요하면 history+rank_label 누적 학습 run을 별도 ablation으로 둘 수 있지만, main 비교는 retrieval과 rerank가 같은 `rank_label` positive를 쓰는 버전으로 둔다.

### ALS baseline

- input: `rank_label`의 user-repo weighted matrix
- negative: implicit ALS 내부 confidence 방식에 맡긴다.
- output: user별 top-K candidates

### Two-Tower retrieval

- positive: `rank_label`의 user-repo positive
- negative:
  - in-batch negatives
  - sampled catalog negatives
  - optional hard negatives: retrieval score가 높지만 positive가 아닌 item
- 평가: `test` positive에 대한 retrieval top-K 성능
- 주의: Two-Tower retrieval 성능은 re-ranker 학습 데이터와 섞지 않고 별도로 기록한다.

## Re-rank 학습 데이터 설계

새 설계의 핵심은 re-ranker 학습을 candidate-only positive에 묶지 않는 것이다.

### Positive

- source: `rank_label` split의 전체 positive user-repo pair
- filter:
  - user는 history에 존재하는 warm user 중심으로 시작한다.
  - repo는 retained catalog 안에 있는 item으로 제한한다.
  - history seen item은 제외한다.
- weight:
  - 기본 label은 binary `1`
  - optional relevance weight는 `log1p(score)` 또는 event-strength bucket으로 별도 실험한다.

### Negative

user별 positive 하나당 `N`개 negative를 샘플링한다. 기본값은 `N=20`으로 시작한다.

외부 자료 기준:

- BPR은 implicit feedback에서 observed interaction은 positive지만 unobserved pair는 실제 negative와 missing value가 섞여 있다고 본다. 그래서 negative를 어떻게 뽑는지가 학습 문제의 핵심이다. 참고: Rendle et al., "BPR: Bayesian Personalized Ranking from Implicit Feedback" https://arxiv.org/abs/1205.2618
- TensorFlow Recommenders retrieval task는 in-batch negatives, hard negative 유지, accidental hit 제거를 API 차원에서 지원한다. 즉 positive pair를 두고 batch 내 다른 candidate를 negative로 쓰는 방식은 표준적인 retrieval 학습 패턴이다. 참고: https://www.tensorflow.org/recommenders/api_docs/python/tfrs/tasks/Retrieval
- Ding et al. 2018은 BPR 성능이 negative sampler 품질에 크게 의존하며, 전체 item space uniform negative가 비효율적이고 성능을 낮출 수 있다고 보고한다. 참고: https://hexiangnan.github.io/papers/www18-improvedBPR.pdf
- implicit feedback은 positive-unlabeled / MNAR 문제가 있으므로, unobserved를 모두 확정 negative처럼 취급하면 편향이 생긴다. 참고: Saito et al., "Unbiased Recommender Learning from Missing-Not-At-Random Implicit Feedback" https://huggingface.co/papers/1909.03601

negative source mix:

| source | 비율 | 설명 |
|---|---:|---|
| retrieval hard negative | 40% | 같은 train positive로 학습한 retriever 후보 중 positive가 아닌 item |
| popular/recent negative | 25% | train 기간 기준 인기/최근 item 중 user positive가 아닌 item |
| related/source negative | 20% | co-occurrence, same language/owner/topic 후보 중 positive가 아닌 item |
| random catalog negative | 15% | retained catalog에서 무작위 샘플링한 unobserved item |

샘플링 규칙:

- train positive는 negative로 쓰지 않는다.
- `history` seen item은 기본적으로 negative로 쓰지 않는다. 단, "이미 본 repo를 다시 추천하지 않는다" 정책 평가에서는 seen exclusion을 유지한다.
- `rank_label` positive는 negative로 쓰지 않는다.
- `test` positive는 학습 시점에 알 수 없으므로 negative sampling에서 제외하지 않는다.
- user별 group에는 모든 train positive를 먼저 넣고, negative를 source 비율에 맞춰 채운다.
- negative source, candidate rank, retrieval score를 feature/metadata로 남긴다.

현재 주장에 대한 판정:

- "retrieval과 rerank가 같은 positive를 써야 한다"는 주장은 틀리지 않다. TFRS multi-task 예제도 retrieval/ranking task를 같은 train set 안에서 함께 최적화하는 형태를 보여준다. 참고: https://www.tensorflow.org/recommenders/examples/multitask/
- "적절한 negative sampling이 필요하다"는 주장도 맞다. 특히 random negative만 쓰면 너무 쉬운 문제가 되고, hard negative만 쓰면 false negative와 instability가 커질 수 있다. 그래서 mixed negative가 합리적이다.
- 단, unobserved item을 모두 true negative로 해석하면 안 된다. 문서와 summary에는 "sampled unlabeled negatives"로 기록한다.

## 평가 프로토콜

### Main evaluation

학습과 평가는 단순하게 분리한다.

1. `rank_label` train positive로 candidate generator를 학습한다.
2. 같은 `rank_label` train positive로 re-ranker를 학습한다.
3. candidate generator가 `test` user 후보를 만든다.
4. re-ranker가 그 후보를 재정렬한다.
5. metric 분모는 전체 `test` positive다.

이 평가는 최종 성능 표의 main metric이다.

### Diagnostic

re-ranker 자체가 user-item scoring을 배웠는지 확인한다.

1. `rank_label` 전체 positive + sampled negative로 re-ranker를 학습한다.
2. evaluation candidate set은 retrieval candidates에 test positive 일부를 섞은 diagnostic set으로 만든다.
3. 이 결과는 모델 학습력 확인용으로만 보고, main 성능 표와 섞지 않는다.

이 프로토콜은 "retrieval recall 병목"과 "ranker 학습 실패"를 분리해 보기 위한 것이다.

## 공정성 / 누수 체크리스트

- candidate generator와 re-ranker가 같은 train positive split을 쓰는가?
- retrieval 학습 matrix와 rerank train label에 `test`가 들어가지 않았는가?
- re-ranker positive 원천이 retrieval과 같은 canonical train dataset에서 오는가?
- `rank_label` positive가 candidate 안에 들어왔는지 여부가 학습 positive 사용 여부를 결정하지 않는가?
- negative sampling에서 train positive를 제외했는가?
- popular/recent/related negative source가 train split 안에서만 만들어졌는가?
- `repo_feature_mart.snapshot_date`, `user_profile_mart.window_end_date`가 feature cutoff보다 미래가 아닌가?
- `experiment_split_mart`의 split 기간이 CLI args와 정확히 일치하는가?
- ALS, Two-Tower, LGBM, DeepFM/DLRM이 같은 catalog, 같은 user/item id mapping을 쓰는가?
- model별 candidate set과 fallback policy가 명시돼 있는가?
- negative source mix, positive count, negative count, positive rate를 run summary에 저장했는가?
- hyperparameter tuning 결과를 test metric으로 반복 선택하지 않았는가?

## 구현 계획

기존 파일을 수정하지 않고 새 파일을 추가한다.

1. `scripts/recsys_build_canonical_dataset.py`
   - daily agg 또는 mart에서 canonical split parquet 생성
   - split 기간, event weights, retained catalog, user/item counts를 summary json에 저장
2. `scripts/recsys_sample_rerank_data.py`
   - rank-label 전체 positive 기반으로 mixed negative sampling
   - negative source mix와 false-negative 방지 규칙을 metadata에 저장
   - output: `data/features/recsys_v2/rerank_train_*.parquet`
   - columns: `actor_id`, `repo_id`, `label`, `source`, `retrieval_score`, `candidate_rank`, feature columns
3. `scripts/recsys_train_retrieval_v2.py`
   - ALS baseline 우선
   - 이후 Two-Tower 추가
4. `scripts/recsys_train_rerank_v2.py`
   - LGBM LambdaRank와 BCE scorer를 분리
   - LambdaRank는 sampled groups, BCE neural ranker는 같은 sampled data 사용
5. `scripts/recsys_eval_v2.py`
   - main test metric과 diagnostic metric을 분리 출력

첫 구현은 ALS + LGBM만으로 작게 시작한다.

권장 smoke:

```bash
uv run python scripts/recsys_build_canonical_dataset.py --smoke --suffix v2_smoke
uv run python scripts/recsys_train_retrieval_v2.py --suffix v2_smoke
uv run python scripts/recsys_sample_rerank_data.py --suffix v2_smoke --negatives-per-positive 20
uv run python scripts/recsys_train_rerank_v2.py --suffix v2_smoke --ranker lgbm
uv run python scripts/recsys_eval_v2.py --suffix v2_smoke
```

full 비교 suffix:

```text
retrieval_rerank_v2_week7_full_20260502
```

비교 대상:

- 기존 `week7_full_500k_u100k_e3_reuse_b32768`
- 새 `retrieval_rerank_v2_week7_full_20260502`

## 일차 결론

사용자 지적은 타당하다. 이전 노트북에는 test leakage가 있고, 최신 스크립트도 split은 개선됐지만 ranker 학습 positive가 retrieval 후보에 들어온 소수 positive로 제한되는 구조가 남아 있다. 새 실험은 `rank_label` 전체 positive를 candidate generator와 re-ranker가 같이 쓰고, mixed negative sampling을 명시적으로 설계한다.

최종 metric은 `test` positive로 평가한다. 이때 candidate recall과 rerank NDCG를 같이 보고, 성능 차이가 retrieval recall 병목인지 ranker 학습 문제인지 분리해서 해석한다.
