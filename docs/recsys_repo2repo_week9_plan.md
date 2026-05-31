# Week 9 repo2repo personalization experiment plan

## 1. Problem definition

개인화 추천의 후보 생성 단계에서 `related` repo-to-repo 후보가 실제로 성능을 올리는지 검증한다. 질문은 "ML repo2repo가 rule 기반 related보다 개인별 held-out repo를 더 잘 후보군에 올리고, 최종 rerank 품질도 개선하는가?"이다.

## 2. Metric

V2 평가와 동일하게 test split 기준 `precision@k`, `recall@k`, `ndcg@k`, `unique_recommended`를 본다. 핵심은 후보 생성 영향인 candidate recall과 최종 개인화 영향인 rerank NDCG를 함께 비교하는 것이다.

해석은 분리한다. Candidate recall 변화는 related graph 후보 소스의 직접 효과로 볼 수 있다. 반면 rerank NDCG는 후보군 변화뿐 아니라 ranker가 학습한 source feature 분포와 평가 후보의 source composition 변화가 같이 섞일 수 있으므로, ML related 자체의 순수 효과라고 단정하지 않는다.

## 3. Split

기존 V2 split을 그대로 사용한다.

- `history`: 사용자 context, seen filter, feature statistics
- `rank_label`: ranker 학습 positive
- `test`: 최종 평가 positive

이 실험은 split을 새로 만들지 않고, 같은 base suffix의 canonical/ranker를 고정한 상태에서 related 후보 소스만 바꾼다.

## 4. Baseline

세 조건을 비교한다.

- `related_off`: ALS head, recent, popular, ALS tail만 사용한다. `related-candidate-cap=0`, `related-top-per-anchor=0`으로 related parquet 접근도 제거한다.
- `rule_related`: 기존 `data/marts/week6/repo_repo_related_mart.parquet`를 사용한다.
- `ml_repo2repo_related`: ML 산출 parquet을 임시 mart dir의 `repo_repo_related_mart.parquet`로 노출하고, 기존 V2 hybridizer가 그대로 읽게 한다.

## 5. ML experiment

ML repo2repo parquet은 mart 호환 schema를 전제로 한다.

- `anchor_repo_id`
- `related_repo_id`
- `rank`
- `cooc_score`

이 파일은 일반 `data/features/recsys_v2/repo2repo_candidates_*` parquet이 아니다. `scripts/recsys_repo2repo_v2.py`를 `--export-mart-path`와 함께 실행해서 만든 `anchor_repo_id, related_repo_id, rank, cooc_score` parquet만 runner의 `--ml-related-path`에 넣는다.

```bash
uv run python scripts/recsys_repo2repo_v2.py \
  --suffix week7_full_20260502 \
  --canonical-path data/features/recsys_v2/canonical_retrieval_rerank_v2_week7_full_20260502.parquet \
  --export-mart-path data/marts/week9/ml_repo_repo_related_mart.parquet \
  --export-mart-run hybrid_rule_als
```

`scripts/recsys_hybridize_candidates_v2.py`는 `--mart-dir` 아래의 `repo_repo_related_mart.parquet`와 `repo_feature_mart.parquet`를 읽는다. 따라서 러너는 rule mart의 `repo_feature_mart.parquet`와 ML related parquet을 임시 mart dir에 symlink 또는 copy해서 기존 CLI를 변경하지 않는다. 러너는 `--ml-related-path`가 위 schema를 가진 parquet인지 먼저 검사한다.

## 6. Personalization validation

실행 흐름은 조건별로 동일하다.

```bash
uv run python scripts/recsys_hybridize_candidates_v2.py ...
uv run python scripts/recsys_eval_v2.py ...
```

평가는 같은 canonical, 같은 base ranker, 같은 k-values를 사용한다. 결과 CSV는 `data/results/recsys_v2/eval_metrics_<condition_suffix>.csv`에 저장되며, `related_off -> rule_related -> ml_repo2repo_related` 순서로 candidate recall과 rerank NDCG가 어떻게 바뀌는지 비교한다.

기본 러너는 dry-run으로 명령만 출력한다.

```bash
uv run python scripts/recsys_repo2repo_personalization_experiment.py \
  --ml-related-path data/marts/week9/ml_repo_repo_related_mart.parquet
```

기본 `--base-suffix`는 현재 로컬에 있는 V2 산출물 기준인 `retrieval_rerank_v2_week7_full_20260502`이고, 기본 ranker는 `retrieval_rerank_v2_week7_full_20260502_fullusers_items100k_n20_norel`이다. 다른 canonical/candidate/ranker를 쓰면 `--base-suffix`, `--ranker-suffix` 또는 명시 path 옵션을 같이 바꾼다.

실행하려면 `--execute`를 추가한다.
