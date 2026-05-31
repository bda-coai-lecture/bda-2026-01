#!/usr/bin/env bash
set -euo pipefail

export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export VECLIB_MAXIMUM_THREADS="${VECLIB_MAXIMUM_THREADS:-1}"

COOC_PATH="data/features/recsys_v2/repo2repo_candidates_cooc_norm_week9_als_hybrid_large_users500k_20260530.parquet"
ALS_PATH="data/features/recsys_v2/repo2repo_candidates_als_item_cosine_week9_als_hybrid_large_users500k_20260530.parquet"
CANONICAL="data/features/recsys_v2/canonical_repo2repo_personalization_20260530_sample100k.parquet"
BASE_CANDIDATES="data/features/recsys_v2/retrieval_candidates_repo2repo_personalization_20260530_sample100k_base.parquet"
RULE_FEATURE_MART="$(pwd)/data/marts/week9_20260530_rule/repo_feature_mart.parquet"
TMP_ROOT="data/tmp/recsys_repo2repo_personalization"

for WEIGHT in 0.65 0.75 0.85 0.95; do
  TAG="${WEIGHT/./}"
  MART="data/marts/week9/ml_repo_repo_related_mart_hybrid_w${TAG}_large_users500k_20260530.parquet"
  OUT_SUFFIX="repo2repo_personalization_20260530_sample100k_hybrid_w${TAG}_related"
  TRAIN_SUFFIX="repo2repo_retrained_20260530_sample100k_hybrid_w${TAG}_related"
  EVAL_SUFFIX="repo2repo_retrained_eval_20260530_sample100k_hybrid_w${TAG}_related"
  MART_DIR="${TMP_ROOT}/repo2repo_20260530_hybrid_w${TAG}_mart"

  echo "=== weight=${WEIGHT} export mart ==="
  uv run python scripts/export_repo2repo_hybrid_weight_mart.py \
    --cooc-path "${COOC_PATH}" \
    --als-path "${ALS_PATH}" \
    --output-path "${MART}" \
    --cooc-weight "${WEIGHT}" \
    --top-k 100

  mkdir -p "${MART_DIR}"
  ln -sf "${RULE_FEATURE_MART}" "${MART_DIR}/repo_feature_mart.parquet"
  ln -sf "$(pwd)/${MART}" "${MART_DIR}/repo_repo_related_mart.parquet"

  echo "=== weight=${WEIGHT} hybridize ==="
  uv run python scripts/recsys_hybridize_candidates_v2.py \
    --suffix retrieval_rerank_v2_20260530_full \
    --output-suffix "${OUT_SUFFIX}" \
    --canonical-path "${CANONICAL}" \
    --candidate-path "${BASE_CANDIDATES}" \
    --mart-dir "${MART_DIR}" \
    --candidate-k 300 \
    --als-head 80 \
    --related-candidate-cap 80 \
    --related-top-per-anchor 10 \
    --related-max-seen-anchors 20 \
    --recent-candidate-cap 20 \
    --popular-candidate-cap 20

  echo "=== weight=${WEIGHT} sample rerank data ==="
  uv run python scripts/recsys_sample_rerank_data.py \
    --suffix retrieval_rerank_v2_20260530_full \
    --canonical-path "${CANONICAL}" \
    --candidate-path "data/features/recsys_v2/retrieval_candidates_${OUT_SUFFIX}.parquet" \
    --output-suffix "${TRAIN_SUFFIX}" \
    --negatives-per-positive 20 \
    --negative-mix hard=0.7,popular=0.15,related=0,random=0.15 \
    --max-train-positives 100000 \
    --max-hard-candidates-per-user 80 \
    --candidate-batch-size 500000 \
    --canonical-batch-size 500000 \
    --write-batch-users 5000 \
    --max-catalog-items 200000

  echo "=== weight=${WEIGHT} train ranker ==="
  uv run python scripts/recsys_train_rerank_v2.py \
    --suffix "${TRAIN_SUFFIX}" \
    --output-suffix "${TRAIN_SUFFIX}" \
    --ranker lgbm \
    --n-estimators 300 \
    --learning-rate 0.05 \
    --num-leaves 63 \
    --min-child-samples 20 \
    --subsample 0.9 \
    --colsample 0.9 \
    --n-jobs 1

  echo "=== weight=${WEIGHT} eval ==="
  uv run python scripts/recsys_eval_v2.py \
    --suffix "${EVAL_SUFFIX}" \
    --canonical-path "${CANONICAL}" \
    --candidate-path "data/features/recsys_v2/retrieval_candidates_${OUT_SUFFIX}.parquet" \
    --ranker-path "data/models/recsys_v2/ranker_lgbm_${TRAIN_SUFFIX}.pkl" \
    --k-values 10,50,100,200 \
    --device cpu \
    --predict-batch-size 8192
done
