# Week 10 추천시스템 최종 복습 HTML 콘텐츠

목표: 2기 추천시스템 Google Slides 1~6강의 용어와 난이도를 기준으로, 최종 복습 HTML을 만든다. 핵심은 "좋은 모델 하나"가 아니라, Feedback Data에서 시작해 Sparsity, Matrix Factorization, Embedding Search, Two-stage Architecture, FastAPI/Streamlit 시뮬레이터, Cold Start 정책까지 하나의 추천 시스템으로 연결해 보는 것이다.

## 기준 Google Slides

서비스 계정으로 접근 가능한 2기 추천시스템 덱을 기준으로 용어를 맞췄다.

| 강의 | 제목 | Slides ID | 활용 |
|---|---|---|---|
| 1강 | `[추천시스템 2기] 1. 개인화를 소개합니다` | `1oC4J85T16ObWpcSwvsB0GQJRDiENwEMQ6qky0gnnKSU` | 추천 문제 정의, GitHub 공개 데이터셋 |
| 2강 | `[추천시스템 2기] 2. 비개인화 추천을 평가해요` | `1ZBmMPGmyIBxiU0-Nf1Ldky2HcxUkOV_oJ6lnB4xzYeA` | Baseline & Fallback |
| 3강 | `[추천시스템 2기] 3. 행렬을 분해해요` | `1Ztmf2QC5DNojN8CbD7LjPG4mr0fM3pgOMBR1N50mfN4` | Feedback, Sparsity, Sparse matrix, Matrix factorization |
| 4강 | `[추천시스템 2기] 4. 행렬을 쪼개요` | `1GWfrwEGXauYYEFiIEhJ0wXyLbbqaDN-wit2xNqNX7pc` | 숨은 취향 차원, ALS/BPR, Embedding, FAISS |
| 5강 | `[추천시스템 2기] 5. Two-stage` | `1e-nASJ7qcpZ3HYWMdsc5tzKRpOJeA3yD9qZGh4RBib0` | Two-stage, Candidate Generation, Re-ranking, Cold Start |
| 6강 | `[추천시스템 2기] 6. Two stage recap` | `11KIgAsHmubxxcwUu4aqURIz08P0NKklE2RuR-NsKkn4` | 전체 복습 흐름의 주 참고자료 |

## 핵심 키워드

- user-item interaction
- Feedback Data
- weighted score
- implicit feedback
- sparsity
- sparse matrix
- train/rank/test split
- seen filtering
- Popularity baseline
- Baseline & Fallback
- Matrix Factorization
- ALS collaborative retrieval
- 숨은 취향 차원
- embedding similarity
- Embedding Search
- BPR
- Two-stage Architecture
- Candidate Generation
- Re-ranking
- LGBM ranker
- candidate recall
- FAISS FlatIP
- 정확도-속도 trade-off
- Two-Tower retrieval
- side feature
- candidate diversity
- Unique@100
- warm user
- cold user
- cold item
- fallback policy
- 추천 운영
- 모델 산출물 묶음
- backend integration
- API contract
- API 응답 약속
- FastAPI
- Streamlit simulator
- temporal leakage

## 44장 구성

1. 제목: 추천시스템 최종 복습
2. 개념, 모델, 운영을 닫는 최종 로드맵
3. 추천 문제 정의: 고객이 고를 것을 미리 맞추는 문제
4. Feedback Data: Explicit은 적지만 명확하고, Implicit은 많지만 noisy함
5. Sparsity: 대부분의 칸은 비어 있음
6. sparse matrix: 비어 있는 칸은 저장하지 않기
7. Split의 목적: 미래 행동을 feature에 섞지 않기
8. seen filtering: 이미 본 item 제외 여부는 서비스 정책
9. Baseline & Fallback
10. Matrix Factorization 파트 전환
11. Matrix Factorization: R ≈ P × Q
12. ALS/BPR: confidence와 순위 학습
13. ALS의 강점
14. ALS의 한계
15. embedding, repo2repo, BPR 관점
16. Embedding Search 파트 전환
17. nearest neighbor search와 brute force
18. FAISS: 정확하게 찾기 vs 더 빠르게 찾기
19. Two-Tower 구조
20. 최신 Two-Tower 전체 warm user 재학습 결과
21. 정확도 vs 다양성
22. Two-Tower의 production 해석
23. Two-stage Architecture 파트 전환
24. Candidate Generation과 Re-ranking 분리
25. Candidate source 혼합
26. 피처 조합과 ranking feature 구조
27. 최신 LGBM re-rank 결과
28. deep ranker가 바로 이기지 못한 이유
29. CTR/CVR만으로는 부족함, nDCG/Recall/Unique/Latency
30. leakage 주의
31. 최종 시스템 연결 파트 전환
32. Notebook에서 백엔드가 호출하는 서비스까지
33. API는 검증된 모델 버전을 읽음
34. 모델 파일, mapping, metadata를 함께 읽는 serving architecture
35. 백엔드와 추천 API 사이의 API contract
36. Streamlit 시뮬레이터로 추천 결과 확인
37. Cold Start와 Fallback 파트 전환
38. 현재 Cold Start 구현 상태
39. Warm user와 Cold Start fallback 설계
40. 현재 404 처리 vs production fallback 응답
41. cold item 해결 방향
42. 추천 실험 체크리스트
43. 핵심 키워드 정리
44. 최종 한 문장

## 최신 Two-Tower 재학습 결과

실행:

```bash
OMP_NUM_THREADS=1 uv run python scripts/train_two_tower_week6_full_v2.py \
  --suffix airflow_20260516_lgbm_eval \
  --output-suffix latest_20260705_full_e3 \
  --epochs 3 \
  --batch-size 4096 \
  --eval-users 999999 \
  --no-mlflow
```

설정:

- item catalog: saved ALS/mapping 기준 100k repo
- train interactions: 2,377,667
- test interactions: 245,540
- eval users: 141,190 warm users
- elapsed: 15.35 min

결과:

| Model | NDCG@10 | NDCG@50 | NDCG@100 | Recall@100 | Unique@100 |
|---|---:|---:|---:|---:|---:|
| Popularity | 0.002234 | 0.005661 | 0.008002 | 0.035600 | 183 |
| ALS | 0.008141 | 0.010538 | 0.011666 | 0.030740 | 9,970 |
| Two-Tower | 0.003480 | 0.005865 | 0.007365 | 0.025077 | 99,615 |

해석:

- ALS는 정확도 기준으로 가장 강한 retrieval baseline이다.
- Two-Tower는 정확도는 낮지만 100k catalog 거의 전체를 추천 후보로 열어 다양성이 크다.
- Two-Tower를 ALS 대체재로 보지 말고, candidate source 확장용으로 보는 것이 맞다.

## 최신 re-rank 비교

2026-05-16 Airflow feature 기준:

- rank rows: 163,400
- positive labels: 877
- Two-Stage/LGBM: NDCG@10 0.010989, NDCG@100 0.022160, Recall@100 0.075824
- ALS/Fallback: NDCG@100 0.018460
- FM/Deep&Wide/DeepFM/DLRM: NDCG@100 0.015860으로 같은 값에 수렴

해석:

- LGBM LambdaRank가 적은 positive label에서도 가장 안정적이었다.
- neural ranker 수치가 같은 값으로 수렴한 것은 모델 우열보다 학습 데이터/튜닝 한계로 보는 편이 안전하다.

## 주의할 점

`notebooks/ghrec/07_two_stage.ipynb` 초기 구현은 개념 설명용으로 남긴다. ranker 학습 label과 평가 label이 모두 test에서 오는 leakage가 있으므로, 성능 결론은 최신 script/docs 기준으로 말한다.

## Cold-start 메시지

현재 API는 unknown actor에 대해 fallback 결과를 직접 반환하지 않는다. 미리 계산한 후보 목록에 actor가 없으면 `ActorNotFoundError`가 발생하고, Streamlit simulator가 cold-start 안내 문구를 보여준다. 따라서 10주차에서는 cold-start를 "해결 완료"가 아니라 "production 추천에서 반드시 별도 정책으로 설계해야 하는 영역"으로 설명한다.

추천 fallback 설계:

- cold user: recent popular, trending repo, onboarding language/topic 선택 기반 추천
- cold item: repo metadata, language/topic/owner graph, related repo candidate로 탐색 기회 확보
- warm user: ALS/Two-Tower retrieval 후보를 LGBM ranker로 재정렬
