# Slack 분석 봇 BigQuery 읽기 전용 권한 설정

이 문서는 Slack 분석 봇(`scripts/slack_analyst_bot.py`)이 BigQuery를 **읽기만** 하도록
전용 서비스 계정을 만드는 운영 절차다. 실행 순서대로 그대로 따라가면 된다.

## 1. 왜 IAM에서 막아야 하나

봇은 세션에 `Bash(bq query:*)`를 허용한다. `bq query`는 읽기 전용 표면이 아니다.
`CREATE OR REPLACE`, `DROP`, `DELETE`, `MERGE`가 모두 같은 명령으로 실행되고,
tool 권한 규칙은 **명령 문자열**만 본다. SQL 본문은 무엇으로도 제약할 수 없다.
`Bash(uv run --no-project --with dbt-bigquery dbt:*)`도 마찬가지로 `dbt build`를 포함한다.

즉 쓰기를 막는 지점은 IAM 하나뿐이다.

현재 봇이 쓰게 되는 키는 운영자 키 `gcp-key.json`
(`dane-gcp@bda-coai.iam.gserviceaccount.com`)이고, 2026-07-26 기준 프로젝트 `bda-coai`에서
아래 role을 갖는다. IAM condition은 없다.

```
roles/owner
roles/bigquery.admin
roles/bigquery.user
roles/bigquery.dataViewer
roles/iam.serviceAccountUser
roles/run.developer
roles/run.invoker
roles/secretmanager.secretAccessor
```

`roles/owner` + `roles/bigquery.admin`이다. mart를 통째로 지울 수 있는 키다.
실측 결과는 4절에 있다.

## 2. 전용 서비스 계정 만들기

```bash
gcloud config set project bda-coai

gcloud iam service-accounts create bda-analyst-ro \
  --project=bda-coai \
  --display-name="BDA Slack analyst (BigQuery read-only)" \
  --description="Slack analyst bot. SELECT + dry run only. No DDL/DML."
```

계정: `bda-analyst-ro@bda-coai.iam.gserviceaccount.com`

### 2.1 프로젝트 레벨 — 쿼리 실행 권한만

```bash
gcloud projects add-iam-policy-binding bda-coai \
  --member="serviceAccount:bda-analyst-ro@bda-coai.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser" \
  --condition=None
```

`roles/bigquery.jobUser`는 `bigquery.jobs.create`만 준다. 쿼리 실행과 dry run에 필요하고,
데이터를 읽을 권한은 전혀 포함하지 않는다. 이 권한은 **프로젝트 레벨에만** 존재한다.
dataset으로 좁힐 수 없다.

공개 데이터셋 `githubarchive.day.*`는 별도 grant가 필요 없다(누구나 READER). 다만 쿼리를
돌리는 것 자체는 과금 프로젝트 `bda-coai`의 `bigquery.jobs.create`를 요구하므로, 위 binding이
없으면 공개 데이터도 못 읽는다.

### 2.2 dataset 레벨 — `bda-coai:mart` 읽기만

프로젝트 레벨 `roles/bigquery.dataViewer`는 프로젝트의 **모든** dataset을 열어준다.
쓰지 않는다. dataset 레벨로 좁힌다.

`bq`는 dataset access 정책을 JSON으로 갱신한다.

```bash
bq --project_id=bda-coai show --format=prettyjson bda-coai:mart > /tmp/mart_access.json

jq '.access += [{
      "role": "READER",
      "userByEmail": "bda-analyst-ro@bda-coai.iam.gserviceaccount.com"
    }]' /tmp/mart_access.json > /tmp/mart_access.new.json

bq update --source /tmp/mart_access.new.json bda-coai:mart
```

`access[].role: "READER"`는 `roles/bigquery.dataViewer`와 동일하다.

적용 확인:

```bash
bq --project_id=bda-coai show --format=prettyjson bda-coai:mart \
  | jq '.access[] | select(.userByEmail=="bda-analyst-ro@bda-coai.iam.gserviceaccount.com")'
```

**대안 두 가지**

- 테이블 단위로 더 좁히려면 `bq add-iam-policy-binding --member=... --role=roles/bigquery.dataViewer bda-coai:mart.metrics_daily`.
  단 mart에는 테이블/뷰가 31개(2026-07-26)이고 새 모델이 계속 늘어난다. 분석 봇은 mart 전체를 읽어야 하므로 권장하지 않는다.
- dataset JSON 편집이 부담스러우면 프로젝트 레벨
  `gcloud projects add-iam-policy-binding bda-coai --member=serviceAccount:... --role=roles/bigquery.dataViewer`.
  **읽기 범위가 프로젝트 전체로 넓어진다.** 차선책으로만 쓴다.

### 2.3 부여하면 안 되는 role

| Role | 이유 |
|---|---|
| `roles/bigquery.dataEditor` | `tables.create` / `tables.updateData` / `tables.delete`. `CREATE OR REPLACE`, `INSERT`, `DELETE`, `MERGE`가 전부 통과한다. 막으려는 것 그 자체. |
| `roles/bigquery.dataOwner` | dataEditor + `tables.delete` + dataset ACL 변경. 봇이 자기 권한을 넓힐 수 있다. |
| `roles/bigquery.admin` | 위 전부 + job 취소 + 예약/할당량 조작. |
| `roles/bigquery.user` | **이름이 오해를 부른다. 읽기 전용 role이 아니다.** `bigquery.datasets.create`를 포함하므로 봇이 새 dataset을 만들고 그 dataset의 OWNER가 되어 자유롭게 테이블을 쓸 수 있다. 2.1의 `jobUser`가 필요한 권한(`jobs.create`)만 담은 정확한 role이다. |
| `roles/owner`, `roles/editor` | 설명 불필요. |

## 3. 키 발급과 배치

```bash
mkdir -p /Users/kakao/bda-2/secrets
chmod 700 /Users/kakao/bda-2/secrets

gcloud iam service-accounts keys create /Users/kakao/bda-2/secrets/analyst-bq-key.json \
  --iam-account=bda-analyst-ro@bda-coai.iam.gserviceaccount.com

chmod 600 /Users/kakao/bda-2/secrets/analyst-bq-key.json
```

컨테이너 마운트는 이 경로(`secrets/analyst-bq-key.json`)를 기준으로 별도 작업에서 연결한다.

### 3.1 `.gitignore` 확인 — 커밋 전 필수

`gcp-key.json` 패턴은 이 파일명을 덮지 않는다. `secrets/` 규칙이 별도로 있어야 한다.
키를 만든 뒤 **반드시** 확인한다.

```bash
cd /Users/kakao/bda-2
git check-ignore -v secrets/analyst-bq-key.json
# 기대: .gitignore:18:secrets/	secrets/analyst-bq-key.json
# 출력이 없으면 추적 대상이라는 뜻이다
```

출력이 없으면 `.gitignore`에 아래를 추가한 뒤 다시 확인한다.

```
# Service account keys
secrets/
```

`git status`에 `secrets/`가 보이는 상태로 커밋하지 않는다. 이미 커밋했다면 키를 즉시 폐기
(6절)하고 새로 발급한다. 히스토리에서 지우는 것보다 폐기가 빠르고 확실하다.

## 4. 검증

`scripts/verify_bq_readonly.py`가 **검사 대상 키 하나만** 써서 읽기 전용을 실증한다.

```bash
uv run --no-project --with google-cloud-bigquery \
  python scripts/verify_bq_readonly.py --key-path secrets/analyst-bq-key.json
```

6개 검사: mart SELECT 성공, 공개 raw dry run 성공, `CREATE OR REPLACE` 거부,
`DELETE` 거부, `INSERT` 거부, 스캔 상한 발동. 하나라도 실패하면 exit 1이다.

쓰기 probe는 권한 설정이 틀려도 실제 데이터를 건드릴 수 없게 만들었다.
CREATE는 버려도 되는 `mart.__readonly_probe__`만 대상으로 하고 성공 시 즉시 DROP하며,
DELETE/INSERT는 `WHERE FALSE`를 달아 0행만 건드린다.

### 참고: 현행 운영자 키의 실측 노출

같은 스크립트를 `gcp-key.json`으로 돌린 결과(2026-07-26):

```
[PASS] read_mart_select    (expect succeed) observed=ok
[PASS] dry_run_public_raw  (expect succeed) bytes_processed=43791104
[FAIL] create_table_refused (expect fail) observed=write_succeeded   <- CREATE OR REPLACE 통과
[FAIL] delete_refused       (expect fail) observed=write_succeeded   <- DELETE 통과
[FAIL] insert_refused       (expect fail) observed=write_succeeded   <- INSERT 통과
[PASS] cost_ceiling_bites   (expect fail) observed=500:bytesBilledLimitExceeded
exit 1
```

지금 봇에 이 키를 주면 세션이 mart를 쓸 수 있다. 새 키로는 3~5가 PASS(=거부)여야 한다.

### 4.1 dbt 호환성 — compile은 되고 build는 안 된다

**읽기 전용 SA는 문서화된 compile + dry run 흐름을 깨지 않는다.** 실측으로 확인했다.

`dbt compile --select metrics_daily`이 BigQuery에 하는 일은 `list_bda-coai_mart`
연결 하나, 즉 `mart` dataset의 테이블 목록 조회뿐이다. SQL job은 0건이고 DDL도 없다
(dbt 로그의 새 구간에 `select`/`create`/`insert`/`merge` 문이 한 줄도 없다).
필요한 권한은 `bigquery.tables.list` + `bigquery.datasets.get`으로, 2.2의 dataset READER에
포함된다. `dbt_project.yml`에 `on-run-start` hook도 없어서 compile 중 DDL이 끼어들 여지가 없다.

compile 결과 SQL을 `bq query --dry_run`으로 쏘는 단계는 `bigquery.jobs.create`(2.1)와
읽는 테이블의 dataViewer(2.2)면 충분하다. 공개 `githubarchive.day.*` dry run도 동일하다.

반대로 `dbt run` / `dbt build`는 `CREATE OR REPLACE TABLE` / `MERGE`를 실행하므로
읽기 전용 SA에서 **첫 모델에서 실패한다. 이게 의도한 동작이다.** 봇의 tool 허용 목록은
`dbt build`를 문자열로 구분하지 않으므로, 적재를 막는 것도 IAM이다.
mart 적재는 Airflow(`dags/gharchive_dbt_metrics.py`)가 운영자 키로 계속 수행한다.

새 키 적용 후 아래로 확인한다.

```bash
export GCP_KEY_PATH=/Users/kakao/bda-2/secrets/analyst-bq-key.json
export GOOGLE_APPLICATION_CREDENTIALS=$GCP_KEY_PATH

# 성공해야 한다
uv run --no-project --with dbt-bigquery dbt compile \
  --project-dir dbt/gharchive_metrics --profiles-dir dbt/profiles --select metrics_daily

# 실패해야 한다 (권한 거부)
uv run --no-project --with dbt-bigquery dbt build \
  --project-dir dbt/gharchive_metrics --profiles-dir dbt/profiles --select metrics_daily
```

## 5. 비용 상한

IAM은 쓰기를 막지만 스캔 비용은 막지 못한다. 읽기 전용 키로도 `githubarchive.day.20*`를
범위 없이 긁으면 요금이 난다. 상한은 별도로 걸어야 한다.

### 5.1 쿼리별 상한 (이미 적용됨)

봇은 자식 프로세스 환경에 `BIGQUERY_MAXIMUM_BYTES_BILLED`를 주입한다
(`scripts/slack_analyst_bot.py` `child_env()`). 기본값 200 GiB = 온디맨드
$6.25/TiB 기준 쿼리당 최대 약 $1.25다. `bq`와 클라이언트 라이브러리가 이 값을 읽어
추정치가 넘으면 **실행 전에 job을 실패시킨다. 이때 과금은 0이다.**

조정:

```bash
python scripts/slack_analyst_bot.py --max-scan-gib 50   # 쿼리당 약 $0.31
```

수동 확인:

```bash
BIGQUERY_MAXIMUM_BYTES_BILLED=1048576 \
  bq --project_id=bda-coai query --use_legacy_sql=false \
  'select count(distinct user_id) from `bda-coai.mart.fact_user_repo_activity`'
# -> bytesBilledLimitExceeded 로 즉시 실패해야 한다
```

`verify_bq_readonly.py`의 6번 검사가 이걸 자동으로 확인한다.

### 5.2 누적 상한 (권장 추가 방어선)

쿼리별 상한은 "하루에 200 GiB 쿼리 50번"을 막지 못한다. 계정 단위 누적 상한이 필요하면
BigQuery custom quota를 건다.

- Console: IAM & Admin → Quotas → 서비스 `BigQuery API` →
  `Query usage per day per user` → 한도를 `bda-analyst-ro` 기준으로 설정.
- 사용자별 일일 한도이므로 전용 SA를 쓰는 이 구성과 잘 맞는다. 운영자 키는 영향받지 않는다.
- 한도 초과 시 그 SA의 쿼리만 실패하고, Airflow 적재는 계속 돈다.

사후 감시는 기존 스크립트를 쓴다.

```bash
uv run --no-project --with google-cloud-bigquery \
  python scripts/check_bigquery_cost_guard.py --project bda-coai --lookback-hours 2 --max-usd 3
```

## 6. 회수와 롤백

봇이 이상 동작하면 **키 폐기가 가장 빠르다.** role 회수보다 즉효다.

```bash
# 1) 키 목록 확인
gcloud iam service-accounts keys list \
  --iam-account=bda-analyst-ro@bda-coai.iam.gserviceaccount.com

# 2) 해당 키 삭제 -> 즉시 인증 불가
gcloud iam service-accounts keys delete <KEY_ID> \
  --iam-account=bda-analyst-ro@bda-coai.iam.gserviceaccount.com

rm -f /Users/kakao/bda-2/secrets/analyst-bq-key.json
```

계정 자체를 잠그려면(키가 여러 개일 때 확실하다):

```bash
gcloud iam service-accounts disable bda-analyst-ro@bda-coai.iam.gserviceaccount.com
# 복구
gcloud iam service-accounts enable  bda-analyst-ro@bda-coai.iam.gserviceaccount.com
```

권한만 걷어내려면:

```bash
gcloud projects remove-iam-policy-binding bda-coai \
  --member="serviceAccount:bda-analyst-ro@bda-coai.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# dataset READER 제거
bq --project_id=bda-coai show --format=prettyjson bda-coai:mart > /tmp/mart_access.json
jq 'del(.access[] | select(.userByEmail=="bda-analyst-ro@bda-coai.iam.gserviceaccount.com"))' \
  /tmp/mart_access.json > /tmp/mart_access.new.json
bq update --source /tmp/mart_access.new.json bda-coai:mart
```

완전 제거:

```bash
gcloud iam service-accounts delete bda-analyst-ro@bda-coai.iam.gserviceaccount.com
```

봇을 임시로 멈추는 것뿐이라면 `slack_analyst_bot.py` 프로세스를 죽이는 게 먼저다.
키 폐기는 그 다음이 아니라 **동시에** 한다. 진행 중 세션이 남아 있을 수 있다.

## 7. 체크리스트

- [ ] `bda-analyst-ro` SA 생성
- [ ] 프로젝트 `roles/bigquery.jobUser` 부여
- [ ] `bda-coai:mart` dataset READER 부여 (프로젝트 레벨 dataViewer 아님)
- [ ] `dataEditor` / `dataOwner` / `admin` / `user` 미부여 확인
- [ ] 키를 `secrets/analyst-bq-key.json`에 발급, `chmod 600`
- [ ] `git check-ignore -v secrets/analyst-bq-key.json` 통과
- [ ] `verify_bq_readonly.py` 6/6 PASS
- [ ] `dbt compile` 성공 / `dbt build` 실패 확인
- [ ] `BIGQUERY_MAXIMUM_BYTES_BILLED` 값 확인 (기본 200 GiB)
- [ ] 회수 절차(6절)를 운영자가 한 번 읽어둠
