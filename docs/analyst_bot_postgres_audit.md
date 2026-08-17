# Slack Analyst Bot Postgres Audit

분석 봇의 JSONL audit/trace/feedback 로그를 별도 Postgres/RDS audit DB에도 함께 저장하는 운영 설계다.
JSONL은 계속 남긴다. RDS 장애나 일시적 네트워크 오류가 Slack 답변 전달을 막으면 안 되기 때문이다.

## 저장 위치

권장 순서:

| 환경 | 위치 | 비고 |
|---|---|---|
| 운영 | 별도 RDS Postgres DB 또는 전용 database/schema | Airflow metadata DB와 분리 |
| 로컬/dev | compose `postgres` 안의 별도 DB/schema | 빠른 검증용. 운영 데이터와 섞지 않음 |
| fallback | `logs/analyst-bot/` 또는 Docker `/home/analyst/state` JSONL | DB 장애 시 재적재 원천 |

RDS connection string은 봇 프로세스만 읽고, 봇이 띄우는 Claude 분석 세션 환경에서는 제거한다.
즉 분석 세션은 audit DB 비밀번호를 볼 수 없다.

## 1. RDS/Postgres 스키마 생성

audit DB에 접속해서 아래 파일을 실행한다.

```bash
scripts/analyst_audit_schema.sql
```

기본 schema 이름은 `analyst_audit`이다. 다른 schema를 쓰려면 SQL 파일의 schema 이름과
봇 환경변수 `ANALYST_AUDIT_SCHEMA`를 같은 값으로 맞춘다.

운영 RDS에서는 봇 전용 DB user를 만들고, 이 schema에만 권한을 준다. Airflow metadata DB 계정이나
RDS superuser를 봇에 넘기지 않는다.

권장 권한:

```sql
grant usage on schema analyst_audit to analyst_bot;
grant select, insert, update on all tables in schema analyst_audit to analyst_bot;
grant usage, select on all sequences in schema analyst_audit to analyst_bot;
alter default privileges in schema analyst_audit
  grant select, insert, update on tables to analyst_bot;
alter default privileges in schema analyst_audit
  grant usage, select on sequences to analyst_bot;
```

## 2. 환경변수

`.env`에 추가한다.

```bash
ANALYST_AUDIT_DATABASE_URL=postgresql://analyst_bot:...@<rds-endpoint>:5432/<db>?sslmode=require
ANALYST_AUDIT_SCHEMA=analyst_audit
```

이 값은 secret이다. Slack 답변, dry-run summary, audit row, Claude child env에 노출하지 않는다.
`--dry-run` summary는 password 없는 `postgresql://user@host:port/db` 형태만 출력한다.

## 3. Docker 실행

이미 `docker-compose.yml`의 `analyst-bot` 서비스가 아래 변수를 전달한다.

```yaml
ANALYST_AUDIT_DATABASE_URL: ${ANALYST_AUDIT_DATABASE_URL:-}
ANALYST_AUDIT_SCHEMA: ${ANALYST_AUDIT_SCHEMA:-analyst_audit}
```

이미지에는 `psycopg[binary]`가 포함된다. 변경 후에는 다시 빌드한다.

```bash
docker compose build analyst-bot
docker compose run --rm analyst-bot --dry-run
docker compose up -d analyst-bot
```

`--dry-run`에서 `audit_database = configured (...)`가 보이면 연결과 필수 테이블 확인이 끝난 상태다.

## 4. 로컬 직접 실행

Docker가 아닌 호스트에서 직접 실행하면서 Postgres audit도 쓰려면 `psycopg`를 같이 주입한다.

```bash
uv run \
  --with slack-bolt \
  --with google-cloud-bigquery \
  --with 'psycopg[binary]>=3.2,<4' \
  python scripts/slack_analyst_bot.py
```

`ANALYST_AUDIT_DATABASE_URL`을 설정하지 않으면 기존처럼 JSONL만 기록한다.

## 5. 기존 JSONL backfill

이미 쌓인 JSONL 로그는 backfill 스크립트로 Postgres에 재적재한다.

```bash
ANALYST_AUDIT_DATABASE_URL=postgresql://... \
uv run --with 'psycopg[binary]>=3.2,<4' \
  python scripts/backfill_analyst_audit_to_postgres.py \
  --state-dir logs/analyst-bot \
  --dry-run
```

문제 없으면 `--dry-run`을 빼고 실행한다.

Docker named volume의 `/home/analyst/state`를 backfill하려면 컨테이너 안에서 실행하거나,
state 파일을 호스트로 복사한 뒤 `--state-dir`를 그 경로로 지정한다.

## 6. 테이블 역할

| 테이블 | grain | 내용 |
|---|---|---|
| `analyst_audit.threads` | Slack thread | team/channel/thread와 Claude session 연결 |
| `analyst_audit.turns` | 분석 요청 1회 | 질문, 답변, 상태, 오류, Slack message ts |
| `analyst_audit.turn_usage` | 분석 요청 1회 | Claude 사용량, BigQuery job/bytes/GiB/USD, 권한 거부 |
| `analyst_audit.trace_events` | trace line | 도구 사용 흐름과 최종 summary |
| `analyst_audit.feedback` | 버튼 클릭 1회 | helpful/inaccurate/needs_more_investigation |
| `analyst_audit.eval_*` | 채점 케이스/실행/점수 | 나중에 testset 자동 채점 연결용 |

채점 기본 view는 `analyst_audit.v_turns_for_scoring`이다.

```sql
select
  audit_id,
  status,
  started_at,
  question,
  answer,
  bq_gib,
  bq_error,
  feedback
from analyst_audit.v_turns_for_scoring
order by started_at desc
limit 20;
```

## 7. 운영 원칙

- Postgres write 실패는 로그에 `failed to write ... to Postgres`로 남기고 Slack 답변은 계속 보낸다.
- `ANALYST_AUDIT_DATABASE_URL`을 설정했는데 연결이나 테이블 확인이 실패하면 preflight에서 막는다.
- 질문/답변 원문이 저장되므로 RDS security group, DB user, secret 보관 범위를 좁게 둔다.
- 운영에서는 Airflow metadata DB나 Metabase application DB에 섞지 않는다. 별도 DB/schema를 쓴다.
