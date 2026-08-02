# Slack 분석 봇 데모 실행 런북

이 문서는 Slack 분석 봇을 짧게 켜고, 질문하고, 정상 동작을 확인하는 순서만 담는다.
권한 설계와 상세 장애 기록은 `docs/analyst_bot_handoff.md`를 본다.

## 1. 실행 전 확인

터미널에서 먼저 설정만 점검한다.

```bash
cd /Users/kakao/bda-2
set -a && source ./.env && set +a

uv run --with slack-bolt --with google-cloud-bigquery \
  python scripts/slack_analyst_bot.py --dry-run
```

마지막에 `모든 사전 점검 통과.`가 나오면 실행할 수 있다.
여기서 실패하면 Slack에 메시지를 보내도 답장이 오지 않는다.

Metabase 카드 생성까지 시연하려면 실행 전에 아래도 필요하다. 없으면 봇은 분석 답변만 하고
Metabase MCP는 비활성 상태로 둔다.

```bash
export ANALYST_ENABLE_METABASE_MCP=1
export METABASE_URL=http://localhost:3001
export METABASE_PUBLIC_URL=http://localhost:3001
export METABASE_API_KEY=...
```

## 2. 봇 켜기

```bash
uv run --with slack-bolt --with google-cloud-bigquery \
  python scripts/slack_analyst_bot.py --max-workers 1
```

Metabase MCP를 켠 상태로 실행할 때는:

```bash
uv run --with slack-bolt --with google-cloud-bigquery \
  python scripts/slack_analyst_bot.py --max-workers 1 --enable-metabase-mcp
```

아래 로그가 보이면 Slack 이벤트를 받을 준비가 된 상태다.

```text
connected as bot_user_id=...
⚡️ Bolt app is running!
```

터미널을 닫거나 `Ctrl-C`를 누르면 봇도 멈춘다. 멈춘 동안 Slack에 보낸 메시지는 처리되지 않는다.

## 3. Slack에서 질문하기

채널에서는 봇을 멘션한다.

```text
@분석봇 최근 완료된 1일 활동 계정 수 간단히 봐줘
```

DM에서는 멘션 없이 질문해도 된다.

```text
최근 완료된 1일 활동 계정 수 간단히 봐줘
```

채널 thread의 후속 질문은 계속 멘션을 붙인다. DM thread에서는 그대로 이어서 물어보면 된다.

## 4. 정상 화면 기준

실행 중 진행 메시지는 사람이 읽는 상태만 보여야 한다.

```text
분석 중
쿼리 실행 전 안전 조건을 확인하고 있습니다.

진행상황
• 분석 환경을 준비했습니다.
• 분석 절차와 안전 규칙을 확인하고 있습니다.

12초 경과 / 최대 15분 · 같은 thread에서 이어서 질문하면 맥락을 이어갑니다.
```

아래처럼 내부 trace가 Slack에 보이면 마감 버그다.

```text
init model=...
rate-limit {...}
tool Skill: ...
result[ok] ...
```

최종 답변은 진행 메시지가 답변으로 교체되는 형태여야 한다. 첫 줄은 결론이고,
`간단히`라고 물었으면 2-4문장과 한계 한 줄 정도로 끝나야 한다.
답변 하단에는 짧은 실행 footer와 피드백 버튼이 붙는다.
`분석 완료`, `아래가 답변입니다`, `---`, `모드: 활동량`, `인풋`, `관측 grain` 같은
내부 절차 라벨이 답변 본문에 보이면 안 된다. `dry run`, `bq query`, `dbt compile`,
`Claude N턴`, `BigQuery N GiB`도 Slack 최종 답변에 보이면 안 된다. 이런 값은 로그로만 본다.
`active actor`, `actor.id`, `event type`, `raw/mart/grain` 같은 용어가 첫 화면을 지배하면
톤 회귀다. 필요한 경우에도 `활동 계정`, `계정 ID`, `활동 종류`, `집계 단위`처럼 먼저 풀어 쓴다.

Metabase MCP가 켜져 있고 추이/Top-N처럼 차트가 유효한 질문이면 최종 답변에
`Metabase 카드` 또는 `Metabase 대시보드` 링크와 `Metabase 열기` 버튼이 붙을 수 있다.
단일 숫자 질문에는 불필요하게 카드를 만들지 않는 것이 정상이다.

## 5. 중단 버튼 확인

진행 메시지의 `중단하기` 버튼을 누르면 먼저 중단 요청 상태가 보여야 한다.

```text
분석 중단 요청됨
현재 실행 중인 작업을 중단하고 있습니다.
```

조금 뒤 최종 상태는 아래처럼 정리되어야 한다.

```text
분석 중단됨
분석을 멈췄습니다. 부분 결과는 게시하지 않았습니다.
```

중단된 요청은 성공 답변을 올리지 않는다. 필요하면 같은 thread에서 다시 요청한다.

## 6. 로컬 로그 확인

실행 기록은 기본적으로 `logs/analyst-bot/` 아래에 남는다.

```bash
find logs/analyst-bot -type f | sort | tail
```

주요 파일:

| 경로 | 의미 |
|---|---|
| `logs/analyst-bot/audit/*.jsonl` | 질문, 성공/실패/중단 상태, 사용량 요약 |
| `logs/analyst-bot/traces/*.jsonl` | Slack에 숨긴 원시 진행 trace |
| `logs/analyst-bot/feedback/*.jsonl` | 답변 버튼 피드백 |

dry run 바이트, `bq`/`dbt` 명령, Claude 턴 수, BigQuery GiB/USD/job 수는 여기서 확인한다.

## 7. Docker/headless canary

Docker로 넘길 때는 Slack canary 전에 아래 순서로 확인한다.

```bash
docker compose config --services | grep analyst-bot
docker compose build analyst-bot
docker compose run --rm analyst-bot --dry-run
docker compose run --rm analyst-bot --self-test
docker compose up -d analyst-bot
docker compose logs -f analyst-bot
```

Docker 기본값은 headless지만 `ANALYST_LOG_LEVEL=INFO`라서 `docker compose logs`에 진행 로그가 남아야 한다.
`--dry-run`에서 gcloud config 경로가 `/home/analyst/.config/gcloud`, state dir이 `/home/analyst/state`로 보이면 맞다.

Docker에서 Metabase MCP를 켜려면 `.env`에 아래를 둔다.

```bash
ANALYST_ENABLE_METABASE_MCP=1
ANALYST_METABASE_INTERNAL_URL=http://metabase:3000
ANALYST_METABASE_PUBLIC_URL=http://localhost:3001
METABASE_API_KEY=...
```

## 8. 비용/실행 상세 읽는 법

Slack 최종 답변에는 비용·스캔 상세를 표시하지 않는다. 완료 후 실제 사용량은
`logs/analyst-bot/audit/*.jsonl`에서 두 종류를 나눠서 본다.

- `Claude 환산 $...`: 구독 사용량을 API 단가처럼 환산한 값이다. 실제 청구액으로 더하지 않는다.
- `BigQuery ... GiB`: 실제 스캔 사용량이다. 이쪽이 비용 방어 대상이다.

쿼리당 기본 스캔 상한은 10 GiB다. 더 큰 분석이 필요하면 봇을 다시 시작할 때 `--max-scan-gib` 값을 명시적으로 올린다.

## 9. 문제가 생겼을 때 먼저 볼 것

| 증상 | 먼저 확인할 것 |
|---|---|
| Slack 답장이 없다 | 터미널에 `⚡️ Bolt app is running!`이 있는지 확인 |
| 채널에서는 답장 없고 DM은 됨 | 채널에서 봇을 멘션했는지 확인 |
| DM도 답장 없음 | 앱이 DM 이벤트를 구독하는지, 봇과 DM을 열었는지 확인 |
| 진행 메시지가 내부 로그처럼 보임 | `scripts/slack_analyst_bot.py` 최신 변경으로 실행 중인지 확인 |
| 중단 후 성공 답변이 올라옴 | 취소 처리 회귀. `tests/test_slack_analyst_bot.py`를 먼저 실행 |
| BigQuery 권한 오류 | `--dry-run`에서 `bq_identity`가 `bda-analyst-ro@...`인지 확인 |
| Metabase 카드가 안 만들어짐 | `ANALYST_ENABLE_METABASE_MCP=1`, `METABASE_URL`, `METABASE_API_KEY` 확인 |
