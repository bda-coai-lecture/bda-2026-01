from __future__ import annotations

import importlib.util
import json
import sys
import threading
import time
from pathlib import Path


def load_bot_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "slack_analyst_bot.py"
    spec = importlib.util.spec_from_file_location("slack_analyst_bot", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


bot = load_bot_module()

FAKE_XOXB = "xoxb-" + "abc-secret"
FAKE_XAPP = "xapp-" + "def-secret"
FAKE_MB_KEY = "mb_live_" + "secret_12345"
FAKE_ANTHROPIC_KEY = "sk-ant-" + "api03-secret-token-12345"
FAKE_PRIVATE_KEY_BLOCK = (
    "-----BEGIN "
    + "PRIVATE KEY-----\nabc\n-----END "
    + "PRIVATE KEY-----"
)


def test_session_id_for_is_deterministic_and_thread_scoped() -> None:
    first = bot.session_id_for("T1", "C1", "1700000000.000100")
    second = bot.session_id_for("T1", "C1", "1700000000.000100")
    other = bot.session_id_for("T1", "C1", "1700000000.000101")

    assert first == second
    assert first != other


def test_build_command_uses_create_or_resume_not_both() -> None:
    cfg = bot.Config()

    create_cmd = bot.build_command(cfg, "session-a", resume=False)
    resume_cmd = bot.build_command(cfg, "session-a", resume=True)

    assert "--session-id" in create_cmd
    assert "--resume" not in create_cmd
    assert "--resume" in resume_cmd
    assert "--session-id" not in resume_cmd


def test_append_prompt_requires_product_friendly_slack_answer() -> None:
    prompt = bot.APPEND_SYSTEM_PROMPT

    assert "helpful teammate in Slack" in prompt
    assert "활동 계정" in prompt
    assert "2-4 short sentences" in prompt
    assert "간단히" in prompt
    assert "technical audit note" in prompt
    assert '"event" -> "활동 기록"' in prompt


def test_child_env_forces_analyst_gcloud_and_strips_slack_tokens(monkeypatch) -> None:
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-alert")
    monkeypatch.setenv(bot.BOT_TOKEN_ENV, "xoxb-analyst")
    monkeypatch.setenv(bot.APP_TOKEN_ENV, "xapp-analyst")
    monkeypatch.setenv("CLOUDSDK_CONFIG", "/tmp/operator-gcloud")
    monkeypatch.setenv("BIGQUERY_MAXIMUM_BYTES_BILLED", str(999 * 1024**3))
    monkeypatch.setenv("METABASE_API_KEY", "mb-parent-secret")

    cfg = bot.Config(repo_dir="/repo", max_scan_bytes=10 * 1024**3)
    env = bot.child_env(cfg)

    assert env["CLOUDSDK_CONFIG"] == "/repo/secrets/gcloud-analyst"
    assert env["BIGQUERYRC"] == "/repo/secrets/gcloud-analyst/.bigqueryrc"
    assert env["BIGQUERY_MAXIMUM_BYTES_BILLED"] == str(10 * 1024**3)
    assert "SLACK_BOT_TOKEN" not in env
    assert bot.BOT_TOKEN_ENV not in env
    assert bot.APP_TOKEN_ENV not in env
    assert "METABASE_API_KEY" not in env


def test_child_env_allows_container_gcloud_config_override(monkeypatch) -> None:
    monkeypatch.setenv(bot.ANALYST_GCLOUD_CONFIG_ENV, "/home/analyst/.config/gcloud")

    env = bot.child_env(bot.Config(repo_dir="/app"))

    assert env["CLOUDSDK_CONFIG"] == "/home/analyst/.config/gcloud"
    assert env["BIGQUERYRC"] == "/home/analyst/.config/gcloud/.bigqueryrc"


def test_bq_usage_since_is_disabled_when_operator_key_is_not_mounted(tmp_path) -> None:
    usage = bot.bq_usage_since(
        bot.Config(repo_dir=str(tmp_path)),
        bot.datetime.now(bot.timezone.utc),
    )

    assert usage.jobs == 0
    assert usage.gib == 0
    assert "usage lookup disabled" in (usage.error or "")


def test_format_final_answer_removes_leaked_preamble_and_markdown_noise() -> None:
    raw = """**분석 완료.**
---
모드: 활동량
인풋
관측 grain: activity_date
세그먼트: 전체
dry run: Query successfully validated. This will process 4.4 GiB.
Claude 2턴 · BigQuery 1.50 GiB
검증: shard 마감, mart 커버리지, grain 유일성, raw 대조를 모두 통과했습니다.
재현: dbt/gharchive_metrics/analyses/20260802_daily_active_actor_latest_closed_day.sql
### 결론
**active actor**는 감소했습니다.

- 근거 1
- 근거 2

---
한계: 대조군이 없어 인과는 말할 수 없습니다.
"""

    formatted = bot.format_final_answer_for_slack(raw)

    assert formatted.startswith("*결론*")
    assert "분석 완료" not in formatted
    assert "모드:" not in formatted
    assert "관측 grain" not in formatted
    assert "세그먼트" not in formatted
    assert "dry run" not in formatted
    assert "Query successfully validated" not in formatted
    assert "BigQuery 1.50 GiB" not in formatted
    assert "Claude 2턴" not in formatted
    assert "검증:" not in formatted
    assert "재현:" not in formatted
    assert "20260802_daily_active_actor_latest_closed_day.sql" not in formatted
    assert "---" not in formatted
    assert "**" not in formatted
    assert "active actor" not in formatted
    assert "활동 계정" in formatted
    assert "• 근거 1" in formatted
    assert "한계:" in formatted


def test_format_final_answer_casualizes_schema_terms() -> None:
    raw = (
        "2026-08-01(UTC) active actor는 305,409명입니다. "
        "actor.id distinct count이고 actor당 이벤트는 13.3건입니다. "
        "event type별 raw/mart grain은 생략했습니다."
    )

    formatted = bot.format_final_answer_for_slack(raw)

    assert "active actor" not in formatted
    assert "actor.id" not in formatted
    assert "distinct count" not in formatted
    assert "actor당" not in formatted
    assert "이벤트" not in formatted
    assert "event type" not in formatted
    assert "raw" not in formatted
    assert "mart" not in formatted
    assert "grain" not in formatted
    assert "UTC" not in formatted
    assert "활동 계정은 305,409명" in formatted
    assert "계정 ID" in formatted
    assert "계정당 활동 기록" in formatted
    assert "활동 종류" in formatted
    assert "원천 데이터/집계 데이터" in formatted
    assert "집계 단위" in formatted
    assert "데이터 기준" in formatted


def test_format_final_answer_preserves_metabase_link_but_hides_mcp() -> None:
    raw = """결론: 금요일 지표는 전주 대비 감소했습니다.

Metabase MCP로 만든 카드: http://localhost:3001/question/123

한계: 이벤트 actor 기준이라 실제 사람 수와 다를 수 있습니다.
"""

    formatted = bot.format_final_answer_for_slack(raw)

    assert "MCP" not in formatted
    assert "http://localhost:3001/question/123" in formatted
    assert bot.extract_metabase_url(formatted) == "http://localhost:3001/question/123"


def test_metabase_mcp_config_is_opt_in_and_secret_stays_out_of_argv(tmp_path) -> None:
    default_cmd = bot.build_command(bot.Config(state_dir=str(tmp_path)), "session-a", resume=False)

    assert "--mcp-config" not in default_cmd
    assert "--strict-mcp-config" not in default_cmd
    assert "mcp__metabase__create_card" not in default_cmd

    cfg = bot.Config(
        state_dir=str(tmp_path),
        enable_metabase_mcp=True,
        metabase_url="http://localhost:3001/",
        metabase_public_url="http://localhost:3999/",
        metabase_api_key="mb-secret-key",
        append_system_prompt="base prompt\n",
    )
    cmd = bot.build_command(cfg, "session-a", resume=False)
    rendered = " ".join(cmd)
    config_path = Path(cmd[cmd.index("--mcp-config") + 1])
    payload = json.loads(config_path.read_text(encoding="utf-8"))

    assert "mcp__metabase__create_card" in cmd
    assert "mcp__metabase__add_card_to_dashboard" in cmd
    assert "--strict-mcp-config" in cmd
    assert "mb-secret-key" not in rendered
    assert payload["mcpServers"]["metabase"]["command"] == "npx"
    assert payload["mcpServers"]["metabase"]["env"]["METABASE_URL"] == "http://localhost:3001"
    assert payload["mcpServers"]["metabase"]["env"]["METABASE_API_KEY"] == "mb-secret-key"
    assert config_path.name == bot.METABASE_RUNTIME_CONFIG_FILENAME
    assert "Metabase is available through MCP" in cmd[cmd.index("--append-system-prompt") + 1]
    assert "http://localhost:3999/question/{id}" in cmd[cmd.index("--append-system-prompt") + 1]

    child = bot.child_env(cfg)
    assert "METABASE_API_KEY" not in child


def test_rewrite_metabase_links_for_slack_uses_public_url() -> None:
    cfg = bot.Config(
        enable_metabase_mcp=True,
        metabase_url="http://metabase:3000",
        metabase_public_url="http://localhost:3001",
        metabase_api_key="mb-secret-key",
    )

    rewritten = bot.rewrite_metabase_links_for_slack(
        "Metabase 카드: http://metabase:3000/question/123",
        cfg,
    )

    assert rewritten == "Metabase 카드: http://localhost:3001/question/123"


def test_format_final_answer_redacts_secrets() -> None:
    raw = f"""### 결론
토큰 {FAKE_XOXB} 과 {FAKE_XAPP} 은 노출되면 안 됩니다.
METABASE_API_KEY={FAKE_MB_KEY}
ANTHROPIC_API_KEY={FAKE_ANTHROPIC_KEY}

{FAKE_PRIVATE_KEY_BLOCK}
"""

    formatted = bot.format_final_answer_for_slack(raw)

    assert FAKE_XOXB not in formatted
    assert FAKE_XAPP not in formatted
    assert FAKE_MB_KEY not in formatted
    assert FAKE_ANTHROPIC_KEY not in formatted
    assert "PRIVATE KEY" not in formatted
    assert "[REDACTED]" in formatted


def test_format_final_answer_keeps_metabase_failure_without_mcp_noise() -> None:
    raw = """결론: 감소했습니다.

Metabase MCP tool error 403 permission denied.

한계: 이벤트 actor 기준입니다.
"""

    formatted = bot.format_final_answer_for_slack(raw)

    assert "MCP" not in formatted
    assert "tool" not in formatted
    assert "403" not in formatted
    assert "Metabase 카드는 생성하지 못했습니다" in formatted
    assert "한계:" in formatted


def test_format_final_answer_caps_long_wall_of_text() -> None:
    raw = "결론: 핵심입니다.\n\n" + "\n\n".join(
        f"세부 {idx}: " + ("가" * 300) for idx in range(30)
    ) + "\n\n한계: 계측 공백 때문에 원인 단정은 불가합니다."

    formatted = bot.format_final_answer_for_slack(raw, limit=900)

    assert len(formatted) <= 900
    assert formatted.startswith("결론: 핵심입니다.")
    assert "한계:" in formatted


def test_progress_elapsed_context_matches_hamji_style() -> None:
    assert bot.progress_elapsed_context(12_300, 900_000).startswith("12초 경과 / 최대 15분")
    assert bot.progress_elapsed_context(61_000, None).startswith("1분 1초 경과")


def test_section_blocks_expand_to_avoid_slack_show_more() -> None:
    direct = bot.section_block("긴 답변")
    feedback = bot.feedback_blocks("최종 답변", "audit-1")[0]
    _, cancelled = bot.cancelled_payload(10_000, "U2")

    sections = [direct, feedback, *[block for block in cancelled if block.get("type") == "section"]]
    assert sections
    assert all(section.get("expand") is True for section in sections)


def test_feedback_recorded_blocks_replace_buttons_with_visible_state() -> None:
    blocks = bot.feedback_blocks("최종 답변", "audit-1", "12초 · BigQuery 0.01 GiB")

    updated = bot.feedback_recorded_blocks(blocks, "helpful", "U2")

    rendered = str(updated)
    assert not any(block.get("type") == "actions" for block in updated)
    assert "피드백 기록됨: 도움됨" in rendered
    assert "<@U2>" in rendered


def test_feedback_blocks_add_metabase_button_when_url_present() -> None:
    blocks = bot.feedback_blocks(
        "최종 답변\nMetabase 카드: http://localhost:3001/question/123",
        "audit-1",
        "12초 · 상세 실행 내역은 로그에 저장됨",
        metabase_url="http://localhost:3001/question/123",
    )

    rendered = str(blocks)
    assert "Metabase 열기" in rendered
    assert "http://localhost:3001/question/123" in rendered


def test_clean_text_removes_only_bot_mention_when_bot_id_is_known() -> None:
    cleaned = bot.clean_text("<@UBOT> <@U123>가 물어본 active actor 봐줘", "UBOT")

    assert cleaned == "<@U123>가 물어본 active actor 봐줘"


def test_session_registry_serializes_same_thread_fifo() -> None:
    registry = bot.SessionRegistry(max_concurrent_turns=2)
    order: list[str] = []
    first_entered = threading.Event()

    def first() -> None:
        with registry.claim("C", "T"):
            order.append("first-start")
            first_entered.set()
            time.sleep(0.05)
            order.append("first-end")

    def second() -> None:
        first_entered.wait(timeout=1)
        with registry.claim("C", "T"):
            order.append("second-start")
            order.append("second-end")

    t1 = threading.Thread(target=first)
    t2 = threading.Thread(target=second)
    t1.start()
    t2.start()
    t1.join(timeout=2)
    t2.join(timeout=2)

    assert order == ["first-start", "first-end", "second-start", "second-end"]


def test_active_turn_registry_requests_cancel() -> None:
    registry = bot.ActiveTurnRegistry()
    cancel_event = threading.Event()
    turn = bot.ActiveTurn(
        cancel_event=cancel_event,
        channel="C",
        thread_ts="T",
        pending_ts="P",
        requester="U1",
        started_at=bot.datetime.now(bot.timezone.utc),
    )
    registry.register("run-1", turn)

    found = registry.request_cancel("run-1", "U2")

    assert found is turn
    assert cancel_event.is_set()
    assert turn.cancel_requested_by == "U2"
    registry.finish("run-1")
    assert registry.request_cancel("run-1", "U2") is None


def test_active_turn_registry_requests_shutdown_cancel_for_all() -> None:
    registry = bot.ActiveTurnRegistry()
    first = bot.ActiveTurn(
        cancel_event=threading.Event(),
        channel="C",
        thread_ts="T1",
        pending_ts="P1",
        requester="U1",
        started_at=bot.datetime.now(bot.timezone.utc),
    )
    second = bot.ActiveTurn(
        cancel_event=threading.Event(),
        channel="C",
        thread_ts="T2",
        pending_ts="P2",
        requester="U2",
        started_at=bot.datetime.now(bot.timezone.utc),
    )
    registry.register("run-1", first)
    registry.register("run-2", second)

    turns = registry.request_cancel_all("system")

    assert turns == [first, second]
    assert first.cancel_event.is_set()
    assert second.cancel_event.is_set()
    assert first.cancel_requested_by == "system"
    assert second.cancel_requested_by == "system"


def test_cancellation_payloads_are_user_facing() -> None:
    requested_text, requested_blocks = bot.cancellation_requested_payload(65_000, "U2")
    cancelled_text, cancelled_blocks = bot.cancelled_payload(
        70_000,
        "U2",
        bot.ScanUsage(gib=0.25, usd=0.01, jobs=1),
    )

    rendered = str(requested_blocks + cancelled_blocks)
    assert "분석 중단 요청됨" in requested_text
    assert "분석 중단됨" in cancelled_text
    assert "<@U2>" in rendered
    assert "BigQuery 0.25 GiB" not in rendered
    assert "세부 내역은 실행 로그" in rendered
    assert "Claude 0턴" not in rendered
    assert "session=" not in rendered
    assert "tool " not in rendered


def test_slack_trace_cancel_state_is_not_overwritten_by_late_trace() -> None:
    class FakeSlackClient:
        def __init__(self) -> None:
            self.posts: list[dict] = []
            self.updates: list[dict] = []

        def chat_postMessage(self, **kwargs):
            ts = "1700000003.000001"
            self.posts.append({**kwargs, "ts": ts})
            return {"ts": ts}

        def chat_update(self, **kwargs):
            self.updates.append(kwargs)
            return {"ok": True}

    client = FakeSlackClient()
    trace = bot.SlackTrace(client, "C", "T", ":bar_chart: *분석 중*", cancel_action_value="run-1")
    text, blocks = bot.cancellation_requested_payload(12_000, "U2")

    trace.replace_and_stop_progress(text, blocks=blocks)
    trace.add("tool Bash: bq query --dry_run")

    assert "분석 중단 요청됨" in str(client.updates[-1]["blocks"])
    assert "중단하기" not in str(client.updates[-1]["blocks"])
    assert "BigQuery 실행 전" not in str(client.updates[-1]["blocks"])


def test_slack_progress_hides_reproduction_sql_language() -> None:
    assert bot.slack_progress_line("tool Write: dbt/gharchive_metrics/analyses/foo.sql") == "계산 근거를 정리하고 있습니다."
    assert "재현" not in bot.slack_progress_line("tool Edit: dbt/gharchive_metrics/analyses/foo.sql")


def test_manifest_enables_interactivity_for_buttons() -> None:
    manifest = (Path(__file__).resolve().parents[1] / "config" / "slack_analyst_app_manifest.yaml").read_text(
        encoding="utf-8"
    )

    assert "interactivity:" in manifest
    assert "is_enabled: true" in manifest


def test_handle_turn_posts_sanitized_answer_single_footer_and_audit(monkeypatch, tmp_path) -> None:
    class FakeSlackClient:
        def __init__(self) -> None:
            self.posts: list[dict] = []
            self.updates: list[dict] = []

        def chat_postMessage(self, **kwargs):
            ts = f"1700000000.{len(self.posts) + 1:06d}"
            self.posts.append({**kwargs, "ts": ts})
            return {"ts": ts}

        def chat_update(self, **kwargs):
            self.updates.append(kwargs)
            return {"ok": True}

    def fake_run_turn(cfg, session_id, prompt, *, resume, cancel_event, on_trace, on_text):
        assert not resume
        assert cancel_event is not None
        assert not cancel_event.is_set()
        on_trace("tool Bash: bq query --dry_run")
        return bot.TurnResult(
            ok=True,
            session_id=session_id,
            final_text=(
                "**분석 완료.**\n---\n### 결론\n**감소했습니다.**\n\n"
                f"운영 메모: {FAKE_XOXB} 값은 숨겨야 합니다.\n\n"
                f"{FAKE_PRIVATE_KEY_BLOCK}\n\n"
                "한계: 대조군이 없어 인과는 말할 수 없습니다."
            ),
            num_turns=2,
            duration_ms=1200,
            total_cost_usd=0.03,
            trace=["tool Bash: bq query --dry_run"],
        )

    monkeypatch.setattr(bot, "run_turn", fake_run_turn)
    monkeypatch.setattr(bot, "bq_usage_since", lambda cfg, since: bot.ScanUsage(gib=1.5, usd=0.01, jobs=2))
    monkeypatch.setattr(bot.time, "sleep", lambda _: None)

    client = FakeSlackClient()
    bot.handle_turn(
        bot.Config(repo_dir=str(tmp_path), state_dir=str(tmp_path / "state")),
        bot.SessionRegistry(),
        bot.ActiveTurnRegistry(),
        bot.BotAuditLogger(str(tmp_path / "state")),
        client,
        team="T",
        channel="C",
        thread_ts="1700000000.000001",
        user="U",
        question="지난 7일 active actor 변화",
    )

    assert len(client.posts) == 1  # placeholder only; final answer replaces it via chat.update
    answer_updates = [update for update in client.updates if update.get("blocks") and "도움됨" in str(update["blocks"])]
    assert len(answer_updates) == 1
    assert answer_updates[0]["text"].startswith("*결론*")
    assert "분석 완료" not in answer_updates[0]["text"]
    assert "---" not in answer_updates[0]["text"]
    assert FAKE_XOXB not in str(answer_updates[0])
    assert "PRIVATE KEY" not in str(answer_updates[0])
    assert "[REDACTED]" in str(answer_updates[0])
    assert not any(":receipt:" in post["text"] for post in client.posts)
    assert "BigQuery 1.50 GiB" not in str(answer_updates[0]["blocks"])
    assert "Claude 2턴" not in str(answer_updates[0]["blocks"])
    assert "상세 실행 내역은 로그에 저장됨" in str(answer_updates[0]["blocks"])
    assert all("질문을 정리하고" not in update["text"] for update in client.updates)
    assert all("tool " not in update["text"] for update in client.updates)
    progress_messages = [str(post.get("blocks", "")) for post in client.posts] + [
        str(update.get("blocks", "")) for update in client.updates
    ]
    assert any("경과 / 최대 15분" in message for message in progress_messages)

    audit_files = list((tmp_path / "state" / "audit").glob("*.jsonl"))
    assert len(audit_files) == 1
    audit_text = audit_files[0].read_text(encoding="utf-8")
    assert '"status": "success"' in audit_text
    assert FAKE_XOXB not in audit_text
    assert "PRIVATE KEY" not in audit_text


def test_handle_turn_hides_raw_error_from_slack(monkeypatch, tmp_path) -> None:
    class FakeSlackClient:
        def __init__(self) -> None:
            self.posts: list[dict] = []
            self.updates: list[dict] = []

        def chat_postMessage(self, **kwargs):
            ts = f"1700000001.{len(self.posts) + 1:06d}"
            self.posts.append({**kwargs, "ts": ts})
            return {"ts": ts}

        def chat_update(self, **kwargs):
            self.updates.append(kwargs)
            return {"ok": True}

    def fake_run_turn(cfg, session_id, prompt, *, resume, cancel_event, on_trace, on_text):
        return bot.TurnResult(
            ok=False,
            session_id=session_id,
            error=(
                "claude 종료 코드 1\nTraceback secret raw stderr "
                "bytesBilledLimitExceeded "
                + "xoxb-"
                + "should-redact"
            ),
            trace=["result[error] raw stderr"],
        )

    monkeypatch.setattr(bot, "run_turn", fake_run_turn)
    monkeypatch.setattr(bot, "bq_usage_since", lambda cfg, since: bot.ScanUsage())
    monkeypatch.setattr(bot.time, "sleep", lambda _: None)

    client = FakeSlackClient()
    bot.handle_turn(
        bot.Config(repo_dir=str(tmp_path), state_dir=str(tmp_path / "state")),
        bot.SessionRegistry(),
        bot.ActiveTurnRegistry(),
        bot.BotAuditLogger(str(tmp_path / "state")),
        client,
        team="T",
        channel="C",
        thread_ts="1700000001.000001",
        user="U",
        question="큰 raw 조회",
    )

    slack_text = "\n".join(update["text"] for update in client.updates) + "\n".join(
        post["text"] for post in client.posts
    )
    assert "Traceback" not in slack_text
    assert "xoxb-" + "should-redact" not in slack_text
    assert "실행 상한" in slack_text
    assert "예상 BigQuery 스캔량" not in slack_text

    audit_files = list((tmp_path / "state" / "audit").glob("*.jsonl"))
    assert len(audit_files) == 1
    audit_text = audit_files[0].read_text(encoding="utf-8")
    assert "[REDACTED]" in audit_text
    assert "bytesBilledLimitExceeded" in audit_text


def test_handle_turn_records_delivery_error_when_final_answer_never_posts(monkeypatch, tmp_path) -> None:
    class FailingFinalSlackClient:
        def __init__(self) -> None:
            self.posts: list[dict] = []
            self.updates: list[dict] = []

        def chat_postMessage(self, **kwargs):
            if not self.posts:
                ts = "1700000002.000001"
                self.posts.append({**kwargs, "ts": ts})
                return {"ts": ts}
            raise RuntimeError("post failed")

        def chat_update(self, **kwargs):
            self.updates.append(kwargs)
            raise RuntimeError("update failed")

    def fake_run_turn(cfg, session_id, prompt, *, resume, cancel_event, on_trace, on_text):
        on_trace("tool Bash: bq query --dry_run")
        return bot.TurnResult(
            ok=True,
            session_id=session_id,
            final_text="### 결론\n전달 실패 테스트입니다.",
            num_turns=1,
            duration_ms=1000,
            trace=["tool Bash: bq query --dry_run"],
        )

    monkeypatch.setattr(bot, "run_turn", fake_run_turn)
    monkeypatch.setattr(bot, "bq_usage_since", lambda cfg, since: bot.ScanUsage())
    monkeypatch.setattr(bot.time, "sleep", lambda _: None)

    client = FailingFinalSlackClient()
    bot.handle_turn(
        bot.Config(repo_dir=str(tmp_path), state_dir=str(tmp_path / "state")),
        bot.SessionRegistry(),
        bot.ActiveTurnRegistry(),
        bot.BotAuditLogger(str(tmp_path / "state")),
        client,
        team="T",
        channel="C",
        thread_ts="1700000002.000001",
        user="U",
        question="전달 실패",
    )

    audit_files = list((tmp_path / "state" / "audit").glob("*.jsonl"))
    assert len(audit_files) == 1
    audit_text = audit_files[0].read_text(encoding="utf-8")
    assert '"status": "delivery_error"' in audit_text
    assert '"status": "success"' not in audit_text
