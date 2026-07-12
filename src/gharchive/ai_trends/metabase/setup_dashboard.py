#!/usr/bin/env python3
"""GitHub AI 개발추세 Metabase 대시보드 구성 (bda-2 인스턴스 재사용).
기존 BigQuery 커넥션(db id=2)에 native SQL 카드로 붙임 — BDA 커넥션/컬렉션 무수정.
멱등: 전용 컬렉션 안의 카드/대시보드를 매번 재생성.
실행: uv run --with requests python metabase/setup_dashboard.py
"""
from __future__ import annotations
import os, requests

URL = os.environ.get("METABASE_URL", "http://localhost:3001")
EMAIL = os.environ.get("METABASE_EMAIL", "bda@local.dev")
PASSWORD = os.environ.get("METABASE_PASSWORD", "bda-local-2026")
DB_ID = int(os.environ.get("METABASE_BQ_DB_ID", "2"))   # BigQuery bda-coai
V = "`bda-coai.github_ai_analysis.ai_pr_metrics_monthly`"
COLLECTION = "GitHub AI 개발추세"
DASHBOARD = "GitHub AI 개발추세 (자율 에이전트 PR)"

s = requests.Session()
def api(method, path, **kw):
    r = s.request(method, f"{URL}{path}", timeout=60, **kw)
    if r.status_code >= 400:
        raise RuntimeError(f"{method} {path} -> {r.status_code} {r.text[:300]}")
    return r.json() if r.text else {}

# ── login
tok = api("POST", "/api/session", json={"username": EMAIL, "password": PASSWORD})["id"]
s.headers.update({"X-Metabase-Session": tok})
print(f"logged in ({tok[:8]}...)")

# ── collection: 전용 컬렉션 시도, 실패(H2 permissions 버그) 시 루트로 폴백
cols = api("GET", "/api/collection")
col = next((c for c in cols if c.get("name") == COLLECTION), None)
if col:
    col_id = col["id"]
else:
    try:
        col_id = api("POST", "/api/collection",
                     json={"name": COLLECTION, "description": "GH Archive 기반 자율 AI 에이전트 PR 장기추세"})["id"]
    except RuntimeError as e:
        print(f"collection 생성 실패 → 루트 사용 ({str(e)[:60]})")
        col_id = None
print(f"collection id={col_id}")

# ── 최신 완전월 (제목/설명에 표기)
latest = api("POST", "/api/dataset", json={
    "database": DB_ID, "type": "native",
    "native": {"query": f"SELECT MAX(ym) FROM {V} WHERE data_quality='full'"}})["data"]["rows"][0][0]
print(f"latest full month: {latest}")

# ── cards
def card(name, sql, display, viz=None, desc=None):
    return {"name": name, "description": desc,
            "dataset_query": {"database": DB_ID, "type": "native", "native": {"query": sql}},
            "display": display, "visualization_settings": viz or {}, "collection_id": col_id}

FULL = f"FROM {V} WHERE data_quality='full' ORDER BY ym"
cards = [
  card("GH-AI: 전역 AI PR 비중 추세 (합산 vs 봇전용)",
       f"SELECT ym, ai_pct AS ai_pct_combined, ai_pct_bot_only AS ai_pct_bot {FULL}",
       "line", {"graph.dimensions": ["ym"], "graph.metrics": ["ai_pct_combined", "ai_pct_bot"],
                "series_settings": {"ai_pct_combined": {"title": "합산(실제)"},
                                    "ai_pct_bot": {"title": "봇계정만(하한)"}}},
       desc="합산=봇 계정+브랜치명(codex/claude/cursor…)으로 탐지한 실제 AI PR 비중. 봇계정만=Copilot·Devin·Jules 등 봇 계정으로 연 PR만 센 하한선(Codex·Claude 클라우드는 사람 로그인이라 누락됨). 두 선의 격차 = 브랜치명으로 복원한 숨은 AI."),
  card("GH-AI: 툴별 점유 추세",
       f"SELECT ym, copilot, codex, claude, cursor, devin, jules {FULL}",
       "line", {"graph.dimensions": ["ym"],
               "graph.metrics": ["copilot","codex","claude","cursor","devin","jules"]},
       desc="각 AI 툴이 연 PR 수(봇계정+브랜치명 합산). 표본 매월 9·10·11일."),
  card("GH-AI: 도입 레포 내 AI 비중 추세 (희석 제거)",
       f"SELECT ym, active_repo_ai_pct_botonly AS adopter_ai_pct {FULL}",
       "line", {"graph.dimensions": ["ym"], "graph.metrics": ["adopter_ai_pct"]},
       desc="AI PR이 1건 이상 있는 '도입 레포' 내부에서, 그 레포들의 전체 PR 중 AI 비중. 전역 비중(전세계 레포로 희석됨)과 달리 실제 도입한 곳의 밀도. 현재 봇계정 기준 보수적 하한."),
  card("GH-AI: 최신 전역 AI PR %",
       f"SELECT ai_pct FROM {V} WHERE data_quality='full' ORDER BY ym DESC LIMIT 1", "scalar",
       desc=f"가장 최근 데이터 완전월({latest})의 전역 AI PR 비중."),
  card("GH-AI: 최신 AI 도입 레포 수",
       f"SELECT active_repos FROM {V} WHERE data_quality='full' ORDER BY ym DESC LIMIT 1", "scalar",
       desc=f"가장 최근 데이터 완전월({latest})의 AI 도입 레포 수."),
  card(f"GH-AI: 툴 점유 3파전 ({latest} 기준)",
       f"""SELECT tool, prs FROM (
  SELECT 'Copilot' tool, copilot prs, ym FROM {V}
  UNION ALL SELECT 'Codex', codex, ym FROM {V}
  UNION ALL SELECT 'Claude', claude, ym FROM {V}
  UNION ALL SELECT 'Cursor', cursor, ym FROM {V}
  UNION ALL SELECT 'Devin', devin, ym FROM {V}
  UNION ALL SELECT 'Jules', jules, ym FROM {V})
WHERE ym=(SELECT MAX(ym) FROM {V} WHERE data_quality='full') ORDER BY prs DESC""",
       "row", {"graph.dimensions": ["tool"], "graph.metrics": ["prs"]},
       desc=f"가장 최근 데이터 완전월({latest})의 툴별 PR 수. 데이터는 자동 갱신, 제목의 월은 이 스크립트 재실행 시 갱신."),
  card("GH-AI: AI 브랜치 생성 추세 (PR결손 보강 · CreateEvent)",
       "SELECT ym, ai_branch_pct FROM `bda-coai.github_ai_analysis.ai_branch_monthly` ORDER BY ym",
       "line", {"graph.dimensions": ["ym"], "graph.metrics": ["ai_branch_pct"]},
       desc="CreateEvent(브랜치 생성) 기반 AI 활동 지표. PullRequestEvent가 언더수집된 2026-06+에도 정상 추적됨(Push·Create 스트림은 무결). codex/claude/cursor/devin 등 AI 브랜치 프리픽스 비율. 단 Copilot은 이 방식으로 브랜치를 안 만들어 누락 → PR% 지표와 스케일 다름(트렌드 비교·연속성 보강용)."),
]

# idempotent: 루트 내 GH-AI 카드 전부 삭제 후 재생성 (리네임된 카드 고아 방지)
for c in api("GET", "/api/card"):
    if c.get("collection_id") == col_id and str(c.get("name","")).startswith("GH-AI:"):
        api("DELETE", f"/api/card/{c['id']}")
card_ids = [api("POST", "/api/card", json=c)["id"] for c in cards]
print(f"created {len(card_ids)} cards: {card_ids}")

# ── dashboard (idempotent)
dash = next((d for d in api("GET", "/api/dashboard")
             if d.get("name") == DASHBOARD and not d.get("archived")), None)
if dash:
    dash_id = dash["id"]
    api("PUT", f"/api/dashboard/{dash_id}", json={"collection_id": col_id})
else:
    dash_id = api("POST", "/api/dashboard", json={
        "name": DASHBOARD, "collection_id": col_id,
        "description": "GH Archive 전수 PR에서 자율 AI 에이전트(봇계정+브랜치명) 탐지. 표본 매월 9·10·11일. combined=실제, bot_only=하한."})["id"]
print(f"dashboard id={dash_id}")

# layout: (row,col,size_x,size_y) — 18-wide grid
layout = [(3,0,18,7), (10,0,18,8), (18,0,9,7), (0,0,9,3), (0,9,9,3), (18,9,9,7), (25,0,18,7)]
dashcards = [{"id": -(i+1), "card_id": cid, "row": r, "col": c, "size_x": sx, "size_y": sy,
              "parameter_mappings": [], "series": [], "inline_parameters": []}
             for i, (cid, (r, c, sx, sy)) in enumerate(zip(card_ids, layout))]
api("PUT", f"/api/dashboard/{dash_id}", json={"dashcards": dashcards, "tabs": []})
print(f"\n✅ 완료: {URL}/dashboard/{dash_id}")
