#!/usr/bin/env python
"""Append DAU/MAU decline-diagnosis cards to the 'GitHub Core Metrics' dashboard.

Idempotent: cards are namespaced with a '[진단]' prefix; a re-run deletes the
prior '[진단]' cards + their dashcards and recreates them, leaving all other
dashboard content untouched.
"""
from __future__ import annotations
import requests

BASE = "http://localhost:3001"
EMAIL, PW = "bda@local.dev", "bda-local-2026"
DB = 2                 # BigQuery bda-coai.mart
COLLECTION = 5
DASHBOARD_ID = 4
PREFIX = "[진단]"
MARK = "decline-diagnosis"   # marker inside text card

s = requests.Session()
tok = s.post(f"{BASE}/api/session", json={"username": EMAIL, "password": PW}).json()["id"]
s.headers["X-Metabase-Session"] = tok


def line_viz(dims, metrics, right=None):
    v = {"graph.dimensions": dims, "graph.metrics": metrics}
    if right:
        v["series_settings"] = {m: {"axis": "right"} for m in right}
    return v


# ---- card definitions (name, sql, display, viz) ----
CARDS = [
    (
        f"{PREFIX} 이벤트 액션 수 인덱스 (2025-05=100)",
        "SELECT month, push_idx AS Push_idx, star_idx AS Star_idx, fork_idx AS Fork_idx, "
        "pr_idx AS PR_idx, issue_idx AS Issue_idx "
        "FROM `bda-coai.mart.diag_event_type_monthly` ORDER BY month",
        "line",
        line_viz(["month"], ["Push_idx", "Star_idx", "Fork_idx", "PR_idx", "Issue_idx"]),
    ),
    (
        f"{PREFIX} 소셜 이벤트 액션 수 (월, 원계열)",
        "SELECT month, watch AS Star_Watch, fork AS Fork, pr AS PullRequest, "
        "issue AS Issue, issue_comment AS IssueComment "
        "FROM `bda-coai.mart.diag_event_type_monthly` ORDER BY month",
        "line",
        line_viz(["month"], ["Star_Watch", "Fork", "PullRequest", "Issue", "IssueComment"]),
    ),
    (
        f"{PREFIX} 활성 유저 수: Push 유저 vs 소셜-온리 유저 (월)",
        "SELECT month, push_users AS Push_users, social_only_users AS Social_only_users "
        "FROM `bda-coai.mart.diag_user_population_monthly` ORDER BY month",
        "line",
        line_viz(["month"], ["Push_users", "Social_only_users"], right=["Social_only_users"]),
    ),
    (
        f"{PREFIX} 소셜 이벤트 커버리지 % (주) — 워치독",
        "SELECT week, social_event_pct AS Social_event_pct "
        "FROM `bda-coai.mart.diag_social_coverage_weekly` ORDER BY week",
        "line",
        line_viz(["week"], ["Social_event_pct"]),
    ),
    (
        f"{PREFIX} 활성화 발산: DAU vs 인당 이벤트 (월)",
        "SELECT month, dau_avg AS DAU_avg, events_per_user AS Events_per_user "
        "FROM `bda-coai.mart.diag_activation_monthly` ORDER BY month",
        "line",
        line_viz(["month"], ["DAU_avg", "Events_per_user"], right=["Events_per_user"]),
    ),
    (
        f"{PREFIX} 대표 레포 스타(Watch) 궤적 (월)",
        "SELECT month, repo, stars "
        "FROM `bda-coai.mart.diag_hero_repo_stars_monthly` ORDER BY month, repo",
        "line",
        line_viz(["month", "repo"], ["stars"]),
    ),
]

TEXT = (
    "## 📉 DAU/MAU 감소 진단 — 이벤트 타입 붕괴\n"
    "핵심: 유저 이탈이 아니라 **원천(GH Archive)에서 Push 외 소셜 이벤트(Star/Fork/PR/Issue)가 "
    "2026-05부터 붕괴**. 라이트 유저·인기 레포가 집계에서 사라진 **데이터 커버리지 문제**. "
    f"(marker: {MARK})"
)

# ---- 1) remove prior '[진단]' cards + text card from dashboard, then delete cards ----
dash = s.get(f"{BASE}/api/dashboard/{DASHBOARD_ID}").json()
existing = dash.get("dashcards") or []
kept, to_delete_card_ids = [], []
for dc in existing:
    card = dc.get("card") or {}
    name = card.get("name") or ""
    txt = (dc.get("visualization_settings") or {}).get("text", "")
    if name.startswith(PREFIX) or (MARK in txt):
        if dc.get("card_id"):
            to_delete_card_ids.append(dc["card_id"])
        continue
    kept.append(dc)

# find max row among kept to append below
next_row = max([(dc["row"] + dc["size_y"]) for dc in kept], default=0)

# ---- 2) create new cards ----
new_dashcards = []
neg = -1
# section header (text card), full width
new_dashcards.append({
    "id": neg, "card_id": None, "row": next_row, "col": 0, "size_x": 24, "size_y": 2,
    "series": [], "parameter_mappings": [],
    "visualization_settings": {"text": TEXT, "virtual_card": {
        "name": None, "display": "text", "dataset_query": {}, "visualization_settings": {}}},
})
neg -= 1
row = next_row + 2
for idx, (name, sql, display, viz) in enumerate(CARDS):
    card = s.post(f"{BASE}/api/card", json={
        "name": name,
        "dataset_query": {"database": DB, "type": "native", "native": {"query": sql.strip()}},
        "display": display,
        "visualization_settings": viz,
        "collection_id": COLLECTION,
    }).json()
    cid = card["id"]
    col = 0 if idx % 2 == 0 else 12
    new_dashcards.append({
        "id": neg, "card_id": cid, "row": row, "col": col, "size_x": 12, "size_y": 6,
        "series": [], "parameter_mappings": [], "visualization_settings": {},
    })
    neg -= 1
    if idx % 2 == 1:
        row += 6
    print(f"created card {cid}: {name}")

# ---- 3) PUT dashboard with kept + new dashcards ----
payload_dashcards = kept + new_dashcards
resp = s.put(f"{BASE}/api/dashboard/{DASHBOARD_ID}",
             json={"dashcards": payload_dashcards, "tabs": dash.get("tabs") or []})
if resp.status_code >= 400:
    raise SystemExit(f"dashboard PUT failed: {resp.status_code} {resp.text[:500]}")

# ---- 4) cleanup old cards ----
for cid in to_delete_card_ids:
    s.delete(f"{BASE}/api/card/{cid}")

print(f"\nDONE. appended {len(CARDS)} cards + header to dashboard {DASHBOARD_ID} "
      f"(removed {len(to_delete_card_ids)} old). URL: {BASE}/dashboard/{DASHBOARD_ID}")
