#!/usr/bin/env python3
import json, urllib.request, urllib.error

MB = "http://localhost:3001"
DB = 3
DASH = 7
USER, PW = "bda@local.dev", "bda-local-2026"

def api(method, path, token=None, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(MB + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token: req.add_header("X-Metabase-Session", token)
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        print(f"  ! {method} {path} -> {e.code}: {e.read().decode()[:400]}")
        raise

token = api("POST", "/api/session", body={"username": USER, "password": PW})["id"]
print("session ok")

# ── 신규 카드 2개 (유저/기기 기준) ──────────────────────────────
new_cards = [
    ("찰나 · 정리 퍼널 (유저 기준)", "funnel",
     """with d as (
          select device_id,
            bool_or(event_name='app_opened') opened,
            bool_or(event_name='screen_viewed' and properties->>'screen_name'='정리') home,
            bool_or(event_name='screen_viewed' and properties->>'screen_name'='Import') imp,
            bool_or(event_name='screen_viewed' and properties->>'screen_name'='SwipeSelect') swipe,
            bool_or(event_name='cleanup_session_completed') done
          from events group by device_id)
        select step, 기기 from (values
          (1,'앱 실행',(select count(*) from d where opened)),
          (2,'홈(정리)',(select count(*) from d where home)),
          (3,'사진 가져오기',(select count(*) from d where imp)),
          (4,'스와이프 선택',(select count(*) from d where swipe)),
          (5,'정리 완료',(select count(*) from d where done))
        ) t(ord, step, 기기) order by ord""",
     {"funnel.dimension": "step", "funnel.metric": "기기"}),

    ("찰나 · 화면별 도달 기기", "row",
     "select properties->>'screen_name' as 화면, count(distinct device_id) as 기기 from events where event_name='screen_viewed' group by 1 order by 2 desc",
     {"graph.dimensions": ["화면"], "graph.metrics": ["기기"]}),
]
new_id = {}
for name, display, sql, viz in new_cards:
    res = api("POST", "/api/card", token, {
        "name": name,
        "dataset_query": {"database": DB, "type": "native", "native": {"query": sql}},
        "display": display, "visualization_settings": viz})
    new_id[name] = res["id"]
    print(f"  + card {res['id']}  {name}")

FUNNEL_USER = new_id["찰나 · 정리 퍼널 (유저 기준)"]
SCREEN_USER = new_id["찰나 · 화면별 도달 기기"]

# ── 탭 정의 ────────────────────────────────────────────────
TAB_USER, TAB_SESS = -100, -200
tabs = [
    {"id": TAB_USER, "name": "유저 (기기 기준)"},
    {"id": TAB_SESS, "name": "세션 · 이벤트"},
]

# ── 기존 카드 id (build 스크립트에서 생성된 것) ────────────────
C = {"events":951,"devices":952,"sessions":953,"dau":954,"deleted":955,"freed":956,
     "dau_trend":957,"new_ret":958,"funnel_sess":959,"screen_views":960,
     "event_count":961,"cleanup_outcome":962,"locale":963}

dc_seq = [0]
def dc(card_id, tab, row, col, sx, sy):
    dc_seq[0] -= 1
    return {"id": dc_seq[0], "card_id": card_id, "dashboard_tab_id": tab,
            "row": row, "col": col, "size_x": sx, "size_y": sy,
            "series": [], "parameter_mappings": [], "visualization_settings": {}}

dashcards = [
    # ── Tab 1: 유저 (기기 기준) ──
    dc(C["devices"],   TAB_USER, 0, 0, 4, 3),
    dc(C["dau"],       TAB_USER, 0, 4, 4, 3),
    dc(C["deleted"],   TAB_USER, 0, 8, 4, 3),
    dc(C["freed"],     TAB_USER, 0, 12, 4, 3),
    dc(C["dau_trend"], TAB_USER, 3, 0, 12, 6),
    dc(C["new_ret"],   TAB_USER, 3, 12, 12, 6),
    dc(FUNNEL_USER,    TAB_USER, 9, 0, 8, 8),
    dc(SCREEN_USER,    TAB_USER, 9, 8, 8, 8),
    dc(C["locale"],    TAB_USER, 9, 16, 8, 8),
    # ── Tab 2: 세션 · 이벤트 ──
    dc(C["events"],         TAB_SESS, 0, 0, 4, 3),
    dc(C["sessions"],       TAB_SESS, 0, 4, 4, 3),
    dc(C["funnel_sess"],    TAB_SESS, 3, 0, 8, 8),
    dc(C["screen_views"],   TAB_SESS, 3, 8, 8, 8),
    dc(C["cleanup_outcome"],TAB_SESS, 3, 16, 8, 8),
    dc(C["event_count"],    TAB_SESS, 11, 0, 12, 6),
]

api("PUT", f"/api/dashboard/{DASH}", token, {"tabs": tabs, "dashcards": dashcards})
print(f"\n✅ 재구성 완료: {MB}/dashboard/{DASH}")
print("   탭1 '유저(기기 기준)' 9카드 / 탭2 '세션·이벤트' 6카드")
