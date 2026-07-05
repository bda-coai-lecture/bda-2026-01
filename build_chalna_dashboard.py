#!/usr/bin/env python3
import json, urllib.request, urllib.error

MB = "http://localhost:3001"
DB = 3
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
        print(f"  ! {method} {path} -> {e.code}: {e.read().decode()[:300]}")
        raise

token = api("POST", "/api/session", body={"username": USER, "password": PW})["id"]
print("session ok")

KST = "at time zone 'Asia/Seoul'"

cards = [
    # name, display, sql, viz_settings
    ("찰나 · 누적 이벤트", "scalar",
     "select count(*) from events", {}),
    ("찰나 · 누적 기기 수", "scalar",
     "select count(distinct device_id) from events", {}),
    ("찰나 · 누적 세션 수", "scalar",
     "select count(distinct session_id) from events", {}),
    ("찰나 · 오늘 DAU (KST)", "scalar",
     f"select count(distinct device_id) from events where (event_time {KST})::date = (now() {KST})::date", {}),
    ("찰나 · 누적 삭제 사진 수", "scalar",
     "select coalesce(sum((properties->>'deleted_count')::int),0) from events where event_name='cleanup_session_completed'", {}),
    ("찰나 · 누적 확보 용량(GB)", "scalar",
     "select round(coalesce(sum((properties->>'freed_bytes')::numeric),0)/1e9, 2) from events where event_name='cleanup_session_completed'",
     {"column_settings": {}}),

    ("찰나 · DAU 추이 (일별, KST)", "line",
     f"select (event_time {KST})::date as 날짜, count(distinct device_id) as dau from events group by 1 order by 1",
     {"graph.dimensions": ["날짜"], "graph.metrics": ["dau"]}),

    ("찰나 · 신규 vs 재방문 기기 (일별)", "bar",
     f"""with da as (select distinct device_id, (event_time {KST})::date d from events),
              f as (select device_id, min(d) fd from da group by 1)
         select da.d as 날짜,
                count(*) filter (where da.d = f.fd) as 신규,
                count(*) filter (where da.d > f.fd) as 재방문
         from da join f using(device_id) group by da.d order by da.d""",
     {"graph.dimensions": ["날짜"], "graph.metrics": ["신규", "재방문"],
      "stackable.stack_type": "stacked"}),

    ("찰나 · 정리 퍼널 (세션 도달)", "funnel",
     """with s as (
          select session_id,
            bool_or(event_name='app_opened') opened,
            bool_or(event_name='screen_viewed' and properties->>'screen_name'='정리') home,
            bool_or(event_name='screen_viewed' and properties->>'screen_name'='Import') imp,
            bool_or(event_name='screen_viewed' and properties->>'screen_name'='SwipeSelect') swipe,
            bool_or(event_name='cleanup_session_completed') done
          from events group by session_id)
        select step, sessions from (values
          (1,'앱 실행',(select count(*) from s where opened)),
          (2,'홈(정리)',(select count(*) from s where home)),
          (3,'사진 가져오기',(select count(*) from s where imp)),
          (4,'스와이프 선택',(select count(*) from s where swipe)),
          (5,'정리 완료',(select count(*) from s where done))
        ) t(ord, step, sessions) order by ord""",
     {"funnel.dimension": "step", "funnel.metric": "sessions"}),

    ("찰나 · 화면별 조회수", "row",
     "select properties->>'screen_name' as 화면, count(*) as 조회 from events where event_name='screen_viewed' group by 1 order by 2 desc",
     {"graph.dimensions": ["화면"], "graph.metrics": ["조회"]}),

    ("찰나 · 이벤트별 발생 수", "bar",
     "select event_name as 이벤트, count(*) as n from events group by 1 order by 2 desc",
     {"graph.dimensions": ["이벤트"], "graph.metrics": ["n"]}),

    ("찰나 · 정리 완료 vs 중단", "bar",
     """select case event_name when 'cleanup_session_completed' then '완료' else '중단' end as 상태,
               count(*) as 세션
        from events where event_name in ('cleanup_session_completed','cleanup_session_abandoned')
        group by 1 order by 2 desc""",
     {"graph.dimensions": ["상태"], "graph.metrics": ["세션"]}),

    ("찰나 · locale별 기기", "table",
     "select locale, count(distinct device_id) as 기기, count(*) as 이벤트 from events group by 1 order by 2 desc",
     {}),
]

card_ids = []
for name, display, sql, viz in cards:
    payload = {
        "name": name,
        "dataset_query": {"database": DB, "type": "native", "native": {"query": sql}},
        "display": display,
        "visualization_settings": viz,
    }
    res = api("POST", "/api/card", token, payload)
    card_ids.append(res["id"])
    print(f"  card {res['id']:>3}  {display:<7} {name}")

# dashboard
dash = api("POST", "/api/dashboard", token, {"name": "찰나 (Chalna) · 운영 대시보드"})
did = dash["id"]
print("dashboard id:", did)

# layout (24-col grid): id index maps to card_ids order
def dc(i, row, col, sx, sy):
    return {"id": -(i+1), "card_id": card_ids[i], "row": row, "col": col,
            "size_x": sx, "size_y": sy, "series": [], "parameter_mappings": [],
            "visualization_settings": {}}

layout = [
    dc(0, 0, 0, 4, 3), dc(1, 0, 4, 4, 3), dc(2, 0, 8, 4, 3),
    dc(3, 0, 12, 4, 3), dc(4, 0, 16, 4, 3), dc(5, 0, 20, 4, 3),
    dc(6, 3, 0, 12, 6), dc(7, 3, 12, 12, 6),
    dc(8, 9, 0, 8, 8), dc(9, 9, 8, 8, 8), dc(10, 9, 16, 8, 8),
    dc(11, 17, 0, 8, 5), dc(12, 17, 8, 10, 5),
]
api("PUT", f"/api/dashboard/{did}", token, {"dashcards": layout})
print(f"\n✅ 대시보드 완성: {MB}/dashboard/{did}")
print(f"   카드 {len(card_ids)}개 배치 완료")
