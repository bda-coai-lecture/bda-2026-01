#!/usr/bin/env python3
import json, urllib.request, urllib.error

MB = "http://localhost:3001"
USER, PW = "bda@local.dev", "bda-local-2026"

def api(method, path, token=None, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(MB + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token: req.add_header("X-Metabase-Session", token)
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read().decode() or "{}")

token = api("POST", "/api/session", body={"username": USER, "password": PW})["id"]

KST = "at time zone 'Asia/Seoul'"
sql = f"""select
  (event_time {KST})::date as 날짜,
  count(distinct device_id) filter (where event_name='app_opened') as 앱실행,
  count(distinct device_id) filter (where event_name='screen_viewed' and properties->>'screen_name'='SwipeSelect') as 정리진입,
  count(distinct device_id) filter (where event_name='cleanup_session_completed') as 정리완료,
  count(distinct device_id) filter (where event_name in ('cleanup_session_completed','cleanup_session_abandoned')
        and coalesce((properties->>'deleted_count')::int,0) > 0) as 삭제발생
from events group by 1 order by 1"""

body = {
    "name": "찰나 · DAU 분해 추이 (실행→진입→완료→삭제, KST)",
    "dataset_query": {"database": 3, "type": "native", "native": {"query": sql}},
    "display": "line",
    "visualization_settings": {
        "graph.dimensions": ["날짜"],
        "graph.metrics": ["앱실행", "정리진입", "정리완료", "삭제발생"],
        "graph.x_axis.title_text": "날짜",
        "graph.y_axis.title_text": "DAU (기기)",
    },
}
res = api("PUT", "/api/card/957", token, body)
print("✅ card 957 업데이트:", res["name"])
