#!/usr/bin/env python3
"""로컬 캐시(data/*.csv) 기반 상세분석. BigQuery 재쿼리 없이 전부 오프라인."""
import pandas as pd

D = "/Users/kakao/bda-2/data/ai_trends"
ev = pd.read_csv(f"{D}/events.csv")
ra = pd.read_csv(f"{D}/repo_active.csv")
mg = pd.read_csv(f"{D}/monthly_global.csv").sort_values("ym")

pd.set_option("display.width", 200)
pd.set_option("display.max_rows", 100)

print("### 1) 월별 전역 트렌드 + 분모 희석 (#3)")
g = mg.copy()
g["global_ai_pct"] = (100 * g.ai_pr / g.total_pr).round(2)
# 활성 레포(AI PR>=1) 내부 분모
act = ra.groupby("ym").agg(active_repos=("repo", "nunique"),
                           active_total_pr=("total_pr", "sum"),
                           active_ai_pr=("ai_pr", "sum")).reset_index()
g = g.merge(act, on="ym", how="left")
g["active_repo_ai_pct"] = (100 * g.active_ai_pr / g.active_total_pr).round(2)
g["dilution_x"] = (g.active_repo_ai_pct / g.global_ai_pct).round(1)
print(g[["ym", "total_pr", "ai_pr", "global_ai_pct",
         "active_repos", "active_repo_ai_pct", "dilution_x",
         "repos_total", "repos_ai"]].to_string(index=False))

print("\n### 2) 툴별 월별 PR 수")
pt = ev.pivot_table(index="ym", columns="tool", values="repo",
                    aggfunc="count", fill_value=0).sort_index()
print(pt.to_string())

print("\n### 3) AI PR 상위 레포 (전 기간 누적)")
top = ev.groupby("repo").agg(ai_prs=("tool", "count"),
                             tools=("tool", lambda s: ",".join(sorted(set(s))))).reset_index()
print(top.sort_values("ai_prs", ascending=False).head(20).to_string(index=False))

print("\n### 4) 집중도: 상위 N개 레포가 AI PR의 몇 %?")
tot = len(ev)
s = ev.groupby("repo").size().sort_values(ascending=False)
for n in [10, 50, 100, 500]:
    print(f"  top {n:>4} repos = {100*s.head(n).sum()/tot:5.1f}%")
print(f"  총 AI PR = {tot:,},  고유 레포 = {ev.repo.nunique():,}")
onehit = (s == 1).sum()
print(f"  AI PR 딱 1건인 레포 = {onehit:,} ({100*onehit/ev.repo.nunique():.1f}%)")

print("\n### 5) 레포당 여러 AI 툴 동시 사용")
per_repo_tools = ev.groupby("repo")["tool"].nunique()
print(per_repo_tools.value_counts().sort_index().to_string())
