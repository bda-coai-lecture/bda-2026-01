#!/usr/bin/env python3
"""리포트용 심화 분석: 확산 폭, Claude<->Codex, 폭발 타이밍. 전부 로컬 캐시."""
import pandas as pd
D = "/Users/kakao/bda-2/data/ai_trends"
ev = pd.read_csv(f"{D}/events.csv")
mg = pd.read_csv(f"{D}/monthly_global.csv").sort_values("ym").reset_index(drop=True)
months = sorted(ev.ym.unique())

print("### A) 폭발 타이밍: 월별 AI PR 절대량 + 증감")
a = mg.copy()
a["ai_pct"] = (100*a.ai_pr/a.total_pr).round(2)
a["MoM_abs"] = a.ai_pr.diff().astype("Int64")
a["MoM_pct"] = (100*a.ai_pr.pct_change()).round(0)
print(a[["ym","ai_pr","ai_pct","MoM_abs","MoM_pct"]].to_string(index=False))

print("\n### B) 확산 폭: 신규 도입 레포 & 누적 침투")
first_seen = ev.groupby("repo").ym.min()
newrepo = first_seen.value_counts().sort_index()
cum = newrepo.cumsum()
b = pd.DataFrame({"new_repos": newrepo, "cumulative_repos": cum})
print(b.to_string())

print("\n### C) 툴 점유율 % (월별, 이벤트 기준)")
pt = ev.pivot_table(index="ym", columns="tool", values="repo", aggfunc="count", fill_value=0)
share = (100*pt.div(pt.sum(axis=1), axis=0)).round(1)
print(share.to_string())

print("\n### D) Claude vs Codex (봇 계정) 월별 + 레포 전환")
cc = pt[["claude","codex"]].copy()
cc["codex_over_claude"] = (cc.codex/cc.claude.where(cc.claude>0)).round(2)
print(cc.to_string())
# 레포 단위: claude 쓴 레포가 나중에 codex로?
claude_first = ev[ev.tool=="claude"].groupby("repo").ym.min()
codex_first  = ev[ev.tool=="codex"].groupby("repo").ym.min()
both = set(claude_first.index) & set(codex_first.index)
c2x = sum(codex_first[r] > claude_first[r] for r in both)
x2c = sum(codex_first[r] < claude_first[r] for r in both)
same = sum(codex_first[r] == claude_first[r] for r in both)
print(f"\nclaude∩codex 레포 = {len(both)} | claude→codex 순서 = {c2x} | codex→claude = {x2c} | 동월 = {same}")
print(f"claude 총 레포 = {claude_first.nunique()} | codex 총 레포 = {codex_first.nunique()} | codex 총이벤트 = {(ev.tool=='codex').sum()}")

print("\n### E) 대형/실제 프로젝트 침투 (알려진 org)")
for org in ["microsoft/","Azure/","github/","google","openai/","vercel/","facebook/","apache/"]:
    sub = ev[ev.repo.str.startswith(org)]
    if len(sub):
        print(f"  {org:12} AI PR={len(sub):5}  repos={sub.repo.nunique():4}  tools={sorted(sub.tool.unique())}")
