#!/usr/bin/env python
"""Build a single-file HTML report for active-user decline analysis.

The report reads mart tables from BigQuery and embeds matplotlib charts as
base64 PNGs, so the output HTML can be shared without extra assets.
"""

from __future__ import annotations

import argparse
import base64
import html
import io
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib import font_manager
import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery


PIVOT_DATE = pd.Timestamp("2025-12-01")
DEFAULT_PROJECT = "bda-coai"
DEFAULT_DATASET = "mart"


@dataclass
class ReportData:
    daily: pd.DataFrame
    event_daily: pd.DataFrame
    weekly: pd.DataFrame
    agent_trendy: pd.DataFrame | None
    agent_validation: pd.DataFrame | None
    optional_notes: list[str]


def configure_korean_font() -> None:
    candidates = [
        "AppleGothic",
        "NanumGothic",
        "Noto Sans CJK KR",
        "Noto Sans KR",
        "Arial Unicode MS",
    ]
    available = {font.name for font in font_manager.fontManager.ttflist}
    for name in candidates:
        if name in available:
            plt.rcParams["font.family"] = name
            plt.rcParams["axes.unicode_minus"] = False
            return


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a Korean HTML report for AU decline analysis from BigQuery mart tables."
    )
    parser.add_argument("--project", default=DEFAULT_PROJECT, help="GCP project id.")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="BigQuery dataset name.")
    parser.add_argument(
        "--key-path",
        default=None,
        help="Service account JSON path. If omitted, Application Default Credentials are used.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("reports/week8_au_decline_report.html"),
        help="Output HTML path.",
    )
    return parser.parse_args()


def make_client(project: str, key_path: str | None) -> bigquery.Client:
    if key_path:
        return bigquery.Client.from_service_account_json(key_path, project=project)
    return bigquery.Client(project=project)


def table_id(project: str, dataset: str, table: str) -> str:
    return f"{project}.{dataset}.{table}"


def read_table(
    client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    order_by: str | None = None,
    required: bool = True,
) -> pd.DataFrame | None:
    full_id = table_id(project, dataset, table)
    try:
        client.get_table(full_id)
    except NotFound:
        if required:
            raise
        return None

    sql = f"SELECT * FROM `{full_id}`"
    if order_by:
        sql += f" ORDER BY {order_by}"
    return client.query(sql).to_dataframe()


def load_data(client: bigquery.Client, project: str, dataset: str) -> ReportData:
    daily = read_table(client, project, dataset, "metrics_daily", "activity_date")
    event_daily = read_table(
        client, project, dataset, "metrics_event_type_daily", "activity_date, action"
    )
    weekly = read_table(client, project, dataset, "metrics_weekly", "week_start")

    notes: list[str] = []
    agent_trendy = read_table(
        client,
        project,
        dataset,
        "metrics_agent_trendy_repos",
        "trend_score DESC",
        required=False,
    )
    if agent_trendy is None:
        notes.append("metrics_agent_trendy_repos 테이블이 없어 AI/agent repo 후보 분석은 생략했습니다.")

    agent_validation = read_table(
        client,
        project,
        dataset,
        "metrics_agent_trend_validation",
        "model",
        required=False,
    )
    if agent_validation is None:
        notes.append("metrics_agent_trend_validation 테이블이 없어 trend 모델 검증 요약은 생략했습니다.")

    daily = normalize_dates(daily, "activity_date")
    event_daily = normalize_dates(event_daily, "activity_date")
    weekly = normalize_dates(weekly, "week_start")
    return ReportData(daily, event_daily, weekly, agent_trendy, agent_validation, notes)


def normalize_dates(df: pd.DataFrame, col: str) -> pd.DataFrame:
    out = df.copy()
    out[col] = pd.to_datetime(out[col])
    return out.sort_values(col).reset_index(drop=True)


def fmt_int(value: Any) -> str:
    if pd.isna(value):
        return "-"
    return f"{int(round(float(value))):,}"


def fmt_float(value: Any, digits: int = 2) -> str:
    if pd.isna(value):
        return "-"
    return f"{float(value):,.{digits}f}"


def fmt_pct(value: Any, digits: int = 1) -> str:
    if pd.isna(value):
        return "-"
    return f"{float(value) * 100:.{digits}f}%"


def pct_change(before: float, after: float) -> float:
    if before == 0 or pd.isna(before) or pd.isna(after):
        return math.nan
    return after / before - 1.0


def window_summary(df: pd.DataFrame, date_col: str, metric_col: str, days: int = 56) -> dict[str, Any]:
    before = df[
        (df[date_col] >= PIVOT_DATE - pd.Timedelta(days=days))
        & (df[date_col] < PIVOT_DATE)
    ][metric_col].dropna()
    after = df[
        (df[date_col] >= PIVOT_DATE)
        & (df[date_col] < PIVOT_DATE + pd.Timedelta(days=days))
    ][metric_col].dropna()

    return {
        "before_n": len(before),
        "after_n": len(after),
        "before_mean": before.mean() if len(before) else math.nan,
        "after_mean": after.mean() if len(after) else math.nan,
        "change": pct_change(before.mean(), after.mean()) if len(before) and len(after) else math.nan,
    }


def build_metric_summaries(data: ReportData) -> list[dict[str, Any]]:
    specs = [
        ("DAU", data.daily, "activity_date", "active_users", "일별 활성 사용자"),
        ("WAU", data.weekly, "week_start", "weekly_active_users", "주별 활성 사용자"),
        ("Events / AU", data.daily, "activity_date", "events_per_active_user", "활성 사용자 1명당 이벤트"),
        ("Total events", data.daily, "activity_date", "total_events", "전체 이벤트 수"),
        ("Active repos", data.daily, "activity_date", "active_repos", "활성 repo 수"),
    ]
    rows = []
    for name, df, date_col, metric_col, label in specs:
        if metric_col not in df.columns:
            continue
        summary = window_summary(df, date_col, metric_col)
        summary.update({"metric": name, "label": label})
        rows.append(summary)
    return rows


def monthly_summary(daily: pd.DataFrame) -> pd.DataFrame:
    out = daily.copy()
    out["month"] = out["activity_date"].dt.strftime("%Y-%m")
    return (
        out.groupby("month")
        .agg(
            avg_dau=("active_users", "mean"),
            avg_events=("total_events", "mean"),
            avg_events_per_au=("events_per_active_user", "mean"),
            min_dau=("active_users", "min"),
            max_dau=("active_users", "max"),
            days=("activity_date", "nunique"),
        )
        .reset_index()
    )


def monthly_table(daily: pd.DataFrame) -> str:
    rows = []
    for _, row in monthly_summary(daily).iterrows():
        rows.append(
            [
                row["month"],
                int(row["days"]),
                fmt_int(row["avg_dau"]),
                fmt_int(row["min_dau"]),
                fmt_int(row["max_dau"]),
                fmt_int(row["avg_events"]),
                fmt_float(row["avg_events_per_au"]),
            ]
        )
    return html_table(
        ["월", "일수", "평균 DAU", "최소 DAU", "최대 DAU", "평균 이벤트", "이벤트/AU"],
        rows,
    )


def data_quality_notes(daily: pd.DataFrame) -> list[str]:
    notes = []
    expected_days = (daily["activity_date"].max() - daily["activity_date"].min()).days + 1
    actual_days = daily["activity_date"].nunique()
    if actual_days == expected_days:
        notes.append(f"일별 mart는 {actual_days}일 모두 존재합니다.")
    else:
        notes.append(f"일별 mart가 {expected_days}일 중 {actual_days}일만 있어 날짜 누락 확인이 필요합니다.")

    median_rows = daily["user_repo_action_rows"].median()
    suspicious = daily[daily["user_repo_action_rows"] < median_rows * 0.2]
    if not suspicious.empty:
        start = suspicious["activity_date"].min().date()
        end = suspicious["activity_date"].max().date()
        notes.append(
            f"{start}~{end}에 user-repo-action row가 평소의 20% 미만인 날이 있어, "
            "이 구간은 실제 사용자 감소보다 원천 이벤트 관측/적재 이상 가능성을 먼저 봐야 합니다."
        )

    latest = daily["activity_date"].max().date()
    notes.append(
        f"최신일은 {latest}입니다. GH Archive 최신 1~2일은 지연 가능성이 있으므로 운영 리포트에서는 보수적으로 해석합니다."
    )
    return notes


def plot_to_base64(fig: plt.Figure) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=170, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def add_pivot_line(ax: plt.Axes) -> None:
    ax.axvline(PIVOT_DATE, color="#d62728", linestyle="--", linewidth=1.2, label="2025-12-01")


def style_time_axis(ax: plt.Axes) -> None:
    ax.grid(True, axis="y", color="#e6e8eb", linewidth=0.8)
    ax.spines[["top", "right"]].set_visible(False)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.tick_params(axis="x", rotation=30)


def chart_daily_au(daily: pd.DataFrame, weekly: pd.DataFrame) -> str:
    fig, ax = plt.subplots(figsize=(10, 4.8))
    ax.plot(daily["activity_date"], daily["active_users"], color="#1f77b4", label="DAU", linewidth=1.8)
    if "weekly_active_users" in weekly.columns and not weekly.empty:
        ax2 = ax.twinx()
        ax2.plot(
            weekly["week_start"],
            weekly["weekly_active_users"],
            color="#2ca02c",
            label="WAU",
            linewidth=1.8,
            alpha=0.85,
        )
        ax2.set_ylabel("WAU")
        ax2.spines["top"].set_visible(False)
    add_pivot_line(ax)
    style_time_axis(ax)
    ax.set_title("활성 사용자 추이와 2025-12 전환점")
    ax.set_ylabel("DAU")
    ax.legend(loc="upper left")
    return plot_to_base64(fig)


def chart_activity_intensity(daily: pd.DataFrame) -> str:
    fig, ax = plt.subplots(figsize=(10, 4.8))
    ax.plot(
        daily["activity_date"],
        daily["events_per_active_user"],
        color="#9467bd",
        label="events per active user",
        linewidth=1.8,
    )
    add_pivot_line(ax)
    style_time_axis(ax)
    ax.set_title("활성 사용자 1명당 활동량")
    ax.set_ylabel("events / AU")
    ax.legend(loc="upper left")
    return plot_to_base64(fig)


def chart_event_mix(event_daily: pd.DataFrame) -> str:
    top_actions = (
        event_daily.groupby("action")["total_events"]
        .sum()
        .sort_values(ascending=False)
        .head(6)
        .index.tolist()
    )
    pivot = (
        event_daily[event_daily["action"].isin(top_actions)]
        .pivot_table(index="activity_date", columns="action", values="total_events", aggfunc="sum")
        .fillna(0)
        .sort_index()
    )
    share = pivot.div(pivot.sum(axis=1).replace(0, math.nan), axis=0)

    fig, ax = plt.subplots(figsize=(10, 4.8))
    share.plot.area(ax=ax, linewidth=0, alpha=0.88)
    add_pivot_line(ax)
    style_time_axis(ax)
    ax.set_title("상위 이벤트 타입 비중")
    ax.set_ylabel("event share")
    ax.set_ylim(0, 1)
    ax.legend(loc="upper left", ncols=2, fontsize=8)
    return plot_to_base64(fig)


def chart_agent_trendy(agent_trendy: pd.DataFrame | None) -> str | None:
    if agent_trendy is None or agent_trendy.empty or "repo_name" not in agent_trendy.columns:
        return None
    df = agent_trendy.head(12).copy()
    metric = "growth_ratio" if "growth_ratio" in df.columns else "trend_score"
    df["repo_name"] = df["repo_name"].fillna(df["repo_id"].astype(str)).astype(str).str.slice(0, 32)
    fig, ax = plt.subplots(figsize=(10, 5.2))
    ax.barh(df["repo_name"][::-1], df[metric][::-1], color="#ff7f0e")
    ax.grid(True, axis="x", color="#e6e8eb", linewidth=0.8)
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_title("AI/agent 후보 repo 최근 성장 관찰")
    ax.set_xlabel(metric)
    return plot_to_base64(fig)


def html_table(headers: list[str], rows: list[list[Any]]) -> str:
    th = "".join(f"<th>{html.escape(str(h))}</th>" for h in headers)
    trs = []
    for row in rows:
        tds = "".join(f"<td>{html.escape(str(cell))}</td>" for cell in row)
        trs.append(f"<tr>{tds}</tr>")
    return f"<table><thead><tr>{th}</tr></thead><tbody>{''.join(trs)}</tbody></table>"


def summary_table(metric_summaries: list[dict[str, Any]]) -> str:
    rows = []
    for item in metric_summaries:
        rows.append(
            [
                item["label"],
                item["before_n"],
                fmt_float(item["before_mean"]),
                item["after_n"],
                fmt_float(item["after_mean"]),
                fmt_pct(item["change"]),
            ]
        )
    return html_table(
        ["지표", "전 56일 n", "전 평균", "후 56일 n", "후 평균", "변화율"],
        rows,
    )


def event_change_table(event_daily: pd.DataFrame) -> str:
    rows = []
    for action, part in event_daily.groupby("action"):
        summary = window_summary(part, "activity_date", "total_events")
        if summary["before_n"] == 0 or summary["after_n"] == 0:
            continue
        rows.append(
            {
                "action": action,
                "before": summary["before_mean"],
                "after": summary["after_mean"],
                "change": summary["change"],
            }
        )
    rows = sorted(rows, key=lambda x: abs(x["change"]) if not pd.isna(x["change"]) else -1, reverse=True)[:12]
    return html_table(
        ["이벤트 타입", "전 평균 이벤트", "후 평균 이벤트", "변화율"],
        [[r["action"], fmt_int(r["before"]), fmt_int(r["after"]), fmt_pct(r["change"])] for r in rows],
    )


def agent_table(agent_trendy: pd.DataFrame | None) -> str:
    if agent_trendy is None or agent_trendy.empty:
        return "<p class=\"muted\">AI/agent 후보 repo 테이블이 없어 이 섹션은 생략했습니다.</p>"
    cols = [
        c
        for c in [
            "repo_name",
            "trend_score",
            "growth_ratio",
            "seed_affinity",
            "recent_active_users",
            "baseline_active_users",
            "recent_top_action",
            "why_trendy",
        ]
        if c in agent_trendy.columns
    ]
    df = agent_trendy[cols].head(15).copy()
    rows = []
    for _, row in df.iterrows():
        out = []
        for col in cols:
            value = row[col]
            if col in {"trend_score", "growth_ratio"}:
                out.append(fmt_float(value))
            elif col == "seed_affinity":
                out.append(fmt_pct(value))
            elif col.endswith("active_users"):
                out.append(fmt_int(value))
            else:
                out.append(str(value)[:140])
        rows.append(out)
    return html_table(cols, rows)


def validation_table(agent_validation: pd.DataFrame | None) -> str:
    if agent_validation is None or agent_validation.empty:
        return "<p class=\"muted\">검증 테이블이 없어 모델 품질 비교는 생략했습니다.</p>"
    cols = [
        c
        for c in [
            "model",
            "candidates",
            "spearman_next_score",
            "precision_at_20_next_top100",
            "ndcg_at_20",
            "avg_next_score_at_20",
        ]
        if c in agent_validation.columns
    ]
    rows = []
    for _, row in agent_validation[cols].iterrows():
        out = []
        for col in cols:
            value = row[col]
            if col == "model":
                out.append(value)
            elif col == "candidates":
                out.append(fmt_int(value))
            else:
                out.append(fmt_float(value, 3))
        rows.append(out)
    return html_table(cols, rows)


def key_findings(data: ReportData, summaries: list[dict[str, Any]]) -> list[str]:
    by_metric = {item["metric"]: item for item in summaries}
    findings = []
    dau = by_metric.get("DAU")
    wau = by_metric.get("WAU")
    intensity = by_metric.get("Events / AU")
    total_events = by_metric.get("Total events")
    monthly = monthly_summary(data.daily)

    if dau and not pd.isna(dau["change"]):
        direction = "감소" if dau["change"] < 0 else "증가"
        findings.append(
            "2025-12-01 전후 56일 평균 기준 DAU는 "
            f"{fmt_int(dau['before_mean'])}명에서 {fmt_int(dau['after_mean'])}명으로 "
            f"{fmt_pct(dau['change'])} {direction}했습니다. 따라서 이 비교만으로는 "
            "'12월 이후 내리막'이라고 보기 어렵습니다."
        )
    if wau and not pd.isna(wau["change"]):
        findings.append(
            "같은 기준에서 WAU는 "
            f"{fmt_int(wau['before_mean'])}명에서 {fmt_int(wau['after_mean'])}명으로 "
            f"{fmt_pct(wau['change'])} 변했습니다."
        )
    if intensity and total_events and not pd.isna(intensity["change"]):
        user_direction = "사용자 수 변화와 활동 강도가"
        if dau and not pd.isna(dau["change"]):
            user_direction = "사용자 수가 줄었을 때 남은 사용자의 활동 강도가" if dau["change"] < 0 else "사용자 수와 활동 강도가"
        findings.append(
            "활성 사용자 1명당 이벤트는 "
            f"{fmt_pct(intensity['change'])} 변했고, 전체 이벤트 수는 "
            f"{fmt_pct(total_events['change'])} 변했습니다. {user_direction} "
            "같이 움직였는지 확인하는 핵심 단서입니다."
        )
    if data.agent_trendy is not None and not data.agent_trendy.empty:
        findings.append(
            "AI/agent 후보 repo에서는 최근 성장 지표가 관찰됩니다. 다만 이 리포트는 "
            "AI 활동량 증가와 AU 감소를 같은 기간에 관찰된 현상 또는 상관 후보로만 다루며, "
            "AI가 AU 감소의 원인이라고 단정하지 않습니다."
        )
    if not monthly.empty:
        best = monthly.sort_values("avg_dau", ascending=False).iloc[0]
        dec = monthly[monthly["month"] == "2025-12"]
        if not dec.empty:
            findings.append(
                f"월평균 DAU 기준 2025-12는 {fmt_int(dec.iloc[0]['avg_dau'])}명이고, "
                f"분석 기간 최고 월은 {best['month']} {fmt_int(best['avg_dau'])}명입니다. "
                "12월은 낮아졌지만 2026년 2~4월에 회복/상승 구간이 확인됩니다."
            )
    return findings


def build_html(data: ReportData) -> str:
    configure_korean_font()
    summaries = build_metric_summaries(data)
    charts = {
        "daily_au": chart_daily_au(data.daily, data.weekly),
        "intensity": chart_activity_intensity(data.daily),
        "event_mix": chart_event_mix(data.event_daily),
        "agent": chart_agent_trendy(data.agent_trendy),
    }
    findings = key_findings(data, summaries)
    min_date = data.daily["activity_date"].min().date()
    max_date = data.daily["activity_date"].max().date()
    optional_notes = "".join(f"<li>{html.escape(note)}</li>" for note in data.optional_notes)
    finding_items = "".join(f"<li>{html.escape(item)}</li>" for item in findings)
    quality_items = "".join(f"<li>{html.escape(item)}</li>" for item in data_quality_notes(data.daily))
    agent_chart = (
        f"<img src=\"data:image/png;base64,{charts['agent']}\" alt=\"AI agent repo trend chart\">"
        if charts["agent"]
        else ""
    )

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Week 8 AU 감소 원인 분석 리포트</title>
  <style>
    body {{
      margin: 0;
      background: #f6f7f9;
      color: #20242a;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      line-height: 1.65;
    }}
    main {{
      max-width: 1080px;
      margin: 0 auto;
      padding: 36px 24px 56px;
      background: white;
    }}
    h1, h2, h3 {{ line-height: 1.25; }}
    h1 {{ margin: 0 0 8px; font-size: 30px; }}
    h2 {{ margin-top: 40px; padding-top: 18px; border-top: 1px solid #e1e4e8; }}
    .meta, .muted {{ color: #616b76; }}
    .callout {{
      border-left: 4px solid #1f77b4;
      background: #eef6ff;
      padding: 14px 18px;
      margin: 20px 0;
    }}
    img {{
      width: 100%;
      max-width: 100%;
      display: block;
      margin: 16px 0 26px;
      border: 1px solid #e1e4e8;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin: 16px 0 28px;
      font-size: 14px;
    }}
    th, td {{
      border-bottom: 1px solid #e6e8eb;
      padding: 9px 10px;
      text-align: left;
      vertical-align: top;
    }}
    th {{ background: #f1f3f5; font-weight: 650; }}
    code {{ background: #f1f3f5; padding: 2px 5px; border-radius: 4px; }}
  </style>
</head>
<body>
<main>
  <h1>Week 8 AU 감소 원인 분석 리포트</h1>
  <p class="meta">데이터 범위: {min_date} ~ {max_date} / 전환점 가설: 2025-12-01</p>

  <div class="callout">
    <strong>해석 원칙:</strong> 이 문서는 AU 감소와 AI/agent 활동량 변화를 같은 기간에 관찰된 현상으로 비교합니다.
    상관 또는 동시 관찰은 원인 증명이 아니므로, "AI 활동 증가가 AU 감소를 일으켰다"처럼 단정하지 않습니다.
  </div>

  <h2>1. 한 줄 요약</h2>
  <ul>{finding_items}</ul>

  <h2>2. 2025-12 전환점 가설 검증</h2>
  <p>
    주니어 분석가 관점에서는 먼저 전환점 전후 같은 길이의 기간을 비교합니다.
    여기서는 2025-12-01 이전 56일과 이후 56일 평균을 비교했습니다.
    표의 변화율이 음수면 전환점 이후 평균이 낮아졌다는 뜻입니다.
  </p>
  {summary_table(summaries)}
  <img src="data:image/png;base64,{charts['daily_au']}" alt="AU trend chart">
  <h3>월별 확인</h3>
  <p>
    전환점 분석은 한 날짜 전후 평균만 보면 위험합니다. 아래 월별 평균을 같이 보면
    2025년 12월은 낮지만 2026년 2~4월에는 DAU가 다시 올라갑니다.
  </p>
  {monthly_table(data.daily)}

  <h2>3. 데이터 품질 체크</h2>
  <p>
    감소 원인을 찾기 전에 같은 정의와 같은 커버리지의 데이터인지 확인해야 합니다.
    특히 2025년 10월 일부 날짜는 이벤트 row와 AU가 비정상적으로 낮아 전체 추세 비교에서 별도 표시가 필요합니다.
  </p>
  <ul>{quality_items}</ul>

  <h2>4. 활동 강도 변화</h2>
  <p>
    AU가 줄어도 전체 이벤트가 같이 줄지, 아니면 남은 사용자가 더 많이 활동했는지를 분리해서 봐야 합니다.
    <code>events_per_active_user</code>가 상승했다면 사용자 수 감소와 활동량 집중이 동시에 발생했을 가능성이 있습니다.
  </p>
  <img src="data:image/png;base64,{charts['intensity']}" alt="Events per active user chart">

  <h2>5. 이벤트 타입별 변화</h2>
  <p>
    어떤 이벤트가 AU 변화와 같은 시점에 움직였는지 확인합니다.
    특정 이벤트 타입의 급증/급감은 제품 사용 방식이나 데이터 수집 방식 변화의 후보 신호입니다.
  </p>
  {event_change_table(data.event_daily)}
  <img src="data:image/png;base64,{charts['event_mix']}" alt="Event mix chart">

  <h2>6. AI/agent 활동 후보 관찰</h2>
  <p>
    <code>metrics_agent_trendy_repos</code>가 있으면 AI/agent 관련 후보 repo의 최근 성장 신호를 봅니다.
    이 표는 "같은 기간에 AI/agent 후보 활동이 커졌는가"를 보기 위한 것이며, AU 감소 원인이라고 결론내리려면
    사용자 단위의 유입/이탈, cohort, 봇/자동화 계정 식별, repo별 사용자 중복 검증이 추가로 필요합니다.
  </p>
  {agent_chart}
  {agent_table(data.agent_trendy)}

  <h2>7. Trend 후보 검증 테이블</h2>
  <p>
    검증 테이블은 trend score가 다음 기간의 관심 repo를 얼마나 잘 맞췄는지 보는 보조 자료입니다.
    점수가 좋아도 AU 감소 원인 검증이 아니라, "AI/agent 후보 repo를 고르는 방식이 쓸 만한가"에 대한 평가입니다.
  </p>
  {validation_table(data.agent_validation)}

  <h2>8. 외부 맥락</h2>
  <p>
    외부 자료는 원인 증명이 아니라 배경입니다. GitHub Octoverse 2025는 GitHub 전체 개발자와
    public/open-source 활동이 성장했다고 설명하므로, 이 데이터의 AU 변화가 있다면 GitHub 전체 사용 감소보다
    특정 관측 범위, 이벤트 노출, 사용자군 변화로 먼저 보는 편이 안전합니다.
    2025년 하반기~2026년 초에는 Codex, Claude Code, GitHub Agent HQ 같은 agentic coding 제품 공개도 이어졌지만,
    현재 mart만으로 AI 도구가 AU를 감소시켰다는 인과는 말할 수 없습니다.
  </p>
  <ul>
    <li><a href="https://github.blog/news-insights/octoverse/octoverse-a-new-developer-joins-github-every-second-as-ai-leads-typescript-to-1/">GitHub Octoverse 2025</a></li>
    <li><a href="https://www.gharchive.org/">GH Archive 설명</a></li>
    <li><a href="https://openai.com/index/introducing-codex/">OpenAI Codex 공개</a>, <a href="https://claude.com/blog/claude-code-on-the-web">Claude Code on the web</a>, <a href="https://github.blog/news-insights/company-news/pick-your-agent-use-claude-and-codex-on-agent-hq/">GitHub Agent HQ</a></li>
    <li><a href="https://github.blog/news-insights/company-news/github-availability-report-october-2025/">GitHub Availability Report: October 2025</a></li>
  </ul>

  <h2>9. 다음 분석 제안</h2>
  <ul>
    <li>신규 사용자와 기존 사용자로 나눠 2025-12 이후 감소가 유입 감소인지 재방문 감소인지 분해합니다.</li>
    <li>AI/agent 후보 repo 활동 사용자와 일반 repo 활동 사용자의 cohort retention을 비교합니다.</li>
    <li>자동화 계정, bot 패턴, 극단적으로 높은 이벤트 사용자를 분리해 AU와 이벤트 수를 다시 계산합니다.</li>
    <li>데이터 파이프라인 변경, 수집 누락, public event 정책 변화 같은 외부 요인을 같은 날짜 축에 표시합니다.</li>
  </ul>

  <h2>10. 데이터 로딩 메모</h2>
  <ul>
    <li>필수 테이블: <code>metrics_daily</code>, <code>metrics_event_type_daily</code>, <code>metrics_weekly</code></li>
    <li>선택 테이블: <code>metrics_agent_trendy_repos</code>, <code>metrics_agent_trend_validation</code></li>
    {optional_notes}
  </ul>
</main>
</body>
</html>
"""


def main() -> None:
    args = parse_args()
    client = make_client(args.project, args.key_path)
    data = load_data(client, args.project, args.dataset)
    html_text = build_html(data)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(html_text, encoding="utf-8")
    print(f"WROTE {args.output}")


if __name__ == "__main__":
    main()
