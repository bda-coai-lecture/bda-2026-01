"use client";

import {
  Activity,
  AlertCircle,
  ExternalLink,
  GitBranch,
  Info,
  Loader2,
  RefreshCw,
  Search,
  Star,
  User,
} from "lucide-react";
import { FormEvent, useEffect, useMemo, useState } from "react";

type RepoItem = {
  repo_id: number;
  full_name: string;
  description?: string | null;
  language?: string | null;
  stars?: number | null;
  forks?: number | null;
  topics?: string[];
  url?: string | null;
  rank: number;
  score?: number | null;
  reason?: string | null;
  candidate_source?: string | null;
  growth_ratio?: number | null;
  cooc_users?: number | null;
};

type ApiPayload = {
  items: RepoItem[];
  metadata?: {
    warnings?: string[];
    cold_start?: boolean;
    count?: number;
    candidate_count?: number;
  };
  user?: {
    username?: string;
    actor_id?: number;
    url?: string;
  };
  anchor?: Omit<RepoItem, "rank">;
  bundle_id?: string;
};

type Tab = "trending" | "user" | "related";

const apiBase = process.env.NEXT_PUBLIC_RECSYS_API_BASE_URL || "http://localhost:8001";

function compactNumber(value?: number | null) {
  if (value === null || value === undefined) return "-";
  return Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

function formatScore(value?: number | null) {
  if (value === null || value === undefined) return "-";
  if (Math.abs(value) >= 100) return value.toFixed(1);
  return value.toFixed(4);
}

async function fetchJson(path: string): Promise<ApiPayload> {
  const response = await fetch(path, { headers: { Accept: "application/json" } });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `HTTP ${response.status}`);
  }
  return response.json();
}

function scoreTooltip(mode: Tab) {
  if (mode === "user") {
    return "Personalized ranker score from the local two-stage model. Higher means the repo is ranked as a better fit for this actor's GitHub activity history.";
  }
  if (mode === "related") {
    return "Repo-to-repo relatedness score from co-occurrence signals. Higher means the repo appeared more strongly with the anchor repo in user histories.";
  }
  return "Trending score from recent GitHub activity growth. Higher means stronger recent activity compared with the prior window.";
}

function InfoTip({ label }: { label: string }) {
  return (
    <span className="infoTip">
      <button type="button" aria-label={label}>
        <Info size={14} aria-hidden />
      </button>
      <span className="tooltip" role="tooltip">
        {label}
      </span>
    </span>
  );
}

function RepoTable({ items, mode }: { items: RepoItem[]; mode: Tab }) {
  if (!items.length) {
    return <div className="empty">결과가 없습니다.</div>;
  }

  return (
    <div className="tableWrap">
      <table>
        <thead>
          <tr>
            <th className="rankCol">Rank</th>
            <th>Repository</th>
            <th>Language</th>
            <th>Stars</th>
            <th>
              <span className="thWithInfo">
                Score
                <InfoTip label={scoreTooltip(mode)} />
              </span>
            </th>
            <th>Signal</th>
          </tr>
        </thead>
        <tbody>
          {items.map((item) => (
            <tr key={`${item.rank}-${item.repo_id}`}>
              <td className="rankCol">{item.rank}</td>
              <td>
                <div className="repoMain">
                  <a className="repoName" href={item.url || `https://github.com/${item.full_name}`} target="_blank">
                    {item.full_name}
                    <ExternalLink size={14} aria-hidden />
                  </a>
                  <p>{item.description || "No cached description"}</p>
                  {item.topics?.length ? (
                    <div className="topics">
                      {item.topics.slice(0, 3).map((topic) => (
                        <span key={topic}>{topic}</span>
                      ))}
                    </div>
                  ) : null}
                </div>
              </td>
              <td>{item.language || "-"}</td>
              <td>
                <span className="metric">
                  <Star size={14} aria-hidden />
                  {compactNumber(item.stars)}
                </span>
              </td>
              <td className="score">
                <span className="scoreValue">
                  {formatScore(item.score)}
                  <InfoTip label={scoreTooltip(mode)} />
                </span>
              </td>
              <td className="signal">
                {item.reason || item.candidate_source || (item.growth_ratio ? `growth ${item.growth_ratio.toFixed(1)}x` : "-")}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function StatusLine({ payload, error }: { payload: ApiPayload | null; error: string | null }) {
  const warnings = payload?.metadata?.warnings || [];
  return (
    <div className="statusLine">
      <span className={error ? "statusBadge error" : "statusBadge"}>
        {error ? <AlertCircle size={15} /> : <Activity size={15} />}
        {error ? "API error" : `API via Next proxy -> ${apiBase}`}
      </span>
      {payload?.bundle_id ? <span className="muted">bundle {payload.bundle_id}</span> : null}
      {payload?.metadata?.cold_start ? <span className="muted">cold start fallback</span> : null}
      {warnings.map((warning) => (
        <span className="warning" key={warning}>
          {warning}
        </span>
      ))}
    </div>
  );
}

export default function Home() {
  const [tab, setTab] = useState<Tab>("trending");
  const [payload, setPayload] = useState<ApiPayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [actorId, setActorId] = useState("4");
  const [repoName, setRepoName] = useState("microsoft/markitdown");
  const [limit, setLimit] = useState(20);

  const title = useMemo(() => {
    if (tab === "user") return "Personalized Recommendations";
    if (tab === "related") return "Related Repositories";
    return "Trending Repositories";
  }, [tab]);

  async function run(nextTab = tab) {
    setLoading(true);
    setError(null);
    try {
      let path = `/api/recsys/trending?limit=${limit}`;
      if (nextTab === "user") {
        path = `/api/recsys/personalized?actor_id=${encodeURIComponent(actorId)}&limit=${limit}`;
      }
      if (nextTab === "related") {
        const [owner, repo] = repoName.split("/");
        if (!owner || !repo) throw new Error("repo는 owner/name 형식이어야 합니다.");
        path = `/api/recsys/related?owner=${encodeURIComponent(owner)}&repo=${encodeURIComponent(repo)}&limit=${limit}`;
      }
      setPayload(await fetchJson(path));
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      setPayload(null);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    run("trending");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    run();
  }

  function switchTab(nextTab: Tab) {
    setTab(nextTab);
    run(nextTab);
  }

  return (
    <main>
      <header className="topbar">
        <div>
          <p className="eyebrow">BDA GitHub Recommender</p>
          <h1>{title}</h1>
        </div>
        <button className="iconButton" type="button" onClick={() => run()} aria-label="Refresh">
          {loading ? <Loader2 className="spin" size={18} /> : <RefreshCw size={18} />}
        </button>
      </header>

      <section className="controls" aria-label="Recommendation controls">
        <div className="tabs" role="tablist" aria-label="Recommendation mode">
          <button className={tab === "trending" ? "active" : ""} onClick={() => switchTab("trending")} type="button">
            <Activity size={16} />
            Trending
          </button>
          <button className={tab === "user" ? "active" : ""} onClick={() => switchTab("user")} type="button">
            <User size={16} />
            For User
          </button>
          <button className={tab === "related" ? "active" : ""} onClick={() => switchTab("related")} type="button">
            <GitBranch size={16} />
            Similar
          </button>
        </div>

        <form className="queryForm" onSubmit={submit}>
          {tab === "user" ? (
            <label>
              Actor ID
              <input value={actorId} onChange={(event) => setActorId(event.target.value)} inputMode="numeric" />
            </label>
          ) : null}
          {tab === "related" ? (
            <label>
              Repository
              <input value={repoName} onChange={(event) => setRepoName(event.target.value)} placeholder="owner/name" />
            </label>
          ) : null}
          <label>
            Limit
            <input
              value={limit}
              min={5}
              max={100}
              type="number"
              onChange={(event) => setLimit(Number(event.target.value))}
            />
          </label>
          <button className="primary" type="submit">
            <Search size={16} />
            Run
          </button>
        </form>
      </section>

      <StatusLine payload={payload} error={error} />
      {error ? <pre className="errorBox">{error}</pre> : null}

      {payload?.user ? (
        <section className="contextBar">
          <span>User</span>
          <strong>{payload.user.username || payload.user.actor_id}</strong>
          {payload.user.url ? <a href={payload.user.url}>GitHub</a> : null}
        </section>
      ) : null}

      {payload?.anchor ? (
        <section className="contextBar">
          <span>Anchor</span>
          <strong>{payload.anchor.full_name}</strong>
          <a href={payload.anchor.url || `https://github.com/${payload.anchor.full_name}`}>GitHub</a>
        </section>
      ) : null}

      <RepoTable items={payload?.items || []} mode={tab} />
    </main>
  );
}
