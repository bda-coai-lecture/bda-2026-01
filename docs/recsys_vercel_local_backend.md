# Vercel Frontend + Local Recsys API

## Current Deployment

- Frontend: `https://recsys-vercel.vercel.app`
- Vercel project: `danes-projects-cc3e75bf/recsys-vercel`
- Source directory: `web/recsys-vercel`
- Local API app: `ghrec.local_api:app`
- Local API port: `127.0.0.1:8001`
- Temporary API tunnel at time of latest recovery: `https://boulder-explicit-draws-revised.trycloudflare.com`

The tunnel URL is a Cloudflare quick tunnel URL. It changes when the tunnel process is restarted.

## Architecture

```text
external user
  -> https://recsys-vercel.vercel.app
  -> Vercel Next.js route handlers
  -> Cloudflare Tunnel or another public tunnel
  -> local Mac: http://127.0.0.1:8001
  -> local parquet/model/SQLite artifacts under data/
```

Vercel hosts only the UI and light proxy route handlers. The recommendation model, parquet files, registry, and metadata cache stay local because the serving stack depends on local artifacts and native Python packages.

## Local Run

```bash
cd /Users/kakao/bda-2
PYTHONPATH=src uv run uvicorn ghrec.local_api:app --host 127.0.0.1 --port 8001 --reload
```

```bash
cd /Users/kakao/bda-2/web/recsys-vercel
source ~/.nvm/nvm.sh && nvm use
npm install
NEXT_PUBLIC_RECSYS_API_BASE_URL=http://localhost:8001 npm run dev
```

Open:

```text
http://127.0.0.1:3000
```

## Public Tunnel

Install once:

```bash
brew install cloudflared
```

Run while the demo should be externally reachable:

```bash
cloudflared tunnel --url http://127.0.0.1:8001
```

Copy the generated `https://*.trycloudflare.com` URL. Keep this process running; if it exits, Vercel still loads but recommendation calls fail.

For a session-independent local recovery, use the LaunchAgents created on this Mac:

```bash
launchctl print gui/$(id -u)/com.kakao.ghrec-local-api
launchctl print gui/$(id -u)/com.kakao.ghrec-cloudflared
tail -f /Users/kakao/bda-2/logs/recsys-local-api.log
tail -f /Users/kakao/bda-2/logs/recsys-cloudflared.err.log
```

The plist files live under `~/Library/LaunchAgents/`. The cloudflared quick tunnel URL is still temporary; if the tunnel restarts and emits a new URL, update `NEXT_PUBLIC_RECSYS_API_BASE_URL` in Vercel and redeploy.

## Vercel Deploy

Set Vercel root directory to:

```text
web/recsys-vercel
```

Set or update the production environment variable whenever the tunnel URL changes:

```bash
cd /Users/kakao/bda-2/web/recsys-vercel
source ~/.nvm/nvm.sh && nvm use
vercel env add NEXT_PUBLIC_RECSYS_API_BASE_URL production
```

Then deploy:

```bash
vercel deploy --prod --yes
```

For one-off deploys, the API URL can also be passed directly:

```bash
vercel deploy --prod --yes \
  --build-env NEXT_PUBLIC_RECSYS_API_BASE_URL=https://your-tunnel.trycloudflare.com \
  --env NEXT_PUBLIC_RECSYS_API_BASE_URL=https://your-tunnel.trycloudflare.com
```

## Verification

```bash
curl -s http://127.0.0.1:8001/health
curl -s https://your-tunnel.trycloudflare.com/health
curl -I https://recsys-vercel.vercel.app
curl -s 'https://recsys-vercel.vercel.app/api/recsys/trending?limit=1'
curl -s 'https://recsys-vercel.vercel.app/api/recsys/personalized?actor_id=4&limit=1'
curl -s 'https://recsys-vercel.vercel.app/api/recsys/related?owner=microsoft&repo=markitdown&limit=1'
```

Local build check:

```bash
cd /Users/kakao/bda-2/web/recsys-vercel
source ~/.nvm/nvm.sh && nvm use
npm run build
```

## API Contract

- `GET /health`
- `GET /api/trending?limit=20`
- `GET /api/users/{actor_id}/recommendations?limit=20`
- `GET /api/users/by-login/{username}/recommendations?limit=20`
- `GET /api/repos/{owner}/{repo}/related?limit=20`
- `GET /api/repos/by-id/{repo_id}/related?limit=20`

The API returns enriched repo rows with `repo_id`, `full_name`, `description`, `language`, `stars`, `forks`, `url`, `rank`, `score`, and `reason` where cached metadata is available.

The Vercel route handlers proxy these endpoints as:

- `GET /api/recsys/trending?limit=20`
- `GET /api/recsys/personalized?actor_id=4&limit=20`
- `GET /api/recsys/related?owner=microsoft&repo=markitdown&limit=20`

## Score Tooltips

The UI shows an info icon next to the `Score` header and each score value.

- Trending: recent activity growth score.
- For User: personalized two-stage ranker score.
- Similar: repo-to-repo co-occurrence relatedness score.

## Metadata Cache

Airflow DAG `gharchive_repo_metadata_refresh` syncs metadata to BigQuery table:

```text
bda-coai.mart.repo_metadata
```

The local API reads local SQLite:

```text
data/repo_metadata.db
```

If the Vercel UI shows many `repo_...` names or empty stars/descriptions, refresh the local SQLite cache from BigQuery:

```bash
cd /Users/kakao/bda-2
GCP_KEY_PATH=gcp-key.json PYTHONPATH=src uv run python scripts/refresh_repo_metadata.py \
  --source bigquery \
  --project bda-coai \
  --dataset mart \
  --fact-table fact_user_repo_activity \
  --metadata-table repo_metadata \
  --key-path gcp-key.json \
  --db-path data/repo_metadata.db \
  --start 2026-03-02 \
  --end 2026-05-30 \
  --top-n 0 \
  --max-fetch 0 \
  --dry-run
```

This command is intentionally `--dry-run`: with BigQuery source mode, it first loads `bda-coai.mart.repo_metadata` into the local SQLite DB, then skips GitHub fetching.

Check cache health:

```bash
sqlite3 data/repo_metadata.db '
select
  count(*) total,
  sum(case when http_status=200 then 1 else 0 end) ok,
  sum(case when description is not null then 1 else 0 end) has_desc,
  sum(case when stargazers is not null then 1 else 0 end) has_stars,
  max(fetched_at) latest_fetch
from repo_metadata;
'
```

Restart `ghrec.local_api:app` after refreshing SQLite because the API caches metadata in memory.

To backfill specific missing repo IDs:

```bash
GCP_KEY_PATH=gcp-key.json PYTHONPATH=src uv run python scripts/refresh_repo_metadata.py \
  --source bigquery \
  --project bda-coai \
  --dataset mart \
  --fact-table fact_user_repo_activity \
  --metadata-table repo_metadata \
  --key-path gcp-key.json \
  --db-path data/repo_metadata.db \
  --start 2026-03-02 \
  --end 2026-05-30 \
  --top-n 0 \
  --repo 888092115=microsoft/markitdown \
  --force-refresh \
  --max-fetch 20 \
  --rate-limit-pause 0.2
```

## CORS

The local API reads:

```text
GHREC_ALLOWED_ORIGINS=http://localhost:3000,https://your-app.vercel.app
GHREC_ALLOWED_ORIGIN_REGEX=https://.*\.vercel\.app
```

Keep `ghrec.local_api` read-only when using a public tunnel. The older `ghrec.api` app includes registry management and path-check endpoints and should not be exposed casually.

The `ghrec-local-api` console script binds to `127.0.0.1:8001` by default. Use an explicit tunnel command when external access is required.

## Known Caveats

- Cloudflare quick tunnel URLs are temporary and have no uptime guarantee.
- Vercel stays up even if the local API or tunnel is down, but recommendation calls fail.
- If `NEXT_PUBLIC_RECSYS_API_BASE_URL` is missing in production, the Next route handlers reject localhost API URLs rather than silently calling a visitor's machine.
- The default shell may use Node 14. Use `source ~/.nvm/nvm.sh && nvm use` in `web/recsys-vercel`; the app has `.nvmrc` set to Node 22.
- `web/recsys-vercel/.next` and `node_modules` are generated and ignored. Do not force-add them.
