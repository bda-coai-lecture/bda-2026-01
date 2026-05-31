# BDA Recsys Vercel App

Next.js frontend for the local recommendation API.

## Local run

```bash
cd web/recsys-vercel
source ~/.nvm/nvm.sh && nvm use
npm install
NEXT_PUBLIC_RECSYS_API_BASE_URL=http://localhost:8001 npm run dev
```

Open `http://localhost:3000`.

## API base URL

The app reads `NEXT_PUBLIC_RECSYS_API_BASE_URL`. If it is unset, the route handlers use
`http://localhost:8001`.

For Vercel, set:

```bash
NEXT_PUBLIC_RECSYS_API_BASE_URL=https://your-recsys-api.example.com
```

Run the local API from the repository root:

```bash
PYTHONPATH=src uv run uvicorn ghrec.local_api:app --host 127.0.0.1 --port 8001 --reload
```
