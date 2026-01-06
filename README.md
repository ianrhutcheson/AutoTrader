# Perps Trader

React dashboard + Node/Express backend for perpetual futures trading analysis.

The backend pulls market data from Gains.trade (prices + trading variables), computes indicators, stores the latest state for every available pair in SQLite, and can rank the top opportunities for the AI bot to analyze.

## Quick Start

1) Backend

```bash
cd server
cp .env.example .env
# set OPENAI_API_KEY in server/.env
npm install
npm run dev
```

2) Frontend

```bash
npm install
npm run dev
```

## Deploy to Railway

This project is set up to deploy as a single Railway service:
- Railway runs `npm install` at the repo root (which also installs `server/` deps via `postinstall`).
- Railway runs `npm run build` at the repo root (builds the Vite app to `dist/`).
- Railway runs `npm start` at the repo root (starts the Express server, which serves both `/api/*` and the built frontend).

Steps:

1) Push to GitHub
- Create a new GitHub repo
- Add it as a remote and push your `main` branch

2) Create a Railway project
- New Project → Deploy from GitHub Repo → select this repo
- Railway should auto-detect Node and use the root `package.json`

3) Configure environment variables (Railway → Variables)
- `OPENAI_API_KEY` (required for bot analysis)
- Optional: any `BOT_*` / `MARKET_*` vars from `server/.env.example`

Notes:
- The frontend API base defaults to same-origin in production (`/api`). For local dev it uses `http://localhost:3000/api`.
- If you want to override the API base (e.g. separate backend service), set `VITE_API_BASE` at build time.

4) Persist the SQLite DB (recommended)

By default the backend uses SQLite at `server/trades.db`. On Railway, attach a Volume so DB state survives redeploys.

- Create a Volume and mount it at `/app/data`
- Set Railway Variable: `SQLITE_DB_PATH=/app/data/trades.db`

If you don’t attach a volume, trades/bot state may be lost on redeploy.

## Market Data + Universe Scan

The backend maintains these tables in `server/trades.db`:
- `pairs`: Gains pair universe (index → from/to)
- `pair_trading_variables`: spread/fees/OI/funding/borrowing (refreshed periodically)
- `market_state`: latest indicators per pair + timeframe
- `bot_universe_decisions`: top-10 candidates + selection + downstream pair analysis

Key endpoints:
- `GET /api/market/status`
- `GET /api/market/pairs?limit=5000`
- `GET /api/market/opportunities?timeframeMin=15&limit=10`
- `POST /api/bot/analyze-universe?timeframeMin=15&limit=10`
- `POST /api/bot/run` (single backend bot run for UI recommendations)

## Autotrade (optional)

Autotrade runs a server-side loop:
1) Scan opportunities from stored `market_state` (cheap)
2) Only call OpenAI when score thresholds pass (cost-controlled)
3) Execute via the same server-enforced validations as manual execution (supports multiple simultaneous bot positions up to `BOT_MAX_OPEN_POSITIONS`, and alternates open/manage when both are possible)
4) Optional: auto-close bot positions when TP/SL prices are hit (`BOT_TP_SL_TRIGGERS_ENABLED=true`)

Endpoints:
- `GET /api/bot/autotrade/status`
- `POST /api/bot/autotrade/toggle` with `{ "enabled": true|false }`
- `POST /api/bot/autotrade/config` with `{ "intervalSec": 15, "minScore": 70, "minLlmIntervalSec": 300 }`

Config knobs live in `server/.env.example` (see `MARKET_*` + `BOT_*` vars).
