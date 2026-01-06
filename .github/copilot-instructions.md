# Copilot instructions (Perps Trader)

## Architecture (read this first)
- Two processes:
  - Frontend: Vite + React + TS in `src/` (runs on Vite dev server).
  - Backend: Node/Express in `server/` (CommonJS) + SQLite (`server/trades.db`).
- Market data sources are Gains.trade:
  - OHLC comes directly from Gains charts endpoint (frontend uses it in `src/services/api.ts`).
  - Realtime tick prices come from a Gains WebSocket and are proxied to the UI as SSE via `GET /api/prices/stream` (see `server/pricesStream.js`, consumed via `EventSource` in `src/App.tsx`).
  - Cross-asset “universe scan” is server-side: `server/marketData.js` backfills candles for *all pairs/timeframes*, computes indicators, and upserts a single latest row per `(pair_index,timeframe_min)` into `market_state` (see schema in `server/db.js`).

## Key backend entrypoints & flows
- Server boot: `server/index.js` calls `initSchema()` then `marketData.start()` and registers the SSE price proxy.
- Important tables live in `server/db.js`: `market_state`, `pair_trading_variables`, `trades`, `bot_decisions`, `bot_universe_decisions`.
- SQL convention: use Postgres-style placeholders (`$1`, `$2`, …). The wrapper in `server/db.js` converts them to SQLite `?`.

## APIs the UI relies on
- Trading CRUD (manual + bot trades): `GET/POST /api/trades`, `PUT /api/trades/:id/close`, `PUT /api/trades/:id/cancel` (implemented in `server/index.js`, called from `src/services/api.ts`).
- Market status + ranking:
  - `GET /api/market/status` and `GET /api/market/opportunities` (ranking uses `server/opportunityRanker.js` + DB state).
- Bot + autotrade:
  - `POST /api/bot/run`, `POST /api/bot/analyze/:pairIndex`, `POST /api/bot/analyze-universe` (UI: `src/components/BotPanel.tsx`).
  - Autotrade endpoints: `GET /api/bot/autotrade/status`, `POST /api/bot/autotrade/toggle`, `POST /api/bot/autotrade/config`.

## Local dev workflows
- Backend (required for trades/bot/opportunity endpoints):
  - `cd server && cp .env.example .env && npm install && npm run dev`
  - Needs `OPENAI_API_KEY` in `server/.env` for bot analysis (`server/aiBot.js`).
- Frontend:
  - `npm install && npm run dev`
- Ports/URLs are currently hard-coded in the frontend:
  - Backend API base: `http://localhost:3000/api` (see `src/components/BotPanel.tsx` and `src/services/api.ts`).
  - OHLC base: `https://backend-pricing.eu.gains.trade/charts` (see `src/services/api.ts`).

## Project-specific conventions to follow
- Keep backend files CommonJS (`require`, `module.exports`) under `server/`.
- Prefer extending existing endpoints/flows in `server/index.js` rather than adding new servers.
- When working with candles/series:
  - Times are unix seconds (not ms) for Lightweight Charts.
  - UI dedupes identical candle timestamps and merges incremental updates (see `normalizeChartData` + `mergeIncrementalCandles` in `src/App.tsx`).
- Opportunity scoring logic is centralized in `server/opportunityRanker.js` (`scoreOpportunity`/`rankTop`); reuse it instead of re-implementing ranking.

## Environment knobs (don’t guess)
- Add new config as env vars in `server/.env.example` alongside existing `MARKET_*` and `BOT_*` settings.
