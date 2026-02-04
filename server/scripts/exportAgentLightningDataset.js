const fs = require('fs');
const path = require('path');
require('dotenv').config({ path: path.resolve(__dirname, '..', '.env') });

const { query, initSchema } = require('../db');

function parseFiniteNumber(value) {
  const parsed = typeof value === 'number' ? value : Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function safeJsonParse(value) {
  if (typeof value !== 'string' || !value.trim()) return null;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function pickAnalysisInput(analysis) {
  // Keep the training input compact and stable.
  if (!analysis || typeof analysis !== 'object') return null;
  const a = analysis;
  return {
    marketSummary: a.marketSummary ?? null,
    indicators: a.indicators ?? null,
    candidateScore: a.candidateScore ?? null,
    tradingVariables: a.tradingVariables ?? null,
    // Include minimal open positions context (not full history)
    openPositions: Array.isArray(a?.toolCall?.openPositions)
      ? a.toolCall.openPositions
      : null
  };
}

async function computeDrawdown({ pairIndex, timeframeMin, startCandleTime, endCandleTime, entryPrice, direction }) {
  const row = await query(
    `SELECT MIN(price) AS min_price, MAX(price) AS max_price
     FROM market_state_history
     WHERE pair_index = $1
       AND timeframe_min = $2
       AND candle_time >= $3
       AND candle_time <= $4`,
    [pairIndex, timeframeMin, startCandleTime, endCandleTime]
  );

  const minPrice = parseFiniteNumber(row?.rows?.[0]?.min_price);
  const maxPrice = parseFiniteNumber(row?.rows?.[0]?.max_price);
  if (!Number.isFinite(entryPrice) || entryPrice <= 0) return null;

  if (direction === 'LONG') {
    if (!Number.isFinite(minPrice)) return null;
    return Math.max(0, (entryPrice - minPrice) / entryPrice);
  }

  if (direction === 'SHORT') {
    if (!Number.isFinite(maxPrice)) return null;
    return Math.max(0, (maxPrice - entryPrice) / entryPrice);
  }

  return null;
}

async function exportDataset({
  lookbackDays = 30,
  horizonSec = 3600,
  maxRows = 2000,
  outPath = path.resolve(process.cwd(), 'datasets', 'agentlightning_episodes.jsonl'),
  ddLambda = 2.0
} = {}) {
  const nowSec = Math.floor(Date.now() / 1000);
  const sinceSec = nowSec - Math.max(1, Number(lookbackDays) || 30) * 24 * 3600;

  // Determine optional columns present in bot_decisions (older DBs may be missing newer fields).
  const colsResult = await query(`PRAGMA table_info(bot_decisions)`);
  const cols = new Set((colsResult.rows || []).map((c) => c?.name).filter(Boolean));

  const optCols = [
    cols.has('prompt_version') ? 'd.prompt_version' : 'NULL AS prompt_version',
    cols.has('model') ? 'd.model' : 'NULL AS model',
    cols.has('usage_json') ? 'd.usage_json' : 'NULL AS usage_json',
    cols.has('analysis_cost_json') ? 'd.analysis_cost_json' : 'NULL AS analysis_cost_json'
  ].join(',\n            ');

  // Join outcomes -> decisions (only execute_trade decisions get outcomes)
  const results = await query(
    `SELECT o.decision_id,
            o.timestamp AS outcome_timestamp,
            o.pair_index,
            o.timeframe_min,
            o.candle_time,
            o.horizon_sec,
            o.entry_price,
            o.future_price,
            o.forward_return,
            o.correct,
            o.details_json,
            d.analysis,
            d.decision,
            d.trade_id,
            ${optCols}
     FROM decision_outcomes o
     JOIN bot_decisions d ON d.id = o.decision_id
     WHERE o.timestamp >= $1
       AND o.horizon_sec = $2
     ORDER BY o.timestamp DESC
     LIMIT $3`,
    [sinceSec, Math.floor(Number(horizonSec) || 3600), Math.min(Math.max(Number(maxRows) || 2000, 1), 50000)]
  );

  const rows = results.rows || [];
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  const out = fs.createWriteStream(outPath, { flags: 'w' });

  let written = 0;

  for (const row of rows) {
    const analysis = safeJsonParse(row.analysis);
    const details = safeJsonParse(row.details_json);

    const direction = details?.direction;
    if (direction !== 'LONG' && direction !== 'SHORT') continue;

    const entryPrice = parseFiniteNumber(row.entry_price);
    const forwardReturn = parseFiniteNumber(row.forward_return);
    if (!Number.isFinite(entryPrice) || entryPrice <= 0) continue;
    if (!Number.isFinite(forwardReturn)) continue;

    // Directional 1h PnL in return terms.
    const pnlReturn = direction === 'LONG' ? forwardReturn : -forwardReturn;

    const dd = await computeDrawdown({
      pairIndex: Number(row.pair_index),
      timeframeMin: Number(row.timeframe_min),
      startCandleTime: Number(row.candle_time),
      endCandleTime: Number(row.candle_time) + Number(row.horizon_sec),
      entryPrice,
      direction
    });

    const reward = (Number.isFinite(pnlReturn) ? pnlReturn : 0) - (Number.isFinite(dd) ? (ddLambda * dd) : 0);

    const episode = {
      episode_id: row.decision_id,
      timestamp: row.candle_time,
      horizon_sec: row.horizon_sec,
      input: {
        pair_index: row.pair_index,
        timeframe_min: row.timeframe_min,
        candle_time: row.candle_time,
        analysis: pickAnalysisInput(analysis)
      },
      action: {
        action: row.decision,
        // The normalized tool call is stored in analysis.toolCall
        tool_call: analysis?.toolCall ?? null
      },
      reward,
      metrics: {
        pnl_return_1h: pnlReturn,
        forward_return_1h: forwardReturn,
        drawdown_1h: dd,
        dd_lambda: ddLambda,
        correct: row.correct ?? null
      },
      meta: {
        trade_id: row.trade_id ?? null,
        prompt_version: row.prompt_version ?? null,
        model: row.model ?? null,
        usage: safeJsonParse(row.usage_json) ?? null,
        analysis_cost: safeJsonParse(row.analysis_cost_json) ?? null
      }
    };

    out.write(`${JSON.stringify(episode)}\n`);
    written += 1;
  }

  out.end();
  return { totalOutcomes: rows.length, written, outPath };
}

async function main() {
  initSchema();

  const lookbackDays = Number.parseInt(process.env.AGL_LOOKBACK_DAYS || process.env.BOT_EVAL_OUTCOMES_LOOKBACK_DAYS || '30', 10);
  const horizonSec = Number.parseInt(process.env.AGL_HORIZON_SEC || '3600', 10);
  const maxRows = Number.parseInt(process.env.AGL_MAX_ROWS || '2000', 10);
  const ddLambda = parseFiniteNumber(process.env.AGL_DD_LAMBDA) ?? 2.0;

  const outPath = process.env.AGL_OUT_PATH
    ? path.resolve(process.env.AGL_OUT_PATH)
    : path.resolve(process.cwd(), 'datasets', `agentlightning_episodes_${horizonSec}s.jsonl`);

  const result = await exportDataset({ lookbackDays, horizonSec, maxRows, outPath, ddLambda });
  console.log(`[exportAgentLightningDataset] outcomes=${result.totalOutcomes} written=${result.written} -> ${result.outPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
