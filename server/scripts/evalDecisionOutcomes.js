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

async function evaluateOnce({ lookbackDays = 30, horizonsSec = [3600, 14400, 86400], maxDecisions = 500 } = {}) {
    const nowSec = Math.floor(Date.now() / 1000);
    const sinceSec = nowSec - Math.max(1, lookbackDays) * 24 * 3600;

    const decisions = await query(
        `SELECT id, timestamp, pair_index, timeframe_min, candle_time, analysis, decision
         FROM bot_decisions
         WHERE timestamp >= $1
           AND pair_index IS NOT NULL
           AND timeframe_min IS NOT NULL
           AND candle_time IS NOT NULL
         ORDER BY timestamp DESC
         LIMIT $2`,
        [sinceSec, Math.min(Math.max(Number(maxDecisions) || 500, 1), 5000)]
    );

    const rows = decisions.rows || [];
    let inserted = 0;

    for (const row of rows) {
        const decisionId = row.id;
        const pairIndex = Number.isFinite(Number(row.pair_index)) ? Number(row.pair_index) : null;
        const timeframeMin = Number.isFinite(Number(row.timeframe_min)) ? Number(row.timeframe_min) : null;
        const candleTime = Number.isFinite(Number(row.candle_time)) ? Number(row.candle_time) : null;
        if (pairIndex === null || timeframeMin === null || candleTime === null) continue;

        const analysis = safeJsonParse(row.analysis);
        const action = String(row.decision || analysis?.toolCall?.name || '');
        if (action !== 'execute_trade') continue;

        const direction = analysis?.toolCall?.args?.direction;
        if (direction !== 'LONG' && direction !== 'SHORT') continue;

        const entryPriceFromAnalysis = typeof analysis?.currentPrice === 'number' ? analysis.currentPrice : null;
        const entryPrice = Number.isFinite(entryPriceFromAnalysis) && entryPriceFromAnalysis > 0 ? entryPriceFromAnalysis : null;

        for (const horizonSec of horizonsSec) {
            const parsedHorizon = Number.isFinite(Number(horizonSec)) ? Math.floor(Number(horizonSec)) : null;
            if (!parsedHorizon || parsedHorizon <= 0) continue;

            const existing = await query(
                'SELECT 1 FROM decision_outcomes WHERE decision_id = $1 AND horizon_sec = $2 LIMIT 1',
                [decisionId, parsedHorizon]
            );
            if (existing.rows && existing.rows.length > 0) continue;

            const targetCandleTime = candleTime + parsedHorizon;
            const future = await query(
                `SELECT candle_time, price
                 FROM market_state_history
                 WHERE pair_index = $1
                   AND timeframe_min = $2
                   AND candle_time >= $3
                 ORDER BY candle_time ASC
                 LIMIT 1`,
                [pairIndex, timeframeMin, targetCandleTime]
            );

            const futureRow = future.rows?.[0] ?? null;
            const futurePrice = futureRow && Number.isFinite(Number(futureRow.price)) ? Number(futureRow.price) : null;
            if (!Number.isFinite(futurePrice) || futurePrice <= 0) continue;

            let resolvedEntryPrice = entryPrice;
            if (!Number.isFinite(resolvedEntryPrice) || resolvedEntryPrice <= 0) {
                const at = await query(
                    `SELECT price
                     FROM market_state_history
                     WHERE pair_index = $1
                       AND timeframe_min = $2
                       AND candle_time = $3
                     LIMIT 1`,
                    [pairIndex, timeframeMin, candleTime]
                );
                const atPrice = Number.isFinite(Number(at.rows?.[0]?.price)) ? Number(at.rows[0].price) : null;
                if (!Number.isFinite(atPrice) || atPrice <= 0) continue;
                resolvedEntryPrice = atPrice;
            }

            const forwardReturn = (futurePrice - resolvedEntryPrice) / resolvedEntryPrice;
            const correct = direction === 'LONG' ? (forwardReturn > 0 ? 1 : 0) : (forwardReturn < 0 ? 1 : 0);

            const details = {
                direction,
                action,
                futureCandleTime: futureRow?.candle_time ?? null,
                usedEntryPriceFrom: Number.isFinite(entryPrice) ? 'analysis.currentPrice' : 'market_state_history'
            };

            await query(
                `INSERT INTO decision_outcomes(
                    decision_id, timestamp, pair_index, timeframe_min, candle_time,
                    horizon_sec, entry_price, future_price, forward_return, correct, details_json
                 ) VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8, $9, $10, $11
                 )`,
                [
                    decisionId,
                    nowSec,
                    pairIndex,
                    timeframeMin,
                    candleTime,
                    parsedHorizon,
                    resolvedEntryPrice,
                    futurePrice,
                    forwardReturn,
                    correct,
                    JSON.stringify(details)
                ]
            );

            inserted += 1;
        }
    }

    return { evaluated: rows.length, inserted };
}

async function printSummary({ days = 30 } = {}) {
    const nowSec = Math.floor(Date.now() / 1000);
    const sinceSec = nowSec - Math.max(1, days) * 24 * 3600;

    const summary = await query(
        `SELECT horizon_sec,
                COUNT(1) AS count,
                AVG(forward_return) AS avg_forward_return,
                AVG(correct) AS win_rate
         FROM decision_outcomes
         WHERE timestamp >= $1
         GROUP BY horizon_sec
         ORDER BY horizon_sec ASC`,
        [sinceSec]
    );

    console.log(JSON.stringify({ sinceSec, nowSec, rows: summary.rows || [] }, null, 2));
}

async function main() {
    initSchema();

    const lookbackDays = Number.parseInt(process.env.BOT_EVAL_OUTCOMES_LOOKBACK_DAYS || '30', 10);
    const horizonsSec = (process.env.BOT_EVAL_OUTCOMES_HORIZONS_SEC || '3600,14400,86400')
        .split(',')
        .map(s => Number.parseInt(s.trim(), 10))
        .filter(n => Number.isFinite(n) && n > 0);

    const result = await evaluateOnce({ lookbackDays, horizonsSec });
    console.log(`[evalDecisionOutcomes] evaluated ${result.evaluated} decisions, inserted ${result.inserted} outcomes`);

    await printSummary({ days: lookbackDays });
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
