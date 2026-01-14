const path = require('path');
const crypto = require('crypto');
const fs = require('fs');
require('dotenv').config({ path: path.resolve(__dirname, '.env') });
const express = require('express');
const cors = require('cors');
const { query, initSchema } = require('./db');
const dbMaintenance = require('./dbMaintenance');
const { calculateIndicators, generateMarketSummary } = require('./indicators');
const { registerPriceStreamRoutes, addPriceObserver, getLastPrice } = require('./pricesStream');
const marketData = require('./marketData');
const { rankTop, rankTopOverall } = require('./opportunityRanker');
const { fetchTradingVariablesCached, buildTradingVariablesForPair } = require('./gainsTradingVariables');

const BOT_REFLECTIONS_ENABLED = process.env.BOT_REFLECTIONS_ENABLED !== 'false';
const BOT_REFLECTION_RETENTION_DAYS = Number.isFinite(Number(process.env.BOT_REFLECTION_RETENTION_DAYS))
    ? Math.max(1, Math.min(3650, Number(process.env.BOT_REFLECTION_RETENTION_DAYS)))
    : 365;

const BOT_TUNING_ENABLED = process.env.BOT_TUNING_ENABLED !== 'false';
const BOT_TUNING_INTERVAL_SEC = Number.isFinite(Number(process.env.BOT_TUNING_INTERVAL_SEC))
    ? Math.max(60, Math.min(24 * 60 * 60, Number(process.env.BOT_TUNING_INTERVAL_SEC)))
    : 60 * 60;
const BOT_TUNING_LOOKBACK_DAYS = Number.isFinite(Number(process.env.BOT_TUNING_LOOKBACK_DAYS))
    ? Math.max(3, Math.min(365, Number(process.env.BOT_TUNING_LOOKBACK_DAYS)))
    : 30;
const BOT_TUNING_MIN_TRADES = Number.isFinite(Number(process.env.BOT_TUNING_MIN_TRADES))
    ? Math.max(5, Math.min(500, Number(process.env.BOT_TUNING_MIN_TRADES)))
    : 20;

const BOT_EVAL_OUTCOMES_ENABLED = process.env.BOT_EVAL_OUTCOMES_ENABLED !== 'false';
const BOT_EVAL_OUTCOMES_INTERVAL_SEC = Number.isFinite(Number(process.env.BOT_EVAL_OUTCOMES_INTERVAL_SEC))
    ? Math.max(60, Math.min(24 * 60 * 60, Number(process.env.BOT_EVAL_OUTCOMES_INTERVAL_SEC)))
    : 5 * 60;
const BOT_EVAL_OUTCOMES_LOOKBACK_DAYS = Number.isFinite(Number(process.env.BOT_EVAL_OUTCOMES_LOOKBACK_DAYS))
    ? Math.max(1, Math.min(365, Number(process.env.BOT_EVAL_OUTCOMES_LOOKBACK_DAYS)))
    : 30;
const BOT_EVAL_OUTCOMES_HORIZONS_SEC = typeof process.env.BOT_EVAL_OUTCOMES_HORIZONS_SEC === 'string' && process.env.BOT_EVAL_OUTCOMES_HORIZONS_SEC.trim()
    ? process.env.BOT_EVAL_OUTCOMES_HORIZONS_SEC
        .split(',')
        .map(s => Number.parseInt(s.trim(), 10))
        .filter(n => Number.isFinite(n) && n > 0)
    : [60 * 60, 4 * 60 * 60, 24 * 60 * 60];

const app = express();
const port = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// Realtime prices (SSE proxy to Gains websocket)
registerPriceStreamRoutes(app);

// Initialize DB
initSchema();
dbMaintenance.start();
void marketData.start();

// ============================
// Bot thresholds (DB-backed versions; foundation for tuning)
// ============================
let activeExecutionThresholdParams = null;
let activeExecutionThresholdVersion = null;
let tuningInFlight = false;
let outcomesInFlight = false;

async function refreshActiveExecutionThresholds() {
    try {
        const result = await query(
            `SELECT id, created_at, scope, params_json, metrics_json, reason, parent_version_id
             FROM bot_threshold_versions
             WHERE scope = $1 AND is_active = 1
             ORDER BY created_at DESC
             LIMIT 1`,
            ['execution']
        );

        const row = result.rows?.[0] ?? null;
        if (!row?.params_json) {
            activeExecutionThresholdParams = null;
            activeExecutionThresholdVersion = null;
            return;
        }

        let parsedParams = null;
        try {
            parsedParams = JSON.parse(row.params_json);
        } catch {
            parsedParams = null;
        }

        activeExecutionThresholdParams = parsedParams;
        activeExecutionThresholdVersion = {
            id: row.id,
            created_at: row.created_at,
            scope: row.scope,
            reason: row.reason ?? null,
            parent_version_id: row.parent_version_id ?? null,
            metrics_json: row.metrics_json ?? null
        };
    } catch {
        // Best-effort.
    }
}

function buildDefaultExecutionThresholdParams(timeframeMin = 15) {
    const thresholds = resolveExecutionThresholds(timeframeMin);
    return {
        minOiTotal: thresholds.minOiTotal,
        maxCostPercent: thresholds.maxCostPercent,
        maxOpenPositions: thresholds.maxOpenPositions,
        allowMultiplePositionsPerPair: thresholds.allowMultiplePositionsPerPair,
        maxDecisionAgeSec: thresholds.maxDecisionAgeSec,
        maxTradingVariablesAgeSec: thresholds.maxTradingVariablesAgeSec,
        maxMarketStateAgeMultiplier: 2,
        minConfidence: thresholds.minConfidence
    };
}

async function ensureDefaultExecutionThresholdVersionActive() {
    try {
        const existing = await query(
            'SELECT 1 FROM bot_threshold_versions WHERE scope = $1 AND is_active = 1 LIMIT 1',
            ['execution']
        );
        if (existing.rows && existing.rows.length > 0) return;

        const nowSec = Math.floor(Date.now() / 1000);
        const params = buildDefaultExecutionThresholdParams(15);

        // Clear any stray actives for safety, then insert a default active version.
        await query(
            'UPDATE bot_threshold_versions SET is_active = 0 WHERE scope = $1',
            ['execution']
        );

        await query(
            `INSERT INTO bot_threshold_versions(created_at, scope, params_json, metrics_json, reason, parent_version_id, is_active)
             VALUES ($1, $2, $3, $4, $5, $6, $7)`,
            [
                nowSec,
                'execution',
                JSON.stringify(params),
                null,
                'bootstrap default thresholds from env',
                null,
                1
            ]
        );
    } catch {
        // Best-effort.
    }
}

async function runMinConfidenceTuner() {
    if (!BOT_TUNING_ENABLED) return;
    if (tuningInFlight) return;
    tuningInFlight = true;

    try {
        const nowSec = Math.floor(Date.now() / 1000);
        const sinceSec = nowSec - BOT_TUNING_LOOKBACK_DAYS * 24 * 3600;

        const rowsResult = await query(
            `SELECT d.confidence AS confidence, t.pnl AS pnl
             FROM trades t
             JOIN bot_decisions d ON d.trade_id = t.id
             WHERE t.source = 'BOT'
               AND t.status = 'CLOSED'
               AND t.exit_time >= $1
               AND d.confidence IS NOT NULL`,
            [sinceSec]
        );

        const rows = (rowsResult.rows || [])
            .map((r) => ({
                confidence: parseFiniteNumber(r.confidence),
                pnl: parseFiniteNumber(r.pnl)
            }))
            .filter((r) => r.confidence !== null && r.pnl !== null);

        if (rows.length < BOT_TUNING_MIN_TRADES) return;

        const candidates = [];
        for (let c = 0.5; c <= 0.95 + 1e-9; c += 0.05) {
            candidates.push(Math.round(c * 100) / 100);
        }

        let best = null;
        for (const threshold of candidates) {
            const filtered = rows.filter((r) => r.confidence >= threshold);
            if (filtered.length < BOT_TUNING_MIN_TRADES) continue;

            const pnlSum = filtered.reduce((acc, r) => acc + (r.pnl ?? 0), 0);
            const avgPnl = pnlSum / filtered.length;
            const winCount = filtered.reduce((acc, r) => acc + (r.pnl > 0 ? 1 : 0), 0);
            const winRate = winCount / filtered.length;

            const candidate = {
                threshold,
                count: filtered.length,
                avgPnl,
                winRate
            };

            if (!best) {
                best = candidate;
                continue;
            }

            if (candidate.avgPnl > best.avgPnl + 1e-12) {
                best = candidate;
                continue;
            }

            if (Math.abs(candidate.avgPnl - best.avgPnl) <= 1e-12 && candidate.count > best.count) {
                best = candidate;
            }
        }

        if (!best) return;

        const currentMinConfidence =
            parseFiniteNumber(activeExecutionThresholdParams?.minConfidence) ??
            parseFiniteNumber(process.env.BOT_MIN_CONFIDENCE) ??
            0.7;

        // Avoid thrashing: require a meaningful change.
        if (Math.abs(best.threshold - currentMinConfidence) < 0.025) return;

        const nextMinConfidence = Math.max(0.5, Math.min(0.95, best.threshold));
        const baseParams = (activeExecutionThresholdParams && typeof activeExecutionThresholdParams === 'object')
            ? { ...activeExecutionThresholdParams }
            : buildDefaultExecutionThresholdParams(15);

        const nextParams = {
            ...baseParams,
            minConfidence: nextMinConfidence
        };

        const nowSecInsert = Math.floor(Date.now() / 1000);
        const parentId = activeExecutionThresholdVersion?.id ?? null;
        const metrics = {
            lookbackDays: BOT_TUNING_LOOKBACK_DAYS,
            minTrades: BOT_TUNING_MIN_TRADES,
            evaluatedCandidates: candidates,
            selected: best,
            previousMinConfidence: currentMinConfidence
        };

        await query('UPDATE bot_threshold_versions SET is_active = 0 WHERE scope = $1', ['execution']);
        await query(
            `INSERT INTO bot_threshold_versions(created_at, scope, params_json, metrics_json, reason, parent_version_id, is_active)
             VALUES ($1, $2, $3, $4, $5, $6, $7)`,
            [
                nowSecInsert,
                'execution',
                JSON.stringify(nextParams),
                JSON.stringify(metrics),
                'auto-tune minConfidence from recent BOT trade outcomes',
                parentId,
                1
            ]
        );

        await refreshActiveExecutionThresholds();
    } catch (err) {
        console.warn('[botTuning] minConfidence tuner failed:', err?.message || err);
    } finally {
        tuningInFlight = false;
    }
}

function safeJsonParse(value) {
    if (typeof value !== 'string' || !value.trim()) return null;
    try {
        return JSON.parse(value);
    } catch {
        return null;
    }
}

async function evaluateDecisionOutcomesOnce({ maxDecisions = 200 } = {}) {
    if (!BOT_EVAL_OUTCOMES_ENABLED) return { evaluated: 0, inserted: 0 };
    if (outcomesInFlight) return { evaluated: 0, inserted: 0 };
    outcomesInFlight = true;

    try {
        const nowSec = Math.floor(Date.now() / 1000);
        const sinceSec = nowSec - BOT_EVAL_OUTCOMES_LOOKBACK_DAYS * 24 * 3600;
        const limit = Math.min(Math.max(Number(maxDecisions) || 200, 1), 2000);

        const decisions = await query(
            `SELECT id, timestamp, pair_index, timeframe_min, candle_time, analysis, decision
             FROM bot_decisions
             WHERE timestamp >= $1
               AND pair_index IS NOT NULL
               AND timeframe_min IS NOT NULL
               AND candle_time IS NOT NULL
             ORDER BY timestamp DESC
             LIMIT $2`,
            [sinceSec, limit]
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
            const entryPrice = Number.isFinite(entryPriceFromAnalysis) && entryPriceFromAnalysis > 0
                ? entryPriceFromAnalysis
                : null;

            for (const horizonSec of BOT_EVAL_OUTCOMES_HORIZONS_SEC) {
                const parsedHorizon = Number.isFinite(Number(horizonSec)) ? Math.floor(Number(horizonSec)) : null;
                if (!parsedHorizon || parsedHorizon <= 0) continue;

                // Skip if already computed.
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
                const correct = direction === 'LONG'
                    ? (forwardReturn > 0 ? 1 : 0)
                    : (forwardReturn < 0 ? 1 : 0);

                const details = {
                    direction,
                    action,
                    futureCandleTime: futureRow?.candle_time ?? null,
                    usedEntryPriceFrom: Number.isFinite(entryPrice) ? 'analysis.currentPrice' : 'market_state_history',
                    horizonsSec: BOT_EVAL_OUTCOMES_HORIZONS_SEC
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

        try {
            await query(
                'INSERT INTO metrics_events(timestamp, name, details_json) VALUES ($1, $2, $3)',
                [
                    Math.floor(Date.now() / 1000),
                    'decision_outcomes_eval',
                    JSON.stringify({ evaluatedDecisions: rows.length, inserted, horizonsSec: BOT_EVAL_OUTCOMES_HORIZONS_SEC, lookbackDays: BOT_EVAL_OUTCOMES_LOOKBACK_DAYS })
                ]
            );
        } catch {
            // Best-effort.
        }

        return { evaluated: rows.length, inserted };
    } finally {
        outcomesInFlight = false;
    }
}

void (async () => {
    await ensureDefaultExecutionThresholdVersionActive();
    await refreshActiveExecutionThresholds();
    setInterval(() => {
        void refreshActiveExecutionThresholds();
    }, 30 * 1000);

    if (BOT_TUNING_ENABLED) {
        // Run once shortly after startup, then periodically.
        setTimeout(() => {
            void runMinConfidenceTuner();
        }, 15 * 1000);
        setInterval(() => {
            void runMinConfidenceTuner();
        }, BOT_TUNING_INTERVAL_SEC * 1000);
    }

    if (BOT_EVAL_OUTCOMES_ENABLED) {
        setTimeout(() => {
            void evaluateDecisionOutcomesOnce();
        }, 25 * 1000);
        setInterval(() => {
            void evaluateDecisionOutcomesOnce();
        }, BOT_EVAL_OUTCOMES_INTERVAL_SEC * 1000);
    }
})();

// ============================
// Bot reflections (trade-close memory)
// ============================
const reflectionInFlight = new Set();

async function pruneBotReflections() {
    const nowSec = Math.floor(Date.now() / 1000);
    const cutoffSec = nowSec - BOT_REFLECTION_RETENTION_DAYS * 24 * 3600;
    try {
        await query('DELETE FROM bot_reflections WHERE timestamp < $1', [cutoffSec]);
    } catch (err) {
        console.warn('[botReflections] Prune failed:', err?.message || err);
    }
}

async function enqueueTradeCloseReflection(tradeId, { timeframeMin = null } = {}) {
    if (!BOT_REFLECTIONS_ENABLED) return;
    if (!Number.isFinite(Number(tradeId))) return;
    const key = String(tradeId);
    if (reflectionInFlight.has(key)) return;
    reflectionInFlight.add(key);

    setTimeout(async () => {
        try {
            const tradeResult = await query('SELECT * FROM trades WHERE id = $1', [tradeId]);
            const tradeRow = tradeResult.rows?.[0] ?? null;
            if (!tradeRow) return;
            if (tradeRow.source !== 'BOT') return;
            if (tradeRow.status !== 'CLOSED') return;

            const existing = await query(
                'SELECT 1 FROM bot_reflections WHERE scope = $1 AND trade_id = $2 LIMIT 1',
                ['trade_close', tradeId]
            );
            if (existing.rows && existing.rows.length > 0) return;

            const decisionResult = await query(
                'SELECT * FROM bot_decisions WHERE trade_id = $1 ORDER BY timestamp DESC LIMIT 1',
                [tradeId]
            );
            const decisionRow = decisionResult.rows?.[0] ?? null;
            const tf = Number.isFinite(Number(timeframeMin))
                ? Number(timeframeMin)
                : Number.isFinite(Number(decisionRow?.timeframe_min))
                    ? Number(decisionRow.timeframe_min)
                    : null;

            const reflection = await aiBot.generateTradeCloseReflection({
                tradeRow,
                decisionRow,
                timeframeMin: tf
            });
            if (!reflection?.success) {
                console.warn('[botReflections] Reflection generation failed:', reflection?.error);
                return;
            }

            const payload = reflection.output;
            const nowSec = Math.floor(Date.now() / 1000);
            await query(
                `INSERT INTO bot_reflections (
                    timestamp, scope, trade_id, decision_id, pair_index, timeframe_min,
                    summary, tags_json, reflection_json, inputs_json,
                    model, prompt_version
                ) VALUES (
                    $1, $2, $3, $4, $5, $6,
                    $7, $8, $9, $10,
                    $11, $12
                )`,
                [
                    nowSec,
                    'trade_close',
                    tradeId,
                    decisionRow?.id ?? null,
                    tradeRow.pair_index,
                    tf,
                    payload.summary,
                    JSON.stringify(payload.tags || []),
                    JSON.stringify(payload),
                    JSON.stringify({ tradeId, decisionId: decisionRow?.id ?? null }),
                    reflection.model,
                    reflection.promptVersion
                ]
            );
        } catch (err) {
            console.warn('[botReflections] enqueue failed:', err?.message || err);
        } finally {
            reflectionInFlight.delete(key);
        }
    }, 0);
}

// Prune reflections on boot and daily.
void pruneBotReflections();
setInterval(() => void pruneBotReflections(), 24 * 3600 * 1000);

// ============================
// Triggered entry orders (PENDING -> OPEN)
// ============================
const TRIGGERED_ORDER_REFRESH_MS = 5_000;
const TRIGGERED_ORDER_DEBOUNCE_MS = 250;
const TRIGGER_ORDERS_DEBUG = process.env.TRIGGER_ORDERS_DEBUG === 'true';
const pendingTriggerPairs = new Set(); // pairIndex numbers
const lastTriggerCheckByPairMs = new Map(); // pairIndex -> tsMs

// ============================
// TP/SL triggers for bot trades (OPEN -> CLOSED)
// ============================
const BOT_TP_SL_TRIGGERS_ENABLED = process.env.BOT_TP_SL_TRIGGERS_ENABLED !== 'false';
const TP_SL_TRIGGER_REFRESH_MS = 5_000;
const TP_SL_TRIGGER_DEBOUNCE_MS = 250;
const tpSlTriggerPairs = new Set(); // pairIndex numbers
const lastTpSlCheckByPairMs = new Map(); // pairIndex -> tsMs

async function refreshPendingTriggerPairs() {
    try {
        const result = await query(
            "SELECT DISTINCT pair_index FROM trades WHERE status = 'PENDING' AND trigger_price IS NOT NULL"
        );
        pendingTriggerPairs.clear();
        for (const row of result.rows || []) {
            const idx = Number(row?.pair_index);
            if (Number.isFinite(idx)) pendingTriggerPairs.add(idx);
        }
    } catch (err) {
        console.warn('[triggerOrders] Failed to refresh pending pairs:', err?.message || err);
    }
}

async function refreshTpSlTriggerPairs() {
    if (!BOT_TP_SL_TRIGGERS_ENABLED) {
        tpSlTriggerPairs.clear();
        return;
    }
    try {
        const result = await query(
            `SELECT DISTINCT pair_index
             FROM trades
             WHERE status = 'OPEN'
               AND source = 'BOT'
               AND (stop_loss_price IS NOT NULL OR take_profit_price IS NOT NULL)`
        );
        tpSlTriggerPairs.clear();
        for (const row of result.rows || []) {
            const idx = Number(row?.pair_index);
            if (Number.isFinite(idx)) tpSlTriggerPairs.add(idx);
        }
    } catch (err) {
        console.warn('[tpSlTriggers] Failed to refresh pairs:', err?.message || err);
    }
}

function computePnl({ direction, entryPrice, exitPrice, collateral, leverage }) {
    if (!Number.isFinite(entryPrice) || entryPrice <= 0) return null;
    if (!Number.isFinite(exitPrice) || exitPrice <= 0) return null;
    if (direction !== 'LONG' && direction !== 'SHORT') return null;

    const parsedCollateral = Number.isFinite(collateral) ? collateral : 0;
    const parsedLeverage = Number.isFinite(leverage) ? leverage : 1;
    const positionNotional = parsedCollateral * parsedLeverage;

    if (direction === 'LONG') {
        return ((exitPrice - entryPrice) / entryPrice) * positionNotional;
    }
    return ((entryPrice - exitPrice) / entryPrice) * positionNotional;
}

async function processTriggeredOrdersForTick(pairIndex, price, tsMs) {
    if (process.env.TRIGGER_ORDERS_DEBUG === 'true') {
        console.log(`[triggerOrders] Checking triggers for pairIndex=${pairIndex} at price=${price} tsMs=${tsMs}`);
    }
    if (!pendingTriggerPairs.has(pairIndex)) return;
    if (!Number.isFinite(price)) return;

    const lastMs = lastTriggerCheckByPairMs.get(pairIndex) ?? 0;
    if (tsMs - lastMs < TRIGGERED_ORDER_DEBOUNCE_MS) return;
    lastTriggerCheckByPairMs.set(pairIndex, tsMs);

    let pending;
    try {
        pending = await query(
            "SELECT * FROM trades WHERE status = 'PENDING' AND pair_index = $1 AND trigger_price IS NOT NULL ORDER BY id ASC",
            [pairIndex]
        );
    } catch (err) {
        console.warn('[triggerOrders] Failed to load pending orders:', err?.message || err);
        return;
    }

    if (TRIGGER_ORDERS_DEBUG) {
        const count = pending?.rows?.length || 0;
        if (count > 0) {
            console.log(`[triggerOrders] tick pair=${pairIndex} price=${price} pending=${count}`);
        }
    }

    const nowSec = Math.floor(Date.now() / 1000);
    for (const trade of pending.rows || []) {
        if (process.env.TRIGGER_ORDERS_DEBUG === 'true') {
            console.log(`[triggerOrders] Pending trade: id=${trade.id} dir=${trade.direction} entry=${trade.entry_price} trigger=${trade.trigger_price} status=${trade.status}`);
        }
        const tradeId = Number(trade?.id);
        const triggerPrice = parseFiniteNumber(trade?.trigger_price);
        const referencePrice = parseFiniteNumber(trade?.entry_price);
        const direction = trade?.direction;
        if (!Number.isFinite(tradeId) || triggerPrice === null) continue;

        // Trigger orders support both limit + stop behavior.
        // We decide which one based on where the trigger sits relative to the reference price
        // captured when the order was created (stored in entry_price while status=PENDING).
        let shouldTrigger = false;
        if (direction === 'LONG') {
            if (referencePrice !== null && triggerPrice < referencePrice) {
                // Buy limit
                shouldTrigger = price <= triggerPrice;
            } else {
                // Buy stop (default)
                shouldTrigger = price >= triggerPrice;
            }
        } else if (direction === 'SHORT') {
            if (referencePrice !== null && triggerPrice > referencePrice) {
                // Sell limit
                shouldTrigger = price >= triggerPrice;
            } else {
                // Sell stop (default)
                shouldTrigger = price <= triggerPrice;
            }
        }

        if (!shouldTrigger) continue;

        if (process.env.TRIGGER_ORDERS_DEBUG === 'true') {
            console.log(`[triggerOrders] Trade ${tradeId} should TRIGGER! price=${price} trigger=${triggerPrice} ref=${referencePrice} dir=${direction}`);
        }

        try {
            await query(
                `UPDATE trades
                 SET status = 'OPEN',
                     entry_price = $1,
                     entry_time = $2,
                     triggered_price = $1,
                     triggered_time = $2
                 WHERE id = $3 AND status = 'PENDING'`,
	                [price, nowSec, tradeId]
	            );
                if (TRIGGER_ORDERS_DEBUG) {
                    console.log(`[triggerOrders] Triggered trade ${tradeId} (${direction}) at ${price} (trigger=${triggerPrice}, ref=${referencePrice})`);
                }
	            if (BOT_TP_SL_TRIGGERS_ENABLED && trade?.source === 'BOT') tpSlTriggerPairs.add(pairIndex);
	        } catch (err) {
	            console.warn('[triggerOrders] Failed to trigger order:', err?.message || err);
	        }
            if (process.env.TRIGGER_ORDERS_DEBUG === 'true') {
                const updated = await query('SELECT * FROM trades WHERE id = $1', [tradeId]);
                console.log(`[triggerOrders] After update:`, updated.rows[0]);
            }
	    }

    try {
        const remaining = await query(
            "SELECT 1 FROM trades WHERE status = 'PENDING' AND pair_index = $1 AND trigger_price IS NOT NULL LIMIT 1",
            [pairIndex]
        );
        if (!remaining.rows || remaining.rows.length === 0) {
            pendingTriggerPairs.delete(pairIndex);
        }
    } catch {
        // ignore
    }
}

async function processTpSlTriggersForTick(pairIndex, price, tsMs) {
    if (!BOT_TP_SL_TRIGGERS_ENABLED) return;
    if (!tpSlTriggerPairs.has(pairIndex)) return;
    if (!Number.isFinite(price)) return;

    const lastMs = lastTpSlCheckByPairMs.get(pairIndex) ?? 0;
    if (tsMs - lastMs < TP_SL_TRIGGER_DEBOUNCE_MS) return;
    lastTpSlCheckByPairMs.set(pairIndex, tsMs);

    let open;
    try {
        open = await query(
            `SELECT *
             FROM trades
             WHERE status = 'OPEN'
               AND source = 'BOT'
               AND pair_index = $1
               AND (stop_loss_price IS NOT NULL OR take_profit_price IS NOT NULL)
             ORDER BY id ASC`,
            [pairIndex]
        );
    } catch (err) {
        console.warn('[tpSlTriggers] Failed to load open bot trades:', err?.message || err);
        return;
    }

    const nowSec = Math.floor(Date.now() / 1000);
    for (const trade of open.rows || []) {
        const tradeId = Number(trade?.id);
        if (!Number.isFinite(tradeId)) continue;

        const direction = trade?.direction;
        const stopLossPrice = parseFiniteNumber(trade?.stop_loss_price);
        const takeProfitPrice = parseFiniteNumber(trade?.take_profit_price);

        let hit = null;
        if (direction === 'LONG') {
            if (takeProfitPrice !== null && price >= takeProfitPrice) hit = { type: 'TP', level: takeProfitPrice };
            else if (stopLossPrice !== null && price <= stopLossPrice) hit = { type: 'SL', level: stopLossPrice };
        } else if (direction === 'SHORT') {
            if (takeProfitPrice !== null && price <= takeProfitPrice) hit = { type: 'TP', level: takeProfitPrice };
            else if (stopLossPrice !== null && price >= stopLossPrice) hit = { type: 'SL', level: stopLossPrice };
        }

        if (!hit) continue;

        const entryPrice = parseFiniteNumber(trade?.entry_price);
        const collateral = parseFiniteNumber(trade?.collateral);
        const leverage = parseFiniteNumber(trade?.leverage);
        const pnl = computePnl({
            direction,
            entryPrice,
            exitPrice: price,
            collateral,
            leverage
        });

        if (pnl === null) continue;

        try {
            const updated = await query(
                `UPDATE trades
                 SET exit_price = $1,
                     exit_time = $2,
                     status = 'CLOSED',
                     pnl = $3
                 WHERE id = $4
                   AND status = 'OPEN'
                   AND source = 'BOT'`,
                [price, nowSec, pnl, tradeId]
            );
            if (updated.changes) {
                console.log(`[tpSlTriggers] Closed trade ${tradeId} (${direction}) via ${hit.type} at ${price}`);
                void enqueueTradeCloseReflection(tradeId);
            }
        } catch (err) {
            console.warn('[tpSlTriggers] Failed to close trade:', err?.message || err);
        }
    }

    try {
        const remaining = await query(
            `SELECT 1
             FROM trades
             WHERE status = 'OPEN'
               AND source = 'BOT'
               AND pair_index = $1
               AND (stop_loss_price IS NOT NULL OR take_profit_price IS NOT NULL)
             LIMIT 1`,
            [pairIndex]
        );
        if (!remaining.rows || remaining.rows.length === 0) {
            tpSlTriggerPairs.delete(pairIndex);
        }
    } catch {
        // ignore
    }
}

void refreshPendingTriggerPairs();
setInterval(() => void refreshPendingTriggerPairs(), TRIGGERED_ORDER_REFRESH_MS);

// Safety net: also evaluate pending trigger orders against the last known tick price.
// This reduces the chance of a trigger being missed due to timing gaps.
setInterval(() => {
    for (const pairIndex of pendingTriggerPairs) {
        const last = getLastPrice(pairIndex);
        if (!last) continue;
        void processTriggeredOrdersForTick(pairIndex, last.price, last.tsMs);
    }
}, 1_000);
if (BOT_TP_SL_TRIGGERS_ENABLED) {
    void refreshTpSlTriggerPairs();
    setInterval(() => void refreshTpSlTriggerPairs(), TP_SL_TRIGGER_REFRESH_MS);
}
addPriceObserver((pairIndex, price, tsMs) => {
    void processTriggeredOrdersForTick(pairIndex, price, tsMs);
    void processTpSlTriggersForTick(pairIndex, price, tsMs);
});

// Market data status / diagnostics
app.get('/api/market/status', (req, res) => {
    res.json(marketData.getStatus());
});

// Trigger a backfill + indicator recompute for all pairs/timeframes.
// Useful if the upstream charts API rate-limited the server during startup backfill.
app.post('/api/market/backfill', async (req, res) => {
    try {
        void marketData.triggerBackfill();
        res.status(202).json({ success: true, status: marketData.getStatus() });
    } catch (err) {
        res.status(500).json({ success: false, error: err?.message || String(err) });
    }
});

// Market pairs universe (from Gains trading variables)
app.get('/api/market/pairs', async (req, res) => {
    const limit = Math.min(parseInt(req.query.limit, 10) || 5000, 5000);
    try {
        const result = await query('SELECT * FROM pairs ORDER BY pair_index ASC LIMIT $1', [limit]);
        res.json({ pairs: result.rows });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Top opportunities (heuristic ranking) for AI + UI.
function normalizeNumber(value) {
    const num = typeof value === 'number' ? value : Number(value);
    return Number.isFinite(num) ? num : null;
}

function clampInt(value, { min, max }) {
    if (!Number.isFinite(value)) return null;
    const intValue = Math.trunc(value);
    return Math.max(min, Math.min(max, intValue));
}

function getOpportunityQueryParams(req) {
    // timeframeMin=0 is a special mode: aggregate across all stored timeframes into one per-pair overall score.
    const timeframeMin = clampInt(normalizeNumber(req.query.timeframeMin), { min: 0, max: 60 * 24 }) ?? 15;
    const limit = clampInt(normalizeNumber(req.query.limit), { min: 1, max: 50 }) ?? 10;

    const minOiTotal = normalizeNumber(req.query.minOiTotal)
        ?? (Number.isFinite(Number(process.env.MARKET_MIN_OI_TOTAL)) ? Number(process.env.MARKET_MIN_OI_TOTAL) : 1);

    const maxCostPercent = normalizeNumber(req.query.maxCostPercent)
        ?? (Number.isFinite(Number(process.env.MARKET_MAX_COST_PERCENT)) ? Number(process.env.MARKET_MAX_COST_PERCENT) : 0.25);

    return {
        timeframeMin,
        limit,
        minOiTotal,
        maxCostPercent
    };
}

async function buildOpportunitiesResponse({ timeframeMin, limit, minOiTotal, maxCostPercent }) {
    const isOverall = timeframeMin === 0;

    const result = isOverall
        ? await query(
            `SELECT
                ms.*,
                p.from_symbol,
                p.to_symbol,
                tv.spread_percent,
                tv.fee_position_size_percent,
                tv.fee_oracle_position_size_percent,
                tv.min_position_size_usd,
                tv.group_max_leverage,
                tv.oi_long,
                tv.oi_short,
                tv.oi_skew_percent
            FROM market_state ms
            LEFT JOIN pairs p ON p.pair_index = ms.pair_index
            LEFT JOIN pair_trading_variables tv ON tv.pair_index = ms.pair_index`,
            []
        )
        : await query(
            `SELECT
                ms.*,
                p.from_symbol,
                p.to_symbol,
                tv.spread_percent,
                tv.fee_position_size_percent,
                tv.fee_oracle_position_size_percent,
                tv.min_position_size_usd,
                tv.group_max_leverage,
                tv.oi_long,
                tv.oi_short,
                tv.oi_skew_percent
            FROM market_state ms
            LEFT JOIN pairs p ON p.pair_index = ms.pair_index
            LEFT JOIN pair_trading_variables tv ON tv.pair_index = ms.pair_index
            WHERE ms.timeframe_min = $1`,
            [timeframeMin]
        );

    const ranked = (isOverall
        ? rankTopOverall(result.rows, { minOiTotal, maxCostPercent })
        : rankTop(result.rows, { minOiTotal, maxCostPercent })
    ).slice(0, limit);

    const candidates = ranked.map(item => ({
        pair_index: item.row.pair_index,
        symbol: item.row.from_symbol && item.row.to_symbol ? `${item.row.from_symbol}/${item.row.to_symbol}` : null,
        timeframe_min: timeframeMin,
        best_timeframe_min: isOverall ? (item.bestTimeframeMin ?? null) : undefined,
        candle_time: item.row.candle_time,
        price: item.row.price,
        side: item.side,
        score: item.score,
        reasons: item.reasons,
        timeframes: isOverall ? (item.timeframes ?? null) : undefined,
        indicators: {
            rsi: item.row.rsi,
            macd_histogram: item.row.macd_histogram,
            ema9: item.row.ema9,
            ema21: item.row.ema21,
            bb_upper: item.row.bb_upper,
            bb_middle: item.row.bb_middle,
            bb_lower: item.row.bb_lower,
            atr: item.row.atr,
            stoch_k: item.row.stoch_k,
            overall_bias: item.row.overall_bias
        },
        tradingVariables: {
            spread_percent: item.row.spread_percent ?? null,
            fee_position_size_percent: item.row.fee_position_size_percent ?? null,
            fee_oracle_position_size_percent: item.row.fee_oracle_position_size_percent ?? null,
            oi_long: item.row.oi_long ?? null,
            oi_short: item.row.oi_short ?? null,
            oi_skew_percent: item.row.oi_skew_percent ?? null,
            min_position_size_usd: item.row.min_position_size_usd ?? null,
            group_max_leverage: item.row.group_max_leverage ?? null
        }
    }));

    let scanned = result.rows.length;
    let scannedPairs = null;
    if (isOverall) {
        const uniquePairs = new Set();
        for (const row of result.rows) {
            if (Number.isFinite(row?.pair_index)) uniquePairs.add(row.pair_index);
        }
        scannedPairs = uniquePairs.size;
        scanned = scannedPairs;
    }

    return {
        timeframeMin,
        limit,
        candidates,
        scanned,
        scannedRows: isOverall ? result.rows.length : undefined
    };
}

app.get('/api/market/opportunities', async (req, res) => {
    const params = getOpportunityQueryParams(req);
    try {
        res.json(await buildOpportunitiesResponse(params));
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/market/opportunities/stream', async (req, res) => {
    const params = getOpportunityQueryParams(req);
    const intervalMs = clampInt(normalizeNumber(req.query.intervalMs), { min: 250, max: 10_000 }) ?? 2000;

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders?.();
    res.write('retry: 1000\n\n');

    let closed = false;
    let inFlight = false;
    let lastHash = null;

    const keepAlive = setInterval(() => {
        try {
            res.write(`: ping\n\n`);
        } catch {
            // ignore
        }
    }, 15_000);

    const tick = async () => {
        if (closed || inFlight) return;
        inFlight = true;
        try {
            const payload = await buildOpportunitiesResponse(params);
            const envelope = { ...payload, ts: Date.now() };
            const body = JSON.stringify(envelope);
            const hash = crypto.createHash('sha1').update(body).digest('hex');
            if (hash !== lastHash) {
                lastHash = hash;
                res.write(`data: ${body}\n\n`);
            }
        } catch (err) {
            const message = err?.message || String(err);
            try {
                res.write(`event: server_error\ndata: ${JSON.stringify({ error: message, ts: Date.now() })}\n\n`);
            } catch {
                // ignore
            }
        } finally {
            inFlight = false;
        }
    };

    const interval = setInterval(() => void tick(), intervalMs);
    void tick();

    req.on('close', () => {
        closed = true;
        clearInterval(interval);
        clearInterval(keepAlive);
    });
});

// Fetch OHLC data from Gains API
async function fetchOHLCData(pairIndex, from, to, resolution) {
    const url = `https://backend-pricing.eu.gains.trade/charts/${pairIndex}/${from}/${to}/${resolution}`;
    const maxAttempts = 3;
    let attempt = 0;
    let delayMs = 250;

    const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
    const jitter = () => Math.floor(Math.random() * 150);

    // Avoid log spam when the Gains charts endpoint rate-limits.
    const logKey = (suffix) => `ohlc:${suffix}:${resolution}`;
    fetchOHLCData._lastLogAtMs ??= new Map();
    const warnThrottled = (key, message) => {
        const now = Date.now();
        const last = fetchOHLCData._lastLogAtMs.get(key) ?? 0;
        if (now - last < 30_000) return;
        fetchOHLCData._lastLogAtMs.set(key, now);
        console.warn(message);
    };

    try {
        while (attempt < maxAttempts) {
            attempt += 1;

            try {
                const controller = new AbortController();
                const timeout = setTimeout(() => controller.abort(), 10_000);
                let response;
                try {
                    response = await fetch(url, { signal: controller.signal });
                } finally {
                    clearTimeout(timeout);
                }

                const bodyText = await response.text();

                if (!response.ok) {
                    const status = response.status;
                    const retryable = status === 429 || (status >= 500 && status <= 599);
                    if (retryable && attempt < maxAttempts) {
                        const retryAfter = response.headers.get('retry-after');
                        const retryAfterSec = retryAfter ? Number.parseFloat(retryAfter) : null;
                        const waitMs = typeof retryAfterSec === 'number' && Number.isFinite(retryAfterSec)
                            ? Math.max(0, retryAfterSec * 1000)
                            : delayMs + jitter();
                        await sleep(waitMs);
                        delayMs = Math.min(delayMs * 2, 5_000);
                        continue;
                    }

                    const preview = bodyText ? bodyText.trim().slice(0, 120) : '';
                    warnThrottled(
                        logKey(`http-${status}`),
                        `[ohlc] HTTP ${status} ${response.statusText || ''}${preview ? `: ${preview}` : ''}`
                    );
                    return [];
                }

                let data;
                try {
                    data = bodyText ? JSON.parse(bodyText) : null;
                } catch {
                    const preview = bodyText ? bodyText.trim().slice(0, 120) : '';
                    warnThrottled(
                        logKey('invalid-json'),
                        `[ohlc] Invalid JSON response${preview ? `: ${preview}` : ''}`
                    );
                    return [];
                }

                if (!data?.table) return [];
                return data.table
                    .map(item => ({
                        time: item.time / 1000,
                        open: item.open,
                        high: item.high,
                        low: item.low,
                        close: item.close
                    }))
                    .filter(c =>
                        Number.isFinite(c.time) &&
                        Number.isFinite(c.open) &&
                        Number.isFinite(c.high) &&
                        Number.isFinite(c.low) &&
                        Number.isFinite(c.close)
                    );
            } catch (err) {
                if (attempt < maxAttempts) {
                    await sleep(delayMs + jitter());
                    delayMs = Math.min(delayMs * 2, 5_000);
                    continue;
                }
                throw err;
            }
        }

        return [];
    } catch (err) {
        warnThrottled(logKey('fetch-failed'), `[ohlc] Fetch failed: ${err?.message || String(err)}`);
        return [];
    }
}

// GET all trades
app.get('/api/trades', async (req, res) => {
    try {
        const result = await query('SELECT * FROM trades ORDER BY entry_time DESC');
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// POST new trade (Entry)
app.post('/api/trades', async (req, res) => {
    const {
        pair_index,
        entry_price,
        entry_time,
        collateral,
        leverage,
        direction,
        source,
        stop_loss_price,
        take_profit_price,
        trigger_price
    } = req.body;
    try {
        const parsedPairIndex = parseFiniteInt(pair_index);
        if (!Number.isFinite(parsedPairIndex)) {
            return res.status(400).json({ error: 'Invalid pair_index' });
        }

        const entryPrice = parseFiniteNumber(entry_price);
        const triggerPrice = parseFiniteNumber(trigger_price);
        const hasTrigger = triggerPrice !== null;
        if (hasTrigger) {
            if (!Number.isFinite(entryPrice) || entryPrice <= 0) {
                return res.status(400).json({ error: 'entry_price is required when using trigger_price' });
            }
        } else if (!Number.isFinite(entryPrice) || entryPrice <= 0) {
            return res.status(400).json({ error: 'Invalid entry_price' });
        }

        const parsedCollateral = parseFiniteNumber(collateral);
        const parsedLeverage = parseFiniteNumber(leverage);
        if (!Number.isFinite(parsedCollateral) || parsedCollateral <= 0) {
            return res.status(400).json({ error: 'Invalid collateral' });
        }
        if (!Number.isFinite(parsedLeverage) || parsedLeverage <= 0) {
            return res.status(400).json({ error: 'Invalid leverage' });
        }

        if (direction !== 'LONG' && direction !== 'SHORT') {
            return res.status(400).json({ error: 'Invalid direction' });
        }

        const entryTime = Number.isFinite(Number.parseInt(entry_time, 10))
            ? Number.parseInt(entry_time, 10)
            : Math.floor(Date.now() / 1000);

        const stopLossPrice = parseFiniteNumber(stop_loss_price);
        const takeProfitPrice = parseFiniteNumber(take_profit_price);
        const referenceEntryPrice = hasTrigger ? triggerPrice : entryPrice;
        if (stopLossPrice !== null) {
            if (direction === 'LONG' && stopLossPrice >= referenceEntryPrice) {
                return res.status(400).json({ error: 'Invalid stop_loss_price for LONG' });
            }
            if (direction === 'SHORT' && stopLossPrice <= referenceEntryPrice) {
                return res.status(400).json({ error: 'Invalid stop_loss_price for SHORT' });
            }
        }
        if (takeProfitPrice !== null) {
            if (direction === 'LONG' && takeProfitPrice <= referenceEntryPrice) {
                return res.status(400).json({ error: 'Invalid take_profit_price for LONG' });
            }
            if (direction === 'SHORT' && takeProfitPrice >= referenceEntryPrice) {
                return res.status(400).json({ error: 'Invalid take_profit_price for SHORT' });
            }
        }

        // Best-effort entry-time cost snapshot (for cost-adjusted PnL + evaluation).
        let entryCost = null;
        try {
            const tvRowResult = await query('SELECT * FROM pair_trading_variables WHERE pair_index = $1', [parsedPairIndex]);
            const tvRow = tvRowResult.rows?.[0] ?? null;
            const totalPercent = computeCostPercentFromTradingVariablesRow(tvRow);
            entryCost = tvRow
                ? {
                    source: 'db',
                    updated_at: parseFiniteInt(tvRow.updated_at) ?? null,
                    spread_percent: parseFiniteNumber(tvRow.spread_percent),
                    fee_position_size_percent: parseFiniteNumber(tvRow.fee_position_size_percent),
                    fee_oracle_position_size_percent: parseFiniteNumber(tvRow.fee_oracle_position_size_percent),
                    total_percent: totalPercent
                }
                : null;
        } catch {
            entryCost = null;
        }

        const entryCostSpread = entryCost ? parseFiniteNumber(entryCost.spread_percent) : null;
        const entryCostFee = entryCost ? parseFiniteNumber(entryCost.fee_position_size_percent) : null;
        const entryCostOracleFee = entryCost ? parseFiniteNumber(entryCost.fee_oracle_position_size_percent) : null;
        const entryCostTotal = entryCost ? parseFiniteNumber(entryCost.total_percent) : null;
        const entryCostSource = entryCost ? entryCost.source : null;
        const entryCostUpdatedAt = entryCost ? parseFiniteInt(entryCost.updated_at) : null;
        const entryCostJson = entryCost ? JSON.stringify(entryCost) : null;

        const result = await query(
            `INSERT INTO trades (
                pair_index,
                entry_price,
                entry_time,
                collateral,
                leverage,
                direction,
                source,
                stop_loss_price,
                take_profit_price,
                trigger_price,
                status,
                entry_cost_spread_percent,
                entry_cost_fee_position_size_percent,
                entry_cost_fee_oracle_position_size_percent,
                entry_cost_total_percent,
                entry_cost_source,
                entry_cost_updated_at,
                entry_cost_snapshot_json
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                $12, $13, $14, $15, $16, $17, $18
            )`,
            [
                parsedPairIndex,
                // For trigger orders, store the reference price at creation time in entry_price.
                // When the trigger hits, entry_price will be updated to the triggered price.
                entryPrice,
                entryTime,
                parsedCollateral,
                parsedLeverage,
                direction,
                source || 'MANUAL',
                stopLossPrice,
                takeProfitPrice,
                hasTrigger ? triggerPrice : null,
                hasTrigger ? 'PENDING' : 'OPEN',
                entryCostSpread,
                entryCostFee,
                entryCostOracleFee,
                entryCostTotal,
                entryCostSource,
                entryCostUpdatedAt,
                entryCostJson
            ]
        );
        const newTrade = await query('SELECT * FROM trades WHERE id = $1', [result.lastID]);
        if (hasTrigger) {
            pendingTriggerPairs.add(parsedPairIndex);
            const last = getLastPrice(parsedPairIndex);
            if (last) {
                void processTriggeredOrdersForTick(parsedPairIndex, last.price, last.tsMs);
            }
            // Force another check after a short delay to catch any race conditions
            setTimeout(() => {
                const again = getLastPrice(parsedPairIndex);
                if (again) {
                    void processTriggeredOrdersForTick(parsedPairIndex, again.price, again.tsMs);
                }
            }, 1000);
        }
        res.status(201).json(newTrade.rows[0]);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// PUT cancel pending trade
app.put('/api/trades/:id/cancel', async (req, res) => {
    const { id } = req.params;
    const tradeId = parseFiniteInt(id);
    if (!Number.isFinite(tradeId)) return res.status(400).json({ error: 'Invalid trade id' });

    try {
        const result = await query(
            "UPDATE trades SET status = 'CANCELED' WHERE id = $1 AND status = 'PENDING'",
            [tradeId]
        );
        if (!result.changes) return res.status(404).json({ error: 'Pending trade not found' });
        const updatedTrade = await query('SELECT * FROM trades WHERE id = $1', [tradeId]);
        res.json(updatedTrade.rows[0]);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// PUT close trade (Exit)
app.put('/api/trades/:id/close', async (req, res) => {
    const { id } = req.params;
    const { exit_price, exit_time } = req.body;
    try {
        const currentTradeResult = await query('SELECT * FROM trades WHERE id = $1', [id]);
        if (currentTradeResult.rows.length === 0) return res.status(404).json({ error: 'Trade not found' });

        const trade = currentTradeResult.rows[0];
        if (trade.status && trade.status !== 'OPEN') {
            return res.status(400).json({ error: 'Trade is not open' });
        }

        const entryPrice = parseFloat(trade.entry_price);
        const exitPrice = parseFloat(exit_price);
        if (!Number.isFinite(exitPrice)) {
            return res.status(400).json({ error: 'Invalid exit_price' });
        }

        const exitTime = Number.isFinite(Number.parseInt(exit_time, 10))
            ? Number.parseInt(exit_time, 10)
            : Math.floor(Date.now() / 1000);

        const entryTime = Number.parseInt(trade.entry_time, 10);
        if (Number.isFinite(entryTime) && exitTime < entryTime) {
            return res.status(400).json({ error: 'exit_time is before entry_time' });
        }

        const collateral = parseFloat(trade.collateral) || 0;
        const leverage = parseFloat(trade.leverage) || 1;
        const direction = trade.direction;

        let pnl = 0;
        if (direction === 'LONG') {
            pnl = ((exitPrice - entryPrice) / entryPrice) * (collateral * leverage);
        } else {
            pnl = ((entryPrice - exitPrice) / entryPrice) * (collateral * leverage);
        }

        await query(
            'UPDATE trades SET exit_price = $1, exit_time = $2, status = $3, pnl = $4 WHERE id = $5',
            [exitPrice, exitTime, 'CLOSED', pnl, id]
        );

        const updatedTrade = await query('SELECT * FROM trades WHERE id = $1', [id]);
        if (updatedTrade.rows?.[0]?.source === 'BOT') {
            void enqueueTradeCloseReflection(Number.parseInt(id, 10));
        }
        res.json(updatedTrade.rows[0]);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// GET indicators for a pair
app.get('/api/indicators/:pairIndex', async (req, res) => {
    const { pairIndex } = req.params;
    const parsedPairIndex = parseFiniteInt(pairIndex);
    if (!Number.isFinite(parsedPairIndex)) {
        return res.status(400).json({ error: 'Invalid pairIndex' });
    }

    const resolution = '15'; // 15-minute candles
    const timeframeMin = 15;
    const nowSec = Math.floor(Date.now() / 1000);

    const parseRowNum = (value) => {
        const num = typeof value === 'number' ? value : Number.parseFloat(value);
        return Number.isFinite(num) ? num : null;
    };

    const buildLatestFromMarketState = (row) => {
        if (!row) return null;
        const price = parseRowNum(row.price);
        if (price === null) return null;

        const macd = parseRowNum(row.macd);
        const macdSignal = parseRowNum(row.macd_signal);
        const macdHistogram = parseRowNum(row.macd_histogram);

        const bbUpper = parseRowNum(row.bb_upper);
        const bbMiddle = parseRowNum(row.bb_middle);
        const bbLower = parseRowNum(row.bb_lower);

        const stochK = parseRowNum(row.stoch_k);
        const stochD = parseRowNum(row.stoch_d);

        return {
            price,
            rsi: parseRowNum(row.rsi),
            macd: macd !== null || macdSignal !== null || macdHistogram !== null
                ? { MACD: macd, signal: macdSignal, histogram: macdHistogram }
                : null,
            bollingerBands: bbUpper !== null || bbMiddle !== null || bbLower !== null
                ? { upper: bbUpper, middle: bbMiddle, lower: bbLower }
                : null,
            ema: {
                ema9: parseRowNum(row.ema9),
                ema21: parseRowNum(row.ema21),
                ema50: parseRowNum(row.ema50),
                ema200: parseRowNum(row.ema200)
            },
            sma: {
                sma20: parseRowNum(row.sma20),
                sma50: parseRowNum(row.sma50),
                sma200: parseRowNum(row.sma200)
            },
            atr: parseRowNum(row.atr),
            stochastic: stochK !== null || stochD !== null
                ? { k: stochK, d: stochD }
                : null
        };
    };

    try {
        // Prefer DB-cached indicators from market_state (fast, avoids external OHLC fetches).
        const marketStateResult = await query(
            'SELECT * FROM market_state WHERE pair_index = $1 AND timeframe_min = $2',
            [parsedPairIndex, timeframeMin]
        );
        const marketStateRow = marketStateResult.rows?.[0] ?? null;
        const latestFromDb = buildLatestFromMarketState(marketStateRow);
        if (latestFromDb) {
            const indicators = { latest: latestFromDb, history: {} };
            const summary = generateMarketSummary(indicators, latestFromDb.price);
            return res.json({ indicators: latestFromDb, summary, price: latestFromDb.price, source: 'market_state', updatedAt: marketStateRow?.updated_at ?? null });
        }

        // Fallback: compute from live OHLC fetch (slower; may be rate-limited).
        const to = nowSec;
        const from = to - (24 * 60 * 60); // Last 24 hours
        const ohlcData = await fetchOHLCData(parsedPairIndex, from, to, resolution);
        if (ohlcData.length < 50) {
            return res.status(503).json({ error: 'Indicators warming up (market_state not ready yet). Try again in ~1-2 minutes.' });
        }

        const indicators = calculateIndicators(ohlcData);
        const currentPrice = ohlcData[ohlcData.length - 1].close;
        const summary = generateMarketSummary(indicators, currentPrice);

        // Store snapshot in database
        const { latest } = indicators;
        await query(
            `INSERT INTO market_snapshots (
                pair_index, timestamp, price, rsi, macd, macd_signal, macd_histogram,
                bb_upper, bb_middle, bb_lower, ema9, ema21, ema50, ema200,
                sma20, sma50, sma200, atr, stoch_k, stoch_d, overall_bias
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)`,
            [
                pairIndex, Math.floor(Date.now() / 1000), currentPrice,
                latest.rsi, latest.macd?.MACD, latest.macd?.signal, latest.macd?.histogram,
                latest.bollingerBands?.upper, latest.bollingerBands?.middle, latest.bollingerBands?.lower,
                latest.ema?.ema9, latest.ema?.ema21, latest.ema?.ema50, latest.ema?.ema200,
                latest.sma?.sma20, latest.sma?.sma50, latest.sma?.sma200,
                latest.atr, latest.stochastic?.k, latest.stochastic?.d,
                summary.overallBias
            ]
        );

        res.json({ indicators: latest, summary, price: currentPrice, source: 'ohlc' });
    } catch (err) {
        console.error('Indicator calculation error:', err);
        res.status(500).json({ error: err.message });
    }
});

// GET market snapshots history
app.get('/api/snapshots/:pairIndex', async (req, res) => {
    const { pairIndex } = req.params;
    const limit = parseInt(req.query.limit) || 100;
    try {
        const result = await query(
            'SELECT * FROM market_snapshots WHERE pair_index = $1 ORDER BY timestamp DESC LIMIT $2',
            [pairIndex, limit]
        );
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ==================== AI BOT ENDPOINTS ====================
const aiBot = require('./aiBot');

function parseFiniteNumber(value) {
    const parsed = typeof value === 'number' ? value : Number.parseFloat(value);
    return Number.isFinite(parsed) ? parsed : null;
}

function parseFiniteInt(value) {
    const parsed = typeof value === 'number' ? value : Number.parseInt(value, 10);
    return Number.isFinite(parsed) ? parsed : null;
}

function startOfLocalDayUnixSec() {
    const now = new Date();
    now.setHours(0, 0, 0, 0);
    return Math.floor(now.getTime() / 1000);
}

function sumOrNull(...values) {
    const nums = values
        .map(v => (typeof v === 'number' && Number.isFinite(v) ? v : null))
        .filter(v => v !== null);
    if (nums.length === 0) return null;
    return nums.reduce((acc, v) => acc + v, 0);
}

function computeCostPercentFromTradingVariablesRow(row) {
    if (!row) return null;
    return sumOrNull(
        parseFiniteNumber(row.spread_percent),
        parseFiniteNumber(row.fee_position_size_percent),
        parseFiniteNumber(row.fee_oracle_position_size_percent)
    );
}

function computeOiTotalFromTradingVariablesRow(row) {
    if (!row) return null;
    const oiLong = parseFiniteNumber(row.oi_long);
    const oiShort = parseFiniteNumber(row.oi_short);
    if (oiLong === null || oiShort === null) return null;
    return oiLong + oiShort;
}

function computeCostPercentFromTradingVariablesForPair(tvForPair) {
    if (!tvForPair) return null;
    return sumOrNull(
        parseFiniteNumber(tvForPair.pair?.spreadPercent),
        parseFiniteNumber(tvForPair.fees?.positionSizeFeePercent),
        parseFiniteNumber(tvForPair.fees?.oraclePositionSizeFeePercent)
    );
}

function computeOiTotalFromTradingVariablesForPair(tvForPair) {
    if (!tvForPair) return null;
    const oiLong = parseFiniteNumber(tvForPair.openInterest?.long);
    const oiShort = parseFiniteNumber(tvForPair.openInterest?.short);
    if (oiLong === null || oiShort === null) return null;
    return oiLong + oiShort;
}

function resolveExecutionThresholds(timeframeMin = 15) {
    const minOiTotal =
        parseFiniteNumber(process.env.BOT_MIN_OI_TOTAL) ??
        parseFiniteNumber(process.env.MARKET_MIN_OI_TOTAL) ??
        1;

    const maxCostPercent =
        parseFiniteNumber(process.env.BOT_MAX_COST_PERCENT) ??
        parseFiniteNumber(process.env.MARKET_MAX_COST_PERCENT) ??
        0.25;

    const maxOpenPositions =
        parseFiniteInt(process.env.BOT_MAX_OPEN_POSITIONS) ??
        5;

    const allowMultiplePositionsPerPair =
        process.env.BOT_ALLOW_MULTIPLE_POSITIONS_PER_PAIR === 'true';

    const maxDecisionAgeSec =
        parseFiniteInt(process.env.BOT_MAX_DECISION_AGE_SEC) ??
        10 * 60;

    const maxMarketStateAgeSec =
        parseFiniteInt(process.env.BOT_MAX_MARKET_STATE_AGE_SEC) ??
        timeframeMin * 60 * 2;

    const maxTradingVariablesAgeSec =
        parseFiniteInt(process.env.BOT_MAX_TRADING_VARIABLES_AGE_SEC) ??
        5 * 60;

    const minConfidence =
        parseFiniteNumber(process.env.BOT_MIN_CONFIDENCE) ??
        0.7;

    const override = activeExecutionThresholdParams;
    const overrideMinOiTotal = parseFiniteNumber(override?.minOiTotal);
    const overrideMaxCostPercent = parseFiniteNumber(override?.maxCostPercent);
    const overrideMaxOpenPositions = parseFiniteInt(override?.maxOpenPositions);
    const overrideAllowMultiplePositionsPerPair = typeof override?.allowMultiplePositionsPerPair === 'boolean'
        ? override.allowMultiplePositionsPerPair
        : null;
    const overrideMaxDecisionAgeSec = parseFiniteInt(override?.maxDecisionAgeSec);
    const overrideMaxTradingVariablesAgeSec = parseFiniteInt(override?.maxTradingVariablesAgeSec);
    const overrideMinConfidence = parseFiniteNumber(override?.minConfidence);

    const overrideMaxMarketStateAgeSecDirect = parseFiniteInt(override?.maxMarketStateAgeSec);
    const overrideMaxMarketStateAgeMultiplier = parseFiniteNumber(override?.maxMarketStateAgeMultiplier);
    const maxMarketStateAgeSecWithOverride =
        overrideMaxMarketStateAgeSecDirect ??
        (overrideMaxMarketStateAgeMultiplier !== null
            ? Math.floor(timeframeMin * 60 * overrideMaxMarketStateAgeMultiplier)
            : maxMarketStateAgeSec);

    return {
        timeframeMin,
        minOiTotal: overrideMinOiTotal ?? minOiTotal,
        maxCostPercent: overrideMaxCostPercent ?? maxCostPercent,
        maxOpenPositions: overrideMaxOpenPositions ?? maxOpenPositions,
        allowMultiplePositionsPerPair: overrideAllowMultiplePositionsPerPair ?? allowMultiplePositionsPerPair,
        maxDecisionAgeSec: overrideMaxDecisionAgeSec ?? maxDecisionAgeSec,
        maxMarketStateAgeSec: maxMarketStateAgeSecWithOverride,
        maxTradingVariablesAgeSec: overrideMaxTradingVariablesAgeSec ?? maxTradingVariablesAgeSec,
        minConfidence: overrideMinConfidence ?? minConfidence,
        thresholdsVersion: activeExecutionThresholdVersion
    };
}

async function mapWithConcurrency(items, concurrency, handler) {
    if (!Array.isArray(items) || items.length === 0) return [];
    const safeConcurrency = Math.max(1, Math.min(concurrency || 1, 8));

    let cursor = 0;
    const results = new Array(items.length);
    const workers = Array.from({ length: safeConcurrency }, async () => {
        while (cursor < items.length) {
            const idx = cursor++;
            try {
                results[idx] = await handler(items[idx], idx);
            } catch (err) {
                results[idx] = { success: false, error: err?.message || String(err) };
            }
        }
    });

    await Promise.all(workers);
    return results;
}

async function getBotDailyStatsFromDb() {
    const startOfDaySec = startOfLocalDayUnixSec();

    const pnlResult = await query(
        `SELECT COALESCE(SUM(pnl), 0) AS pnl
         FROM trades
         WHERE source = 'BOT'
           AND status = 'CLOSED'
           AND exit_time >= $1`,
        [startOfDaySec]
    );

    const tradesResult = await query(
        `SELECT COUNT(*) AS count
         FROM trades
         WHERE source = 'BOT'
           AND entry_time >= $1`,
        [startOfDaySec]
    );

    const todayPnLRaw = pnlResult.rows?.[0]?.pnl;
    const todayPnL = parseFiniteNumber(todayPnLRaw) ?? 0;

    const tradesExecutedRaw = tradesResult.rows?.[0]?.count;
    const tradesExecuted = parseFiniteInt(tradesExecutedRaw) ?? 0;

    return {
        todayPnL,
        tradesExecuted,
        startOfDaySec
    };
}

function clampFloat(value, { min, max }) {
    if (!Number.isFinite(value)) return null;
    return Math.max(min, Math.min(max, value));
}

function resolveAutotradeConfig() {
    const enabled = false;
    const intervalSec = clampInt(parseFiniteInt(process.env.BOT_AUTOTRADE_INTERVAL_SEC), { min: 3, max: 3600 }) ?? 15;
    const minScore = clampFloat(parseFiniteNumber(process.env.BOT_AUTOTRADE_MIN_SCORE), { min: 0, max: 100 }) ?? 70;
    const minLlmIntervalSec = clampInt(parseFiniteInt(process.env.BOT_AUTOTRADE_MIN_LLM_INTERVAL_SEC), { min: 10, max: 24 * 3600 }) ?? 300;

    const timeframeMin = clampInt(parseFiniteInt(process.env.BOT_AUTOTRADE_TIMEFRAME_MIN), { min: 1, max: 240 }) ?? 15;
    const regimeTimeframeMin = clampInt(parseFiniteInt(process.env.BOT_AUTOTRADE_REGIME_TIMEFRAME_MIN), { min: 1, max: 1440 }) ?? 60;

    const lookbackSec = clampInt(parseFiniteInt(process.env.BOT_AUTOTRADE_LOOKBACK_SEC), { min: 6 * 3600, max: 90 * 24 * 3600 }) ?? 7 * 24 * 3600;
    const regimeLookbackSec = clampInt(parseFiniteInt(process.env.BOT_AUTOTRADE_REGIME_LOOKBACK_SEC), { min: 2 * 24 * 3600, max: 180 * 24 * 3600 }) ?? 14 * 24 * 3600;

    return {
        enabled,
        intervalSec,
        minScore,
        minLlmIntervalSec,
        timeframeMin,
        regimeTimeframeMin,
        lookbackSec,
        regimeLookbackSec
    };
}

let autotradeConfig = resolveAutotradeConfig();
const autotradeState = {
    enabled: autotradeConfig.enabled,
    isTicking: false,
    timer: null,
    nextIntent: 'open',
    lastTickAtMs: null,
    lastScan: null,
    lastLlmAtMs: null,
    lastExecution: null,
    lastError: null
};

function getAutotradeStatus() {
    return {
        enabled: autotradeState.enabled,
        config: autotradeConfig,
        state: {
            isTicking: autotradeState.isTicking,
            nextIntent: autotradeState.nextIntent,
            lastTickAtMs: autotradeState.lastTickAtMs,
            lastLlmAtMs: autotradeState.lastLlmAtMs,
            lastScan: autotradeState.lastScan,
            lastExecution: autotradeState.lastExecution,
            lastError: autotradeState.lastError
        }
    };
}

function scheduleAutotradeTick({ immediate = false } = {}) {
    if (!autotradeState.enabled) return;
    if (autotradeState.timer) return;
    const delayMs = immediate ? 0 : autotradeConfig.intervalSec * 1000;
    autotradeState.timer = setTimeout(() => {
        autotradeState.timer = null;
        void runAutotradeTick();
    }, delayMs);
}

function stopAutotradeWorker() {
    autotradeState.enabled = false;
    autotradeConfig = { ...autotradeConfig, enabled: false };
    autotradeState.lastError = null;
    if (autotradeState.timer) {
        clearTimeout(autotradeState.timer);
        autotradeState.timer = null;
    }
}

function startAutotradeWorker() {
    autotradeState.enabled = true;
    autotradeConfig = { ...autotradeConfig, enabled: true };
    autotradeState.lastError = null;
    scheduleAutotradeTick({ immediate: true });
}

function updateAutotradeConfig(partial = {}) {
    const next = { ...autotradeConfig };

    if (partial.intervalSec !== undefined) {
        const parsed = clampInt(parseFiniteInt(partial.intervalSec), { min: 3, max: 3600 });
        if (parsed !== null) next.intervalSec = parsed;
    }
    if (partial.minScore !== undefined) {
        const parsed = clampFloat(parseFiniteNumber(partial.minScore), { min: 0, max: 100 });
        if (parsed !== null) next.minScore = parsed;
    }
    if (partial.minLlmIntervalSec !== undefined) {
        const parsed = clampInt(parseFiniteInt(partial.minLlmIntervalSec), { min: 10, max: 24 * 3600 });
        if (parsed !== null) next.minLlmIntervalSec = parsed;
    }
    if (partial.timeframeMin !== undefined) {
        const parsed = clampInt(parseFiniteInt(partial.timeframeMin), { min: 1, max: 240 });
        if (parsed !== null) next.timeframeMin = parsed;
    }
    if (partial.regimeTimeframeMin !== undefined) {
        const parsed = clampInt(parseFiniteInt(partial.regimeTimeframeMin), { min: 1, max: 1440 });
        if (parsed !== null) next.regimeTimeframeMin = parsed;
    }

    autotradeConfig = next;

    // Reschedule with the updated interval.
    if (autotradeState.timer) {
        clearTimeout(autotradeState.timer);
        autotradeState.timer = null;
    }
    scheduleAutotradeTick({ immediate: false });

    return getAutotradeStatus();
}

async function getRegimeBias(pairIndex, timeframeMin, lookbackSec) {
    const to = Math.floor(Date.now() / 1000);
    const from = to - lookbackSec;
    const ohlcData = await fetchOHLCData(pairIndex, from, to, String(timeframeMin));
    if (ohlcData.length < 50) {
        return { success: false, error: 'Insufficient OHLC data for regime', timeframeMin };
    }
    const indicators = calculateIndicators(ohlcData);
    if (!indicators?.latest) {
        return { success: false, error: 'Failed to compute regime indicators', timeframeMin };
    }
    const currentPrice = ohlcData[ohlcData.length - 1].close;
    const summary = generateMarketSummary(indicators, currentPrice);
    return { success: true, timeframeMin, overallBias: summary.overallBias };
}

function computeNotionalUsd(tradeRow) {
    const collateral = parseFiniteNumber(tradeRow?.collateral);
    const leverage = parseFiniteNumber(tradeRow?.leverage);
    if (!Number.isFinite(collateral) || !Number.isFinite(leverage)) return null;
    return collateral * leverage;
}

function pickPositionToManage(openBotPositions) {
    if (!Array.isArray(openBotPositions) || openBotPositions.length === 0) return null;

    const scored = openBotPositions.map((row) => ({
        row,
        notional: computeNotionalUsd(row) ?? 0,
        entryTime: parseFiniteInt(row?.entry_time) ?? 0
    }));

    scored.sort((a, b) => {
        if (b.notional !== a.notional) return b.notional - a.notional;
        return a.entryTime - b.entryTime;
    });

    return scored[0]?.row ?? null;
}

async function executeBotDecision({ pairIndex, action, args, currentPrice, decisionId, timeframeMin = 15, executionMode = 'manual' }) {
    const parsedPairIndex = parseFiniteInt(pairIndex);
    const thresholds = resolveExecutionThresholds(timeframeMin);

    const botStatus = aiBot.getBotStatus();
    if (!botStatus.isActive) {
        return { status: 400, payload: { success: false, error: 'Bot is not active' } };
    }

    if (!Number.isFinite(parsedPairIndex)) {
        return { status: 400, payload: { success: false, error: 'Invalid pairIndex' } };
    }

    const parsedDecisionId = parseFiniteInt(decisionId);
    if (!Number.isFinite(parsedDecisionId)) {
        return { status: 400, payload: { success: false, error: 'Missing or invalid decisionId' } };
    }

    const decisionResult = await query('SELECT * FROM bot_decisions WHERE id = $1', [parsedDecisionId]);
    const decisionRow = decisionResult.rows?.[0] ?? null;
    if (!decisionRow) {
        return { status: 400, payload: { success: false, error: 'Decision not found' } };
    }

    if (parseFiniteInt(decisionRow.pair_index) !== parsedPairIndex) {
        return { status: 400, payload: { success: false, error: 'Decision pair mismatch' } };
    }

    if (decisionRow.action && decisionRow.action !== action) {
        return { status: 400, payload: { success: false, error: 'Decision action mismatch' } };
    }

    if (decisionRow.trade_id) {
        return { status: 400, payload: { success: false, error: 'Decision already executed' } };
    }

    const decisionAgeSec = Math.floor(Date.now() / 1000) - parseFiniteInt(decisionRow.timestamp);
    if (Number.isFinite(decisionAgeSec) && decisionAgeSec > thresholds.maxDecisionAgeSec) {
        return { status: 400, payload: { success: false, error: 'Decision is too old; re-run analysis' } };
    }

    const dailyStats = await getBotDailyStatsFromDb();
    if (dailyStats.todayPnL < -botStatus.safetyLimits.dailyLossLimit) {
        return { status: 400, payload: { success: false, error: 'Daily loss limit reached. Bot paused.' } };
    }

    let executionPrice = parseFiniteNumber(currentPrice);
    if (!Number.isFinite(executionPrice)) {
        // Fall back to latest candle close if the caller didn't pass a price
        const to = Math.floor(Date.now() / 1000);
        const from = to - (24 * 60 * 60);
        const ohlcData = await fetchOHLCData(parsedPairIndex, from, to, String(timeframeMin));
        if (ohlcData.length === 0) {
            return { status: 400, payload: { success: false, error: 'Unable to determine current price' } };
        }
        executionPrice = ohlcData[ohlcData.length - 1].close;
    }

    if (action === 'execute_trade') {
        const { direction, collateral, leverage, stop_loss_price, take_profit_price, trigger_price, confidence } = args || {};
        if (direction !== 'LONG' && direction !== 'SHORT') {
            return { status: 400, payload: { success: false, error: 'Invalid trade direction' } };
        }

        const parsedCollateral = parseFiniteNumber(collateral);
        const parsedLeverage = parseFiniteNumber(leverage);
        if (!Number.isFinite(parsedCollateral) || parsedCollateral <= 0) {
            return { status: 400, payload: { success: false, error: 'Invalid collateral' } };
        }
        if (!Number.isFinite(parsedLeverage) || parsedLeverage <= 0) {
            return { status: 400, payload: { success: false, error: 'Invalid leverage' } };
        }

        const parsedConfidence =
            parseFiniteNumber(confidence) ??
            parseFiniteNumber(decisionRow?.confidence);

        const minConfidence = parseFiniteNumber(thresholds.minConfidence) ?? 0.7;
        if (parsedConfidence !== null && parsedConfidence < minConfidence) {
            return { status: 400, payload: { success: false, error: `Confidence below threshold (${minConfidence})` } };
        }

        const stopLossPrice = parseFiniteNumber(stop_loss_price);
        const takeProfitPrice = parseFiniteNumber(take_profit_price);
        if (!Number.isFinite(stopLossPrice) || !Number.isFinite(takeProfitPrice)) {
            return { status: 400, payload: { success: false, error: 'Missing stop_loss_price or take_profit_price' } };
        }

	        const triggerPrice = parseFiniteNumber(trigger_price);
	        if (triggerPrice !== null) {
	            const validTrigger =
	                direction === 'LONG'
	                    ? triggerPrice >= executionPrice
	                    : triggerPrice <= executionPrice;
            if (!validTrigger) {
                return {
                    status: 400,
                    payload: {
                        success: false,
                        error: direction === 'LONG'
                            ? 'For LONG trigger orders, trigger_price must be >= current price'
                            : 'For SHORT trigger orders, trigger_price must be <= current price'
                    }
                };
	            }
	        }

	        const referenceEntryPrice = triggerPrice !== null ? triggerPrice : executionPrice;

	        if (direction === 'LONG') {
	            if (stopLossPrice >= referenceEntryPrice) {
	                return { status: 400, payload: { success: false, error: 'Invalid stop_loss_price for LONG' } };
	            }
	            if (takeProfitPrice <= referenceEntryPrice) {
	                return { status: 400, payload: { success: false, error: 'Invalid take_profit_price for LONG' } };
	            }
	        } else {
	            if (stopLossPrice <= referenceEntryPrice) {
	                return { status: 400, payload: { success: false, error: 'Invalid stop_loss_price for SHORT' } };
	            }
	            if (takeProfitPrice >= referenceEntryPrice) {
	                return { status: 400, payload: { success: false, error: 'Invalid take_profit_price for SHORT' } };
	            }
	        }

        // Max open positions applies only to bot trades.
        const openBotPositionsResult = await query("SELECT * FROM trades WHERE status IN ('OPEN', 'PENDING') AND source = 'BOT'");
        const openBotPositionsAll = openBotPositionsResult.rows || [];
        if (openBotPositionsAll.length >= thresholds.maxOpenPositions) {
            return { status: 400, payload: { success: false, error: `Max open positions reached (${thresholds.maxOpenPositions})` } };
        }

        // Still avoid any duplicate positions on the same pair (including manual ones).
        const openPositionsForPairResult = await query("SELECT * FROM trades WHERE status IN ('OPEN', 'PENDING') AND pair_index = $1", [parsedPairIndex]);
        const openPositionsForPair = openPositionsForPairResult.rows || [];
        if (openPositionsForPair.length > 0 && !thresholds.allowMultiplePositionsPerPair) {
            return { status: 400, payload: { success: false, error: 'Open position already exists for this pair' } };
        }

        const nowSec = Math.floor(Date.now() / 1000);

        const marketStateResult = await query(
            'SELECT * FROM market_state WHERE pair_index = $1 AND timeframe_min = $2',
            [parsedPairIndex, timeframeMin]
        );
        const marketStateRow = marketStateResult.rows?.[0] ?? null;
        let marketFreshEnough = false;
        if (marketStateRow?.updated_at) {
            const marketAgeSec = nowSec - parseFiniteInt(marketStateRow.updated_at);
            if (Number.isFinite(marketAgeSec) && marketAgeSec <= thresholds.maxMarketStateAgeSec) {
                marketFreshEnough = true;
            }
        }

        const tvResult = await query(
            'SELECT * FROM pair_trading_variables WHERE pair_index = $1',
            [parsedPairIndex]
        );
        const tvRow = tvResult.rows?.[0] ?? null;

        let tvConstraintsSource = 'db';
        let tvForPair = null;

        let tvFreshEnough = false;
        if (tvRow?.updated_at) {
            const tvAgeSec = nowSec - parseFiniteInt(tvRow.updated_at);
            if (Number.isFinite(tvAgeSec) && tvAgeSec <= thresholds.maxTradingVariablesAgeSec) {
                tvFreshEnough = true;
            }
        }

        if (!marketFreshEnough) {
            const to = Math.floor(Date.now() / 1000);
            const from = to - (24 * 60 * 60);
            const ohlcData = await fetchOHLCData(parsedPairIndex, from, to, String(timeframeMin));
            if (ohlcData.length === 0) {
                return { status: 400, payload: { success: false, error: 'Market data unavailable; try again' } };
            }
            const lastCandleTime = parseFiniteInt(ohlcData[ohlcData.length - 1]?.time);
            if (!Number.isFinite(lastCandleTime) || nowSec - lastCandleTime > thresholds.maxMarketStateAgeSec) {
                return { status: 400, payload: { success: false, error: 'Market data is stale; wait for refresh' } };
            }
            marketFreshEnough = true;
        }

        if (!tvFreshEnough) {
            try {
                const tradingVariables = await fetchTradingVariablesCached();
                tvForPair = buildTradingVariablesForPair(tradingVariables, parsedPairIndex);
                if (!tvForPair) {
                    return { status: 400, payload: { success: false, error: 'Trading variables unavailable; try again' } };
                }
                tvConstraintsSource = 'api';
                tvFreshEnough = true;
            } catch (err) {
                return { status: 400, payload: { success: false, error: 'Trading variables unavailable; try again' } };
            }
        }

        const adjustments = [];
        let effectiveCollateral = parsedCollateral;
        let effectiveLeverage = parsedLeverage;

        if (effectiveCollateral > botStatus.safetyLimits.maxCollateral) {
            effectiveCollateral = botStatus.safetyLimits.maxCollateral;
            adjustments.push({ field: 'collateral', applied: effectiveCollateral });
        }
        if (effectiveLeverage > botStatus.safetyLimits.maxLeverage) {
            effectiveLeverage = botStatus.safetyLimits.maxLeverage;
            adjustments.push({ field: 'leverage', applied: effectiveLeverage });
        }

        const groupMaxLeverage = tvRow && tvFreshEnough && tvConstraintsSource === 'db'
            ? parseFiniteNumber(tvRow.group_max_leverage)
            : parseFiniteNumber(tvForPair?.group?.maxLeverage);
        if (groupMaxLeverage !== null && effectiveLeverage > groupMaxLeverage) {
            effectiveLeverage = groupMaxLeverage;
            adjustments.push({ field: 'leverage', applied: effectiveLeverage, reason: 'group_max_leverage' });
        }

        const costPercent = tvRow && tvFreshEnough && tvConstraintsSource === 'db'
            ? computeCostPercentFromTradingVariablesRow(tvRow)
            : computeCostPercentFromTradingVariablesForPair(tvForPair);
        if (costPercent !== null && costPercent > thresholds.maxCostPercent) {
            return { status: 400, payload: { success: false, error: `Costs too high (~${costPercent.toFixed(4)}%)` } };
        }

        const entryCostSnapshot = (() => {
            if (tvRow && tvFreshEnough && tvConstraintsSource === 'db') {
                return {
                    source: 'db',
                    updated_at: parseFiniteInt(tvRow.updated_at) ?? null,
                    spread_percent: parseFiniteNumber(tvRow.spread_percent),
                    fee_position_size_percent: parseFiniteNumber(tvRow.fee_position_size_percent),
                    fee_oracle_position_size_percent: parseFiniteNumber(tvRow.fee_oracle_position_size_percent),
                    total_percent: costPercent
                };
            }

            if (tvForPair && tvConstraintsSource === 'api') {
                return {
                    source: 'api',
                    updated_at: nowSec,
                    spread_percent: parseFiniteNumber(tvForPair.pair?.spreadPercent),
                    fee_position_size_percent: parseFiniteNumber(tvForPair.fees?.positionSizeFeePercent),
                    fee_oracle_position_size_percent: parseFiniteNumber(tvForPair.fees?.oraclePositionSizeFeePercent),
                    total_percent: costPercent,
                    refreshId: tvForPair.source?.refreshId ?? null,
                    lastRefreshed: tvForPair.source?.lastRefreshed ?? null
                };
            }

            return null;
        })();

        const oiTotal = tvRow && tvFreshEnough && tvConstraintsSource === 'db'
            ? computeOiTotalFromTradingVariablesRow(tvRow)
            : computeOiTotalFromTradingVariablesForPair(tvForPair);
        if (oiTotal !== null && oiTotal < thresholds.minOiTotal) {
            return { status: 400, payload: { success: false, error: `Open interest too low (~${oiTotal.toFixed(0)})` } };
        }

        const minPositionSizeUsd = tvRow && tvFreshEnough && tvConstraintsSource === 'db'
            ? parseFiniteNumber(tvRow.min_position_size_usd)
            : parseFiniteNumber(tvForPair?.fees?.minPositionSizeUsd);
        const notionalUsd = effectiveCollateral * effectiveLeverage;
        if (minPositionSizeUsd !== null && notionalUsd < minPositionSizeUsd) {
            return { status: 400, payload: { success: false, error: `Position size too small ($${notionalUsd.toFixed(2)} < $${minPositionSizeUsd.toFixed(0)} min)` } };
        }

        const tradeResult = await aiBot.executeBotTrade(
            parsedPairIndex,
            direction,
            effectiveCollateral,
            effectiveLeverage,
            executionPrice,
            stopLossPrice,
            takeProfitPrice,
            triggerPrice,
            entryCostSnapshot
        );

	        if (tradeResult?.tradeId) {
	            await query('UPDATE bot_decisions SET trade_id = $1 WHERE id = $2', [tradeResult.tradeId, parsedDecisionId]);
	        }
	        if (BOT_TP_SL_TRIGGERS_ENABLED && tradeResult?.status === 'OPEN') tpSlTriggerPairs.add(parsedPairIndex);
	        if (tradeResult?.status === 'PENDING') pendingTriggerPairs.add(parsedPairIndex);

	        return {
	            status: 200,
            payload: {
                success: true,
                action,
                tradeExecuted: tradeResult,
                adjustments,
                constraintsSource: tvConstraintsSource,
                executionMode
            }
        };
    }

    if (action === 'close_position') {
        const tradeId = parseFiniteInt(args?.trade_id);
        if (!Number.isFinite(tradeId)) {
            return { status: 400, payload: { success: false, error: 'Invalid trade_id' } };
        }

        const tradeLookup = await query('SELECT * FROM trades WHERE id = $1', [tradeId]);
        const tradeRow = tradeLookup.rows?.[0] ?? null;
        if (!tradeRow) {
            return { status: 400, payload: { success: false, error: 'Trade not found' } };
        }
        if (tradeRow.status && tradeRow.status !== 'OPEN') {
            return { status: 400, payload: { success: false, error: 'Trade is not open' } };
        }
        if (parseFiniteInt(tradeRow.pair_index) !== parsedPairIndex) {
            return { status: 400, payload: { success: false, error: 'Trade pair mismatch' } };
        }
        if (executionMode === 'autotrade' && tradeRow.source !== 'BOT') {
            return { status: 400, payload: { success: false, error: 'Autotrade cannot close non-bot trades' } };
        }

        const closeResult = await aiBot.closeBotPosition(tradeId, executionPrice);

        if (closeResult?.success) {
            await query('UPDATE bot_decisions SET trade_id = $1 WHERE id = $2', [tradeId, parsedDecisionId]);
            void enqueueTradeCloseReflection(tradeId, { timeframeMin });
        }

        return { status: 200, payload: { success: true, action, positionClosed: closeResult, executionMode } };
    }

    if (action === 'cancel_pending') {
        const tradeId = parseFiniteInt(args?.trade_id);
        if (!Number.isFinite(tradeId)) {
            return { status: 400, payload: { success: false, error: 'Invalid trade_id' } };
        }

        const tradeLookup = await query('SELECT * FROM trades WHERE id = $1', [tradeId]);
        const tradeRow = tradeLookup.rows?.[0] ?? null;
        if (!tradeRow) {
            return { status: 400, payload: { success: false, error: 'Trade not found' } };
        }
        if (tradeRow.status !== 'PENDING') {
            return { status: 400, payload: { success: false, error: 'Trade is not pending' } };
        }
        if (parseFiniteInt(tradeRow.pair_index) !== parsedPairIndex) {
            return { status: 400, payload: { success: false, error: 'Trade pair mismatch' } };
        }
        if (executionMode === 'autotrade' && tradeRow.source !== 'BOT') {
            return { status: 400, payload: { success: false, error: 'Autotrade cannot cancel non-bot trades' } };
        }

        const updated = await query(
            "UPDATE trades SET status = 'CANCELED' WHERE id = $1 AND status = 'PENDING'",
            [tradeId]
        );

        if (!updated.changes) {
            return { status: 400, payload: { success: false, error: 'Pending trade could not be canceled (already updated)' } };
        }

        await query('UPDATE bot_decisions SET trade_id = $1 WHERE id = $2', [tradeId, parsedDecisionId]);

        // Best-effort: remove the pair from the pending trigger watcher set if no more pending trigger orders exist.
        try {
            const remaining = await query(
                "SELECT 1 FROM trades WHERE status = 'PENDING' AND pair_index = $1 AND trigger_price IS NOT NULL LIMIT 1",
                [parsedPairIndex]
            );
            if (!remaining.rows || remaining.rows.length === 0) {
                pendingTriggerPairs.delete(parsedPairIndex);
            }
        } catch {
            // ignore
        }

        const updatedTrade = await query('SELECT * FROM trades WHERE id = $1', [tradeId]);
        return { status: 200, payload: { success: true, action, tradeCanceled: updatedTrade.rows?.[0] ?? null, executionMode } };
    }

    return { status: 400, payload: { success: false, error: `Unsupported action: ${action}` } };
}

async function runAutotradeTick() {
    if (!autotradeState.enabled) return;
    if (autotradeState.isTicking) {
        scheduleAutotradeTick({ immediate: false });
        return;
    }

    autotradeState.isTicking = true;
    autotradeState.lastTickAtMs = Date.now();
    autotradeState.lastError = null;

    try {
        const botStatus = aiBot.getBotStatus();
        if (!botStatus.isActive) {
            autotradeState.lastScan = { skipped: true, reason: 'bot_inactive' };
            return;
        }

        const dailyStats = await getBotDailyStatsFromDb();
        if (dailyStats.todayPnL < -botStatus.safetyLimits.dailyLossLimit) {
            autotradeState.lastScan = { skipped: true, reason: 'daily_loss_limit' };
            return;
        }

        const nowMs = Date.now();
        const llmCooldownActive =
            !!autotradeState.lastLlmAtMs &&
            nowMs - autotradeState.lastLlmAtMs < autotradeConfig.minLlmIntervalSec * 1000;

        const thresholds = resolveExecutionThresholds(autotradeConfig.timeframeMin);

        // Open positions (bot-only) for risk + management decisions.
        const openBotPositionsResult = await query("SELECT * FROM trades WHERE status IN ('OPEN', 'PENDING') AND source = 'BOT' ORDER BY entry_time DESC");
        const openBotPositionsAll = openBotPositionsResult.rows || [];

        const openPairsResult = await query("SELECT pair_index FROM trades WHERE status IN ('OPEN', 'PENDING')");
        const openPairIndices = new Set();
        for (const row of openPairsResult.rows || []) {
            const idx = parseFiniteInt(row?.pair_index);
            if (Number.isFinite(idx)) openPairIndices.add(idx);
        }

        const marketStateResult = await query(
            `SELECT
                ms.*,
                p.from_symbol,
                p.to_symbol,
                tv.spread_percent,
                tv.fee_position_size_percent,
                tv.fee_oracle_position_size_percent,
                tv.min_position_size_usd,
                tv.group_max_leverage,
                tv.oi_long,
                tv.oi_short,
                tv.oi_skew_percent
            FROM market_state ms
            LEFT JOIN pairs p ON p.pair_index = ms.pair_index
            LEFT JOIN pair_trading_variables tv ON tv.pair_index = ms.pair_index`,
            []
        );

        const nowSec = Math.floor(Date.now() / 1000);

        // Rank only fresh market_state rows.
        // If we rank everything first, stale high-scoring rows can dominate the top-N slice,
        // leaving zero actionable candidates after freshness filtering.
        const marketStateRowsAll = marketStateResult.rows || [];
        const maxUpdatedAtAll = marketStateRowsAll.reduce((acc, row) => {
            const updatedAt = parseFiniteInt(row?.updated_at);
            if (!Number.isFinite(updatedAt) || updatedAt <= 0) return acc;
            return Math.max(acc, updatedAt);
        }, 0);
        const maxUpdatedAtAllSafe = maxUpdatedAtAll > 0 ? maxUpdatedAtAll : null;
        const marketStateRowsFresh = marketStateRowsAll.filter((row) => {
            const updatedAt = parseFiniteInt(row?.updated_at);
            if (!Number.isFinite(updatedAt)) return false;
            const timeframeMin = parseFiniteInt(row?.timeframe_min);
            if (!Number.isFinite(timeframeMin)) return false;
            const rowThresholds = resolveExecutionThresholds(timeframeMin);
            return nowSec - updatedAt <= rowThresholds.maxMarketStateAgeSec;
        });

        // Overall (multi-timeframe) scoring: ensures minScore threshold applies to the aggregated score.
        const ranked = rankTopOverall(marketStateRowsFresh, {
            minOiTotal: thresholds.minOiTotal,
            maxCostPercent: thresholds.maxCostPercent
        });
        const maxCandidatesToTry = 40;
        const candidatePool = ranked.slice(0, maxCandidatesToTry);

        const top = candidatePool[0] ?? null;
        const topPairIndex = parseFiniteInt(top?.row?.pair_index);
        const topSummary = top && Number.isFinite(topPairIndex)
            ? { pairIndex: topPairIndex, score: top.score, side: top.side }
            : null;

        const collectCandidates = (minScore) => {
            const collected = [];
            for (const item of candidatePool) {
            const pairIndex = parseFiniteInt(item?.row?.pair_index);
            if (!Number.isFinite(pairIndex)) continue;

            // Freshness is already enforced pre-ranking; keep this as a safety net.
            const updatedAt = parseFiniteInt(item?.row?.updated_at);
            const itemTf = parseFiniteInt(item?.row?.timeframe_min);
            if (Number.isFinite(updatedAt) && Number.isFinite(itemTf)) {
                const itemThresholds = resolveExecutionThresholds(itemTf);
                if (nowSec - updatedAt > itemThresholds.maxMarketStateAgeSec) continue;
            }

            const oiTotal = item.metrics?.oiTotal ?? null;
            const costPercent = item.metrics?.costPercent ?? null;
            if (typeof oiTotal === 'number' && oiTotal < thresholds.minOiTotal) continue;
            if (typeof costPercent === 'number' && costPercent > thresholds.maxCostPercent) continue;
            if (item.score < minScore) continue;

            if (!thresholds.allowMultiplePositionsPerPair && openPairIndices.has(pairIndex)) continue;

            collected.push(item);
            }
            return collected;
        };

        const strictMinScore = autotradeConfig.minScore;
        const minScoreFloor = 25;

        let baseCandidates = collectCandidates(strictMinScore);
        let usedMinScore = strictMinScore;

        // If nothing qualifies, progressively relax the score threshold to avoid idling all day.
        // Confidence enforcement still happens at execution time (minConfidence), so this mainly increases
        // how often we ask the LLM to evaluate borderline-but-plausible setups.
        if (baseCandidates.length === 0) {
            for (const delta of [10, 20, 30, 40, 50]) {
                const relaxed = Math.max(minScoreFloor, strictMinScore - delta);
                if (relaxed >= usedMinScore) continue;
                const next = collectCandidates(relaxed);
                if (next.length > 0) {
                    baseCandidates = next;
                    usedMinScore = relaxed;
                    break;
                }
                if (relaxed === minScoreFloor) break;
            }
        }

        let entryCandidate = null;
        let entryCandidateRegime = null;

        if (baseCandidates.length > 0) {
            if (llmCooldownActive) {
                entryCandidate = baseCandidates[0];
            } else if (
                autotradeConfig.regimeTimeframeMin &&
                autotradeConfig.regimeTimeframeMin !== autotradeConfig.timeframeMin
            ) {
                const regimeThresholds = resolveExecutionThresholds(autotradeConfig.regimeTimeframeMin);

                for (const item of baseCandidates) {
                    const pairIndex = parseFiniteInt(item?.row?.pair_index);
                    if (!Number.isFinite(pairIndex)) continue;

                    let bias = null;
                    let biasSource = null;

                    try {
                        const regimeStateResult = await query(
                            'SELECT overall_bias, updated_at FROM market_state WHERE pair_index = $1 AND timeframe_min = $2',
                            [pairIndex, autotradeConfig.regimeTimeframeMin]
                        );
                        const regimeRow = regimeStateResult.rows?.[0] ?? null;
                        const regimeUpdatedAt = parseFiniteInt(regimeRow?.updated_at);
                        if (
                            typeof regimeRow?.overall_bias === 'string' &&
                            Number.isFinite(regimeUpdatedAt) &&
                            nowSec - regimeUpdatedAt <= regimeThresholds.maxMarketStateAgeSec
                        ) {
                            bias = regimeRow.overall_bias;
                            biasSource = 'db';
                        }
                    } catch {
                        // ignore
                    }

                    if (!bias) {
                        const regime = await getRegimeBias(pairIndex, autotradeConfig.regimeTimeframeMin, autotradeConfig.regimeLookbackSec);
                        if (regime.success) {
                            bias = regime.overallBias;
                            biasSource = 'ohlc';
                        }
                    }

                    if (bias) {
                        const side = item.side;
                        if ((side === 'LONG' && bias === 'BEARISH') || (side === 'SHORT' && bias === 'BULLISH')) {
                            continue;
                        }
                        entryCandidateRegime = { overallBias: bias, source: biasSource };
                    }

                    entryCandidate = item;
                    break;
                }
            } else {
                entryCandidate = baseCandidates[0];
            }
        }

        const candidateSummary = entryCandidate
            ? { pairIndex: parseFiniteInt(entryCandidate.row?.pair_index), score: entryCandidate.score, side: entryCandidate.side }
            : null;

        const selectedTimeframeMin = entryCandidate
            ? (
                parseFiniteInt(entryCandidate?.bestTimeframeMin) ??
                parseFiniteInt(entryCandidate?.row?.timeframe_min) ??
                autotradeConfig.timeframeMin
            )
            : null;

        if (llmCooldownActive) {
            autotradeState.lastScan = {
                skipped: true,
                reason: 'llm_cooldown',
                openBotPositions: openBotPositionsAll.length,
                usedMinScore,
                top: topSummary,
                candidate: candidateSummary,
                candidateTimeframeMin: selectedTimeframeMin
            };
            return;
        }

        const canOpenNew = openBotPositionsAll.length < thresholds.maxOpenPositions;
        const openAvailable = !!entryCandidate && canOpenNew;
        const manageAvailable = openBotPositionsAll.length > 0;
        const nextIntent = autotradeState.nextIntent === 'manage' ? 'manage' : 'open';
        const intent = openAvailable && manageAvailable ? nextIntent : openAvailable ? 'open' : manageAvailable ? 'manage' : null;

        if (intent === 'open') {
            const bestPairIndex = parseFiniteInt(entryCandidate.row?.pair_index);
            if (!Number.isFinite(bestPairIndex)) {
                autotradeState.lastScan = { skipped: true, reason: 'invalid_candidate' };
                return;
            }

            let tradingVariables = null;
            try {
                tradingVariables = await fetchTradingVariablesCached();
            } catch {
                tradingVariables = null;
            }
            const tvForPair = tradingVariables ? buildTradingVariablesForPair(tradingVariables, bestPairIndex) : null;

            const candidateTimeframeMin = Number.isFinite(selectedTimeframeMin)
                ? selectedTimeframeMin
                : autotradeConfig.timeframeMin;

            const candidateThresholds = resolveExecutionThresholds(candidateTimeframeMin);

            const to = Math.floor(Date.now() / 1000);
            const from = to - autotradeConfig.lookbackSec;
            const ohlcData = await fetchOHLCData(bestPairIndex, from, to, String(candidateTimeframeMin));
            if (ohlcData.length < 50) {
                autotradeState.lastScan = { skipped: true, reason: 'insufficient_ohlc', pairIndex: bestPairIndex, timeframeMin: candidateTimeframeMin };
                return;
            }

            const openPositionsForPair = thresholds.allowMultiplePositionsPerPair
                ? (await query("SELECT * FROM trades WHERE status IN ('OPEN', 'PENDING') AND pair_index = $1", [bestPairIndex])).rows || []
                : [];

            const entryCandidateScore = typeof entryCandidate?.score === 'number' && Number.isFinite(entryCandidate.score)
                ? entryCandidate.score
                : null;
            const analysis = await aiBot.analyzeMarket(
                bestPairIndex,
                ohlcData,
                openPositionsForPair,
                tvForPair,
                {
                    candidateScore: entryCandidateScore,
                    timeframeMin: candidateTimeframeMin,
                    allowMultiplePositionsPerPair: candidateThresholds.allowMultiplePositionsPerPair
                }
            );
            analysis.tradingVariables = tvForPair;

            autotradeState.lastLlmAtMs = Date.now();
            autotradeState.nextIntent = 'manage';
            autotradeState.lastScan = {
                skipped: false,
                reason: 'open_new_position',
                pairIndex: bestPairIndex,
                score: entryCandidate.score,
                side: entryCandidate.side,
                timeframeMin: candidateTimeframeMin,
                usedMinScore,
                regime: entryCandidateRegime,
                analysis: analysis.success ? { success: true, action: analysis.action, decisionId: analysis.decisionId } : { success: false, error: analysis.error }
            };

            if (!analysis.success) return;
            if (analysis.action !== 'execute_trade') return;

            const execution = await executeBotDecision({
                pairIndex: bestPairIndex,
                action: analysis.action,
                args: analysis.args,
                currentPrice: analysis.currentPrice,
                decisionId: analysis.decisionId,
                timeframeMin: candidateTimeframeMin,
                executionMode: 'autotrade'
            });

            autotradeState.lastExecution = {
                atMs: Date.now(),
                pairIndex: bestPairIndex,
                action: analysis.action,
                decisionId: analysis.decisionId,
                result: execution.payload
            };

            return;
        }

        if (intent === null) {
            const reason = (() => {
                if (!canOpenNew) return 'max_open_positions';
                if (marketStateRowsAll.length === 0) return 'market_state_empty';
                if (marketStateRowsFresh.length === 0) return 'market_state_stale';
                if (ranked.length === 0) return 'no_ranked_candidates';
                return 'no_actionable_candidates';
            })();

            autotradeState.lastScan = {
                skipped: true,
                reason,
                usedMinScore,
                top: topSummary,
                candidate: candidateSummary,
                market: {
                    totalRows: marketStateRowsAll.length,
                    freshRows: marketStateRowsFresh.length,
                    rankedRows: ranked.length,
                    maxUpdatedAt: maxUpdatedAtAllSafe,
                    maxUpdatedAtAgeSec: maxUpdatedAtAllSafe ? nowSec - maxUpdatedAtAllSafe : null
                }
            };
            return;
        }

        // Otherwise, manage an existing open bot position.
        const positionToManage = pickPositionToManage(openBotPositionsAll);
        const managePairIndex = parseFiniteInt(positionToManage?.pair_index);
        const manageTradeId = parseFiniteInt(positionToManage?.id);
        if (!Number.isFinite(managePairIndex) || !Number.isFinite(manageTradeId)) {
            autotradeState.lastScan = { skipped: true, reason: 'invalid_open_position' };
            return;
        }

        let tradingVariables = null;
        try {
            tradingVariables = await fetchTradingVariablesCached();
        } catch {
            tradingVariables = null;
        }

        const tvForPair = tradingVariables ? buildTradingVariablesForPair(tradingVariables, managePairIndex) : null;

        const to = Math.floor(Date.now() / 1000);
        const from = to - autotradeConfig.lookbackSec;
        const ohlcData = await fetchOHLCData(managePairIndex, from, to, String(autotradeConfig.timeframeMin));
        if (ohlcData.length < 50) {
            autotradeState.lastScan = { skipped: true, reason: 'insufficient_ohlc_manage', pairIndex: managePairIndex, tradeId: manageTradeId };
            return;
        }

        const openBotPositionsForPairResult = await query(
            "SELECT * FROM trades WHERE status IN ('OPEN', 'PENDING') AND source = 'BOT' AND pair_index = $1 ORDER BY entry_time DESC",
            [managePairIndex]
        );
        const openBotPositionsForPair = openBotPositionsForPairResult.rows || [];

        const analysis = await aiBot.analyzeMarket(
            managePairIndex,
            ohlcData,
            openBotPositionsForPair,
            tvForPair,
            {
                timeframeMin: autotradeConfig.timeframeMin,
                allowMultiplePositionsPerPair: thresholds.allowMultiplePositionsPerPair
            }
        );
        analysis.tradingVariables = tvForPair;

        autotradeState.lastLlmAtMs = Date.now();
        autotradeState.nextIntent = 'open';
        autotradeState.lastScan = {
            skipped: false,
            reason: 'manage_open_position',
            openBotPositions: openBotPositionsAll.length,
            pairIndex: managePairIndex,
            tradeId: manageTradeId,
            analysis: analysis.success ? { success: true, action: analysis.action, decisionId: analysis.decisionId } : { success: false, error: analysis.error }
        };

        if (!analysis.success) return;
        if (analysis.action !== 'close_position' && analysis.action !== 'cancel_pending') return;

        const execution = await executeBotDecision({
            pairIndex: managePairIndex,
            action: analysis.action,
            args: analysis.args,
            currentPrice: analysis.currentPrice,
            decisionId: analysis.decisionId,
            timeframeMin: autotradeConfig.timeframeMin,
            executionMode: 'autotrade'
        });

        autotradeState.lastExecution = {
            atMs: Date.now(),
            pairIndex: managePairIndex,
            action: analysis.action,
            decisionId: analysis.decisionId,
            result: execution.payload
        };
    } catch (err) {
        autotradeState.lastError = err?.message || String(err);
    } finally {
        autotradeState.isTicking = false;
        scheduleAutotradeTick({ immediate: false });
    }
}

// GET bot status
app.get('/api/bot/status', async (req, res) => {
    try {
        const status = aiBot.getBotStatus();
        const dailyStats = await getBotDailyStatsFromDb();

        res.json({
            ...status,
            todayPnL: dailyStats.todayPnL,
            tradesExecuted: dailyStats.tradesExecuted,
            autotrade: getAutotradeStatus()
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// GET autotrade worker status/config
app.get('/api/bot/autotrade/status', (req, res) => {
    res.json(getAutotradeStatus());
});

// POST toggle autotrade worker on/off
app.post('/api/bot/autotrade/toggle', (req, res) => {
    const enabled = req.body?.enabled === true;
    if (enabled) {
        aiBot.toggleBot(true);
        startAutotradeWorker();
    } else {
        stopAutotradeWorker();
        aiBot.toggleBot(false);
    }
    res.json(getAutotradeStatus());
});

// POST update autotrade config (interval/thresholds)
app.post('/api/bot/autotrade/config', (req, res) => {
    res.json(updateAutotradeConfig(req.body || {}));
});

// POST toggle bot
app.post('/api/bot/toggle', (req, res) => {
    const { active } = req.body;
    const status = aiBot.toggleBot(active);
    res.json(status);
});

// POST analyze market (trigger AI analysis)
app.post('/api/bot/analyze/:pairIndex', async (req, res) => {
    const { pairIndex } = req.params;
    const parsedPairIndex = Number.parseInt(pairIndex, 10);
    const to = Math.floor(Date.now() / 1000);
    const from = to - (24 * 60 * 60);
    const resolution = '15';

    try {
        if (!Number.isFinite(parsedPairIndex)) {
            return res.status(400).json({ error: 'Invalid pairIndex' });
        }

        // Fetch OHLC data
        const ohlcData = await fetchOHLCData(pairIndex, from, to, resolution);
        if (ohlcData.length < 50) {
            return res.status(400).json({ error: 'Insufficient data for analysis' });
        }

        // Get open positions
        const openPositionsResult = await query(
            "SELECT * FROM trades WHERE status IN ('OPEN', 'PENDING') AND pair_index = $1",
            [pairIndex]
        );
        const openPositions = openPositionsResult.rows;

        let tradingVariablesForPair = null;
        let tradingVariablesError = null;
        try {
            const tradingVariables = await fetchTradingVariablesCached();
            tradingVariablesForPair = buildTradingVariablesForPair(tradingVariables, parsedPairIndex);
        } catch (err) {
            tradingVariablesError = err?.message || 'Failed to fetch trading variables';
        }

        // Run AI analysis
        const analyzeThresholds = resolveExecutionThresholds(Number(resolution));
        const analysis = await aiBot.analyzeMarket(
            parsedPairIndex,
            ohlcData,
            openPositions,
            tradingVariablesForPair,
            {
                timeframeMin: Number(resolution),
                allowMultiplePositionsPerPair: analyzeThresholds.allowMultiplePositionsPerPair
            }
        );
        analysis.tradingVariables = tradingVariablesForPair;
        analysis.tradingVariablesError = tradingVariablesError;

        if (!analysis.success) {
            return res.json(analysis);
        }

        res.json(analysis);
    } catch (err) {
        console.error('Bot analysis error:', err);
        res.status(500).json({ error: err.message });
    }
});

// POST analyze the whole universe: rank -> select best pair -> analyze that pair
app.post('/api/bot/analyze-universe', async (req, res) => {
    const timeframeMin = Number.parseInt(req.body?.timeframeMin ?? req.query?.timeframeMin, 10) || 15;
    const limit = Math.min(Number.parseInt(req.body?.limit ?? req.query?.limit, 10) || 10, 25);

    const minOiTotal = Number.isFinite(Number(req.body?.minOiTotal ?? req.query?.minOiTotal))
        ? Number(req.body?.minOiTotal ?? req.query?.minOiTotal)
        : (Number.isFinite(Number(process.env.MARKET_MIN_OI_TOTAL)) ? Number(process.env.MARKET_MIN_OI_TOTAL) : 1);

    const maxCostPercent = Number.isFinite(Number(req.body?.maxCostPercent ?? req.query?.maxCostPercent))
        ? Number(req.body?.maxCostPercent ?? req.query?.maxCostPercent)
        : (Number.isFinite(Number(process.env.MARKET_MAX_COST_PERCENT)) ? Number(process.env.MARKET_MAX_COST_PERCENT) : 0.25);

    try {
        // Rank opportunities from the latest stored market state
        const marketStateResult = await query(
            `SELECT
                ms.*,
                p.from_symbol,
                p.to_symbol,
                tv.spread_percent,
                tv.fee_position_size_percent,
                tv.fee_oracle_position_size_percent,
                tv.min_position_size_usd,
                tv.group_max_leverage,
                tv.oi_long,
                tv.oi_short,
                tv.oi_skew_percent
            FROM market_state ms
            LEFT JOIN pairs p ON p.pair_index = ms.pair_index
            LEFT JOIN pair_trading_variables tv ON tv.pair_index = ms.pair_index
            WHERE ms.timeframe_min = $1`,
            [timeframeMin]
        );

        const ranked = rankTop(marketStateResult.rows, { minOiTotal, maxCostPercent }).slice(0, limit);
        const candidates = ranked.map(item => ({
            pair_index: item.row.pair_index,
            symbol: item.row.from_symbol && item.row.to_symbol ? `${item.row.from_symbol}/${item.row.to_symbol}` : null,
            timeframe_min: timeframeMin,
            candle_time: item.row.candle_time,
            price: item.row.price,
            side: item.side,
            score: item.score,
            reasons: item.reasons,
            indicators: {
                rsi: item.row.rsi,
                macd_histogram: item.row.macd_histogram,
                ema9: item.row.ema9,
                ema21: item.row.ema21,
                bb_upper: item.row.bb_upper,
                bb_middle: item.row.bb_middle,
                bb_lower: item.row.bb_lower,
                atr: item.row.atr,
                stoch_k: item.row.stoch_k,
                overall_bias: item.row.overall_bias
            },
            tradingVariables: {
                spread_percent: item.row.spread_percent ?? null,
                fee_position_size_percent: item.row.fee_position_size_percent ?? null,
                fee_oracle_position_size_percent: item.row.fee_oracle_position_size_percent ?? null,
                oi_long: item.row.oi_long ?? null,
                oi_short: item.row.oi_short ?? null,
                oi_skew_percent: item.row.oi_skew_percent ?? null,
                min_position_size_usd: item.row.min_position_size_usd ?? null,
                group_max_leverage: item.row.group_max_leverage ?? null
            }
        }));

        if (candidates.length === 0) {
            return res.status(400).json({ success: false, error: 'No ranked candidates available yet (market_state empty)' });
        }

        // Open positions across all pairs (for selection context)
        const openPositionsAllResult = await query("SELECT * FROM trades WHERE status IN ('OPEN', 'PENDING') ORDER BY entry_time DESC");
        const openPositionsAll = openPositionsAllResult.rows;

        // Ask the AI to select the best market from top candidates
        const selection = await aiBot.selectBestMarket(candidates, openPositionsAll);
        const selectionTimestamp = Math.floor(Date.now() / 1000);

        if (!selection.success) {
            return res.json({ success: false, selection, candidates, timeframeMin });
        }

        if (selection.action === 'skip_trade') {
            await query(
                `INSERT INTO bot_universe_decisions (
                    timestamp, timeframe_min, candidates_json, selection_json, analysis_json, selected_pair_index, action, trade_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
                [
                    selectionTimestamp,
                    timeframeMin,
                    JSON.stringify(candidates),
                    JSON.stringify(selection),
                    null,
                    null,
                    'skip_trade',
                    null
                ]
            );

            return res.json({ success: true, timeframeMin, candidates, selection });
        }

        const selectedPairIndex = Number.parseInt(selection.args?.pair_index, 10);
        if (!Number.isFinite(selectedPairIndex)) {
            return res.status(400).json({ success: false, error: 'Invalid selected pair_index', selection });
        }

        const to = Math.floor(Date.now() / 1000);
        const from = to - (24 * 60 * 60);
        const resolution = String(timeframeMin);

        const ohlcData = await fetchOHLCData(selectedPairIndex, from, to, resolution);
        if (ohlcData.length < 50) {
            return res.status(400).json({ success: false, error: 'Insufficient OHLC data for selected market', selectedPairIndex });
        }

        const openPositionsForPair = openPositionsAll.filter(p => Number(p.pair_index) === selectedPairIndex);

        let tradingVariablesForPair = null;
        let tradingVariablesError = null;
        try {
            const tradingVariables = await fetchTradingVariablesCached();
            tradingVariablesForPair = buildTradingVariablesForPair(tradingVariables, selectedPairIndex);
        } catch (err) {
            tradingVariablesError = err?.message || 'Failed to fetch trading variables';
        }

        const candidateForPair = candidates.find((c) => c.pair_index === selectedPairIndex);
        const candidateScore = typeof candidateForPair?.score === 'number' && Number.isFinite(candidateForPair.score)
            ? candidateForPair.score
            : null;
        const analysis = await aiBot.analyzeMarket(
            selectedPairIndex,
            ohlcData,
            openPositionsForPair,
            tradingVariablesForPair,
            {
                candidateScore,
                timeframeMin,
                allowMultiplePositionsPerPair: resolveExecutionThresholds(timeframeMin).allowMultiplePositionsPerPair
            }
        );
        analysis.tradingVariables = tradingVariablesForPair;
        analysis.tradingVariablesError = tradingVariablesError;

        await query(
            `INSERT INTO bot_universe_decisions (
                timestamp, timeframe_min, candidates_json, selection_json, analysis_json, selected_pair_index, action, trade_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
            [
                selectionTimestamp,
                timeframeMin,
                JSON.stringify(candidates),
                JSON.stringify(selection),
                JSON.stringify(analysis),
                selectedPairIndex,
                analysis.action ?? null,
                null
            ]
        );

        return res.json({
            success: true,
            timeframeMin,
            candidates,
            selection,
            selectedPairIndex,
            analysis
        });
    } catch (err) {
        console.error('Universe analysis error:', err);
        res.status(500).json({ success: false, error: err.message });
    }
});

// POST unified bot run: opportunities -> multi-pair analysis -> actionable recommendations
app.post('/api/bot/run', async (req, res) => {
    const timeframeMin = Number.parseInt(req.body?.timeframeMin ?? 15, 10) || 15;
    const opportunitiesLimit = Math.min(Number.parseInt(req.body?.opportunitiesLimit ?? 10, 10) || 10, 50);
    const maxAnalyses = Math.min(Number.parseInt(req.body?.maxAnalyses ?? 5, 10) || 5, 10);
    const primaryPairIndex = Number.isFinite(Number(req.body?.primaryPairIndex))
        ? Number.parseInt(req.body.primaryPairIndex, 10)
        : null;

    const minOiTotal = Number.isFinite(Number(req.body?.minOiTotal))
        ? Number(req.body.minOiTotal)
        : (Number.isFinite(Number(process.env.MARKET_MIN_OI_TOTAL)) ? Number(process.env.MARKET_MIN_OI_TOTAL) : 1);

    const maxCostPercent = Number.isFinite(Number(req.body?.maxCostPercent))
        ? Number(req.body.maxCostPercent)
        : (Number.isFinite(Number(process.env.MARKET_MAX_COST_PERCENT)) ? Number(process.env.MARKET_MAX_COST_PERCENT) : 0.25);

    const analysisConcurrency = Math.min(
        Math.max(Number.parseInt(process.env.BOT_ANALYSIS_CONCURRENCY ?? '2', 10) || 2, 1),
        4
    );

    try {
        const runId = typeof crypto.randomUUID === 'function' ? crypto.randomUUID() : String(Date.now());

        const runThresholds = resolveExecutionThresholds(timeframeMin);

        // Rank opportunities from the latest stored market state.
        const marketStateResult = await query(
            `SELECT
                ms.*,
                p.from_symbol,
                p.to_symbol,
                tv.spread_percent,
                tv.fee_position_size_percent,
                tv.fee_oracle_position_size_percent,
                tv.min_position_size_usd,
                tv.group_max_leverage,
                tv.oi_long,
                tv.oi_short,
                tv.oi_skew_percent
            FROM market_state ms
            LEFT JOIN pairs p ON p.pair_index = ms.pair_index
            LEFT JOIN pair_trading_variables tv ON tv.pair_index = ms.pair_index
            WHERE ms.timeframe_min = $1`,
            [timeframeMin]
        );

        const ranked = rankTop(marketStateResult.rows, { minOiTotal, maxCostPercent }).slice(0, opportunitiesLimit);
        const candidates = ranked.map(item => ({
            pair_index: item.row.pair_index,
            symbol: item.row.from_symbol && item.row.to_symbol ? `${item.row.from_symbol}/${item.row.to_symbol}` : null,
            timeframe_min: timeframeMin,
            candle_time: item.row.candle_time,
            price: item.row.price,
            side: item.side,
            score: item.score,
            reasons: item.reasons,
            indicators: {
                rsi: item.row.rsi,
                macd_histogram: item.row.macd_histogram,
                ema9: item.row.ema9,
                ema21: item.row.ema21,
                bb_upper: item.row.bb_upper,
                bb_middle: item.row.bb_middle,
                bb_lower: item.row.bb_lower,
                atr: item.row.atr,
                stoch_k: item.row.stoch_k,
                overall_bias: item.row.overall_bias
            },
            tradingVariables: {
                spread_percent: item.row.spread_percent ?? null,
                fee_position_size_percent: item.row.fee_position_size_percent ?? null,
                fee_oracle_position_size_percent: item.row.fee_oracle_position_size_percent ?? null,
                oi_long: item.row.oi_long ?? null,
                oi_short: item.row.oi_short ?? null,
                oi_skew_percent: item.row.oi_skew_percent ?? null,
                min_position_size_usd: item.row.min_position_size_usd ?? null,
                group_max_leverage: item.row.group_max_leverage ?? null
            }
        }));

        const candidateScoreByPair = new Map();
        for (const candidate of candidates) {
            const normalizedScore = typeof candidate.score === 'number' && Number.isFinite(candidate.score)
                ? candidate.score
                : null;
            candidateScoreByPair.set(candidate.pair_index, normalizedScore);
        }

        const selectedPairIndices = (() => {
            const unique = new Set();
            if (Number.isFinite(primaryPairIndex)) unique.add(primaryPairIndex);
            for (const candidate of candidates) unique.add(candidate.pair_index);
            return Array.from(unique).slice(0, maxAnalyses);
        })();

        const openPositionsAllResult = await query("SELECT * FROM trades WHERE status IN ('OPEN', 'PENDING') ORDER BY entry_time DESC");
        const openPositionsAll = openPositionsAllResult.rows || [];

        let tradingVariables = null;
        let tradingVariablesError = null;
        try {
            tradingVariables = await fetchTradingVariablesCached();
        } catch (err) {
            tradingVariablesError = err?.message || 'Failed to fetch trading variables';
        }

        const to = Math.floor(Date.now() / 1000);
        const from = to - (24 * 60 * 60);
        const resolution = String(timeframeMin);

        const analyses = await mapWithConcurrency(selectedPairIndices, analysisConcurrency, async (pairIdx) => {
            const ohlcData = await fetchOHLCData(pairIdx, from, to, resolution);
            if (ohlcData.length < 50) {
                return { success: false, error: 'Insufficient OHLC data for analysis', pairIndex: pairIdx };
            }

            const openPositionsForPair = openPositionsAll.filter(p => Number(p.pair_index) === pairIdx);
            const tvForPair = tradingVariables ? buildTradingVariablesForPair(tradingVariables, pairIdx) : null;

            const candidateScore = candidateScoreByPair.get(pairIdx) ?? null;
            const analysis = await aiBot.analyzeMarket(
                pairIdx,
                ohlcData,
                openPositionsForPair,
                tvForPair,
                {
                    candidateScore,
                    timeframeMin,
                    allowMultiplePositionsPerPair: runThresholds.allowMultiplePositionsPerPair
                }
            );
            analysis.tradingVariables = tvForPair;
            analysis.tradingVariablesError = tradingVariablesError;
            analysis.runId = runId;
            return { pairIndex: pairIdx, analysis };
        });

        const symbolByPairIndex = new Map();
        for (const candidate of candidates) {
            symbolByPairIndex.set(candidate.pair_index, candidate.symbol ?? null);
        }

        const recommendations = [];
        for (const item of analyses) {
            const analysis = item?.analysis;
            if (!analysis?.success) continue;
            if (analysis.action !== 'execute_trade' && analysis.action !== 'close_position' && analysis.action !== 'cancel_pending') continue;

            const symbolFromVariables = analysis.tradingVariables?.pair?.from && analysis.tradingVariables?.pair?.to
                ? `${analysis.tradingVariables.pair.from}/${analysis.tradingVariables.pair.to}`
                : null;

            recommendations.push({
                pairIndex: item.pairIndex,
                symbol: symbolFromVariables ?? symbolByPairIndex.get(item.pairIndex) ?? null,
                analysis
            });
        }

        const primaryAnalysis = analyses.find(item => item?.pairIndex === primaryPairIndex)?.analysis ?? null;

        return res.json({
            success: true,
            runId,
            timeframeMin,
            candidates,
            pairIndices: selectedPairIndices,
            primaryPairIndex,
            primaryAnalysis,
            recommendations
        });
    } catch (err) {
        console.error('Bot run error:', err);
        res.status(500).json({ success: false, error: err.message });
    }
});

// GET bot decisions history
app.get('/api/bot/decisions', async (req, res) => {
    const rawLimit = parseInt(req.query.limit, 10);
    const limit = Math.min(Math.max(Number.isFinite(rawLimit) ? rawLimit : 50, 1), 200);
    const beforeParam = Number.isFinite(Number(req.query.before)) ? Number(req.query.before) : null;
    const beforeTimestamp = beforeParam !== null ? Math.floor(beforeParam) : null;
    const beforeIdParam = Number.isFinite(Number(req.query.beforeId)) ? Number(req.query.beforeId) : null;
    const beforeId = beforeIdParam !== null ? Math.floor(beforeIdParam) : null;

    try {
        const params = [];
        let sql = 'SELECT * FROM bot_decisions';

        if (beforeTimestamp !== null) {
            if (beforeId !== null) {
                sql += ' WHERE (timestamp < ? OR (timestamp = ? AND id < ?))';
                params.push(beforeTimestamp, beforeTimestamp, beforeId);
            } else {
                sql += ' WHERE timestamp < ?';
                params.push(beforeTimestamp);
            }
        }

        sql += ' ORDER BY timestamp DESC, id DESC LIMIT ?';
        params.push(limit);

        const result = await query(sql, params);
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// GET universe decisions history
app.get('/api/bot/universe/decisions', async (req, res) => {
    const limit = Math.min(parseInt(req.query.limit, 10) || 25, 200);
    try {
        const result = await query(
            'SELECT * FROM bot_universe_decisions ORDER BY timestamp DESC LIMIT $1',
            [limit]
        );
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// POST execute a bot-recommended action (human confirmation)
app.post('/api/bot/execute/:pairIndex', async (req, res) => {
    const { pairIndex } = req.params;
    const { action, args, currentPrice, decisionId } = req.body || {};

    try {
        const result = await executeBotDecision({
            pairIndex,
            action,
            args,
            currentPrice,
            decisionId,
            timeframeMin: 15,
            executionMode: 'manual'
        });

        return res.status(result.status).json(result.payload);
    } catch (err) {
        console.error('Bot execute error:', err);
        res.status(500).json({ success: false, error: err.message });
    }
});

// --- Bot debug endpoints (minimal visibility into cache/reflections/thresholds/metrics) ---

app.get('/api/bot/debug/overview', async (req, res) => {
    try {
        const nowSec = Math.floor(Date.now() / 1000);
        const metricsSinceSec = nowSec - 24 * 60 * 60;

        const [decisionsAgg, reflectionsAgg, historyAgg, metricsAgg] = await Promise.all([
            query('SELECT COUNT(1) AS count, MAX(timestamp) AS last_ts FROM bot_decisions'),
            query('SELECT COUNT(1) AS count, MAX(timestamp) AS last_ts FROM bot_reflections'),
            query('SELECT COUNT(1) AS count, MAX(candle_time) AS last_candle_time FROM market_state_history'),
            query('SELECT COUNT(1) AS count, MAX(timestamp) AS last_ts FROM metrics_events')
        ]);

        const activeThreshold = await query(
            `SELECT *
             FROM bot_threshold_versions
             WHERE is_active = 1
             ORDER BY created_at DESC
             LIMIT 1`
        );

        const recentThresholds = await query(
            `SELECT id, created_at, scope, is_active, reason, parent_version_id, params_json, metrics_json
             FROM bot_threshold_versions
             ORDER BY created_at DESC
             LIMIT 5`
        );

        const recentMetrics = await query(
            `SELECT name,
                    COUNT(1) AS count,
                    MIN(timestamp) AS first_ts,
                    MAX(timestamp) AS last_ts
             FROM metrics_events
             WHERE timestamp >= $1
             GROUP BY name
             ORDER BY count DESC`,
            [metricsSinceSec]
        );

        const activeThresholdRow = activeThreshold.rows?.[0] ?? null;
        const parsedActiveThreshold = activeThresholdRow
            ? {
                ...activeThresholdRow,
                params: (() => {
                    try {
                        return JSON.parse(activeThresholdRow.params_json);
                    } catch {
                        return null;
                    }
                })(),
                metrics: (() => {
                    try {
                        return activeThresholdRow.metrics_json ? JSON.parse(activeThresholdRow.metrics_json) : null;
                    } catch {
                        return null;
                    }
                })()
            }
            : null;

        const thresholdRows = (recentThresholds.rows || []).map((row) => ({
            ...row,
            params: (() => {
                try {
                    return JSON.parse(row.params_json);
                } catch {
                    return null;
                }
            })(),
            metrics: (() => {
                try {
                    return row.metrics_json ? JSON.parse(row.metrics_json) : null;
                } catch {
                    return null;
                }
            })()
        }));

        const decisionsRow = decisionsAgg.rows?.[0] ?? {};
        const reflectionsRow = reflectionsAgg.rows?.[0] ?? {};
        const historyRow = historyAgg.rows?.[0] ?? {};
        const metricsRow = metricsAgg.rows?.[0] ?? {};

        return res.json({
            nowSec,
            env: {
                OPENAI_TRADING_MODEL: process.env.OPENAI_TRADING_MODEL ?? null,
                BOT_ANALYSIS_CACHE_ENABLED: process.env.BOT_ANALYSIS_CACHE_ENABLED ?? null,
                BOT_REFLECTIONS_ENABLED: process.env.BOT_REFLECTIONS_ENABLED ?? null,
                BOT_REFLECTION_RETENTION_DAYS: process.env.BOT_REFLECTION_RETENTION_DAYS ?? null
            },
            counts: {
                botDecisions: Number(decisionsRow.count ?? 0),
                botReflections: Number(reflectionsRow.count ?? 0),
                marketStateHistory: Number(historyRow.count ?? 0),
                metricsEvents: Number(metricsRow.count ?? 0)
            },
            last: {
                botDecisionTs: decisionsRow.last_ts ?? null,
                botReflectionTs: reflectionsRow.last_ts ?? null,
                marketStateHistoryCandleTime: historyRow.last_candle_time ?? null,
                metricsEventTs: metricsRow.last_ts ?? null
            },
            thresholds: {
                active: parsedActiveThreshold,
                recent: thresholdRows
            },
            metrics: {
                sinceSec: metricsSinceSec,
                recentSummary: recentMetrics.rows || []
            }
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/bot/debug/metrics/summary', async (req, res) => {
    try {
        const nowSec = Math.floor(Date.now() / 1000);
        const horizonSecRaw = Number(req.query.horizonSec);
        const horizonSec = Number.isFinite(horizonSecRaw) ? Math.max(60, Math.min(30 * 24 * 60 * 60, Math.floor(horizonSecRaw))) : 24 * 60 * 60;
        const sinceSec = nowSec - horizonSec;

        const result = await query(
            `SELECT name,
                    COUNT(1) AS count,
                    MIN(timestamp) AS first_ts,
                    MAX(timestamp) AS last_ts
             FROM metrics_events
             WHERE timestamp >= $1
             GROUP BY name
             ORDER BY count DESC`,
            [sinceSec]
        );

        res.json({ nowSec, sinceSec, horizonSec, rows: result.rows || [] });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/bot/debug/thresholds', async (req, res) => {
    try {
        const limitRaw = parseInt(req.query.limit, 10);
        const limit = Math.min(Math.max(Number.isFinite(limitRaw) ? limitRaw : 10, 1), 50);
        const scope = typeof req.query.scope === 'string' && req.query.scope.trim() ? req.query.scope.trim() : null;

        const whereClause = scope ? 'WHERE scope = $1' : '';
        const whereParams = scope ? [scope] : [];

        const active = await query(
            `SELECT * FROM bot_threshold_versions ${whereClause} ${scope ? 'AND' : 'WHERE'} is_active = 1 ORDER BY created_at DESC LIMIT 1`,
            whereParams
        );

        const recent = await query(
            `SELECT id, created_at, scope, is_active, reason, parent_version_id, params_json, metrics_json
             FROM bot_threshold_versions ${whereClause}
             ORDER BY created_at DESC
             LIMIT $${whereParams.length + 1}`,
            [...whereParams, limit]
        );

        const parseRow = (row) => ({
            ...row,
            params: (() => {
                try {
                    return JSON.parse(row.params_json);
                } catch {
                    return null;
                }
            })(),
            metrics: (() => {
                try {
                    return row.metrics_json ? JSON.parse(row.metrics_json) : null;
                } catch {
                    return null;
                }
            })()
        });

        res.json({
            scope,
            active: active.rows?.[0] ? parseRow(active.rows[0]) : null,
            recent: (recent.rows || []).map(parseRow)
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/bot/debug/reflections', async (req, res) => {
    try {
        const limitRaw = parseInt(req.query.limit, 10);
        const limit = Math.min(Math.max(Number.isFinite(limitRaw) ? limitRaw : 10, 1), 50);
        const pairIndexRaw = Number(req.query.pairIndex);
        const pairIndex = Number.isFinite(pairIndexRaw) ? Math.floor(pairIndexRaw) : null;
        const scope = typeof req.query.scope === 'string' && req.query.scope.trim() ? req.query.scope.trim() : null;

        const params = [];
        const where = [];
        let sql = `SELECT id, timestamp, scope, trade_id, decision_id, pair_index, timeframe_min, summary,
                          tags_json, metrics_json, reflection_json, inputs_json, model, prompt_version
                   FROM bot_reflections`;

        if (pairIndex !== null) {
            where.push('pair_index = ?');
            params.push(pairIndex);
        }

        if (scope !== null) {
            where.push('scope = ?');
            params.push(scope);
        }

        if (where.length > 0) {
            sql += ` WHERE ${where.join(' AND ')}`;
        }

        sql += ' ORDER BY timestamp DESC LIMIT ?';
        params.push(limit);

        const result = await query(sql, params);
        const rows = (result.rows || []).map((row) => ({
            ...row,
            tags: (() => {
                try {
                    return row.tags_json ? JSON.parse(row.tags_json) : null;
                } catch {
                    return null;
                }
            })(),
            metrics: (() => {
                try {
                    return row.metrics_json ? JSON.parse(row.metrics_json) : null;
                } catch {
                    return null;
                }
            })(),
            reflection: (() => {
                try {
                    return row.reflection_json ? JSON.parse(row.reflection_json) : null;
                } catch {
                    return null;
                }
            })(),
            inputs: (() => {
                try {
                    return row.inputs_json ? JSON.parse(row.inputs_json) : null;
                } catch {
                    return null;
                }
            })()
        }));

        res.json({ limit, pairIndex, scope, rows });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.get('/api/bot/debug/outcomes/summary', async (req, res) => {
    try {
        const nowSec = Math.floor(Date.now() / 1000);
        const daysRaw = Number(req.query.days);
        const days = Number.isFinite(daysRaw) ? Math.max(1, Math.min(365, Math.floor(daysRaw))) : 30;
        const sinceSec = nowSec - days * 24 * 3600;

        const horizonRaw = Number(req.query.horizonSec);
        const horizonSec = Number.isFinite(horizonRaw) ? Math.max(60, Math.min(30 * 24 * 60 * 60, Math.floor(horizonRaw))) : null;

        const params = [sinceSec];
        let where = 'WHERE timestamp >= ?';
        if (horizonSec !== null) {
            where += ' AND horizon_sec = ?';
            params.push(horizonSec);
        }

        const summary = await query(
            `SELECT horizon_sec,
                    COUNT(1) AS count,
                    AVG(forward_return) AS avg_forward_return,
                    AVG(correct) AS win_rate
             FROM decision_outcomes
             ${where}
             GROUP BY horizon_sec
             ORDER BY horizon_sec ASC`,
            params
        );

        res.json({
            nowSec,
            sinceSec,
            days,
            horizonSec,
            rows: summary.rows || []
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// In production, serve the built frontend (single-service deploy).
// This must come after API routes so `/api/*` continues to work.
const distPath = path.resolve(__dirname, '..', 'dist');
if (process.env.NODE_ENV !== 'development' && fs.existsSync(distPath)) {
    app.use(express.static(distPath));

    app.get('*', (req, res, next) => {
        if (req.path && req.path.startsWith('/api')) return next();
        res.sendFile(path.join(distPath, 'index.html'));
    });
}

app.listen(port, () => {
    console.log(`Backend server running on port ${port}`);
});
