const { query } = require('./db');
const { rankTop } = require('./opportunityRanker');

function parseFiniteNumber(value) {
    const num = typeof value === 'number' ? value : Number(value);
    return Number.isFinite(num) ? num : null;
}

function parseFiniteInt(value) {
    const num = typeof value === 'number' ? value : Number(value);
    if (!Number.isFinite(num)) return null;
    return Math.trunc(num);
}

function safeJsonParse(value) {
    if (typeof value !== 'string' || !value.trim()) return null;
    try {
        return JSON.parse(value);
    } catch {
        return null;
    }
}

function computeBbZ({ price, bb_upper, bb_middle, bb_lower }) {
    if (!Number.isFinite(price) || !Number.isFinite(bb_middle) || !Number.isFinite(bb_upper) || !Number.isFinite(bb_lower)) return null;
    const halfWidth = (bb_upper - bb_lower) / 2;
    if (!Number.isFinite(halfWidth) || halfWidth <= 0) return null;
    return (price - bb_middle) / halfWidth;
}

function computePctDiff(a, b) {
    if (!Number.isFinite(a) || !Number.isFinite(b) || b === 0) return null;
    return (a - b) / b;
}

function extractDecisionFeaturesFromAnalysis(analysis) {
    const indicators = analysis?.indicators || null;
    const price = parseFiniteNumber(analysis?.currentPrice) ?? parseFiniteNumber(analysis?.price) ?? null;

    const rsi = parseFiniteNumber(indicators?.rsi);
    const macdHist = parseFiniteNumber(indicators?.macd?.histogram ?? indicators?.macd_histogram);

    const ema21 = parseFiniteNumber(indicators?.ema?.ema21 ?? indicators?.ema21);
    const ema200 = parseFiniteNumber(indicators?.ema?.ema200 ?? indicators?.ema200);

    const bbUpper = parseFiniteNumber(indicators?.bollingerBands?.upper ?? indicators?.bb_upper);
    const bbMiddle = parseFiniteNumber(indicators?.bollingerBands?.middle ?? indicators?.bb_middle);
    const bbLower = parseFiniteNumber(indicators?.bollingerBands?.lower ?? indicators?.bb_lower);
    const bbZ = computeBbZ({ price, bb_upper: bbUpper, bb_middle: bbMiddle, bb_lower: bbLower });

    const atr = parseFiniteNumber(indicators?.atr);
    const atrPct = (Number.isFinite(atr) && Number.isFinite(price) && price > 0) ? (atr / price) : null;
    const stochK = parseFiniteNumber(indicators?.stochastic?.k ?? indicators?.stoch_k);

    const priceVsEma21 = computePctDiff(price, ema21);
    const priceVsEma200 = computePctDiff(price, ema200);

    return {
        price,
        rsi,
        macdHist,
        bbZ,
        atrPct,
        stochK,
        priceVsEma21,
        priceVsEma200
    };
}

function featureDistance(a, b) {
    // Weighted L1 distance on normalized-ish features.
    // Keep weights conservative to avoid overfitting.
    const terms = [
        { key: 'rsi', scale: 20, weight: 1.0 },
        { key: 'macdHist', scale: 0.005, weight: 1.0 },
        { key: 'bbZ', scale: 1.0, weight: 0.8 },
        { key: 'atrPct', scale: 0.01, weight: 0.6 },
        { key: 'stochK', scale: 30, weight: 0.4 },
        { key: 'priceVsEma21', scale: 0.01, weight: 0.7 },
        { key: 'priceVsEma200', scale: 0.02, weight: 0.5 }
    ];

    let acc = 0;
    let used = 0;
    for (const t of terms) {
        const va = a?.[t.key];
        const vb = b?.[t.key];
        if (!Number.isFinite(va) || !Number.isFinite(vb)) continue;
        const denom = t.scale;
        if (!Number.isFinite(denom) || denom <= 0) continue;
        acc += (Math.abs(va - vb) / denom) * t.weight;
        used += 1;
    }
    if (used === 0) return null;
    return acc / used;
}

async function getSimilarDecisions({
    pairIndex,
    timeframeMin,
    lookbackDays = 60,
    limit = 8,
    includeOtherPairs = true,
    requireSameDirection = false,
    currentFeatures = null
} = {}) {
    const tf = parseFiniteInt(timeframeMin);
    const p = parseFiniteInt(pairIndex);
    if (!Number.isFinite(tf) || tf <= 0) return [];
    if (!Number.isFinite(p) || p < 0) return [];

    const nowSec = Math.floor(Date.now() / 1000);
    const sinceSec = nowSec - Math.max(1, Math.min(365, parseFiniteInt(lookbackDays) || 60)) * 24 * 3600;
    const maxRows = 2000;

    const where = [
        'timestamp >= $1',
        'timeframe_min = $2',
        "decision = 'execute_trade'",
        'analysis IS NOT NULL'
    ];
    const params = [sinceSec, tf];
    if (!includeOtherPairs) {
        where.push('pair_index = $3');
        params.push(p);
    }

    const sql = `SELECT id, timestamp, pair_index, timeframe_min, candle_time, analysis, confidence, trade_id
                 FROM bot_decisions
                 WHERE ${where.join(' AND ')}
                 ORDER BY timestamp DESC
                 LIMIT ${maxRows}`;

    const decisions = await query(sql, params);
    const rows = decisions.rows || [];

    const queryFeatures = currentFeatures && typeof currentFeatures === 'object'
        ? currentFeatures
        : await (async () => {
            const snap = await getMarketSnapshot(p, tf);
            if (!snap) return null;
            return {
                price: snap.price,
                rsi: snap.indicators?.rsi ?? null,
                macdHist: snap.indicators?.macd_histogram ?? null,
                bbZ: computeBbZ({
                    price: snap.price,
                    bb_upper: snap.indicators?.bb_upper,
                    bb_middle: snap.indicators?.bb_middle,
                    bb_lower: snap.indicators?.bb_lower
                }),
                atrPct: (Number.isFinite(snap.indicators?.atr) && Number.isFinite(snap.price) && snap.price > 0) ? (snap.indicators.atr / snap.price) : null,
                stochK: snap.indicators?.stoch_k ?? null,
                priceVsEma21: computePctDiff(snap.price, snap.indicators?.ema21),
                priceVsEma200: computePctDiff(snap.price, snap.indicators?.ema200)
            };
        })();

    if (!queryFeatures) return [];

    let queryDirection = null;
    if (requireSameDirection) {
        queryDirection = null;
    }

    const scored = [];
    for (const row of rows) {
        const analysis = safeJsonParse(row.analysis);
        if (!analysis) continue;

        const toolCallName = analysis?.toolCall?.name;
        if (toolCallName !== 'execute_trade') continue;

        const direction = analysis?.toolCall?.args?.direction;
        if (direction !== 'LONG' && direction !== 'SHORT') continue;

        if (requireSameDirection) {
            // If caller provides current direction, enforce it. Otherwise infer nothing.
            if (queryDirection && direction !== queryDirection) continue;
        }

        const features = extractDecisionFeaturesFromAnalysis(analysis);
        const dist = featureDistance(queryFeatures, features);
        if (dist === null) continue;

        scored.push({
            decisionId: parseFiniteInt(row.id),
            timestamp: parseFiniteInt(row.timestamp),
            pairIndex: parseFiniteInt(row.pair_index),
            timeframeMin: parseFiniteInt(row.timeframe_min),
            candleTime: parseFiniteInt(row.candle_time),
            confidence: parseFiniteNumber(row.confidence),
            tradeId: parseFiniteInt(row.trade_id),
            direction,
            distance: dist,
            features
        });
    }

    scored.sort((a, b) => a.distance - b.distance);
    const top = scored.slice(0, Math.min(Math.max(parseFiniteInt(limit) || 8, 1), 25));
    if (top.length === 0) return [];

    const ids = top.map((t) => t.decisionId).filter((v) => Number.isFinite(v));
    const placeholders = ids.map((_, i) => `$${i + 1}`).join(',');

    const outcomes = await query(
        `SELECT decision_id, horizon_sec,
                AVG(forward_return) AS avg_forward_return,
                AVG(correct) AS hit_rate,
                COUNT(*) AS n
         FROM decision_outcomes
         WHERE decision_id IN (${placeholders})
         GROUP BY decision_id, horizon_sec`,
        ids
    );

    const outcomesByDecision = new Map();
    for (const r of outcomes.rows || []) {
        const decisionId = parseFiniteInt(r.decision_id);
        if (!Number.isFinite(decisionId)) continue;
        const horizon = parseFiniteInt(r.horizon_sec);
        if (!Number.isFinite(horizon)) continue;
        const entry = outcomesByDecision.get(decisionId) || {};
        entry[horizon] = {
            n: parseFiniteInt(r.n),
            avg_forward_return: parseFiniteNumber(r.avg_forward_return),
            hit_rate: parseFiniteNumber(r.hit_rate)
        };
        outcomesByDecision.set(decisionId, entry);
    }

    const tradeIds = top.map((t) => t.tradeId).filter((v) => Number.isFinite(v));
    const tradePnlById = new Map();
    if (tradeIds.length > 0) {
        const tradePlaceholders = tradeIds.map((_, i) => `$${i + 1}`).join(',');
        const trades = await query(
            `SELECT id, status, pnl, entry_time, exit_time
             FROM trades
             WHERE id IN (${tradePlaceholders})`,
            tradeIds
        );
        for (const t of trades.rows || []) {
            const id = parseFiniteInt(t.id);
            if (!Number.isFinite(id)) continue;
            tradePnlById.set(id, {
                status: t.status ?? null,
                pnl: parseFiniteNumber(t.pnl),
                entry_time: parseFiniteInt(t.entry_time),
                exit_time: parseFiniteInt(t.exit_time)
            });
        }
    }

    return top.map((t) => ({
        ...t,
        outcomes: outcomesByDecision.get(t.decisionId) || {},
        trade: t.tradeId ? tradePnlById.get(t.tradeId) || null : null
    }));
}

function computeCostPercentFromTradingVariablesRow(row) {
    const spread = parseFiniteNumber(row?.spread_percent) ?? 0;
    const fee = parseFiniteNumber(row?.fee_position_size_percent) ?? 0;
    const oracleFee = parseFiniteNumber(row?.fee_oracle_position_size_percent) ?? 0;
    return spread + fee + oracleFee;
}

function computeOiTotalFromTradingVariablesRow(row) {
    const oiLong = parseFiniteNumber(row?.oi_long);
    const oiShort = parseFiniteNumber(row?.oi_short);
    if (oiLong === null || oiShort === null) return null;
    return oiLong + oiShort;
}

async function getMarketStateRow(pairIndex, timeframeMin) {
    const result = await query(
        `SELECT ms.*,
                p.from_symbol AS from_symbol,
                p.to_symbol AS to_symbol
         FROM market_state ms
         LEFT JOIN pairs p ON p.pair_index = ms.pair_index
         WHERE ms.pair_index = $1 AND ms.timeframe_min = $2
         LIMIT 1`,
        [pairIndex, timeframeMin]
    );
    return result.rows?.[0] ?? null;
}

async function getTradingVariablesRow(pairIndex) {
    const result = await query(
        `SELECT *
         FROM pair_trading_variables
         WHERE pair_index = $1
         LIMIT 1`,
        [pairIndex]
    );
    return result.rows?.[0] ?? null;
}

async function getMarketSnapshot(pairIndex, timeframeMin) {
    const row = await getMarketStateRow(pairIndex, timeframeMin);
    if (!row) return null;

    const nowSec = Math.floor(Date.now() / 1000);
    const updatedAt = parseFiniteInt(row.updated_at);

    return {
        pairIndex,
        timeframeMin,
        candleTime: parseFiniteInt(row.candle_time),
        price: parseFiniteNumber(row.price),
        overallBias: row.overall_bias ?? null,
        updatedAt,
        ageSec: updatedAt !== null ? Math.max(0, nowSec - updatedAt) : null,
        pair: {
            from: row.from_symbol ?? null,
            to: row.to_symbol ?? null
        },
        indicators: {
            rsi: parseFiniteNumber(row.rsi),
            macd: parseFiniteNumber(row.macd),
            macd_signal: parseFiniteNumber(row.macd_signal),
            macd_histogram: parseFiniteNumber(row.macd_histogram),
            bb_upper: parseFiniteNumber(row.bb_upper),
            bb_middle: parseFiniteNumber(row.bb_middle),
            bb_lower: parseFiniteNumber(row.bb_lower),
            ema9: parseFiniteNumber(row.ema9),
            ema21: parseFiniteNumber(row.ema21),
            ema50: parseFiniteNumber(row.ema50),
            ema200: parseFiniteNumber(row.ema200),
            sma20: parseFiniteNumber(row.sma20),
            sma50: parseFiniteNumber(row.sma50),
            sma200: parseFiniteNumber(row.sma200),
            atr: parseFiniteNumber(row.atr),
            stoch_k: parseFiniteNumber(row.stoch_k),
            stoch_d: parseFiniteNumber(row.stoch_d)
        }
    };
}

async function getCostsAndLiquidity(pairIndex) {
    const row = await getTradingVariablesRow(pairIndex);
    if (!row) return null;

    const nowSec = Math.floor(Date.now() / 1000);
    const updatedAt = parseFiniteInt(row.updated_at);
    const costPercent = computeCostPercentFromTradingVariablesRow(row);
    const oiTotal = computeOiTotalFromTradingVariablesRow(row);

    return {
        pairIndex,
        updatedAt,
        ageSec: updatedAt !== null ? Math.max(0, nowSec - updatedAt) : null,
        costs: {
            spread_percent: parseFiniteNumber(row.spread_percent),
            fee_position_size_percent: parseFiniteNumber(row.fee_position_size_percent),
            fee_oracle_position_size_percent: parseFiniteNumber(row.fee_oracle_position_size_percent),
            total_percent: parseFiniteNumber(costPercent)
        },
        liquidity: {
            collateral_symbol: row.collateral_symbol ?? null,
            collateral_price_usd: parseFiniteNumber(row.collateral_price_usd),
            oi_long: parseFiniteNumber(row.oi_long),
            oi_short: parseFiniteNumber(row.oi_short),
            oi_total: parseFiniteNumber(oiTotal),
            oi_skew_percent: parseFiniteNumber(row.oi_skew_percent)
        },
        constraints: {
            group_name: row.group_name ?? null,
            group_min_leverage: parseFiniteNumber(row.group_min_leverage),
            group_max_leverage: parseFiniteNumber(row.group_max_leverage),
            min_position_size_usd: parseFiniteNumber(row.min_position_size_usd)
        },
        funding: {
            enabled: row.funding_enabled === 1 ? true : row.funding_enabled === 0 ? false : null,
            last_update_ts: parseFiniteInt(row.funding_last_update_ts)
        },
        borrowing: {
            borrowing_rate_per_second_p: row.borrowing_rate_per_second_p ?? null,
            last_update_ts: parseFiniteInt(row.borrowing_last_update_ts)
        }
    };
}

async function getRegimeSnapshot(pairIndex, regimeTimeframeMin) {
    const snapshot = await getMarketSnapshot(pairIndex, regimeTimeframeMin);
    if (!snapshot) return null;

    const rsi = snapshot.indicators.rsi;
    const ema50 = snapshot.indicators.ema50;
    const ema200 = snapshot.indicators.ema200;
    const price = snapshot.price;

    let regime = null;
    if (price !== null && ema200 !== null) {
        regime = price > ema200 ? 'BULL' : 'BEAR';
    }

    let trend = null;
    if (ema50 !== null && ema200 !== null) {
        trend = ema50 > ema200 ? 'UP' : 'DOWN';
    }

    const chop = typeof rsi === 'number' && rsi > 45 && rsi < 55;

    return {
        ...snapshot,
        regime,
        trend,
        chop
    };
}

async function getOpenPositions({ source = null } = {}) {
    const params = [];
    let where = "WHERE status IN ('OPEN', 'PENDING')";
    if (source) {
        where += ' AND source = $1';
        params.push(source);
    }

    const result = await query(
        `SELECT *
         FROM trades
         ${where}
         ORDER BY entry_time DESC`,
        params
    );

    return result.rows || [];
}

function summarizeExposure(trades) {
    const perPair = new Map();
    let totalNotional = 0;

    for (const t of trades) {
        const pairIndex = parseFiniteInt(t.pair_index);
        const collateral = parseFiniteNumber(t.collateral) ?? 0;
        const leverage = parseFiniteNumber(t.leverage) ?? 0;
        const notional = collateral * leverage;
        if (pairIndex === null || notional <= 0) continue;

        totalNotional += notional;

        const entry = perPair.get(pairIndex) || { pairIndex, count: 0, notional: 0, longNotional: 0, shortNotional: 0 };
        entry.count += 1;
        entry.notional += notional;
        if (t.direction === 'LONG') entry.longNotional += notional;
        if (t.direction === 'SHORT') entry.shortNotional += notional;
        perPair.set(pairIndex, entry);
    }

    const byPair = Array.from(perPair.values()).sort((a, b) => b.notional - a.notional);

    return {
        totalNotional,
        pairCount: byPair.length,
        byPair
    };
}

async function getOpenExposure({ source = null } = {}) {
    const trades = await getOpenPositions({ source });
    const exposure = summarizeExposure(trades);
    return {
        openPositions: trades.map((t) => ({
            id: parseFiniteInt(t.id),
            pair_index: parseFiniteInt(t.pair_index),
            status: t.status ?? null,
            direction: t.direction ?? null,
            collateral: parseFiniteNumber(t.collateral),
            leverage: parseFiniteNumber(t.leverage),
            entry_price: parseFiniteNumber(t.entry_price),
            trigger_price: parseFiniteNumber(t.trigger_price),
            stop_loss_price: parseFiniteNumber(t.stop_loss_price),
            take_profit_price: parseFiniteNumber(t.take_profit_price),
            entry_time: parseFiniteInt(t.entry_time)
        })),
        exposure
    };
}

async function getPositionRisk(tradeId, { currentPrice = null, timeframeMin = null } = {}) {
    const tradeResult = await query('SELECT * FROM trades WHERE id = $1 LIMIT 1', [tradeId]);
    const trade = tradeResult.rows?.[0] ?? null;
    if (!trade) return null;

    const pairIndex = parseFiniteInt(trade.pair_index);
    let priceNow = parseFiniteNumber(currentPrice);

    if (priceNow === null && pairIndex !== null && timeframeMin !== null) {
        const snapshot = await getMarketSnapshot(pairIndex, timeframeMin);
        priceNow = snapshot?.price ?? null;
    }

    const entryPrice = parseFiniteNumber(trade.entry_price);
    const direction = trade.direction;
    const collateral = parseFiniteNumber(trade.collateral);
    const leverage = parseFiniteNumber(trade.leverage);
    const sl = parseFiniteNumber(trade.stop_loss_price);
    const tp = parseFiniteNumber(trade.take_profit_price);

    let pnl = null;
    if (priceNow !== null && entryPrice !== null && collateral !== null && leverage !== null && entryPrice > 0) {
        const rawReturn = direction === 'LONG'
            ? (priceNow - entryPrice) / entryPrice
            : (entryPrice - priceNow) / entryPrice;
        pnl = rawReturn * (collateral * leverage);
    }

    const distanceToSl = sl !== null && priceNow !== null
        ? (direction === 'LONG' ? (priceNow - sl) : (sl - priceNow))
        : null;

    const distanceToTp = tp !== null && priceNow !== null
        ? (direction === 'LONG' ? (tp - priceNow) : (priceNow - tp))
        : null;

    return {
        tradeId: parseFiniteInt(trade.id),
        pairIndex,
        status: trade.status ?? null,
        direction: direction ?? null,
        entryPrice,
        currentPrice: priceNow,
        pnl,
        stopLossPrice: sl,
        takeProfitPrice: tp,
        distanceToSl,
        distanceToTp,
        entry_time: parseFiniteInt(trade.entry_time)
    };
}

async function getRankedCandidates({ timeframeMin, limit = 10, minOiTotal = 1, maxCostPercent = 0.25 } = {}) {
    const tf = parseFiniteInt(timeframeMin);
    if (!Number.isFinite(tf) || tf <= 0) return [];

    const result = await query(
        `SELECT ms.*,
                tv.spread_percent,
                tv.fee_position_size_percent,
                tv.fee_oracle_position_size_percent,
                tv.oi_long,
                tv.oi_short,
                tv.oi_skew_percent,
                tv.updated_at AS tv_updated_at,
                p.from_symbol,
                p.to_symbol
         FROM market_state ms
         LEFT JOIN pair_trading_variables tv ON tv.pair_index = ms.pair_index
         LEFT JOIN pairs p ON p.pair_index = ms.pair_index
         WHERE ms.timeframe_min = $1`,
        [tf]
    );

    const scored = rankTop(result.rows || [], { minOiTotal, maxCostPercent });

    const max = Math.min(Math.max(parseFiniteInt(limit) || 10, 1), 100);
    return scored.slice(0, max).map((s) => ({
        pairIndex: parseFiniteInt(s.row.pair_index),
        timeframeMin: parseFiniteInt(s.row.timeframe_min),
        candleTime: parseFiniteInt(s.row.candle_time),
        price: parseFiniteNumber(s.row.price),
        overallBias: s.row.overall_bias ?? null,
        pair: {
            from: s.row.from_symbol ?? null,
            to: s.row.to_symbol ?? null
        },
        score: s.score,
        side: s.side,
        reasons: s.reasons,
        metrics: s.metrics,
        costsAndLiquidity: {
            costPercent: computeCostPercentFromTradingVariablesRow(s.row),
            oiTotal: computeOiTotalFromTradingVariablesRow(s.row),
            oiSkewPercent: parseFiniteNumber(s.row.oi_skew_percent),
            tvUpdatedAt: parseFiniteInt(s.row.tv_updated_at)
        }
    }));
}

module.exports = {
    computeCostPercentFromTradingVariablesRow,
    computeOiTotalFromTradingVariablesRow,
    getMarketSnapshot,
    getCostsAndLiquidity,
    getRegimeSnapshot,
    getOpenExposure,
    getPositionRisk,
    getRankedCandidates,
    getSimilarDecisions
};
