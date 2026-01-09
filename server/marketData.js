const { query } = require('./db');
const { calculateIndicators, generateMarketSummary } = require('./indicators');
const { fetchTradingVariablesCached, buildTradingVariablesForPair, listPairs } = require('./gainsTradingVariables');
const WebSocket = require('ws');

const DEFAULT_PRICE_WS_URL = 'wss://backend-pricing.eu.gains.trade';

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseCsvNumberList(value, fallback) {
    if (typeof value !== 'string' || value.trim() === '') return fallback;
    const parsed = value
        .split(',')
        .map(s => Number.parseInt(s.trim(), 10))
        .filter(n => Number.isFinite(n) && n > 0);
    return parsed.length > 0 ? Array.from(new Set(parsed)) : fallback;
}

function clampInt(value, { min, max }) {
    if (!Number.isFinite(value)) return null;
    const intValue = Math.trunc(value);
    return Math.max(min, Math.min(max, intValue));
}

function normalizeNumber(value) {
    const num = typeof value === 'number' ? value : Number(value);
    return Number.isFinite(num) ? num : null;
}

function dataToString(data) {
    if (typeof data === 'string') return data;
    if (data instanceof ArrayBuffer) return Buffer.from(data).toString('utf8');
    if (ArrayBuffer.isView(data)) {
        return Buffer.from(data.buffer, data.byteOffset, data.byteLength).toString('utf8');
    }
    return null;
}

const CONFIG = {
    enabled: process.env.MARKET_DATA_ENABLED !== 'false',
    wsUrl: process.env.GAINS_PRICE_STREAM_WS_URL || process.env.MARKET_PRICE_WS_URL || DEFAULT_PRICE_WS_URL,
    timeframesMin: parseCsvNumberList(process.env.MARKET_TIMEFRAMES_MINUTES, [1, 15]),
    historyEnabled: process.env.MARKET_STATE_HISTORY_ENABLED !== 'false',
    historyTimeframesMin: parseCsvNumberList(process.env.MARKET_STATE_HISTORY_TIMEFRAMES_MINUTES, null),
    backfillCandles: clampInt(normalizeNumber(process.env.MARKET_BACKFILL_CANDLES), { min: 60, max: 2_000 }) ?? 250,
    backfillConcurrency: clampInt(normalizeNumber(process.env.MARKET_BACKFILL_CONCURRENCY), { min: 1, max: 32 }) ?? 6,
    tradingVariablesRefreshMs: clampInt(normalizeNumber(process.env.MARKET_TRADING_VARIABLES_REFRESH_MS), { min: 10_000, max: 10 * 60_000 }) ?? 60_000
};

const HISTORY_TIMEFRAMES_SET = Array.isArray(CONFIG.historyTimeframesMin)
    ? new Set(CONFIG.historyTimeframesMin)
    : null;

let ws = null;
let reconnectTimer = null;
let reconnectDelayMs = 500;

const state = {
    startedAtMs: null,
    pairs: {
        discovered: 0,
        storedAtMs: null,
        lastError: null
    },
    tradingVariables: {
        storedAtMs: null,
        lastError: null
    },
    backfill: {
        inProgress: false,
        startedAtMs: null,
        finishedAtMs: null,
        totalTasks: 0,
        completedTasks: 0,
        errors: 0
    },
    ws: {
        connected: false,
        lastMessageAtMs: null,
        lastError: null
    }
};

// In-memory candle state: pairIndex -> timeframeMin -> { history: Candle[], current: Candle|null }
const candlesByPair = new Map();

// Throttle DB indicator writes for non-1m timeframes.
// Key: `${pairIndex}:${timeframeMin}` -> unix seconds of last successful upsert attempt.
const lastIndicatorUpsertAtSec = new Map();

function getPairTimeframeState(pairIndex, timeframeMin) {
    let timeframes = candlesByPair.get(pairIndex);
    if (!timeframes) {
        timeframes = new Map();
        candlesByPair.set(pairIndex, timeframes);
    }

    let tf = timeframes.get(timeframeMin);
    if (!tf) {
        tf = { history: [], current: null };
        timeframes.set(timeframeMin, tf);
    }
    return tf;
}

function timeframeSec(timeframeMin) {
    return timeframeMin * 60;
}

function getCandleBucketStart(tsMs, timeframeMin) {
    const sec = Math.floor(tsMs / 1000);
    const bucket = timeframeSec(timeframeMin);
    return Math.floor(sec / bucket) * bucket;
}

function clearReconnectTimer() {
    if (!reconnectTimer) return;
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
}

async function fetchJsonWithRetry(url, { maxAttempts = 4 } = {}) {
    let attempt = 0;
    let delayMs = 250;
    let lastErr = null;

    while (attempt < maxAttempts) {
        attempt += 1;
        try {
            const response = await fetch(url);
            if (response.ok) return response.json();

            const status = response.status;
            const retryable = status === 429 || (status >= 500 && status <= 599);
            if (!retryable) {
                throw new Error(`HTTP ${status} ${response.statusText}`);
            }
            lastErr = new Error(`HTTP ${status} ${response.statusText}`);
        } catch (err) {
            lastErr = err;
        }

        if (attempt >= maxAttempts) break;
        const jitter = Math.floor(Math.random() * 150);
        await sleep(delayMs + jitter);
        delayMs = Math.min(delayMs * 2, 5000);
    }

    throw lastErr ?? new Error('Request failed');
}

function scheduleReconnect() {
    if (reconnectTimer) return;
    reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        connectWs();
    }, reconnectDelayMs);
    reconnectDelayMs = Math.min(reconnectDelayMs * 2, 30_000);
}

function connectWs() {
    if (!CONFIG.enabled) return;
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;

    clearReconnectTimer();

    try {
        ws = new WebSocket(CONFIG.wsUrl);
    } catch (err) {
        state.ws.lastError = err?.message || String(err);
        scheduleReconnect();
        return;
    }

    ws.on('open', () => {
        state.ws.connected = true;
        state.ws.lastError = null;
        reconnectDelayMs = 500;
    });

    ws.on('message', (data) => {
        handleWsMessage(data);
    });

    ws.on('error', (err) => {
        state.ws.lastError = err?.message || 'WebSocket error';
        try {
            ws?.close();
        } catch {
            // ignore
        }
    });

    ws.on('close', () => {
        state.ws.connected = false;
        ws = null;
        scheduleReconnect();
    });
}

async function upsertPairsAndTradingVariables() {
    try {
        const tradingVariables = await fetchTradingVariablesCached();
        const now = Math.floor(Date.now() / 1000);

        const pairs = listPairs(tradingVariables);
        state.pairs.discovered = pairs.length;

        // Store pairs metadata
        for (const pair of pairs) {
            if (!Number.isFinite(pair.pair_index)) continue;
            await query(
                `INSERT INTO pairs (pair_index, from_symbol, to_symbol, updated_at)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT(pair_index) DO UPDATE SET
                    from_symbol = excluded.from_symbol,
                    to_symbol = excluded.to_symbol,
                    updated_at = excluded.updated_at`,
                [pair.pair_index, pair.from, pair.to, now]
            );
        }
        state.pairs.storedAtMs = Date.now();
        state.pairs.lastError = null;

        // Store per-pair trading variables (costs / OI / constraints)
        for (let pairIndex = 0; pairIndex < pairs.length; pairIndex++) {
            const tv = buildTradingVariablesForPair(tradingVariables, pairIndex);
            if (!tv) continue;

            await query(
                `INSERT INTO pair_trading_variables (
                    pair_index, updated_at, spread_percent,
                    group_name, group_min_leverage, group_max_leverage,
                    fee_position_size_percent, fee_oracle_position_size_percent, min_position_size_usd,
                    collateral_symbol, collateral_price_usd,
                    oi_long, oi_short, oi_skew_percent,
                    funding_enabled, funding_last_update_ts,
                    borrowing_rate_per_second_p, borrowing_last_update_ts
                ) VALUES (
                    $1, $2, $3,
                    $4, $5, $6,
                    $7, $8, $9,
                    $10, $11,
                    $12, $13, $14,
                    $15, $16,
                    $17, $18
                )
                ON CONFLICT(pair_index) DO UPDATE SET
                    updated_at = excluded.updated_at,
                    spread_percent = excluded.spread_percent,
                    group_name = excluded.group_name,
                    group_min_leverage = excluded.group_min_leverage,
                    group_max_leverage = excluded.group_max_leverage,
                    fee_position_size_percent = excluded.fee_position_size_percent,
                    fee_oracle_position_size_percent = excluded.fee_oracle_position_size_percent,
                    min_position_size_usd = excluded.min_position_size_usd,
                    collateral_symbol = excluded.collateral_symbol,
                    collateral_price_usd = excluded.collateral_price_usd,
                    oi_long = excluded.oi_long,
                    oi_short = excluded.oi_short,
                    oi_skew_percent = excluded.oi_skew_percent,
                    funding_enabled = excluded.funding_enabled,
                    funding_last_update_ts = excluded.funding_last_update_ts,
                    borrowing_rate_per_second_p = excluded.borrowing_rate_per_second_p,
                    borrowing_last_update_ts = excluded.borrowing_last_update_ts`,
                [
                    pairIndex,
                    now,
                    tv.pair?.spreadPercent ?? null,
                    tv.group?.name ?? null,
                    tv.group?.minLeverage ?? null,
                    tv.group?.maxLeverage ?? null,
                    tv.fees?.positionSizeFeePercent ?? null,
                    tv.fees?.oraclePositionSizeFeePercent ?? null,
                    tv.fees?.minPositionSizeUsd ?? null,
                    tv.openInterest?.collateralSymbol ?? null,
                    tv.openInterest?.collateralPriceUsd ?? null,
                    tv.openInterest?.long ?? null,
                    tv.openInterest?.short ?? null,
                    tv.openInterest?.skewPercent ?? null,
                    tv.funding?.enabled === true ? 1 : tv.funding?.enabled === false ? 0 : null,
                    tv.funding?.lastUpdateTs ?? null,
                    tv.borrowing?.borrowingRatePerSecondP ?? null,
                    tv.borrowing?.lastUpdateTs ?? null
                ]
            );
        }

        state.tradingVariables.storedAtMs = Date.now();
        state.tradingVariables.lastError = null;
    } catch (err) {
        state.pairs.lastError = err?.message || String(err);
        state.tradingVariables.lastError = err?.message || String(err);
    }
}

async function fetchOhlc(pairIndex, timeframeMin, candlesToFetch) {
    const to = Math.floor(Date.now() / 1000);
    const seconds = timeframeMin * 60 * candlesToFetch;
    const from = to - seconds;
    const resolution = String(timeframeMin);
    const url = `https://backend-pricing.eu.gains.trade/charts/${pairIndex}/${from}/${to}/${resolution}`;

    const data = await fetchJsonWithRetry(url);
    const table = data?.table;
    if (!Array.isArray(table) || table.length === 0) return [];

    const mapped = table
        .map(item => ({
            time: Math.floor(item.time / 1000),
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
        )
        .sort((a, b) => a.time - b.time);

    const deduped = [];
    for (const candle of mapped) {
        const prev = deduped[deduped.length - 1];
        if (prev && prev.time === candle.time) {
            deduped[deduped.length - 1] = candle;
        } else {
            deduped.push(candle);
        }
    }

    return deduped;
}

async function upsertMarketState(pairIndex, timeframeMin, latestCandleTime, latestClose, indicatorsLatest, overallBias) {
    const now = Math.floor(Date.now() / 1000);
    const params = [
        pairIndex,
        timeframeMin,
        latestCandleTime,
        latestClose,
        indicatorsLatest?.rsi ?? null,
        indicatorsLatest?.macd?.MACD ?? null,
        indicatorsLatest?.macd?.signal ?? null,
        indicatorsLatest?.macd?.histogram ?? null,
        indicatorsLatest?.bollingerBands?.upper ?? null,
        indicatorsLatest?.bollingerBands?.middle ?? null,
        indicatorsLatest?.bollingerBands?.lower ?? null,
        indicatorsLatest?.ema?.ema9 ?? null,
        indicatorsLatest?.ema?.ema21 ?? null,
        indicatorsLatest?.ema?.ema50 ?? null,
        indicatorsLatest?.ema?.ema200 ?? null,
        indicatorsLatest?.sma?.sma20 ?? null,
        indicatorsLatest?.sma?.sma50 ?? null,
        indicatorsLatest?.sma?.sma200 ?? null,
        indicatorsLatest?.atr ?? null,
        indicatorsLatest?.stochastic?.k ?? null,
        indicatorsLatest?.stochastic?.d ?? null,
        overallBias ?? null,
        now
    ];

    await query(
        `INSERT INTO market_state (
            pair_index, timeframe_min, candle_time, price,
            rsi, macd, macd_signal, macd_histogram,
            bb_upper, bb_middle, bb_lower,
            ema9, ema21, ema50, ema200,
            sma20, sma50, sma200,
            atr, stoch_k, stoch_d,
            overall_bias, updated_at
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7, $8,
            $9, $10, $11,
            $12, $13, $14, $15,
            $16, $17, $18,
            $19, $20, $21,
            $22, $23
        )
        ON CONFLICT(pair_index, timeframe_min) DO UPDATE SET
            candle_time = excluded.candle_time,
            price = excluded.price,
            rsi = excluded.rsi,
            macd = excluded.macd,
            macd_signal = excluded.macd_signal,
            macd_histogram = excluded.macd_histogram,
            bb_upper = excluded.bb_upper,
            bb_middle = excluded.bb_middle,
            bb_lower = excluded.bb_lower,
            ema9 = excluded.ema9,
            ema21 = excluded.ema21,
            ema50 = excluded.ema50,
            ema200 = excluded.ema200,
            sma20 = excluded.sma20,
            sma50 = excluded.sma50,
            sma200 = excluded.sma200,
            atr = excluded.atr,
            stoch_k = excluded.stoch_k,
            stoch_d = excluded.stoch_d,
            overall_bias = excluded.overall_bias,
            updated_at = excluded.updated_at`,
        params
    );

    // Persist an auditable history row for evaluation/replay.
    // This can be disabled or restricted to specific timeframes to control DB growth.
    if (CONFIG.historyEnabled && (!HISTORY_TIMEFRAMES_SET || HISTORY_TIMEFRAMES_SET.has(timeframeMin))) {
    await query(
        `INSERT INTO market_state_history (
            pair_index, timeframe_min, candle_time, price,
            rsi, macd, macd_signal, macd_histogram,
            bb_upper, bb_middle, bb_lower,
            ema9, ema21, ema50, ema200,
            sma20, sma50, sma200,
            atr, stoch_k, stoch_d,
            overall_bias, updated_at
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7, $8,
            $9, $10, $11,
            $12, $13, $14, $15,
            $16, $17, $18,
            $19, $20, $21,
            $22, $23
        )
        ON CONFLICT(pair_index, timeframe_min, candle_time) DO UPDATE SET
            price = excluded.price,
            rsi = excluded.rsi,
            macd = excluded.macd,
            macd_signal = excluded.macd_signal,
            macd_histogram = excluded.macd_histogram,
            bb_upper = excluded.bb_upper,
            bb_middle = excluded.bb_middle,
            bb_lower = excluded.bb_lower,
            ema9 = excluded.ema9,
            ema21 = excluded.ema21,
            ema50 = excluded.ema50,
            ema200 = excluded.ema200,
            sma20 = excluded.sma20,
            sma50 = excluded.sma50,
            sma200 = excluded.sma200,
            atr = excluded.atr,
            stoch_k = excluded.stoch_k,
            stoch_d = excluded.stoch_d,
            overall_bias = excluded.overall_bias,
            updated_at = excluded.updated_at`,
        params
    );
    }
}

async function computeAndStoreIndicators(pairIndex, timeframeMin, { includeCurrent = true } = {}) {
    const tf = getPairTimeframeState(pairIndex, timeframeMin);
    const series = includeCurrent && tf.current ? tf.history.concat([tf.current]) : tf.history;
    const indicators = calculateIndicators(series);
    if (!indicators?.latest) return;

    const latestCandle = series[series.length - 1];
    if (!latestCandle || !Number.isFinite(latestCandle.time) || !Number.isFinite(latestCandle.close)) return;
    const summary = generateMarketSummary(indicators, latestCandle.close);
    await upsertMarketState(pairIndex, timeframeMin, latestCandle.time, latestCandle.close, indicators.latest, summary.overallBias);
}

function trimHistory(tf, maxCandles) {
    if (tf.history.length <= maxCandles) return;
    tf.history = tf.history.slice(-maxCandles);
}

async function finalizeCandle(pairIndex, timeframeMin) {
    // Persist at the close of the candle that was just finalized (exclude the new in-progress candle).
    await computeAndStoreIndicators(pairIndex, timeframeMin, { includeCurrent: false });
}

function updateCandleFromTick(pairIndex, price, tsMs) {
    for (const timeframeMin of CONFIG.timeframesMin) {
        const tf = getPairTimeframeState(pairIndex, timeframeMin);
        const bucketStart = getCandleBucketStart(tsMs, timeframeMin);

        if (!tf.current) {
            tf.current = { time: bucketStart, open: price, high: price, low: price, close: price };
            continue;
        }

        if (tf.current.time === bucketStart) {
            tf.current.high = Math.max(tf.current.high, price);
            tf.current.low = Math.min(tf.current.low, price);
            tf.current.close = price;

            // For higher timeframes, don't rely solely on candle rollovers.
            // If the upstream websocket misses a boundary tick (or the process lags), the DB can go stale
            // and downstream systems (autotrade) will filter out all candidates.
            // Best-effort: recompute indicators including the in-progress candle at most once per timeframe.
            if (timeframeMin >= 15) {
                const nowSec = Math.floor(Date.now() / 1000);
                const key = `${pairIndex}:${timeframeMin}`;
                const lastUpsert = lastIndicatorUpsertAtSec.get(key) ?? 0;
                if (nowSec - lastUpsert >= timeframeSec(timeframeMin)) {
                    lastIndicatorUpsertAtSec.set(key, nowSec);
                    void computeAndStoreIndicators(pairIndex, timeframeMin, { includeCurrent: true });
                }
            }
            continue;
        }

        // Candle rolled over; finalize previous and start new.
        const closed = tf.current;
        tf.history.push(closed);
        trimHistory(tf, Math.max(CONFIG.backfillCandles, 220));
        tf.current = { time: bucketStart, open: price, high: price, low: price, close: price };

        // Mark a write attempt at rollover time (helps throttle the refresh path above).
        if (timeframeMin >= 15) {
            const key = `${pairIndex}:${timeframeMin}`;
            lastIndicatorUpsertAtSec.set(key, Math.floor(Date.now() / 1000));
        }

        void finalizeCandle(pairIndex, timeframeMin);
    }
}

function handleWsMessage(rawData) {
    const message = dataToString(rawData);
    if (!message) return;

    let payload;
    try {
        payload = JSON.parse(message);
    } catch {
        return;
    }

    if (!Array.isArray(payload)) return;
    if (payload.length === 1) {
        state.ws.lastMessageAtMs = Date.now();
        return;
    }

    state.ws.lastMessageAtMs = Date.now();
    const tsMs = normalizeNumber(payload[payload.length - 1]) ?? Date.now();

    for (let i = 0; i < payload.length - 1; i += 2) {
        const pairIndex = normalizeNumber(payload[i]);
        const price = normalizeNumber(payload[i + 1]);
        if (pairIndex === null || price === null) continue;
        updateCandleFromTick(pairIndex, price, tsMs);
    }
}

async function runBackfill() {
    if (!CONFIG.enabled) return;

    state.backfill.inProgress = true;
    state.backfill.startedAtMs = Date.now();
    state.backfill.finishedAtMs = null;
    state.backfill.completedTasks = 0;
    state.backfill.errors = 0;

    let tradingVariables;
    try {
        tradingVariables = await fetchTradingVariablesCached();
    } catch (err) {
        state.backfill.errors += 1;
        state.backfill.finishedAtMs = Date.now();
        state.pairs.lastError = err?.message || String(err);
        return;
    }

    const pairs = listPairs(tradingVariables);
    const tasks = [];
    for (const timeframeMin of CONFIG.timeframesMin) {
        for (let pairIndex = 0; pairIndex < pairs.length; pairIndex++) {
            tasks.push({ pairIndex, timeframeMin });
        }
    }

    state.backfill.totalTasks = tasks.length;

    let cursor = 0;
    const workers = Array.from({ length: CONFIG.backfillConcurrency }, async () => {
        while (cursor < tasks.length) {
            const taskIndex = cursor++;
            const task = tasks[taskIndex];

            try {
                const candles = await fetchOhlc(task.pairIndex, task.timeframeMin, CONFIG.backfillCandles);
                const tf = getPairTimeframeState(task.pairIndex, task.timeframeMin);
                tf.history = [];
                tf.current = null;

                if (candles.length > 0) {
                    const nowBucketStart = getCandleBucketStart(Date.now(), task.timeframeMin);
                    const last = candles[candles.length - 1];
                    if (last.time === nowBucketStart) {
                        tf.history = candles.slice(0, -1);
                        tf.current = last;
                    } else {
                        tf.history = candles;
                        tf.current = null;
                    }
                    trimHistory(tf, Math.max(CONFIG.backfillCandles, 220));

                    // Store a baseline snapshot (make it part of task success/failure).
                    await computeAndStoreIndicators(task.pairIndex, task.timeframeMin, { includeCurrent: true });
                }
            } catch (err) {
                state.backfill.errors += 1;
            } finally {
                state.backfill.completedTasks += 1;
            }
        }
    });

    await Promise.all(workers);
    state.backfill.finishedAtMs = Date.now();
    state.backfill.inProgress = false;
}

let tradingVariablesInterval = null;
let backfillPromise = null;

async function triggerBackfill() {
    if (backfillPromise) return backfillPromise;
    backfillPromise = (async () => {
        try {
            await runBackfill();
        } finally {
            state.backfill.inProgress = false;
            backfillPromise = null;
        }
    })();
    return backfillPromise;
}

async function start() {
    if (!CONFIG.enabled) return;
    if (state.startedAtMs) return;

    state.startedAtMs = Date.now();

    // Seed metadata and variables first; then backfill candles.
    await upsertPairsAndTradingVariables();
    void triggerBackfill();

    // Keep trading variables reasonably fresh for scoring filters.
    tradingVariablesInterval = setInterval(() => {
        void upsertPairsAndTradingVariables();
    }, CONFIG.tradingVariablesRefreshMs);

    connectWs();
}

function stop() {
    if (tradingVariablesInterval) clearInterval(tradingVariablesInterval);
    tradingVariablesInterval = null;
    clearReconnectTimer();
    try {
        ws?.close();
    } catch {
        // ignore
    }
    ws = null;
    state.ws.connected = false;
}

function getStatus() {
    return {
        config: CONFIG,
        state
    };
}

module.exports = {
    start,
    stop,
    getStatus,
    triggerBackfill
};
