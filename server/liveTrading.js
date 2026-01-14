const { query } = require('./db');
const symphonyClient = require('./symphonyClient');

function parseFiniteNumber(value) {
    const parsed = typeof value === 'number' ? value : Number.parseFloat(String(value));
    return Number.isFinite(parsed) ? parsed : null;
}

function parseFiniteInt(value) {
    const parsed = typeof value === 'number' ? value : Number.parseInt(String(value), 10);
    return Number.isFinite(parsed) ? parsed : null;
}

function parseEnvBool(name, fallback = false) {
    const raw = process.env[name];
    if (typeof raw !== 'string') return fallback;
    const v = raw.trim().toLowerCase();
    if (['true', '1', 'yes', 'y', 'on'].includes(v)) return true;
    if (['false', '0', 'no', 'n', 'off'].includes(v)) return false;
    return fallback;
}

function clampNumber(value, { min, max }) {
    if (!Number.isFinite(value)) return null;
    return Math.max(min, Math.min(max, value));
}

function normalizeExecutionProvider(value) {
    const raw = typeof value === 'string' ? value.trim().toLowerCase() : '';
    if (raw === 'live' || raw === 'symphony') return 'live';
    return 'paper';
}

function mapLiveStatusToBotStatus(status) {
    // The bot guardrails only understand OPEN/PENDING.
    if (status === 'CLOSED') return 'CLOSED';
    if (status === 'ERROR') return 'ERROR';
    return 'OPEN';
}

async function getLiveTradingSettings() {
    const result = await query('SELECT * FROM live_trading_settings WHERE id = 1 LIMIT 1');
    const row = result.rows?.[0] ?? null;
    if (!row) {
        // Should not happen (db.js inserts default), but be resilient.
        await query(
            `INSERT OR IGNORE INTO live_trading_settings(id, enabled, pool_usd, updated_at, created_at)
             VALUES (1, 0, 0, strftime('%s','now'), strftime('%s','now'))`
        );
        const again = await query('SELECT * FROM live_trading_settings WHERE id = 1 LIMIT 1');
        return again.rows?.[0] ?? null;
    }
    return row;
}

async function updateLiveTradingSettings(patch = {}) {
    const current = await getLiveTradingSettings();
    if (!current) return null;

    const enabled = typeof patch.enabled === 'boolean'
        ? (patch.enabled ? 1 : 0)
        : (patch.enabled === 1 ? 1 : patch.enabled === 0 ? 0 : null);

    const poolUsd = patch.pool_usd !== undefined ? clampNumber(parseFiniteNumber(patch.pool_usd), { min: 0, max: 1e9 }) : null;
    const maxTradeWeightPct = patch.max_trade_weight_pct !== undefined ? clampNumber(parseFiniteNumber(patch.max_trade_weight_pct), { min: 0, max: 100 }) : null;
    const maxTotalOpenWeightPct = patch.max_total_open_weight_pct !== undefined ? clampNumber(parseFiniteNumber(patch.max_total_open_weight_pct), { min: 0, max: 100 }) : null;
    const maxOpenPositions = patch.max_open_positions !== undefined ? clampNumber(parseFiniteInt(patch.max_open_positions), { min: 0, max: 1000 }) : null;
    const maxLeverage = patch.max_leverage !== undefined ? clampNumber(parseFiniteNumber(patch.max_leverage), { min: 1, max: 200 }) : null;
    const dailyLossLimitUsd = patch.daily_loss_limit_usd !== undefined ? clampNumber(parseFiniteNumber(patch.daily_loss_limit_usd), { min: 0, max: 1e9 }) : null;

    const next = {
        enabled: enabled ?? current.enabled,
        pool_usd: poolUsd ?? current.pool_usd,
        max_trade_weight_pct: maxTradeWeightPct ?? current.max_trade_weight_pct,
        max_total_open_weight_pct: maxTotalOpenWeightPct ?? current.max_total_open_weight_pct,
        max_open_positions: maxOpenPositions ?? current.max_open_positions,
        max_leverage: maxLeverage ?? current.max_leverage,
        daily_loss_limit_usd: dailyLossLimitUsd ?? current.daily_loss_limit_usd
    };

    // Keep max_total_open_weight_pct >= max_trade_weight_pct for sanity.
    if (Number.isFinite(next.max_trade_weight_pct) && Number.isFinite(next.max_total_open_weight_pct)) {
        next.max_total_open_weight_pct = Math.max(next.max_total_open_weight_pct, next.max_trade_weight_pct);
    }

    await query(
        `UPDATE live_trading_settings
         SET enabled = $1,
             pool_usd = $2,
             max_trade_weight_pct = $3,
             max_total_open_weight_pct = $4,
             max_open_positions = $5,
             max_leverage = $6,
             daily_loss_limit_usd = $7,
             updated_at = strftime('%s','now')
         WHERE id = 1`,
        [
            next.enabled,
            next.pool_usd,
            next.max_trade_weight_pct,
            next.max_total_open_weight_pct,
            next.max_open_positions,
            next.max_leverage,
            next.daily_loss_limit_usd
        ]
    );

    return (await getLiveTradingSettings()) ?? null;
}

async function listLiveTrades({ limit = 200 } = {}) {
    const safeLimit = Math.min(Math.max(parseFiniteInt(limit) ?? 200, 1), 1000);
    const result = await query(
        `SELECT *
         FROM live_trades
         ORDER BY created_at DESC, id DESC
         LIMIT $1`,
        [safeLimit]
    );
    return result.rows || [];
}

function isOpenishStatus(status) {
    return status === 'OPENING' || status === 'OPEN' || status === 'CLOSING';
}

async function getOpenLiveTrades({ pairIndex = null } = {}) {
    const params = [];
    let where = "WHERE status IN ('OPENING', 'OPEN', 'CLOSING')";
    if (Number.isFinite(pairIndex)) {
        where += ' AND pair_index = $1';
        params.push(pairIndex);
    }
    const result = await query(
        `SELECT *
         FROM live_trades
         ${where}
         ORDER BY created_at DESC, id DESC`,
        params
    );
    return result.rows || [];
}

async function getOpenLivePositionsForBot({ pairIndex = null } = {}) {
    const rows = await getOpenLiveTrades({ pairIndex });
    return rows.map((row) => ({
        id: parseFiniteInt(row.id),
        pair_index: parseFiniteInt(row.pair_index),
        status: mapLiveStatusToBotStatus(row.status),
        direction: row.direction ?? null,
        collateral: parseFiniteNumber(row.requested_collateral_usd),
        leverage: parseFiniteNumber(row.leverage),
        entry_price: parseFiniteNumber(row.requested_entry_price),
        trigger_price: parseFiniteNumber(row.trigger_price),
        stop_loss_price: parseFiniteNumber(row.stop_loss_price),
        take_profit_price: parseFiniteNumber(row.take_profit_price),
        entry_time: parseFiniteInt(row.opened_at ?? row.created_at) ?? null
    }));
}

async function resolveSymbolForPairIndex(pairIndex) {
    const result = await query('SELECT from_symbol FROM pairs WHERE pair_index = $1 LIMIT 1', [pairIndex]);
    const row = result.rows?.[0] ?? null;
    const symbol = typeof row?.from_symbol === 'string' ? row.from_symbol.trim() : '';
    return symbol || null;
}

async function computeOpenWeightPct() {
    const result = await query(
        `SELECT COALESCE(SUM(resolved_weight_pct), 0) AS total
         FROM live_trades
         WHERE status IN ('OPENING', 'OPEN', 'CLOSING')`
    );
    const totalRaw = result.rows?.[0]?.total ?? 0;
    return parseFiniteNumber(totalRaw) ?? 0;
}

async function countOpenPositions() {
    const result = await query(
        `SELECT COUNT(1) AS count
         FROM live_trades
         WHERE status IN ('OPENING', 'OPEN', 'CLOSING')`
    );
    const countRaw = result.rows?.[0]?.count ?? 0;
    return parseFiniteInt(countRaw) ?? 0;
}

function computeWeightPct({ collateralUsd, poolUsd }) {
    if (!Number.isFinite(collateralUsd) || collateralUsd <= 0) return null;
    if (!Number.isFinite(poolUsd) || poolUsd <= 0) return null;
    return (collateralUsd / poolUsd) * 100;
}

function getEnvStatus() {
    const allowed = parseEnvBool('LIVE_TRADING_ALLOWED', false);
    const { apiKey, agentId, baseUrl } = symphonyClient.getSymphonyConfigFromEnv();
    const configured = !!apiKey && !!agentId;
    return {
        allowed,
        configured,
        baseUrl,
        agentIdConfigured: !!agentId,
        apiKeyConfigured: !!apiKey
    };
}

async function openLiveTradeFromDecision({
    decisionId,
    executionMode = 'manual',
    pairIndex,
    direction,
    collateralUsd,
    leverage,
    currentPrice,
    stopLossPrice,
    takeProfitPrice
}) {
    const env = getEnvStatus();
    if (!env.allowed) {
        return { success: false, error: 'Live trading is disabled by server config (set LIVE_TRADING_ALLOWED=true)' };
    }
    if (!env.configured) {
        return { success: false, error: 'Symphony API is not configured (set SYMPHONY_API_KEY and SYMPHONY_AGENT_ID)' };
    }

    const settings = await getLiveTradingSettings();
    if (!settings || settings.enabled !== 1) {
        return { success: false, error: 'Live trading is disabled (toggle it on and set a pool balance)' };
    }

    const poolUsd = parseFiniteNumber(settings.pool_usd);
    if (!Number.isFinite(poolUsd) || poolUsd <= 0) {
        return { success: false, error: 'Live trading pool is not set (set a starting balance)' };
    }

    const parsedCollateral = parseFiniteNumber(collateralUsd);
    const parsedLeverage = parseFiniteNumber(leverage);
    if (!Number.isFinite(parsedCollateral) || parsedCollateral <= 0) {
        return { success: false, error: 'Invalid collateral' };
    }
    if (!Number.isFinite(parsedLeverage) || parsedLeverage <= 0) {
        return { success: false, error: 'Invalid leverage' };
    }

    const maxLev = parseFiniteNumber(settings.max_leverage);
    if (Number.isFinite(maxLev) && parsedLeverage > maxLev) {
        return { success: false, error: `Leverage too high (${parsedLeverage}x > ${maxLev}x)` };
    }

    const symbol = await resolveSymbolForPairIndex(pairIndex);
    if (!symbol) {
        return { success: false, error: 'Unable to resolve symbol for pair (pairs table missing from_symbol)' };
    }

    const weightPctRaw = computeWeightPct({ collateralUsd: parsedCollateral, poolUsd });
    if (weightPctRaw === null) {
        return { success: false, error: 'Unable to compute live weight from pool' };
    }

    const weightPct = Math.max(0, Math.min(100, weightPctRaw));
    const maxTradeWeightPct = parseFiniteNumber(settings.max_trade_weight_pct);
    if (Number.isFinite(maxTradeWeightPct) && weightPct > maxTradeWeightPct + 1e-9) {
        return { success: false, error: `Trade size too large (${weightPct.toFixed(2)}% > ${maxTradeWeightPct}% max)` };
    }

    const openWeight = await computeOpenWeightPct();
    const maxTotalWeight = parseFiniteNumber(settings.max_total_open_weight_pct);
    if (Number.isFinite(maxTotalWeight) && openWeight + weightPct > maxTotalWeight + 1e-9) {
        return { success: false, error: `Total open weight too high (${(openWeight + weightPct).toFixed(2)}% > ${maxTotalWeight}% max)` };
    }

    const openCount = await countOpenPositions();
    const maxOpenPositions = parseFiniteInt(settings.max_open_positions);
    if (Number.isFinite(maxOpenPositions) && openCount >= maxOpenPositions) {
        return { success: false, error: `Max live open positions reached (${openCount} >= ${maxOpenPositions})` };
    }

    const nowSec = Math.floor(Date.now() / 1000);
    const requestPayload = {
        agentId: env.agentIdConfigured ? symphonyClient.getSymphonyConfigFromEnv().agentId : null,
        symbol,
        action: direction,
        weight: weightPct,
        leverage: parsedLeverage,
        orderOptions: {
            triggerPrice: 0,
            stopLossPrice: stopLossPrice ?? 0,
            takeProfitPrice: takeProfitPrice ?? 0
        }
    };

    const insert = await query(
        `INSERT INTO live_trades (
            decision_id,
            execution_mode,
            pair_index,
            symbol,
            direction,
            requested_collateral_usd,
            resolved_weight_pct,
            leverage,
            requested_entry_price,
            stop_loss_price,
            take_profit_price,
            trigger_price,
            batch_id,
            status,
            created_at,
            opened_at,
            request_json
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17
        )`,
        [
            Number.isFinite(decisionId) ? decisionId : null,
            executionMode,
            pairIndex,
            symbol,
            direction,
            parsedCollateral,
            weightPct,
            parsedLeverage,
            parseFiniteNumber(currentPrice),
            parseFiniteNumber(stopLossPrice),
            parseFiniteNumber(takeProfitPrice),
            null,
            null,
            'OPENING',
            nowSec,
            null,
            JSON.stringify(requestPayload)
        ]
    );

    const liveTradeId = insert.lastID;

    try {
        const { apiKey, agentId, baseUrl } = symphonyClient.getSymphonyConfigFromEnv();
        const openResponse = await symphonyClient.batchOpen({
            agentId,
            apiKey,
            baseUrl,
            symbol,
            action: direction,
            weight: weightPct,
            leverage: parsedLeverage,
            orderOptions: requestPayload.orderOptions
        });

        const batchId = typeof openResponse?.data?.batchId === 'string' ? openResponse.data.batchId : null;

        await query(
            `UPDATE live_trades
             SET batch_id = $1,
                 status = $2,
                 opened_at = $3,
                 open_response_json = $4,
                 last_error = NULL
             WHERE id = $5`,
            [
                batchId,
                'OPEN',
                nowSec,
                JSON.stringify(openResponse?.data ?? null),
                liveTradeId
            ]
        );

        return {
            success: true,
            liveTradeId,
            batchId,
            symbol,
            weightPct,
            response: openResponse?.data ?? null
        };
    } catch (err) {
        await query(
            `UPDATE live_trades
             SET status = $1,
                 last_error = $2
             WHERE id = $3`,
            [
                'ERROR',
                err?.message || String(err),
                liveTradeId
            ]
        );
        return { success: false, error: err?.message || String(err) };
    }
}

async function closeLiveTrade({
    decisionId,
    executionMode = 'manual',
    liveTradeId
}) {
    const env = getEnvStatus();
    if (!env.allowed) {
        return { success: false, error: 'Live trading is disabled by server config (set LIVE_TRADING_ALLOWED=true)' };
    }
    if (!env.configured) {
        return { success: false, error: 'Symphony API is not configured (set SYMPHONY_API_KEY and SYMPHONY_AGENT_ID)' };
    }

    const tradeId = parseFiniteInt(liveTradeId);
    if (!Number.isFinite(tradeId) || tradeId <= 0) {
        return { success: false, error: 'Invalid live trade id' };
    }

    const lookup = await query('SELECT * FROM live_trades WHERE id = $1 LIMIT 1', [tradeId]);
    const row = lookup.rows?.[0] ?? null;
    if (!row) return { success: false, error: 'Live trade not found' };
    if (row.status === 'CLOSED') return { success: false, error: 'Trade already closed' };

    const batchId = typeof row.batch_id === 'string' ? row.batch_id.trim() : '';
    if (!batchId) return { success: false, error: 'Trade is missing batch_id' };

    const nowSec = Math.floor(Date.now() / 1000);

    try {
        const { apiKey, agentId, baseUrl } = symphonyClient.getSymphonyConfigFromEnv();
        const closeResponse = await symphonyClient.batchClose({
            agentId,
            apiKey,
            baseUrl,
            batchId
        });

        await query(
            `UPDATE live_trades
             SET status = $1,
                 close_response_json = $2,
                 last_error = NULL
             WHERE id = $3`,
            [
                'CLOSING',
                JSON.stringify(closeResponse?.data ?? null),
                tradeId
            ]
        );

        return { success: true, liveTradeId: tradeId, batchId, response: closeResponse?.data ?? null, requestedAt: nowSec };
    } catch (err) {
        await query(
            `UPDATE live_trades
             SET last_error = $1
             WHERE id = $2`,
            [err?.message || String(err), tradeId]
        );
        return { success: false, error: err?.message || String(err) };
    }
}

async function syncLiveTradesOnce({ force = false } = {}) {
    const env = getEnvStatus();
    const settings = await getLiveTradingSettings();
    const enabled = settings?.enabled === 1;
    if (!enabled && !force) {
        // Avoid surfacing sync errors when live trading is off and there is nothing to sync.
        const open = await countOpenPositions();
        if (open === 0) {
            return {
                success: true,
                skipped: true,
                reason: env.configured ? 'disabled_no_open_trades' : 'not_configured_no_open_trades'
            };
        }
    }

    if (!env.configured) {
        return { success: false, error: 'Symphony API not configured' };
    }

    const openTrades = await getOpenLiveTrades();
    if (openTrades.length === 0) return { success: true, skipped: true, reason: 'no_open_trades' };

    const { apiKey, agentId, baseUrl } = symphonyClient.getSymphonyConfigFromEnv();
    const nowSec = Math.floor(Date.now() / 1000);

    // Fetch batch statuses once.
    let batches = null;
    try {
        const resp = await symphonyClient.getAgentBatches({ agentId, apiKey, baseUrl });
        batches = Array.isArray(resp?.data?.batches) ? resp.data.batches : [];
    } catch (err) {
        return { success: false, error: err?.message || String(err) };
    }

    const statusByBatchId = new Map();
    for (const b of batches || []) {
        const bid = typeof b?.batchId === 'string' ? b.batchId : null;
        const st = typeof b?.status === 'string' ? b.status.toUpperCase() : null;
        if (!bid || !st) continue;
        statusByBatchId.set(bid, st);
    }

    const updated = [];
    for (const trade of openTrades) {
        const tradeId = parseFiniteInt(trade.id);
        const batchId = typeof trade.batch_id === 'string' ? trade.batch_id.trim() : '';
        if (!Number.isFinite(tradeId) || !batchId) continue;

        const batchStatus = statusByBatchId.get(batchId) || null;

        let positionsPayload = null;
        try {
            const resp = await symphonyClient.getBatchPositions({ batchId, apiKey, baseUrl });
            positionsPayload = resp?.data ?? null;
        } catch (err) {
            await query(
                `UPDATE live_trades
                 SET last_error = $1,
                     last_sync_at = $2
                 WHERE id = $3`,
                [err?.message || String(err), nowSec, tradeId]
            );
            continue;
        }

        const positions = Array.isArray(positionsPayload?.positions) ? positionsPayload.positions : [];

        // Replace positions snapshot for this trade.
        try {
            await query('DELETE FROM live_trade_positions WHERE live_trade_id = $1', [tradeId]);
            for (const p of positions) {
                await query(
                    `INSERT INTO live_trade_positions (
                        live_trade_id,
                        batch_id,
                        smart_account,
                        symphony_position_hash,
                        protocol_position_hash,
                        status,
                        collateral_amount,
                        pnl_percentage,
                        pnl_usd,
                        index_token,
                        leverage,
                        raw_json,
                        updated_at
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13
                    )`,
                    [
                        tradeId,
                        batchId,
                        typeof p?.smartAccount === 'string' ? p.smartAccount : null,
                        typeof p?.symphonyPositionHash === 'string' ? p.symphonyPositionHash : null,
                        typeof p?.protocolPositionHash === 'string' ? p.protocolPositionHash : null,
                        typeof p?.status === 'string' ? p.status : null,
                        parseFiniteNumber(p?.collateralAmount),
                        parseFiniteNumber(p?.pnlPercentage),
                        parseFiniteNumber(p?.pnlUSD),
                        typeof p?.indexToken === 'string' ? p.indexToken : null,
                        parseFiniteNumber(p?.leverage),
                        JSON.stringify(p ?? null),
                        nowSec
                    ]
                );
            }
        } catch (err) {
            await query(
                `UPDATE live_trades
                 SET last_error = $1,
                     last_sync_at = $2
                 WHERE id = $3`,
                [err?.message || String(err), nowSec, tradeId]
            );
            continue;
        }

        const pnlUsd = positions.reduce((acc, p) => acc + (parseFiniteNumber(p?.pnlUSD) ?? 0), 0);
        const collateralAmount = positions.reduce((acc, p) => acc + (parseFiniteNumber(p?.collateralAmount) ?? 0), 0);
        const pnlPct = collateralAmount > 0 ? pnlUsd / collateralAmount : null;

        const nextStatus = batchStatus === 'CLOSED' ? 'CLOSED' : (trade.status === 'OPENING' ? 'OPEN' : trade.status);

        await query(
            `UPDATE live_trades
             SET status = $1,
                 last_sync_at = $2,
                 last_pnl_usd = $3,
                 last_pnl_percent = $4,
                 last_collateral_amount = $5,
                 last_error = NULL,
                 closed_at = CASE WHEN $1 = 'CLOSED' AND (closed_at IS NULL OR closed_at = 0) THEN $2 ELSE closed_at END
             WHERE id = $6`,
            [
                nextStatus,
                nowSec,
                Number.isFinite(pnlUsd) ? pnlUsd : null,
                pnlPct,
                Number.isFinite(collateralAmount) ? collateralAmount : null,
                tradeId
            ]
        );

        updated.push({ tradeId, batchId, batchStatus: batchStatus ?? null, positions: positions.length });
    }

    return { success: true, updated, at: nowSec };
}

function createLiveSyncState() {
    return {
        isSyncing: false,
        lastSyncAt: null,
        lastError: null
    };
}

function startLiveSyncWorker({ intervalSec = 30, state }) {
    const safeIntervalSec = Math.max(5, Math.min(parseFiniteInt(intervalSec) ?? 30, 600));
    const syncState = state || createLiveSyncState();

    let timer = null;
    let stopped = false;

    async function tick() {
        if (stopped) return;
        if (syncState.isSyncing) {
            timer = setTimeout(tick, safeIntervalSec * 1000);
            return;
        }

        syncState.isSyncing = true;
        syncState.lastError = null;
        try {
            const result = await syncLiveTradesOnce();
            if (!result.success) {
                syncState.lastError = result.error || 'sync_failed';
            }
            syncState.lastSyncAt = Date.now();
        } catch (err) {
            syncState.lastError = err?.message || String(err);
            syncState.lastSyncAt = Date.now();
        } finally {
            syncState.isSyncing = false;
            timer = setTimeout(tick, safeIntervalSec * 1000);
        }
    }

    timer = setTimeout(tick, 0);

    return {
        state: syncState,
        stop: () => {
            stopped = true;
            if (timer) clearTimeout(timer);
            timer = null;
        }
    };
}

module.exports = {
    normalizeExecutionProvider,
    getEnvStatus,
    getLiveTradingSettings,
    updateLiveTradingSettings,
    listLiveTrades,
    getOpenLivePositionsForBot,
    openLiveTradeFromDecision,
    closeLiveTrade,
    syncLiveTradesOnce,
    createLiveSyncState,
    startLiveSyncWorker,
    isOpenishStatus
};
