const { query } = require('./db');

function clampInt(value, { min, max }) {
    if (!Number.isFinite(value)) return null;
    const intValue = Math.trunc(value);
    return Math.max(min, Math.min(max, intValue));
}

function parseEnvBool(name, fallback = true) {
    const raw = process.env[name];
    if (typeof raw !== 'string') return fallback;
    const v = raw.trim().toLowerCase();
    if (v === 'true' || v === '1' || v === 'yes' || v === 'y' || v === 'on') return true;
    if (v === 'false' || v === '0' || v === 'no' || v === 'n' || v === 'off') return false;
    return fallback;
}

function parseEnvInt(name, fallback, { min, max }) {
    const raw = process.env[name];
    const n = typeof raw === 'string' && raw.trim() ? Number.parseInt(raw.trim(), 10) : Number.NaN;
    const clamped = clampInt(Number.isFinite(n) ? n : Number.NaN, { min, max });
    return clamped ?? fallback;
}

function parseWalCheckpointMode() {
    const raw = typeof process.env.DB_WAL_CHECKPOINT_MODE === 'string' ? process.env.DB_WAL_CHECKPOINT_MODE.trim().toUpperCase() : '';
    const allowed = new Set(['PASSIVE', 'FULL', 'RESTART', 'TRUNCATE']);
    return allowed.has(raw) ? raw : 'TRUNCATE';
}

function secondsFromDays(days) {
    return Math.max(0, Math.floor(days)) * 24 * 3600;
}

function computeDefaultRetentionDays() {
    const evalLookback = parseEnvInt('BOT_EVAL_OUTCOMES_LOOKBACK_DAYS', 30, { min: 1, max: 365 });
    const tuningLookback = parseEnvInt('BOT_TUNING_LOOKBACK_DAYS', 30, { min: 1, max: 365 });
    // Keep a small buffer beyond configured lookbacks.
    return Math.max(14, evalLookback + 7, tuningLookback + 7);
}

async function batchedDeleteByRowid({ table, whereSql, whereParams, batchSize = 10_000 }) {
    const safeBatchSize = Math.min(Math.max(Number(batchSize) || 10_000, 100), 50_000);
    let totalDeleted = 0;

    // Use rowid batching to avoid long-running DELETEs and huge WAL spikes.
    // Note: All tables we target are rowid tables (not WITHOUT ROWID).
    while (true) {
        // IMPORTANT: whereSql often uses $1, $2, ... placeholders.
        // If we also use LIMIT $1 we'd alias the first placeholder, and passing an extra param
        // would trigger SQLITE_RANGE ("column index out of range").
        const limitPlaceholder = `$${whereParams.length + 1}`;
        const selectSql = `SELECT rowid AS rowid FROM ${table} WHERE ${whereSql} LIMIT ${limitPlaceholder}`;
        const batch = await query(selectSql, [...whereParams, safeBatchSize]);
        const rowids = (batch.rows || []).map((r) => r.rowid).filter((id) => Number.isFinite(Number(id)));
        if (rowids.length === 0) break;

        const placeholders = rowids.map((_, i) => `$${i + 1}`).join(',');
        const deleteSql = `DELETE FROM ${table} WHERE rowid IN (${placeholders})`;
        const result = await query(deleteSql, rowids);
        totalDeleted += Number(result?.changes) || rowids.length;

        if (rowids.length < safeBatchSize) break;
    }

    return totalDeleted;
}

async function deleteMarketStateHistoryOlderThan({ cutoffSec, batchSize }) {
    // Delete per timeframe to take advantage of idx_market_state_history_time(timeframe_min, candle_time)
    const tfs = await query('SELECT DISTINCT timeframe_min AS tf FROM market_state_history');
    const timeframes = (tfs.rows || []).map((r) => Number(r.tf)).filter((n) => Number.isFinite(n) && n > 0);

    let total = 0;
    for (const tf of timeframes) {
        total += await batchedDeleteByRowid({
            table: 'market_state_history',
            whereSql: 'timeframe_min = $1 AND candle_time < $2',
            whereParams: [tf, cutoffSec],
            batchSize
        });
    }
    return total;
}

async function runMaintenanceOnce(config) {
    const nowSec = Math.floor(Date.now() / 1000);

    // 1) Prune (bounded growth)
    if (config.marketStateHistoryRetentionDays > 0) {
        const cutoff = nowSec - secondsFromDays(config.marketStateHistoryRetentionDays);
        try {
            const deleted = await deleteMarketStateHistoryOlderThan({ cutoffSec: cutoff, batchSize: config.deleteBatchSize });
            if (deleted > 0) console.log(`[dbMaintenance] pruned market_state_history rows=${deleted}`);
        } catch (err) {
            console.warn('[dbMaintenance] prune market_state_history failed:', err?.message || err);
        }
    }

    if (config.botDecisionsRetentionDays > 0) {
        const cutoff = nowSec - secondsFromDays(config.botDecisionsRetentionDays);
        try {
            // Keep decisions linked to trades for auditability.
            const deleted = await batchedDeleteByRowid({
                table: 'bot_decisions',
                whereSql: 'timestamp < $1 AND (trade_id IS NULL OR trade_id = 0)',
                whereParams: [cutoff],
                batchSize: config.deleteBatchSize
            });
            if (deleted > 0) console.log(`[dbMaintenance] pruned bot_decisions rows=${deleted}`);
        } catch (err) {
            console.warn('[dbMaintenance] prune bot_decisions failed:', err?.message || err);
        }
    }

    if (config.botUniverseDecisionsRetentionDays > 0) {
        const cutoff = nowSec - secondsFromDays(config.botUniverseDecisionsRetentionDays);
        try {
            const deleted = await batchedDeleteByRowid({
                table: 'bot_universe_decisions',
                whereSql: 'timestamp < $1',
                whereParams: [cutoff],
                batchSize: config.deleteBatchSize
            });
            if (deleted > 0) console.log(`[dbMaintenance] pruned bot_universe_decisions rows=${deleted}`);
        } catch (err) {
            console.warn('[dbMaintenance] prune bot_universe_decisions failed:', err?.message || err);
        }
    }

    if (config.decisionOutcomesRetentionDays > 0) {
        const cutoff = nowSec - secondsFromDays(config.decisionOutcomesRetentionDays);
        try {
            const deleted = await batchedDeleteByRowid({
                table: 'decision_outcomes',
                whereSql: 'timestamp < $1',
                whereParams: [cutoff],
                batchSize: config.deleteBatchSize
            });
            if (deleted > 0) console.log(`[dbMaintenance] pruned decision_outcomes rows=${deleted}`);
        } catch (err) {
            console.warn('[dbMaintenance] prune decision_outcomes failed:', err?.message || err);
        }
    }

    if (config.metricsEventsRetentionDays > 0) {
        const cutoff = nowSec - secondsFromDays(config.metricsEventsRetentionDays);
        try {
            const deleted = await batchedDeleteByRowid({
                table: 'metrics_events',
                whereSql: 'timestamp < $1',
                whereParams: [cutoff],
                batchSize: config.deleteBatchSize
            });
            if (deleted > 0) console.log(`[dbMaintenance] pruned metrics_events rows=${deleted}`);
        } catch (err) {
            console.warn('[dbMaintenance] prune metrics_events failed:', err?.message || err);
        }
    }

    if (config.marketSnapshotsRetentionDays > 0) {
        const cutoff = nowSec - secondsFromDays(config.marketSnapshotsRetentionDays);
        try {
            const deleted = await batchedDeleteByRowid({
                table: 'market_snapshots',
                whereSql: 'timestamp < $1',
                whereParams: [cutoff],
                batchSize: config.deleteBatchSize
            });
            if (deleted > 0) console.log(`[dbMaintenance] pruned market_snapshots rows=${deleted}`);
        } catch (err) {
            console.warn('[dbMaintenance] prune market_snapshots failed:', err?.message || err);
        }
    }

    // 2) SQLite maintenance (controls WAL growth / frees space if vacuum enabled)
    if (config.journalMode) {
        try {
            await query(`PRAGMA journal_mode=${config.journalMode}`);
        } catch {
            // Best-effort
        }
    }

    try {
        const mode = parseWalCheckpointMode();
        await query(`PRAGMA wal_checkpoint(${mode})`);
    } catch (err) {
        console.warn('[dbMaintenance] WAL checkpoint failed:', err?.message || err);
    }

    if (config.optimizeEnabled) {
        try {
            await query('PRAGMA optimize');
        } catch {
            // Best-effort
        }
    }

    // Vacuum can be disruptive; keep off by default.
    if (config.vacuumEnabled) {
        const nowMs = Date.now();
        const due = config.vacuumLastRunAtMs === null || (nowMs - config.vacuumLastRunAtMs) >= config.vacuumIntervalHours * 3600 * 1000;
        if (due) {
            try {
                if (config.incrementalVacuumPages > 0) {
                    await query(`PRAGMA incremental_vacuum(${config.incrementalVacuumPages})`);
                    console.log(`[dbMaintenance] incremental_vacuum pages=${config.incrementalVacuumPages}`);
                } else {
                    await query('VACUUM');
                    console.log('[dbMaintenance] VACUUM completed');
                }
                config.vacuumLastRunAtMs = nowMs;
            } catch (err) {
                console.warn('[dbMaintenance] VACUUM failed:', err?.message || err);
            }
        }
    }
}

function buildConfig() {
    const defaultRetentionDays = computeDefaultRetentionDays();

    return {
        enabled: parseEnvBool('DB_MAINTENANCE_ENABLED', true),
        intervalSec: parseEnvInt('DB_MAINTENANCE_INTERVAL_SEC', 3600, { min: 60, max: 24 * 3600 }),
        deleteBatchSize: parseEnvInt('DB_DELETE_BATCH_SIZE', 10_000, { min: 500, max: 50_000 }),

        // Retention knobs (days)
        marketStateHistoryRetentionDays: parseEnvInt('MARKET_STATE_HISTORY_RETENTION_DAYS', defaultRetentionDays, { min: 1, max: 3650 }),
        botDecisionsRetentionDays: parseEnvInt('BOT_DECISIONS_RETENTION_DAYS', defaultRetentionDays, { min: 1, max: 3650 }),
        botUniverseDecisionsRetentionDays: parseEnvInt('BOT_UNIVERSE_DECISIONS_RETENTION_DAYS', 30, { min: 1, max: 3650 }),
        decisionOutcomesRetentionDays: parseEnvInt('DECISION_OUTCOMES_RETENTION_DAYS', Math.max(30, defaultRetentionDays), { min: 1, max: 3650 }),
        metricsEventsRetentionDays: parseEnvInt('METRICS_EVENTS_RETENTION_DAYS', 14, { min: 1, max: 3650 }),
        marketSnapshotsRetentionDays: parseEnvInt('MARKET_SNAPSHOTS_RETENTION_DAYS', 7, { min: 1, max: 3650 }),

        // SQLite maintenance
        journalMode: (typeof process.env.DB_JOURNAL_MODE === 'string' && process.env.DB_JOURNAL_MODE.trim())
            ? process.env.DB_JOURNAL_MODE.trim().toUpperCase()
            : 'WAL',
        optimizeEnabled: parseEnvBool('DB_OPTIMIZE_ENABLED', true),
        vacuumEnabled: parseEnvBool('DB_VACUUM_ENABLED', false),
        vacuumIntervalHours: parseEnvInt('DB_VACUUM_INTERVAL_HOURS', 168, { min: 1, max: 24 * 365 }),
        incrementalVacuumPages: parseEnvInt('DB_INCREMENTAL_VACUUM_PAGES', 0, { min: 0, max: 1_000_000 }),
        vacuumLastRunAtMs: null
    };
}

let timer = null;
let running = false;

async function tick(config) {
    if (running) return;
    running = true;
    try {
        await runMaintenanceOnce(config);
    } catch (err) {
        console.warn('[dbMaintenance] tick failed:', err?.message || err);
    } finally {
        running = false;
    }
}

function start() {
    const config = buildConfig();
    if (!config.enabled) {
        console.log('[dbMaintenance] disabled');
        return;
    }

    // Run once shortly after boot, then on interval.
    setTimeout(() => void tick(config), 10_000);
    timer = setInterval(() => void tick(config), config.intervalSec * 1000);
    console.log(`[dbMaintenance] enabled intervalSec=${config.intervalSec} retentionDays={history:${config.marketStateHistoryRetentionDays}, decisions:${config.botDecisionsRetentionDays}}`);
}

function stop() {
    if (timer) clearInterval(timer);
    timer = null;
}

module.exports = {
    start,
    stop
};
