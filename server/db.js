const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');

const resolveDbPath = () => {
    if (process.env.SQLITE_DB_PATH && process.env.SQLITE_DB_PATH.trim()) {
        return path.resolve(process.env.SQLITE_DB_PATH.trim());
    }

    // Railway deploys run on an ephemeral filesystem by default.
    // If the user attached a Volume and mounted it at a conventional location,
    // prefer that path automatically so DB state survives redeploys.
    const volumeCandidates = ['/data', '/app/data'];
    for (const dir of volumeCandidates) {
        try {
            if (fs.existsSync(dir) && fs.statSync(dir).isDirectory()) {
                return path.join(dir, 'trades.db');
            }
        } catch {
            // ignore
        }
    }

    return path.resolve(__dirname, 'trades.db');
};

const dbPath = resolveDbPath();

try {
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
} catch {
    // Best-effort; sqlite will surface a clearer error if it cannot create/open.
}

try {
    const existed = fs.existsSync(dbPath);
    const size = existed ? fs.statSync(dbPath).size : 0;
    console.log(`[db] SQLITE_DB_PATH=${process.env.SQLITE_DB_PATH || ''}`);
    console.log(`[db] Using SQLite database at: ${dbPath} (exists=${existed}, bytes=${size})`);
} catch {
    console.log(`[db] Using SQLite database at: ${dbPath}`);
}

const db = new sqlite3.Database(dbPath, (err) => {
    if (err) {
        console.error('Error opening database ' + dbPath + ': ' + err.message);
    } else {
        console.log('Connected to the SQLite database.');
    }
});

const initSchema = () => {
    db.serialize(() => {
        const ensureTradeColumns = () => {
            db.all(`PRAGMA table_info(trades)`, (err, columns) => {
                if (err) {
                    console.error('Error checking trades schema:', err.message);
                    return;
                }

                const existing = new Set((columns || []).map((col) => col?.name).filter(Boolean));

                const addColumn = (definition) => {
                    db.run(`ALTER TABLE trades ADD COLUMN ${definition}`, (alterErr) => {
                        if (alterErr) {
                            console.error('Error migrating trades schema:', alterErr.message);
                        }
                    });
                };

                if (!existing.has('stop_loss_price')) addColumn('stop_loss_price REAL');
                if (!existing.has('take_profit_price')) addColumn('take_profit_price REAL');
                if (!existing.has('trigger_price')) addColumn('trigger_price REAL');
                if (!existing.has('triggered_price')) addColumn('triggered_price REAL');
                if (!existing.has('triggered_time')) addColumn('triggered_time INTEGER');

                // Entry-time cost snapshots (for cost-adjusted PnL / eval)
                if (!existing.has('entry_cost_spread_percent')) addColumn('entry_cost_spread_percent REAL');
                if (!existing.has('entry_cost_fee_position_size_percent')) addColumn('entry_cost_fee_position_size_percent REAL');
                if (!existing.has('entry_cost_fee_oracle_position_size_percent')) addColumn('entry_cost_fee_oracle_position_size_percent REAL');
                if (!existing.has('entry_cost_total_percent')) addColumn('entry_cost_total_percent REAL');
                if (!existing.has('entry_cost_source')) addColumn('entry_cost_source TEXT');
                if (!existing.has('entry_cost_updated_at')) addColumn('entry_cost_updated_at INTEGER');
                if (!existing.has('entry_cost_snapshot_json')) addColumn('entry_cost_snapshot_json TEXT');
            });
        };

        const ensureBotDecisionColumns = () => {
            db.all(`PRAGMA table_info(bot_decisions)`, (err, columns) => {
                if (err) {
                    console.error('Error checking bot_decisions schema:', err.message);
                    return;
                }

                const existing = new Set((columns || []).map((col) => col?.name).filter(Boolean));

                const addColumn = (definition) => {
                    db.run(`ALTER TABLE bot_decisions ADD COLUMN ${definition}`, (alterErr) => {
                        if (alterErr) {
                            console.error('Error migrating bot_decisions schema:', alterErr.message);
                        }
                    });
                };

                // Used for caching + evaluation
                if (!existing.has('analysis_key')) addColumn('analysis_key TEXT');
                if (!existing.has('timeframe_min')) addColumn('timeframe_min INTEGER');
                if (!existing.has('candle_time')) addColumn('candle_time INTEGER');
                if (!existing.has('prompt_version')) addColumn('prompt_version TEXT');
                if (!existing.has('model')) addColumn('model TEXT');
                if (!existing.has('usage_json')) addColumn('usage_json TEXT');
                if (!existing.has('analysis_cost_json')) addColumn('analysis_cost_json TEXT');
                if (!existing.has('cache_source_decision_id')) addColumn('cache_source_decision_id INTEGER');
            });
        };

        const ensureBotReflectionColumns = () => {
            db.all(`PRAGMA table_info(bot_reflections)`, (err, columns) => {
                if (err) {
                    console.error('Error checking bot_reflections schema:', err.message);
                    return;
                }

                const existing = new Set((columns || []).map((col) => col?.name).filter(Boolean));

                const addColumn = (definition) => {
                    db.run(`ALTER TABLE bot_reflections ADD COLUMN ${definition}`, (alterErr) => {
                        if (alterErr) {
                            console.error('Error migrating bot_reflections schema:', alterErr.message);
                        }
                    });
                };

                // Links reflections to the originating bot_decisions row (optional).
                if (!existing.has('decision_id')) addColumn('decision_id INTEGER');
            });
        };

        // Pairs metadata (Gains pairs universe)
        db.run(`CREATE TABLE IF NOT EXISTS pairs(
            pair_index INTEGER PRIMARY KEY,
            from_symbol TEXT,
            to_symbol TEXT,
            updated_at INTEGER NOT NULL
        )`);

        // Latest Gains trading variables per pair (costs/liquidity for ranking)
        db.run(`CREATE TABLE IF NOT EXISTS pair_trading_variables(
            pair_index INTEGER PRIMARY KEY,
            updated_at INTEGER NOT NULL,
            spread_percent REAL,
            group_name TEXT,
            group_min_leverage REAL,
            group_max_leverage REAL,
            fee_position_size_percent REAL,
            fee_oracle_position_size_percent REAL,
            min_position_size_usd REAL,
            collateral_symbol TEXT,
            collateral_price_usd REAL,
            oi_long REAL,
            oi_short REAL,
            oi_skew_percent REAL,
            funding_enabled INTEGER,
            funding_last_update_ts INTEGER,
            borrowing_rate_per_second_p TEXT,
            borrowing_last_update_ts INTEGER
        )`);

        // Latest computed market state per pair/timeframe (stable size; overwritten via upsert)
        db.run(`CREATE TABLE IF NOT EXISTS market_state(
            pair_index INTEGER NOT NULL,
            timeframe_min INTEGER NOT NULL,
            candle_time INTEGER NOT NULL,
            price REAL NOT NULL,
            rsi REAL,
            macd REAL,
            macd_signal REAL,
            macd_histogram REAL,
            bb_upper REAL,
            bb_middle REAL,
            bb_lower REAL,
            ema9 REAL,
            ema21 REAL,
            ema50 REAL,
            ema200 REAL,
            sma20 REAL,
            sma50 REAL,
            sma200 REAL,
            atr REAL,
            stoch_k REAL,
            stoch_d REAL,
            overall_bias TEXT,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (pair_index, timeframe_min)
        )`);

        db.run(`CREATE INDEX IF NOT EXISTS idx_market_state_timeframe_updated
                ON market_state(timeframe_min, updated_at)`);

        // Historical market states (append-only; enables evaluation and reproducibility).
        db.run(`CREATE TABLE IF NOT EXISTS market_state_history(
            pair_index INTEGER NOT NULL,
            timeframe_min INTEGER NOT NULL,
            candle_time INTEGER NOT NULL,
            price REAL NOT NULL,
            rsi REAL,
            macd REAL,
            macd_signal REAL,
            macd_histogram REAL,
            bb_upper REAL,
            bb_middle REAL,
            bb_lower REAL,
            ema9 REAL,
            ema21 REAL,
            ema50 REAL,
            ema200 REAL,
            sma20 REAL,
            sma50 REAL,
            sma200 REAL,
            atr REAL,
            stoch_k REAL,
            stoch_d REAL,
            overall_bias TEXT,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (pair_index, timeframe_min, candle_time)
        )`);

        db.run(`CREATE INDEX IF NOT EXISTS idx_market_state_history_time
            ON market_state_history(timeframe_min, candle_time)`);

        // Universe analysis runs (top-10 candidates + selected pair + downstream decision)
        db.run(`CREATE TABLE IF NOT EXISTS bot_universe_decisions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            timeframe_min INTEGER NOT NULL,
            candidates_json TEXT NOT NULL,
            selection_json TEXT NOT NULL,
            analysis_json TEXT,
            selected_pair_index INTEGER,
            action TEXT,
            trade_id INTEGER
        )`);

        // Trades table (don't drop if exists to preserve data)
        db.run(`CREATE TABLE IF NOT EXISTS trades(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair_index INTEGER NOT NULL,
            entry_price REAL NOT NULL,
            exit_price REAL,
            entry_time INTEGER NOT NULL,
            exit_time INTEGER,
            status TEXT DEFAULT 'OPEN',
            pnl REAL,
            collateral REAL,
            leverage REAL,
            direction TEXT,
            source TEXT DEFAULT 'MANUAL',
            stop_loss_price REAL,
            take_profit_price REAL,
            trigger_price REAL,
            triggered_price REAL,
            triggered_time INTEGER
        )`, ensureTradeColumns);

        // Market snapshots - store price + indicators at each interval
        db.run(`CREATE TABLE IF NOT EXISTS market_snapshots(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair_index INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            price REAL NOT NULL,
            rsi REAL,
            macd REAL,
            macd_signal REAL,
            macd_histogram REAL,
            bb_upper REAL,
            bb_middle REAL,
            bb_lower REAL,
            ema9 REAL,
            ema21 REAL,
            ema50 REAL,
            ema200 REAL,
            sma20 REAL,
            sma50 REAL,
            sma200 REAL,
            atr REAL,
            stoch_k REAL,
            stoch_d REAL,
            overall_bias TEXT
        )`);

        // Bot decisions - store AI analysis and trade decisions
        db.run(`CREATE TABLE IF NOT EXISTS bot_decisions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair_index INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            analysis TEXT,
            decision TEXT,
            confidence REAL,
            action TEXT,
            trade_id INTEGER,
            reasoning TEXT
        )`, ensureBotDecisionColumns);

        // Bot reflections (daily/weekly/trade-close; used as memory + evaluation artifacts)
        db.run(`CREATE TABLE IF NOT EXISTS bot_reflections(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            scope TEXT NOT NULL,
            trade_id INTEGER,
            decision_id INTEGER,
            pair_index INTEGER,
            timeframe_min INTEGER,
            period_start INTEGER,
            period_end INTEGER,
            summary TEXT,
            tags_json TEXT,
            metrics_json TEXT,
            reflection_json TEXT,
            inputs_json TEXT,
            model TEXT,
            prompt_version TEXT
        )`, ensureBotReflectionColumns);

        db.run(`CREATE INDEX IF NOT EXISTS idx_bot_reflections_time
                ON bot_reflections(timestamp)`);

        // Threshold versioning (tuning + rollback)
        db.run(`CREATE TABLE IF NOT EXISTS bot_threshold_versions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at INTEGER NOT NULL,
            scope TEXT NOT NULL,
            params_json TEXT NOT NULL,
            metrics_json TEXT,
            reason TEXT,
            parent_version_id INTEGER,
            is_active INTEGER DEFAULT 0
        )`);

        db.run(`CREATE INDEX IF NOT EXISTS idx_bot_threshold_versions_active
                ON bot_threshold_versions(scope, is_active, created_at)`);

        // Metrics events (latency/freshness/cache counts)
        db.run(`CREATE TABLE IF NOT EXISTS metrics_events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            name TEXT NOT NULL,
            details_json TEXT
        )`);

        db.run(`CREATE INDEX IF NOT EXISTS idx_metrics_events_time
                ON metrics_events(timestamp)`);

        // Decision outcomes (markouts and correctness labels)
        db.run(`CREATE TABLE IF NOT EXISTS decision_outcomes(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            decision_id INTEGER,
            timestamp INTEGER NOT NULL,
            pair_index INTEGER NOT NULL,
            timeframe_min INTEGER,
            candle_time INTEGER,
            horizon_sec INTEGER NOT NULL,
            entry_price REAL,
            future_price REAL,
            forward_return REAL,
            correct INTEGER,
            details_json TEXT
        )`);

        db.run(`CREATE INDEX IF NOT EXISTS idx_decision_outcomes_decision
                ON decision_outcomes(decision_id, horizon_sec)`);

        db.run(`CREATE INDEX IF NOT EXISTS idx_decision_outcomes_time
                ON decision_outcomes(timestamp)`);

        // Schema init completion log
        db.run('SELECT 1', (err) => {
            if (err) {
                console.error('Error initializing schema:', err);
            } else {
                console.log('Database schema initialized.');
            }
        });
    });
};

// Wrapper to mimic pool.query for async/await usage
const query = (sql, params = []) => {
    return new Promise((resolve, reject) => {
        // SQLite supports numbered parameters (?1, ?2, ...). Convert Postgres-style placeholders
        // ($1, $2, ...) into SQLite numbered parameters while preserving reuse of the same index.
        // This is important for statements like "triggered_price = $1" which reuse $1 multiple times.
        const sqliteSql = sql.replace(/\$(\d+)/g, '?$1');

        if (sql.trim().toUpperCase().startsWith('SELECT')) {
            db.all(sqliteSql, params, (err, rows) => {
                if (err) reject(err);
                else resolve({ rows });
            });
        } else {
            db.run(sqliteSql, params, function (err) {
                if (err) reject(err);
                else {
                    resolve({ rows: [], lastID: this.lastID, changes: this.changes });
                }
            });
        }
    });
};

module.exports = { db, initSchema, query };
