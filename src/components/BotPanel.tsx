import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { PAIRS } from '../data/pairs';
import { API_BASE } from '../services/apiBase';
import './BotPanel.css';

interface BotStatus {
    todayPnL: number;
    tradesExecuted: number;
    safetyLimits: {
        maxCollateral: number;
        maxLeverage: number;
        dailyLossLimit: number;
    };
    apiConfigured: boolean;
    liveTrading?: {
        allowed: boolean;
        configured: boolean;
        baseUrl: string;
        settings: LiveTradingSettings | null;
        todayPnL: number;
        tradesExecuted: number;
        sync: {
            isSyncing: boolean;
            lastSyncAt: number | null;
            lastError: string | null;
        };
    };
}

interface LiveTradingSettings {
    id: number;
    enabled: number;
    pool_usd: number;
    max_trade_weight_pct: number;
    max_total_open_weight_pct: number;
    max_open_positions: number;
    max_leverage: number;
    daily_loss_limit_usd: number;
    updated_at: number;
    created_at: number;
}

interface BotRunRow {
    id: string;
    created_at: number;
    status: 'running' | 'completed' | 'failed';
    execution_provider: ExecutionProvider;
    timeframe_min: number | null;
    decision_id: number | null;
    summary_json: string | null;
    error: string | null;
}

interface BotRunEvent {
    id: number;
    run_id: string;
    timestamp: number;
    event_type: string;
    payload_json: string | null;
}

interface TradeRow {
    id: number;
    pair_index: number;
    entry_price: number;
    trigger_price?: number | null;
    entry_time: number;
    status: 'OPEN' | 'CLOSED' | 'PENDING' | 'CANCELED';
    collateral: number;
    leverage: number;
    direction: 'LONG' | 'SHORT';
    source?: 'MANUAL' | 'USER' | 'BOT';
}

interface LiveTradeRow {
    id: number;
    decision_id: number | null;
    execution_mode: string;
    pair_index: number;
    symbol: string;
    direction: 'LONG' | 'SHORT';
    requested_collateral_usd: number;
    resolved_weight_pct: number;
    leverage: number;
    requested_entry_price: number | null;
    stop_loss_price: number | null;
    take_profit_price: number | null;
    batch_id: string | null;
    status: string;
    created_at: number;
    opened_at: number | null;
    closed_at: number | null;
    last_sync_at: number | null;
    last_pnl_usd: number | null;
    last_pnl_percent: number | null;
    last_collateral_amount: number | null;
    last_error: string | null;
}

type ExecutionProvider = 'paper' | 'live';
type TraceFilter = 'all' | 'tools' | 'risk' | 'agent' | 'errors';

interface BotPanelProps {
    pairIndex: number;
    onFocusTrade?: (tradeId: number, pairIndex?: number | null) => void;
}

const API_URL = API_BASE;
const EXECUTION_PROVIDER_STORAGE_KEY = 'perpsTrader.executionProvider';

const readJson = async <T,>(res: Response): Promise<{ data: T | null; text: string }> => {
    const text = await res.text();
    if (!text) return { data: null, text: '' };
    try {
        return { data: JSON.parse(text) as T, text };
    } catch {
        return { data: null, text };
    }
};

const formatTimestamp = (value?: number | null) => {
    if (!value) return 'N/A';
    const date = new Date(value * 1000);
    return date.toLocaleString();
};

const parseJsonSafe = <T,>(raw: string | null): T | null => {
    if (!raw) return null;
    try {
        return JSON.parse(raw) as T;
    } catch {
        return null;
    }
};

const pairNameByIndex = new Map<number, string>(PAIRS.map((pair) => [pair.index, pair.name]));
const getPairLabel = (pairIndex?: number | null) => {
    if (typeof pairIndex !== 'number') return 'Pair N/A';
    return pairNameByIndex.get(pairIndex) ?? `Pair ${pairIndex}`;
};

const TRACE_FILTERS: TraceFilter[] = ['all', 'tools', 'risk', 'agent', 'errors'];
const TRACE_FILTER_LABELS: Record<TraceFilter, string> = {
    all: 'All',
    tools: 'Tools',
    risk: 'Risk',
    agent: 'Agent',
    errors: 'Errors'
};

const getTraceCategory = (eventType: string): TraceFilter | 'other' => {
    if (eventType === 'tool_call' || eventType === 'tool_result') return 'tools';
    if (eventType.startsWith('risk_')) return 'risk';
    if (eventType === 'agent_start' || eventType === 'agent_output') return 'agent';
    if (eventType.includes('error')) return 'errors';
    return 'other';
};

export const BotPanel: React.FC<BotPanelProps> = ({ pairIndex }) => {
    const [status, setStatus] = useState<BotStatus | null>(null);
    const [executionProvider, setExecutionProvider] = useState<ExecutionProvider>(() => {
        try {
            const saved = window.localStorage.getItem(EXECUTION_PROVIDER_STORAGE_KEY);
            return saved === 'live' ? 'live' : 'paper';
        } catch {
            return 'paper';
        }
    });

    const [timeframeMin, setTimeframeMin] = useState<string>('15');
    const [opportunitiesLimit, setOpportunitiesLimit] = useState<string>('10');
    const [isRunning, setIsRunning] = useState(false);
    const [runError, setRunError] = useState<string | null>(null);

    const [runs, setRuns] = useState<BotRunRow[]>([]);
    const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
    const [runEvents, setRunEvents] = useState<BotRunEvent[]>([]);
    const [eventsLoading, setEventsLoading] = useState(false);
    const [traceFilter, setTraceFilter] = useState<TraceFilter>('all');

    const [livePoolDraft, setLivePoolDraft] = useState<string>('');
    const [isLiveSettingsSaving, setIsLiveSettingsSaving] = useState(false);
    const [liveSettingsError, setLiveSettingsError] = useState<string | null>(null);

    const [liveTrades, setLiveTrades] = useState<LiveTradeRow[]>([]);
    const [isLiveTradesLoading, setIsLiveTradesLoading] = useState(false);
    const [liveTradesError, setLiveTradesError] = useState<string | null>(null);

    const [paperTrades, setPaperTrades] = useState<TradeRow[]>([]);
    const [paperTradesError, setPaperTradesError] = useState<string | null>(null);

    useEffect(() => {
        try {
            window.localStorage.setItem(EXECUTION_PROVIDER_STORAGE_KEY, executionProvider);
        } catch {
            // ignore
        }
    }, [executionProvider]);

    const fetchStatus = useCallback(async () => {
        try {
            const res = await fetch(`${API_URL}/bot/status`, { cache: 'no-store' });
            const { data } = await readJson<BotStatus>(res);
            if (res.ok && data) setStatus(data);
        } catch (err) {
            console.error('Failed to fetch bot status:', err);
        }
    }, []);

    const fetchRuns = useCallback(async () => {
        try {
            const res = await fetch(`${API_URL}/bot/runs?limit=20`, { cache: 'no-store' });
            const { data, text } = await readJson<BotRunRow[]>(res);
            if (!res.ok || !Array.isArray(data)) {
                console.warn('Failed to load runs:', text);
                return;
            }
            setRuns(data);
            if (!selectedRunId && data.length > 0) {
                setSelectedRunId(data[0].id);
            }
        } catch (err) {
            console.error('Failed to load runs:', err);
        }
    }, [selectedRunId]);

    const fetchRunEvents = useCallback(async (runId: string) => {
        if (!runId) return;
        setEventsLoading(true);
        try {
            const res = await fetch(`${API_URL}/bot/runs/${runId}/events`, { cache: 'no-store' });
            const { data, text } = await readJson<BotRunEvent[]>(res);
            if (!res.ok || !Array.isArray(data)) {
                console.warn('Failed to load run events:', text);
                setRunEvents([]);
                return;
            }
            setRunEvents(data);
        } catch (err) {
            console.error('Failed to load run events:', err);
            setRunEvents([]);
        } finally {
            setEventsLoading(false);
        }
    }, []);

    const refreshLiveTrades = useCallback(async () => {
        setIsLiveTradesLoading(true);
        setLiveTradesError(null);
        try {
            const res = await fetch(`${API_URL}/live/trades?limit=100`, { cache: 'no-store' });
            const { data, text } = await readJson<LiveTradeRow[]>(res);
            if (!res.ok || !Array.isArray(data)) {
                setLiveTradesError(text ? text.slice(0, 200) : 'Failed to load live trades');
                setLiveTrades([]);
                return;
            }
            setLiveTrades(data);
        } catch (err) {
            console.error('Failed to load live trades:', err);
            setLiveTradesError('Failed to load live trades');
            setLiveTrades([]);
        } finally {
            setIsLiveTradesLoading(false);
        }
    }, []);

    const refreshPaperTrades = useCallback(async () => {
        setPaperTradesError(null);
        try {
            const res = await fetch(`${API_URL}/trades`, { cache: 'no-store' });
            const { data, text } = await readJson<TradeRow[]>(res);
            if (!res.ok || !Array.isArray(data)) {
                setPaperTradesError(text ? text.slice(0, 200) : 'Failed to load trades');
                setPaperTrades([]);
                return;
            }
            setPaperTrades(data.filter((t) => t.source === 'BOT'));
        } catch (err) {
            console.error('Failed to load trades:', err);
            setPaperTradesError('Failed to load trades');
        }
    }, []);

    useEffect(() => {
        fetchStatus();
        fetchRuns();
        refreshLiveTrades();
        refreshPaperTrades();
    }, [fetchRuns, fetchStatus, refreshLiveTrades, refreshPaperTrades]);

    useEffect(() => {
        if (!selectedRunId) return;
        fetchRunEvents(selectedRunId);
    }, [fetchRunEvents, selectedRunId]);

    useEffect(() => {
        const pool = status?.liveTrading?.settings?.pool_usd;
        if (typeof pool !== 'number' || !Number.isFinite(pool)) return;
        setLivePoolDraft((prev) => (prev === '' ? String(pool) : prev));
    }, [status?.liveTrading?.settings?.pool_usd]);

    const saveLivePool = useCallback(async () => {
        setIsLiveSettingsSaving(true);
        setLiveSettingsError(null);

        try {
            const poolUsd = Number.parseFloat(livePoolDraft);
            if (!Number.isFinite(poolUsd) || poolUsd < 0) {
                setLiveSettingsError('Invalid pool balance');
                return;
            }

            const res = await fetch(`${API_URL}/live/settings`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ pool_usd: poolUsd })
            });

            const { data, text } = await readJson<unknown>(res);
            if (!res.ok) {
                setLiveSettingsError((data && typeof data === 'object' && 'error' in data ? String((data as { error?: string }).error) : null) ?? (text ? text.slice(0, 200) : 'Failed to save live settings'));
                return;
            }

            await fetchStatus();
        } catch (err) {
            console.error('Failed to save live settings:', err);
            setLiveSettingsError('Failed to save live settings');
        } finally {
            setIsLiveSettingsSaving(false);
        }
    }, [fetchStatus, livePoolDraft]);

    const setLiveTradingEnabled = useCallback(async (enabled: boolean) => {
        setIsLiveSettingsSaving(true);
        setLiveSettingsError(null);

        try {
            const res = await fetch(`${API_URL}/live/toggle`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ enabled })
            });

            const { data, text } = await readJson<unknown>(res);
            if (!res.ok) {
                setLiveSettingsError((data && typeof data === 'object' && 'error' in data ? String((data as { error?: string }).error) : null) ?? (text ? text.slice(0, 200) : 'Failed to toggle live trading'));
                return;
            }

            await fetchStatus();
        } catch (err) {
            console.error('Failed to toggle live trading:', err);
            setLiveSettingsError('Failed to toggle live trading');
        } finally {
            setIsLiveSettingsSaving(false);
        }
    }, [fetchStatus]);

    const runAgent = useCallback(async () => {
        setIsRunning(true);
        setRunError(null);

        try {
            const tf = Number.parseInt(timeframeMin, 10);
            const limit = Number.parseInt(opportunitiesLimit, 10);

            const res = await fetch(`${API_URL}/bot/run`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    timeframeMin: Number.isFinite(tf) ? tf : 15,
                    opportunitiesLimit: Number.isFinite(limit) ? limit : 10,
                    executionProvider
                })
            });

            const { data, text } = await readJson<{ runId?: string; error?: string; trace?: BotRunEvent[] }>(res);
            if (!res.ok || !data) {
                setRunError((data?.error ?? text ?? 'Run failed').toString());
                return;
            }

            if (data.runId) {
                setSelectedRunId(data.runId);
                await fetchRuns();
                await fetchRunEvents(data.runId);
            }

            await fetchStatus();
            await refreshLiveTrades();
            await refreshPaperTrades();
        } catch (err) {
            console.error('Failed to run agent:', err);
            setRunError('Failed to run agent');
        } finally {
            setIsRunning(false);
        }
    }, [executionProvider, fetchRunEvents, fetchRuns, fetchStatus, opportunitiesLimit, refreshLiveTrades, refreshPaperTrades, timeframeMin]);

    const selectedRun = useMemo(() => runs.find((r) => r.id === selectedRunId) ?? null, [runs, selectedRunId]);
    const selectedRunSummary = useMemo(() => parseJsonSafe<Record<string, unknown>>(selectedRun?.summary_json ?? null), [selectedRun?.summary_json]);
    const traceCounts = useMemo(() => {
        const counts: Record<TraceFilter, number> = {
            all: runEvents.length,
            tools: 0,
            risk: 0,
            agent: 0,
            errors: 0
        };
        runEvents.forEach((event) => {
            const category = getTraceCategory(event.event_type);
            if (category !== 'other') counts[category] += 1;
        });
        return counts;
    }, [runEvents]);
    const filteredRunEvents = useMemo(() => {
        if (traceFilter === 'all') return runEvents;
        return runEvents.filter((event) => getTraceCategory(event.event_type) === traceFilter);
    }, [runEvents, traceFilter]);

    const liveTrading = status?.liveTrading ?? null;
    const liveEnabled = liveTrading?.settings?.enabled === 1;
    const liveWarning = executionProvider === 'live' && !liveEnabled;

    return (
        <div className="bot-panel">
            <div className="bot-header">
                <div>
                    <h3>Agent Loop</h3>
                    <div className="bot-subtitle">{getPairLabel(pairIndex)}</div>
                </div>
                <span className={`bot-status-badge ${status?.apiConfigured ? 'active' : 'inactive'}`}>
                    {status?.apiConfigured ? 'API Ready' : 'API Missing'}
                </span>
            </div>

            {liveWarning && (
                <div className="bot-warning">
                    Live execution is selected but live trading is disabled. Enable it below or switch to paper.
                </div>
            )}

            <div className="bot-section">
                <div className="bot-section-title">Run Controls</div>
                <div className="bot-control-row">
                    <label className="bot-label">Execution</label>
                    <div className="bot-toggle-group">
                        <button
                            type="button"
                            className={`bot-toggle ${executionProvider === 'paper' ? 'active' : ''}`}
                            onClick={() => setExecutionProvider('paper')}
                            disabled={isRunning}
                        >
                            Paper
                        </button>
                        <button
                            type="button"
                            className={`bot-toggle ${executionProvider === 'live' ? 'active' : ''}`}
                            onClick={() => setExecutionProvider('live')}
                            disabled={isRunning}
                        >
                            Live
                        </button>
                    </div>
                </div>
                <div className="bot-control-row">
                    <label className="bot-label">Timeframe (min)</label>
                    <input
                        className="bot-input"
                        value={timeframeMin}
                        onChange={(e) => setTimeframeMin(e.target.value)}
                        disabled={isRunning}
                    />
                </div>
                <div className="bot-control-row">
                    <label className="bot-label">Candidates</label>
                    <input
                        className="bot-input"
                        value={opportunitiesLimit}
                        onChange={(e) => setOpportunitiesLimit(e.target.value)}
                        disabled={isRunning}
                    />
                </div>
                <button
                    className="bot-run-button"
                    type="button"
                    onClick={runAgent}
                    disabled={isRunning || !status?.apiConfigured}
                >
                    {isRunning ? 'Running…' : 'Run Agent'}
                </button>
                {runError && <div className="bot-error">{runError}</div>}
            </div>

            <div className="bot-section">
                <div className="bot-section-title">Recent Runs</div>
                <div className="bot-run-list">
                    {runs.length === 0 && <div className="bot-muted">No runs yet.</div>}
                    {runs.map((run) => (
                        <button
                            key={run.id}
                            type="button"
                            className={`bot-run-item ${run.id === selectedRunId ? 'active' : ''}`}
                            onClick={() => setSelectedRunId(run.id)}
                        >
                            <div className="bot-run-item-row">
                                <span className="bot-run-id">{run.id.slice(0, 8)}</span>
                                <span className={`bot-run-status ${run.status}`}>{run.status}</span>
                            </div>
                            <div className="bot-run-item-meta">
                                {formatTimestamp(run.created_at)} · {run.execution_provider} · {run.timeframe_min ?? 'n/a'}m
                            </div>
                            {run.error && <div className="bot-run-error">{run.error}</div>}
                        </button>
                    ))}
                </div>
            </div>

            <div className="bot-section">
                <div className="bot-section-title">Run Summary</div>
                {selectedRunSummary ? (
                    <pre className="bot-json">{JSON.stringify(selectedRunSummary, null, 2)}</pre>
                ) : (
                    <div className="bot-muted">Select a run to view summary.</div>
                )}
            </div>

            <div className="bot-section">
                <div className="bot-section-title">Trace Timeline</div>
                <div className="bot-trace-filters">
                    <span className="bot-label">Filter</span>
                    <div className="bot-trace-filter-list">
                        {TRACE_FILTERS.map((filter) => (
                            <button
                                key={filter}
                                type="button"
                                className={`bot-trace-filter ${traceFilter === filter ? 'active' : ''}`}
                                onClick={() => setTraceFilter(filter)}
                            >
                                {TRACE_FILTER_LABELS[filter]} ({traceCounts[filter]})
                            </button>
                        ))}
                    </div>
                </div>
                {eventsLoading && <div className="bot-muted">Loading trace…</div>}
                {!eventsLoading && filteredRunEvents.length === 0 && <div className="bot-muted">No trace events for this filter.</div>}
                <div className="bot-trace">
                    {filteredRunEvents.map((event) => (
                        <div key={event.id} className="trace-item">
                            <div className="trace-header">
                                <span className="trace-type">{event.event_type}</span>
                                <span className="trace-time">{formatTimestamp(event.timestamp)}</span>
                            </div>
                            {event.payload_json && (
                                <pre className="bot-json">{JSON.stringify(parseJsonSafe(event.payload_json), null, 2)}</pre>
                            )}
                        </div>
                    ))}
                </div>
            </div>

            <div className="bot-section">
                <div className="bot-section-title">Live Trading</div>
                <div className="bot-inline">
                    <span className="bot-muted">Allowed: {liveTrading?.allowed ? 'Yes' : 'No'}</span>
                    <span className="bot-muted">Configured: {liveTrading?.configured ? 'Yes' : 'No'}</span>
                </div>
                <div className="bot-control-row">
                    <label className="bot-label">Live Enabled</label>
                    <div className="bot-toggle-group">
                        <button
                            type="button"
                            className={`bot-toggle ${liveEnabled ? 'active' : ''}`}
                            onClick={() => setLiveTradingEnabled(true)}
                            disabled={isLiveSettingsSaving}
                        >
                            On
                        </button>
                        <button
                            type="button"
                            className={`bot-toggle ${!liveEnabled ? 'active' : ''}`}
                            onClick={() => setLiveTradingEnabled(false)}
                            disabled={isLiveSettingsSaving}
                        >
                            Off
                        </button>
                    </div>
                </div>
                <div className="bot-control-row">
                    <label className="bot-label">Pool (USD)</label>
                    <input
                        className="bot-input"
                        value={livePoolDraft}
                        onChange={(e) => setLivePoolDraft(e.target.value)}
                        disabled={isLiveSettingsSaving}
                    />
                </div>
                <button
                    className="bot-secondary-button"
                    type="button"
                    onClick={saveLivePool}
                    disabled={isLiveSettingsSaving}
                >
                    Save Live Settings
                </button>
                {liveSettingsError && <div className="bot-error">{liveSettingsError}</div>}
            </div>

            <div className="bot-section">
                <div className="bot-section-title">Live Trades (Read-Only)</div>
                {liveTradesError && <div className="bot-error">{liveTradesError}</div>}
                {isLiveTradesLoading && <div className="bot-muted">Loading live trades…</div>}
                {!isLiveTradesLoading && liveTrades.length === 0 && <div className="bot-muted">No live trades.</div>}
                <div className="bot-list">
                    {liveTrades.map((trade) => (
                        <div key={trade.id} className="bot-list-item">
                            <div className="bot-list-title">{trade.symbol ?? getPairLabel(trade.pair_index)}</div>
                            <div className="bot-list-meta">
                                {trade.direction} · {trade.status} · {formatTimestamp(trade.created_at)}
                            </div>
                        </div>
                    ))}
                </div>
            </div>

            <div className="bot-section">
                <div className="bot-section-title">Paper Trades (Bot Only)</div>
                {paperTradesError && <div className="bot-error">{paperTradesError}</div>}
                {paperTrades.length === 0 && <div className="bot-muted">No paper trades.</div>}
                <div className="bot-list">
                    {paperTrades.map((trade) => (
                        <div key={trade.id} className="bot-list-item">
                            <div className="bot-list-title">{getPairLabel(trade.pair_index)}</div>
                            <div className="bot-list-meta">
                                {trade.direction} · {trade.status} · {formatTimestamp(trade.entry_time)}
                            </div>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
};
