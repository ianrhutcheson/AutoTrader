import React, { useCallback, useEffect, useRef, useState } from 'react';
import { PAIRS } from '../data/pairs';
import { API_BASE } from '../services/apiBase';
import './BotPanel.css';

interface BotStatus {
    isActive: boolean;
    lastAnalysis: unknown;
    todayPnL: number;
    tradesExecuted: number;
    autotrade?: {
        enabled: boolean;
        config: {
            enabled: boolean;
            intervalSec: number;
            minScore: number;
            minLlmIntervalSec: number;
            timeframeMin: number;
            regimeTimeframeMin: number;
            lookbackSec: number;
            regimeLookbackSec: number;
        };
        state: {
            isTicking: boolean;
            nextIntent?: 'open' | 'manage';
            lastTickAtMs: number | null;
            lastLlmAtMs: number | null;
            lastScan: unknown;
            lastExecution: unknown;
            lastError: string | null;
        };
    };
    safetyLimits: {
        maxCollateral: number;
        maxLeverage: number;
        dailyLossLimit: number;
    };
    apiConfigured: boolean;
}

interface BotDecision {
    id: number;
    timestamp: number;
    pair_index?: number;
    decision: string;
    reasoning?: string | null;
    analysis?: string | null;
    action: string;
    trade_id?: number | null;
    timeframe_min?: number | null;
    candle_time?: number | null;
    model?: string | null;
    prompt_version?: string | null;
}

interface UniverseDecision {
    id: number;
    timestamp: number;
    timeframe_min: number;
    candidates_json: string;
    selection_json: string;
    analysis_json: string | null;
    selected_pair_index: number | null;
    action: string | null;
    trade_id: number | null;
}

interface IndicatorValues {
    price: number;
    rsi: number | null;
    macd: { MACD: number; signal: number; histogram: number } | null;
    bollingerBands: { upper: number; middle: number; lower: number } | null;
    ema: { ema9: number; ema21: number; ema50: number; ema200: number } | null;
    sma: { sma20: number; sma50: number; sma200: number } | null;
    atr: number | null;
    stochastic: { k: number; d: number } | null;
}

interface IndicatorsResponse {
    indicators: IndicatorValues;
    summary: {
        summary: string;
        overallBias: string;
        signals: Array<{ indicator: string; signal: string; value: string }>;
    };
    price: number;
}

type TradeDirection = 'LONG' | 'SHORT';
type BotAction = 'execute_trade' | 'close_position' | 'cancel_pending' | 'hold_position';

interface ExecuteTradeArgs {
    direction: TradeDirection;
    collateral: number;
    leverage: number;
    stop_loss_price: number;
    take_profit_price: number;
    trigger_price?: number;
    confidence: number;
    reasoning: string;
    invalidation?: string;
}

interface ClosePositionArgs {
    trade_id: number;
    confidence: number;
    reasoning: string;
}

interface CancelPendingArgs {
    trade_id: number;
    confidence: number;
    reasoning: string;
}

interface HoldPositionArgs {
    confidence: number;
    reasoning: string;
}

type BotActionArgs = ExecuteTradeArgs | ClosePositionArgs | CancelPendingArgs | HoldPositionArgs;

interface MarketSignal {
    indicator: string;
    signal: string;
    value: string;
}

interface MarketSummary {
    summary: string;
    overallBias: string;
    signals: MarketSignal[];
}

interface BotAnalysisCost {
    estimatedUsd: number | null;
    tokens: { prompt: number | null; completion: number | null; total: number | null };
    rates: {
        promptPer1KTokensUsd: number | null;
        completionPer1KTokensUsd: number | null;
        totalPer1KTokensUsd: number | null;
    };
}

interface BotCacheMeta {
    status: 'hit' | 'miss' | 'bypass';
    cacheKey?: string;
    sourceDecisionId?: number;
}

interface TradingVariablesForPair {
    source?: { url: string | null; lastRefreshed: string | null; refreshId: number | null } | null;
    pair?: {
        index: number;
        from: string;
        to: string;
        spreadPercent: number | null;
        groupIndex: number | null;
        feeIndex: number | null;
    } | null;
    group?: { name: string | null; minLeverage: number | null; maxLeverage: number | null } | null;
    fees?: { positionSizeFeePercent: number | null; oraclePositionSizeFeePercent: number | null; minPositionSizeUsd: number | null } | null;
    openInterest?: { collateralSymbol: string | null; collateralPriceUsd: number | null; long: number | null; short: number | null; skewPercent: number | null } | null;
    funding?: { enabled: boolean; lastFundingRatePerSecondP: string | null; lastUpdateTs: number | null } | null;
    borrowing?: { borrowingRatePerSecondP: string | null; lastUpdateTs: number | null } | null;
}

interface BotTradeExecuted {
    success: boolean;
    tradeId: number;
    direction: TradeDirection;
    collateral: number;
    leverage: number;
    entryPrice: number | null;
    status?: 'OPEN' | 'PENDING' | 'CLOSED' | 'CANCELED';
    triggerPrice?: number | null;
}

interface BotPositionClosed {
    success: boolean;
    tradeId: number;
    pnl: number;
    exitPrice: number;
}

interface BotAnalysisResponse {
    success?: boolean;
    error?: string;
    action?: BotAction;
    args?: BotActionArgs;
    summary?: MarketSummary;
    currentPrice?: number;
    decisionId?: number;
    analysisCost?: BotAnalysisCost;
    requiresConfirmation?: boolean;
    tradeExecuted?: BotTradeExecuted;
    positionClosed?: BotPositionClosed;
    tradeCanceled?: unknown;
    declined?: boolean;
    tradingVariables?: TradingVariablesForPair | null;
    tradingVariablesError?: string | null;
    runId?: string;
    constraintsSource?: string;
    adjustments?: Array<{ field: string; applied: number; reason?: string }>;
    cacheMeta?: BotCacheMeta;
}

interface DebugMetricSummaryRow {
    name: string;
    count: number;
    first_ts: number;
    last_ts: number;
}

interface DebugOverviewResponse {
    nowSec: number;
    env: Record<string, string | null>;
    counts: {
        botDecisions: number;
        botReflections: number;
        marketStateHistory: number;
        metricsEvents: number;
    };
    last: {
        botDecisionTs: number | null;
        botReflectionTs: number | null;
        marketStateHistoryCandleTime: number | null;
        metricsEventTs: number | null;
    };
    thresholds: {
        active: unknown | null;
        recent: unknown[];
    };
    metrics: {
        sinceSec: number;
        recentSummary: DebugMetricSummaryRow[];
    };
}

interface DebugReflectionsResponse {
    limit: number;
    pairIndex: number | null;
    scope: string | null;
    rows: Array<{
        id: number;
        timestamp: number;
        scope: string;
        trade_id: number | null;
        decision_id: number | null;
        pair_index: number | null;
        timeframe_min: number | null;
        summary: string | null;
        tags: unknown | null;
        metrics: unknown | null;
        reflection: unknown | null;
        model: string | null;
        prompt_version: string | null;
    }>;
}

interface OpportunityCandidate {
    pair_index: number;
    symbol: string | null;
    timeframe_min: number;
    candle_time: number;
    price: number;
    side: TradeDirection;
    score: number;
    reasons: string[];
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
    direction: TradeDirection;
    source?: 'MANUAL' | 'USER' | 'BOT';
}

interface BotRunResponse {
    success: boolean;
    error?: string;
    runId?: string;
    timeframeMin?: number;
    candidates?: OpportunityCandidate[];
    pairIndices?: number[];
    primaryPairIndex?: number | null;
    primaryAnalysis?: BotAnalysisResponse | null;
    recommendations?: Array<{ pairIndex: number; symbol: string | null; analysis: BotAnalysisResponse }>;
}

interface BotPanelProps {
    pairIndex: number;
    onFocusTrade?: (tradeId: number, pairIndex?: number | null) => void;
}

const pairNameByIndex = new Map<number, string>(PAIRS.map((pair) => [pair.index, pair.name]));

const getPairLabel = (pairIndex?: number | null) => {
    if (typeof pairIndex !== 'number') return 'Pair N/A';
    return pairNameByIndex.get(pairIndex) ?? `Pair ${pairIndex}`;
};

const API_URL = API_BASE;

const readJson = async <T,>(res: Response): Promise<{ data: T | null; text: string }> => {
    const text = await res.text();
    if (!text) return { data: null, text: '' };
    try {
        return { data: JSON.parse(text) as T, text };
    } catch {
        return { data: null, text };
    }
};

const getErrorMessage = (value: unknown): string | null => {
    if (!value || typeof value !== 'object') return null;
    if (!('error' in value)) return null;
    const error = (value as { error?: unknown }).error;
    return typeof error === 'string' ? error : null;
};

type BotMode = 'manual' | 'autotrade';
const BOT_MODE_STORAGE_KEY = 'perpsTrader.botMode';
const BOT_DECISIONS_PAGE_SIZE = 25;
const BOT_UNIVERSE_DECISIONS_PAGE_SIZE = 15;

type RecommendationStatus = 'pending' | 'accepted' | 'rejected';

interface BotTradeRecommendation {
    status: RecommendationStatus;
    pairIndex: number;
    symbol: string | null;
    analysis: BotAnalysisResponse;
}

export const BotPanel: React.FC<BotPanelProps> = ({ pairIndex, onFocusTrade }) => {
    const [status, setStatus] = useState<BotStatus | null>(null);
    const [decisions, setDecisions] = useState<BotDecision[]>([]);
    const [indicators, setIndicators] = useState<IndicatorsResponse | null>(null);
    const [indicatorsError, setIndicatorsError] = useState<string | null>(null);
    const [isAnalyzing, setIsAnalyzing] = useState(false);
    const [lastAnalysis, setLastAnalysis] = useState<BotAnalysisResponse | null>(null);
    const [isExecuting, setIsExecuting] = useState(false);
    const [executionError, setExecutionError] = useState<string | null>(null);
    const [runError, setRunError] = useState<string | null>(null);
    const [recommendations, setRecommendations] = useState<BotTradeRecommendation[]>([]);
    const [isRecommendationsOpen, setIsRecommendationsOpen] = useState(false);
    const [recommendationIndex, setRecommendationIndex] = useState(0);
    const [recommendationStopLoss, setRecommendationStopLoss] = useState<string>('');
    const [recommendationTakeProfit, setRecommendationTakeProfit] = useState<string>('');
    const [recommendationTriggerPrice, setRecommendationTriggerPrice] = useState<string>('');
    const [autotradeDraft, setAutotradeDraft] = useState<{ intervalSec: number; minScore: number; minLlmIntervalSec: number } | null>(null);
    const [isAutotradeUpdating, setIsAutotradeUpdating] = useState(false);
    const [autotradeError, setAutotradeError] = useState<string | null>(null);
    const [expandedDecisionId, setExpandedDecisionId] = useState<number | null>(null);
    const [universeDecisions, setUniverseDecisions] = useState<UniverseDecision[]>([]);
    const [expandedUniverseDecisionId, setExpandedUniverseDecisionId] = useState<number | null>(null);
    const [isLoadingMoreDecisions, setIsLoadingMoreDecisions] = useState(false);
    const [hasMoreDecisions, setHasMoreDecisions] = useState(true);
    const decisionsListRef = useRef<HTMLDivElement | null>(null);
    const [isDebugOpen, setIsDebugOpen] = useState(false);
    const [isDebugLoading, setIsDebugLoading] = useState(false);
    const [debugError, setDebugError] = useState<string | null>(null);
    const [debugOverview, setDebugOverview] = useState<DebugOverviewResponse | null>(null);
    const [debugReflections, setDebugReflections] = useState<DebugReflectionsResponse | null>(null);
    const [botMode, setBotMode] = useState<BotMode>(() => {
        try {
            const saved = window.localStorage.getItem(BOT_MODE_STORAGE_KEY);
            return saved === 'autotrade' ? 'autotrade' : 'manual';
        } catch {
            return 'manual';
        }
    });
    const isAutotradeRunning = status?.autotrade?.enabled === true;

    const [pendingBotOrders, setPendingBotOrders] = useState<TradeRow[]>([]);
    const [pendingOrdersError, setPendingOrdersError] = useState<string | null>(null);
    const [isPendingOrdersLoading, setIsPendingOrdersLoading] = useState(false);
    const [cancelingTradeId, setCancelingTradeId] = useState<number | null>(null);

    const refreshPendingBotOrders = useCallback(async () => {
        setIsPendingOrdersLoading(true);
        setPendingOrdersError(null);

        try {
            const res = await fetch(`${API_URL}/trades`, { cache: 'no-store' });
            const { data, text } = await readJson<TradeRow[]>(res);
            if (!res.ok || !Array.isArray(data)) {
                setPendingOrdersError(text ? text.slice(0, 200) : 'Failed to load trades');
                setPendingBotOrders([]);
                return;
            }

            const pending = data
                .filter((t) => t && t.status === 'PENDING' && t.source === 'BOT')
                .sort((a, b) => (b.entry_time ?? 0) - (a.entry_time ?? 0));
            setPendingBotOrders(pending);
        } catch (err) {
            console.error('Failed to load pending bot orders:', err);
            setPendingOrdersError('Failed to load pending bot orders');
            setPendingBotOrders([]);
        } finally {
            setIsPendingOrdersLoading(false);
        }
    }, []);

    const cancelPendingOrder = useCallback(async (tradeId: number) => {
        if (!Number.isFinite(tradeId)) return;
        setCancelingTradeId(tradeId);
        setPendingOrdersError(null);
        try {
            const res = await fetch(`${API_URL}/trades/${tradeId}/cancel`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' }
            });
            const { data, text } = await readJson<unknown>(res);
            if (!res.ok) {
                const msg = getErrorMessage(data) ?? (text ? text.slice(0, 200) : 'Failed to cancel pending order');
                setPendingOrdersError(msg);
                return;
            }

            await refreshPendingBotOrders();
        } catch (err) {
            console.error('Failed to cancel pending bot order:', err);
            setPendingOrdersError('Failed to cancel pending order');
        } finally {
            setCancelingTradeId(null);
        }
    }, [refreshPendingBotOrders]);

    // Fetch bot status
    const fetchStatus = useCallback(async () => {
        try {
            const res = await fetch(`${API_URL}/bot/status`);
            const { data } = await readJson<BotStatus>(res);
            if (res.ok && data) setStatus(data);
        } catch (err) {
            console.error('Failed to fetch bot status:', err);
        }
    }, []);

    useEffect(() => {
        refreshPendingBotOrders();
        const interval = window.setInterval(() => {
            if (document.hidden) return;
            refreshPendingBotOrders();
        }, 15000);

        return () => window.clearInterval(interval);
    }, [refreshPendingBotOrders]);

    useEffect(() => {
        try {
            window.localStorage.setItem(BOT_MODE_STORAGE_KEY, botMode);
        } catch {
            // ignore
        }
    }, [botMode]);

    useEffect(() => {
        if (status?.autotrade?.enabled) {
            setBotMode('autotrade');
        }
    }, [status?.autotrade?.enabled]);

    // Fetch indicators
    const fetchIndicators = useCallback(async () => {
        try {
            const res = await fetch(`${API_URL}/indicators/${pairIndex}`);
            const text = await res.text();
            const parsed = text ? (JSON.parse(text) as unknown) : null;
            if (!res.ok) {
                const message = getErrorMessage(parsed) ?? `Failed to load indicators (${res.status})`;
                setIndicators(null);
                setIndicatorsError(message);
                return;
            }

            setIndicators(parsed as IndicatorsResponse);
            setIndicatorsError(null);
        } catch (err) {
            console.error('Failed to fetch indicators:', err);
            setIndicators(null);
            setIndicatorsError('Failed to load indicators');
        }
    }, [pairIndex]);

    const refreshDecisions = useCallback(async () => {
        try {
            const params = new URLSearchParams();
            params.set('limit', String(BOT_DECISIONS_PAGE_SIZE));

            const res = await fetch(`${API_URL}/bot/decisions?${params.toString()}`);
            const { data } = await readJson<BotDecision[]>(res);
            const page = Array.isArray(data) ? data : [];

            setDecisions((prev) => {
                const byId = new Map<number, BotDecision>();
                for (const item of prev) {
                    if (typeof item?.id !== 'number') continue;
                    byId.set(item.id, item);
                }
                for (const item of page) {
                    if (typeof item?.id !== 'number') continue;
                    byId.set(item.id, item);
                }

                const merged = Array.from(byId.values());
                merged.sort((a, b) => {
                    const timestampDiff = (b.timestamp ?? 0) - (a.timestamp ?? 0);
                    if (timestampDiff !== 0) return timestampDiff;
                    return (b.id ?? 0) - (a.id ?? 0);
                });
                return merged;
            });

            setHasMoreDecisions((prevHasMore) => {
                if (decisions.length > BOT_DECISIONS_PAGE_SIZE) return prevHasMore;
                return page.length >= BOT_DECISIONS_PAGE_SIZE;
            });
        } catch (err) {
            console.error('Failed to fetch decisions:', err);
            setDecisions([]);
        }
    }, [decisions.length]);

    const refreshUniverseDecisions = useCallback(async () => {
        try {
            const params = new URLSearchParams();
            params.set('limit', String(BOT_UNIVERSE_DECISIONS_PAGE_SIZE));
            const res = await fetch(`${API_URL}/bot/universe/decisions?${params.toString()}`);
            const { data } = await readJson<UniverseDecision[]>(res);
            const page = Array.isArray(data) ? data : [];
            setUniverseDecisions(page);
        } catch (err) {
            console.error('Failed to fetch universe decisions:', err);
            setUniverseDecisions([]);
        }
    }, []);

    const fetchOlderDecisions = useCallback(async (opts: { beforeTimestamp: number; beforeId: number }) => {
        if (isLoadingMoreDecisions || !hasMoreDecisions) return;
        setIsLoadingMoreDecisions(true);

        try {
            const params = new URLSearchParams();
            params.set('limit', String(BOT_DECISIONS_PAGE_SIZE));
            params.set('before', String(Math.floor(opts.beforeTimestamp)));
            params.set('beforeId', String(Math.floor(opts.beforeId)));

            const res = await fetch(`${API_URL}/bot/decisions?${params.toString()}`);
            const { data } = await readJson<BotDecision[]>(res);
            const page = Array.isArray(data) ? data : [];

            if (page.length === 0) {
                setHasMoreDecisions(false);
                return;
            }

            setDecisions((prev) => {
                const byId = new Set<number>(prev.map((item) => item.id));
                const deduped = page.filter((item) => typeof item?.id === 'number' && !byId.has(item.id));
                return prev.concat(deduped);
            });
            setHasMoreDecisions(page.length >= BOT_DECISIONS_PAGE_SIZE);
        } catch (err) {
            console.error('Failed to fetch decisions:', err);
        } finally {
            setIsLoadingMoreDecisions(false);
        }
    }, [hasMoreDecisions, isLoadingMoreDecisions]);

    useEffect(() => {
        const config = status?.autotrade?.config;
        if (!config) return;
        setAutotradeDraft((prev) =>
            prev ?? {
                intervalSec: config.intervalSec ?? 15,
                minScore: config.minScore ?? 70,
                minLlmIntervalSec: config.minLlmIntervalSec ?? 300
            }
        );
    }, [status?.autotrade?.config]);

    const ensureBotActive = async () => {
        if (status?.isActive) return true;
        try {
            const res = await fetch(`${API_URL}/bot/toggle`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ active: true })
            });
            const { data } = await readJson<BotStatus & { error?: string }>(res);
            if (!res.ok) {
                setRunError(data?.error || 'Failed to start bot');
                return false;
            }
            if (data) setStatus(prev => prev ? { ...prev, ...data } : prev);
            await fetchStatus();
            return true;
        } catch (err) {
            console.error('Failed to start bot:', err);
            return false;
        }
    };

    const setAutotradeWorkerEnabled = async (enabled: boolean) => {
        if (!status?.autotrade) {
            setAutotradeError('Backend missing autotrade endpoints (restart the backend)');
            return false;
        }

        if (enabled) {
            if (!status.apiConfigured) {
                setAutotradeError('OpenAI API key not configured. Add it to server/.env');
                return false;
            }
            if (!autotradeDraft) {
                setAutotradeError('Autotrade config not loaded yet');
                return false;
            }
        }

        setIsAutotradeUpdating(true);
        setAutotradeError(null);

        try {
            if (enabled && autotradeDraft) {
                const configRes = await fetch(`${API_URL}/bot/autotrade/config`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        intervalSec: autotradeDraft.intervalSec,
                        minScore: autotradeDraft.minScore,
                        minLlmIntervalSec: autotradeDraft.minLlmIntervalSec
                    })
                });

                const { data, text } = await readJson<{ error?: string }>(configRes);
                if (!configRes.ok) {
                    const hint = text.includes('Cannot POST /api/bot/autotrade/config')
                        ? 'Backend missing autotrade endpoints (restart the backend)'
                        : (data?.error || 'Failed to update autotrade config');
                    setAutotradeError(hint);
                    return false;
                }
            }

            const toggleRes = await fetch(`${API_URL}/bot/autotrade/toggle`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ enabled })
            });

            const { data, text } = await readJson<{ error?: string }>(toggleRes);
            if (!toggleRes.ok) {
                const hint = text.includes('Cannot POST /api/bot/autotrade/toggle')
                    ? 'Backend missing autotrade endpoints (restart the backend)'
                    : (data?.error || 'Failed to toggle autotrade');
                setAutotradeError(hint);
                return false;
            }

            await fetchStatus();
            return true;
        } catch (err) {
            console.error('Failed to toggle autotrade:', err);
            setAutotradeError('Failed to toggle autotrade');
            return false;
        } finally {
            setIsAutotradeUpdating(false);
        }
    };

    const selectMode = async (nextMode: BotMode) => {
        if (nextMode === botMode) return;

        if (isAutotradeRunning) {
            return;
        }

        setBotMode(nextMode);
    };

    const handleRunButtonClick = async () => {
        setAutotradeError(null);

        if (botMode === 'manual') {
            if (status?.autotrade?.enabled) {
                await setAutotradeWorkerEnabled(false);
            }
            await runUnifiedBotFlow();
            return;
        }

        if (!status?.autotrade) {
            setAutotradeError('Backend missing autotrade endpoints (restart the backend)');
            return;
        }

        setIsRecommendationsOpen(false);
        const enabled = status.autotrade.enabled === true;
        await setAutotradeWorkerEnabled(!enabled);
    };

    const executeRecommendedAction = async (recommendation: BotTradeRecommendation, overrideArgs?: BotActionArgs): Promise<boolean> => {
        if (!recommendation?.analysis?.action) return false;
        setIsExecuting(true);
        setExecutionError(null);

        try {
            const res = await fetch(`${API_URL}/bot/execute/${recommendation.pairIndex}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    action: recommendation.analysis.action,
                    args: overrideArgs ?? recommendation.analysis.args,
                    currentPrice: recommendation.analysis.currentPrice,
                    decisionId: recommendation.analysis.decisionId
                })
            });

            const data: BotAnalysisResponse = await res.json();
            if (!res.ok || data?.success === false) {
                setExecutionError(data?.error || 'Trade execution failed');
                return false;
            }

            setRecommendations((prev) =>
                prev.map((item) =>
                    item.pairIndex === recommendation.pairIndex && item.analysis.decisionId === recommendation.analysis.decisionId
                        ? { ...item, status: 'accepted', analysis: { ...item.analysis, ...data } }
                        : item
                )
            );

            refreshDecisions();
            fetchStatus();
            return true;
        } catch (err) {
            console.error('Execution failed:', err);
            setExecutionError('Trade execution failed');
            return false;
        } finally {
            setIsExecuting(false);
        }
    };

    useEffect(() => {
        if (!isRecommendationsOpen) return;
        if (recommendations.length === 0) return;
        if (recommendationIndex < 0 || recommendationIndex >= recommendations.length) return;

        const current = recommendations[recommendationIndex];
        const isTrade = current.analysis.action === 'execute_trade';
        const args = isTrade ? (current.analysis.args as ExecuteTradeArgs | undefined) : undefined;

        if (isTrade) {
            setRecommendationStopLoss(typeof args?.stop_loss_price === 'number' ? String(args.stop_loss_price) : '');
            setRecommendationTakeProfit(typeof args?.take_profit_price === 'number' ? String(args.take_profit_price) : '');
            setRecommendationTriggerPrice(typeof args?.trigger_price === 'number' ? String(args.trigger_price) : '');
        } else {
            setRecommendationStopLoss('');
            setRecommendationTakeProfit('');
            setRecommendationTriggerPrice('');
        }
    }, [isRecommendationsOpen, recommendationIndex, recommendations]);

    const declineRecommendedAction = (recommendation: BotTradeRecommendation) => {
        setExecutionError(null);
        setRecommendations((prev) =>
            prev.map((item) =>
                item.pairIndex === recommendation.pairIndex && item.analysis.decisionId === recommendation.analysis.decisionId
                    ? { ...item, status: 'rejected' }
                    : item
            )
        );
    };

    const advanceRecommendation = () => {
        setExecutionError(null);
        setRecommendationIndex((prev) => {
            const next = prev + 1;
            return next;
        });
    };

    const closeRecommendations = () => {
        setExecutionError(null);
        setIsRecommendationsOpen(false);
    };

    const runUnifiedBotFlow = async () => {
        setIsAnalyzing(true);
        setExecutionError(null);
        setRunError(null);
        setRecommendations([]);
        setRecommendationIndex(0);
        try {
            const started = await ensureBotActive();
            if (!started) {
                setRunError('Failed to start bot');
                return;
            }

            const runRes = await fetch(`${API_URL}/bot/run`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    primaryPairIndex: pairIndex,
                    timeframeMin: 15,
                    opportunitiesLimit: 10,
                    maxAnalyses: 5
                })
            });

            const { data: runData, text: runText } = await readJson<BotRunResponse>(runRes);
            if (!runRes.ok || runData?.success === false || !runData) {
                const hint = runText.includes('Cannot POST /api/bot/run')
                    ? 'Backend is running an older version (missing /api/bot/run). Restart the backend.'
                    : (runData?.error || (runText ? runText.slice(0, 200) : 'Analysis failed'));
                setRunError(hint);
                return;
            }

            if (runData.primaryAnalysis) setLastAnalysis(runData.primaryAnalysis);

            const nextRecommendations: BotTradeRecommendation[] = Array.isArray(runData.recommendations)
                ? runData.recommendations.map((rec) => ({
                    status: 'pending',
                    pairIndex: rec.pairIndex,
                    symbol: rec.symbol ?? null,
                    analysis: rec.analysis
                }))
                : [];

            setRecommendations(nextRecommendations);
            setIsRecommendationsOpen(true);
            refreshDecisions();
            fetchStatus();
        } catch (err) {
            console.error('Unified bot run failed:', err);
            setRunError('Analysis failed');
        } finally {
            setIsAnalyzing(false);
        }
    };

    useEffect(() => {
        fetchStatus();
        fetchIndicators();
        refreshDecisions();
        refreshUniverseDecisions();

        const statusInterval = window.setInterval(() => {
            if (document.hidden) return;
            fetchStatus();
        }, 5000);

        // Refresh indicators every 30 seconds
        const indicatorsInterval = window.setInterval(() => {
            if (document.hidden) return;
            fetchIndicators();
        }, 30000);

        // Refresh decisions so autotrade activity shows up without manual interaction.
        const decisionsInterval = window.setInterval(() => {
            if (document.hidden) return;
            refreshDecisions();
        }, 10000);

        const universeInterval = window.setInterval(() => {
            if (document.hidden) return;
            refreshUniverseDecisions();
        }, 15000);

        return () => {
            window.clearInterval(statusInterval);
            window.clearInterval(indicatorsInterval);
            window.clearInterval(decisionsInterval);
            window.clearInterval(universeInterval);
        };
    }, [fetchIndicators, fetchStatus, refreshDecisions, refreshUniverseDecisions]);

    useEffect(() => {
        setExpandedDecisionId(null);
        setExpandedUniverseDecisionId(null);
    }, [pairIndex]);

    const loadOlderDecisions = useCallback(async () => {
        const last = decisions[decisions.length - 1];
        if (!last) return;
        if (typeof last.timestamp !== 'number') return;
        if (typeof last.id !== 'number') return;
        await fetchOlderDecisions({ beforeTimestamp: last.timestamp, beforeId: last.id });
    }, [decisions, fetchOlderDecisions]);

    const onDecisionsListScroll = useCallback(() => {
        const el = decisionsListRef.current;
        if (!el) return;
        if (isLoadingMoreDecisions || !hasMoreDecisions) return;
        const thresholdPx = 60;
        if (el.scrollTop + el.clientHeight >= el.scrollHeight - thresholdPx) {
            loadOlderDecisions();
        }
    }, [hasMoreDecisions, isLoadingMoreDecisions, loadOlderDecisions]);

    const formatSignal = (value: number | null | undefined, decimals = 2) => {
        if (value === null || value === undefined) return 'N/A';
        return value.toFixed(decimals);
    };

    const getRsiClass = (rsi: number | null | undefined) => {
        if (typeof rsi !== 'number') return '';
        if (rsi > 70) return 'overbought';
        if (rsi < 30) return 'oversold';
        return '';
    };

    const formatUsd = (value: number | null | undefined) => {
        if (value === null || value === undefined) return 'N/A';
        if (value === 0) return '$0.00';
        return `$${value.toFixed(value < 0.01 ? 4 : 2)}`;
    };

    const formatPercent = (value: number | null | undefined, decimals = 4) => {
        if (value === null || value === undefined) return 'N/A';
        return `${value.toFixed(decimals)}%`;
    };

    const formatCompactNumber = (value: number | null | undefined) => {
        if (value === null || value === undefined) return 'N/A';
        return new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 2 }).format(value);
    };

    const parseDecisionAnalysis = (decision: BotDecision): unknown | null => {
        if (typeof decision.analysis !== 'string' || !decision.analysis) return null;
        try {
            return JSON.parse(decision.analysis) as unknown;
        } catch {
            return null;
        }
    };

    const getDecisionLlmLabel = (decision: BotDecision): string | null => {
        const parsed = parseDecisionAnalysis(decision);
        if (!parsed || typeof parsed !== 'object') return null;
        const llm = (parsed as { llm?: unknown }).llm;
        if (!llm || typeof llm !== 'object') return null;
        const api = (llm as { api?: unknown }).api;
        const model = (llm as { model?: unknown }).model;
        const apiStr = typeof api === 'string' && api.trim() ? api.trim() : null;
        const modelStr = typeof model === 'string' && model.trim() ? model.trim() : null;
        if (!apiStr && !modelStr) return null;
        return [apiStr, modelStr].filter(Boolean).join(' · ');
    };

    const safeJsonParse = (value: string | null | undefined): unknown | null => {
        if (typeof value !== 'string' || !value.trim()) return null;
        try {
            return JSON.parse(value) as unknown;
        } catch {
            return null;
        }
    };

    const getUniverseDecisionReasoning = (row: UniverseDecision): string | null => {
        const selection = safeJsonParse(row.selection_json);
        if (!selection || typeof selection !== 'object') return null;
        const args = (selection as { args?: unknown }).args;
        if (!args || typeof args !== 'object') return null;
        const reasoning = (args as { reasoning?: unknown }).reasoning;
        return typeof reasoning === 'string' && reasoning.trim() ? reasoning.trim() : null;
    };

    const getUniverseDecisionLlmLabel = (row: UniverseDecision): string | null => {
        const selection = safeJsonParse(row.selection_json);
        if (!selection || typeof selection !== 'object') return null;
        const llm = (selection as { llm?: unknown }).llm;
        if (!llm || typeof llm !== 'object') return null;
        const api = (llm as { api?: unknown }).api;
        const model = (llm as { model?: unknown }).model;
        const apiStr = typeof api === 'string' && api.trim() ? api.trim() : null;
        const modelStr = typeof model === 'string' && model.trim() ? model.trim() : null;
        if (!apiStr && !modelStr) return null;
        return [apiStr, modelStr].filter(Boolean).join(' · ');
    };

    const getDecisionCandidateScore = (decision: BotDecision): number | null => {
        const parsed = parseDecisionAnalysis(decision);
        if (!parsed || typeof parsed !== 'object') return null;
        const candidateScore = (parsed as { candidateScore?: number }).candidateScore;
        if (typeof candidateScore !== 'number') return null;
        return Number.isFinite(candidateScore) ? candidateScore : null;
    };

    const getDecisionToolArgs = (decision: BotDecision): Record<string, unknown> | null => {
        const parsed = parseDecisionAnalysis(decision);
        if (!parsed || typeof parsed !== 'object') return null;
        const toolCall = (parsed as { toolCall?: unknown }).toolCall;
        if (!toolCall || typeof toolCall !== 'object') return null;
        const args = (toolCall as { args?: unknown }).args;
        if (!args || typeof args !== 'object') return null;
        return args as Record<string, unknown>;
    };

    const getDecisionReasoning = (decision: BotDecision): string | null => {
        if (typeof decision.reasoning === 'string' && decision.reasoning.trim()) return decision.reasoning.trim();
        const args = getDecisionToolArgs(decision);
        const fallback = args?.reasoning;
        if (typeof fallback === 'string' && fallback.trim()) return fallback.trim();
        return null;
    };

    const getDecisionPositionId = (decision: BotDecision): number | null => {
        if (typeof decision.trade_id === 'number' && Number.isFinite(decision.trade_id)) return decision.trade_id;
        const args = getDecisionToolArgs(decision);
        const maybeTradeId = args?.trade_id ?? args?.tradeId;
        const parsed = Number.parseInt(String(maybeTradeId), 10);
        return Number.isFinite(parsed) ? parsed : null;
    };

    const formatReasoningSnippet = (reasoning: string, maxLen = 60) => {
        const trimmed = reasoning.trim();
        if (trimmed.length <= maxLen) return trimmed;
        return `${trimmed.slice(0, maxLen - 1)}…`;
    };

    const refreshDebug = useCallback(async () => {
        setIsDebugLoading(true);
        setDebugError(null);

        try {
            const [overviewRes, reflectionsRes] = await Promise.all([
                fetch(`${API_URL}/bot/debug/overview`),
                fetch(`${API_URL}/bot/debug/reflections?pairIndex=${encodeURIComponent(String(pairIndex))}&limit=5`)
            ]);

            const overviewJson = await readJson<DebugOverviewResponse>(overviewRes);
            if (!overviewRes.ok || !overviewJson.data) {
                throw new Error(overviewJson.data ? 'Failed to load debug overview' : (overviewJson.text || 'Failed to load debug overview'));
            }

            const reflectionsJson = await readJson<DebugReflectionsResponse>(reflectionsRes);
            if (!reflectionsRes.ok || !reflectionsJson.data) {
                throw new Error(reflectionsJson.text || 'Failed to load debug reflections');
            }

            setDebugOverview(overviewJson.data);
            setDebugReflections(reflectionsJson.data);
        } catch (err) {
            setDebugError(err instanceof Error ? err.message : 'Debug request failed');
        } finally {
            setIsDebugLoading(false);
        }
    }, [pairIndex]);

    useEffect(() => {
        if (!isDebugOpen) return;
        void refreshDebug();
    }, [isDebugOpen, refreshDebug]);

    return (
        <div className="bot-panel">
            {isRecommendationsOpen && (
                <div className="recommendations-overlay" onClick={closeRecommendations} role="presentation">
                    <div className="recommendations-modal" onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
                        <div className="recommendations-header">
                            <div>
                                <div className="recommendations-title">Trade Recommendations</div>
                                <div className="recommendations-subtitle">
                                    {recommendations.length === 0
                                        ? 'No actionable trades found.'
                                        : `Reviewing ${Math.min(recommendationIndex + 1, recommendations.length)} of ${recommendations.length}`}
                                </div>
                            </div>
                            <button className="recommendations-close" onClick={closeRecommendations} aria-label="Close">
                                ✕
                            </button>
                        </div>

                        {recommendations.length > 0 && recommendationIndex < recommendations.length && (() => {
                            const recommendation = recommendations[recommendationIndex];
                            const analysis = recommendation.analysis;
                            const isTrade = analysis.action === 'execute_trade';
                            const isClose = analysis.action === 'close_position';
                            const isCancel = analysis.action === 'cancel_pending';
                            const canExecute = analysis.action === 'execute_trade' || analysis.action === 'close_position' || analysis.action === 'cancel_pending';
                            const tradeArgs = isTrade ? (analysis.args as ExecuteTradeArgs | undefined) : undefined;
                            const closeArgs = isClose ? (analysis.args as ClosePositionArgs | undefined) : undefined;
                            const cancelArgs = isCancel ? (analysis.args as CancelPendingArgs | undefined) : undefined;

                            const entryPrice = typeof analysis.currentPrice === 'number' ? analysis.currentPrice : null;
                            const stopLoss = tradeArgs?.stop_loss_price;
                            const takeProfit = tradeArgs?.take_profit_price;
                            const confidence = typeof (analysis.args as { confidence?: unknown } | undefined)?.confidence === 'number'
                                ? ((analysis.args as { confidence: number }).confidence)
                                : null;

                            const risk = entryPrice !== null && typeof stopLoss === 'number'
                                ? (tradeArgs?.direction === 'LONG' ? entryPrice - stopLoss : stopLoss - entryPrice)
                                : null;
                            const reward = entryPrice !== null && typeof takeProfit === 'number'
                                ? (tradeArgs?.direction === 'LONG' ? takeProfit - entryPrice : entryPrice - takeProfit)
                                : null;
                            const rr = risk !== null && reward !== null && risk > 0 ? reward / risk : null;

                            const spreadPercent = analysis.tradingVariables?.pair?.spreadPercent ?? null;
                            const positionFeePercent = analysis.tradingVariables?.fees?.positionSizeFeePercent ?? null;
                            const oracleFeePercent = analysis.tradingVariables?.fees?.oraclePositionSizeFeePercent ?? null;
                            const costPercent = (typeof spreadPercent === 'number' && typeof positionFeePercent === 'number' && typeof oracleFeePercent === 'number')
                                ? spreadPercent + positionFeePercent + oracleFeePercent
                                : null;
                            const oiLong = analysis.tradingVariables?.openInterest?.long ?? null;
                            const oiShort = analysis.tradingVariables?.openInterest?.short ?? null;
                            const oiTotal = (typeof oiLong === 'number' && typeof oiShort === 'number')
                                ? oiLong + oiShort
                                : null;
                            const groupMaxLeverage = analysis.tradingVariables?.group?.maxLeverage ?? null;
                            return (
                                <div className="recommendations-body">
                                    <div className="recommendation-meta">
                                        <span className="recommendation-chip">
                                            {recommendation.symbol ?? `Pair #${recommendation.pairIndex}`}
                                        </span>
                                        <span className={`recommendation-chip ${analysis.action || 'hold_position'}`}>
                                            {analysis.action}
                                        </span>
                                        {isTrade && tradeArgs && (
                                            <span className="recommendation-chip">
                                                {tradeArgs.direction} ${tradeArgs.collateral} @ {tradeArgs.leverage}x
                                            </span>
                                        )}
                                        {isClose && closeArgs && (
                                            <span className="recommendation-chip">
                                                Close #{closeArgs.trade_id}
                                            </span>
                                        )}
                                        {isCancel && cancelArgs && (
                                            <span className="recommendation-chip">
                                                Cancel #{cancelArgs.trade_id}
                                            </span>
                                        )}
                                        {confidence !== null && (
                                            <span className="recommendation-chip">
                                                Conf {(confidence * 100).toFixed(0)}%
                                            </span>
                                        )}
                                    </div>

                                    {analysis.summary?.summary && (
                                        <div className="recommendation-summary">
                                            {analysis.summary.summary}
                                        </div>
                                    )}

                                    {analysis.args?.reasoning && (
                                        <div className="recommendation-reasoning">
                                            {analysis.args.reasoning}
                                        </div>
                                    )}

                                    {isTrade && tradeArgs && entryPrice !== null && (
                                        <div className="recommendation-summary">
                                            {typeof tradeArgs.trigger_price === 'number' && Number.isFinite(tradeArgs.trigger_price) && (
                                                <>
                                                    Trigger {formatUsd(tradeArgs.trigger_price)} ·{' '}
                                                </>
                                            )}
                                            {typeof tradeArgs.trigger_price === 'number' && Number.isFinite(tradeArgs.trigger_price)
                                                ? `Now ${formatUsd(entryPrice)}`
                                                : `Entry ${formatUsd(entryPrice)}`}
                                            {' '}· SL {formatUsd(tradeArgs.stop_loss_price)} · TP {formatUsd(tradeArgs.take_profit_price)}
                                            {rr !== null && Number.isFinite(rr) && (
                                                <span>
                                                    {' '}· R:R {rr.toFixed(2)}
                                                </span>
                                            )}
                                        </div>
                                    )}

                                    {isTrade && tradeArgs && (
                                        <div className="recommendation-summary">
                                            <span style={{ opacity: 0.8 }}>Adjust SL/TP/Trigger:</span>{' '}
                                            <input
                                                type="number"
                                                inputMode="decimal"
                                                value={recommendationStopLoss}
                                                onChange={(e) => setRecommendationStopLoss(e.target.value)}
                                                style={{ width: 120, marginLeft: 8 }}
                                                placeholder="Stop loss"
                                                disabled={isExecuting}
                                            />
                                            <input
                                                type="number"
                                                inputMode="decimal"
                                                value={recommendationTakeProfit}
                                                onChange={(e) => setRecommendationTakeProfit(e.target.value)}
                                                style={{ width: 120, marginLeft: 8 }}
                                                placeholder="Take profit"
                                                disabled={isExecuting}
                                            />
                                            <input
                                                type="number"
                                                inputMode="decimal"
                                                value={recommendationTriggerPrice}
                                                onChange={(e) => setRecommendationTriggerPrice(e.target.value)}
                                                style={{ width: 120, marginLeft: 8 }}
                                                placeholder="Trigger (optional)"
                                                disabled={isExecuting}
                                            />
                                        </div>
                                    )}

                                    {isTrade && tradeArgs?.invalidation && (
                                        <div className="recommendation-reasoning">
                                            Invalidation: {tradeArgs.invalidation}
                                        </div>
                                    )}

                                    {analysis.tradingVariablesError && (
                                        <div className="analysis-error">
                                            Trading variables: {analysis.tradingVariablesError}
                                        </div>
                                    )}

                                    {analysis.tradingVariables && (
                                        <div className="recommendation-summary">
                                            Costs {costPercent === null ? 'N/A' : formatPercent(costPercent)} · OI {oiTotal === null ? 'N/A' : formatCompactNumber(oiTotal)} · Group max lev {typeof groupMaxLeverage === 'number' ? `${groupMaxLeverage.toFixed(1)}x` : 'N/A'}
                                        </div>
                                    )}

                                    {executionError && (
                                        <div className="analysis-error">
                                            {executionError}
                                        </div>
                                    )}

                                    <div className="recommendation-actions">
                                        <button
                                            className="confirm-yes"
                                            onClick={async () => {
                                                if (!canExecute) return;
                                                let overrideArgs: BotActionArgs | undefined;
                                                if (analysis.action === 'execute_trade' && tradeArgs) {
                                                    const stopLoss = Number.parseFloat(recommendationStopLoss);
                                                    const takeProfit = Number.parseFloat(recommendationTakeProfit);
                                                    if (!Number.isFinite(stopLoss) || !Number.isFinite(takeProfit)) {
                                                        setExecutionError('Invalid stop loss / take profit');
                                                        return;
                                                    }

                                                    const triggerTrimmed = recommendationTriggerPrice.trim();
                                                    let triggerValue: number | null = null;
                                                    if (triggerTrimmed) {
                                                        const parsed = Number.parseFloat(triggerTrimmed);
                                                        if (!Number.isFinite(parsed)) {
                                                            setExecutionError('Invalid trigger price');
                                                            return;
                                                        }
                                                        triggerValue = parsed;
                                                    }

                                                    if (entryPrice !== null) {
                                                        if (tradeArgs.direction === 'LONG' && stopLoss >= entryPrice) {
                                                            setExecutionError('Stop loss must be below entry for LONG');
                                                            return;
                                                        }
                                                        if (tradeArgs.direction === 'LONG' && takeProfit <= entryPrice) {
                                                            setExecutionError('Take profit must be above entry for LONG');
                                                            return;
                                                        }
                                                        if (tradeArgs.direction === 'SHORT' && stopLoss <= entryPrice) {
                                                            setExecutionError('Stop loss must be above entry for SHORT');
                                                            return;
                                                        }
                                                        if (tradeArgs.direction === 'SHORT' && takeProfit >= entryPrice) {
                                                            setExecutionError('Take profit must be below entry for SHORT');
                                                            return;
                                                        }

                                                        if (triggerValue !== null) {
                                                            if (tradeArgs.direction === 'LONG' && triggerValue < entryPrice) {
                                                                setExecutionError('Trigger must be >= current price for LONG');
                                                                return;
                                                            }
                                                            if (tradeArgs.direction === 'SHORT' && triggerValue > entryPrice) {
                                                                setExecutionError('Trigger must be <= current price for SHORT');
                                                                return;
                                                            }
                                                        }
                                                    }

                                                    const nextArgs: ExecuteTradeArgs = { ...tradeArgs, stop_loss_price: stopLoss, take_profit_price: takeProfit };
                                                    if (triggerValue !== null) {
                                                        nextArgs.trigger_price = triggerValue;
                                                    } else {
                                                        delete (nextArgs as Partial<ExecuteTradeArgs>).trigger_price;
                                                    }
                                                    overrideArgs = nextArgs;
                                                }

                                                const ok = await executeRecommendedAction(recommendation, overrideArgs);
                                                if (ok) advanceRecommendation();
                                            }}
                                            disabled={isExecuting || !canExecute}
                                        >
                                            {isExecuting ? 'Executing…' : 'Accept'}
                                        </button>
                                        <button
                                            className="confirm-no"
                                            onClick={() => {
                                                declineRecommendedAction(recommendation);
                                                advanceRecommendation();
                                            }}
                                            disabled={isExecuting}
                                        >
                                            Reject
                                        </button>
                                    </div>

                                    {analysis.analysisCost && (
                                        <div className="analysis-cost">
                                            Cost: <strong>{analysis.analysisCost.estimatedUsd === null ? 'N/A' : formatUsd(analysis.analysisCost.estimatedUsd)}</strong>
                                            {analysis.analysisCost.tokens?.total !== null && analysis.analysisCost.tokens?.total !== undefined && (
                                                <span className="analysis-tokens">
                                                    {' '}({analysis.analysisCost.tokens.total} tokens)
                                                </span>
                                            )}
                                        </div>
                                    )}
                                </div>
                            );
                        })()}

                        {recommendations.length > 0 && recommendationIndex >= recommendations.length && (
                            <div className="recommendations-body">
                                <div className="recommendation-summary">
                                    Done — all recommendations processed.
                                </div>
                            </div>
                        )}
                    </div>
                </div>
            )}

            <div className="bot-header">
                <h3>AI Trading Bot</h3>
                <div className={`bot-status-badge ${status?.isActive ? 'active' : 'inactive'}`}>
                    {status?.isActive ? 'ACTIVE' : 'PAUSED'}
                </div>
            </div>

            {!status?.apiConfigured && (
                <div className="bot-warning">
                    ⚠️ OpenAI API key not configured. Add it to server/.env
                </div>
            )}

            <div className="bot-controls">
                <button
                    className="bot-primary"
                    onClick={() => void handleRunButtonClick()}
                    disabled={botMode === 'manual' ? isAnalyzing : isAutotradeUpdating || !status?.autotrade}
                    title={
                        botMode === 'manual'
                            ? 'Runs analysis once and shows recommendations for confirmation.'
                            : status?.autotrade?.enabled
                                ? 'Stops the autotrade worker.'
                                : 'Starts the autotrade worker loop.'
                    }
                >
                    {botMode === 'manual'
                        ? (isAnalyzing ? '🔄 Running...' : '🚀 Run Bot')
                        : isAutotradeUpdating
                            ? (status?.autotrade?.enabled ? '⏹ Stopping…' : '🚀 Starting…')
                            : (status?.autotrade?.enabled ? '⏹ Stop Bot' : '🚀 Run Bot')}
                </button>
            </div>

            {status && (
                <div className="autotrade-panel">
                    <div className="autotrade-header">
                        <div className="autotrade-mode" role="group" aria-label="Trading mode">
                            <button
                                type="button"
                                className={`autotrade-mode-button ${botMode === 'manual' ? 'active' : ''}`}
                                onClick={() => void selectMode('manual')}
                                disabled={isAutotradeUpdating || isAutotradeRunning}
                                title={
                                    isAutotradeRunning
                                        ? 'Stop the autotrade worker before switching modes.'
                                        : 'Manual mode: run the bot for recommendations, execute only with explicit confirmation.'
                                }
                            >
                                Manual
                            </button>
                            <button
                                type="button"
                                className={`autotrade-mode-button ${botMode === 'autotrade' ? 'active' : ''}`}
                                onClick={() => void selectMode('autotrade')}
                                disabled={isAutotradeUpdating || !status.autotrade}
                                title="Autotrade worker: backend loop may analyze + execute automatically within bot limits."
                            >
                                Autotrade
                            </button>
                        </div>
                        <div className="autotrade-meta">
                            {status.autotrade ? (
                                <>
                                    <span>Entry {status.autotrade.config.timeframeMin}m</span>
                                    <span> · Regime {status.autotrade.config.regimeTimeframeMin}m</span>
                                    {status.autotrade.state.nextIntent && (
                                        <span> · Next {status.autotrade.state.nextIntent === 'manage' ? 'Manage' : 'Open'}</span>
                                    )}
                                </>
                            ) : (
                                <span>Backend update required</span>
                            )}
                        </div>
                    </div>

                    {botMode === 'autotrade' && !status.autotrade && (
                        <div className="analysis-error">
                            Autotrade + the unified `/api/bot/run` endpoint require the latest backend.
                            Restart `node server/index.js` (or `npm --prefix server run dev`).
                        </div>
                    )}

                    {botMode === 'autotrade' && status.autotrade && autotradeDraft && (
                        <div className="autotrade-config">
                            <label>
                                Scan (sec)
                                <input
                                    type="number"
                                    min={3}
                                    max={3600}
                                    value={autotradeDraft.intervalSec}
                                    onChange={(e) => {
                                        const next = Number.parseInt(e.target.value, 10);
                                        setAutotradeDraft((prev) => (prev ? { ...prev, intervalSec: Number.isFinite(next) ? next : prev.intervalSec } : prev));
                                    }}
                                    disabled={isAutotradeUpdating || status.autotrade.enabled}
                                />
                            </label>
                            <label>
                                LLM cooldown (sec)
                                <input
                                    type="number"
                                    min={10}
                                    max={86400}
                                    value={autotradeDraft.minLlmIntervalSec}
                                    onChange={(e) => {
                                        const next = Number.parseInt(e.target.value, 10);
                                        setAutotradeDraft((prev) => (prev ? { ...prev, minLlmIntervalSec: Number.isFinite(next) ? next : prev.minLlmIntervalSec } : prev));
                                    }}
                                    disabled={isAutotradeUpdating || status.autotrade.enabled}
                                />
                            </label>
                            <label>
                                Score ≥
                                <input
                                    type="number"
                                    min={0}
                                    max={100}
                                    value={autotradeDraft.minScore}
                                    onChange={(e) => {
                                        const next = Number.parseFloat(e.target.value);
                                        setAutotradeDraft((prev) => (prev ? { ...prev, minScore: Number.isFinite(next) ? next : prev.minScore } : prev));
                                    }}
                                    disabled={isAutotradeUpdating || status.autotrade.enabled}
                                />
                            </label>
                        </div>
                    )}

                    {autotradeError && (
                        <div className="analysis-error">
                            {autotradeError}
                        </div>
                    )}

                    {status.autotrade?.state.lastScan != null && (
                        <div className="analysis-note">
                            {(() => {
                                const scan = status.autotrade?.state.lastScan;
                                if (!scan || typeof scan !== 'object') return null;
                                const scanRecord = scan as Record<string, unknown>;
                                const reason = typeof scanRecord.reason === 'string' ? scanRecord.reason : null;
                                const skipped = typeof scanRecord.skipped === 'boolean' ? scanRecord.skipped : null;

                                const candidate = scanRecord.candidate;
                                const candidatePairIndex = candidate && typeof candidate === 'object' && Number.isFinite((candidate as Record<string, unknown>).pairIndex as number)
                                    ? Number((candidate as Record<string, unknown>).pairIndex)
                                    : null;
                                const candidateScore = candidate && typeof candidate === 'object' && Number.isFinite((candidate as Record<string, unknown>).score as number)
                                    ? Number((candidate as Record<string, unknown>).score)
                                    : null;

                                const parts: string[] = [];
                                parts.push(`Last scan: ${skipped === null ? 'unknown' : (skipped ? 'skipped' : 'ran')}`);
                                if (reason) parts.push(`reason=${reason}`);
                                if (status.autotrade?.state.lastTickAtMs) {
                                    parts.push(`tick=${new Date(status.autotrade.state.lastTickAtMs).toLocaleTimeString()}`);
                                }
                                if (candidatePairIndex !== null) {
                                    parts.push(`candidate=${getPairLabel(candidatePairIndex)}${candidateScore !== null ? ` (${candidateScore.toFixed(1)})` : ''}`);
                                }

                                const hint = reason === 'market_state_stale'
                                    ? 'Market data looks stale — restart the backend or POST /api/market/backfill.'
                                    : reason === 'market_state_empty'
                                        ? 'Market data is warming up — wait a minute for backfill.'
                                        : reason === 'no_ranked_candidates'
                                            ? 'No scorable candidates yet — likely indicators still warming up.'
                                            : null;

                                return `${parts.join(' · ')}${hint ? ` — ${hint}` : ''}`;
                            })()}
                        </div>
                    )}

                    {status.autotrade?.state.lastError && (
                        <div className="analysis-error">
                            Autotrade: {status.autotrade.state.lastError}
                        </div>
                    )}
                </div>
            )}

            {runError && (
                <div className="analysis-error">
                    {runError}
                </div>
            )}

            <div className="bot-stats">
                <div className="stat">
                    <span className="stat-label">Today's PnL</span>
                    <span className={`stat-value ${(status?.todayPnL || 0) >= 0 ? 'positive' : 'negative'}`}>
                        ${(status?.todayPnL || 0).toFixed(2)}
                    </span>
                </div>
                <div className="stat">
                    <span className="stat-label">Trades Today</span>
                    <span className="stat-value">{status?.tradesExecuted || 0}</span>
                </div>
                <div className="stat">
                    <span className="stat-label">Loss Limit</span>
                    <span className="stat-value">-${status?.safetyLimits?.dailyLossLimit || 0}</span>
                </div>
            </div>

            <div className="pending-orders">
                <div className="pending-orders-header">
                    <h4>⏳ Pending BOT Orders</h4>
                    <button
                        type="button"
                        className="pending-orders-refresh"
                        onClick={() => void refreshPendingBotOrders()}
                        disabled={isPendingOrdersLoading}
                        title="Refresh pending orders"
                    >
                        {isPendingOrdersLoading ? 'Refreshing…' : 'Refresh'}
                    </button>
                </div>

                {pendingOrdersError && (
                    <div className="analysis-error">
                        {pendingOrdersError}
                    </div>
                )}

                {!pendingOrdersError && pendingBotOrders.length === 0 && (
                    <div className="loading">
                        {isPendingOrdersLoading ? 'Loading pending orders…' : 'No pending bot orders.'}
                    </div>
                )}

                {pendingBotOrders.length > 0 && (
                    <div className="pending-orders-list">
                        {pendingBotOrders.slice(0, 12).map((t) => {
                            const label = getPairLabel(t.pair_index);
                            const trigger = typeof t.trigger_price === 'number' ? t.trigger_price : null;
                            const entry = typeof t.entry_price === 'number' ? t.entry_price : null;
                            const ageMin = typeof t.entry_time === 'number'
                                ? Math.max(0, Math.floor((Date.now() / 1000 - t.entry_time) / 60))
                                : null;

                            return (
                                <div key={t.id} className="pending-order-row">
                                    <div className="pending-order-main">
                                        <div className="pending-order-title">
                                            <span className="pending-order-pair">{label}</span>
                                            <span className="pending-order-chip">#{t.id}</span>
                                            <span className="pending-order-chip">{t.direction}</span>
                                            {ageMin !== null && <span className="pending-order-chip">{ageMin}m</span>}
                                        </div>
                                        <div className="pending-order-sub">
                                            {trigger !== null ? `Trigger ${formatUsd(trigger)}` : 'Trigger N/A'}
                                            {' '}· Ref {entry !== null ? formatUsd(entry) : 'N/A'}
                                            {' '}· {t.collateral} @ {t.leverage}x
                                        </div>
                                    </div>

                                    <div className="pending-order-actions">
                                        {onFocusTrade && (
                                            <button
                                                type="button"
                                                className="pending-order-focus"
                                                onClick={() => onFocusTrade(t.id, t.pair_index)}
                                                disabled={cancelingTradeId === t.id}
                                            >
                                                View
                                            </button>
                                        )}
                                        <button
                                            type="button"
                                            className="pending-order-cancel"
                                            onClick={() => void cancelPendingOrder(t.id)}
                                            disabled={cancelingTradeId !== null}
                                            title="Cancel this pending (trigger) order"
                                        >
                                            {cancelingTradeId === t.id ? 'Canceling…' : 'Cancel'}
                                        </button>
                                    </div>
                                </div>
                            );
                        })}

                        {pendingBotOrders.length > 12 && (
                            <div className="loading">Showing 12 of {pendingBotOrders.length} pending orders.</div>
                        )}
                    </div>
                )}
            </div>

            <div className="indicators-section">
                <h4>📊 Live Indicators</h4>
                {indicators ? (
	                    <div className="indicators-grid">
	                        <div className="indicator">
	                            <span className="ind-label">RSI</span>
	                            <span className={`ind-value ${getRsiClass(indicators.indicators?.rsi)}`}>
	                                {formatSignal(indicators.indicators?.rsi)}
	                            </span>
	                        </div>
                        <div className="indicator">
                            <span className="ind-label">MACD</span>
                            <span className={`ind-value ${(indicators.indicators?.macd?.MACD || 0) > (indicators.indicators?.macd?.signal || 0) ? 'bullish' : 'bearish'
                                }`}>
                                {formatSignal(indicators.indicators?.macd?.MACD)}
                            </span>
                        </div>
                        <div className="indicator">
                            <span className="ind-label">BB Position</span>
                            <span className="ind-value">
                                {indicators.price && indicators.indicators?.bollingerBands ? (
                                    indicators.price > indicators.indicators.bollingerBands.upper ? 'Above' :
                                        indicators.price < indicators.indicators.bollingerBands.lower ? 'Below' : 'Within'
                                ) : 'N/A'}
                            </span>
                        </div>
                        <div className="indicator">
                            <span className="ind-label">EMA Trend</span>
                            <span className={`ind-value ${(indicators.indicators?.ema?.ema9 || 0) > (indicators.indicators?.ema?.ema21 || 0) ? 'bullish' : 'bearish'
                                }`}>
                                {(indicators.indicators?.ema?.ema9 || 0) > (indicators.indicators?.ema?.ema21 || 0) ? '↑ Bullish' : '↓ Bearish'}
                            </span>
                        </div>
                        <div className="indicator">
                            <span className="ind-label">ATR</span>
                            <span className="ind-value">{formatSignal(indicators.indicators?.atr)}</span>
                        </div>
                        <div className="indicator">
                            <span className="ind-label">Stoch K</span>
                            <span className={`ind-value ${(indicators.indicators?.stochastic?.k || 50) > 80 ? 'overbought' :
                                (indicators.indicators?.stochastic?.k || 50) < 20 ? 'oversold' : ''
                                }`}>
                                {formatSignal(indicators.indicators?.stochastic?.k)}
                            </span>
                        </div>
                        <div className="indicator overall">
                            <span className="ind-label">Overall Bias</span>
                            <span className={`ind-value ${indicators.summary?.overallBias?.toLowerCase()}`}>
                                {indicators.summary?.overallBias || 'N/A'}
                            </span>
                        </div>
                    </div>
                ) : indicatorsError ? (
                    <div className="loading">{indicatorsError}</div>
                ) : (
                    <div className="loading">Loading indicators...</div>
                )}
            </div>

            {lastAnalysis && (
                <div className="last-analysis">
                    <h4>🎯 Last Analysis</h4>
                    <div className="analysis-result">
                        <div className="analysis-action">
                            Action: <strong>{lastAnalysis.action}</strong>
                        </div>
                        {lastAnalysis.cacheMeta?.status && (
                            <div className="analysis-note">
                                Cache: <strong>{lastAnalysis.cacheMeta.status.toUpperCase()}</strong>
                            </div>
                        )}
                        {lastAnalysis.summary?.summary && (
                            <div className="analysis-summary">
                                {lastAnalysis.summary.summary}
                            </div>
                        )}
                        {Array.isArray(lastAnalysis.summary?.signals) && lastAnalysis.summary.signals.length > 0 && (
                            <div className="analysis-signals">
                                {lastAnalysis.summary.signals.slice(0, 4).map((s: MarketSignal, i: number) => (
                                    <div key={`${s.indicator}-${i}`} className="analysis-signal">
                                        <span className="signal-indicator">{s.indicator}</span>
                                        <span className="signal-value">{s.signal}</span>
                                    </div>
                                ))}
                            </div>
                        )}
                        {lastAnalysis.analysisCost && (
                            <div className="analysis-cost">
                                Cost: <strong>{lastAnalysis.analysisCost.estimatedUsd === null ? 'N/A' : formatUsd(lastAnalysis.analysisCost.estimatedUsd)}</strong>
                                {lastAnalysis.analysisCost.estimatedUsd === null && (
                                    <span className="analysis-tokens"> {' '}(set BOT_COST_PER_1K_TOKENS_USD)</span>
                                )}
                                {lastAnalysis.analysisCost.tokens?.total !== null && lastAnalysis.analysisCost.tokens?.total !== undefined && (
                                    <span className="analysis-tokens">
                                        {' '}({lastAnalysis.analysisCost.tokens.total} tokens)
                                    </span>
                                )}
                            </div>
                        )}
                        {lastAnalysis.tradingVariables && (
                            <div className="analysis-trading-vars">
                                <div className="analysis-trading-vars-title">Trading Variables</div>
                                <div className="analysis-trading-vars-grid">
                                    <div>
                                        <span className="tv-label">Spread</span>
                                        <span className="tv-value">{formatPercent(lastAnalysis.tradingVariables.pair?.spreadPercent)}</span>
                                    </div>
                                    <div>
                                        <span className="tv-label">Fees</span>
                                        <span className="tv-value">
                                            {formatPercent(lastAnalysis.tradingVariables.fees?.positionSizeFeePercent)} pos / {formatPercent(lastAnalysis.tradingVariables.fees?.oraclePositionSizeFeePercent)} oracle
                                        </span>
                                    </div>
                                    <div>
                                        <span className="tv-label">Min Size</span>
                                        <span className="tv-value">{lastAnalysis.tradingVariables.fees?.minPositionSizeUsd ? formatUsd(lastAnalysis.tradingVariables.fees.minPositionSizeUsd) : 'N/A'}</span>
                                    </div>
                                    <div>
                                        <span className="tv-label">OI Skew</span>
                                        <span className="tv-value">
                                            {formatPercent(lastAnalysis.tradingVariables.openInterest?.skewPercent, 1)} ({formatCompactNumber(lastAnalysis.tradingVariables.openInterest?.long)} L / {formatCompactNumber(lastAnalysis.tradingVariables.openInterest?.short)} S)
                                        </span>
                                    </div>
                                </div>
                            </div>
                        )}
                        {lastAnalysis.tradingVariablesError && !lastAnalysis.tradingVariables && (
                            <div className="analysis-note">
                                Trading variables unavailable: {lastAnalysis.tradingVariablesError}
                            </div>
                        )}
                        {lastAnalysis.args?.reasoning && (
                            <div className="analysis-reasoning">
                                {lastAnalysis.args.reasoning}
                            </div>
                        )}

                        {lastAnalysis.declined && (
                            <div className="analysis-note">
                                Skipped (not executed).
                            </div>
                        )}

                        {lastAnalysis.tradeExecuted && (
                            <div className="trade-executed">
                                Trade #{lastAnalysis.tradeExecuted.tradeId} executed
                            </div>
                        )}
                        {lastAnalysis.positionClosed && (
                            <div className="trade-executed">
                                Position #{lastAnalysis.positionClosed.tradeId} closed
                            </div>
                        )}
                    </div>
                </div>
            )}

            <div className="debug-section">
                <div className="debug-header">
                    <h4>🧪 Debug</h4>
                    <button
                        type="button"
                        className="debug-toggle"
                        onClick={() => setIsDebugOpen((prev) => !prev)}
                    >
                        {isDebugOpen ? 'Hide' : 'Show'}
                    </button>
                    {isDebugOpen && (
                        <button
                            type="button"
                            className="debug-refresh"
                            disabled={isDebugLoading}
                            onClick={() => void refreshDebug()}
                        >
                            {isDebugLoading ? 'Loading…' : 'Refresh'}
                        </button>
                    )}
                </div>

                {isDebugOpen && debugError && (
                    <div className="analysis-error">
                        {debugError}
                    </div>
                )}

                {isDebugOpen && debugOverview && (() => {
                    const hit = debugOverview.metrics.recentSummary.find((r) => r.name === 'llm_analysis_cache_hit')?.count ?? 0;
                    const miss = debugOverview.metrics.recentSummary.find((r) => r.name === 'llm_analysis_cache_miss')?.count ?? 0;
                    const lastReflection = debugOverview.last.botReflectionTs ? new Date(debugOverview.last.botReflectionTs * 1000).toLocaleString() : 'N/A';
                    return (
                        <>
                            <div className="debug-kv">
                                <div>
                                    <span className="debug-key">Cache (24h)</span>
                                    <span className="debug-value">{hit} hit · {miss} miss</span>
                                </div>
                                <div>
                                    <span className="debug-key">Reflections</span>
                                    <span className="debug-value">{debugOverview.counts.botReflections} total · last {lastReflection}</span>
                                </div>
                                <div>
                                    <span className="debug-key">Thresholds</span>
                                    <span className="debug-value">{debugOverview.thresholds.active ? 'active' : 'none active'}</span>
                                </div>
                            </div>

                            {debugReflections?.rows?.length ? (
                                <div className="debug-reflections">
                                    <div className="debug-subtitle">Recent reflections (this pair)</div>
                                    {debugReflections.rows.slice(0, 3).map((row) => (
                                        <div key={row.id} className="debug-reflection-item">
                                            <div className="debug-reflection-meta">
                                                {new Date(row.timestamp * 1000).toLocaleString()} · {row.scope}
                                                {typeof row.trade_id === 'number' ? ` · trade #${row.trade_id}` : ''}
                                            </div>
                                            <div className="debug-reflection-summary">
                                                {row.summary ?? 'No summary'}
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            ) : (
                                <div className="analysis-note">No reflections for this pair yet.</div>
                            )}

                            <details className="debug-details">
                                <summary>Raw debug JSON</summary>
                                <pre className="debug-pre">{JSON.stringify({ overview: debugOverview, reflections: debugReflections }, null, 2)}</pre>
                            </details>
                        </>
                    );
                })()}
            </div>

            <div className="decisions-section">
                <h4>📜 Recent Decisions</h4>
                <div
                    className="decisions-list"
                    ref={decisionsListRef}
                    onScroll={onDecisionsListScroll}
                    role="feed"
                    aria-busy={isLoadingMoreDecisions}
                    aria-label="Bot decisions history"
                >
                    {decisions.length === 0 ? (
                        <div className="no-decisions">No decisions yet</div>
                    ) : (
                        decisions.map((d) => {
                            const reasoning = getDecisionReasoning(d);
                            const positionId = getDecisionPositionId(d);
                            const candidateScore = getDecisionCandidateScore(d);
                            const isExpanded = expandedDecisionId === d.id;
                            return (
                                <div
                                    key={d.id}
                                    className={`decision-item ${isExpanded ? 'expanded' : ''}`}
                                    role="button"
                                    tabIndex={0}
                                    onClick={() => setExpandedDecisionId((prev) => (prev === d.id ? null : d.id))}
                                    onKeyDown={(event) => {
                                        if (event.key !== 'Enter' && event.key !== ' ') return;
                                        event.preventDefault();
                                        setExpandedDecisionId((prev) => (prev === d.id ? null : d.id));
                                    }}
                                >
                                    <div className="decision-row">
                                        <span className="decision-time">
                                            {new Date(d.timestamp * 1000).toLocaleTimeString()}
                                        </span>
                                        <span className={`decision-action ${d.action}`}>
                                            {d.action}
                                        </span>
                                        {typeof positionId === 'number' && (
                                            <button
                                                type="button"
                                                className="decision-position"
                                                onClick={(event) => {
                                                    event.stopPropagation();
                                                    onFocusTrade?.(
                                                        positionId,
                                                        typeof d.pair_index === 'number' ? d.pair_index : null
                                                    );
                                                }}
                                            >
                                                Position #{positionId}
                                            </button>
                                        )}
                                        {candidateScore !== null && (
                                            <span className="decision-score">
                                                Score {candidateScore.toFixed(2)}
                                            </span>
                                        )}
                                        <span className="decision-reasoning">
                                            {reasoning ? formatReasoningSnippet(reasoning) : 'No reasoning'}
                                        </span>
                                        </div>
                                    {isExpanded && (
                                        <div className="decision-details">
                                            <div className="decision-details-meta">
                                                {getPairLabel(d.pair_index)}
                                                {typeof positionId === 'number' ? ` · Position #${positionId}` : ''}
                                                {typeof d.timeframe_min === 'number' ? ` · ${d.timeframe_min}m` : ''}
                                                {(() => {
                                                    const llmLabel = getDecisionLlmLabel(d);
                                                    return llmLabel ? ` · ${llmLabel}` : '';
                                                })()}
                                            </div>
                                            <div className="decision-details-reasoning">
                                                {reasoning ?? 'No reasoning recorded for this decision.'}
                                            </div>
                                        </div>
                                    )}
                                </div>
                            );
                        })
                    )}
                </div>
                <div className="decisions-footer">
                    {hasMoreDecisions ? (
                        <button
                            type="button"
                            className="decisions-load-more"
                            disabled={isLoadingMoreDecisions || decisions.length === 0}
                            onClick={loadOlderDecisions}
                        >
                            {isLoadingMoreDecisions ? 'Loading…' : 'Load older'}
                        </button>
                    ) : (
                        <div className="decisions-end">End of history</div>
                    )}
                </div>
            </div>

            <div className="decisions-section">
                <h4>🧭 Recent Universe Decisions</h4>
                <div className="decisions-list" role="feed" aria-label="Universe selection history">
                    {universeDecisions.length === 0 ? (
                        <div className="no-decisions">No universe decisions yet</div>
                    ) : (
                        universeDecisions.map((row) => {
                            const reasoning = getUniverseDecisionReasoning(row);
                            const llmLabel = getUniverseDecisionLlmLabel(row);
                            const isExpanded = expandedUniverseDecisionId === row.id;

                            const actionLabel = row.action ?? 'unknown';
                            const selectedPair = typeof row.selected_pair_index === 'number' ? getPairLabel(row.selected_pair_index) : null;

                            return (
                                <div
                                    key={row.id}
                                    className={`decision-item ${isExpanded ? 'expanded' : ''}`}
                                    role="button"
                                    tabIndex={0}
                                    onClick={() => setExpandedUniverseDecisionId((prev) => (prev === row.id ? null : row.id))}
                                    onKeyDown={(event) => {
                                        if (event.key !== 'Enter' && event.key !== ' ') return;
                                        event.preventDefault();
                                        setExpandedUniverseDecisionId((prev) => (prev === row.id ? null : row.id));
                                    }}
                                >
                                    <div className="decision-row">
                                        <span className="decision-time">{new Date(row.timestamp * 1000).toLocaleTimeString()}</span>
                                        <span className={`decision-action ${actionLabel}`}>{actionLabel}</span>
                                        {selectedPair && (
                                            <span className="decision-score">{selectedPair}</span>
                                        )}
                                        <span className="decision-reasoning">
                                            {reasoning ? formatReasoningSnippet(reasoning) : 'No reasoning'}
                                        </span>
                                    </div>

                                    {isExpanded && (
                                        <div className="decision-details">
                                            <div className="decision-details-meta">
                                                {typeof row.timeframe_min === 'number' ? `${row.timeframe_min}m` : 'Timeframe N/A'}
                                                {llmLabel ? ` · ${llmLabel}` : ''}
                                                {selectedPair ? ` · ${selectedPair}` : ''}
                                            </div>
                                            <div className="decision-details-reasoning">
                                                {reasoning ?? 'No reasoning recorded for this universe decision.'}
                                            </div>
                                        </div>
                                    )}
                                </div>
                            );
                        })
                    )}
                </div>
            </div>
        </div>
    );
};
