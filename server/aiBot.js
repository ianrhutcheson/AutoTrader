/**
 * AI Trading Bot Module
 * Uses OpenAI GPT-5.2 via the OpenAI Agents SDK for structured decisions
 */

require('dotenv').config();
const crypto = require('crypto');
const { query } = require('./db');
const { calculateIndicators, generateMarketSummary } = require('./indicators');
const botContext = require('./botContext');

let _agentsSdkPromise = null;
let _agentsRunner = null;

async function loadAgentsSdk() {
    if (_agentsSdkPromise) return _agentsSdkPromise;
    _agentsSdkPromise = import('@openai/agents');
    return _agentsSdkPromise;
}

async function getAgentsRunner() {
    if (_agentsRunner) return _agentsRunner;
    const sdk = await loadAgentsSdk();
    if (typeof sdk.setDefaultOpenAIKey === 'function') {
        sdk.setDefaultOpenAIKey(process.env.OPENAI_API_KEY);
    }
    _agentsRunner = new sdk.Runner({
        workflowName: 'perps-trader-bot'
    });
    return _agentsRunner;
}

function getZod() {
    // Zod v3 is required by the Agents SDK for strict output validation.
    // Keep it lazy to avoid loading unless the bot is actually used.
    // eslint-disable-next-line global-require
    return require('zod');
}

function buildUniverseAgentInstructions() {
    return `You are a discretionary perps market selector.

Given a ranked list of candidates (with indicators, costs, and liquidity), pick exactly one market to analyze/trade next.

Prefer using tool-provided context (ranked candidates, open exposure) over assumptions.

Rules:
1. Prefer clear signal confluence and higher liquidity/open-interest.
2. Penalize high costs (spread + fees).
3. Avoid conflicted/low-quality setups.
4. Avoid markets where there is already an open position on the same pair.
5. If none are good enough, skip.

Return ONLY a JSON object matching the required schema.`;
}

function buildTradingAgentInstructions(pairLabel = 'BTC/USD') {
    return `You are an expert cryptocurrency trader analyzing ${pairLabel} perpetual futures.

Your job is to output a single structured decision for what to do next. You do NOT execute trades.

Prefer using tool-provided context (market snapshot, costs, regime, open exposure) over assumptions.

TRADING RULES:
1. Only trade when you have high confidence (confidence >= 0.70) based on multiple confirming indicators.
2. Respect RSI/MACD/EMA/Bollinger/overall bias signals.
3. Consider costs and liquidity, and avoid trades where costs/liquidity make the setup unattractive.
4. Maximum collateral per trade: $${SAFETY_LIMITS.maxCollateral}.
5. Maximum leverage: ${SAFETY_LIMITS.maxLeverage}x.
6. If daily loss exceeds $${SAFETY_LIMITS.dailyLossLimit}, stop trading.

POSITION MANAGEMENT RULES:
1. If there is an existing OPEN position for this pair, prefer managing it (hold or close) rather than opening a new one.
2. If there is a PENDING (trigger) order for this pair that no longer makes sense, cancel it.
3. When closing or canceling, you MUST reference the correct trade_id from the provided openPositions list.

Output ONLY a JSON object matching the required schema.`;
}

async function runUniverseSelectionWithAgentsSdk({ candidates, openPositionsSummary, model }) {
    const sdk = await loadAgentsSdk();
    const { z } = getZod();

    const UniverseContextSchema = z.object({});
    const getUniverseContextTool = sdk.tool({
        name: 'get_universe_context',
        description: 'Returns the current ranked candidate markets and open positions summary for market selection.',
        parameters: UniverseContextSchema,
        strict: true,
        execute: async () => ({
            candidates,
            openPositions: openPositionsSummary
        })
    });

    const RankedCandidatesSchema = z.object({
        timeframe_min: z.number().int().positive().optional(),
        limit: z.number().int().positive().max(50).optional()
    });
    const getRankedCandidatesTool = sdk.tool({
        name: 'get_ranked_candidates',
        description: 'Returns the latest ranked candidates from the DB for a timeframe (preferred to avoid stale in-memory lists).',
        parameters: RankedCandidatesSchema,
        strict: true,
        execute: async (input) => {
            const timeframeMin = Number.isFinite(Number(input?.timeframe_min)) ? Number(input.timeframe_min) : 15;
            const limit = Number.isFinite(Number(input?.limit)) ? Number(input.limit) : 10;
            return botContext.getRankedCandidates({ timeframeMin, limit });
        }
    });

    const OpenExposureSchema = z.object({});
    const getOpenExposureTool = sdk.tool({
        name: 'get_open_exposure',
        description: 'Returns open BOT positions and an exposure summary. Use this to avoid adding risk when already exposed.',
        parameters: OpenExposureSchema,
        strict: true,
        execute: async () => botContext.getOpenExposure({ source: 'BOT' })
    });

    const SupportedTimeframesSchema = z.object({
        pair_index: z.number().int().nonnegative().optional(),
        timeframe_min: z.number().int().positive().optional()
    });
    const getSupportedTimeframesTool = sdk.tool({
        name: 'get_supported_timeframes',
        description: 'Returns available timeframe_min values currently stored in market_state (global or per pair).',
        parameters: SupportedTimeframesSchema,
        strict: true,
        execute: async (input) => {
            const pairIndex = Number.isFinite(Number(input?.pair_index)) ? Number(input.pair_index) : null;
            const timeframeMin = Number.isFinite(Number(input?.timeframe_min)) ? Number(input.timeframe_min) : null;
            return botContext.getSupportedTimeframes({ pairIndex, timeframeMin });
        }
    });

    const SelectionSchema = z.discriminatedUnion('action', [
        z.object({
            action: z.literal('select_market'),
            args: z.object({
                pair_index: z.number().int().nonnegative(),
                reasoning: z.string().min(1)
            })
        }),
        z.object({
            action: z.literal('skip_trade'),
            args: z.object({
                reasoning: z.string().min(1)
            })
        })
    ]);

    const agent = new sdk.Agent({
        name: 'Universe Selector',
        instructions: buildUniverseAgentInstructions(),
        model: model || 'gpt-5.2',
        outputType: SelectionSchema,
        tools: [getUniverseContextTool, getSupportedTimeframesTool, getRankedCandidatesTool, getOpenExposureTool],
        modelSettings: {
            temperature: 0.2
        }
    });

    const runner = await getAgentsRunner();
    const input = 'Select the best market now. First call get_supported_timeframes (optional; can include timeframe_min to check freshness), get_ranked_candidates (or get_universe_context), and get_open_exposure, then output your decision JSON.';
    const result = await runner.run(agent, input, {
        maxTurns: 3,
        traceMetadata: {
            workflow: 'select_best_market',
            candidates: Array.isArray(candidates) ? candidates.length : 0
        }
    });

    const usage = result?.rawResponses?.[result.rawResponses.length - 1]?.usage ?? null;
    return {
        output: result.finalOutput,
        usage
    };
}

async function runMarketDecisionWithAgentsSdk({ pairLabel, marketContext, model, timeframeMin, pairIndex }) {
    const sdk = await loadAgentsSdk();
    const { z } = getZod();

    const MarketContextSchema = z.object({});
    const getMarketContextTool = sdk.tool({
        name: 'get_market_context',
        description: 'Returns the current market context, indicators, trading variables, and open positions summary for this pair/timeframe.',
        parameters: MarketContextSchema,
        strict: true,
        execute: async () => marketContext
    });

    const CostsSchema = z.object({});
    const getCostsAndLiquidityTool = sdk.tool({
        name: 'get_costs_and_liquidity',
        description: 'Returns latest costs (spread/fees) and liquidity (OI/skew) from stored Gains trading variables for this pair.',
        parameters: CostsSchema,
        strict: true,
        execute: async () => botContext.getCostsAndLiquidity(pairIndex)
    });

    const RegimeSchema = z.object({
        regime_timeframe_min: z.number().int().positive().optional()
    });
    const getRegimeSnapshotTool = sdk.tool({
        name: 'get_regime_snapshot',
        description: 'Returns higher-timeframe regime snapshot (trend/regime/chop).',
        parameters: RegimeSchema,
        strict: true,
        execute: async (input) => {
            const fallback = Number.isFinite(Number(process.env.BOT_AUTOTRADE_REGIME_TIMEFRAME_MIN))
                ? Number(process.env.BOT_AUTOTRADE_REGIME_TIMEFRAME_MIN)
                : 60;
            const tf = Number.isFinite(Number(input?.regime_timeframe_min)) ? Number(input.regime_timeframe_min) : fallback;
            return botContext.getRegimeSnapshot(pairIndex, tf);
        }
    });

    const SupportedTimeframesSchema = z.object({
        pair_index: z.number().int().nonnegative().optional(),
        timeframe_min: z.number().int().positive().optional()
    });
    const getSupportedTimeframesTool = sdk.tool({
        name: 'get_supported_timeframes',
        description: 'Returns available timeframe_min values currently stored in market_state (global or per pair).',
        parameters: SupportedTimeframesSchema,
        strict: true,
        execute: async (input) => {
            const requestedPair = Number.isFinite(Number(input?.pair_index)) ? Number(input.pair_index) : pairIndex;
            const timeframeMin = Number.isFinite(Number(input?.timeframe_min)) ? Number(input.timeframe_min) : null;
            return botContext.getSupportedTimeframes({ pairIndex: requestedPair, timeframeMin });
        }
    });

    const MultiTimeframeSchema = z.object({
        timeframes_min: z.array(z.number().int().positive()).min(1).max(12),
        pair_index: z.number().int().nonnegative().optional()
    });
    const getMultiTimeframeSnapshotsTool = sdk.tool({
        name: 'get_multi_timeframe_snapshots',
        description: 'Returns market_state snapshots for a pair across multiple timeframe_min values in one call.',
        parameters: MultiTimeframeSchema,
        strict: true,
        execute: async (input) => {
            const requestedPair = Number.isFinite(Number(input?.pair_index)) ? Number(input.pair_index) : pairIndex;
            return botContext.getMultiTimeframeSnapshots(requestedPair, input?.timeframes_min);
        }
    });

    const ExposureSchema = z.object({});
    const getOpenExposureTool = sdk.tool({
        name: 'get_open_exposure',
        description: 'Returns open BOT positions and exposure summary. Use this before adding risk.',
        parameters: ExposureSchema,
        strict: true,
        execute: async () => botContext.getOpenExposure({ source: 'BOT' })
    });

    const SimilarDecisionsSchema = z.object({
        lookback_days: z.number().int().positive().max(365).optional(),
        limit: z.number().int().positive().max(25).optional(),
        include_other_pairs: z.boolean().optional()
    });
    const getSimilarDecisionsTool = sdk.tool({
        name: 'get_similar_decisions',
        description: 'Returns similar past execute_trade bot decisions (by indicator-state similarity) with forward-return outcomes and trade PnL when available.',
        parameters: SimilarDecisionsSchema,
        strict: true,
        execute: async (input) => {
            const lookbackDays = Number.isFinite(Number(input?.lookback_days)) ? Number(input.lookback_days) : 60;
            const limit = Number.isFinite(Number(input?.limit)) ? Number(input.limit) : 8;
            const includeOtherPairs = typeof input?.include_other_pairs === 'boolean' ? input.include_other_pairs : true;
            return botContext.getSimilarDecisions({
                pairIndex,
                timeframeMin,
                lookbackDays,
                limit,
                includeOtherPairs,
                currentFeatures: marketContext?.indicators ? {
                    price: marketContext.currentPrice ?? marketContext.price ?? null,
                    rsi: marketContext.indicators?.rsi ?? null,
                    macdHist: marketContext.indicators?.macd?.histogram ?? marketContext.indicators?.macd_histogram ?? null,
                    bbZ: null,
                    atrPct: (Number.isFinite(marketContext.indicators?.atr) && Number.isFinite(marketContext.currentPrice) && marketContext.currentPrice > 0)
                        ? (marketContext.indicators.atr / marketContext.currentPrice)
                        : null,
                    stochK: marketContext.indicators?.stochastic?.k ?? marketContext.indicators?.stoch_k ?? null,
                    priceVsEma21: (Number.isFinite(marketContext.currentPrice) && Number.isFinite(marketContext.indicators?.ema?.ema21))
                        ? ((marketContext.currentPrice - marketContext.indicators.ema.ema21) / marketContext.indicators.ema.ema21)
                        : null,
                    priceVsEma200: (Number.isFinite(marketContext.currentPrice) && Number.isFinite(marketContext.indicators?.ema?.ema200))
                        ? ((marketContext.currentPrice - marketContext.indicators.ema.ema200) / marketContext.indicators.ema.ema200)
                        : null
                } : null
            });
        }
    });

    const ExecuteTradeArgsSchema = z.object({
        direction: z.enum(['LONG', 'SHORT']),
        collateral: z.number().positive(),
        leverage: z.number().positive(),
        stop_loss_price: z.number().positive(),
        take_profit_price: z.number().positive(),
        trigger_price: z.number().positive().optional(),
        confidence: z.number().min(0).max(1),
        reasoning: z.string().min(1),
        invalidation: z.string().min(1).optional()
    });

    const ClosePositionArgsSchema = z.object({
        trade_id: z.number().int().positive(),
        confidence: z.number().min(0).max(1),
        reasoning: z.string().min(1)
    });

    const CancelPendingArgsSchema = z.object({
        trade_id: z.number().int().positive(),
        confidence: z.number().min(0).max(1),
        reasoning: z.string().min(1)
    });

    const HoldPositionArgsSchema = z.object({
        confidence: z.number().min(0).max(1),
        reasoning: z.string().min(1)
    });

    const DecisionSchema = z.discriminatedUnion('action', [
        z.object({ action: z.literal('execute_trade'), args: ExecuteTradeArgsSchema }),
        z.object({ action: z.literal('close_position'), args: ClosePositionArgsSchema }),
        z.object({ action: z.literal('cancel_pending'), args: CancelPendingArgsSchema }),
        z.object({ action: z.literal('hold_position'), args: HoldPositionArgsSchema })
    ]);

    const agent = new sdk.Agent({
        name: 'Trader',
        instructions: buildTradingAgentInstructions(pairLabel),
        model: model || 'gpt-5.2',
        outputType: DecisionSchema,
        tools: [getMarketContextTool, getSupportedTimeframesTool, getMultiTimeframeSnapshotsTool, getCostsAndLiquidityTool, getRegimeSnapshotTool, getOpenExposureTool, getSimilarDecisionsTool],
        modelSettings: {
            temperature: 0.2
        }
    });

    const runner = await getAgentsRunner();
    const input = 'Analyze the market now. First call get_market_context and check freshness (stale/ageSec). If you need multi-timeframe confirmation, call get_supported_timeframes (optionally with timeframe_min) and/or get_multi_timeframe_snapshots (check anyStale). Then call get_costs_and_liquidity, get_regime_snapshot, get_open_exposure, and get_similar_decisions. Then output a decision JSON.';
    const result = await runner.run(agent, input, {
        maxTurns: 3,
        traceMetadata: {
            workflow: 'analyze_market',
            pairIndex,
            timeframeMin
        }
    });

    const usage = result?.rawResponses?.[result.rawResponses.length - 1]?.usage ?? null;
    return {
        output: result.finalOutput,
        usage
    };
}

// Safety limits from environment
const SAFETY_LIMITS = {
    maxCollateral: parseFloat(process.env.BOT_MAX_COLLATERAL) || 500,
    maxLeverage: parseFloat(process.env.BOT_MAX_LEVERAGE) || 20,
    dailyLossLimit: parseFloat(process.env.BOT_DAILY_LOSS_LIMIT) || 1000,
    // If >0, requires reward/risk >= this ratio when both SL and TP are set.
    // Default 0 disables the check to preserve legacy behavior.
    minRiskReward: parseFloat(process.env.BOT_MIN_RISK_REWARD) || 0
};

function parseOptionalFloat(value) {
    const parsed = parseFloat(value);
    return Number.isFinite(parsed) ? parsed : null;
}

function validateAndNormalizeBotTradeParams({
    direction,
    collateral,
    leverage,
    currentPrice,
    stopLossPrice,
    takeProfitPrice,
    triggerPrice
}) {
    if (direction !== 'LONG' && direction !== 'SHORT') {
        return { ok: false, error: 'Invalid direction (must be LONG or SHORT)' };
    }

    const normalizedCurrentPrice = parseOptionalFloat(currentPrice);
    if (normalizedCurrentPrice === null || normalizedCurrentPrice <= 0) {
        return { ok: false, error: 'Invalid currentPrice' };
    }

    const normalizedCollateral = parseOptionalFloat(collateral);
    const normalizedLeverage = parseOptionalFloat(leverage);
    if (normalizedCollateral === null || normalizedCollateral <= 0) {
        return { ok: false, error: 'Invalid collateral' };
    }
    if (normalizedLeverage === null || normalizedLeverage <= 0) {
        return { ok: false, error: 'Invalid leverage' };
    }

    const safeCollateral = Math.min(normalizedCollateral, SAFETY_LIMITS.maxCollateral);
    const safeLeverage = Math.min(normalizedLeverage, SAFETY_LIMITS.maxLeverage);

    const normalizedStopLossPrice = parseOptionalFloat(stopLossPrice);
    const normalizedTakeProfitPrice = parseOptionalFloat(takeProfitPrice);
    const normalizedTriggerPrice = parseOptionalFloat(triggerPrice);

    if (normalizedStopLossPrice !== null && normalizedStopLossPrice <= 0) {
        return { ok: false, error: 'Invalid stop_loss_price' };
    }
    if (normalizedTakeProfitPrice !== null && normalizedTakeProfitPrice <= 0) {
        return { ok: false, error: 'Invalid take_profit_price' };
    }
    if (normalizedTriggerPrice !== null && normalizedTriggerPrice <= 0) {
        return { ok: false, error: 'Invalid trigger_price' };
    }

    if (normalizedStopLossPrice !== null) {
        if (direction === 'LONG' && normalizedStopLossPrice >= normalizedCurrentPrice) {
            return { ok: false, error: 'For LONG, stop_loss_price must be below current price' };
        }
        if (direction === 'SHORT' && normalizedStopLossPrice <= normalizedCurrentPrice) {
            return { ok: false, error: 'For SHORT, stop_loss_price must be above current price' };
        }
    }

    if (normalizedTakeProfitPrice !== null) {
        if (direction === 'LONG' && normalizedTakeProfitPrice <= normalizedCurrentPrice) {
            return { ok: false, error: 'For LONG, take_profit_price must be above current price' };
        }
        if (direction === 'SHORT' && normalizedTakeProfitPrice >= normalizedCurrentPrice) {
            return { ok: false, error: 'For SHORT, take_profit_price must be below current price' };
        }
    }

    if (normalizedStopLossPrice !== null && normalizedTakeProfitPrice !== null) {
        if (direction === 'LONG' && normalizedStopLossPrice >= normalizedTakeProfitPrice) {
            return { ok: false, error: 'For LONG, stop_loss_price must be below take_profit_price' };
        }
        if (direction === 'SHORT' && normalizedStopLossPrice <= normalizedTakeProfitPrice) {
            return { ok: false, error: 'For SHORT, stop_loss_price must be above take_profit_price' };
        }

        const risk = direction === 'LONG'
            ? (normalizedCurrentPrice - normalizedStopLossPrice)
            : (normalizedStopLossPrice - normalizedCurrentPrice);
        const reward = direction === 'LONG'
            ? (normalizedTakeProfitPrice - normalizedCurrentPrice)
            : (normalizedCurrentPrice - normalizedTakeProfitPrice);

        if (!(risk > 0) || !(reward > 0)) {
            return { ok: false, error: 'Invalid risk/reward distances (check SL/TP vs current price)' };
        }

        if (SAFETY_LIMITS.minRiskReward > 0) {
            const rr = reward / risk;
            if (!(rr >= SAFETY_LIMITS.minRiskReward)) {
                return { ok: false, error: `Risk/reward too low (${rr.toFixed(2)} < ${SAFETY_LIMITS.minRiskReward})` };
            }
        }
    }

    return {
        ok: true,
        direction,
        currentPrice: normalizedCurrentPrice,
        collateral: safeCollateral,
        leverage: safeLeverage,
        stopLossPrice: normalizedStopLossPrice,
        takeProfitPrice: normalizedTakeProfitPrice,
        triggerPrice: normalizedTriggerPrice
    };
}

const ANALYSIS_COST_RATES = {
    promptPer1KTokensUsd: parseOptionalFloat(process.env.BOT_COST_PROMPT_PER_1K_TOKENS_USD),
    completionPer1KTokensUsd: parseOptionalFloat(process.env.BOT_COST_COMPLETION_PER_1K_TOKENS_USD),
    totalPer1KTokensUsd: parseOptionalFloat(process.env.BOT_COST_PER_1K_TOKENS_USD)
};

function buildAnalysisCost(usage) {
    const normalized = normalizeUsage(usage);
    const promptTokens = normalized?.prompt_tokens ?? null;
    const completionTokens = normalized?.completion_tokens ?? null;
    const totalTokens = normalized?.total_tokens ?? null;

    let estimatedUsd = null;

    if (
        ANALYSIS_COST_RATES.promptPer1KTokensUsd !== null &&
        ANALYSIS_COST_RATES.completionPer1KTokensUsd !== null &&
        Number.isFinite(promptTokens) &&
        Number.isFinite(completionTokens)
    ) {
        estimatedUsd =
            (promptTokens / 1000) * ANALYSIS_COST_RATES.promptPer1KTokensUsd +
            (completionTokens / 1000) * ANALYSIS_COST_RATES.completionPer1KTokensUsd;
    } else if (
        ANALYSIS_COST_RATES.totalPer1KTokensUsd !== null &&
        Number.isFinite(totalTokens)
    ) {
        estimatedUsd = (totalTokens / 1000) * ANALYSIS_COST_RATES.totalPer1KTokensUsd;
    }

    return {
        estimatedUsd,
        tokens: {
            prompt: Number.isFinite(promptTokens) ? promptTokens : null,
            completion: Number.isFinite(completionTokens) ? completionTokens : null,
            total: Number.isFinite(totalTokens) ? totalTokens : null
        },
        rates: ANALYSIS_COST_RATES
    };
}

function normalizeUsage(usage) {
    if (!usage || typeof usage !== 'object') return null;

    // Chat Completions format
    const promptTokens = usage.prompt_tokens;
    const completionTokens = usage.completion_tokens;
    const totalTokens = usage.total_tokens;
    if (
        Number.isFinite(Number(promptTokens)) ||
        Number.isFinite(Number(completionTokens)) ||
        Number.isFinite(Number(totalTokens))
    ) {
        return {
            prompt_tokens: Number.isFinite(Number(promptTokens)) ? Number(promptTokens) : null,
            completion_tokens: Number.isFinite(Number(completionTokens)) ? Number(completionTokens) : null,
            total_tokens: Number.isFinite(Number(totalTokens)) ? Number(totalTokens) : null
        };
    }

    // Responses format
    const inputTokens = usage.input_tokens;
    const outputTokens = usage.output_tokens;
    const responsesTotal = usage.total_tokens;
    return {
        prompt_tokens: Number.isFinite(Number(inputTokens)) ? Number(inputTokens) : null,
        completion_tokens: Number.isFinite(Number(outputTokens)) ? Number(outputTokens) : null,
        total_tokens: Number.isFinite(Number(responsesTotal))
            ? Number(responsesTotal)
            : (Number.isFinite(Number(inputTokens)) && Number.isFinite(Number(outputTokens))
                ? Number(inputTokens) + Number(outputTokens)
                : null)
    };
}

// Bot state
let botState = {
    isActive: false,
    lastAnalysis: null,
    todayPnL: 0,
    tradesExecuted: 0
};

const PROMPT_VERSION = process.env.BOT_PROMPT_VERSION || 'v1';
const ANALYSIS_CACHE_ENABLED = process.env.BOT_ANALYSIS_CACHE_ENABLED !== 'false';

function sha1Hex(input) {
    return crypto.createHash('sha1').update(String(input)).digest('hex');
}

function buildOpenPositionsSignature(openPositions) {
    if (!Array.isArray(openPositions) || openPositions.length === 0) return 'none';

    const normalized = openPositions
        .map((p) => ({
            id: Number.isFinite(Number(p?.id)) ? Number(p.id) : null,
            status: typeof p?.status === 'string' ? p.status : null,
            direction: typeof p?.direction === 'string' ? p.direction : null
        }))
        .filter((p) => p.id !== null)
        .sort((a, b) => a.id - b.id);

    return sha1Hex(JSON.stringify(normalized));
}

function buildTradingVariablesSignature(tradingVariablesForPair) {
    if (!tradingVariablesForPair || typeof tradingVariablesForPair !== 'object') return 'none';

    const pair = tradingVariablesForPair.pair || {};
    const group = tradingVariablesForPair.group || {};
    const fees = tradingVariablesForPair.fees || {};
    const oi = tradingVariablesForPair.openInterest || {};
    const src = tradingVariablesForPair.source || {};

    const payload = {
        refreshId: src.refreshId ?? null,
        lastRefreshed: src.lastRefreshed ?? null,
        spreadPercent: pair.spreadPercent ?? null,
        groupMaxLeverage: group.maxLeverage ?? null,
        feePositionSize: fees.positionSizeFeePercent ?? null,
        feeOracle: fees.oraclePositionSizeFeePercent ?? null,
        oiSkew: oi.skewPercent ?? null,
        oiLong: oi.long ?? null,
        oiShort: oi.short ?? null,
        minPositionUsd: fees.minPositionSizeUsd ?? null
    };

    return sha1Hex(JSON.stringify(payload));
}

function startOfLocalDayUnixSec() {
    const now = new Date();
    now.setHours(0, 0, 0, 0);
    return Math.floor(now.getTime() / 1000);
}

async function getTodayPnLFromDb() {
    const startOfDaySec = startOfLocalDayUnixSec();
    const result = await query(
        `SELECT COALESCE(SUM(pnl), 0) AS pnl
         FROM trades
         WHERE source = 'BOT'
           AND status = 'CLOSED'
           AND exit_time >= $1`,
        [startOfDaySec]
    );

    const pnlRaw = result.rows?.[0]?.pnl ?? 0;
    const pnl = Number.isFinite(Number(pnlRaw)) ? Number(pnlRaw) : 0;
    return pnl;
}

async function runTradeCloseReflectionWithAgentsSdk({ trade, decision, timeframeMin, model, promptVersion }) {
    const sdk = await loadAgentsSdk();
    const { z } = getZod();

    const ReflectionSchema = z.object({
        summary: z.string().min(1),
        tags: z.array(z.string()).max(12),
        lessons: z.array(z.string()).max(10)
    });

    const agent = new sdk.Agent({
        name: 'Trade Reflection Writer',
        instructions: `You are a trading performance reviewer.

Given a single closed trade and the model's decision context (if available), produce a compact, structured reflection for future retrieval.

Rules:
- Output STRICT JSON (no markdown, no prose outside JSON).
- Keep it short.
- Focus on actionable rules and anti-patterns.
- Do NOT propose increasing leverage/collateral beyond safety limits.

Return JSON with:
{ "summary": string, "tags": string[], "lessons": string[] }`,
        model: model || 'gpt-5.2',
        outputType: ReflectionSchema,
        modelSettings: {
            temperature: 0.2
        }
    });

    const runner = await getAgentsRunner();
    const input = JSON.stringify({ trade, decision, timeframeMin });
    const result = await runner.run(agent, input, {
        maxTurns: 1,
        traceMetadata: {
            workflow: 'trade_close_reflection',
            promptVersion: promptVersion || null,
            tradeId: trade?.id ?? null
        }
    });

    const usage = result?.rawResponses?.[result.rawResponses.length - 1]?.usage ?? null;
    return {
        output: result.finalOutput,
        usage
    };
}

function buildAnalysisKey({ pairIndex, timeframeMin, candleTime, openPositionsSig, tradingVarsSig, promptVersion }) {
    const keyPayload = {
        v: promptVersion,
        pairIndex,
        timeframeMin,
        candleTime,
        openPositionsSig,
        tradingVarsSig
    };
    return `analyze:${sha1Hex(JSON.stringify(keyPayload))}`;
}

function normalizeAgentDecisionAgainstOpenPositions({
    action,
    args,
    openPositions,
    allowMultiplePositionsPerPair
}) {
    const normalizedAction = typeof action === 'string' ? action : null;
    const normalizedArgs = (args && typeof args === 'object') ? args : {};

    const positions = Array.isArray(openPositions) ? openPositions : [];
    const allowMulti = allowMultiplePositionsPerPair === true;

    const idsAll = new Set();
    const idsOpen = new Set();
    const idsPending = new Set();
    for (const p of positions) {
        const idNum = Number.isFinite(Number(p?.id)) ? Number(p.id) : null;
        if (!Number.isFinite(idNum) || idNum <= 0) continue;
        idsAll.add(idNum);
        if (p?.status === 'OPEN') idsOpen.add(idNum);
        if (p?.status === 'PENDING') idsPending.add(idNum);
    }

    const result = {
        action: normalizedAction,
        args: normalizedArgs,
        guardrail: null
    };

    // If we already have an OPEN/PENDING position for this pair and multi-position is disabled,
    // do not let the agent open a new one.
    if (normalizedAction === 'execute_trade' && positions.length > 0 && !allowMulti) {
        result.guardrail = {
            reason: 'existing_position',
            message: `Existing position(s) already present for this pair (ids: ${Array.from(idsAll).join(', ') || 'unknown'}). Not opening a new trade.`,
            original: { action: normalizedAction, args: normalizedArgs }
        };
        result.action = 'hold_position';
        result.args = {
            confidence: 0.85,
            reasoning: result.guardrail.message
        };
        return result;
    }

    // Ensure close/cancel references a valid trade_id from the provided open positions.
    if (normalizedAction === 'close_position') {
        const tradeId = Number.isFinite(Number(normalizedArgs?.trade_id)) ? Number(normalizedArgs.trade_id) : null;
        if (!Number.isFinite(tradeId) || tradeId <= 0 || !idsAll.has(tradeId) || (!idsOpen.has(tradeId) && idsPending.has(tradeId))) {
            result.guardrail = {
                reason: 'invalid_close_target',
                message: `Invalid trade_id for close_position. Choose an OPEN trade_id from: ${Array.from(idsOpen).join(', ') || 'none'}.`,
                original: { action: normalizedAction, args: normalizedArgs }
            };
            result.action = 'hold_position';
            result.args = {
                confidence: 0.8,
                reasoning: result.guardrail.message
            };
        }
        return result;
    }

    if (normalizedAction === 'cancel_pending') {
        const tradeId = Number.isFinite(Number(normalizedArgs?.trade_id)) ? Number(normalizedArgs.trade_id) : null;
        if (!Number.isFinite(tradeId) || tradeId <= 0 || !idsPending.has(tradeId)) {
            result.guardrail = {
                reason: 'invalid_cancel_target',
                message: `Invalid trade_id for cancel_pending. Choose a PENDING trade_id from: ${Array.from(idsPending).join(', ') || 'none'}.`,
                original: { action: normalizedAction, args: normalizedArgs }
            };
            result.action = 'hold_position';
            result.args = {
                confidence: 0.8,
                reasoning: result.guardrail.message
            };
        }
        return result;
    }

    return result;
}

async function getRecentLessonsForPair(pairIndex, { timeframeMin = null, limit = 3 } = {}) {
    const safeLimit = Math.max(0, Math.min(Number(limit) || 0, 5));
    if (safeLimit === 0) return [];

    try {
        const rows = await query(
            `SELECT timestamp, summary, tags_json
             FROM bot_reflections
             WHERE pair_index = $1
               AND scope = 'trade_close'
             ORDER BY timestamp DESC
             LIMIT $2`,
            [pairIndex, safeLimit]
        );

        return (rows.rows || []).map((row) => {
            let tags = null;
            if (row.tags_json) {
                try {
                    tags = JSON.parse(row.tags_json);
                } catch {
                    tags = null;
                }
            }
            return {
                timestamp: row.timestamp,
                summary: typeof row.summary === 'string' ? row.summary : null,
                tags
            };
        });
    } catch {
        return [];
    }
}

async function generateTradeCloseReflection({ tradeRow, decisionRow, timeframeMin = null } = {}) {
    if (!process.env.OPENAI_API_KEY || process.env.OPENAI_API_KEY === 'your_openai_api_key_here') {
        return { success: false, error: 'OpenAI API key not configured. Please set OPENAI_API_KEY in server/.env' };
    }
    if (!tradeRow) return { success: false, error: 'Missing tradeRow' };

    const model = process.env.OPENAI_REFLECTION_MODEL || process.env.OPENAI_TRADING_MODEL || 'gpt-5.2';
    const promptVersion = `${PROMPT_VERSION}:reflection:v1`;

    const trade = {
        id: tradeRow.id,
        pair_index: tradeRow.pair_index,
        direction: tradeRow.direction,
        entry_price: tradeRow.entry_price,
        exit_price: tradeRow.exit_price,
        entry_time: tradeRow.entry_time,
        exit_time: tradeRow.exit_time,
        pnl: tradeRow.pnl,
        collateral: tradeRow.collateral,
        leverage: tradeRow.leverage,
        stop_loss_price: tradeRow.stop_loss_price,
        take_profit_price: tradeRow.take_profit_price,
        trigger_price: tradeRow.trigger_price,
        entry_cost_total_percent: tradeRow.entry_cost_total_percent
    };

    let decision = null;
    if (decisionRow) {
        decision = {
            id: decisionRow.id,
            timestamp: decisionRow.timestamp,
            action: decisionRow.action,
            confidence: decisionRow.confidence,
            reasoning: decisionRow.reasoning,
            analysis: decisionRow.analysis
        };
    }

    try {
        const result = await runTradeCloseReflectionWithAgentsSdk({
            trade,
            decision,
            timeframeMin,
            model,
            promptVersion
        });

        const output = result?.output;
        const summary = typeof output?.summary === 'string' ? output.summary.trim() : null;
        const tags = Array.isArray(output?.tags) ? output.tags.filter((t) => typeof t === 'string').slice(0, 12) : [];
        const lessons = Array.isArray(output?.lessons) ? output.lessons.filter((l) => typeof l === 'string').slice(0, 10) : [];

        if (!summary) return { success: false, error: 'Reflection missing summary' };

        return {
            success: true,
            model,
            promptVersion,
            output: { summary, tags, lessons },
            usage: normalizeUsage(result?.usage ?? null)
        };
    } catch (err) {
        return { success: false, error: err?.message || String(err) };
    }
}

/**
 * Analyze market and make a trading decision
 */
async function analyzeMarket(pairIndex, ohlcData, openPositions, tradingVariablesForPair = null, context = {}) {
    if (!process.env.OPENAI_API_KEY || process.env.OPENAI_API_KEY === 'your_openai_api_key_here') {
        return {
            success: false,
            error: 'OpenAI API key not configured. Please set OPENAI_API_KEY in server/.env'
        };
    }

    const rawCandidateScore = context?.candidateScore;
    const candidateScore = (() => {
        if (rawCandidateScore === null || rawCandidateScore === undefined) return null;
        if (typeof rawCandidateScore === 'number') {
            return Number.isFinite(rawCandidateScore) ? rawCandidateScore : null;
        }

        if (typeof rawCandidateScore === 'string' && rawCandidateScore.trim() === '') return null;

        const coerced = Number(rawCandidateScore);
        return Number.isFinite(coerced) ? coerced : null;
    })();

    // Check daily loss limit
    const todayPnL = await getTodayPnLFromDb();
    botState.todayPnL = todayPnL;
    if (todayPnL < -SAFETY_LIMITS.dailyLossLimit) {
        return {
            success: false,
            error: 'Daily loss limit reached. Bot paused.',
            action: 'PAUSED'
        };
    }

    // Calculate indicators
    const indicators = calculateIndicators(ohlcData);
    if (!indicators) {
        return { success: false, error: 'Insufficient data for analysis' };
    }

    const currentPrice = ohlcData[ohlcData.length - 1].close;
    const candleTime = ohlcData[ohlcData.length - 1].time;
    const timeframeMin = Number.isFinite(Number(context?.timeframeMin)) ? Number(context.timeframeMin) : null;
    const marketSummary = generateMarketSummary(indicators, currentPrice);
    const pairLabel = tradingVariablesForPair?.pair?.from && tradingVariablesForPair?.pair?.to
        ? `${tradingVariablesForPair.pair.from}/${tradingVariablesForPair.pair.to}`
        : `Pair ${pairIndex}`;

    const tradingVariablesSummary = (() => {
        if (!tradingVariablesForPair) return null;
        const pair = tradingVariablesForPair.pair || {};
        const group = tradingVariablesForPair.group || {};
        const fees = tradingVariablesForPair.fees || {};
        const oi = tradingVariablesForPair.openInterest || {};
        const funding = tradingVariablesForPair.funding || {};
        const borrowing = tradingVariablesForPair.borrowing || {};

        const spreadPercent = typeof pair.spreadPercent === 'number' ? `${pair.spreadPercent.toFixed(4)}%` : 'N/A';
        const positionFeePercent = typeof fees.positionSizeFeePercent === 'number' ? `${fees.positionSizeFeePercent.toFixed(4)}%` : 'N/A';
        const oracleFeePercent = typeof fees.oraclePositionSizeFeePercent === 'number' ? `${fees.oraclePositionSizeFeePercent.toFixed(4)}%` : 'N/A';
        const minPositionUsd = typeof fees.minPositionSizeUsd === 'number' ? `$${fees.minPositionSizeUsd.toFixed(0)}` : 'N/A';
        const minLev = typeof group.minLeverage === 'number' ? `${group.minLeverage.toFixed(1)}x` : 'N/A';
        const maxLev = typeof group.maxLeverage === 'number' ? `${group.maxLeverage.toFixed(1)}x` : 'N/A';

        const oiLong = typeof oi.long === 'number' ? oi.long.toFixed(0) : 'N/A';
        const oiShort = typeof oi.short === 'number' ? oi.short.toFixed(0) : 'N/A';
        const oiSkew = typeof oi.skewPercent === 'number' ? `${oi.skewPercent.toFixed(1)}%` : 'N/A';

        return `
TRADING VARIABLES (Gains):
- Pair: ${pair.from || 'N/A'}/${pair.to || 'N/A'} (index ${pair.index ?? pairIndex})
- Spread: ${spreadPercent}
- Fees: position ${positionFeePercent}, oracle ${oracleFeePercent}, min position ${minPositionUsd}
- Leverage limits (group ${group.name || 'N/A'}): min ${minLev}, max ${maxLev}
- Open interest (${oi.collateralSymbol || 'collateral'}): long ${oiLong}, short ${oiShort} (skew ${oiSkew})
- Funding enabled: ${funding.enabled === true ? 'true' : funding.enabled === false ? 'false' : 'N/A'} (last update ${funding.lastUpdateTs || 'N/A'})
- Borrowing rate per second (raw): ${borrowing.borrowingRatePerSecondP ?? 'N/A'} (last update ${borrowing.lastUpdateTs || 'N/A'})`.trim();
    })();

    const recentLessons = await getRecentLessonsForPair(pairIndex, { timeframeMin, limit: 3 });
    const lessonsBlock = recentLessons.length
        ? `\n\nLESSONS LEARNED (recent closed bot trades):\n${recentLessons
            .map((item) => item.summary)
            .filter(Boolean)
            .map((s) => `- ${s}`)
            .join('\n')}`
        : '';

    const openPositionsSummary = Array.isArray(openPositions) && openPositions.length > 0
        ? openPositions.map((p) => ({
            id: p.id,
            pair_index: p.pair_index,
            status: p.status ?? null,
            direction: p.direction ?? null,
            collateral: p.collateral ?? null,
            leverage: p.leverage ?? null,
            entry_price: p.entry_price ?? null,
            entry_time: p.entry_time ?? null,
            trigger_price: p.trigger_price ?? null,
            stop_loss_price: p.stop_loss_price ?? null,
            take_profit_price: p.take_profit_price ?? null
        }))
        : [];

    // Build context message
    const contextMessage = `
CURRENT MARKET STATE for ${pairLabel}:
- Current Price: $${currentPrice.toFixed(2)}
- Overall Bias: ${marketSummary.overallBias}

TECHNICAL INDICATORS:
- RSI (14): ${indicators.latest.rsi?.toFixed(2) || 'N/A'}
- MACD: ${indicators.latest.macd?.MACD?.toFixed(2) || 'N/A'} (Signal: ${indicators.latest.macd?.signal?.toFixed(2) || 'N/A'})
- Bollinger Bands: Upper ${indicators.latest.bollingerBands?.upper?.toFixed(2)}, Middle ${indicators.latest.bollingerBands?.middle?.toFixed(2)}, Lower ${indicators.latest.bollingerBands?.lower?.toFixed(2)}
- EMA9: ${indicators.latest.ema?.ema9?.toFixed(2)}, EMA21: ${indicators.latest.ema?.ema21?.toFixed(2)}
- ATR: ${indicators.latest.atr?.toFixed(2)}
- Stochastic K: ${indicators.latest.stochastic?.k?.toFixed(2)}

${tradingVariablesSummary ? `${tradingVariablesSummary}\n` : ''}

OPEN POSITIONS (summary):
${openPositionsSummary.length ? JSON.stringify(openPositionsSummary, null, 2) : 'None'}

TIMEFRAME (minutes): ${timeframeMin ?? 'N/A'}
${lessonsBlock}
`.trim();

    const openPositionsSig = buildOpenPositionsSignature(openPositions);
    const tradingVarsSig = buildTradingVariablesSignature(tradingVariablesForPair);
    const analysisKey = buildAnalysisKey({
        pairIndex,
        timeframeMin,
        candleTime,
        openPositionsSig,
        tradingVarsSig,
        promptVersion: PROMPT_VERSION
    });

    if (ANALYSIS_CACHE_ENABLED) {
        try {
            const cached = await query(
                `SELECT *
                 FROM bot_decisions
                 WHERE analysis_key = $1
                 ORDER BY timestamp DESC
                 LIMIT 1`,
                [analysisKey]
            );

            const cachedRow = cached.rows?.[0] ?? null;
            if (cachedRow?.analysis && cachedRow?.decision) {
                let cachedAnalysis = null;
                try {
                    cachedAnalysis = JSON.parse(cachedRow.analysis);
                } catch {
                    cachedAnalysis = null;
                }

                const toolCall = cachedAnalysis?.toolCall;
                const cachedArgs = toolCall?.args;
                const cachedAction = cachedRow.decision;

                if (toolCall?.name && cachedArgs) {
                    const cachedUsage = cachedRow.usage_json ? (() => {
                        try {
                            return JSON.parse(cachedRow.usage_json);
                        } catch {
                            return null;
                        }
                    })() : null;

                    const cachedCost = cachedRow.analysis_cost_json ? (() => {
                        try {
                            return JSON.parse(cachedRow.analysis_cost_json);
                        } catch {
                            return null;
                        }
                    })() : null;

                    try {
                        await query(
                            'INSERT INTO metrics_events(timestamp, name, details_json) VALUES ($1, $2, $3)',
                            [
                                Math.floor(Date.now() / 1000),
                                'llm_analysis_cache_hit',
                                JSON.stringify({ pairIndex, timeframeMin, candleTime, promptVersion: PROMPT_VERSION, sourceDecisionId: cachedRow.id })
                            ]
                        );
                    } catch {
                        // Metrics are best-effort.
                    }

                    return {
                        success: true,
                        action: cachedAction,
                        args: cachedArgs,
                        indicators: cachedAnalysis?.indicators ?? indicators.latest,
                        summary: cachedAnalysis?.marketSummary ?? marketSummary,
                        currentPrice: cachedAnalysis?.currentPrice ?? currentPrice,
                        decisionId: cachedRow.id,
                        usage: cachedUsage,
                        analysisCost: cachedCost,
                        candidateScore: cachedAnalysis?.candidateScore ?? candidateScore,
                        cacheMeta: {
                            status: 'hit',
                            cacheKey: analysisKey,
                            sourceDecisionId: cachedRow.id
                        }
                    };
                }
            }
        } catch {
            // Cache is best-effort.
        }
    }

    if (ANALYSIS_CACHE_ENABLED) {
        try {
            await query(
                'INSERT INTO metrics_events(timestamp, name, details_json) VALUES ($1, $2, $3)',
                [
                    Math.floor(Date.now() / 1000),
                    'llm_analysis_cache_miss',
                    JSON.stringify({ pairIndex, timeframeMin, candleTime, promptVersion: PROMPT_VERSION })
                ]
            );
        } catch {
            // Metrics are best-effort.
        }
    }

    try {
        const model = process.env.OPENAI_TRADING_MODEL || 'gpt-5.2';

        const marketContext = {
            pairLabel,
            pairIndex,
            timeframeMin,
            candleTime,
            currentPrice,
            candidateScore,
            marketSummary,
            indicators: indicators.latest,
            tradingVariables: tradingVariablesForPair,
            openPositions: openPositionsSummary,
            lessons: recentLessons,
            contextMessage
        };
        const decision = await runMarketDecisionWithAgentsSdk({
            pairLabel,
            marketContext,
            model,
            timeframeMin,
            pairIndex
        });

        const usage = decision.usage || null;
        let functionName = decision?.output?.action ?? null;
        let args = decision?.output?.args ?? null;
        const llmMeta = { api: 'agents_sdk', model };

        const analysisCost = buildAnalysisCost(usage);

        const allowMultiplePositionsPerPair = context?.allowMultiplePositionsPerPair === true;
        const normalized = normalizeAgentDecisionAgainstOpenPositions({
            action: functionName,
            args,
            openPositions: openPositionsSummary,
            allowMultiplePositionsPerPair
        });

        functionName = normalized.action;
        args = normalized.args;

        if (typeof functionName === 'string' && functionName) {
            const parsedConfidence = Number.isFinite(Number(args?.confidence))
                ? Math.max(0, Math.min(1, Number(args.confidence)))
                : null;

            // Store the decision in database
            const decisionAnalysis = {
                marketSummary,
                tradingVariables: tradingVariablesForPair,
                indicators: indicators.latest,
                currentPrice,
                toolCall: {
                    name: functionName,
                    args
                },
                guardrail: normalized.guardrail,
                candidateScore,
                llm: {
                    api: llmMeta.api,
                    model: llmMeta.model
                }
            };

            const nowSec = Math.floor(Date.now() / 1000);
            const normalizedUsage = normalizeUsage(usage);
            const analysisCostJson = analysisCost ? JSON.stringify(analysisCost) : null;

            const decisionResult = await query(
                `INSERT INTO bot_decisions (
                    pair_index, timestamp, analysis, decision, confidence, action, reasoning,
                    analysis_key, timeframe_min, candle_time,
                    prompt_version, model, usage_json, analysis_cost_json
                 ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10,
                    $11, $12, $13, $14
                 )`,
                [
                    pairIndex,
                    nowSec,
                    JSON.stringify(decisionAnalysis),
                    functionName,
                    parsedConfidence ?? 0.75,
                    functionName,
                    args.reasoning,
                    analysisKey,
                    timeframeMin,
                    candleTime,
                    PROMPT_VERSION,
                    model,
                    normalizedUsage ? JSON.stringify(normalizedUsage) : null,
                    analysisCostJson
                ]
            );

            const decisionId = decisionResult.lastID;

            botState.lastAnalysis = {
                timestamp: Date.now(),
                indicators: indicators.latest,
                summary: marketSummary,
                decision: functionName,
                args,
                confidence: parsedConfidence,
                candidateScore
            };

            return {
                success: true,
                action: functionName,
                args,
                indicators: indicators.latest,
                summary: marketSummary,
                currentPrice,
                decisionId,
                usage: normalizeUsage(usage),
                analysisCost,
                candidateScore,
                cacheMeta: {
                    status: 'miss',
                    cacheKey: analysisKey,
                    sourceDecisionId: decisionId
                }
            };
        }

        return { success: false, error: 'No tool call in response' };

    } catch (error) {
        console.error('OpenAI API error:', error);
        return { success: false, error: error.message };
    }
}

/**
 * Select the best market from top-ranked candidates.
 */
async function selectBestMarket(candidates, openPositions = []) {
    if (!process.env.OPENAI_API_KEY || process.env.OPENAI_API_KEY === 'your_openai_api_key_here') {
        return {
            success: false,
            error: 'OpenAI API key not configured. Please set OPENAI_API_KEY in server/.env'
        };
    }

    if (!Array.isArray(candidates) || candidates.length === 0) {
        return { success: false, error: 'No candidates available' };
    }

    const openPositionsSummary = Array.isArray(openPositions) && openPositions.length > 0
        ? openPositions.map(p => ({
            id: p.id,
            pair_index: p.pair_index,
            direction: p.direction,
            collateral: p.collateral,
            leverage: p.leverage,
            entry_price: p.entry_price
        }))
        : [];

    const userMessage = {
        candidates,
        openPositions: openPositionsSummary
    };

    try {
        const model = process.env.OPENAI_TRADING_MODEL || 'gpt-5.2';

        const selection = await runUniverseSelectionWithAgentsSdk({
            candidates,
            openPositionsSummary,
            model
        });

        const output = selection.output;
        const usage = selection.usage || null;
        const analysisCost = buildAnalysisCost(usage);

        const llm = { api: 'agents_sdk', model };

        const action = output?.action;
        const args = output?.args;
        if (action === 'select_market' || action === 'skip_trade') {
            return {
                success: true,
                action,
                args,
                usage: normalizeUsage(usage),
                analysisCost,
                llm
            };
        }

        return { success: false, error: 'Invalid selection output from agent' };

    } catch (error) {
        console.error('OpenAI API error:', error);
        return { success: false, error: error.message };
    }
}

/**
 * Execute a trade based on AI decision
 */
async function executeBotTrade(pairIndex, direction, collateral, leverage, currentPrice, stopLossPrice = null, takeProfitPrice = null, triggerPrice = null, entryCostSnapshot = null) {
    const nowSec = Math.floor(Date.now() / 1000);

    const validated = validateAndNormalizeBotTradeParams({
        direction,
        collateral,
        leverage,
        currentPrice,
        stopLossPrice,
        takeProfitPrice,
        triggerPrice
    });

    if (!validated.ok) {
        return { success: false, error: validated.error };
    }

    const hasTrigger = validated.triggerPrice !== null;
    const safeCollateral = validated.collateral;
    const safeLeverage = validated.leverage;

    // For trigger orders, store the reference price at creation time in entry_price.
    // When triggered, the server will update entry_price to the actual triggered price.
    const entryPrice = validated.currentPrice;

    const spread = parseOptionalFloat(entryCostSnapshot?.spread_percent);
    const fee = parseOptionalFloat(entryCostSnapshot?.fee_position_size_percent);
    const oracleFee = parseOptionalFloat(entryCostSnapshot?.fee_oracle_position_size_percent);
    const total = parseOptionalFloat(entryCostSnapshot?.total_percent);
    const source = typeof entryCostSnapshot?.source === 'string' ? entryCostSnapshot.source : null;
    const updatedAt = Number.isFinite(Number(entryCostSnapshot?.updated_at)) ? Number(entryCostSnapshot.updated_at) : null;
    const snapshotJson = entryCostSnapshot ? JSON.stringify(entryCostSnapshot) : null;

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
            pairIndex,
            entryPrice,
            nowSec,
            safeCollateral,
            safeLeverage,
            direction,
            'BOT',
            validated.stopLossPrice,
            validated.takeProfitPrice,
            hasTrigger ? validated.triggerPrice : null,
            hasTrigger ? 'PENDING' : 'OPEN',
            spread,
            fee,
            oracleFee,
            total,
            source,
            updatedAt,
            snapshotJson
        ]
    );

    botState.tradesExecuted++;

    return {
        success: true,
        tradeId: result.lastID,
        direction,
        collateral: safeCollateral,
        leverage: safeLeverage,
        entryPrice: hasTrigger ? null : entryPrice,
        status: hasTrigger ? 'PENDING' : 'OPEN',
        triggerPrice: hasTrigger ? validated.triggerPrice : null,
        stopLossPrice: validated.stopLossPrice,
        takeProfitPrice: validated.takeProfitPrice
    };
}

/**
 * Close a position
 */
async function closeBotPosition(tradeId, currentPrice) {
    const tradeResult = await query('SELECT * FROM trades WHERE id = $1', [tradeId]);
    if (tradeResult.rows.length === 0) {
        return { success: false, error: 'Trade not found' };
    }

    const trade = tradeResult.rows[0];
    if (trade.status && trade.status !== 'OPEN') {
        return { success: false, error: 'Trade is not open' };
    }
    const entryPrice = parseFloat(trade.entry_price);
    const collateral = parseFloat(trade.collateral);
    const leverage = parseFloat(trade.leverage);
    const direction = trade.direction;

    let pnl = 0;
    if (direction === 'LONG') {
        pnl = ((currentPrice - entryPrice) / entryPrice) * (collateral * leverage);
    } else {
        pnl = ((entryPrice - currentPrice) / entryPrice) * (collateral * leverage);
    }

    await query(
        'UPDATE trades SET exit_price = $1, exit_time = $2, status = $3, pnl = $4 WHERE id = $5',
        [currentPrice, Math.floor(Date.now() / 1000), 'CLOSED', pnl, tradeId]
    );

    // Update daily PnL
    botState.todayPnL += pnl;

    return {
        success: true,
        tradeId,
        pnl,
        exitPrice: currentPrice
    };
}

/**
 * Get bot status
 */
function getBotStatus() {
    return {
        ...botState,
        safetyLimits: SAFETY_LIMITS,
        apiConfigured: !!process.env.OPENAI_API_KEY && process.env.OPENAI_API_KEY !== 'your_openai_api_key_here'
    };
}

/**
 * Toggle bot active state
 */
function toggleBot(active) {
    botState.isActive = active;
    return botState;
}

/**
 * Reset daily stats (call at midnight)
 */
function resetDailyStats() {
    botState.todayPnL = 0;
    botState.tradesExecuted = 0;
}

module.exports = {
    analyzeMarket,
    selectBestMarket,
    executeBotTrade,
    closeBotPosition,
    generateTradeCloseReflection,
    getBotStatus,
    toggleBot,
    resetDailyStats,
    SAFETY_LIMITS
};
