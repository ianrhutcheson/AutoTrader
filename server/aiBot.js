/**
 * AI Trading Bot Module
 * Uses OpenAI GPT-5.2 via the OpenAI Agents SDK for structured decisions
 */

require('dotenv').config();
const crypto = require('crypto');
const { query } = require('./db');
const { calculateIndicators, generateMarketSummary } = require('./indicators');
const botContext = require('./botContext');
const { fetchTradingVariablesCached, buildTradingVariablesForPair } = require('./gainsTradingVariables');
const liveTrading = require('./liveTrading');

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

function buildTradingAgentInstructions(pairLabel = 'BTC/USD', executionProvider = 'paper') {
    const provider = typeof executionProvider === 'string' ? executionProvider.trim().toLowerCase() : '';
    const liveMode = provider === 'live' || provider === 'symphony';

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

METHODOLOGY:
1. Use the multi-timeframe snapshots and consensus to determine regime/trend alignment.
2. Use the opportunity scanner hint (suggested side + score + reasons) as a starting point.
3. Prefer trades with clear confluence (trend + momentum + structure) OR clear mean-reversion extremes.
4. If a setup is good but needs confirmation or a better entry, prefer a trigger order (trigger_price) over doing nothing.

TRIGGER ORDERS (paper mode only):
- Trigger orders support BOTH stop and limit behavior:
  - LONG: trigger_price above reference is buy-stop; below reference is buy-limit.
  - SHORT: trigger_price below reference is sell-stop; above reference is sell-limit.
- If you use trigger_price, stop_loss_price and take_profit_price MUST be consistent with the trigger entry (not the current price).
- Set trigger_price to null for market entries.

${liveMode ? `LIVE EXECUTION MODE (Symphony):
1. Do NOT use trigger orders. You MUST set trigger_price to null.
2. Collateral is still expressed in USD in your output; the server will convert it into a % weight of the operator's live trading pool.
3. Stop-loss and take-profit will be passed through as orderOptions where supported.
` : ''}

POSITION MANAGEMENT RULES:
1. If there is an existing OPEN position for this pair, prefer managing it (hold or close) rather than opening a new one.
2. If there is a PENDING (trigger) order for this pair that no longer makes sense, cancel it.
3. When closing or canceling, you MUST reference the correct trade_id from the provided openPositions list.

Output ONLY a JSON object matching the required schema.`;
}

async function runUniverseSelectionWithAgentsSdk({ candidates, openPositionsSummary, model }) {
    const sdk = await loadAgentsSdk();
    const { z } = getZod();

    // NOTE: The Agents SDK currently auto-converts only *Zod objects* into the strict JSON schema
    // format expected by the Responses API. Wrap discriminated unions inside a top-level object.
    const SelectionPayloadSchema = z.discriminatedUnion('action', [
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

    const SelectionSchema = z.object({
        selection: SelectionPayloadSchema
    });

    const agent = new sdk.Agent({
        name: 'Universe Selector',
        instructions: buildUniverseAgentInstructions(),
        model: model || 'gpt-5.2',
        outputType: SelectionSchema,
        modelSettings: {
            temperature: 0.2,
            toolChoice: 'none'
        }
    });

    const runner = await getAgentsRunner();
    const input = `UNIVERSE CONTEXT (JSON):
${JSON.stringify({ candidates, openPositions: openPositionsSummary }, null, 2)}

Select exactly one market to analyze next, or skip if none are good enough.

Return ONLY the required JSON.`;
    const result = await runner.run(agent, input, {
        maxTurns: 2,
        traceMetadata: {
            workflow: 'select_best_market',
            candidates: Array.isArray(candidates) ? candidates.length : 0
        }
    });

    const usage = result?.rawResponses?.[result.rawResponses.length - 1]?.usage ?? null;
    const output = result?.finalOutput?.selection ?? result?.finalOutput ?? null;
    return {
        output,
        usage
    };
}

async function runMarketDecisionWithAgentsSdk({ pairLabel, marketContext, model, timeframeMin, pairIndex, executionProvider = 'paper' }) {
    const sdk = await loadAgentsSdk();
    const { z } = getZod();

    const ExecuteTradeArgsSchema = z.object({
        direction: z.enum(['LONG', 'SHORT']),
        collateral: z.number().positive(),
        leverage: z.number().positive(),
        stop_loss_price: z.number().positive(),
        take_profit_price: z.number().positive(),
        // Responses API strict schemas don't support optional fields; use null when absent.
        trigger_price: z.number().positive().nullable(),
        confidence: z.number().min(0).max(1),
        reasoning: z.string().min(1),
        invalidation: z.string().min(1).nullable()
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

    const OutputSchema = z.object({
        decision: DecisionSchema
    });

    const agent = new sdk.Agent({
        name: 'Trader',
        instructions: buildTradingAgentInstructions(pairLabel, executionProvider),
        model: model || 'gpt-5.2',
        outputType: OutputSchema,
        modelSettings: {
            temperature: 0.2,
            toolChoice: 'none'
        }
    });

    const runner = await getAgentsRunner();
    const contextText = typeof marketContext?.contextMessage === 'string' && marketContext.contextMessage.trim()
        ? marketContext.contextMessage.trim()
        : JSON.stringify(marketContext);
    const input = `${contextText}\n\nReturn ONLY the required JSON.`;
    const result = await runner.run(agent, input, {
        maxTurns: 2,
        traceMetadata: {
            workflow: 'analyze_market',
            pairIndex,
            timeframeMin
        }
    });

    const usage = result?.rawResponses?.[result.rawResponses.length - 1]?.usage ?? null;
    const output = result?.finalOutput?.decision ?? result?.finalOutput ?? null;
    return {
        output,
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

    const referenceEntryPrice = normalizedTriggerPrice !== null ? normalizedTriggerPrice : normalizedCurrentPrice;

    if (normalizedStopLossPrice !== null) {
        if (direction === 'LONG' && normalizedStopLossPrice >= referenceEntryPrice) {
            return { ok: false, error: 'For LONG, stop_loss_price must be below entry reference price' };
        }
        if (direction === 'SHORT' && normalizedStopLossPrice <= referenceEntryPrice) {
            return { ok: false, error: 'For SHORT, stop_loss_price must be above entry reference price' };
        }
    }

    if (normalizedTakeProfitPrice !== null) {
        if (direction === 'LONG' && normalizedTakeProfitPrice <= referenceEntryPrice) {
            return { ok: false, error: 'For LONG, take_profit_price must be above entry reference price' };
        }
        if (direction === 'SHORT' && normalizedTakeProfitPrice >= referenceEntryPrice) {
            return { ok: false, error: 'For SHORT, take_profit_price must be below entry reference price' };
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
            ? (referenceEntryPrice - normalizedStopLossPrice)
            : (normalizedStopLossPrice - referenceEntryPrice);
        const reward = direction === 'LONG'
            ? (normalizedTakeProfitPrice - referenceEntryPrice)
            : (referenceEntryPrice - normalizedTakeProfitPrice);

        if (!(risk > 0) || !(reward > 0)) {
            return { ok: false, error: 'Invalid risk/reward distances (check SL/TP vs entry reference price)' };
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
    isActive: true,
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

    const payload = {
        spreadPercent: pair.spreadPercent ?? null,
        groupMaxLeverage: group.maxLeverage ?? null,
        feePositionSize: fees.positionSizeFeePercent ?? null,
        feeOracle: fees.oraclePositionSizeFeePercent ?? null,
        minPositionUsd: fees.minPositionSizeUsd ?? null,
        collateralSymbol: oi.collateralSymbol ?? null,
        collateralPriceUsd: oi.collateralPriceUsd ?? null,
        oiSkew: oi.skewPercent ?? null,
        oiLong: oi.long ?? null,
        oiShort: oi.short ?? null
    };

    return sha1Hex(JSON.stringify(payload));
}

function startOfLocalDayUnixSec() {
    const now = new Date();
    now.setHours(0, 0, 0, 0);
    return Math.floor(now.getTime() / 1000);
}

function normalizeExecutionProvider(value) {
    const raw = typeof value === 'string' ? value.trim().toLowerCase() : '';
    if (raw === 'live' || raw === 'symphony') return 'live';
    return 'paper';
}

async function getLiveDailyLossLimitFromDb() {
    try {
        const result = await query('SELECT daily_loss_limit_usd FROM live_trading_settings WHERE id = 1 LIMIT 1');
        const raw = result.rows?.[0]?.daily_loss_limit_usd;
        const limit = Number.isFinite(Number(raw)) ? Number(raw) : null;
        return limit;
    } catch {
        return null;
    }
}

async function getTodayPnLFromDb(executionProvider = 'paper') {
    const startOfDaySec = startOfLocalDayUnixSec();
    const provider = normalizeExecutionProvider(executionProvider);

    const result = provider === 'live'
        ? await query(
            `SELECT COALESCE(SUM(COALESCE(last_pnl_usd, 0)), 0) AS pnl
             FROM live_trades
             WHERE status = 'CLOSED'
               AND closed_at >= $1`,
            [startOfDaySec]
        )
        : await query(
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

function buildAnalysisKey({ pairIndex, timeframeMin, candleTime, openPositionsSig, tradingVarsSig, promptVersion, executionProvider }) {
    const keyPayload = {
        v: promptVersion,
        pairIndex,
        timeframeMin,
        candleTime,
        openPositionsSig,
        tradingVarsSig,
        executionProvider: normalizeExecutionProvider(executionProvider)
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

function preflightValidateAgentDecision({
    action,
    args,
    currentPrice,
    executionProvider
}) {
    if (action !== 'execute_trade') {
        return { action, args, guardrail: null };
    }

    const original = { action, args };
    const normalizedArgs = (args && typeof args === 'object') ? args : {};

    const direction = normalizedArgs.direction;
    const collateral = normalizedArgs.collateral;
    const leverage = normalizedArgs.leverage;
    const stopLossPrice = normalizedArgs.stop_loss_price;
    const takeProfitPrice = normalizedArgs.take_profit_price;
    const triggerPrice = normalizedArgs.trigger_price;

    // Enforce provider rules: live execution disallows trigger orders.
    if (normalizeExecutionProvider(executionProvider) === 'live' && triggerPrice !== null && triggerPrice !== undefined) {
        const message = 'Trigger orders are not supported in live mode. Converting to hold.';
        return {
            action: 'hold_position',
            args: { confidence: 0.8, reasoning: message },
            guardrail: { reason: 'live_trigger_disallowed', message, original }
        };
    }

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
        const message = `Invalid trade params from model: ${validated.error}. Converting to hold.`;
        return {
            action: 'hold_position',
            args: { confidence: 0.8, reasoning: message },
            guardrail: { reason: 'invalid_trade_params', message, original }
        };
    }

    // Return a normalized args payload (clamped collateral/leverage + parsed prices).
    return {
        action: 'execute_trade',
        args: {
            ...normalizedArgs,
            direction: validated.direction,
            collateral: validated.collateral,
            leverage: validated.leverage,
            stop_loss_price: validated.stopLossPrice,
            take_profit_price: validated.takeProfitPrice,
            trigger_price: validated.triggerPrice
        },
        guardrail: null
    };
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

    const candidateSide = typeof context?.candidateSide === 'string' && (context.candidateSide === 'LONG' || context.candidateSide === 'SHORT')
        ? context.candidateSide
        : null;
    const candidateBestTimeframeMin = Number.isFinite(Number(context?.candidateBestTimeframeMin))
        ? Number(context.candidateBestTimeframeMin)
        : null;
    const candidateReasons = Array.isArray(context?.candidateReasons)
        ? context.candidateReasons.filter((r) => typeof r === 'string' && r.trim()).slice(0, 8)
        : [];
    const candidateTimeframes = Array.isArray(context?.candidateTimeframes)
        ? context.candidateTimeframes
            .filter((row) => row && typeof row === 'object')
            .map((row) => {
                const record = row;
                const tf = Number.isFinite(Number(record.timeframe_min)) ? Number(record.timeframe_min) : null;
                const side = typeof record.side === 'string' ? record.side : null;
                const score = Number.isFinite(Number(record.score)) ? Number(record.score) : null;
                if (!tf || tf <= 0 || (side !== 'LONG' && side !== 'SHORT') || score === null) return null;
                return { timeframe_min: tf, side, score };
            })
            .filter(Boolean)
            .slice(0, 12)
        : [];

    const executionProvider = normalizeExecutionProvider(context?.executionProvider);

    // Check daily loss limit (provider-aware)
    const todayPnL = await getTodayPnLFromDb(executionProvider);
    botState.todayPnL = todayPnL;
    const dailyLossLimit = executionProvider === 'live'
        ? (await getLiveDailyLossLimitFromDb()) ?? SAFETY_LIMITS.dailyLossLimit
        : SAFETY_LIMITS.dailyLossLimit;

    if (todayPnL < -dailyLossLimit) {
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

    let costsAndLiquidity = null;
    try {
        costsAndLiquidity = await botContext.getCostsAndLiquidity(pairIndex);
    } catch {
        costsAndLiquidity = null;
    }

    let multiTimeframe = null;
    try {
        const supported = await botContext.getSupportedTimeframes({ pairIndex });
        const supportedTfs = Array.isArray(supported?.timeframesMin)
            ? supported.timeframesMin.filter((tf) => typeof tf === 'number' && Number.isFinite(tf) && tf > 0)
            : [];
        const preferred = [
            timeframeMin,
            1,
            5,
            15,
            60,
            240,
            1440
        ].filter((tf) => typeof tf === 'number' && Number.isFinite(tf) && tf > 0);
        const selected = [];
        for (const tf of preferred) {
            if (supportedTfs.length > 0 && !supportedTfs.includes(tf)) continue;
            if (!selected.includes(tf)) selected.push(tf);
        }
        const fallback = supportedTfs.slice(0, 8).filter((tf) => !selected.includes(tf));
        multiTimeframe = await botContext.getMultiTimeframeSnapshots(pairIndex, [...selected, ...fallback].slice(0, 8));
    } catch {
        multiTimeframe = null;
    }

    const opportunityBlock = (() => {
        const parts = [];
        if (candidateScore !== null || candidateSide || candidateBestTimeframeMin || candidateReasons.length || candidateTimeframes.length) {
            parts.push('OPPORTUNITY SCAN (from stored market_state):');
            if (candidateSide) parts.push(`- Suggested side: ${candidateSide}`);
            if (candidateScore !== null) parts.push(`- Score: ${candidateScore.toFixed(1)} / 100`);
            if (candidateBestTimeframeMin !== null) parts.push(`- Best timeframe: ${candidateBestTimeframeMin}m`);
            if (candidateReasons.length) parts.push(`- Reasons: ${candidateReasons.join(' · ')}`);
            if (candidateTimeframes.length) {
                const tfSummary = candidateTimeframes
                    .map((t) => `${t.timeframe_min}m ${t.side} ${t.score.toFixed(1)}`)
                    .join(' | ');
                parts.push(`- TF breakdown: ${tfSummary}`);
            }
        }
        return parts.length ? `\n${parts.join('\n')}\n` : '';
    })();

    const costsBlock = (() => {
        if (!costsAndLiquidity) return '';
        const costs = costsAndLiquidity.costs || {};
        const liq = costsAndLiquidity.liquidity || {};
        const c = costsAndLiquidity.constraints || {};
        const age = typeof costsAndLiquidity.ageSec === 'number' ? `${costsAndLiquidity.ageSec}s` : 'N/A';
        const total = typeof costs.total_percent === 'number' ? `${costs.total_percent.toFixed(4)}%` : 'N/A';
        const oiTotal = typeof liq.oi_total === 'number' ? liq.oi_total.toFixed(0) : 'N/A';
        const skew = typeof liq.oi_skew_percent === 'number' ? `${liq.oi_skew_percent.toFixed(1)}%` : 'N/A';
        const minPos = typeof c.min_position_size_usd === 'number' ? `$${c.min_position_size_usd.toFixed(0)}` : 'N/A';
        const maxLev = typeof c.group_max_leverage === 'number' ? `${c.group_max_leverage.toFixed(0)}x` : 'N/A';
        return `\nCOSTS + LIQUIDITY (trading variables; age ${age}):\n- Total cost% (spread+fees): ${total}\n- OI total: ${oiTotal} (skew ${skew})\n- Min position: ${minPos}\n- Group max leverage: ${maxLev}\n`;
    })();

    const fmtFixed = (value, decimals = 2) => (typeof value === 'number' && Number.isFinite(value) ? value.toFixed(decimals) : 'N/A');

    const fmtPrice = (value) => {
        if (typeof value !== 'number' || !Number.isFinite(value)) return 'N/A';
        const abs = Math.abs(value);
        if (abs >= 100) return value.toFixed(2);
        if (abs >= 1) return value.toFixed(4);
        if (abs >= 0.01) return value.toFixed(6);
        return value.toFixed(8);
    };

    const fmtSmall = (value) => {
        if (typeof value !== 'number' || !Number.isFinite(value)) return 'N/A';
        const abs = Math.abs(value);
        if (abs >= 1) return value.toFixed(4);
        if (abs >= 0.01) return value.toFixed(6);
        if (abs >= 0.0001) return value.toFixed(8);
        if (abs === 0) return '0';
        return value.toExponential(2);
    };

    const fmtPct = (value, decimals = 2) => (typeof value === 'number' && Number.isFinite(value) ? `${value.toFixed(decimals)}%` : 'N/A');

    const multiTimeframeBlock = (() => {
        if (!multiTimeframe || !Array.isArray(multiTimeframe.snapshots) || multiTimeframe.snapshots.length === 0) return '';

        const fmtInt = (value) => (typeof value === 'number' && Number.isFinite(value) ? String(Math.round(value)) : 'N/A');

        const lines = [];
        lines.push('\nMULTI-TIMEFRAME SNAPSHOTS (market_state):');
        for (const snap of multiTimeframe.snapshots.slice(0, 8)) {
            const tf = snap?.timeframeMin;
            const bias = typeof snap?.overallBias === 'string' ? snap.overallBias : 'N/A';
            const price = snap?.price;
            const rsi = snap?.indicators?.rsi;
            const macdHist = snap?.indicators?.macd_histogram;
            const ema9 = snap?.indicators?.ema9;
            const ema21 = snap?.indicators?.ema21;
            const stochK = snap?.indicators?.stoch_k;
            const bbUpper = snap?.indicators?.bb_upper;
            const bbLower = snap?.indicators?.bb_lower;
            const bbPos = (typeof price === 'number' && typeof bbUpper === 'number' && typeof bbLower === 'number' && bbUpper > bbLower)
                ? (price - bbLower) / (bbUpper - bbLower)
                : null;
            const ageSec = snap?.ageSec;
            const stale = snap?.stale === true ? ' stale' : '';
            lines.push(`- ${tf}m: bias=${bias}, rsi=${fmtFixed(rsi)}, macdHist=${fmtSmall(macdHist)}, ema9/21=${fmtPrice(ema9)}/${fmtPrice(ema21)}, bbPos=${fmtFixed(bbPos)}, stochK=${fmtFixed(stochK)}, age=${fmtInt(ageSec)}s${stale}`);
        }

        const consensus = multiTimeframe.consensus;
        if (consensus && typeof consensus === 'object') {
            const regime = typeof consensus.regime === 'string' ? consensus.regime : 'N/A';
            const trend = typeof consensus.trend === 'string' ? consensus.trend : 'N/A';
            const chop = consensus.chop === true ? 'true' : consensus.chop === false ? 'false' : 'N/A';
            const alignmentScore = fmtFixed(consensus.alignmentScore);
            lines.push(`Consensus: regime=${regime}, trend=${trend}, chop=${chop}, alignmentScore=${alignmentScore}`);
        }

        if (Array.isArray(multiTimeframe.staleTimeframesMin) && multiTimeframe.staleTimeframesMin.length) {
            lines.push(`Stale timeframes excluded: ${multiTimeframe.staleTimeframesMin.join(', ')}`);
        }

        return `${lines.join('\n')}\n`;
    })();

    const macd = indicators.latest.macd || {};
    const macdHistPct = (typeof macd.histogram === 'number' && Number.isFinite(macd.histogram) && Number.isFinite(currentPrice) && currentPrice > 0)
        ? (macd.histogram / currentPrice) * 100
        : null;
    const atrPct = (typeof indicators.latest.atr === 'number' && Number.isFinite(indicators.latest.atr) && Number.isFinite(currentPrice) && currentPrice > 0)
        ? (indicators.latest.atr / currentPrice) * 100
        : null;

    // Build context message
    const contextMessage = `
CURRENT MARKET STATE for ${pairLabel}:
- Current Price: $${fmtPrice(currentPrice)}
- Overall Bias: ${marketSummary.overallBias}

${opportunityBlock}${multiTimeframeBlock}${costsBlock}

TECHNICAL INDICATORS:
- RSI (14): ${fmtFixed(indicators.latest.rsi)}
- MACD: ${fmtSmall(macd.MACD)} (Signal: ${fmtSmall(macd.signal)}, Hist: ${fmtSmall(macd.histogram)}${macdHistPct !== null ? `, ~${fmtPct(macdHistPct, 4)} of price` : ''})
- Bollinger Bands: Upper ${fmtPrice(indicators.latest.bollingerBands?.upper)}, Middle ${fmtPrice(indicators.latest.bollingerBands?.middle)}, Lower ${fmtPrice(indicators.latest.bollingerBands?.lower)}
- EMA9: ${fmtPrice(indicators.latest.ema?.ema9)}, EMA21: ${fmtPrice(indicators.latest.ema?.ema21)}
- ATR: ${fmtSmall(indicators.latest.atr)}${atrPct !== null ? ` (~${fmtPct(atrPct, 3)} of price)` : ''}
- Stochastic K: ${fmtFixed(indicators.latest.stochastic?.k)}

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
        promptVersion: PROMPT_VERSION,
        executionProvider
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
            pairIndex,
            executionProvider
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

        // Additional preflight validation: catch malformed trade params early and prevent cache re-use surprises.
        const preflight = preflightValidateAgentDecision({
            action: normalized.action,
            args: normalized.args,
            currentPrice,
            executionProvider
        });

        functionName = preflight.action;
        args = preflight.args;

        const mergedGuardrail = normalized.guardrail && preflight.guardrail
            ? { chain: [normalized.guardrail, preflight.guardrail] }
            : (normalized.guardrail || preflight.guardrail);

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
                guardrail: mergedGuardrail,
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

function truncateString(value, max = 400) {
    if (typeof value !== 'string') return value;
    if (value.length <= max) return value;
    return `${value.slice(0, max)}…`;
}

function summarizeForTrace(value, depth = 2) {
    if (depth <= 0) return '[truncated]';
    if (value === null || value === undefined) return value;
    if (typeof value === 'string') return truncateString(value, 400);
    if (typeof value === 'number' || typeof value === 'boolean') return value;
    if (Array.isArray(value)) {
        const sample = value.slice(0, 5).map((item) => summarizeForTrace(item, depth - 1));
        return { count: value.length, sample };
    }
    if (typeof value === 'object') {
        const entries = Object.entries(value).slice(0, 20);
        const out = {};
        for (const [key, val] of entries) {
            out[key] = summarizeForTrace(val, depth - 1);
        }
        const extraKeys = Object.keys(value).length - entries.length;
        if (extraKeys > 0) out._truncated_keys = extraKeys;
        return out;
    }
    return String(value);
}

function summarizeExposureFromPositions(positions = []) {
    const perPair = new Map();
    let totalNotional = 0;

    for (const t of positions) {
        const pairIndex = Number.isFinite(Number(t?.pair_index)) ? Number(t.pair_index) : null;
        const collateral = Number.isFinite(Number(t?.collateral)) ? Number(t.collateral) : 0;
        const leverage = Number.isFinite(Number(t?.leverage)) ? Number(t.leverage) : 0;
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

async function getOpenPositionsForProvider({ executionProvider = 'paper', pairIndex = null } = {}) {
    if (executionProvider === 'live') {
        return liveTrading.getOpenLivePositionsForBot({ pairIndex });
    }

    const params = [];
    let where = "WHERE status IN ('OPEN', 'PENDING')";
    if (Number.isFinite(Number(pairIndex))) {
        where += ' AND pair_index = $1';
        params.push(Number(pairIndex));
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

async function getOpenExposureForProvider(executionProvider = 'paper') {
    const positions = await getOpenPositionsForProvider({ executionProvider });
    const exposure = summarizeExposureFromPositions(positions);
    const openPositions = positions.map((p) => ({
        id: p.id,
        pair_index: p.pair_index,
        status: p.status ?? null,
        direction: p.direction ?? null,
        collateral: p.collateral ?? null,
        leverage: p.leverage ?? null,
        entry_price: p.entry_price ?? null,
        trigger_price: p.trigger_price ?? null,
        stop_loss_price: p.stop_loss_price ?? null,
        take_profit_price: p.take_profit_price ?? null,
        entry_time: p.entry_time ?? null
    }));

    return { openPositions, exposure };
}

function buildAgenticTraderInstructions() {
    return `You are an autonomous perps trading decision agent.

Use tools for all market/position context. Do not guess.

Workflow:
1) get_ranked_candidates for the timeframe.
2) Choose a candidate or an open position to manage.
3) get_pair_context + get_similar_decisions + get_open_exposure for that pair.
4) Use other tools only if needed.

Rules:
- Only trade with high confidence (>= 0.70).
- Avoid high cost / low OI setups.
- If data is stale, hold.
- Always include stop loss + take profit for execute_trade.
- If execution_provider is live, trigger_price must be null.

Return ONLY the required JSON.`;
}

function buildRiskAgentInstructions() {
    return `You are the risk manager for a perps trading agent.

Review the proposed decision using the provided context.
Check exposure, costs/liquidity, staleness/regime alignment, and safety limits.

Return ONLY the required JSON.
Always include "adjustments" and "confidence_override" (use nulls when no change).`;
}

async function runAgentDecision({
    runId,
    timeframeMin = 15,
    opportunitiesLimit = 10,
    executionProvider = 'paper',
    minOiTotal = 1,
    maxCostPercent = 0.25,
    allowMultiplePositionsPerPair = false
} = {}) {
    if (!process.env.OPENAI_API_KEY || process.env.OPENAI_API_KEY === 'your_openai_api_key_here') {
        return { success: false, error: 'OpenAI API key not configured. Please set OPENAI_API_KEY in server/.env' };
    }

    const sdk = await loadAgentsSdk();
    const { z } = getZod();
    const runner = await getAgentsRunner();

    const traceEvents = [];
    const logEvent = async (eventType, payload) => {
        const nowSec = Math.floor(Date.now() / 1000);
        const summarized = summarizeForTrace(payload);
        traceEvents.push({ timestamp: nowSec, eventType, payload: summarized });
        if (runId) {
            try {
                await query(
                    'INSERT INTO bot_run_events(run_id, timestamp, event_type, payload_json) VALUES ($1, $2, $3, $4)',
                    [runId, nowSec, eventType, summarized ? JSON.stringify(summarized) : null]
                );
            } catch {
                // Best-effort trace persistence.
            }
        }
    };

    const runState = {
        lastRiskReview: null,
        toolCalls: [],
        toolResults: {},
        toolCallCounts: {},
        toolsLocked: false
    };

    const ExecuteTradeArgsSchema = z.object({
        direction: z.enum(['LONG', 'SHORT']),
        collateral: z.number().positive(),
        leverage: z.number().positive(),
        stop_loss_price: z.number().positive(),
        take_profit_price: z.number().positive(),
        trigger_price: z.number().positive().nullable(),
        confidence: z.number().min(0).max(1),
        reasoning: z.string().min(1),
        invalidation: z.string().min(1).nullable()
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

    const ExecuteTradeDecisionSchema = z.object({
        action: z.literal('execute_trade'),
        pair_index: z.number().int().nonnegative(),
        timeframe_min: z.number().int().positive(),
        args: ExecuteTradeArgsSchema
    });

    const ClosePositionDecisionSchema = z.object({
        action: z.literal('close_position'),
        pair_index: z.number().int().nonnegative(),
        timeframe_min: z.number().int().positive(),
        args: ClosePositionArgsSchema
    });

    const CancelPendingDecisionSchema = z.object({
        action: z.literal('cancel_pending'),
        pair_index: z.number().int().nonnegative(),
        timeframe_min: z.number().int().positive(),
        args: CancelPendingArgsSchema
    });

    const HoldPositionDecisionSchema = z.object({
        action: z.literal('hold_position'),
        pair_index: z.number().int().nonnegative(),
        timeframe_min: z.number().int().positive(),
        args: HoldPositionArgsSchema
    });

    const DecisionSchema = z.discriminatedUnion('action', [
        ExecuteTradeDecisionSchema,
        ClosePositionDecisionSchema,
        CancelPendingDecisionSchema,
        HoldPositionDecisionSchema
    ]);

    const OutputSchema = z.object({
        decision: DecisionSchema
    });

    const RiskReviewSchema = z.object({
        verdict: z.enum(['approve', 'reject', 'adjust']),
        reasoning: z.string().min(1),
        adjustments: z.object({
            collateral: z.number().positive().nullable(),
            leverage: z.number().positive().nullable(),
            stop_loss_price: z.number().positive().nullable(),
            take_profit_price: z.number().positive().nullable(),
            trigger_price: z.number().positive().nullable()
        }).nullable(),
        confidence_override: z.number().min(0).max(1).nullable()
    });

    const TOOL_CALL_LIMITS = {
        get_ranked_candidates: 1,
        get_pair_context: 1,
        get_open_exposure: 1,
        get_open_positions: 2,
        get_costs_and_liquidity: 1,
        get_market_snapshot: 1,
        get_multi_timeframe_snapshots: 1,
        get_similar_decisions: 2,
        get_position_risk: 1
    };

    const makeTool = (definition, { label = 'trader' } = {}) => {
        const { name, description, parameters, execute } = definition;
        return sdk.tool({
            name,
            description,
            parameters,
            async execute(args, context) {
                const hasPriorResult = Array.isArray(runState.toolResults[name]) && runState.toolResults[name].length > 0;
                if (runState.toolsLocked && !hasPriorResult) {
                    const lockedResult = { error: 'tools_locked', tool: name, note: 'Return a final decision without calling new tools.' };
                    await logEvent('tool_limit', lockedResult);
                    return lockedResult;
                }

                const count = (runState.toolCallCounts[name] ?? 0) + 1;
                runState.toolCallCounts[name] = count;
                const limit = TOOL_CALL_LIMITS[name];
                if (limit && count > limit) {
                    const limitResult = { error: 'tool_call_limit_exceeded', tool: name, limit, note: 'Return a final decision without further tool calls.' };
                    await logEvent('tool_limit', limitResult);
                    return limitResult;
                }

                runState.toolCalls.push({ name, label });
                await logEvent('tool_call', { tool: name, label, args });
                const result = await execute(args, context);
                const summarized = summarizeForTrace(result, 3);
                if (!runState.toolResults[name]) runState.toolResults[name] = [];
                runState.toolResults[name].push(summarized);
                await logEvent('tool_result', { tool: name, label, result: summarized });

                const requiredTools = ['get_ranked_candidates', 'get_pair_context', 'get_open_exposure', 'get_similar_decisions'];
                const hasRequired = requiredTools.every((toolName) => {
                    const results = runState.toolResults[toolName];
                    return Array.isArray(results) && results.length > 0;
                });
                if (hasRequired) runState.toolsLocked = true;

                return result;
            }
        });
    };

    const getRankedCandidatesTool = makeTool({
        name: 'get_ranked_candidates',
        description: 'Return top-ranked market opportunities from stored market_state for a timeframe.',
        parameters: z.object({
            timeframe_min: z.number().int().positive(),
            limit: z.number().int().min(1).max(50).nullable(),
            min_oi_total: z.number().min(0).nullable(),
            max_cost_percent: z.number().min(0).nullable()
        }),
        async execute({ timeframe_min, limit, min_oi_total, max_cost_percent }) {
            const safeLimit =
                limit !== null && Number.isFinite(Number(limit)) && Number(limit) > 0
                    ? Number(limit)
                    : opportunitiesLimit;
            const minOi =
                min_oi_total !== null && Number.isFinite(Number(min_oi_total))
                    ? Number(min_oi_total)
                    : minOiTotal;
            const maxCost =
                max_cost_percent !== null && Number.isFinite(Number(max_cost_percent))
                    ? Number(max_cost_percent)
                    : maxCostPercent;
            return botContext.getRankedCandidates({
                timeframeMin: timeframe_min,
                limit: safeLimit,
                minOiTotal: minOi,
                maxCostPercent: maxCost
            });
        }
    });

    const getOpenExposureTool = makeTool({
        name: 'get_open_exposure',
        description: 'Summarize current open exposure (positions and notional) for this execution provider.',
        parameters: z.object({}),
        async execute() {
            return getOpenExposureForProvider(executionProvider);
        }
    });

    const getOpenPositionsTool = makeTool({
        name: 'get_open_positions',
        description: 'Return open positions for a specific pair (or all pairs if omitted).',
        parameters: z.object({
            pair_index: z.number().int().nonnegative().nullable()
        }),
        async execute({ pair_index }) {
            const pairIndex =
                pair_index !== null && Number.isFinite(Number(pair_index))
                    ? Number(pair_index)
                    : null;
            return getOpenPositionsForProvider({ executionProvider, pairIndex });
        }
    });

    const getCostsAndLiquidityTool = makeTool({
        name: 'get_costs_and_liquidity',
        description: 'Fetch cost + liquidity snapshot for a pair.',
        parameters: z.object({
            pair_index: z.number().int().nonnegative()
        }),
        async execute({ pair_index }) {
            return botContext.getCostsAndLiquidity(pair_index);
        }
    });

    const getMarketSnapshotTool = makeTool({
        name: 'get_market_snapshot',
        description: 'Fetch latest market_state snapshot (indicators + staleness) for a pair/timeframe.',
        parameters: z.object({
            pair_index: z.number().int().nonnegative(),
            timeframe_min: z.number().int().positive()
        }),
        async execute({ pair_index, timeframe_min }) {
            return botContext.getMarketSnapshot(pair_index, timeframe_min);
        }
    });

    const getMultiTimeframeTool = makeTool({
        name: 'get_multi_timeframe_snapshots',
        description: 'Fetch multi-timeframe snapshots + consensus for a pair.',
        parameters: z.object({
            pair_index: z.number().int().nonnegative(),
            timeframes_min: z.array(z.number().int().positive()).min(1).max(8)
        }),
        async execute({ pair_index, timeframes_min }) {
            return botContext.getMultiTimeframeSnapshots(pair_index, timeframes_min);
        }
    });

    const getPairContextTool = makeTool({
        name: 'get_pair_context',
        description: 'Fetch consolidated context for a pair (snapshot, multi-timeframe, costs, trading variables, open positions, lessons).',
        parameters: z.object({
            pair_index: z.number().int().nonnegative(),
            timeframe_min: z.number().int().positive()
        }),
        async execute({ pair_index, timeframe_min }) {
            const snapshot = await botContext.getMarketSnapshot(pair_index, timeframe_min);
            const preferredTimeframes = [timeframe_min, 1, 5, 15, 60, 240, 1440]
                .filter((tf) => Number.isFinite(Number(tf)) && Number(tf) > 0)
                .filter((tf, idx, arr) => arr.indexOf(tf) === idx)
                .slice(0, 8);
            const multiTimeframe = await botContext.getMultiTimeframeSnapshots(pair_index, preferredTimeframes);
            const costsAndLiquidity = await botContext.getCostsAndLiquidity(pair_index);

            let tradingVariables = null;
            try {
                const tradingVariablesRaw = await fetchTradingVariablesCached();
                tradingVariables = buildTradingVariablesForPair(tradingVariablesRaw, pair_index);
            } catch {
                tradingVariables = null;
            }

            const openPositions = await getOpenPositionsForProvider({ executionProvider, pairIndex: pair_index });
            const recentLessons = await getRecentLessonsForPair(pair_index, { timeframeMin: timeframe_min, limit: 3 });

            return {
                pair_index,
                timeframe_min,
                snapshot,
                multi_timeframe: multiTimeframe,
                costs_and_liquidity: costsAndLiquidity,
                trading_variables: tradingVariables,
                open_positions: openPositions,
                recent_lessons: recentLessons
            };
        }
    });

    const getSimilarDecisionsTool = makeTool({
        name: 'get_similar_decisions',
        description: 'Find similar historical bot decisions and their outcomes.',
        parameters: z.object({
            pair_index: z.number().int().nonnegative(),
            timeframe_min: z.number().int().positive(),
            lookback_days: z.number().int().min(1).max(365).nullable(),
            limit: z.number().int().min(1).max(20).nullable()
        }),
        async execute({ pair_index, timeframe_min, lookback_days, limit }) {
            const safeLookback =
                lookback_days !== null && Number.isFinite(Number(lookback_days)) && Number(lookback_days) > 0
                    ? Number(lookback_days)
                    : 60;
            const safeLimit =
                limit !== null && Number.isFinite(Number(limit)) && Number(limit) > 0
                    ? Number(limit)
                    : 8;
            return botContext.getSimilarDecisions({
                pairIndex: pair_index,
                timeframeMin: timeframe_min,
                lookbackDays: safeLookback,
                limit: safeLimit
            });
        }
    });

    const getPositionRiskTool = makeTool({
        name: 'get_position_risk',
        description: 'Compute PnL and distance to SL/TP for a position.',
        parameters: z.object({
            trade_id: z.number().int().positive(),
            timeframe_min: z.number().int().positive().nullable(),
            current_price: z.number().positive().nullable()
        }),
        async execute({ trade_id, timeframe_min, current_price }) {
            const safeTimeframeMin =
                timeframe_min !== null && Number.isFinite(Number(timeframe_min)) && Number(timeframe_min) > 0
                    ? Number(timeframe_min)
                    : null;
            const safeCurrentPrice =
                current_price !== null && Number.isFinite(Number(current_price)) && Number(current_price) > 0
                    ? Number(current_price)
                    : null;
            return botContext.getPositionRisk(trade_id, {
                timeframeMin: safeTimeframeMin,
                currentPrice: safeCurrentPrice
            });
        }
    });

    const riskAgent = new sdk.Agent({
        name: 'Risk Manager',
        instructions: buildRiskAgentInstructions(),
        model: process.env.OPENAI_RISK_MODEL || process.env.OPENAI_TRADING_MODEL || 'gpt-5.2',
        outputType: RiskReviewSchema,
        tools: [],
        modelSettings: {
            temperature: 0.1,
            toolChoice: 'none'
        }
    });

    const traderAgent = new sdk.Agent({
        name: 'Agentic Trader',
        instructions: buildAgenticTraderInstructions(),
        model: process.env.OPENAI_TRADING_MODEL || 'gpt-5.2',
        outputType: OutputSchema,
        tools: [
            getRankedCandidatesTool,
            getPairContextTool,
            getOpenExposureTool,
            getOpenPositionsTool,
            getCostsAndLiquidityTool,
            getMarketSnapshotTool,
            getMultiTimeframeTool,
            getSimilarDecisionsTool
        ],
        modelSettings: {
            temperature: 0.2,
            toolChoice: 'auto',
            parallelToolCalls: true
        }
    });

    const decisionAgent = new sdk.Agent({
        name: 'Decision Synthesizer',
        instructions: 'Given the context, output ONLY the decision JSON. Do not call tools.',
        model: process.env.OPENAI_TRADING_MODEL || 'gpt-5.2',
        outputType: OutputSchema,
        tools: [],
        modelSettings: {
            temperature: 0.1,
            toolChoice: 'none'
        }
    });

    const getLatestToolResult = (name) => {
        const values = runState.toolResults[name];
        if (!Array.isArray(values) || values.length === 0) return null;
        return values[values.length - 1];
    };

    const buildRiskContext = (decision) => {
        const base = {
            execution_provider: executionProvider,
            timeframe_min: timeframeMin,
            decision_action: decision?.action ?? null,
            ranked_candidates: getLatestToolResult('get_ranked_candidates'),
            pair_context: getLatestToolResult('get_pair_context'),
            open_exposure: getLatestToolResult('get_open_exposure'),
            open_positions: getLatestToolResult('get_open_positions'),
            costs_and_liquidity: getLatestToolResult('get_costs_and_liquidity'),
            market_snapshot: getLatestToolResult('get_market_snapshot'),
            multi_timeframe: getLatestToolResult('get_multi_timeframe_snapshots'),
            similar_decisions: getLatestToolResult('get_similar_decisions'),
            position_risk: getLatestToolResult('get_position_risk'),
            tools_used: runState.toolCalls
        };

        return summarizeForTrace(base, 3);
    };

    const buildDecisionContext = () => {
        return summarizeForTrace({
            execution_provider: executionProvider,
            timeframe_min: timeframeMin,
            ranked_candidates: getLatestToolResult('get_ranked_candidates'),
            pair_context: getLatestToolResult('get_pair_context'),
            open_exposure: getLatestToolResult('get_open_exposure'),
            open_positions: getLatestToolResult('get_open_positions'),
            costs_and_liquidity: getLatestToolResult('get_costs_and_liquidity'),
            market_snapshot: getLatestToolResult('get_market_snapshot'),
            multi_timeframe: getLatestToolResult('get_multi_timeframe_snapshots'),
            similar_decisions: getLatestToolResult('get_similar_decisions'),
            position_risk: getLatestToolResult('get_position_risk'),
            tools_used: runState.toolCalls
        }, 3);
    };

    await logEvent('agent_start', {
        runId,
        executionProvider,
        timeframeMin,
        opportunitiesLimit,
        minOiTotal,
        maxCostPercent
    });

    const agentInput = JSON.stringify({
        timeframe_min: timeframeMin,
        opportunities_limit: opportunitiesLimit,
        min_oi_total: minOiTotal,
        max_cost_percent: maxCostPercent,
        execution_provider: executionProvider
    });

    try {
        let traderResult = null;
        let traderError = null;
        try {
            traderResult = await runner.run(traderAgent, agentInput, {
                maxTurns: 8,
                traceMetadata: { workflow: 'agentic_trade', runId }
            });
        } catch (err) {
            traderError = err;
            await logEvent('agent_error', { error: err?.message || String(err) });
        }

        const usage = traderResult?.rawResponses?.[traderResult.rawResponses.length - 1]?.usage ?? null;
        let output = traderResult?.finalOutput?.decision ?? traderResult?.finalOutput ?? null;

        if (!output || typeof output !== 'object') {
            await logEvent('agent_fallback_start', { reason: 'missing_output', error: traderError?.message || null });
            const fallbackContext = buildDecisionContext();
            try {
                const fallbackResult = await runner.run(
                    decisionAgent,
                    JSON.stringify({ context: fallbackContext }),
                    { maxTurns: 2, traceMetadata: { workflow: 'agentic_fallback', runId } }
                );
                output = fallbackResult?.finalOutput?.decision ?? fallbackResult?.finalOutput ?? null;
                await logEvent('agent_fallback_result', { decision: output });
            } catch (err) {
                await logEvent('agent_error', { error: err?.message || String(err) });
                return { success: false, error: err?.message || String(err), trace: traceEvents };
            }
        }

        if (!output || typeof output !== 'object') {
            await logEvent('agent_error', { error: 'Invalid agent output' });
            return { success: false, error: 'Invalid agent output', trace: traceEvents };
        }

        let finalDecision = output;
        let guardrail = null;

        const requiredTools = ['get_ranked_candidates', 'get_pair_context', 'get_open_exposure', 'get_similar_decisions'];
        const missingTools = requiredTools.filter((name) => !(runState.toolResults[name] && runState.toolResults[name].length > 0));

        if (missingTools.length > 0 && finalDecision.action !== 'hold_position') {
            guardrail = {
                reason: 'missing_tools',
                message: `Missing required tool calls: ${missingTools.join(', ')}`,
                original: finalDecision
            };
            finalDecision = {
                action: 'hold_position',
                pair_index: finalDecision.pair_index,
                timeframe_min: finalDecision.timeframe_min,
                args: {
                    confidence: 0.75,
                    reasoning: guardrail.message
                }
            };
        } else {
            const requiresRisk = ['execute_trade', 'close_position', 'cancel_pending'].includes(finalDecision.action);
            if (requiresRisk) {
                const riskContext = buildRiskContext(finalDecision);
                await logEvent('risk_review_start', { decision: finalDecision, context: riskContext });
                try {
                    const riskResult = await runner.run(
                        riskAgent,
                        JSON.stringify({
                            decision: finalDecision,
                            context: riskContext,
                            execution_provider: executionProvider,
                            timeframe_min: timeframeMin
                        }),
                        {
                            maxTurns: 2,
                            traceMetadata: { workflow: 'risk_review', runId }
                        }
                    );
                    runState.lastRiskReview = riskResult?.finalOutput ?? null;
                    await logEvent('risk_review_result', runState.lastRiskReview);
                } catch (err) {
                    runState.lastRiskReview = null;
                    await logEvent('risk_review_result', { error: err?.message || String(err) });
                }
            } else {
                await logEvent('risk_review_skipped', { reason: 'hold_action' });
            }

            if (requiresRisk && !runState.lastRiskReview) {
                guardrail = {
                    reason: 'missing_risk_review',
                    message: 'Risk review missing; defaulting to hold_position.',
                    original: finalDecision
                };
                finalDecision = {
                    action: 'hold_position',
                    pair_index: finalDecision.pair_index,
                    timeframe_min: finalDecision.timeframe_min,
                    args: {
                        confidence: 0.75,
                        reasoning: guardrail.message
                    }
                };
            }

            if (runState.lastRiskReview?.verdict === 'reject') {
                guardrail = {
                    reason: 'risk_reject',
                    message: runState.lastRiskReview.reasoning,
                    original: finalDecision
                };
                finalDecision = {
                    action: 'hold_position',
                    pair_index: finalDecision.pair_index,
                    timeframe_min: finalDecision.timeframe_min,
                    args: {
                        confidence: 0.75,
                        reasoning: runState.lastRiskReview.reasoning
                    }
                };
            }

            if (runState.lastRiskReview?.verdict === 'adjust' && finalDecision.action === 'execute_trade') {
                const adjustments = runState.lastRiskReview.adjustments || {};
                const coerceFiniteNumberOrNull = (value) => {
                    if (value === null || value === undefined) return null;
                    const num = Number(value);
                    return Number.isFinite(num) ? num : null;
                };
                const collateralOverride = coerceFiniteNumberOrNull(adjustments.collateral);
                const leverageOverride = coerceFiniteNumberOrNull(adjustments.leverage);
                const stopLossOverride = coerceFiniteNumberOrNull(adjustments.stop_loss_price);
                const takeProfitOverride = coerceFiniteNumberOrNull(adjustments.take_profit_price);
                const triggerOverride = coerceFiniteNumberOrNull(adjustments.trigger_price);
                const confidenceOverride = coerceFiniteNumberOrNull(runState.lastRiskReview.confidence_override);
                finalDecision = {
                    ...finalDecision,
                    args: {
                        ...finalDecision.args,
                        collateral: collateralOverride ?? finalDecision.args.collateral,
                        leverage: leverageOverride ?? finalDecision.args.leverage,
                        stop_loss_price: stopLossOverride ?? finalDecision.args.stop_loss_price,
                        take_profit_price: takeProfitOverride ?? finalDecision.args.take_profit_price,
                        trigger_price: triggerOverride ?? finalDecision.args.trigger_price,
                        confidence: confidenceOverride ?? finalDecision.args.confidence
                    }
                };
            }
        }

        if (finalDecision.action === 'execute_trade' && !allowMultiplePositionsPerPair) {
            const openPositionsForPair = await getOpenPositionsForProvider({
                executionProvider,
                pairIndex: finalDecision.pair_index
            });
            if (openPositionsForPair.length > 0) {
                guardrail = {
                    reason: 'existing_position',
                    message: 'Existing position already present for this pair; not opening a new trade.',
                    original: finalDecision
                };
                finalDecision = {
                    action: 'hold_position',
                    pair_index: finalDecision.pair_index,
                    timeframe_min: finalDecision.timeframe_min,
                    args: {
                        confidence: 0.8,
                        reasoning: guardrail.message
                    }
                };
            }
        }

        await logEvent('agent_output', { decision: finalDecision, guardrail });

        const model = process.env.OPENAI_TRADING_MODEL || 'gpt-5.2';
        const analysisCost = buildAnalysisCost(usage);
        const decisionAnalysis = {
            runId,
            decision: finalDecision,
            guardrail,
            riskReview: runState.lastRiskReview,
            toolsUsed: runState.toolCalls,
            executionProvider,
            timeframeMin,
            llm: {
                api: 'agents_sdk',
                model
            }
        };

        const parsedConfidence = Number.isFinite(Number(finalDecision?.args?.confidence))
            ? Math.max(0, Math.min(1, Number(finalDecision.args.confidence)))
            : null;

        const nowSec = Math.floor(Date.now() / 1000);
        const normalizedUsage = normalizeUsage(usage);

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
                finalDecision.pair_index,
                nowSec,
                JSON.stringify(decisionAnalysis),
                finalDecision.action,
                parsedConfidence ?? 0.75,
                finalDecision.action,
                finalDecision?.args?.reasoning ?? 'N/A',
                runId ? `agentic:${runId}` : null,
                finalDecision.timeframe_min ?? timeframeMin,
                null,
                `${PROMPT_VERSION}:agentic:v1`,
                model,
                normalizedUsage ? JSON.stringify(normalizedUsage) : null,
                analysisCost ? JSON.stringify(analysisCost) : null
            ]
        );

        return {
            success: true,
            decision: finalDecision,
            decisionId: decisionResult.lastID,
            riskReview: runState.lastRiskReview,
            guardrail,
            usage: normalizeUsage(usage),
            analysisCost,
            trace: traceEvents
        };
    } catch (err) {
        await logEvent('agent_error', { error: err?.message || String(err) });
        return { success: false, error: err?.message || String(err), trace: traceEvents };
    }
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
    SAFETY_LIMITS,
    runAgentDecision
};
