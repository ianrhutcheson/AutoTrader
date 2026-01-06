require('dotenv').config();

const GAINS_TRADING_VARIABLES_URL =
    process.env.GAINS_TRADING_VARIABLES_URL || 'https://backend-base.gains.trade/trading-variables';
const GAINS_TRADING_VARIABLES_CACHE_MS =
    Number.parseInt(process.env.GAINS_TRADING_VARIABLES_CACHE_MS, 10) || 60_000;
const GAINS_COLLATERAL_SYMBOL = process.env.GAINS_COLLATERAL_SYMBOL || null;

let tradingVariablesCache = {
    fetchedAtMs: 0,
    data: null
};

async function fetchTradingVariablesCached() {
    const now = Date.now();
    if (
        tradingVariablesCache.data &&
        now - tradingVariablesCache.fetchedAtMs < GAINS_TRADING_VARIABLES_CACHE_MS
    ) {
        return tradingVariablesCache.data;
    }

    const response = await fetch(GAINS_TRADING_VARIABLES_URL);
    if (!response.ok) {
        throw new Error(`Failed to fetch trading variables: ${response.status} ${response.statusText}`);
    }

    const data = await response.json();
    tradingVariablesCache = { data, fetchedAtMs: now };
    return data;
}

function parseBigIntOrNull(value) {
    if (value === null || value === undefined) return null;
    try {
        return BigInt(value);
    } catch {
        return null;
    }
}

function toNumberOrNull(value) {
    if (value === null || value === undefined) return null;
    const num = typeof value === 'number' ? value : Number(value);
    return Number.isFinite(num) ? num : null;
}

function percentFrom1e10(rawPercentP) {
    const raw = parseBigIntOrNull(rawPercentP);
    if (raw === null) return null;
    return Number(raw) / 1e10;
}

function leverageFrom1e3(rawLeverage) {
    const raw = parseBigIntOrNull(rawLeverage);
    if (raw === null) return null;
    return Number(raw) / 1000;
}

function usdFrom1e3(rawUsd) {
    const raw = parseBigIntOrNull(rawUsd);
    if (raw === null) return null;
    return Number(raw) / 1000;
}

function collateralFromAtomic(rawAmount, precision) {
    const raw = parseBigIntOrNull(rawAmount);
    const denom = parseBigIntOrNull(precision);
    if (raw === null || denom === null || denom === 0n) return null;
    return Number(raw) / Number(denom);
}

function selectCollateral(collaterals) {
    if (!Array.isArray(collaterals) || collaterals.length === 0) return null;
    if (GAINS_COLLATERAL_SYMBOL) {
        const preferred = collaterals.find(c => c?.symbol === GAINS_COLLATERAL_SYMBOL);
        if (preferred) return preferred;
    }
    return collaterals.find(c => c?.isActive) || collaterals[0];
}

function buildTradingVariablesForPair(tradingVariables, pairIndex) {
    if (!tradingVariables || !Array.isArray(tradingVariables.pairs)) return null;
    const pair = tradingVariables.pairs[pairIndex];
    if (!pair) return null;

    const groupIndex = Number.parseInt(pair.groupIndex, 10);
    const feeIndex = Number.parseInt(pair.feeIndex, 10);
    const group = Array.isArray(tradingVariables.groups) ? tradingVariables.groups[groupIndex] : null;
    const fees = Array.isArray(tradingVariables.fees) ? tradingVariables.fees[feeIndex] : null;

    const collateral = selectCollateral(tradingVariables.collaterals);
    const collateralConfig = collateral?.collateralConfig || null;
    const collateralPrecision = collateralConfig?.precision ?? null;

    const pairOis = Array.isArray(collateral?.pairOis) ? collateral.pairOis[pairIndex] : null;
    const oiLong = collateralFromAtomic(pairOis?.collateral?.oiLongCollateral, collateralPrecision);
    const oiShort = collateralFromAtomic(pairOis?.collateral?.oiShortCollateral, collateralPrecision);
    const oiTotal = oiLong !== null && oiShort !== null ? oiLong + oiShort : null;
    const oiSkewPercent = oiLong !== null && oiShort !== null && oiTotal ? ((oiLong - oiShort) / oiTotal) * 100 : null;

    const fundingParams = Array.isArray(collateral?.fundingFees?.pairParams) ? collateral.fundingFees.pairParams[pairIndex] : null;
    const fundingData = Array.isArray(collateral?.fundingFees?.pairData) ? collateral.fundingFees.pairData[pairIndex] : null;

    const borrowingParams = Array.isArray(collateral?.borrowingFees?.v2?.pairParams) ? collateral.borrowingFees.v2.pairParams[pairIndex] : null;
    const borrowingData = Array.isArray(collateral?.borrowingFees?.v2?.pairData) ? collateral.borrowingFees.v2.pairData[pairIndex] : null;

    return {
        source: {
            url: GAINS_TRADING_VARIABLES_URL,
            lastRefreshed: tradingVariables.lastRefreshed ?? null,
            refreshId: tradingVariables.refreshId ?? null
        },
        pair: {
            index: pairIndex,
            from: pair.from ?? null,
            to: pair.to ?? null,
            spreadPercent: percentFrom1e10(pair.spreadP),
            groupIndex: Number.isFinite(groupIndex) ? groupIndex : null,
            feeIndex: Number.isFinite(feeIndex) ? feeIndex : null
        },
        group: group ? {
            name: group.name ?? null,
            minLeverage: leverageFrom1e3(group.minLeverage),
            maxLeverage: leverageFrom1e3(group.maxLeverage)
        } : null,
        fees: fees ? {
            positionSizeFeePercent: percentFrom1e10(fees.totalPositionSizeFeeP),
            oraclePositionSizeFeePercent: percentFrom1e10(fees.oraclePositionSizeFeeP),
            minPositionSizeUsd: usdFrom1e3(fees.minPositionSizeUsd)
        } : null,
        openInterest: collateral ? {
            collateralSymbol: collateral.symbol ?? null,
            collateralPriceUsd: toNumberOrNull(collateral?.prices?.collateralPriceUsd),
            long: oiLong,
            short: oiShort,
            skewPercent: oiSkewPercent
        } : null,
        funding: fundingParams || fundingData ? {
            enabled: !!fundingParams?.fundingFeesEnabled,
            lastFundingRatePerSecondP: fundingData?.lastFundingRatePerSecondP ?? null,
            lastUpdateTs: fundingData?.lastFundingUpdateTs ? Number.parseInt(fundingData.lastFundingUpdateTs, 10) : null
        } : null,
        borrowing: borrowingParams || borrowingData ? {
            borrowingRatePerSecondP: borrowingParams?.borrowingRatePerSecondP ?? null,
            lastUpdateTs: borrowingData?.lastBorrowingUpdateTs ? Number.parseInt(borrowingData.lastBorrowingUpdateTs, 10) : null
        } : null
    };
}

function listPairs(tradingVariables) {
    if (!tradingVariables || !Array.isArray(tradingVariables.pairs)) return [];
    return tradingVariables.pairs.map((pair, index) => ({
        pair_index: index,
        from: pair?.from ?? null,
        to: pair?.to ?? null
    }));
}

module.exports = {
    fetchTradingVariablesCached,
    buildTradingVariablesForPair,
    listPairs,
    percentFrom1e10
};

