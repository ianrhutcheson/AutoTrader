function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
}

function quantile(values, q) {
    if (!Array.isArray(values) || values.length === 0) return null;
    const qq = Math.max(0, Math.min(1, q));
    const sorted = values.slice().sort((a, b) => a - b);
    const pos = (sorted.length - 1) * qq;
    const base = Math.floor(pos);
    const rest = pos - base;
    const left = sorted[base];
    const right = sorted[Math.min(base + 1, sorted.length - 1)];
    return left + (right - left) * rest;
}

function safeNumber(value) {
    return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function safeDiv(numerator, denominator) {
    if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || denominator === 0) return null;
    return numerator / denominator;
}

function computeCostPercent(row) {
    const spread = safeNumber(row.spread_percent) ?? 0;
    const fee = safeNumber(row.fee_position_size_percent) ?? 0;
    const oracleFee = safeNumber(row.fee_oracle_position_size_percent) ?? 0;
    return spread + fee + oracleFee;
}

function computeOiTotal(row) {
    const oiLong = safeNumber(row.oi_long);
    const oiShort = safeNumber(row.oi_short);
    if (oiLong === null || oiShort === null) return null;
    return oiLong + oiShort;
}

function scoreOpportunity(row, options = {}) {
    const minOiTotal = typeof options.minOiTotal === 'number' ? options.minOiTotal : 1;
    const maxCostPercent = typeof options.maxCostPercent === 'number' ? options.maxCostPercent : 0.25;

    const price = safeNumber(row.price);
    const rsi = safeNumber(row.rsi);
    const macdHist = safeNumber(row.macd_histogram);
    const ema9 = safeNumber(row.ema9);
    const ema21 = safeNumber(row.ema21);
    const bbUpper = safeNumber(row.bb_upper);
    const bbMiddle = safeNumber(row.bb_middle);
    const bbLower = safeNumber(row.bb_lower);
    const atr = safeNumber(row.atr);
    const stochK = safeNumber(row.stoch_k);

    if (price === null) return null;
    if (rsi === null || macdHist === null || ema9 === null || ema21 === null) return null;

    const volatilityUnit = atr ?? (price * 0.005); // fallback ~0.5% if ATR isn't available yet
    const emaDiff = ema9 - ema21;
    const emaStrength = safeDiv(emaDiff, volatilityUnit) ?? 0;
    const macdStrength = safeDiv(macdHist, volatilityUnit) ?? 0;

    const bandHalfWidth = (bbUpper !== null && bbLower !== null) ? (bbUpper - bbLower) / 2 : null;
    const bbZ = (bbMiddle !== null && bandHalfWidth && bandHalfWidth > 0)
        ? safeDiv(price - bbMiddle, bandHalfWidth) ?? 0
        : 0;

    const trendLong =
        clamp(emaStrength, 0, 2) * 20 +
        clamp(macdStrength, 0, 2) * 15 +
        clamp((rsi - 50) / 20, 0, 1) * 15 +
        clamp(bbZ, 0, 1) * 10 +
        (stochK !== null ? clamp((stochK - 50) / 30, 0, 1) * 5 : 0);

    const trendShort =
        clamp(-emaStrength, 0, 2) * 20 +
        clamp(-macdStrength, 0, 2) * 15 +
        clamp((50 - rsi) / 20, 0, 1) * 15 +
        clamp(-bbZ, 0, 1) * 10 +
        (stochK !== null ? clamp((50 - stochK) / 30, 0, 1) * 5 : 0);

    const meanRevLong =
        (rsi < 30 ? clamp((30 - rsi) / 15, 0, 1) * 25 : 0) +
        (bbLower !== null && bandHalfWidth && bandHalfWidth > 0 && price < bbLower
            ? clamp((bbLower - price) / bandHalfWidth, 0, 1) * 25
            : 0) +
        (stochK !== null && stochK < 20 ? clamp((20 - stochK) / 20, 0, 1) * 15 : 0);

    const meanRevShort =
        (rsi > 70 ? clamp((rsi - 70) / 15, 0, 1) * 25 : 0) +
        (bbUpper !== null && bandHalfWidth && bandHalfWidth > 0 && price > bbUpper
            ? clamp((price - bbUpper) / bandHalfWidth, 0, 1) * 25
            : 0) +
        (stochK !== null && stochK > 80 ? clamp((stochK - 80) / 20, 0, 1) * 15 : 0);

    const longScore = Math.max(trendLong, meanRevLong);
    const shortScore = Math.max(trendShort, meanRevShort);

    let side = 'LONG';
    let rawScore = longScore;
    if (shortScore > longScore) {
        side = 'SHORT';
        rawScore = shortScore;
    }

    // Liquidity + cost filters
    const oiTotal = computeOiTotal(row);
    if (oiTotal === null || oiTotal <= 0) return null;

    const costPercent = computeCostPercent(row);
    const costPenalty = clamp(costPercent / maxCostPercent, 0, 1) * 25;

    const oiPenalty = oiTotal < minOiTotal ? 20 * clamp((minOiTotal - oiTotal) / minOiTotal, 0, 1) : 0;

    const skew = safeNumber(row.oi_skew_percent);
    const skewPenalty = skew !== null ? clamp(Math.abs(skew) / 80, 0, 1) * 10 : 0;

    // Avoid chasing extreme exhaustion (still allow mean reversion setups)
    const exhaustionPenalty =
        side === 'LONG'
            ? (rsi > 78 ? 10 : 0)
            : (rsi < 22 ? 10 : 0);

    const baseScore = clamp(rawScore - costPenalty - oiPenalty - skewPenalty - exhaustionPenalty, 0, 100);

    const reasons = [];
    reasons.push(side === 'LONG' ? 'Best setup skewed long' : 'Best setup skewed short');
    if (Math.max(trendLong, trendShort) >= Math.max(meanRevLong, meanRevShort)) {
        reasons.push('Trend alignment');
    } else {
        reasons.push('Mean reversion extreme');
    }
    if (costPercent > 0) reasons.push(`Costs ~${costPercent.toFixed(4)}%`);
    if (oiTotal !== null) reasons.push(`OI ~${oiTotal.toFixed(0)}`);

    return {
        side,
        score: baseScore,
        reasons,
        metrics: {
            baseScore,
            rawScore,
            costPercent,
            oiTotal,
            emaStrength,
            macdStrength,
            bbZ
        }
    };
}

function rankTop(rows, options = {}) {
    const scored = [];
    for (const row of rows) {
        const result = scoreOpportunity(row, options);
        if (!result) continue;
        scored.push({ row, ...result });
    }

    scored.sort((a, b) => b.score - a.score);
    return scored;
}

module.exports = {
    scoreOpportunity,
    rankTop
};

