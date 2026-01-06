/**
 * Technical Indicators Module
 * Calculates RSI, MACD, Bollinger Bands, EMA, SMA, ATR, Stochastic
 */

const {
    RSI,
    MACD,
    BollingerBands,
    EMA,
    SMA,
    ATR,
    Stochastic
} = require('technicalindicators');

/**
 * Calculate all indicators from OHLC data
 * @param {Array} ohlcData - Array of {time, open, high, low, close}
 * @returns {Object} - All calculated indicators
 */
function calculateIndicators(ohlcData) {
    if (!ohlcData || ohlcData.length < 50) {
        return null; // Need at least 50 candles for meaningful indicators
    }

    const closes = ohlcData.map(d => d.close);
    const highs = ohlcData.map(d => d.high);
    const lows = ohlcData.map(d => d.low);

    // RSI (14-period)
    const rsiValues = RSI.calculate({
        values: closes,
        period: 14
    });

    // MACD (12, 26, 9)
    const macdValues = MACD.calculate({
        values: closes,
        fastPeriod: 12,
        slowPeriod: 26,
        signalPeriod: 9,
        SimpleMAOscillator: false,
        SimpleMASignal: false
    });

    // Bollinger Bands (20-period, 2 std dev)
    const bbValues = BollingerBands.calculate({
        period: 20,
        values: closes,
        stdDev: 2
    });

    // EMA (9, 21, 50, 200)
    const ema9 = EMA.calculate({ period: 9, values: closes });
    const ema21 = EMA.calculate({ period: 21, values: closes });
    const ema50 = EMA.calculate({ period: 50, values: closes });
    const ema200 = closes.length >= 200 ? EMA.calculate({ period: 200, values: closes }) : [];

    // SMA (20, 50, 200)
    const sma20 = SMA.calculate({ period: 20, values: closes });
    const sma50 = SMA.calculate({ period: 50, values: closes });
    const sma200 = closes.length >= 200 ? SMA.calculate({ period: 200, values: closes }) : [];

    // ATR (14-period)
    const atrValues = ATR.calculate({
        high: highs,
        low: lows,
        close: closes,
        period: 14
    });

    // Stochastic (14, 3, 3)
    const stochValues = Stochastic.calculate({
        high: highs,
        low: lows,
        close: closes,
        period: 14,
        signalPeriod: 3
    });

    // Get the latest values
    const latest = {
        price: closes[closes.length - 1],
        rsi: rsiValues.length > 0 ? rsiValues[rsiValues.length - 1] : null,
        macd: macdValues.length > 0 ? macdValues[macdValues.length - 1] : null,
        bollingerBands: bbValues.length > 0 ? bbValues[bbValues.length - 1] : null,
        ema: {
            ema9: ema9.length > 0 ? ema9[ema9.length - 1] : null,
            ema21: ema21.length > 0 ? ema21[ema21.length - 1] : null,
            ema50: ema50.length > 0 ? ema50[ema50.length - 1] : null,
            ema200: ema200.length > 0 ? ema200[ema200.length - 1] : null
        },
        sma: {
            sma20: sma20.length > 0 ? sma20[sma20.length - 1] : null,
            sma50: sma50.length > 0 ? sma50[sma50.length - 1] : null,
            sma200: sma200.length > 0 ? sma200[sma200.length - 1] : null
        },
        atr: atrValues.length > 0 ? atrValues[atrValues.length - 1] : null,
        stochastic: stochValues.length > 0 ? stochValues[stochValues.length - 1] : null
    };

    return {
        latest,
        history: {
            rsi: rsiValues.slice(-50),
            macd: macdValues.slice(-50),
            bollingerBands: bbValues.slice(-50),
            atr: atrValues.slice(-50),
            stochastic: stochValues.slice(-50)
        }
    };
}

/**
 * Generate a market analysis summary for AI consumption
 */
function generateMarketSummary(indicators, currentPrice) {
    if (!indicators || !indicators.latest) {
        return { summary: "Insufficient data for analysis", signals: [] };
    }

    const { latest } = indicators;
    const signals = [];

    // RSI Analysis
    if (latest.rsi !== null) {
        if (latest.rsi > 70) {
            signals.push({ indicator: 'RSI', signal: 'OVERBOUGHT', value: latest.rsi.toFixed(2) });
        } else if (latest.rsi < 30) {
            signals.push({ indicator: 'RSI', signal: 'OVERSOLD', value: latest.rsi.toFixed(2) });
        } else {
            signals.push({ indicator: 'RSI', signal: 'NEUTRAL', value: latest.rsi.toFixed(2) });
        }
    }

    // MACD Analysis
    if (latest.macd) {
        const macdSignal = latest.macd.MACD > latest.macd.signal ? 'BULLISH' : 'BEARISH';
        signals.push({
            indicator: 'MACD',
            signal: macdSignal,
            value: `MACD: ${latest.macd.MACD?.toFixed(2)}, Signal: ${latest.macd.signal?.toFixed(2)}`
        });
    }

    // Bollinger Bands Analysis
    if (latest.bollingerBands) {
        const bb = latest.bollingerBands;
        if (currentPrice > bb.upper) {
            signals.push({ indicator: 'BB', signal: 'ABOVE_UPPER', value: `Upper: ${bb.upper.toFixed(2)}` });
        } else if (currentPrice < bb.lower) {
            signals.push({ indicator: 'BB', signal: 'BELOW_LOWER', value: `Lower: ${bb.lower.toFixed(2)}` });
        } else {
            signals.push({ indicator: 'BB', signal: 'WITHIN_BANDS', value: `Middle: ${bb.middle.toFixed(2)}` });
        }
    }

    // EMA Trend Analysis
    if (latest.ema.ema9 && latest.ema.ema21) {
        const emaTrend = latest.ema.ema9 > latest.ema.ema21 ? 'BULLISH' : 'BEARISH';
        signals.push({
            indicator: 'EMA_CROSS',
            signal: emaTrend,
            value: `EMA9: ${latest.ema.ema9.toFixed(2)}, EMA21: ${latest.ema.ema21.toFixed(2)}`
        });
    }

    // Stochastic Analysis
    if (latest.stochastic) {
        const stoch = latest.stochastic;
        if (stoch.k > 80) {
            signals.push({ indicator: 'STOCH', signal: 'OVERBOUGHT', value: `K: ${stoch.k.toFixed(2)}` });
        } else if (stoch.k < 20) {
            signals.push({ indicator: 'STOCH', signal: 'OVERSOLD', value: `K: ${stoch.k.toFixed(2)}` });
        }
    }

    // Generate summary
    const bullishCount = signals.filter(s =>
        s.signal === 'BULLISH' || s.signal === 'OVERSOLD' || s.signal === 'BELOW_LOWER'
    ).length;
    const bearishCount = signals.filter(s =>
        s.signal === 'BEARISH' || s.signal === 'OVERBOUGHT' || s.signal === 'ABOVE_UPPER'
    ).length;

    let overallBias = 'NEUTRAL';
    if (bullishCount > bearishCount + 1) overallBias = 'BULLISH';
    else if (bearishCount > bullishCount + 1) overallBias = 'BEARISH';

    return {
        summary: `Market bias: ${overallBias}. ${bullishCount} bullish signals, ${bearishCount} bearish signals.`,
        overallBias,
        signals,
        indicators: latest
    };
}

module.exports = {
    calculateIndicators,
    generateMarketSummary
};
