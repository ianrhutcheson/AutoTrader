import { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import { createPortal } from 'react-dom';
import './App.css';
import { Chart } from './components/Chart';
import { Controls } from './components/Controls';
import { Positions } from './components/Positions';
import { BotPanel } from './components/BotPanel';
import { fetchChartData, createTrade, getTrades, closeTrade, cancelTrade } from './services/api';
import type { ChartDataPoint, Trade } from './services/api';
import { API_BASE } from './services/apiBase';

const FEE_BPS = 6; // 6 basis points = 0.06%
const MIN_FEE = 1.25;

const calculateOpeningFee = (trade: Trade): number => {
  const positionSize = trade.collateral * trade.leverage;
  const fee = positionSize * (FEE_BPS / 10000);
  return Math.max(fee, MIN_FEE);
};

const formatUsd = (value: number): string => {
  const sign = value < 0 ? '-' : '';
  const abs = Math.abs(value);
  return `${sign}$${abs.toFixed(2)}`;
};

interface TooltipAnchorRect {
  top: number;
  left: number;
  width: number;
  height: number;
}

const HISTORY_FROM_SEC = Math.floor(Date.UTC(2024, 0, 1, 0, 0, 0) / 1000);
const MAX_HISTORY_CANDLES = 2500;

const resolutionToSeconds = (resolution: string): number => {
  if (/^\d+$/.test(resolution)) return Number.parseInt(resolution, 10) * 60;
  const dayMatch = resolution.match(/^(\d+)D$/);
  if (dayMatch) return Number.parseInt(dayMatch[1], 10) * 24 * 60 * 60;
  return 60;
};

const THEME_STORAGE_KEY = 'perps-trader-theme';

const mergeIncrementalCandles = (
  existing: ChartDataPoint[],
  incoming: ChartDataPoint[],
): ChartDataPoint[] => {
  if (existing.length === 0) return incoming;
  if (incoming.length === 0) return existing;

  const startTime = incoming[0].time;
  let lo = 0;
  let hi = existing.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (existing[mid].time < startTime) lo = mid + 1;
    else hi = mid;
  }

  const prefix = existing.slice(0, lo);
  const mergedTail: ChartDataPoint[] = [];
  let i = 0;
  let j = lo;
  while (i < incoming.length && j < existing.length) {
    const a = incoming[i];
    const b = existing[j];
    if (a.time < b.time) {
      mergedTail.push(a);
      i += 1;
    } else if (a.time > b.time) {
      mergedTail.push(b);
      j += 1;
    } else {
      mergedTail.push(a);
      i += 1;
      j += 1;
    }
  }
  while (i < incoming.length) mergedTail.push(incoming[i++]);
  while (j < existing.length) mergedTail.push(existing[j++]);
  return prefix.concat(mergedTail);
};

const normalizeChartData = (points: ChartDataPoint[]): ChartDataPoint[] => {
  const filtered = points.filter((point) =>
    Number.isFinite(point.time) &&
    Number.isFinite(point.open) &&
    Number.isFinite(point.high) &&
    Number.isFinite(point.low) &&
    Number.isFinite(point.close)
  );

  const sorted = filtered
    .map((point) => ({ ...point, time: Math.trunc(point.time) }))
    .sort((a, b) => a.time - b.time);

  // Lightweight Charts requires strictly increasing times; some resolutions can return boundary duplicates.
  const deduped: ChartDataPoint[] = [];
  for (const point of sorted) {
    const prev = deduped[deduped.length - 1];
    if (prev && prev.time === point.time) {
      deduped[deduped.length - 1] = point;
    } else {
      deduped.push(point);
    }
  }
  return deduped;
};

function App() {
  const [pairIndex, setPairIndex] = useState(0);
  const [resolution, setResolution] = useState("15");
  const [darkMode, setDarkMode] = useState(() => {
    if (typeof window === 'undefined') return false;
    return window.localStorage.getItem(THEME_STORAGE_KEY) === 'dark';
  });
  const [data, setData] = useState<ChartDataPoint[]>([]);
  const [trades, setTrades] = useState<Trade[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [highlightedTradeId, setHighlightedTradeId] = useState<number | null>(null);
  const [pendingTradeFocus, setPendingTradeFocus] = useState<{ tradeId: number; pairIndex: number } | null>(null);
  const [pnlTooltipAnchor, setPnlTooltipAnchor] = useState<TooltipAnchorRect | null>(null);

  const dataRef = useRef<ChartDataPoint[]>([]);
  const settingsRef = useRef({ pairIndex, resolution });
  const chartAbortRef = useRef<AbortController | null>(null);
  const chartRequestIdRef = useRef(0);
  const priceStreamRef = useRef<EventSource | null>(null);
  const priceStreamConnectedRef = useRef(false);
  const tradesRefreshInFlightRef = useRef(false);

  settingsRef.current = { pairIndex, resolution };

  useEffect(() => {
    dataRef.current = data;
  }, [data]);

  useEffect(() => {
    const theme = darkMode ? 'dark' : 'light';
    document.documentElement.dataset.theme = theme;
    if (typeof window !== 'undefined') {
      window.localStorage?.setItem(THEME_STORAGE_KEY, theme);
    }
  }, [darkMode]);

  const toggleTheme = useCallback(() => {
    setDarkMode((prev) => !prev);
  }, []);

  const loadData = useCallback(async (useCurrentTime = false) => {
    const { pairIndex, resolution } = settingsRef.current;

    if (useCurrentTime && chartAbortRef.current) return;

    const requestId = chartRequestIdRef.current + 1;
    chartRequestIdRef.current = requestId;

    setLoading(true);
    setError(null);
    const controller = new AbortController();

    if (!useCurrentTime && chartAbortRef.current) {
      chartAbortRef.current.abort();
    }

    chartAbortRef.current = controller;

    try {
      const currentTo = Math.floor(Date.now() / 1000);

      const existingData = dataRef.current;
      const candleSeconds = resolutionToSeconds(resolution);
      const overlapSeconds = Math.max(candleSeconds * 5, 60);

      const maxHistoryWindowSec = candleSeconds * MAX_HISTORY_CANDLES;
      const initialHistoryFrom = Math.max(HISTORY_FROM_SEC, currentTo - maxHistoryWindowSec);

      const fetchFrom =
        useCurrentTime && existingData.length > 0
          ? Math.max(HISTORY_FROM_SEC, existingData[existingData.length - 1].time - overlapSeconds)
          : initialHistoryFrom;

      const chartDataPromise =
        fetchFrom >= currentTo
          ? Promise.resolve([] as ChartDataPoint[])
          : fetchChartData(pairIndex, fetchFrom, currentTo, resolution, { signal: controller.signal });

      const [chartResult, tradesResult] = await Promise.allSettled([
        chartDataPromise,
        getTrades(),
      ]);

      if (chartRequestIdRef.current !== requestId) return;

      if (chartResult.status === 'rejected') {
        throw chartResult.reason;
      }

      const chartData = chartResult.value;
      const nextChartData = normalizeChartData(chartData);
      if (useCurrentTime && existingData.length > 0) {
        setData(mergeIncrementalCandles(existingData, nextChartData));
      } else {
        setData(nextChartData);
      }

      if (tradesResult.status === 'fulfilled') {
        setTrades(tradesResult.value);
      } else {
        console.warn("Failed to fetch trades:", tradesResult.reason);
      }
    } catch (err) {
      if ((err as { name?: string })?.name === 'AbortError') return;
      if (chartRequestIdRef.current === requestId) {
        setError("Failed to fetch data");
        console.error(err);
      }
    } finally {
      if (chartAbortRef.current === controller) {
        chartAbortRef.current = null;
      }
      if (chartRequestIdRef.current === requestId) {
        setLoading(false);
      }
    }
  }, []);

  const refreshTrades = useCallback(async () => {
    if (tradesRefreshInFlightRef.current) return;
    tradesRefreshInFlightRef.current = true;
    try {
      const nextTrades = await getTrades();
      setTrades(nextTrades);
    } catch (err) {
      console.warn('Failed to refresh trades:', err);
    } finally {
      tradesRefreshInFlightRef.current = false;
    }
  }, []);

  const applyLivePrice = useCallback((price: number, timestampMs: number) => {
    if (!Number.isFinite(price) || !Number.isFinite(timestampMs)) return;

    setData((prev) => {
      if (prev.length === 0) return prev;

      const resolutionSeconds = resolutionToSeconds(settingsRef.current.resolution);
      const bucketTime = Math.floor(timestampMs / 1000 / resolutionSeconds) * resolutionSeconds;

      const last = prev[prev.length - 1];
      if (bucketTime < last.time) return prev;

      if (bucketTime === last.time) {
        const nextHigh = Math.max(last.high, price);
        const nextLow = Math.min(last.low, price);
        if (last.close === price && last.high === nextHigh && last.low === nextLow) return prev;

        const next = prev.slice();
        next[next.length - 1] = {
          ...last,
          high: nextHigh,
          low: nextLow,
          close: price,
        };
        return next;
      }

      const next = prev.slice();
      const prevClose = last.close;

      let t = last.time + resolutionSeconds;
      while (t < bucketTime) {
        next.push({ time: t, open: prevClose, high: prevClose, low: prevClose, close: prevClose });
        t += resolutionSeconds;
      }

      next.push({ time: bucketTime, open: prevClose, high: price, low: price, close: price });
      return next;
    });
  }, []);

  const handleTrade = async (
    direction: 'LONG' | 'SHORT',
    collateral: number,
    leverage: number,
    stopLossPercent: number | null,
    takeProfitPercent: number | null,
    triggerPrice: number | null,
  ) => {
    console.log(`Executing ${direction} trade with $${collateral} at ${leverage}x`);
    if (data.length === 0) {
      alert("No market data available to trade.");
      return;
    }
    const lastPoint = data[data.length - 1];
    const currentPrice = lastPoint.close;
    const basePrice = triggerPrice ?? currentPrice;

    try {
      const stopLossPrice =
        typeof stopLossPercent === 'number'
          ? (direction === 'LONG'
            ? basePrice * (1 - stopLossPercent / 100)
            : basePrice * (1 + stopLossPercent / 100))
          : undefined;
      const takeProfitPrice =
        typeof takeProfitPercent === 'number'
          ? (direction === 'LONG'
            ? basePrice * (1 + takeProfitPercent / 100)
            : basePrice * (1 - takeProfitPercent / 100))
          : undefined;

      await createTrade({
        pair_index: pairIndex,
        entry_price: currentPrice,
        entry_time: Math.floor(Date.now() / 1000),
        collateral,
        leverage,
        direction,
        stop_loss_price: stopLossPrice,
        take_profit_price: takeProfitPrice,
        trigger_price: triggerPrice,
      });
      await refreshTrades();
    } catch (err) {
      console.error("Trade failed:", err);
      alert("Failed to execute trade.");
    }
  };

  const handleCloseTrade = async (id: number) => {
    if (data.length === 0) return;
    const currentPrice = data[data.length - 1].close;
    try {
      await closeTrade(id, {
        exit_price: currentPrice,
        exit_time: Math.floor(Date.now() / 1000),
      });
      await refreshTrades();
    } catch (err) {
      console.error("Close trade failed:", err);
      alert("Failed to close trade.");
    }
  };

  const handleCancelTrade = async (id: number) => {
    try {
      await cancelTrade(id);
      await refreshTrades();
    } catch (err) {
      console.error("Cancel trade failed:", err);
      alert("Failed to cancel trade.");
    }
  };

  const handleMarkerHover = (tradeId: number | null) => {
    setHighlightedTradeId(tradeId);
  };

  const scrollToTradeRow = useCallback((tradeId: number): boolean => {
    const element = document.getElementById(`trade-row-${tradeId}`);
    if (!element) return false;
    element.scrollIntoView({ behavior: 'smooth', block: 'center' });
    element.classList.add('highlight-row-flash');
    window.setTimeout(() => element.classList.remove('highlight-row-flash'), 2000);
    return true;
  }, []);

  const handleMarkerClick = useCallback(
    (tradeId: number) => {
      setHighlightedTradeId(tradeId);
      scrollToTradeRow(tradeId);
    },
    [scrollToTradeRow],
  );

  const handleDecisionFocusTrade = useCallback(
    (tradeId: number, tradePairIndex?: number | null) => {
      if (typeof tradePairIndex === 'number' && tradePairIndex !== pairIndex) {
        setPendingTradeFocus({ tradeId, pairIndex: tradePairIndex });
        setPairIndex(tradePairIndex);
        return;
      }

      setPendingTradeFocus(null);
      setHighlightedTradeId(tradeId);
      scrollToTradeRow(tradeId);
    },
    [pairIndex, scrollToTradeRow],
  );

  // Auto-refresh data every 5 seconds
  useEffect(() => {
    const interval = setInterval(() => {
      if (document.hidden) return;
      if (!priceStreamConnectedRef.current) loadData(true);
    }, 5000);
    return () => clearInterval(interval);
  }, [loadData]);

  // Trades are independent of chart refresh. Keep the Positions table up to date even when
  // the price SSE connection is healthy (which disables chart polling).
  useEffect(() => {
    const interval = setInterval(() => {
      if (document.hidden) return;
      void refreshTrades();
    }, 5000);
    return () => clearInterval(interval);
  }, [refreshTrades]);

  // Reload history when pair/resolution changes
  useEffect(() => {
    setHighlightedTradeId(null);
    setData([]);
    void loadData(false);
  }, [pairIndex, resolution, loadData]);

  useEffect(() => {
    if (!pendingTradeFocus) return;
    if (pendingTradeFocus.pairIndex !== pairIndex) return;

    let cancelled = false;
    const startedAt = Date.now();

    const attempt = () => {
      if (cancelled) return;
      setHighlightedTradeId(pendingTradeFocus.tradeId);
      const found = scrollToTradeRow(pendingTradeFocus.tradeId);
      if (found || Date.now() - startedAt > 2500) {
        setPendingTradeFocus(null);
        return;
      }
      window.setTimeout(attempt, 150);
    };

    attempt();
    return () => {
      cancelled = true;
    };
  }, [pairIndex, pendingTradeFocus, scrollToTradeRow]);

  // Realtime price stream (fallbacks to polling if disconnected)
  useEffect(() => {
    const url = `${API_BASE}/prices/stream?pairIndex=${pairIndex}&throttleMs=250`;
    const eventSource = new EventSource(url);
    priceStreamRef.current = eventSource;
    priceStreamConnectedRef.current = false;

    eventSource.onopen = () => {
      priceStreamConnectedRef.current = true;
    };

    eventSource.onmessage = (event) => {
      if (document.hidden) return;
      try {
        const parsed = JSON.parse(event.data) as { price?: number; ts?: number };
        if (typeof parsed?.price === 'number') {
          applyLivePrice(parsed.price, typeof parsed.ts === 'number' ? parsed.ts : Date.now());
        }
      } catch {
        // ignore malformed messages
      }
    };

    eventSource.onerror = () => {
      priceStreamConnectedRef.current = false;
      if (eventSource.readyState === EventSource.CLOSED && priceStreamRef.current === eventSource) {
        priceStreamRef.current = null;
      }
    };

    return () => {
      priceStreamConnectedRef.current = false;
      try {
        eventSource.close();
      } catch {
        // ignore
      }
      if (priceStreamRef.current === eventSource) {
        priceStreamRef.current = null;
      }
    };
  }, [applyLivePrice, pairIndex]);

  // If the tab was hidden, resync once when it becomes visible again.
  useEffect(() => {
    const handler = () => {
      if (!document.hidden) {
        void loadData(true);
        void refreshTrades();
      }
    };
    document.addEventListener('visibilitychange', handler);
    return () => document.removeEventListener('visibilitychange', handler);
  }, [loadData, refreshTrades]);

  const currentPrice = data.length > 0 ? data[data.length - 1].close : 0;
  const tradesForPair = trades.filter((trade) => trade.pair_index === pairIndex);

  const pnlHistory = useMemo(() => {
    const closed = trades
      .filter((trade) => trade.status === 'CLOSED' && typeof trade.exit_time === 'number')
      .slice()
      .sort((a, b) => (a.exit_time ?? 0) - (b.exit_time ?? 0));

    let cumulative = 0;
    const series: Array<{ time: number; value: number }> = [];
    for (const trade of closed) {
      const gross = trade.pnl ?? 0;
      const net = gross - calculateOpeningFee(trade);
      cumulative += net;

      const t = trade.exit_time as number;
      const prev = series[series.length - 1];
      if (prev && prev.time === t) {
        prev.value = cumulative;
      } else {
        series.push({ time: t, value: cumulative });
      }
    }

    // Keep the header lightweight.
    const MAX_POINTS = 60;
    return series.length > MAX_POINTS ? series.slice(-MAX_POINTS) : series;
  }, [trades]);

  const latestPnl = pnlHistory.length > 0 ? pnlHistory[pnlHistory.length - 1].value : null;
  const pnlStroke = typeof latestPnl === 'number' && latestPnl < 0 ? 'var(--color-down)' : 'var(--color-up)';

  const unrealizedNetPnl = useMemo(() => {
    if (!Number.isFinite(currentPrice) || currentPrice <= 0) return null;

    let sum = 0;
    let count = 0;
    for (const trade of trades) {
      if (trade.status !== 'OPEN') continue;
      if (trade.pair_index !== pairIndex) continue;
      if (!Number.isFinite(trade.entry_price) || trade.entry_price <= 0) continue;

      let pnlPct = 0;
      if (trade.direction === 'LONG') {
        pnlPct = (currentPrice - trade.entry_price) / trade.entry_price;
      } else {
        pnlPct = (trade.entry_price - currentPrice) / trade.entry_price;
      }

      const gross = pnlPct * trade.collateral * trade.leverage;
      const net = gross - calculateOpeningFee(trade);
      sum += net;
      count += 1;
    }

    return { value: sum, count };
  }, [currentPrice, pairIndex, trades]);

  const totalNetPnl =
    typeof latestPnl === 'number' && unrealizedNetPnl
      ? latestPnl + unrealizedNetPnl.value
      : null;

  const pnlSparkline = useMemo(() => {
    if (pnlHistory.length < 2) return null;

    const width = 140;
    const height = 26;
    const pad = 2;

    const values = pnlHistory.map((p) => p.value);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const range = max - min;

    const toX = (idx: number) => {
      if (pnlHistory.length === 1) return pad;
      const t = idx / (pnlHistory.length - 1);
      return pad + t * (width - pad * 2);
    };

    const toY = (value: number) => {
      if (range <= 0) return height / 2;
      const t = (value - min) / range;
      return pad + (1 - t) * (height - pad * 2);
    };

    let d = '';
    for (let i = 0; i < pnlHistory.length; i += 1) {
      const x = toX(i);
      const y = toY(pnlHistory[i].value);
      d += i === 0 ? `M ${x} ${y}` : ` L ${x} ${y}`;
    }

    return { width, height, d };
  }, [pnlHistory]);

  return (
    <div className="app-container">
      <header>
        <div className="header-left">
          <h1>Zimmer Auto Perps Trader</h1>
        </div>

        <div className="header-center" aria-label="Cumulative realized PnL over time">
          <div
            className="header-pnl"
            onMouseEnter={(event) => {
              const rect = event.currentTarget.getBoundingClientRect();
              setPnlTooltipAnchor({ top: rect.top, left: rect.left, width: rect.width, height: rect.height });
            }}
            onMouseLeave={() => setPnlTooltipAnchor(null)}
          >
            <span className="header-pnl-label">PnL</span>
            <span
              className={`header-pnl-value ${typeof latestPnl === 'number' ? (latestPnl < 0 ? 'pnl-red' : 'pnl-green') : ''}`.trim()}
              title={typeof latestPnl === 'number' ? `Cumulative realized net PnL: ${formatUsd(latestPnl)}` : 'No closed trades yet'}
            >
              {typeof latestPnl === 'number' ? formatUsd(latestPnl) : '—'}
            </span>

            {pnlSparkline ? (
              <svg
                className="header-pnl-sparkline"
                width={pnlSparkline.width}
                height={pnlSparkline.height}
                viewBox={`0 0 ${pnlSparkline.width} ${pnlSparkline.height}`}
                role="img"
                aria-label="PnL sparkline"
              >
                <path
                  d={pnlSparkline.d}
                  fill="none"
                  stroke={pnlStroke}
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            ) : (
              <div className="header-pnl-sparkline-placeholder" aria-hidden="true" />
            )}
          </div>
        </div>

        <div className="header-right">
          <button
            type="button"
            className="theme-toggle"
            onClick={toggleTheme}
            aria-pressed={darkMode}
          >
            {darkMode ? 'Switch to light mode' : 'Switch to dark mode'}
          </button>
        </div>
      </header>

      {pnlTooltipAnchor
        ? createPortal(
          <div
            className="header-pnl-tooltip"
            style={{
              top: pnlTooltipAnchor.top + pnlTooltipAnchor.height + 10,
              left: pnlTooltipAnchor.left + pnlTooltipAnchor.width / 2,
            }}
            role="tooltip"
            aria-label="PnL breakdown"
          >
            <div className="header-pnl-tooltip-row">
              <span>Realized (net):</span>
              <span className={typeof latestPnl === 'number' ? (latestPnl < 0 ? 'pnl-red' : 'pnl-green') : ''}>
                {typeof latestPnl === 'number' ? formatUsd(latestPnl) : '—'}
              </span>
            </div>
            <div className="header-pnl-tooltip-row">
              <span>Unrealized (net):</span>
              <span
                className={unrealizedNetPnl ? (unrealizedNetPnl.value < 0 ? 'pnl-red' : 'pnl-green') : ''}
                title="Computed from OPEN trades on the selected pair"
              >
                {unrealizedNetPnl ? formatUsd(unrealizedNetPnl.value) : '—'}
              </span>
            </div>
            {unrealizedNetPnl ? (
              <div className="header-pnl-tooltip-sub">Open trades (selected pair): {unrealizedNetPnl.count}</div>
            ) : null}
            <div className="header-pnl-tooltip-divider" />
            <div className="header-pnl-tooltip-row header-pnl-tooltip-total">
              <span>Total:</span>
              <span className={typeof totalNetPnl === 'number' ? (totalNetPnl < 0 ? 'pnl-red' : 'pnl-green') : ''}>
                {typeof totalNetPnl === 'number' ? formatUsd(totalNetPnl) : '—'}
              </span>
            </div>
          </div>,
          document.body,
        )
        : null}

      {error && (
        <div className="page-error-banner" role="alert" aria-live="assertive">
          Chart failed to load: {error}
        </div>
      )}

      <div className="main-content">
        <Controls
          pairIndex={pairIndex}
          setPairIndex={setPairIndex}
          resolution={resolution}
          setResolution={setResolution}
          onTrade={handleTrade}
        />

        <div className="trading-area">
          <div className="chart-wrapper">
            {loading && data.length === 0 && <div className="loading-overlay">Loading...</div>}
            <div className="chart-area">
              <Chart
                viewKey={`${pairIndex}:${resolution}`}
                pairIndex={pairIndex}
                data={data}
                trades={tradesForPair}
                highlightedTradeId={highlightedTradeId}
                onMarkerHover={handleMarkerHover}
                onMarkerClick={handleMarkerClick}
              />
            </div>
            <Positions
              trades={trades}
              currentPrice={currentPrice}
              selectedPairIndex={pairIndex}
              onClose={handleCloseTrade}
              onCancel={handleCancelTrade}
              highlightedTradeId={highlightedTradeId}
              onRowHover={handleMarkerHover}
              onRowClick={handleDecisionFocusTrade}
            />
          </div>
          <BotPanel pairIndex={pairIndex} onFocusTrade={handleDecisionFocusTrade} />
        </div>
      </div>
    </div>
  );
}
export default App;
