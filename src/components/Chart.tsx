import { useCallback, useEffect, useRef } from 'react';
import { createChart, ColorType, LineStyle, CandlestickSeries, createSeriesMarkers } from 'lightweight-charts';
import type { CandlestickData, IChartApi, IPriceLine, ISeriesApi, ISeriesMarkersPluginApi, SeriesMarker, Time } from 'lightweight-charts';
import type { ChartDataPoint, Trade } from '../services/api';

interface ChartProps {
    viewKey: string;
    pairIndex: number;
    data: ChartDataPoint[];
    trades: Trade[];
    highlightedTradeId: number | null;
    onMarkerHover: (tradeId: number | null, markerType: 'entry' | 'exit' | null) => void;
    onMarkerClick: (tradeId: number) => void;
}

const toCandle = (point: ChartDataPoint): CandlestickData<Time> => ({
    time: point.time as Time,
    open: point.open,
    high: point.high,
    low: point.low,
    close: point.close,
});

const readCssVar = (name: string, fallback: string): string => {
    if (typeof window === 'undefined') return fallback;
    const value = window.getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    return value || fallback;
};

const buildMarkers = (
    nextTrades: Trade[],
    highlightedId: number | null,
    colors: { up: string; down: string; warning: string; highlight: string },
): SeriesMarker<Time>[] => {
    const markers: SeriesMarker<Time>[] = [];

    nextTrades.forEach(trade => {
        const isHighlighted = highlightedId === trade.id;

        if (trade.status === 'CANCELED') {
            return;
        }

        if (trade.status === 'PENDING') {
            markers.push({
                time: trade.entry_time as Time,
                position: trade.direction === 'LONG' ? 'belowBar' : 'aboveBar',
                color: isHighlighted ? colors.highlight : colors.warning,
                shape: 'circle',
                text: `Pending #${trade.id} ${trade.direction}`,
                size: isHighlighted ? 2 : 1,
            });
            return;
        }

        // Entry marker
        markers.push({
            time: trade.entry_time as Time,
            position: trade.direction === 'LONG' ? 'belowBar' : 'aboveBar',
            color: isHighlighted ? colors.highlight : (trade.direction === 'LONG' ? colors.up : colors.down),
            shape: trade.direction === 'LONG' ? 'arrowUp' : 'arrowDown',
            text: `#${trade.id} ${trade.direction}`,
            size: isHighlighted ? 2 : 1,
        });

        // Exit marker (only for closed trades)
        if (trade.status === 'CLOSED' && trade.exit_time) {
            const pnl = trade.pnl ?? 0;
            markers.push({
                time: trade.exit_time as Time,
                position: trade.direction === 'LONG' ? 'aboveBar' : 'belowBar',
                color: isHighlighted ? colors.highlight : (pnl >= 0 ? colors.up : colors.down),
                shape: 'circle',
                text: `Close #${trade.id}`,
                size: isHighlighted ? 2 : 1,
            });
        }
    });

    // Sort by time
    markers.sort((a, b) => (a.time as number) - (b.time as number));
    return markers;
};

export const Chart = ({ viewKey, pairIndex, data, trades, highlightedTradeId, onMarkerHover, onMarkerClick }: ChartProps) => {
    const chartContainerRef = useRef<HTMLDivElement>(null);
    const chartRef = useRef<IChartApi | null>(null);
    const seriesRef = useRef<ISeriesApi<"Candlestick", Time> | null>(null);
    const markersPluginRef = useRef<ISeriesMarkersPluginApi<Time> | null>(null);
    const stopLossLineRef = useRef<IPriceLine | null>(null);
    const takeProfitLineRef = useRef<IPriceLine | null>(null);
    const previousDataRef = useRef<ChartDataPoint[]>([]);
    const followLatestRef = useRef(true);

    const tradesRef = useRef(trades.filter((trade) => trade.pair_index === pairIndex));
    const highlightedTradeIdRef = useRef(highlightedTradeId);
    const onMarkerHoverRef = useRef(onMarkerHover);
    const onMarkerClickRef = useRef(onMarkerClick);

    useEffect(() => {
        tradesRef.current = trades.filter((trade) => trade.pair_index === pairIndex);
    }, [pairIndex, trades]);

    useEffect(() => {
        highlightedTradeIdRef.current = highlightedTradeId;
    }, [highlightedTradeId]);

    useEffect(() => {
        onMarkerHoverRef.current = onMarkerHover;
    }, [onMarkerHover]);

    useEffect(() => {
        onMarkerClickRef.current = onMarkerClick;
    }, [onMarkerClick]);

    useEffect(() => {
        if (!chartContainerRef.current) return;

        const chartBg = readCssVar('--chart-bg', '#ffffff');
        const chartText = readCssVar('--chart-text', '#1a1a1a');
        const chartGrid = readCssVar('--chart-grid', 'rgba(26, 26, 26, 0.06)');
        const chartBorder = readCssVar('--chart-border', 'rgba(26, 26, 26, 0.12)');
        const up = readCssVar('--color-up', '#16a34a');
        const down = readCssVar('--color-down', '#dc2626');

        const chart = createChart(chartContainerRef.current, {
            layout: {
                background: { type: ColorType.Solid, color: chartBg },
                textColor: chartText,
            },
            grid: {
                vertLines: { color: chartGrid },
                horzLines: { color: chartGrid },
            },
            width: chartContainerRef.current.clientWidth,
            height: chartContainerRef.current.clientHeight,
            timeScale: {
                borderColor: chartBorder,
                timeVisible: true,
                secondsVisible: false,
            },
            rightPriceScale: {
                borderColor: chartBorder,
            },
        });

        const newSeries = chart.addSeries(CandlestickSeries, {
            upColor: up,
            downColor: down,
            borderVisible: false,
            wickUpColor: up,
            wickDownColor: down,
        });

        chartRef.current = chart;
        seriesRef.current = newSeries;
        markersPluginRef.current = createSeriesMarkers(newSeries);

        const handleChartClick = (param: { time?: Time }) => {
            if (!param.time || typeof param.time !== 'number') return;
            const clickTime = param.time;
            const matchingTrade = tradesRef.current.find(
                t => t.entry_time === clickTime || t.exit_time === clickTime
            );
            if (matchingTrade) {
                onMarkerClickRef.current(matchingTrade.id);
            }
        };

        const handleCrosshairMove = (param: { time?: Time }) => {
            if (!param.time || typeof param.time !== 'number') {
                onMarkerHoverRef.current(null, null);
                return;
            }

            const hoverTime = param.time;
            for (const trade of tradesRef.current) {
                if (trade.entry_time === hoverTime) {
                    onMarkerHoverRef.current(trade.id, 'entry');
                    return;
                }
                if (trade.exit_time === hoverTime) {
                    onMarkerHoverRef.current(trade.id, 'exit');
                    return;
                }
            }
            onMarkerHoverRef.current(null, null);
        };

        const handleVisibleLogicalRangeChange = () => {
            const currentChart = chartRef.current;
            if (!currentChart) return;
            const scrollPosition = currentChart.timeScale().scrollPosition();
            followLatestRef.current = scrollPosition <= 0;
        };

        chart.subscribeClick(handleChartClick);
        chart.subscribeCrosshairMove(handleCrosshairMove);
        chart.timeScale().subscribeVisibleLogicalRangeChange(handleVisibleLogicalRangeChange);

        const resizeObserver = new ResizeObserver(entries => {
            if (entries.length === 0 || !entries[0].target) return;
            const newRect = entries[0].contentRect;
            chart.applyOptions({ width: newRect.width, height: newRect.height });
        });

        resizeObserver.observe(chartContainerRef.current);

        return () => {
            resizeObserver.disconnect();
            chart.unsubscribeClick(handleChartClick);
            chart.unsubscribeCrosshairMove(handleCrosshairMove);
            chart.timeScale().unsubscribeVisibleLogicalRangeChange(handleVisibleLogicalRangeChange);
            chart.remove();
        };
    }, []);

    useEffect(() => {
        followLatestRef.current = true;
        chartRef.current?.timeScale().scrollToPosition(0, false);
    }, [viewKey]);

    const clearPriceLines = useCallback(() => {
        const series = seriesRef.current;
        if (!series) return;

        if (stopLossLineRef.current) {
            try {
                series.removePriceLine(stopLossLineRef.current);
            } catch {
                // ignore
            }
            stopLossLineRef.current = null;
        }

        if (takeProfitLineRef.current) {
            try {
                series.removePriceLine(takeProfitLineRef.current);
            } catch {
                // ignore
            }
            takeProfitLineRef.current = null;
        }
    }, []);

    // Draw TP/SL price lines for the highlighted trade (if any).
    useEffect(() => {
        const series = seriesRef.current;
        if (!series) return;

        const up = readCssVar('--color-up', '#16a34a');
        const down = readCssVar('--color-down', '#dc2626');

        clearPriceLines();

        if (highlightedTradeId === null) return;
        const trade = trades.find((t) => t.id === highlightedTradeId && t.pair_index === pairIndex) ?? null;
        if (!trade) return;

        if (typeof trade.stop_loss_price === 'number' && Number.isFinite(trade.stop_loss_price)) {
            stopLossLineRef.current = series.createPriceLine({
                price: trade.stop_loss_price,
                color: down,
                lineWidth: 2,
                lineStyle: LineStyle.Dashed,
                axisLabelVisible: true,
                title: 'SL',
            });
        }

        if (typeof trade.take_profit_price === 'number' && Number.isFinite(trade.take_profit_price)) {
            takeProfitLineRef.current = series.createPriceLine({
                price: trade.take_profit_price,
                color: up,
                lineWidth: 2,
                lineStyle: LineStyle.Dashed,
                axisLabelVisible: true,
                title: 'TP',
            });
        }

        return () => {
            clearPriceLines();
        };
    }, [pairIndex, trades, highlightedTradeId, clearPriceLines]);

    // Update series data (avoid resetting chart on hover/marker changes)
    useEffect(() => {
        if (!seriesRef.current) return;

        const prevData = previousDataRef.current;
        const nextData = data;

        try {
            if (nextData.length === 0) {
                seriesRef.current.setData([]);
                previousDataRef.current = [];
                return;
            }

            if (
                prevData.length > 0 &&
                nextData.length >= prevData.length &&
                nextData[0]?.time === prevData[0]?.time
            ) {
                const checkFrom = Math.max(0, prevData.length - 5);
                let aligned = true;
                for (let i = checkFrom; i < prevData.length; i += 1) {
                    if (nextData[i]?.time !== prevData[i]?.time) {
                        aligned = false;
                        break;
                    }
                }

                if (aligned) {
                    const startIndex = Math.max(0, prevData.length - 1);
                    for (let i = startIndex; i < nextData.length; i += 1) {
                        seriesRef.current.update(toCandle(nextData[i]));
                    }

                    if (followLatestRef.current) {
                        chartRef.current?.timeScale().scrollToPosition(0, false);
                    }

                    previousDataRef.current = nextData;
                    return;
                }
            }

            seriesRef.current.setData(nextData.map(toCandle));
            if (followLatestRef.current) {
                chartRef.current?.timeScale().scrollToPosition(0, false);
            } else if (prevData.length === 0) {
                chartRef.current?.timeScale().fitContent();
            }
            previousDataRef.current = nextData;
        } catch (err) {
            console.error("Error setting data:", err);
        }
    }, [data]);

    // Update markers independently for smooth hovering/refresh
    useEffect(() => {
        if (!markersPluginRef.current) return;

        try {
            const colors = {
                up: readCssVar('--color-up', '#16a34a'),
                down: readCssVar('--color-down', '#dc2626'),
                warning: readCssVar('--color-warning', '#f59e0b'),
                highlight: '#fbbf24',
            };
            const markers = buildMarkers(
                trades.filter((trade) => trade.pair_index === pairIndex),
                highlightedTradeId,
                colors,
            );
            markersPluginRef.current.setMarkers(markers);
        } catch (err) {
            console.error("Error setting markers:", err);
        }
    }, [pairIndex, trades, highlightedTradeId]);

    return <div ref={chartContainerRef} style={{ width: '100%', height: '100%' }} />;
};
