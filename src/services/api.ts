import { apiUrl } from './apiBase';

export type ChartDataPoint = {
    time: number;
    open: number;
    high: number;
    low: number;
    close: number;
};

interface ApiResponse {
    table: {
        time: number;
        open: number;
        high: number;
        low: number;
        close: number;
    }[];
}

const BASE_URL = 'https://backend-pricing.eu.gains.trade/charts';

export type FetchChartDataOptions = {
    signal?: AbortSignal;
};

export const fetchChartData = async (
    pairIndex: number,
    from: number,
    to: number,
    resolution: string, // e.g., "15"
    options: FetchChartDataOptions = {}
): Promise<ChartDataPoint[]> => {
    const url = `${BASE_URL}/${pairIndex}/${from}/${to}/${resolution}`;
    const response = await fetch(url, { signal: options.signal });

    if (!response.ok) {
        throw new Error(`Error fetching data: ${response.statusText}`);
    }

    const data: ApiResponse = await response.json();

    if (!data.table) {
        return [];
    }

    return data.table.map((item) => ({
        time: Math.floor(item.time / 1000), // Convert ms to whole seconds for Lightweight Charts
        open: item.open,
        high: item.high,
        low: item.low,
        close: item.close,
    }));
};

const TRADING_API_URL = apiUrl('/trades');

export interface Trade {
    id: number;
    pair_index: number;
    entry_price: number;
    trigger_price?: number | null;
    triggered_price?: number | null;
    exit_price?: number;
    stop_loss_price?: number | null;
    take_profit_price?: number | null;
    entry_time: number;
    triggered_time?: number | null;
    exit_time?: number;
    status: 'OPEN' | 'CLOSED' | 'PENDING' | 'CANCELED';
    pnl?: number;
    collateral: number;
    leverage: number;
    direction: 'LONG' | 'SHORT';
    source?: 'MANUAL' | 'USER' | 'BOT';
}

export const getTrades = async (): Promise<Trade[]> => {
    const response = await fetch(TRADING_API_URL);
    if (!response.ok) throw new Error('Failed to fetch trades');
    return response.json();
};

export type CreateTradeRequest = Omit<Trade, 'id' | 'status' | 'pnl'> & {
    trigger_price?: number | null;
};

export const createTrade = async (trade: CreateTradeRequest): Promise<Trade> => {
    const tradeWithSource = { ...trade, source: trade.source || 'MANUAL' };
    const response = await fetch(TRADING_API_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(tradeWithSource),
    });
    if (!response.ok) throw new Error('Failed to create trade');
    return response.json();
};

export const closeTrade = async (id: number, exitData: { exit_price: number; exit_time: number }): Promise<Trade> => {
    const response = await fetch(`${TRADING_API_URL}/${id}/close`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(exitData),
    });
    if (!response.ok) throw new Error('Failed to close trade');
    return response.json();
};

export const cancelTrade = async (id: number): Promise<Trade> => {
    const response = await fetch(`${TRADING_API_URL}/${id}/cancel`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
    });
    if (!response.ok) throw new Error('Failed to cancel trade');
    return response.json();
};
