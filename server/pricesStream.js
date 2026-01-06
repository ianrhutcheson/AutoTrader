const DEFAULT_WS_URL = 'wss://backend-pricing.eu.gains.trade';
const DEFAULT_THROTTLE_MS = 250;
const MIN_THROTTLE_MS = 50;
const MAX_THROTTLE_MS = 5_000;

const WebSocket = require('ws');

const RECONNECT_BASE_MS = 500;
const RECONNECT_MAX_MS = 30_000;

const priceSubscribersByPair = new Map(); // pairIndex -> Set<SseClient>
const priceObservers = new Set(); // Set<(pairIndex:number, price:number, tsMs:number) => void>
const lastPriceByPair = new Map(); // pairIndex -> { price:number, tsMs:number }
let ws = null;
let reconnectTimer = null;
let reconnectDelayMs = RECONNECT_BASE_MS;

function normalizeNumber(value) {
    const num = typeof value === 'number' ? value : Number(value);
    return Number.isFinite(num) ? num : null;
}

function clampInt(value, { min, max }) {
    if (!Number.isFinite(value)) return null;
    const intValue = Math.trunc(value);
    return Math.max(min, Math.min(max, intValue));
}

function getWsUrl() {
    return process.env.GAINS_PRICE_STREAM_WS_URL || DEFAULT_WS_URL;
}

function clearReconnectTimer() {
    if (!reconnectTimer) return;
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
}

function scheduleReconnect() {
    if (reconnectTimer) return;
    reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        connectWsIfNeeded();
    }, reconnectDelayMs);

    reconnectDelayMs = Math.min(reconnectDelayMs * 2, RECONNECT_MAX_MS);
}

function dataToString(data) {
    if (typeof data === 'string') return data;
    if (data instanceof ArrayBuffer) return Buffer.from(data).toString('utf8');
    if (ArrayBuffer.isView(data)) {
        return Buffer.from(data.buffer, data.byteOffset, data.byteLength).toString('utf8');
    }
    return null;
}

function handleWsMessage(rawData) {
    const message = dataToString(rawData);
    if (!message) return;

    let payload;
    try {
        payload = JSON.parse(message);
    } catch {
        return;
    }

    if (!Array.isArray(payload)) return;

    const nowMs = Date.now();
    if (payload.length === 1) {
        // Ping message: [timestampMs]
        return;
    }

    for (let i = 0; i < payload.length - 1; i += 2) {
        const pairIndex = normalizeNumber(payload[i]);
        const price = normalizeNumber(payload[i + 1]);
        if (pairIndex === null || price === null) continue;

        lastPriceByPair.set(pairIndex, { price, tsMs: nowMs });

        const subs = priceSubscribersByPair.get(pairIndex);
        if (subs && subs.size > 0) {
            for (const client of subs) {
                client.onPrice(price, nowMs);
            }
        }

        if (priceObservers.size > 0) {
            for (const observer of priceObservers) {
                try {
                    observer(pairIndex, price, nowMs);
                } catch (err) {
                    console.warn('[pricesStream] Price observer error:', err?.message || err);
                }
            }
        }
    }
}

function connectWsIfNeeded() {
    const hasSubscribers =
        priceObservers.size > 0 ||
        Array.from(priceSubscribersByPair.values()).some(set => set.size > 0);
    if (!hasSubscribers) return;
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;

    const url = getWsUrl();
    clearReconnectTimer();

    try {
        ws = new WebSocket(url);
    } catch (err) {
        console.error('[pricesStream] Failed to create WebSocket:', err?.message || err);
        scheduleReconnect();
        return;
    }

    ws.on('open', () => {
        reconnectDelayMs = RECONNECT_BASE_MS;
    });

    ws.on('message', (data) => {
        handleWsMessage(data);
    });

    ws.on('error', (err) => {
        console.warn('[pricesStream] WebSocket error:', err?.message || err);
        try {
            ws?.close();
        } catch {
            // ignore
        }
    });

    ws.on('close', () => {
        ws = null;
        const hasSubscribers =
            priceObservers.size > 0 ||
            Array.from(priceSubscribersByPair.values()).some(set => set.size > 0);
        if (hasSubscribers) scheduleReconnect();
    });
}

function closeWsIfIdle() {
    const hasSubscribers =
        priceObservers.size > 0 ||
        Array.from(priceSubscribersByPair.values()).some(set => set.size > 0);
    if (hasSubscribers) return;

    clearReconnectTimer();
    reconnectDelayMs = RECONNECT_BASE_MS;

    if (!ws) return;
    try {
        ws.close();
    } catch {
        // ignore
    }
    ws = null;
}

function createSseClient(res, throttleMs) {
    const client = {
        lastSentAtMs: 0,
        pending: null,
        timer: null,
        keepAliveTimer: null,
        onPrice(price, tsMs) {
            this.pending = { price, tsMs };
            this.flushMaybe();
        },
        flushMaybe() {
            if (!this.pending) return;

            const now = Date.now();
            const elapsed = now - this.lastSentAtMs;
            const remaining = throttleMs - elapsed;

            if (remaining <= 0) {
                this.flush();
                return;
            }

            if (this.timer) return;
            this.timer = setTimeout(() => {
                this.timer = null;
                this.flush();
            }, remaining);
        },
        flush() {
            if (!this.pending) return;
            const { price, tsMs } = this.pending;
            this.pending = null;

            try {
                res.write(`data: ${JSON.stringify({ price, ts: tsMs })}\n\n`);
                this.lastSentAtMs = Date.now();
            } catch {
                // Client likely disconnected; upstream will remove via req.close handler.
            }
        },
        startKeepAlive() {
            if (this.keepAliveTimer) return;
            this.keepAliveTimer = setInterval(() => {
                try {
                    res.write(`: ping\n\n`);
                } catch {
                    // ignore
                }
            }, 15_000);
        },
        stop() {
            if (this.timer) clearTimeout(this.timer);
            this.timer = null;
            if (this.keepAliveTimer) clearInterval(this.keepAliveTimer);
            this.keepAliveTimer = null;
        }
    };

    client.startKeepAlive();
    return client;
}

function subscribe(pairIndex, client) {
    const set = priceSubscribersByPair.get(pairIndex) || new Set();
    set.add(client);
    priceSubscribersByPair.set(pairIndex, set);
    connectWsIfNeeded();
}

function unsubscribe(pairIndex, client) {
    const set = priceSubscribersByPair.get(pairIndex);
    if (set) {
        set.delete(client);
        if (set.size === 0) {
            priceSubscribersByPair.delete(pairIndex);
        }
    }
    client.stop();
    closeWsIfIdle();
}

function registerPriceStreamRoutes(app) {
    app.get('/api/prices/stream', (req, res) => {
        const pairIndex = clampInt(normalizeNumber(req.query.pairIndex), { min: 0, max: Number.MAX_SAFE_INTEGER });
        if (pairIndex === null) {
            return res.status(400).json({ error: 'pairIndex query param is required' });
        }

        const throttleCandidate = clampInt(normalizeNumber(req.query.throttleMs), { min: MIN_THROTTLE_MS, max: MAX_THROTTLE_MS });
        const throttleMs = throttleCandidate ?? DEFAULT_THROTTLE_MS;

        res.setHeader('Content-Type', 'text/event-stream');
        res.setHeader('Cache-Control', 'no-cache, no-transform');
        res.setHeader('Connection', 'keep-alive');
        res.flushHeaders?.();
        res.write('retry: 1000\n\n');

        const client = createSseClient(res, throttleMs);
        subscribe(pairIndex, client);

        req.on('close', () => {
            unsubscribe(pairIndex, client);
        });
    });
}

function getLastPrice(pairIndex) {
    const entry = lastPriceByPair.get(pairIndex);
    return entry ? { ...entry } : null;
}

function addPriceObserver(observer) {
    if (typeof observer !== 'function') return () => { };
    priceObservers.add(observer);
    connectWsIfNeeded();
    return () => {
        priceObservers.delete(observer);
        closeWsIfIdle();
    };
}

module.exports = { registerPriceStreamRoutes, getLastPrice, addPriceObserver };
