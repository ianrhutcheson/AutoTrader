const DEFAULT_BASE_URL = 'https://api.symphony.io';

function normalizeBaseUrl(value) {
    const raw = typeof value === 'string' ? value.trim() : '';
    if (!raw) return DEFAULT_BASE_URL;
    return raw.endsWith('/') ? raw.slice(0, -1) : raw;
}

function buildUrl(baseUrl, path, query) {
    const url = new URL(`${normalizeBaseUrl(baseUrl)}${path.startsWith('/') ? path : `/${path}`}`);
    if (query && typeof query === 'object') {
        for (const [key, value] of Object.entries(query)) {
            if (value === undefined || value === null) continue;
            url.searchParams.set(key, String(value));
        }
    }
    return url.toString();
}

async function readJsonSafe(response) {
    const text = await response.text();
    if (!text) return { data: null, text: '' };
    try {
        return { data: JSON.parse(text), text };
    } catch {
        return { data: null, text };
    }
}

async function symphonyRequest({ apiKey, baseUrl, method, path, query, body, timeoutMs = 15_000 }) {
    const url = buildUrl(baseUrl, path, query);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    try {
        const response = await fetch(url, {
            method,
            headers: {
                'content-type': 'application/json',
                'x-api-key': apiKey
            },
            body: body ? JSON.stringify(body) : undefined,
            signal: controller.signal
        });

        const { data, text } = await readJsonSafe(response);

        if (!response.ok) {
            const err = new Error(`Symphony request failed (${response.status})`);
            err.status = response.status;
            err.details = { url, method, path, query: query ?? null, body: body ?? null, response: data ?? text ?? null };
            throw err;
        }

        return { status: response.status, data };
    } finally {
        clearTimeout(timer);
    }
}

function getSymphonyConfigFromEnv() {
    const apiKey = typeof process.env.SYMPHONY_API_KEY === 'string' ? process.env.SYMPHONY_API_KEY.trim() : '';
    const agentId = typeof process.env.SYMPHONY_AGENT_ID === 'string' ? process.env.SYMPHONY_AGENT_ID.trim() : '';
    const baseUrl = typeof process.env.SYMPHONY_BASE_URL === 'string' ? process.env.SYMPHONY_BASE_URL.trim() : DEFAULT_BASE_URL;
    return { apiKey, agentId, baseUrl: normalizeBaseUrl(baseUrl) };
}

async function batchOpen({ agentId, apiKey, baseUrl, symbol, action, weight, leverage, orderOptions }) {
    return symphonyRequest({
        apiKey,
        baseUrl,
        method: 'POST',
        path: '/agent/batch-open',
        body: {
            agentId,
            symbol,
            action,
            weight,
            leverage,
            ...(orderOptions ? { orderOptions } : {})
        }
    });
}

async function batchClose({ agentId, apiKey, baseUrl, batchId }) {
    return symphonyRequest({
        apiKey,
        baseUrl,
        method: 'POST',
        path: '/agent/batch-close',
        body: {
            agentId,
            batchId
        }
    });
}

async function getAgentBatches({ agentId, apiKey, baseUrl }) {
    return symphonyRequest({
        apiKey,
        baseUrl,
        method: 'GET',
        path: '/agent/batches',
        query: { agentId },
        body: null
    });
}

async function getBatchPositions({ batchId, apiKey, baseUrl }) {
    return symphonyRequest({
        apiKey,
        baseUrl,
        method: 'GET',
        path: '/agent/batch-positions',
        query: { batchId },
        body: null
    });
}

module.exports = {
    DEFAULT_BASE_URL,
    getSymphonyConfigFromEnv,
    batchOpen,
    batchClose,
    getAgentBatches,
    getBatchPositions
};

