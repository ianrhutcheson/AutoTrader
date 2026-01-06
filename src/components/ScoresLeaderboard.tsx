import { useEffect, useMemo, useRef, useState } from 'react';
import { API_BASE } from '../services/apiBase';
import './ScoresLeaderboard.css';

type LeaderboardCandidate = {
  pair_index: number;
  symbol: string | null;
  timeframe_min: number;
  candle_time: number;
  price: number;
  side: 'LONG' | 'SHORT';
  score: number;
  reasons?: string[];
};

type LeaderboardStreamPayload = {
  timeframeMin: number;
  limit: number;
  scanned: number;
  candidates: LeaderboardCandidate[];
  ts: number;
};

type LeaderboardPollPayload = {
  timeframeMin: number;
  limit: number;
  scanned: number;
  candidates: LeaderboardCandidate[];
};

type Props = {
  currentPairIndex: number;
  onSelectPairIndex: (pairIndex: number) => void;
};

export function ScoresLeaderboard({ currentPairIndex, onSelectPairIndex }: Props) {
  const [timeframeMin, setTimeframeMin] = useState(15);
  const [limit, setLimit] = useState(10);
  const [connected, setConnected] = useState(false);
  const [mode, setMode] = useState<'sse' | 'polling'>('sse');
  const [serverError, setServerError] = useState<string | null>(null);
  const [lastUpdatedTs, setLastUpdatedTs] = useState<number | null>(null);
  const [candidates, setCandidates] = useState<LeaderboardCandidate[]>([]);
  const [scanned, setScanned] = useState<number | null>(null);
  const [nowTs, setNowTs] = useState(() => Date.now());
  const openedRef = useRef(false);

  const handleTimeframeChange = (next: number) => {
    setTimeframeMin(next);
    setConnected(false);
    setMode('sse');
    setServerError(null);
  };

  const handleLimitChange = (next: number) => {
    setLimit(next);
    setConnected(false);
    setMode('sse');
    setServerError(null);
  };

  const url = useMemo(() => {
    const params = new URLSearchParams({
      timeframeMin: String(timeframeMin),
      limit: String(limit),
      intervalMs: '2000',
    });
    return `${API_BASE}/market/opportunities/stream?${params.toString()}`;
  }, [limit, timeframeMin]);

  useEffect(() => {
    openedRef.current = false;
    const es = new EventSource(url);

    const sseTimeout = window.setTimeout(() => {
      if (openedRef.current) return;
      setMode('polling');
      setConnected(false);
      setServerError('SSE not available (fallback to polling). Restart the backend to enable live streaming.');
      try {
        es.close();
      } catch {
        // ignore
      }
    }, 1500);

    es.onopen = () => {
      openedRef.current = true;
      window.clearTimeout(sseTimeout);
      setConnected(true);
      setMode('sse');
    };

    es.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data) as LeaderboardStreamPayload;
        if (!payload || !Array.isArray(payload.candidates)) return;
        setCandidates(payload.candidates);
        setScanned(payload.scanned);
        setLastUpdatedTs(typeof payload.ts === 'number' ? payload.ts : Date.now());
        setServerError(null);
      } catch {
        // ignore
      }
    };

    es.addEventListener('server_error', (event) => {
      try {
        const parsed = JSON.parse((event as MessageEvent).data) as { error?: string };
        setServerError(parsed?.error || 'Server error');
      } catch {
        setServerError('Server error');
      }
    });

    es.onerror = () => {
      if (!openedRef.current) return;
      setConnected(false);
    };

    return () => {
      window.clearTimeout(sseTimeout);
      try {
        es.close();
      } catch {
        // ignore
      }
    };
  }, [url]);

  useEffect(() => {
    if (mode !== 'polling') return;

    let cancelled = false;
    const poll = async () => {
      try {
        const params = new URLSearchParams({
          timeframeMin: String(timeframeMin),
          limit: String(limit),
        });
        const res = await fetch(`${API_BASE}/market/opportunities?${params.toString()}`);
        if (!res.ok) return;
        const payload = (await res.json()) as LeaderboardPollPayload;
        if (cancelled || !payload || !Array.isArray(payload.candidates)) return;
        setCandidates(payload.candidates);
        setScanned(payload.scanned);
        setLastUpdatedTs(Date.now());
      } catch {
        // ignore
      }
    };

    void poll();
    const interval = window.setInterval(() => void poll(), 2000);
    return () => {
      cancelled = true;
      window.clearInterval(interval);
    };
  }, [limit, mode, timeframeMin]);

  useEffect(() => {
    const interval = window.setInterval(() => {
      setNowTs(Date.now());
    }, 1000);
    return () => window.clearInterval(interval);
  }, []);

  const lastUpdatedLabel = useMemo(() => {
    if (!lastUpdatedTs) return '—';
    const s = Math.max(0, Math.floor((nowTs - lastUpdatedTs) / 1000));
    if (s < 2) return 'now';
    if (s < 60) return `${s}s ago`;
    const m = Math.floor(s / 60);
    return `${m}m ago`;
  }, [lastUpdatedTs, nowTs]);

  return (
    <div className="scores-leaderboard">
      <div className="scores-leaderboard-header">
        <div className="scores-leaderboard-title">
          <h3>Scores</h3>
          <span className={`scores-leaderboard-badge ${connected ? 'connected' : 'disconnected'}`}>
            {mode === 'polling' ? 'POLL' : connected ? 'LIVE' : 'OFFLINE'}
          </span>
        </div>
        <div className="scores-leaderboard-meta">
          <span>Updated {lastUpdatedLabel}</span>
          {typeof scanned === 'number' && <span>Scanned {scanned}</span>}
        </div>
      </div>
      {serverError && <div className="scores-leaderboard-warning">{serverError}</div>}

      <div className="scores-leaderboard-controls">
        <label>
          TF
          <select value={timeframeMin} onChange={(e) => handleTimeframeChange(Number.parseInt(e.target.value, 10))}>
            <option value={1}>1m</option>
            <option value={5}>5m</option>
            <option value={15}>15m</option>
            <option value={60}>60m</option>
          </select>
        </label>
        <label>
          Top
          <select value={limit} onChange={(e) => handleLimitChange(Number.parseInt(e.target.value, 10))}>
            <option value={5}>5</option>
            <option value={10}>10</option>
            <option value={20}>20</option>
          </select>
        </label>
      </div>

      <div className="scores-leaderboard-table">
        {candidates.length === 0 ? (
          <div className="scores-leaderboard-empty">No scores yet (waiting for market data).</div>
        ) : (
          candidates.map((c, idx) => {
            const isSelected = c.pair_index === currentPairIndex;
            return (
              <button
                key={`${c.pair_index}-${idx}`}
                className={`scores-row ${isSelected ? 'selected' : ''}`}
                onClick={() => onSelectPairIndex(c.pair_index)}
                title={Array.isArray(c.reasons) && c.reasons.length > 0 ? c.reasons.join('\n') : undefined}
              >
                <span className="scores-rank">{idx + 1}</span>
                <span className="scores-symbol">{c.symbol ?? `Pair ${c.pair_index}`}</span>
                <span className={`scores-side ${c.side.toLowerCase()}`}>{c.side}</span>
                <span className="scores-score">{typeof c.score === 'number' ? c.score.toFixed(1) : '—'}</span>
              </button>
            );
          })
        )}
      </div>
    </div>
  );
}
