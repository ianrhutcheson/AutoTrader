
import React from 'react';
import './Controls.css';
import { PAIRS } from '../data/pairs';
import { ScoresLeaderboard } from './ScoresLeaderboard';

interface ControlsProps {
    pairIndex: number;
    setPairIndex: (val: number) => void;
    resolution: string;
    setResolution: (val: string) => void;
    onTrade?: (
        direction: 'LONG' | 'SHORT',
        collateral: number,
        leverage: number,
        stopLossPercent: number | null,
        takeProfitPercent: number | null,
        triggerPrice: number | null,
    ) => void;
}

export const Controls: React.FC<ControlsProps> = ({
    pairIndex,
    setPairIndex,
    resolution,
    setResolution,
    onTrade
}: ControlsProps) => {
    // Local state for trading controls
    const [collateral, setCollateral] = React.useState(1000);
    const [leverage, setLeverage] = React.useState(10);
    const [stopLossPercent, setStopLossPercent] = React.useState<string>('1');
    const [takeProfitPercent, setTakeProfitPercent] = React.useState<string>('2');
    const [triggerPriceInput, setTriggerPriceInput] = React.useState<string>('');

    const parsePercent = (value: string): number | null => {
        const trimmed = value.trim();
        if (!trimmed) return null;
        const parsed = Number.parseFloat(trimmed);
        return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
    };

    const parseTriggerPrice = (value: string): number | null => {
        const trimmed = value.trim();
        if (!trimmed) return null;
        const parsed = Number.parseFloat(trimmed);
        return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
    };

    // Group pairs by category
    const groupedPairs = PAIRS.reduce((acc, pair) => {
        if (!acc[pair.category]) acc[pair.category] = [];
        acc[pair.category].push(pair);
        return acc;
    }, {} as Record<string, typeof PAIRS>);

    return (
        <div className="controls-container">
            <div className="control-group">
                <label>Asset Pair</label>
                <select
                    value={pairIndex}
                    onChange={(e) => setPairIndex(Number(e.target.value))}
                >
                    {Object.keys(groupedPairs).map(category => (
                        <optgroup key={category} label={category}>
                            {groupedPairs[category].map(pair => (
                                <option key={pair.index} value={pair.index}>
                                    {pair.name}
                                </option>
                            ))}
                        </optgroup>
                    ))}
                </select>
            </div>

            <div className="control-group">
                <label>Resolution</label>
                <select value={resolution} onChange={(e) => setResolution(e.target.value)}>
                    <option value="1">1m</option>
                    <option value="5">5m</option>
                    <option value="15">15m</option>
                    <option value="60">1h</option>
                    <option value="240">4h</option>
                    <option value="1D">1D</option>
                </select>
            </div>

            <div className="separator" />

            <div className="trade-controls">
                <div className="control-row">
                    <div className="control-group">
                        <label>Collateral (DAI)</label>
                        <input
                            type="number"
                            value={collateral}
                            onChange={(e) => setCollateral(Number(e.target.value))}
                            min="10"
                        />
                    </div>
                    <div className="control-group">
                        <label>Leverage (x)</label>
                        <input
                            type="number"
                            value={leverage}
                            onChange={(e) => setLeverage(Number(e.target.value))}
                            min="1"
                            max="150"
                        />
                    </div>
                </div>
                <div className="control-row">
                    <div className="control-group">
                        <label>Stop Loss (%)</label>
                        <input
                            type="number"
                            inputMode="decimal"
                            value={stopLossPercent}
                            onChange={(e) => setStopLossPercent(e.target.value)}
                            min="0"
                            step="0.1"
                            placeholder="e.g., 1"
                        />
                    </div>
                    <div className="control-group">
                        <label>Take Profit (%)</label>
                        <input
                            type="number"
                            inputMode="decimal"
                            value={takeProfitPercent}
                            onChange={(e) => setTakeProfitPercent(e.target.value)}
                            min="0"
                            step="0.1"
                            placeholder="e.g., 2"
                        />
                    </div>
                </div>
                <div className="control-row">
                    <div className="control-group">
                        <label>Trigger Price (optional)</label>
                        <input
                            type="number"
                            inputMode="decimal"
                            value={triggerPriceInput}
                            onChange={(e) => setTriggerPriceInput(e.target.value)}
                            min="0"
                            step="0.0001"
                            placeholder="Market entry"
                        />
                    </div>
                </div>
                <div className="trade-buttons">
                    <button
                        className="refresh-button buy-button"
                        onClick={() => onTrade && onTrade(
                            'LONG',
                            collateral,
                            leverage,
                            parsePercent(stopLossPercent),
                            parsePercent(takeProfitPercent),
                            parseTriggerPrice(triggerPriceInput),
                        )}
                    >
                        Long
                    </button>
                    <button
                        className="refresh-button sell-button"
                        onClick={() => onTrade && onTrade(
                            'SHORT',
                            collateral,
                            leverage,
                            parsePercent(stopLossPercent),
                            parsePercent(takeProfitPercent),
                            parseTriggerPrice(triggerPriceInput),
                        )}
                    >
                        Short
                    </button>
                </div>
            </div>

            <div className="separator" />
            <ScoresLeaderboard currentPairIndex={pairIndex} onSelectPairIndex={setPairIndex} />
        </div>
    );
};
