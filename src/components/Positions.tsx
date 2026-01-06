import React, { useState } from 'react';
import { createPortal } from 'react-dom';
import type { Trade } from '../services/api';
import { PAIRS } from '../data/pairs';
import './Positions.css';

interface PositionsProps {
    trades: Trade[];
    currentPrice: number;
    selectedPairIndex: number;
    onClose: (id: number) => void;
    onCancel: (id: number) => void;
    highlightedTradeId: number | null;
    onRowHover: (id: number | null) => void;
    onRowClick?: (tradeId: number, pairIndex: number) => void;
}

const FEE_BPS = 6; // 6 basis points = 0.06%
const MIN_FEE = 1.25;

interface PnlBreakdown {
    grossPnl: number;
    openingFee: number;
    netPnl: number;
}

interface TooltipAnchorRect {
    top: number;
    left: number;
    width: number;
}

interface ColumnDefinition {
    key: string;
    label: string;
    description: string;
}

const pairNameByIndex = new Map<number, string>(PAIRS.map((pair) => [pair.index, pair.name]));

const COLUMN_DEFINITIONS: ColumnDefinition[] = [
    { key: 'id', label: 'ID', description: 'Unique identifier assigned to each trade by the bot.' },
    { key: 'asset', label: 'Asset', description: 'Human-friendly name for the selected trading pair.' },
    { key: 'time', label: 'Time', description: 'Local timestamp when the position was entered.' },
    { key: 'direction', label: 'Direction', description: 'Long or short bias sent to the exchange.' },
    { key: 'entry', label: 'Entry', description: 'Entry price captured when the position opened.' },
    { key: 'trigger', label: 'Trigger', description: 'Trigger price that caused the order to execute (when applicable).' },
    { key: 'sl', label: 'SL', description: 'Stop-loss level that caps downside risk.' },
    { key: 'tp', label: 'TP', description: 'Take-profit level used to lock in gains.' },
    { key: 'collateral', label: 'Collateral', description: 'Collateral allocated to the position in USD.' },
    { key: 'lev', label: 'Lev', description: 'Leverage multiplier applied to the collateral.' },
    { key: 'size', label: 'Size', description: 'Position size after leverage (collateral × leverage).' },
    { key: 'status', label: 'Status', description: 'Current lifecycle stage of the trade.' },
    { key: 'netPnl', label: 'Net PnL', description: 'Realized or unrealized profit after subtracting opening fees.' },
    { key: 'action', label: 'Action', description: 'Available operator controls (close/cancel) for the position.' }
];

export const Positions: React.FC<PositionsProps> = ({
    trades,
    currentPrice,
    selectedPairIndex,
    onClose,
    onCancel,
    highlightedTradeId,
    onRowHover,
    onRowClick,
}) => {
    const [tooltip, setTooltip] = useState<{ tradeId: number; anchor: TooltipAnchorRect } | null>(null);
    const [headerTooltip, setHeaderTooltip] = useState<{ column: string; description: string; anchor: TooltipAnchorRect } | null>(null);

    const calculateOpeningFee = (trade: Trade): number => {
        const positionSize = trade.collateral * trade.leverage;
        const fee = positionSize * (FEE_BPS / 10000);
        return Math.max(fee, MIN_FEE);
    };

    const calculatePnlBreakdown = (trade: Trade): PnlBreakdown | null => {
        if (trade.status === 'PENDING' || trade.status === 'CANCELED') return null;
        const openingFee = calculateOpeningFee(trade);

        let grossPnl = 0;
        if (trade.status === 'CLOSED') {
            // For closed trades, use the stored PnL as gross
            grossPnl = trade.pnl ?? 0;
        } else {
            // Unrealized PnL
            if (trade.pair_index !== selectedPairIndex) return null;
            if (!Number.isFinite(currentPrice) || currentPrice <= 0) return null;

            let pnlPercentage = 0;
            if (trade.direction === 'LONG') {
                pnlPercentage = (currentPrice - trade.entry_price) / trade.entry_price;
            } else {
                pnlPercentage = (trade.entry_price - currentPrice) / trade.entry_price;
            }
            grossPnl = pnlPercentage * trade.collateral * trade.leverage;
        }

        return {
            grossPnl,
            openingFee,
            netPnl: grossPnl - openingFee
        };
    };

    const tooltipTrade = tooltip ? trades.find((trade) => trade.id === tooltip.tradeId) : null;
    const tooltipBreakdown = tooltipTrade ? calculatePnlBreakdown(tooltipTrade) : null;

    const handleHeaderHover = (column: ColumnDefinition, event: React.MouseEvent<HTMLTableHeaderCellElement>) => {
        const rect = event.currentTarget.getBoundingClientRect();
        setHeaderTooltip({
            column: column.label,
            description: column.description,
            anchor: { top: rect.top, left: rect.left, width: rect.width }
        });
    };

    const clearHeaderTooltip = () => setHeaderTooltip(null);

    return (
        <div className="positions-container">
            <h3>Positions</h3>
            <div className="table-wrapper" onScroll={() => { setTooltip(null); setHeaderTooltip(null); }}>
                <table>
                    <thead>
                        <tr>
                            {COLUMN_DEFINITIONS.map((column) => (
                                <th
                                    key={column.key}
                                    onMouseEnter={(event) => handleHeaderHover(column, event)}
                                    onMouseLeave={clearHeaderTooltip}
                                >
                                    {column.label}
                                </th>
                            ))}
                        </tr>
                    </thead>
                    <tbody>
                        {trades.length === 0 ? (
                            <tr>
                                <td colSpan={COLUMN_DEFINITIONS.length} style={{ textAlign: 'center' }}>No recent trades</td>
                            </tr>
                        ) : (
                            trades.map((trade) => {
                                const breakdown = calculatePnlBreakdown(trade);
                                const isHighlighted = highlightedTradeId === trade.id;
                                const pairName = pairNameByIndex.get(trade.pair_index) ?? `Pair ${trade.pair_index}`;
                                const isClickable = typeof onRowClick === 'function';
                                return (
                                    <tr
                                        key={trade.id}
                                        id={`trade-row-${trade.id}`}
                                        className={`${isHighlighted ? 'highlighted-row' : ''} ${isClickable ? 'clickable-row' : ''}`.trim()}
                                        onClick={() => onRowClick?.(trade.id, trade.pair_index)}
                                        onMouseEnter={() => {
                                            if (trade.pair_index === selectedPairIndex) onRowHover(trade.id);
                                        }}
                                        onMouseLeave={() => {
                                            if (trade.pair_index === selectedPairIndex) onRowHover(null);
                                        }}
                                    >
                                        <td>{trade.id}</td>
                                        <td>{pairName}</td>
                                        <td>{new Date(trade.entry_time * 1000).toLocaleTimeString()}</td>
                                        <td className={trade.direction === 'LONG' ? 'dir-long' : 'dir-short'}>
                                            {trade.direction}
                                        </td>
                                        <td>{trade.status === 'PENDING' ? '—' : `$${trade.entry_price.toFixed(2)}`}</td>
                                        <td>{typeof trade.trigger_price === 'number' ? `$${trade.trigger_price.toFixed(2)}` : '—'}</td>
                                        <td>{typeof trade.stop_loss_price === 'number' ? `$${trade.stop_loss_price.toFixed(2)}` : '—'}</td>
                                        <td>{typeof trade.take_profit_price === 'number' ? `$${trade.take_profit_price.toFixed(2)}` : '—'}</td>
                                        <td>${trade.collateral.toFixed(0)}</td>
                                        <td>{trade.leverage}x</td>
                                        <td>${(trade.collateral * trade.leverage).toFixed(0)}</td>
                                        <td
                                            className={
                                                trade.status === 'OPEN'
                                                    ? 'status-open'
                                                    : trade.status === 'CLOSED'
                                                        ? 'status-closed'
                                                        : trade.status === 'PENDING'
                                                            ? 'status-pending'
                                                            : 'status-canceled'
                                            }
                                        >
                                            {trade.status}
                                        </td>
                                        <td
                                            className={`${breakdown ? 'pnl-cell' : ''} ${breakdown && breakdown.netPnl > 0 ? 'pnl-green' : breakdown && breakdown.netPnl < 0 ? 'pnl-red' : ''}`.trim()}
                                            onMouseEnter={(event) => {
                                                if (!breakdown) return;
                                                const rect = event.currentTarget.getBoundingClientRect();
                                                setTooltip({
                                                    tradeId: trade.id,
                                                    anchor: { top: rect.top, left: rect.left, width: rect.width }
                                                });
                                            }}
                                            onMouseLeave={() => setTooltip(null)}
                                        >
                                            {breakdown ? `$${breakdown.netPnl.toFixed(2)}` : '—'}
                                        </td>
                                        <td>
                                            {trade.status === 'OPEN' && (
                                                <button
                                                    className="close-btn"
                                                    onClick={(event) => {
                                                        event.stopPropagation();
                                                        onClose(trade.id);
                                                    }}
                                                >
                                                    Close
                                                </button>
                                            )}
                                            {trade.status === 'PENDING' && (
                                                <button
                                                    className="close-btn"
                                                    onClick={(event) => {
                                                        event.stopPropagation();
                                                        onCancel(trade.id);
                                                    }}
                                                >
                                                    Cancel
                                                </button>
                                            )}
                                        </td>
                                    </tr>
                                );
                            })
                        )}
                    </tbody>
                </table>
            </div>
            {tooltip && tooltipTrade && tooltipBreakdown
                ? createPortal(
                    <div
                        className="pnl-tooltip pnl-tooltip-portal"
                        style={{
                            top: tooltip.anchor.top - 8,
                            left: tooltip.anchor.left + tooltip.anchor.width / 2,
                        }}
                        role="tooltip"
                    >
                        <div className="tooltip-row">
                            <span>Gross PnL:</span>
                            <span className={tooltipBreakdown.grossPnl >= 0 ? 'pnl-green' : 'pnl-red'}>
                                ${tooltipBreakdown.grossPnl.toFixed(2)}
                            </span>
                        </div>
                        <div className="tooltip-row">
                            <span>Opening Fee:</span>
                            <span className="pnl-red">-${tooltipBreakdown.openingFee.toFixed(2)}</span>
                        </div>
                        <div className="tooltip-divider" />
                        <div className="tooltip-row tooltip-total">
                            <span>Net PnL:</span>
                            <span className={tooltipBreakdown.netPnl >= 0 ? 'pnl-green' : 'pnl-red'}>
                                ${tooltipBreakdown.netPnl.toFixed(2)}
                            </span>
                        </div>
                    </div>,
                    document.body
                )
                : null}
            {headerTooltip
                ? createPortal(
                    <div
                        className="column-tooltip column-tooltip-portal"
                        style={{
                            top: headerTooltip.anchor.top - 8,
                            left: headerTooltip.anchor.left + headerTooltip.anchor.width / 2,
                        }}
                        role="tooltip"
                    >
                        <div className="column-tooltip-label">{headerTooltip.column}</div>
                        <div className="column-tooltip-description">{headerTooltip.description}</div>
                    </div>,
                    document.body
                )
                : null}
        </div>
    );
};
