# Agent Lightning training (AutoTrader)

This folder documents how to produce a JSONL dataset from AutoTrader and feed it into `microsoft/agent-lightning` (AGL) for prompt/policy optimization.

## What exists today

AutoTrader now includes a dataset exporter:

- `server/scripts/exportAgentLightningDataset.js`
- script: `cd server && npm run export:agl`

It exports one JSON object per line (JSONL) for **execute_trade** decisions that have an evaluated **decision_outcomes** row for the configured horizon.

The default setup targets:
- **paper trading**
- **1 hour horizon** (3600s)
- reward = `pnl_return_1h - (AGL_DD_LAMBDA * drawdown_1h)`

## Generate outcomes + dataset

1) Ensure the decision outcomes table is populated:

```bash
cd server
npm run eval:outcomes
```

2) Export the dataset:

```bash
cd server
AGL_HORIZON_SEC=3600 AGL_DD_LAMBDA=2.0 npm run export:agl
```

Output:
- `server/datasets/agentlightning_episodes_3600s.jsonl`

## What’s inside each JSONL row

- `input`: minimal market context from `bot_decisions.analysis`
- `action`: `bot_decisions.decision` and the raw `analysis.toolCall`
- `reward`: scalar reward value
- `metrics`: pnl_return_1h, forward_return_1h, drawdown_1h

## Next step: wire to Agent Lightning

AGL is Python-based. The practical next step is to create a separate training project (recommended):

- `autotrader-agentlightning-trainer/`
  - loads JSONL
  - defines reward/trajectory format expected by AGL
  - runs prompt optimization or RL on the agent policy
  - writes a new prompt template version

### Why a separate repo?

Keeps Python deps and training artifacts out of the Node production app.

## Known TODOs

- If we want PnL in **USD**, we should join `bot_decisions.trade_id -> trades` and compute sizing-aware PnL. v1 uses normalized return + drawdown.
- If we want more accurate drawdown than min/max of `market_state_history.price`, we should export OHLC per candle or use raw candle series.
