# Nexus Strategy: ETF Confluence Sniper

`etf_confluence_sniper` looks for a clean directional ETF move where price momentum, same-side options flow, and option pricing all agree. It is designed for fast 0DTE decision windows where the trader needs to know whether the move is real confluence or just noisy flow.

## Trader Meaning

The strategy should say:

- **Bullish confluence:** underlying momentum is higher, call flow confirms, calls are cheap or fair enough, execution is fresh, and the market reads do not warn about chop, event shock, or reversal risk.
- **Bearish confluence:** underlying momentum is lower, put flow confirms, puts are cheap or fair enough, execution is fresh, and the market reads do not warn about chop, event shock, or reversal risk.
- **Watch / no trade:** one or more reads disagree, pricing is bad, quotes are stale, or the market regime is not clean enough for directional debit risk.

## Window Model

The strategy has two different window sets:

- **Read windows:** all completed market-context windows. These produce `WINDOW_VIEW`, `CHOP`, or `BLOCKED` reads so the trader can see what the strategy thought all day.
- **Trade windows:** the researched strategy-entry windows encoded in code. These are the only windows allowed to produce `INTERMEDIATE` and later `BET` lifecycle signals.

This separation is important: a strategy can explain every window without creating a trade opportunity in every window.

## Read Windows

Read windows use the market-context window schedule. They run every 30 minutes from `09:30 -> 16:00` ET, plus the optional `16:00 -> 16:15` window when available.

For `etf_confluence_sniper`, each read window should emit one DB `WINDOW_VIEW`-style row per symbol:

- `WINDOW_VIEW / BULLISH` when confluence is directionally bullish enough to watch.
- `WINDOW_VIEW / BEARISH` when confluence is directionally bearish enough to watch.
- `WINDOW_VIEW / CHOP` when confluence is not clean.
- `BLOCKED` when required materialized context is missing, stale, or degraded.

Every emitted row must include three canonical reads:

- `direction_read`: `bull`, `bear`, or `range_chop`
- `trade_action_read`: `watch`, `bet`, or `stand_down`
- `signal_stage`: `initial`, `intermediate`, or `final`

All directional `WINDOW_VIEW` rows are `trade_action_read = watch` and `signal_stage = initial`. They should include the same trader-safe expression fields as `WATCH` or `INTERMEDIATE`: option type, expiry, strike, quote freshness, derived limit guidance, and risk/invalidation notes. `CHOP` and `BLOCKED` rows should use `direction_read = range_chop`, `trade_action_read = stand_down`, `signal_stage = initial`, and should not show a trade expression.

## Trade Windows

Trade windows are narrower. Current DB replay code allows `etf_confluence_sniper` candidates only in the five entry windows below.

| Entry label | Data window ET | Intended use |
|---|---|---|
| `10:00` | `09:30 -> 10:00` | Opening confluence after the first 30 minutes. |
| `10:30` | `10:00 -> 10:30` | Primary continuation or post-open confirmation window. |
| `11:00` | `10:30 -> 11:00` | Late-morning continuation window. |
| `11:30` | `11:00 -> 11:30` | Late-morning follow-through; more sensitive to chase/exhaustion reads. |
| `12:00` | `11:30 -> 12:00` | Final configured decision window; should demand clean timing and no exhaustion risk. |

These trade windows map to the materialized DB windows:

| DB window label | Entry label |
|---|---|
| `w0930_1000` | `10:00` |
| `w1000_1030` | `10:30` |
| `w1030_1100` | `11:00` |
| `w1100_1130` | `11:30` |
| `w1130_1200` | `12:00` |

Open point: older docs describe the strategy role as `10:00-12:30 ET`, but the current DB replay emitter stops at `12:00`. Do not extend trade signals to `12:30` until the backtest/code source is confirmed.

## Current Redis Live Conditions

Redis live path source: `src/sigmatiq_nexus/nexus_worker.py`.

Required fields:

- `ts_utc`
- `raw_symbol`
- `side`
- `premium`
- `underlying_mid`
- `delta`
- `option_mid`
- `iv_rank`

Runtime gates:

- Strategy has not already fired for the same symbol/session.
- Required live features are present and fresh.
- Context is usable for the current decision window.

Momentum gate:

- Build one-minute bars from `underlying_mid`.
- Require at least 10 one-minute bars.
- Bullish when bullish minutes exceed bearish minutes by at least 5.
- Bearish when bearish minutes exceed bullish minutes by at least 5.
- Directional persistence must be greater than 50%.

Volatility gate:

- Block when `iv_rank >= 40`.

Pricing gate:

- Select calls for bullish signals and puts for bearish signals.
- Pick the lead contract by same-side premium.
- Calculate option pricing lag from option price change versus delta-adjusted underlying move.
- Require pricing lag `<= -0.05`.
- Require quote timestamps close enough to the trade timestamps at the start and end of the pricing comparison.

Redis live outputs:

- `WINDOW_VIEW` when window assessment can explain the strategy read.
- `BLOCKED` when required features are missing or stale.
- `INTERMEDIATE` when the heuristic qualifies.
- `BET` immediately after `INTERMEDIATE` for this strategy today.

## Current DB Replay Conditions

DB replay path source: `src/sigmatiq_nexus/db_signal_emitter.py`.

Base materialized context gates:

- `readiness_status = ready`
- `daily_context_status IN (ready, healthy)`
- `final_quote_status = fresh_quote_asof_window`
- `lead_contract_raw_symbol IS NOT NULL`
- `lead_quote_mid IS NOT NULL`
- Optional realtime lag guard: `now()` and `materialized_at_utc` must be within `max_realtime_lag_seconds` after the window end when configured.

Current confluence proxy gates:

- `total_premium >= min_window_premium`
- `pricing_quality = ready`
- `cheap_side IN (C, P)`
- `lead_delta IS NOT NULL`
- `lead_gamma IS NOT NULL`
- `equity_first_price` and `equity_last_price` are present and positive
- Bullish candidate when:
  - `equity_last_price > equity_first_price`
  - `cheap_side = C`
  - `call_premium > put_premium`
  - `lead_option_side = C`
- Bearish candidate when:
  - `equity_last_price < equity_first_price`
  - `cheap_side = P`
  - `put_premium > call_premium`
  - `lead_option_side = P`
- Do not emit if the same symbol/session/strategy/entry already has `INTERMEDIATE` or `BET`.

Current DB replay output:

- Emits `INTERMEDIATE` only.
- Should carry `direction_read = bull` or `bear`, `trade_action_read = watch`, and `signal_stage = intermediate`.
- Uses `strategy_read_label = confluence_alignment_candidate`.
- Uses `strategy_action = candidate`.
- Uses `strategy_capability = confluence_alignment`.
- Adds safe `trade_expression` with option type, expiry, and strike.
- Adds safe `risk_guidance` with limit order guidance and max debit derived from `lead_quote_mid`.
- Marks `db_replay.replay_scope = pricing_and_momentum_proxy`.
- Explicitly describes itself as an audit candidate, not a Redis live-overlay `BET`.

Required DB path improvement:

- Emit a `WINDOW_VIEW` row for every read window.
- Emit `INTERMEDIATE` only when a configured trade window passes candidate gates.
- Emit `BET` only when the same selected contract remains fresh, tradable, and inside final execution/risk gates.
- Ensure every emitted row has `direction_read`, `trade_action_read`, and `signal_stage`.
- Keep the same selected contract reference across `WINDOW_VIEW`, `INTERMEDIATE`, and `BET` for the same symbol/strategy/window. If the selected contract changes, treat it as a new candidate version rather than silently mutating the prior signal.
- Preserve raw contract, quote, Greek, IV, premium, and gate evidence in the internal DB signal payload.
- Expose only the sanitized public strategy read through API/UI: direction/action/stage, strategy context, safe expression, derived limit guidance, risk/invalidation summary, market-read labels, and freshness labels.

## Alignment With Market Reads

| Strategy condition | Market read dependency | Required read posture |
|---|---|---|
| Directional impulse | Regime, Timing | Bullish/bearish trend, opening drive, or continuation; not chop. |
| Flow agrees with direction | Flow | Call dominance for bullish; put dominance for bearish; not mixed/divergent. |
| Option side is not overpriced | Volatility, Trade Quality | Fair/cheap premium; avoid premium trap, bad pricing, or vol crush. |
| Entry is not late | Timing, Price Location, Position Management | Entry/manage window is acceptable; avoid wall chase or exhaustion. |
| Thesis is still valid | Invalidation | No pivot/VWAP/flow break against the direction. |
| Market structure is trustworthy | Event Reliability | Normal reliability; avoid active event shock or unreliable data. |
| Trade can be filled | Liquidity / Execution | Fresh quote and usable execution quality. |

## Trade Signals By Trade Window

| Window | Bullish signal | Bearish signal | Watch / no trade |
|---|---|---|---|
| `10:00` | Opening momentum higher, calls dominate, calls cheap/fair, quote fresh, no event shock. | Opening momentum lower, puts dominate, puts cheap/fair, quote fresh, no event shock. | Mixed open, high IV, stale quote, no pricing edge, or conflicted reads. |
| `10:30` | Continuation higher with confirming call flow and acceptable premium. | Continuation lower with confirming put flow and acceptable premium. | Post-open chop, flow disagreement, premium trap, or delayed materialization. |
| `11:00` | Trend still clean and not too extended into resistance. | Trend still clean and not too extended into support. | Late chase risk begins to matter; require stronger trade quality. |
| `11:30` | Only if reads still align and price is not stretched into a wall. | Only if reads still align and price is not stretched into a wall. | Exhaustion, wall rejection risk, or reduced liquidity should suppress. |
| `12:00` | Last configured opportunity; must be clean and fresh. | Last configured opportunity; must be clean and fresh. | Stand down if timing is no longer an entry window or data is stale. |

## Practice Mapping

Use this strategy inside these practice modes:

- **Trade the Day:** ask whether a candidate is real confluence or just one attractive read.
- **Bullish Trend Day:** identify when bullish momentum, call flow, and acceptable premium align.
- **Bearish Trend Day:** identify when bearish momentum, put flow, and acceptable premium align.
- **Range / Pin Day:** teach why confluence should usually not fire in chop.
- **Conflicted / No-Trade Day:** show flow or momentum disagreement and explain why to stand down.
- **Regime Flip Mid-Session:** show when earlier confluence becomes invalid after the regime changes.
- **Trend Exhaustion:** show when the same directional move becomes too late to chase.
- **Vol Crush / Premium Trap:** show when direction is right but the option is too expensive.
- **Event / News Shock:** show why normal confluence reads are unreliable during an active shock.

Practice question templates:

- "Which read is missing before this becomes confluence?"
- "Is this a BET, WATCH, or stand-down candidate?"
- "Did price momentum, flow, and pricing all agree?"
- "What invalidates this confluence read?"
- "Is the trader early enough, or is this now chase/exhaustion risk?"

## Known Gap To Fix

Redis live and DB replay do not yet use the exact same confluence classifier. Redis uses one-minute momentum persistence and direct pricing lag. DB replay uses materialized first/last window price, cheap side, premium dominance, and lead contract side as a proxy. The next backend improvement should materialize the Redis-equivalent momentum evidence into `live.prepared_nexus_window_context` and make DB replay require that shared evidence.
