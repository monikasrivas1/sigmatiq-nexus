# Nexus Live Feature Contract

## Purpose

This document defines the live features Nexus needs to run the currently implemented SPY strategies and compares them with what the current live pipeline already provides.

Scope is intentionally limited to implemented Nexus strategies and implemented live data producers. Strategy decisions stay in Nexus. Upstream workers should publish reusable market features, not strategy-specific decisions, unless the feature is broadly useful and stateful.

Current lock model:

- one independent final-trade lane per symbol per NY session
- plus one shared group lock for `etf_confluence_sniper` across the configured ETF universe
- this allows `SPY` and `QQQ` to fire independently while still keeping the group confluence lane single-fire
- final single-leg locks are enforced in Redis with `SET NX`, not only process memory
- active single-leg positions are persisted in Redis and restored when the worker starts during the same NY session
- Redis Stream offsets are persisted per stream; the worker resumes from the last processed ID after restart
- completed windows are evaluated only after the configured grace period, then marked evaluated once per `session_date + symbol + entry_time`

## Current Nexus Strategies

| Strategy | Current role | Main decision window | Core idea |
|---|---:|---:|---|
| `etf_confluence_sniper` | Phase 1 primary | 10:00-12:30 ET | Flow plus pricing lag plus momentum alignment. |
| `etf_open_specialist` | Phase 2 primary | 10:00 ET entry from 09:30-10:00 window | Cheap-call open rule: call premium dominates while IV rank is low. |
| `etf_put_credit_open30_spread` | Research paper-only | 10:00 ET entry from 09:30-10:00 window | Bullish open30 call dominance expressed as a same-expiry put credit spread. |
| `etf_call_credit_open30_spread` | Research paper-only | 10:00 ET entry from 09:30-10:00 window | Bearish open30 put dominance expressed as a same-expiry call credit spread. |
| `etf_low_sweep_core` | Phase 2 compatibility | 10:00-10:30 ET entries | Low-sweep directional flow candidate retained from prior research. |
| `etf_flow_specialist` | Phase 2 support | 10:30 ET entry from 10:00-10:30 window | Strong option-flow dominance with IV/GEX context. |
| `etf_momentum_specialist` | Phase 2 support | 11:00 ET entry from 10:30-11:00 window | Underlying persistence plus option-flow confirmation. |

All implemented strategies also publish a per-window directional assessment for every completed window from `09:30-12:00` ET:

- `decision = WINDOW_VIEW`
- `sentiment = BULLISH | BEARISH | CHOP`
- this is informational only and does not imply a trade candidate
- `lead_contract_pricing_lag` and `lead_contract_cheapness_score` now annotate the dominant contract inside each strategy window view

Nexus also publishes a symbol-level window pricing summary for every completed window:

- `decision = WINDOW_PRICING`
- includes `cheap_contract_*`, `costly_contract_*`, `cheap_side`, and `costly_side`
- this is also informational only and separate from trade candidates

Spread candidates publish separately from single-leg candidates:

- `decision = BET`
- `instrument_type = vertical_credit_spread`
- `paper_only = true`
- Redis key: `nexus_spread_overlay:{symbol}:{strategy}:{entry_label}`
- Channel: `signal:spread:{strategy}`
- Persistence: appended to `live:persistence:events` for EOD review
- Runtime limitation: spread candidates are not managed by the current single-leg liquidation loop.

Single-leg final candidates have an additional execution gate:

- exact `raw_symbol` quote is fetched from `options:live:contract_state:{raw_symbol}` and `options:live:tradability:{raw_symbol}`
- quote freshness must be acceptable and a reference price must exist
- the recorded `entry_price` is the fresh quote reference, not a stale event-side value
- if the quote gate fails, Nexus logs `strategy_final_blocked_by_quote` and publishes no final `BET`

Current window-completion semantics:

- the worker buffers live events by symbol
- the scheduler checks due windows on every stream loop and after each processed event
- a slot is due when `slot.entry + NEXUS_WINDOW_EVALUATION_GRACE_SECONDS` has passed in New York time
- each scheduler pass processes only the latest due market-context slot and latest due strategy slot; after a restart Nexus does not backfill every missed historical window before returning to live stream consumption
- each window evaluates once per session/symbol/entry label
- window assignment is session-date aware; same clock-time rows from prior sessions must not satisfy today's window
- if the in-memory buffer has no rows for a due slot, Nexus falls back to the raw Redis option stream and scans the latest `NEXUS_STREAM_WINDOW_LOOKBACK_COUNT` entries before declaring `empty_window`
- Redis Stream fallback rows are enriched once per unique `raw_symbol` from `options:live:contract_state:{rawSymbol}` / `options:live:tradability:{rawSymbol}` and once per symbol from `equity:live:context:{symbol}` before feature gates run; this avoids one Redis read per trade while preserving fail-closed behavior when current quote/Greek/spot context is stale or missing
- live context freshness for IV rank, ATM IV, and GEX is checked against the scheduled evaluation timestamp, not the last trade timestamp in the completed window
- timestamp parsing accepts .NET-style 7-digit fractional seconds from live worker payloads and normalizes them to Python microsecond precision before freshness checks
- this is safer than first-event-after-boundary evaluation, but it is still not a true upstream ingestion watermark

## Feature Availability Summary

| Feature group | Needed by | Current availability | Status |
|---|---|---|---|
| Normalized option trade events | All strategies | Raw Redis option trade stream exists. Options live worker enriches trades internally. | Partial |
| Aggressor and sweep flags | All strategies | Nexus accepts raw fields when present and can derive conservative `aggressor`/`is_sweep` from fresh contract quote state. | Available with freshness gate |
| Premium and side | All strategies | Derivable from raw symbol, price, and size. Nexus currently derives some values. | Partial |
| Delta and gamma per trade | Sequence model, flow, confluence | `options:live:contract_state:{rawSymbol}` publishes Greek state when quote, spot, and IV solve are available; Nexus also accepts enriched event fields. | Available with freshness gate |
| Option mid per contract | Confluence pricing-lag | `options:live:tradability:{rawSymbol}` and `options:live:contract_state:{rawSymbol}` carry bid/ask/mid state; Nexus consumes both. | Available with freshness gate |
| Underlying mid per event/window | Momentum, confluence | Nexus enriches from `equity:live:context:{symbol}` and blocks stale/degraded equity context. | Available with freshness gate |
| IV rank / IV percentile | Flow, momentum, confluence | Nexus reads `stats:{symbol}:iv_rank` and live IV/VRP keys, with timestamp enforcement for live decisions. | Partial |
| ATM IV | Flow context | `options:live:iv_surface:{symbol}` exists. | Available |
| VRP regime | Vol context / future filters | `options:live:vrp:{symbol}` exists. Nexus does not use it deeply today. | Available but underused |
| Net GEX / gamma regime | Flow context | `options:live:gex:{symbol}` exists; Nexus blocks stale or timestamp-less GEX context for strategies that require it. | Available with freshness caveat |
| Liquidity / spread guard | All tradeable outputs | Nexus consumes tradability and contract-state spread/tradability flags. | Available |
| Live data health / freshness | All strategies | Nexus now fails closed on stale, missing, or unknown-freshness required feature inputs. | Available |

## Strategy Feature Requirements

### `etf_open_specialist`

Required features:

| Field | Why it matters | Current source | Status |
|---|---|---|---|
| `ts_utc` | Window assignment and time-aware rules. | Raw option trade stream. | Available |
| `symbol` | Strategy scope. | Stream config or event field. | Available |
| `side` | Requires call-side dominance. | Derivable from `raw_symbol`; Nexus also reads event side. | Available |
| `premium` | `$200k` call-premium hurdle. | Derivable from price times size times 100. | Available |
| `iv_rank` | Cheap-volatility filter for the opening window. | `stats:{symbol}:iv_rank` or live VRP fallback. | Partial |

Minimum acceptable live behavior:

- Runs only for the 10:00 ET decision using the completed 09:30-10:00 window.
- Requires call premium above `NEXUS_MIN_WINDOW_PREMIUM`, IV rank below `30`, and call premium dominance above `NEXUS_OPEN_CALL_DOMINANCE`.
- Runtime behavior: Nexus blocks this strategy when IV rank is missing or stale.

### `etf_low_sweep_core`

Required features:

| Field | Why it matters | Current source | Status |
|---|---|---|---|
| `ts_utc` | Window assignment and time-aware rules. | Raw option trade stream. | Available |
| `symbol` | Strategy scope. | Stream config or event field. | Available |
| `raw_symbol` | Contract identity and expiry/side parsing. | Raw option trade stream. | Available |
| `side` | Directional call/put classification. | Derivable from `raw_symbol`; Nexus also reads event side. | Available |
| `premium` | Premium threshold and weighted flow. | Derivable from price times size times 100. | Available |
| `is_sweep` | Sweep-heavy alpha filter. | Raw event field or Nexus quote-derived fallback when fresh bid/ask and aggressive-side premium are available. | Available with freshness gate |

Minimum acceptable live behavior:

- If `is_sweep` is absent, Nexus should not silently treat every trade as non-sweep unless that is intentional and visible in diagnostics.
- A feature-quality reason should explain when the strategy is disabled because sweep classification is unavailable.
- Runtime behavior: Nexus blocks this strategy and emits a stage `0` `BLOCKED` diagnostic when `is_sweep` is missing.

### `etf_flow_specialist`

Required features:

| Field | Why it matters | Current source | Status |
|---|---|---|---|
| Low-sweep fields | Base directional flow. | Same as above. | Partial |
| `aggressor` | Distinguishes ask-lifted demand from bid-side closing/selling ambiguity. | Raw event field or Nexus quote-derived fallback using trade price vs fresh bid/ask. | Available with freshness gate |
| `delta`, `gamma` | Sequence features and flow quality context. | `options:live:contract_state:{rawSymbol}` or enriched event fields. | Available with freshness gate |
| `iv_rank` or percentile | Avoids buying expensive volatility blindly. | `stats:{symbol}:iv_rank`, IV/VRP Redis. | Partial |
| `atm_iv` | Premium context. | `options:live:iv_surface:{symbol}`. | Available |
| `net_gex` | Dealer-positioning context. | `options:live:gex:{symbol}`. | Partial |
| Feature freshness | Prevents stale IV/GEX from steering signals. | Nexus strategy gates. | Available |

Minimum acceptable live behavior:

- If IV or GEX is stale, the strategy should either degrade explicitly or fail closed depending on how central the field is to the decision.
- Missing `delta` and `gamma` should not become silent zeros in production signals.
- Runtime behavior: Nexus blocks this strategy when aggressor, sweep, delta, gamma, IV rank, ATM IV, or net GEX is missing.
- Runtime freshness behavior: Nexus also blocks when IV, VRP, GEX, or Greek inputs are stale or have unknown freshness.

### `etf_momentum_specialist`

Required features:

| Field | Why it matters | Current source | Status |
|---|---|---|---|
| `underlying_mid` by minute | Builds price persistence and direction. | `equity:live:context:{symbol}` enrichment. | Available with freshness gate |
| Option-flow direction | Confirms momentum with flow. | Raw/enriched option events. | Partial |
| `iv_rank` | Avoids poor volatility regime. | Stats/IV/VRP keys. | Partial |
| Window timestamps | Aligns momentum and option flow. | Raw option event timestamps. | Available |

Minimum acceptable live behavior:

- Nexus should not rely on option events carrying `underlying_mid` unless ingestion guarantees it.
- Better source is a reusable underlying-state feature keyed by symbol and timestamp, then Nexus joins or samples it for the completed window.
- Runtime behavior: Nexus blocks this strategy when `underlying_mid` or IV rank is missing.
- Runtime freshness behavior: Nexus also blocks when underlying state or IV context is stale or has unknown freshness.

### `etf_confluence_sniper`

Required features:

| Field | Why it matters | Current source | Status |
|---|---|---|---|
| `raw_symbol` | Tracks same contract over time. | Raw option stream. | Available |
| `option_mid` | Measures lag between theoretical and observed option movement. | `options:live:contract_state:{rawSymbol}` or tradability key. | Available with freshness gate |
| `underlying_mid` | Drives expected option move via delta. | `equity:live:context:{symbol}` enrichment. | Available with freshness gate |
| `delta` | Pricing-lag formula input. | `options:live:contract_state:{rawSymbol}` or enriched event field. | Available with freshness gate |
| At least 5 minutes of same-contract history | Required for lag calculation. | Could come from enriched event or contract-state stream. | Partial |
| Quote freshness / quality | Prevents stale quote lag false positives. | Tradability and contract-state payloads consumed by Nexus. | Available with freshness gate |
| Momentum and flow fields | Confluence confirmation. | Above feature groups. | Partial |

Minimum acceptable live behavior:

- Pricing lag should be disabled when `delta`, `underlying_mid`, or `option_mid` are missing or stale.
- Missing fields should produce a reason such as `missing_contract_state`, not a numeric fallback that looks valid.
- Runtime behavior: Nexus blocks this strategy when `delta`, `underlying_mid`, `option_mid`, or IV rank is missing.
- Runtime freshness behavior: Nexus also blocks when quote/mid, Greek, underlying, or IV context age exceeds the configured limit.

### `etf_put_credit_open30_spread`

Required features:

| Field | Why it matters | Current source | Status |
|---|---|---|---|
| `ts_utc` | Window assignment. | Raw option trade stream. | Available |
| `symbol` | SPY/QQQ scope. | Stream config or event field. | Available |
| `raw_symbol` | Short-leg identity and same-expiry long-leg construction. | Raw option trade stream. | Available |
| `side` | Requires call-dominant open30 flow and put-leg candidate selection. | Event side or parsed contract. | Available |
| `premium` | `$200k` window hurdle and side dominance. | Event premium. | Available |
| `delta` | Selects short put closest to target absolute delta. | Contract-state enrichment. | Available with freshness gate |
| `option_mid` | Ensures quote/mid enrichment is present before candidate selection. | Contract-state enrichment. | Available with freshness gate |
| `iv_rank` | Keeps the live feature contract aligned with open30 volatility gating. | `stats:{symbol}:iv_rank` or live VRP fallback. | Partial |

Minimum acceptable live behavior:

- Runs only at the 10:00 ET decision using the completed 09:30-10:00 window.
- SPY and QQQ are eligible.
- Requires call premium dominance, IV rank below `NEXUS_SPREAD_MAX_IV_RANK`, a valid short put near target delta, a same-expiry lower-strike long put, fresh quotes for both legs, and net credit at or above `NEXUS_SPREAD_MIN_ENTRY_CREDIT`.
- Publishes only paper spread candidates and does not update the single-leg active-position liquidation state.

### `etf_call_credit_open30_spread`

Required features:

| Field | Why it matters | Current source | Status |
|---|---|---|---|
| `ts_utc` | Window assignment. | Raw option trade stream. | Available |
| `symbol` | SPY-only scope from the current research memo. | Stream config or event field. | Available |
| `raw_symbol` | Short-leg identity and same-expiry long-leg construction. | Raw option trade stream. | Available |
| `side` | Requires put-dominant open30 flow and call-leg candidate selection. | Event side or parsed contract. | Available |
| `premium` | `$200k` window hurdle and side dominance. | Event premium. | Available |
| `delta` | Selects short call closest to target absolute delta. | Contract-state enrichment. | Available with freshness gate |
| `option_mid` | Ensures quote/mid enrichment is present before candidate selection. | Contract-state enrichment. | Available with freshness gate |
| `iv_rank` | Keeps the live feature contract aligned with open30 volatility gating. | `stats:{symbol}:iv_rank` or live VRP fallback. | Partial |

Minimum acceptable live behavior:

- Runs only at the 10:00 ET decision using the completed 09:30-10:00 window.
- SPY is eligible; QQQ call-credit is intentionally excluded by current research posture.
- Requires put premium dominance, IV rank below `NEXUS_SPREAD_MAX_IV_RANK`, a valid short call near target delta, a same-expiry higher-strike long call, fresh quotes for both legs, and net credit at or above `NEXUS_SPREAD_MIN_ENTRY_CREDIT`.
- Publishes only paper spread candidates and does not update the single-leg active-position liquidation state.

## Recommended Reusable Live Feature Contracts

### 1. Enriched Option Event

Recommended key or stream:

- `nexus:{symbol}:options:events`
- or `md:{symbol}:options:trades:enriched`

Required fields:

| Field | Notes |
|---|---|
| `symbol` | Underlying symbol, normalized. |
| `raw_symbol` | Full option contract symbol. |
| `ts_utc` | Event timestamp in UTC. |
| `price` | Trade price. |
| `size` | Contracts. |
| `premium` | `price * size * 100`. |
| `side` | `call` or `put`, parsed from contract. |
| `aggressor` | `ask`, `bid`, `mid`, `unknown`, or equivalent enum. |
| `is_sweep` | Boolean sweep classification. |
| `quote_age_ms` | Age of quote used for aggressor classification. |
| `data_quality` | `ok`, `stale_quote`, `wide_spread`, `missing_quote`, etc. |

Current state:

- Raw events exist.
- Production Redis is clustered, so Nexus consumes `md:{symbol}:options:trades` one stream at a time instead of one multi-key `XREAD`; this avoids Redis Cluster cross-slot failures for the combined SPY/QQQ worker.
- Options live worker computes enough enrichment internally for unusual-trade logic.
- Nexus should not assume production raw events include research fields; it now derives `aggressor` and `is_sweep` from fresh contract-state/tradability quotes when raw fields are absent.

### 2. Live Contract State

Recommended key:

- `options:live:contract_state:{rawSymbol}`

Required fields:

| Field | Notes |
|---|---|
| `symbol` | Underlying symbol. |
| `raw_symbol` | Full option contract symbol. |
| `asOfUtc` | State timestamp. |
| `expiry`, `strike`, `optionType` | Parsed contract identity. |
| `bid`, `ask`, `optionMid` | Quote state. |
| `spreadPct` | `(ask - bid) / mid`. |
| `bidSize`, `askSize` | Quote depth. |
| `iv`, `delta`, `gamma`, `thetaPerDay`, `vega` | Contract Greeks. |
| `underlyingMid` | Spot used for Greek calculation. |
| `tradabilityScore`, `tradabilityBucket` | Reuse existing tradability scoring. |
| `blockingReasons`, `warnings` | Missing quote, stale quote, wide spread, bad IV, etc. |

Current state:

- `options:live:tradability:{rawSymbol}` publishes quote/tradability state.
- `options:live:contract_state:{rawSymbol}` publishes the same quote/tradability fields plus underlying spot and Greeks when the live Greek solve succeeds.
- Nexus must read both padded OPRA raw symbols (`SPY   260511P00740000`) and compact raw symbols (`SPY260511P00740000`) for contract-state and tradability enrichment. Final quote lookup, stream-fallback enrichment, and direct payload enrichment should use the same variant lookup behavior so strategy gates do not block only because producers and consumers use different raw-symbol key formatting.
- Runtime behavior: Nexus reads `options:live:contract_state:{rawSymbol}` first, then `options:live:tradability:{rawSymbol}` as a quote-only fallback.
- Remaining gap: if the live Greek solve fails or spot is unavailable, contract-state payloads intentionally omit Greek fields and Nexus blocks strategies that require `delta`/`gamma`.

### 3. Underlying State Window

Recommended key:

- `equity:live:window_state:{symbol}:{window}`
- or reuse an existing equity live context if it already carries these fields.

Required fields:

| Field | Notes |
|---|---|
| `symbol` | Underlying symbol. |
| `as_of` | State timestamp. |
| `underlying_mid` | Latest mid/last used by Nexus. |
| `vwap` | Intraday VWAP if available. |
| `minute_bars` or summarized fields | Needed for persistence. |
| `bullish_minutes`, `bearish_minutes` | Momentum-specialist inputs. |
| `range_pct` | Window range. |
| `freshness_ms` | Staleness guard. |

Current state:

- Equity live context exists elsewhere, but Nexus currently expects `underlying_mid` inside option trade events.
- Runtime behavior: Nexus now enriches option events from `equity:live:context:{symbol}` using `price` plus `lastPriceUtc`. It blocks the enriched value when `warmupComplete=false`, `priceDataStale=true`, or the timestamp is outside the configured freshness window.

### 4. Volatility Context

Existing keys:

- `options:live:iv_surface:{symbol}`
- `options:live:vrp:{symbol}`
- optional `stats:{symbol}:iv_rank`

Required fields:

| Field | Notes |
|---|---|
| `atm_iv` | Current ATM implied volatility. |
| `iv_rank` or `iv_percentile` | Must define lookback and calculation method. |
| `vrp_regime` | Cheap, fair, rich, elevated, etc. |
| `as_of` | Timestamp. |
| `quality` | Surface quality/fallback status. |

Current state:

- IV surface and VRP are available.
- Nexus should add freshness and quality checks before using them for live strategy gating.

### 5. Dealer Positioning Context

Existing key:

- `options:live:gex:{symbol}`

Required fields:

| Field | Notes |
|---|---|
| `net_gex` | Dealer gamma exposure. |
| `gamma_regime` | Positive, negative, transitional. |
| `as_of` | Timestamp. |
| `coverage` | Baseline/chain coverage warning if available. |
| `quality` | Stale or degraded flags. |

Current state:

- GEX is available but Nexus should fail closed or degrade explicitly when stale/unavailable.

## Current Gaps To Close Before Trusting Live Signals

1. Deploy the options live-worker contract-state publisher.
2. Verify actual production Redis fields for `options:live:contract_state:{rawSymbol}` after deployment.
3. Run `nexus-audit-features --symbol SPY --limit 5` during market data flow and confirm raw trade events can be enriched to strategy-ready shape.
4. Capture one production Redis sample and keep it as the replay fixture if the payload shape differs from the current raw msgpack test.

## Recommended Validation Order

1. Deploy the options live-worker contract-state publisher.
2. Run `nexus-audit-features --symbol SPY --limit 5` during market data flow and confirm contract-state fields arrive.
3. Verify `md:SPY:options:trades` can supply or be enriched with `aggressor` and `is_sweep`; current local tests cover raw msgpack trades enriched by contract-state quotes.
4. Replace or extend the replay fixture if the production Redis sample has fields not covered by the raw msgpack test.
5. Only after parity is proven, enable all strategies live with diagnostics stored for EOD review.

## Runtime Freshness Limits

| Feature | Default max age | Config |
|---|---:|---|
| IV / VRP context | 120 seconds | `NEXUS_VOL_CONTEXT_MAX_AGE_SECONDS` |
| GEX context | 120 seconds | `NEXUS_GEX_CONTEXT_MAX_AGE_SECONDS` |
| Underlying state | 5 seconds | `NEXUS_UNDERLYING_MAX_AGE_SECONDS` |
| Option quote / mid | 5 seconds | `NEXUS_OPTION_QUOTE_MAX_AGE_SECONDS` |
| Greeks | 60 seconds | `NEXUS_GREEK_MAX_AGE_SECONDS` |

Timestamp-less context keys are blocked by default through `NEXUS_REQUIRE_CONTEXT_TIMESTAMPS=true`. This is intentional: scalar values can still be read by `get_context()`, but strategy gates require timestamped context before allowing live decisions.

Live OPRA trade payloads use Databento `side` as trade-side/aggressor metadata, not option type. Nexus must derive option `side = C | P` from OSI `raw_symbol`; numeric Databento sides such as `65`, `66`, and `78` map to aggressor `A`, `B`, and neutral/mid `M`. This prevents call/put premium windows from being flattened into `side=78`.

When `options:live:vrp:{symbol}` is fresh but does not carry `ivRank` or a recognized `vrpRegime`, Nexus may satisfy the `iv_rank` quality gate with a conservative fallback value. This is for diagnostics/window views only; it must not create a cheap-vol signal.

## Enriched Option Market Context Feed

Nexus also publishes a non-strategy market-context feed for `strategy-fit` and UI consumers. This feed is intentionally separate from `WINDOW_VIEW` and should not be interpreted as a trade recommendation.

| Artifact | Name | Notes |
|---|---|---|
| Completed-window key | `nexus_option_market_context:{symbol}:{window_id}` | Full payload for a completed 30-minute context window; expires after 48 hours. |
| Latest key | `nexus_option_market_context:{symbol}:latest` | Latest completed context window for API aggregation; expires after 8 hours. |
| Pub/Sub | `signal:option_market_context` | Full payload for subscribers. |

Market-context windows run every 30 minutes from `09:30-16:00` ET, plus optional `16:00-16:15`. Strategy decision slots remain limited to researched strategy windows. If the worker restarts mid-session, it resumes with the latest completed window rather than publishing a catch-up burst for all earlier windows.

Payload includes premium totals, call/put premium bias, trade/contract counts, most-traded contracts, cheapest/costliest contracts, cheap/costly side, liquidity quality, pricing quality, and late-event impact.

Cheap/costly contract and side reads are only emitted when Nexus has point-in-time quote evidence. A pricing profile requires option quote timestamps close to the trade event, a baseline at least `NEXUS_PRICING_LAG_MIN_BASELINE_SECONDS` old, and non-flat option or underlying movement. If those checks fail, `pricing_quality` is `unknown` or `degraded`, `pricing_quality_reason` explains why, and narratives must not say a side appears cheap.

## Published Nexus Messages

- `STRATEGY_AUDIT`
  - Redis key: `nexus_strategy_audit:{symbol}:{strategy}:{entry_label}`
  - Pub/Sub: `signal:strategy_audit` and `signal:strategy_audit:{strategy}`
  - Meaning: per-lane stage-0 decision audit for every completed-window strategy check
  - Decisions include `NO_BET`, `BLOCKED`, `SKIPPED`, and `WINDOW_VIEW`
  - Payload includes the failed `reason`, window input totals, dominance thresholds, and optional `feature_failures`
- `INTERMEDIATE`
  - Redis key: `nexus_intermediate:{symbol}:{strategy}:{entry_label}`
  - Pub/Sub: `nexus_intermediate:updates` with symbol and `signal:intermediate:{strategy}` with full payload
  - Meaning: strategy heuristic passed and the setup is a stage-1 candidate, but no final `BET` has been published yet
- `BET`
  - Redis key: `nexus_live_overlay:{symbol}`
  - Pub/Sub: `nexus_live_overlay:updates`
  - Payload includes the exact traded `raw_symbol`, `expiry_date`, `strike`, `option_side`, `entry_quote`, `quote_freshness`, `quote_valid_until`, and an `execution` block with the current reference price and max published slippage
- `WINDOW_VIEW`
  - Redis key: `nexus_window_view:{symbol}:{strategy}:{entry_label}`
  - Pub/Sub: `signal:window_view:{strategy}`
  - `BLOCKED` diagnostics use the same key and Pub/Sub family with `decision="BLOCKED"` when a strategy cannot safely form a view because live inputs are missing, stale, or untradable
  - `SKIPPED` diagnostics use the same key and Pub/Sub family with `decision="SKIPPED"` when a strategy lane is not applicable to the symbol or completed window
  - The key family is the complete per-strategy/window ledger. Post-session review should expect one latest row per evaluated `symbol + strategy + entry_label`, not only directional `WINDOW_VIEW` rows.
- `WINDOW_PRICING`
  - Redis key: `nexus_window_pricing:{symbol}:{entry_label}`
  - Pub/Sub: `signal:window_pricing`
  - Includes `pricing_quality` and `pricing_quality_reason`; cheap/costly fields are null when point-in-time pricing evidence is not reliable.
- `WINDOW_LATE_EVENT`
  - Redis key: `nexus_window_late_event:{symbol}:{entry_label}`
  - Pub/Sub: `signal:window_late_event`
  - Audit-only payload for delayed trades that belong to an already evaluated window; includes late event count, raw symbol, side, and call/put premium impact
- `OPTION_MARKET_CONTEXT`
  - Redis key: `nexus_option_market_context:{symbol}:{window_id}` and `nexus_option_market_context:{symbol}:latest`
  - TTL: completed-window keys expire after 48 hours; latest keys expire after 8 hours
  - Pub/Sub: `signal:option_market_context`
  - Full-session non-strategy market context for pricing and contract-activity consumers
- `PARTICIPANT_FLOW_CONTEXT`
  - Redis key: `nexus_participant_flow_context:{symbol}:{window_key}` and `nexus_participant_flow_context:{symbol}:latest`
  - TTL: completed-window keys expire after 48 hours; latest keys expire after 8 hours
  - Pub/Sub: `signal:participant_flow_context`
  - Full-session participant flow context for completed 30-minute windows
  - Includes `window_side_read`, `retail_like_flow`, `institutional_like_flow`, `dealer_inferred_pressure`, `dominant_strategy_shape`, `top_contracts`, and `data_quality`
  - v1 publishes `dealer_inferred_pressure` as `unknown` until dealer context reads are wired
- `HEALTH`
  - Redis key: `health:nexus` unless `NEXUS_HEALTH_KEY` overrides it
  - Source scope: `NEXUS_HEALTH_SYMBOLS`, default `SPY,QQQ,IWM,UVXY`
  - Includes per-symbol input stream offsets, consumed counts, output family publish counts, blocked-message reason counts, and last worker errors
  - This is a sidecar monitoring contract; strategy and market-context consumers should continue to read the canonical Nexus message keys above
  - Persistence: the live persistence worker stores this key in `live.pipeline_component_health`
  - Contract-state persistence is event-gated: Nexus emits `source=sigmatiq_nexus_contract_reference` persistence events only for contract-state/tradability keys used by candidate/window evaluation, and the persistence worker rejects broad raw-contract persistence without that source marker
  - v1 does not attempt spread/structure heuristic detection
  - TTL: 48 hours for window keys, 8 hours for latest key
  - See `docs/NEXUS_PARTICIPANT_FLOW_CONTEXT_DESIGN.md` for full schema and labeling taxonomy
- `LIQUIDATE`
  - Redis key: `nexus_live_overlay:{symbol}`
  - Pub/Sub: `nexus_live_overlay:updates`
  - Exit return is computed from the exact tracked contract `raw_symbol` using live contract-state / tradability Redis payloads, and the payload now carries `quote_freshness`, `quote_valid_until`, and an `execution` block for the exit quote snapshot
