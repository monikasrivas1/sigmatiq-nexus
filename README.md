# Sigmatiq Nexus

A high-performance strategy engine for real-time institutional flow analysis.

## Architecture
The Nexus acts as a middle-tier between **Ingestion** and **Execution**.

1. **Ingest (C#):** Pushes raw option trades to `md:options:trades` Redis Stream.
2. **Nexus (Python/Polars):** 
   - **Stage 1 (Heuristic):** Uses Polars to calculate Delta-Skew/Aggressor bias in microseconds.
   - **Stage 2 (AI):** Uses ONNX Runtime to validate trajectories with the RL Brain.
3. **Decisions:** Publishes `BET` or `PASS` results to `signal:final:*` Redis channels.
4. **Persistence:** Appends final/intermediate/final-block payloads to `live:persistence:events`; `Sigmatiq.Options.LivePersistenceWorker` stores them in `live.nexus_strategy_signal` for EOD review. All three lifecycle payloads now carry numeric `confidence` so the persisted `live.nexus_strategy_signal.confidence` column stays populated for stage-1 candidates, final bets, and final-blocked outcomes.

## Live decision contract

- Decisions are based on completed New York trading windows, not wall-clock time on the worker host.
- `10:00` evaluates `09:30-10:00`, `10:30` evaluates `10:00-10:30`, `11:00` evaluates `10:30-11:00`, `11:30` evaluates `11:00-11:30`, and `12:00` evaluates `11:30-12:00`.
- Completed windows are evaluated after `NEXUS_WINDOW_EVALUATION_GRACE_SECONDS`, so the first post-boundary trade does not immediately freeze the window.
- If a trade arrives for a window that was already evaluated, Nexus publishes `WINDOW_LATE_EVENT` as audit-only evidence with late premium totals; it does not re-evaluate or re-trade that window.
- Redis Stream offsets are persisted per input stream under `NEXUS_STREAM_OFFSET_KEY`, so worker restarts resume from the last processed ID instead of starting from new-only `$`.
- Live mode is current-session only. If a delayed/replayed Redis Stream event belongs to a different New York session than the active worker session, Nexus ignores it and logs `stale_session_event_ignored`. Set `NEXUS_REPLAY_MODE=true` only for explicit replay/backfill runs.
- First trigger wins by default per `session_date + symbol`, so each symbol has one independent trade lane per NY session.
- Final single-leg `BET` messages acquire Redis-backed first-trigger locks before publishing, so restarts do not reopen the same symbol lane.
- `etf_confluence_sniper` also participates in a separate shared group lock across the configured ETF universe, controlled by `NEXUS_GROUP_LOCK_STRATEGIES`.
- Result: `SPY` and `QQQ` can each fire independently, while the group confluence lane still remains single-fire.
- `etf_confluence_sniper` is evaluated first for each eligible window.
- `etf_open_specialist` is the explicit 10:00 ET cheap-call rule for the completed 09:30-10:00 window.
- `etf_open_specialist` `WINDOW_VIEW` payloads persist the actual decision evidence used for that read: `iv_rank`, total/call/put premium, premium shares, put-call ratio, dominant side, and the gating thresholds.
- `PARTICIPANT_FLOW_CONTEXT` payloads persist `window_side_read.dominant_side` alongside `directional_read` and `confidence`, and the persistence worker projects that field into `live.nexus_participant_flow_context.dominant_side`.
- `PARTICIPANT_FLOW_CONTEXT` no longer degrades every usable window for missing open/close intent alone; degradation now stays tied to real label-quality or missing-aggressor problems, and dealer pressure uses a bounded net-GEX-plus-flow heuristic when fresh context exists.
- `PARTICIPANT_FLOW_CONTEXT` also treats repeated high-premium contract clusters with a clear premium-side bias as structurally usable, even when the clustered prints still label as low-confidence directional buying because quotes were wide. That keeps main-symbol windows from failing closed on retail-noise alone while still degrading windows where bid-side premium materially overwhelms confirmed ask-side flow.
- `etf_put_credit_open30_spread` is the paper-only 10:00 ET vertical put-credit spread read for SPY/QQQ when open30 call premium dominates and a same-expiry spread can be quoted at the minimum credit.
- `etf_call_credit_open30_spread` is the paper-only 10:00 ET vertical call-credit spread read for SPY only when open30 put premium dominates and a same-expiry spread can be quoted at the minimum credit.
- Spread open30 readiness is gated on completed-window trade identity plus fresh `iv_rank`; per-contract `delta` and executable quotes are resolved later during spread candidate selection from live contract-state keys, rather than forcing the raw window rows to already carry those fields.
- Spread strategy window-view ledger rows are published for every evaluated lane. Applicable lanes emit `decision="WINDOW_VIEW"` or `decision="BLOCKED"`; unsupported symbols or later windows emit `decision="SKIPPED"` with `reason="strategy_not_applicable_to_symbol_or_window"` so the absence of a trade is visible in both Redis and persisted history.
- `etf_low_sweep_core` remains available as the tested low-sweep candidate; `etf_flow_specialist` and `etf_momentum_specialist` are restricted to their researched 10:30 and 11:00 entry windows.
- Every implemented strategy now publishes a per-window `WINDOW_VIEW` sentiment for every completed window from `09:30-12:00` ET, independent of whether that strategy is allowed to emit a trade candidate in that slot.
- Strategy audit and window-view payloads include a normalized `gates` array. Each gate reports `name`, `scope`, `severity`, `status`, `hard_block`, `reason`, and `details` so operators can distinguish applicability skips, hard live-feature blocks, soft heuristic failures, and window-view-only reads without reverse-engineering a single `reason` string.
- Nexus also publishes one per-window `WINDOW_PRICING` message per symbol/window with the cheapest contract, costliest contract, and cheap/costly side summary derived from pricing-lag inside that completed window. The pricing builder now uses the executed trade price as the baseline and the latest fresh live contract snapshot as the end state, so a contract can contribute a pricing profile even when it only printed once inside the window. When a raw trade omits explicit `price` but includes `premium` and `size`, Nexus derives the baseline trade price from `premium / size / 100` before scoring pricing lag.
- `OPTION_MARKET_CONTEXT` payloads include `session_date` plus a nested `data_quality` block with `status`, `missing`, `degraded`, and `reason_codes`, and the persistence worker projects `data_quality.status` into the typed `live.nexus_option_market_context.data_quality_status` column.
- `OPTION_MARKET_CONTEXT` payloads also carry `schema_version`, `worker_name`, `worker_version`, and `pricing_profile_count` so operators can tell whether a row came from the current Nexus build and whether cheap/costly pricing context was derived from zero, one, or more reliable pricing profiles.
- Each strategy now has a fail-closed feature gate. If required live fields are missing, Nexus emits a stage `0` `BLOCKED` diagnostic on the same `nexus_window_view:{symbol}:{strategy}:{entry_label}` key family and skips the strategy instead of defaulting missing booleans/numbers.
- Final `BET` payloads now carry the exact option `raw_symbol`, a live entry quote snapshot, quote freshness, quote-valid-until, and an explicit execution policy block. Runtime liquidation tracks that exact contract via `options:live:contract_state:{raw_symbol}` / `options:live:tradability:{raw_symbol}` instead of comparing against unrelated same-symbol option trades.
- Completed-window and candidate quote lookups append `source=sigmatiq_nexus_contract_reference` persistence events for the exact `options:live:contract_state:{raw_symbol}` / `options:live:tradability:{raw_symbol}` keys Nexus used. The persistence worker stores only those referenced contracts, not the full raw option universe.
- Single-leg final `BET` publishing fails closed when the exact contract has no fresh executable quote; the fresh quote reference becomes the recorded entry price.
- Single-leg active positions are persisted to Redis and restored on worker startup for the current NY session.
- Spread `BET` payloads are paper-only, publish to `nexus_spread_overlay:{symbol}:{strategy}:{entry_label}`, and are not attached to the current single-leg liquidation loop.


## Enriched Option Market Context

Nexus publishes a market-context feed separate from strategy signals. Strategy decisions remain limited to researched morning windows, but option market context continues across the full session.

- Completed-window key: `nexus_option_market_context:{symbol}:{window_id}`
- Latest key: `nexus_option_market_context:{symbol}:latest`
- Pub/Sub: `signal:option_market_context`
- Windows: every 30 minutes from `09:30-16:00` ET plus optional `16:00-16:15`
- Purpose: provide premiums, heavily traded contracts, cheap/costly contracts, cheap/costly side, liquidity quality, pricing quality, and late-event impact to API/UI consumers
- Boundary: this feed is not a strategy signal and does not use `WINDOW_VIEW` sentiment

## Runtime configuration

- `REDIS_URL`: Redis connection URL used by the worker.
- `NEXUS_REDIS_CLUSTER`: set to `true` for Azure clustered Redis. In cluster mode Nexus reads each configured input stream separately so Redis does not reject multi-key `XREAD` calls across hash slots.
- `NEXUS_INPUT_STREAM`: optional explicit Redis stream. If absent, Nexus consumes `md:{symbol}:options:trades`.
- `NEXUS_STREAM_START_ID`: initial Redis Stream ID when no stored offset exists, default `0-0`.
- `NEXUS_STREAM_OFFSET_KEY`: Redis key template for saved stream offsets, default `nexus:stream_offset:{stream}`.
- `NEXUS_REPLAY_MODE`: set to `true` only for replay/backfill materialization. Default `false` makes live Nexus ignore events whose NY session date does not match the active live session.
- `NEXUS_SYMBOLS`: comma-separated symbols to process, default `SPY,QQQ,IWM,UVXY`.
- `NEXUS_HEALTH_SYMBOLS`: comma-separated symbols that must appear in Nexus health, default `SPY,QQQ,IWM,UVXY`.
- `NEXUS_HEALTH_KEY`: Redis key for the Nexus health payload, default `health:nexus`.
- `NEXUS_GROUP_LOCK_STRATEGIES`: comma-separated strategy names that should also share one cross-symbol group lock, default `etf_confluence_sniper`.
- `NEXUS_LOCK_TTL_SECONDS`: Redis first-trigger lock TTL, default `28800`.
- `NEXUS_ACTIVE_POSITION_TTL_SECONDS`: Redis active-position state TTL, default `28800`.
- `NEXUS_WINDOW_EVALUATION_GRACE_SECONDS`: delay after a decision boundary before evaluating the completed window, default `15`.
- `NEXUS_IV_SURFACE_KEY`, `NEXUS_VRP_KEY`, `NEXUS_GEX_KEY`: canonical live options-worker context key templates, defaulting to `options:live:*:{symbol}` keys. Nexus does not use legacy scalar fallback keys for strategy readiness.
- `NEXUS_EQUITY_CONTEXT_KEY`: equity live context key template, default `equity:live:context:{symbol}`.
- `NEXUS_CONTRACT_TRADABILITY_KEY`: option tradability key template, default `options:live:tradability:{raw_symbol}`.
- `NEXUS_CONTRACT_STATE_KEY`: per-contract quote plus Greek state key template, default `options:live:contract_state:{raw_symbol}`.
- `NEXUS_REQUIRE_CONTEXT_TIMESTAMPS`: default `true`; timestamp-less scalar context keys are blocked for live strategy decisions.
- `NEXUS_VOL_CONTEXT_MAX_AGE_SECONDS`: max IV/VRP context age, default `120`.
- `NEXUS_GEX_CONTEXT_MAX_AGE_SECONDS`: max GEX context age, default `120`.
- `NEXUS_UNDERLYING_MAX_AGE_SECONDS`: max underlying state age on enriched events, default `120`. This matches the equity live-context freshness model, which is published from the 1-minute equity context stream rather than tick-by-tick quote updates.
- `NEXUS_OPTION_QUOTE_MAX_AGE_SECONDS`: max option quote/mid age on enriched events, default `5`.
- `NEXUS_GREEK_MAX_AGE_SECONDS`: max Greek age on enriched events, default `60`.
- `NEXUS_SWEEP_PREMIUM_USD`: quote-derived sweep threshold when raw `is_sweep` is absent, default `25000`.
- `NEXUS_EXECUTION_MAX_SLIPPAGE_PCT`: slippage ceiling published in `BET` and `LIQUIDATE` execution blocks, default `5.0`.
- `NEXUS_SPREAD_STRIKE_WIDTH`: vertical spread width, default `5.0`.
- `NEXUS_SPREAD_TARGET_DELTA`: target absolute short-leg delta for spread candidate selection, default `0.15`.
- `NEXUS_SPREAD_MIN_ENTRY_CREDIT`: minimum net credit required before a spread `BET` is published, default `0.30`.
- `NEXUS_SPREAD_MAX_IV_RANK`: maximum IV rank allowed for spread open30 candidates, default `30.0`.
- `NEXUS_SPREAD_TAKE_PROFIT_PCT`, `NEXUS_SPREAD_STOP_LOSS_PCT`, `NEXUS_SPREAD_HOLD_SECONDS`: paper spread risk metadata, defaults `20.0`, `75.0`, and `1800`.
- `NEXUS_MIN_WINDOW_PREMIUM` and `NEXUS_SIDE_DOMINANCE`: window-level premium and side-dominance thresholds.
- `NEXUS_OPEN_CALL_DOMINANCE`: opening cheap-call dominance threshold, default `1.5`.
- `LIVE_PERSISTENCE_EVENT_STREAM`: Redis Stream for durable signal capture, default `live:persistence:events`.

## Health contract

Nexus publishes a sidecar health payload to `health:nexus`. The payload is fail-closed operational evidence for the four-symbol pilot universe and includes:

- `expectedSymbols`, `activeSymbols`, and `missingSymbols`.
- Per-symbol input stream name, offset key, last consumed offset, last input timestamp, event timestamp, and consumed count.
- Per-symbol output families and publish counts for `window_view`, `window_pricing`, `window_late_event`, `intermediate`, `live_overlay`, `spread_overlay`, `final_block`, `option_market_context`, and `participant_flow_context`.
- Per-symbol blocked-message counts and counts by reason, including final-block and live-feature-quality gate reasons.
- `lastError`, `lastErrorUtc`, and `degradedReasons`.

This key is intended for sigmatiq-api and pipeline monitoring. Nexus still writes the canonical payload keys and appends to `live:persistence:events`; the health key does not replace those contracts.

Operationally, a valid no-trade day means the symbol has fresh input activity and Nexus output families such as `window_view`, `window_pricing`, `option_market_context`, or `participant_flow_context` are advancing, while `final_block` or `BLOCKED` counts explain any fail-closed decisions. An unhealthy pipeline has missing symbol activity, stale or absent offsets, no advancing output families, or a populated `lastError`.

## Feature audit

Before enabling a live strategy, verify the Redis payload shape:

```bash
nexus-audit-features --symbol SPY --limit 5
```

The audit reports which implemented strategies are `ready`, `degraded`, or `blocked` based on the sampled trade event plus live IV/GEX context. It also reports `stale` or `unknown_freshness` when a required field exists but cannot be trusted at decision time. The full feature contract is documented in `docs/NEXUS_LIVE_FEATURE_CONTRACT.md`.

## DB Signal-Potential Audit

After or during a session, use the DB-first signal-potential audit to check whether the persisted raw and materialized live tables could have supported Nexus signals:

```bash
nexus-audit-signal-potential \
  --session-date 2026-05-20 \
  --symbols SPY,QQQ,IWM,UVXY \
  --csv artifacts/nexus_signal_potential_2026-05-20.csv
```

The audit is read-only. It compares `live.prepared_*` materializer rows and raw-readiness projections against persisted Nexus outputs (`nexus_window_view`, `nexus_strategy_audit`, `nexus_strategy_signal`, and `nexus_spread_signal`). It classifies each symbol/window/strategy as not applicable, blocked by data or an obvious gate, candidate requiring model probability, candidate with unmaterialized Redis-only gate inputs, or candidate that should have reached the final quote gate. It does not run ONNX models or claim exact Redis-only gate outcomes; rows marked `needs_review_no_nexus_output` are the ones to inspect first.

With the live DB materializer replay migration deployed, the audit also reads materialized EOD context values, lead-contract quote-as-of-window fields, replay gate status, and explicit missing-input JSON from `live.prepared_nexus_window_context`. Fields that persisted raw OPRA trades cannot prove, such as sweep and true aggressor classification, stay as warnings instead of simulated green checks.

## DB Replay Signal Emitter

The DB replay emitter is an additive recovery/audit path for days where Redis live evaluation missed a candidate but persisted raw and materialized DB rows can prove the minimum decision inputs. It writes `INTERMEDIATE` rows to `live.nexus_strategy_signal` with `source="sigmatiq_nexus_db_replay"` and records idempotence in `live.nexus_db_signal_emission`.

```bash
nexus-db-signal-emitter \
  --session-date 2026-05-20 \
  --symbols SPY,QQQ,IWM,UVXY \
  --strategies etf_confluence_sniper,etf_open_specialist,etf_low_sweep_core,etf_flow_specialist,etf_momentum_specialist,etf_put_credit_open30_spread,etf_call_credit_open30_spread \
  --dry-run
```

For v1 this runner emits only DB-proven `INTERMEDIATE` candidates for the seven primary Nexus strategy lanes:

- `etf_confluence_sniper`: 10:00-12:00 ET pricing/momentum proxy only when DB materialization proves reliable pricing, lead-contract Greeks, quote-as-of-window data, and directional equity/flow alignment.
- `etf_open_specialist`: 10:00 ET open30 bullish call dominance with ready materialized context, healthy EOD context, fresh quote-as-of-window data, required premium, and a call-side lead contract.
- `etf_low_sweep_core`: 10:00/10:30 ET low-sweep side dominance only when DB materialization proves `sweep_quality_status="ready"` and `sweep_ratio <= NEXUS_DB_REPLAY_MAX_SWEEP_RATIO` / `--max-sweep-ratio`.
- `etf_flow_specialist`: 10:00/10:30/11:00 ET directional flow alignment only when DB materialization proves sweep/aggressor quality, healthy IV/GEX context, fresh quote-as-of-window data, and lead-contract Greeks. DB replay writes the heuristic `INTERMEDIATE` candidate and marks model scoring as `not_run_db_replay_intermediate_only`; final model/BET promotion remains a separate step.
- `etf_momentum_specialist`: 11:00 ET price-persistence proxy only when DB materialization proves IV rank, lead-contract Greeks, quote-as-of-window data, and directional equity/flow alignment. DB replay marks model scoring as `not_run_db_replay_intermediate_only`.
- `etf_put_credit_open30_spread`: 10:00 ET SPY/QQQ open30 call-dominance context with IV-rank gating. DB replay records `spread_status="not_constructed_db_replay_intermediate_only"` because exact multileg construction remains live-worker owned.
- `etf_call_credit_open30_spread`: 10:00 ET SPY open30 put-dominance context with IV-rank gating. DB replay records `spread_status="not_constructed_db_replay_intermediate_only"` because exact multileg construction remains live-worker owned.

The CLI is EOD/audit-first, but the evaluator is poll-safe: repeated runs are gated by `live.nexus_db_signal_emission` and existing `live.nexus_strategy_signal` rows. It can therefore be reused by a future realtime worker loop without changing strategy eligibility. It does not publish Redis overlays, execution events, or final `BET` rows; live `BET` remains owned by the Redis Nexus worker.

The same runner can optionally publish relaxed-gate review rows:

```bash
nexus-db-signal-emitter \
  --session-date 2026-05-20 \
  --symbols SPY,QQQ,IWM,UVXY \
  --relaxed-watch-only
```

Relaxed rows use `decision="WATCH"` and `source="sigmatiq_nexus_db_replay_relaxed"`. They are not normal `INTERMEDIATE` candidates. The payload records the failed production gate, actual value, required value, relaxed value, risk note, and a DB counterfactual from `live.raw_option_tcbbo_1s`. Current relaxed-watch coverage is limited to one-gate near misses for flow sweep ratio, momentum IV rank, and open30 spread IV rank. Spread watch rows still mark exact spread construction as not run.

The realtime DB signal worker wraps the same idempotent emitter in a polling loop:

```bash
nexus-db-signal-worker \
  --symbols SPY,QQQ,IWM,UVXY \
  --poll-seconds 30
```

By default the worker emits normal DB replay `INTERMEDIATE` rows and relaxed `WATCH` rows for the current New York session date. Set `NEXUS_DB_SIGNAL_SESSION_DATE=YYYY-MM-DD` or pass `--session-date` only for controlled replay/backfill. The worker logs JSON events to stdout and relies on `live.nexus_db_signal_emission` plus existing `live.nexus_strategy_signal` rows for idempotence, so repeated polls do not duplicate signals. It reads only Postgres materialized/raw-live tables and does not write Redis overlays.

Production runs the polling worker as a separate Azure Container App, `sigmatiq-nexus-db-signal-ca`, from the same `sigmatiq-nexus` image. The normal `sigmatiq-nexus-ca` app still runs the Redis live Nexus worker. The DB signal app overrides the container command to `nexus-db-signal-worker`, uses `SHARED_DATABASE_URL=secretref:shared-db-url`, and polls `SPY,QQQ,IWM,UVXY` every 30 seconds with relaxed watch rows enabled. Realtime worker emissions are freshness-gated by `NEXUS_DB_SIGNAL_MAX_WINDOW_LAG_SECONDS` so old materializer backfills do not appear as newly actionable live signals. The default production lag budget is 300 seconds after the source window closes; use the standalone `nexus-db-signal-emitter` CLI for whole-day replay/audit.

At runtime Nexus enriches raw option trade events from:

- `equity:live:context:{symbol}` for `underlying_mid` and spot freshness.
- `options:live:tradability:{raw_symbol}` for option bid/ask/mid, quote timestamp, spread, and executable/tradability flags.
- `options:live:contract_state:{raw_symbol}` for option mid, quote quality, tradability flags, underlying spot, and Greeks.
- If raw trades omit `aggressor` or `is_sweep`, Nexus derives them only from fresh quote state; stale or untradable quotes still block the relevant strategy.
- If raw trades omit `delta` / `gamma`, Nexus leaves those features missing until live contract state provides them; it does not synthesize `0.0` greeks.

## Operational logging

Nexus now emits structured JSON logs for the decision path so production checks can separate input, gating, and signal failures quickly.

- `worker_started`, `redis_connected`, `stream_batch_received`: confirms the worker is alive and reading Redis streams.
- `slot_candidate_received`, `window_evaluation_started`: confirms events are arriving for a symbol and a completed NY window is being evaluated with real premium totals.
- `stream_offsets_restored`: confirms Redis Stream restart offsets were loaded.
- `window_due_for_evaluation`: confirms a completed window passed the configured grace period and is being evaluated.
- `window_late_event_detected`: confirms a delayed trade landed after its window was already evaluated and was recorded as audit-only impact.
- `strategy_blocked`: emitted when fail-closed feature gates reject a strategy due to missing or stale live fields.
- `strategy_window_view_published`: emitted when a strategy publishes its directional read of a completed window as `BULLISH`, `BEARISH`, or `CHOP`.
- `window_pricing_published`: emitted when Nexus publishes the separate cheap-versus-costly contract summary for the completed strategy window.
- `option_market_context_published`: emitted when Nexus publishes full-session enriched option market context for a completed market-context window.
- `strategy_no_signal`: emitted when data is present but the heuristic or model threshold did not qualify.
- `strategy_intermediate_published`, `strategy_final_published`, `strategy_spread_final_published`, `position_liquidated`: emitted when Nexus actually produces a signal or exits a live position.

Example grep targets:

```bash
rg '"event":"window_evaluation_started"|\"event\":\"strategy_no_signal\"|\"event\":\"strategy_final_published\"'
```

## Tech Stack
- **Polars:** Vectorized trade processing.
- **ONNX Runtime:** Native execution of TCN and RL models.
- **Redis Streams:** Persistent, high-throughput message bus.

## Extension
To add a new strategy:
1. Define a new logic class in `src/sigmatiq_nexus/strategies/`.
2. Add the corresponding `.onnx` model to the `models/` folder.
3. Register the strategy in the main `worker.py` loop.
