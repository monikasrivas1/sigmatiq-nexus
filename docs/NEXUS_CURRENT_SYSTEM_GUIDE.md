# Nexus Current System Guide

Current-state guide for what Nexus publishes today, what each message is useful for, and how `sigmatiq-api` and future MCP/trader-agent tools should consume it.

This document describes implemented/current contracts unless a section is explicitly marked as a gap or future integration.

## Current Posture

- Nexus is a research and paper-signal engine, not a live auto-execution service.
- The default live universe is controlled by `NEXUS_SYMBOLS`; current defaults are `SPY,QQQ,IWM,UVXY` unless deployment config overrides them.
- Morning strategy decisions are limited to researched completed windows from `09:30-12:00` ET.
- Full-session context feeds continue every 30 minutes from `09:30-16:00` ET, plus optional `16:00-16:15`.
- Nexus publishes deterministic facts, context, paper lifecycle events, and deterministic narratives. It should not publish user-directed trading instructions.
- `BET` and spread messages are paper-only posture unless a separate execution layer explicitly consumes and authorizes them.

## Runtime Data Flow

```text
Databento / market data
  -> ingestion worker
  -> Redis streams: md:{symbol}:options:trades
  -> Nexus enrichment, window aggregation, strategy checks
  -> Redis keys, Pub/Sub channels, persistence stream
  -> sigmatiq-api read-only endpoints
  -> UI / MCP / chat clients
```

MCP and chat clients should call `sigmatiq-api` endpoints. They should not read Nexus Redis keys directly unless they are operational/debug tooling.

## Upstream Inputs Nexus Uses

| Input | Example key or stream | Current use |
|---|---|---|
| Option trades | `md:{symbol}:options:trades` | Primary stream for window aggregation, strategy checks, participant-flow labeling, pricing context, and heavily traded contracts. |
| Equity context | `equity:live:context:{symbol}` | Underlying state used for enrichment, filters, and quote/price context. |
| Contract tradability | `options:live:tradability:{raw_symbol}` | Final quote gate and liquidation tracking for exact contract. |
| Contract state | `options:live:contract_state:{raw_symbol}` | Live bid/ask/mid, Greeks, timestamps, and exact-contract tracking. |
| IV surface | `options:live:iv_surface:{symbol}` | Vol context for strategy filters and market context. |
| VRP | `options:live:vrp:{symbol}` | Vol pricing context for strategy filters. |
| GEX | `options:live:gex:{symbol}` | Live gamma context for strategy and market-state filters. |
| Canonical live context only | `options:live:iv_surface:{symbol}`, `options:live:vrp:{symbol}`, `options:live:gex:{symbol}` | Strategy readiness uses the canonical live-worker payloads directly; legacy `stats:{symbol}:*` fallbacks are not accepted. |

## Published Nexus Messages

| Message | Redis key(s) | Pub/Sub | Schedule | Primary use | Trade recommendation? |
|---|---|---|---|---|---|
| `STRATEGY_AUDIT` | `nexus_strategy_audit:{symbol}:{strategy}:{entry_label}` | `signal:strategy_audit`, `signal:strategy_audit:{strategy}` | Completed strategy windows | Per-lane decision audit for every strategy check, including skipped/not-applicable lanes, feature blocks, `NO_BET` gates, and directional window reads. | No |
| `INTERMEDIATE` | `nexus_intermediate:{symbol}:{strategy}:{entry_label}` | `nexus_intermediate:updates`, `signal:intermediate:{strategy}` | Strategy windows | Stage-1 paper candidate before final quote gate or final decision. | No |
| `BET` | `nexus_live_overlay:{symbol}` | `nexus_live_overlay:updates` | Strategy windows, only when a strategy passes | Final paper signal with exact `raw_symbol`, quote snapshot, reference price, freshness, and execution metadata. | Paper signal only |
| `WINDOW_VIEW` | `nexus_window_view:{symbol}:{strategy}:{entry_label}` | `signal:window_view:{strategy}` | Completed strategy windows | Strategy-specific directional read for the completed window, even if no trade candidate is emitted. | No |
| `BLOCKED` | Same key family as `WINDOW_VIEW` | Same channel family as `WINDOW_VIEW` | Completed strategy windows | Fail-closed diagnostic when a strategy cannot safely form a view or trade candidate because required live fields are missing, stale, or untradable. | No |
| `SKIPPED` | Same key family as `WINDOW_VIEW` | Same channel family as `WINDOW_VIEW` | Completed strategy windows | Explicit diagnostic when a strategy lane is not applicable to the symbol or completed window. | No |
| `WINDOW_PRICING` | `nexus_window_pricing:{symbol}:{entry_label}` | `signal:window_pricing` | Completed windows | Cheap/costly contract and side read when point-in-time pricing evidence is reliable. | No |
| `WINDOW_LATE_EVENT` | `nexus_window_late_event:{symbol}:{entry_label}` | `signal:window_late_event` | After a completed window is already evaluated | Audit-only record that late trades arrived for a frozen window. | No |
| `OPTION_MARKET_CONTEXT` | `nexus_option_market_context:{symbol}:{window_id}`, `nexus_option_market_context:{symbol}:latest` | `signal:option_market_context` | Full session, completed 30-minute windows | Broad option-market context: premium totals, most-traded contracts, cheap/costly contracts, liquidity quality, pricing quality, and late-event impact. | No |
| `PARTICIPANT_FLOW_CONTEXT` | `nexus_participant_flow_context:{symbol}:{window_key}`, `nexus_participant_flow_context:{symbol}:latest` | `signal:participant_flow_context` | Full session, completed 30-minute windows | Trade-shape and participant-like flow context for the latest completed window. | No |
| `LIQUIDATE` | `nexus_live_overlay:{symbol}` | `nexus_live_overlay:updates` | Active paper-position monitoring | Paper exit context for the exact tracked `raw_symbol`, with exit quote freshness and execution metadata. | Paper lifecycle only |
| `HEALTH` | `health:nexus` | None | On input offset persistence, output publication, and error capture | Sidecar component health for `SPY`, `QQQ`, `IWM`, and `UVXY`: input offsets, output counts, blocked reasons, and last errors. | No |

### Message TTLs

- `OPTION_MARKET_CONTEXT` completed-window keys expire after 48 hours.
- `OPTION_MARKET_CONTEXT` latest keys expire after 8 hours.
- `PARTICIPANT_FLOW_CONTEXT` completed-window keys expire after 48 hours.
- `PARTICIPANT_FLOW_CONTEXT` latest keys expire after 8 hours.
- Paper lifecycle key TTLs are controlled by the runtime lock and active-position settings.

## What Each Message Answers

| Trader or agent question | Primary message | Notes |
|---|---|---|
| Why did this strategy not produce a paper signal? | `STRATEGY_AUDIT` | Use `decision`, `reason`, `inputs`, and optional `feature_failures` for the exact failed gate and threshold context. |
| What did this strategy think about the completed window? | `WINDOW_VIEW` | Directional read only. It is intentionally weaker than a trade signal. |
| Why did a strategy not form a usable read? | `BLOCKED` | Use `reason_summary`, missing fields, freshness, and data-quality fields. |
| Did a strategy candidate appear before final validation? | `INTERMEDIATE` | Useful for audit and model/debug review. |
| What paper signal did Nexus emit? | `BET` | Includes exact contract and quote evidence; still paper-only. |
| Why was a paper signal exited? | `LIQUIDATE` | Uses the exact tracked contract, not a generic same-symbol option. |
| Which contracts were heavily traded? | `OPTION_MARKET_CONTEXT` | Use top-contract fields and premium totals. |
| Which contracts or sides looked cheap or costly? | `OPTION_MARKET_CONTEXT` or `WINDOW_PRICING` | Only trust this when `pricing_quality` is reliable. |
| Was flow call-heavy, put-heavy, or unclear? | `PARTICIPANT_FLOW_CONTEXT` | Uses completed-window participant-like flow labels and data quality. |
| Did late data arrive after the window was frozen? | `WINDOW_LATE_EVENT` | Audit only; it does not retrade or re-evaluate the frozen window. |
| What is different in this window today versus yesterday? | Future day-over-day context | See `NEXUS_SAME_WINDOW_DAY_OVER_DAY_CONTEXT_DESIGN.md`; not current Redis latest-only behavior. |

## Deterministic Narratives

Current Nexus messages include deterministic narrative fields so UI, API, and future MCP tools can explain the payload without asking an LLM to infer meaning from raw numbers.

| Message family | Narrative fields |
|---|---|
| `OPTION_MARKET_CONTEXT` | `narrative_version`, `summary`, `narrative` |
| `PARTICIPANT_FLOW_CONTEXT` | `narrative_version`, `summary`, `narrative` |
| `WINDOW_VIEW` | `narrative_version`, `summary`, `reason_summary` |
| `WINDOW_PRICING` | `narrative_version`, `summary`, `reason_summary` |
| `WINDOW_LATE_EVENT` | `narrative_version`, `reason_summary` |
| `INTERMEDIATE` | `narrative_version`, `reason_summary` |
| `BET` | `narrative_version`, `reason_summary` |
| `BLOCKED` | `narrative_version`, `reason_summary` |
| `LIQUIDATE` | `narrative_version`, `reason_summary` |

Narratives must preserve uncertainty. Degraded, stale, thin, or missing data should soften the read and expose caveats rather than sounding decisive.

## sigmatiq-api Integration

| Endpoint | Nexus source | Current purpose |
|---|---|---|
| `GET /v1/live/symbols` | Core live readiness Redis keys, not Nexus strategy messages | Returns which symbols have enough live data for strategy-fit and live prompts. Call this before symbol-specific prompt flows when the symbol is not fixed. |
| `GET /v1/live/{symbol}/strategy-fit?expiry=0dte&risk_profile=defined_risk` | Live flow, GEX, vol-context, spot, and optional `nexus_option_market_context:{symbol}:latest` | Live-first strategy-family fit ranking. It should not use `WINDOW_VIEW` as an input. |
| `GET /v1/live/{symbol}/participant-flow-context` | `nexus_participant_flow_context:{symbol}:latest` by default, or `nexus_participant_flow_context:{symbol}:{window_key}` when requested | Read-through endpoint for participant-flow context. It does not recompute labels. |

### API Boundaries

- `strategy-fit` ranks strategy families from current live state. It should not choose exact contracts, size, entry timing, or exits.
- `participant-flow-context` explains observed and inferred completed-window flow. It should not claim true account identity.
- Direct standalone API access to `OPTION_MARKET_CONTEXT` is a useful next endpoint if traders need contract/pricing context outside `strategy-fit`.
- API responses should pass Nexus `summary`, `narrative`, `reason_summary`, `freshness`, and `data_quality` through unchanged whenever possible.

## MCP And Chat Integration

MCP tools should wrap `sigmatiq-api`, not Redis. That keeps auth, readiness, freshness, and contract validation in one backend layer.

Recommended MCP tools:

| Tool | Backing endpoint | Use |
|---|---|---|
| `live_symbols` | `GET /v1/live/symbols` | Discover or validate the live-ready universe. |
| `live_strategy_fit` | `GET /v1/live/{symbol}/strategy-fit` | Answer strategy-family fit questions with direction, regime, pricing, ranked fits, and no-trade reasons. |
| `live_participant_flow_context` | `GET /v1/live/{symbol}/participant-flow-context` | Answer latest completed-window flow-shape questions. |
| `live_option_market_context` | Future direct endpoint over `nexus_option_market_context:{symbol}:latest` | Answer contract activity and cheap/costly context questions directly. |
| `live_window_audit` | Future endpoint over `WINDOW_VIEW`, `BLOCKED`, `WINDOW_PRICING`, and late-event keys | Answer why a strategy did or did not produce a paper signal. |
| `live_same_window_day_over_day_context` | Future endpoint based on persisted history or same-window context design | Answer today-versus-yesterday window comparison questions. |

### Prompt Grounding Rules

- Call `live_symbols` first when a user asks broadly or when symbol readiness is unknown.
- Use `strategy-fit` for strategy-family fit, not for exact trade instructions.
- Use `participant-flow-context` for latest completed-window flow-shape questions.
- Use option-market context for contract activity and pricing-quality questions once a direct API is exposed.
- Do not use Nexus `WINDOW_VIEW` as an input to `strategy-fit`.
- Always surface stale, missing, degraded, and low-confidence flags.
- Avoid imperative trade language such as `buy this`, `sell this`, `enter now`, or `guaranteed`.

## Prompt And Use-Case Map

| User question | API or MCP call | Fields to ground on | Guardrail |
|---|---|---|---|
| Which symbols can I ask about live? | `live_symbols` | `symbols`, readiness, missing sources | Readiness only; do not rank symbols. |
| What strategy family fits SPY right now? | `live_strategy_fit` | `status`, `direction`, `regime`, `pricing`, `ranked_fits`, `status_reasons` | Explain as fit, not advice. Include `no_trade` when ranked first. |
| What side is flow leaning in the latest SPY window? | `live_participant_flow_context` | `window_side_read`, `summary`, `data_quality` | Say latest completed window, not tick-now. |
| Are retail-like or block-like traders active? | `live_participant_flow_context` | participant-like flow buckets and caveats | Say inferred from trade shape, not true identity. |
| Which contracts were most traded or looked cheap/costly? | Future `live_option_market_context`, or `strategy-fit` if it embeds option context | top contracts, cheap/costly fields, `pricing_quality` | If pricing quality is unknown/degraded, do not call a side cheap. |
| Why did Nexus not fire today? | Future `live_window_audit` | `STRATEGY_AUDIT`, `BLOCKED`, `WINDOW_VIEW`, `WINDOW_PRICING`, late events | Strategy audit is now the canonical Redis source; API coverage still needs a read-through endpoint. |
| What changed from yesterday in this window? | Future day-over-day endpoint | same-window aggregates, deltas, narratives | Requires persisted history beyond Redis latest TTL. |

## Current Gaps

- A standalone `GET /v1/live/{symbol}/option-market-context` endpoint is not confirmed in the current API surface. `strategy-fit` can consume option-market context internally, but traders may need direct access.
- There is no confirmed current API endpoint for the full `STRATEGY_AUDIT` / `WINDOW_VIEW` / `BLOCKED` / `WINDOW_PRICING` audit surface.
- MCP tools still need to be mapped to the approved `sigmatiq-api` endpoints.
- Day-over-day same-window comparison is designed separately but requires persisted history and an API surface before chat can answer it reliably.
- Participant-flow `dealer_inferred_pressure` is currently `unknown` until dealer context is wired.
- Participant-flow v1 does not attempt full spread or structure heuristic detection.
- `README.md` still contains older `signal:final:*` wording; prefer `docs/NEXUS_LIVE_FEATURE_CONTRACT.md` and this guide for the current publish surface until README is aligned.

## Related Docs

- `docs/NEXUS_LIVE_FEATURE_CONTRACT.md` - current live feature and message contract.
- `docs/NEXUS_PARTICIPANT_FLOW_CONTEXT_DESIGN.md` - participant-flow schema and labeling design.
- `docs/NEXUS_DETERMINISTIC_MESSAGE_NARRATIVES_DESIGN.md` - deterministic summary and narrative rules.
- `docs/NEXUS_SAME_WINDOW_DAY_OVER_DAY_CONTEXT_DESIGN.md` - future same-window comparison design.
- `docs/nexus-v2/` - future platform architecture and product direction.
- `../sigmatiq-api/docs/api/live-symbols.md` - live symbol readiness endpoint.
- `../sigmatiq-api/docs/api/strategy-fit.md` - strategy-fit API design and source rules.
