# Nexus Strategy Capability Matrix

This matrix defines what each Nexus strategy is uniquely allowed to say. Market reads describe the session. Strategy reads describe whether a specific Nexus lane can use that session.

## Strategy Read Contract

Every Redis and DB strategy payload may expose these safe fields:

| Field | Meaning |
|---|---|
| `strategy_read_label` | Strategy-specific label, such as `confluence_alignment_candidate` or `flow_confirmation_watch`. |
| `strategy_action` | `blocked`, `watch`, `candidate`, `bet`, or `no_trade`. |
| `strategy_capability` | The thing this strategy is designed to detect. |
| `strategy_vehicle` | Safe vehicle family, not a raw contract feed. |
| `strategy_direction` | `bullish`, `bearish`, or `neutral`. |
| `strategy_blockers` | Missing or failed inputs when blocked. |
| `strategy_reason_codes` | Deterministic reasons for the read. |

The UI may show the strategy read and safe trade expression. It must not expose raw live option prints, full bid/ask/mid, greeks, or premium totals.

## Capability Matrix

| Strategy | Capability | Best conditions | Bad conditions | Vehicle read |
|---|---|---|---|---|
| `etf_confluence_sniper` | `confluence_alignment` | Flow, underlying momentum, and cheap-side pricing all agree. | Missing quote/Greek/momentum context, stale quote, or pricing no longer cheap. | `long_option_or_debit_spread` |
| `etf_open_specialist` | `opening_drive` | 09:30-10:00 call dominance with acceptable IV. | Weak open flow, rich IV, or no open-drive impulse. | `long_call_or_call_debit` |
| `etf_low_sweep_core` | `clean_directional_flow` | Directional premium dominance without sweep urgency. | Sweep-heavy flow, no dominant side, or below window premium. | `long_option_or_debit_spread` |
| `etf_flow_specialist` | `flow_confirmation` | Strong directional flow with sweep/aggressor, IV, ATM IV, GEX, delta, and gamma context. | Missing flow classification, stale context, or flow not aligned with setup. | `long_option_or_debit_spread` |
| `etf_momentum_specialist` | `momentum_continuation` | Enough intraday price persistence with acceptable IV and confirming option flow. | Too little price history, high IV, or mixed continuation. | `long_option_or_debit_spread` |
| `etf_put_credit_open30_spread` | `defined_risk_bullish_credit` | Bullish open30 call dominance where defined-risk credit is the better expression. | High IV rank, no valid put spread candidate, stale leg quotes. | `put_credit_spread` |
| `etf_call_credit_open30_spread` | `defined_risk_bearish_credit` | Bearish open30 put dominance where defined-risk credit is the better expression. | High IV rank, no valid call spread candidate, stale leg quotes. | `call_credit_spread` |

## Redis And DB Alignment

- Redis path attaches `strategy_read` fields to window views, blocked views, intermediate candidates, final bets, spread bets, and final blocks.
- DB replay path attaches the same top-level safe fields and nested `strategy_read` object to `live.nexus_strategy_signal.payload_json`.
- DB `WATCH` rows use `strategy_action = watch`; DB `INTERMEDIATE` rows use `strategy_action = candidate`.
- Redis final `BET` rows use `strategy_action = bet`.
