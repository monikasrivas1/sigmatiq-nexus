from __future__ import annotations

from typing import Any


STRATEGY_CAPABILITIES: dict[str, dict[str, Any]] = {
    "etf_confluence_sniper": {
        "capability": "confluence_alignment",
        "best_used_for": "Flow, momentum, and cheap pricing alignment.",
        "vehicle": "long_option_or_debit_spread",
        "reason_code": "flow_momentum_pricing_alignment",
    },
    "etf_open_specialist": {
        "capability": "opening_drive",
        "best_used_for": "Early opening imbalance while volatility is still acceptable.",
        "vehicle": "long_call_or_call_debit",
        "reason_code": "open30_call_dominance",
    },
    "etf_low_sweep_core": {
        "capability": "clean_directional_flow",
        "best_used_for": "Directional flow that is not dominated by sweep urgency.",
        "vehicle": "long_option_or_debit_spread",
        "reason_code": "low_sweep_directional_dominance",
    },
    "etf_flow_specialist": {
        "capability": "flow_confirmation",
        "best_used_for": "Strong directional options flow with vol and GEX context.",
        "vehicle": "long_option_or_debit_spread",
        "reason_code": "flow_context_alignment",
    },
    "etf_momentum_specialist": {
        "capability": "momentum_continuation",
        "best_used_for": "Underlying continuation after enough intraday price persistence.",
        "vehicle": "long_option_or_debit_spread",
        "reason_code": "price_persistence_alignment",
    },
    "etf_put_credit_open30_spread": {
        "capability": "defined_risk_bullish_credit",
        "best_used_for": "Bullish open30 flow when defined-risk credit is the better expression.",
        "vehicle": "put_credit_spread",
        "reason_code": "open30_call_dominance_put_credit_context",
    },
    "etf_call_credit_open30_spread": {
        "capability": "defined_risk_bearish_credit",
        "best_used_for": "Bearish open30 flow when defined-risk credit is the better expression.",
        "vehicle": "call_credit_spread",
        "reason_code": "open30_put_dominance_call_credit_context",
    },
}


def classify_strategy_read(
    strategy: str,
    decision: str | None,
    sentiment: str | None,
    reason: str | None,
    *,
    confidence: float | None = None,
    feature_failures: dict[str, str] | None = None,
    market_reads: dict[str, Any] | None = None,
) -> dict[str, Any]:
    spec = STRATEGY_CAPABILITIES.get(strategy, {})
    capability = str(spec.get("capability") or "unknown_strategy_capability")
    normalized_decision = str(decision or "").strip().upper()
    normalized_sentiment = str(sentiment or "").strip().upper()
    blockers = sorted((feature_failures or {}).keys())

    if normalized_decision in {"BLOCKED", "FINAL_BLOCKED"} or blockers:
        action = "blocked"
        label = f"{capability}_blocked"
        reason_codes = ["required_strategy_inputs_unavailable"]
    elif normalized_decision == "BET":
        action = "bet"
        label = f"{capability}_bet"
        reason_codes = [str(spec.get("reason_code") or capability), "candidate_promoted_to_bet"]
    elif normalized_decision == "INTERMEDIATE":
        action = "candidate"
        label = f"{capability}_candidate"
        reason_codes = [str(spec.get("reason_code") or capability), "strategy_candidate_detected"]
    elif normalized_decision in {"WINDOW_VIEW"} and normalized_sentiment not in {"", "CHOP", "UNKNOWN"}:
        action = "candidate"
        label = f"{capability}_window_view"
        reason_codes = [str(spec.get("reason_code") or capability), "strategy_conditions_constructive"]
    elif normalized_decision in {"SKIPPED"}:
        action = "blocked"
        label = f"{capability}_not_applicable"
        reason_codes = ["strategy_not_applicable_to_symbol_or_window"]
    else:
        action = "watch" if normalized_decision in {"WINDOW_VIEW", "NO_BET"} else "no_trade"
        label = f"{capability}_watch"
        reason_codes = ["strategy_conditions_not_clean"]

    if reason:
        reason_codes.append(str(reason))

    direction = "bullish" if normalized_sentiment in {"BULLISH", "CALL", "UP"} else "bearish" if normalized_sentiment in {"BEARISH", "PUT", "DOWN"} else "neutral"
    return {
        "strategy_read_label": label,
        "strategy_action": action,
        "strategy_capability": capability,
        "strategy_vehicle": spec.get("vehicle") or "unknown",
        "strategy_direction": direction,
        "strategy_confidence": confidence,
        "strategy_blockers": blockers,
        "strategy_reason_codes": reason_codes,
        "strategy_best_used_for": spec.get("best_used_for") or "Strategy-specific Nexus read.",
        "market_read_inputs": market_reads or {},
    }
