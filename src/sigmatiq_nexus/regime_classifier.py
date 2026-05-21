from __future__ import annotations

from typing import Any


def _float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return numerator / denominator


def _dominant_side(call_premium: float, put_premium: float, threshold: float = 1.35) -> str | None:
    if call_premium > put_premium * threshold:
        return "CALL"
    if put_premium > call_premium * threshold:
        return "PUT"
    return None


def classify_regime(inputs: dict[str, Any]) -> dict[str, Any]:
    """Classify the completed window into a trader-facing intraday regime.

    This is intentionally deterministic and conservative. It should explain
    why a regime was chosen and what would invalidate that read.
    """
    window_label = str(inputs.get("window_label") or "")
    event_risk = str(inputs.get("event_risk") or "unknown").strip().lower()
    pricing_quality = str(inputs.get("pricing_quality") or "unknown").strip().lower()
    call_premium = _float(inputs.get("call_premium")) or 0.0
    put_premium = _float(inputs.get("put_premium")) or 0.0
    total_premium = _float(inputs.get("total_premium")) or (call_premium + put_premium)
    equity_first = _float(inputs.get("equity_first_price"))
    equity_last = _float(inputs.get("equity_last_price") or inputs.get("underlying_mid"))
    session_open = _float(inputs.get("session_open_price"))
    equity_vwap = _float(inputs.get("equity_vwap"))
    structural_pivot = _float(inputs.get("structural_pivot"))
    net_gex = _float(inputs.get("net_gex"))
    pinning_score = _float(inputs.get("pinning_score")) or 0.0
    acceleration_risk_score = _float(inputs.get("acceleration_risk_score")) or 0.0
    distance_to_call_wall_pct = _float(inputs.get("distance_to_call_wall_pct"))
    distance_to_put_wall_pct = _float(inputs.get("distance_to_put_wall_pct"))
    distance_to_gamma_flip_pct = _float(inputs.get("distance_to_gamma_flip_pct"))
    distance_to_vwap_pct = _float(inputs.get("distance_to_vwap_pct"))
    iv_rank_pct = _float(inputs.get("iv_rank_pct"))
    session_open_price = _float(inputs.get("session_open_price"))
    session_return_pct = _float(inputs.get("session_return_pct"))
    consecutive_trend_windows = int(_float(inputs.get("consecutive_trend_windows")) or 0)
    failed_wall_acceptance = bool(inputs.get("failed_wall_acceptance"))

    equity_return_pct = _float(inputs.get("equity_return_pct"))
    if equity_return_pct is None and equity_first and equity_last:
        equity_return_pct = ((equity_last - equity_first) / equity_first) * 100.0
    if session_return_pct is None and session_open_price and equity_last:
        session_return_pct = ((equity_last - session_open_price) / session_open_price) * 100.0

    dominant = _dominant_side(call_premium, put_premium)
    call_share = _ratio(call_premium, total_premium) or 0.0
    put_share = _ratio(put_premium, total_premium) or 0.0

    missing = []
    for field, value in {
        "equity_last_price": equity_last,
        "equity_vwap": equity_vwap,
        "net_gex": net_gex,
        "structural_pivot": structural_pivot,
    }.items():
        if value is None:
            missing.append(field)
    if missing:
        return _result(
            "data_insufficient",
            0.20,
            ["missing_" + field for field in missing],
            ["wait_for_fresh_equity_structure_and_flow"],
        )

    if event_risk not in {"", "none", "normal", "unknown"}:
        return _result(
            "event_news_shock",
            0.70,
            [f"event_risk_{event_risk}", "regime_temporarily_unreliable"],
            ["event_risk_normalizes", "first_clean_window_after_event"],
        )

    if pricing_quality in {"degraded", "untradable", "stale"} or (iv_rank_pct is not None and iv_rank_pct >= 65):
        return _result(
            "vol_crush_premium_trap",
            0.62,
            ["pricing_quality_" + pricing_quality, "option_pricing_can_override_direction"],
            ["pricing_quality_ready", "iv_rank_back_below_65"],
        )

    if distance_to_gamma_flip_pct is not None and abs(distance_to_gamma_flip_pct) <= 0.15:
        return _result(
            "regime_flip_mid_session",
            0.68,
            ["near_gamma_flip", "morning_read_can_become_invalid"],
            ["clean_hold_above_or_below_gamma_flip", "flow_confirms_new_side"],
        )

    near_call_wall = distance_to_call_wall_pct is not None and 0 <= distance_to_call_wall_pct <= 0.25
    near_put_wall = distance_to_put_wall_pct is not None and 0 <= distance_to_put_wall_pct <= 0.25
    stretched_into_wall = (near_call_wall or near_put_wall) and (
        failed_wall_acceptance
        or consecutive_trend_windows >= 3
        or (session_return_pct is not None and abs(session_return_pct) >= 0.75)
        or (equity_return_pct is not None and abs(equity_return_pct) >= 0.35)
    )
    trend_direction = "up" if (session_return_pct or 0.0) > 0 else "down" if (session_return_pct or 0.0) < 0 else "flat"
    flow_fading_trend = (
        dominant is None
        or (trend_direction == "up" and dominant != "CALL")
        or (trend_direction == "down" and dominant != "PUT")
    )
    if stretched_into_wall and (failed_wall_acceptance or flow_fading_trend or abs(equity_return_pct or 0.0) <= 0.10):
        return _result(
            "trend_exhaustion_reversal_risk",
            0.72 if failed_wall_acceptance or consecutive_trend_windows >= 3 else 0.66,
            [
                "extended_move_into_wall",
                "wall_reversal_risk",
                f"{consecutive_trend_windows}_window_trend_stretch",
                "failed_wall_acceptance" if failed_wall_acceptance else "flow_or_momentum_fade",
            ],
            ["clean_acceptance_through_wall", "flow_reaccelerates_after_wall"],
        )

    first_window = window_label in {"10:00", "w0930_1000"}
    if first_window and equity_return_pct is not None and abs(equity_return_pct) >= 0.15 and dominant:
        side = "bullish" if equity_return_pct > 0 and dominant == "CALL" else "bearish" if equity_return_pct < 0 and dominant == "PUT" else "mixed"
        return _result(
            "opening_drive",
            0.64 if side == "mixed" else 0.72,
            ["first_window_directional_impulse", f"{dominant.lower()}_flow_dominance", f"{side}_price_flow_read"],
            ["price_loses_vwap", "opposite_flow_takes_control", "drive_fails_next_window"],
        )

    inside_walls = (distance_to_call_wall_pct is None or distance_to_call_wall_pct >= 0) and (
        distance_to_put_wall_pct is None or distance_to_put_wall_pct >= 0
    )
    positive_gamma = (net_gex or 0.0) >= 0 or pinning_score >= 0.50
    mixed_flow = dominant is None or abs(call_share - put_share) <= 0.20
    if inside_walls and positive_gamma and mixed_flow and abs(distance_to_vwap_pct or 0.0) <= 0.35:
        return _result(
            "range_pin_day",
            0.74,
            ["inside_walls", "positive_gamma_or_pinning", "mixed_flow", "near_vwap"],
            ["break_above_call_wall", "break_below_put_wall", "negative_gamma_flip"],
        )

    above_vwap = equity_last is not None and equity_vwap is not None and equity_last >= equity_vwap
    below_vwap = equity_last is not None and equity_vwap is not None and equity_last <= equity_vwap
    above_pivot = equity_last is not None and structural_pivot is not None and equity_last >= structural_pivot
    below_pivot = equity_last is not None and structural_pivot is not None and equity_last <= structural_pivot
    if above_vwap and above_pivot and dominant == "CALL":
        return _result(
            "bullish_trend_day",
            0.76,
            ["above_vwap", "above_structural_pivot", "pivot_reclaim", "call_flow_dominance"],
            ["lose_vwap", "lose_structural_pivot", "put_flow_dominates"],
        )
    if below_vwap and below_pivot and dominant == "PUT" and ((net_gex or 0.0) < 0 or acceleration_risk_score >= 0.50):
        return _result(
            "bearish_trend_day",
            0.76,
            ["below_vwap", "below_structural_pivot", "put_flow_dominance", "negative_gamma_or_acceleration"],
            ["reclaim_vwap", "reclaim_structural_pivot", "call_flow_dominates"],
        )

    return _result(
        "conflicted_no_trade_day",
        0.52,
        ["mixed_or_contradictory_inputs", "no_clean_regime_edge"],
        ["wait_for_vwap_pivot_flow_alignment"],
    )


def classify_volatility(inputs: dict[str, Any]) -> dict[str, Any]:
    event_risk = str(inputs.get("event_risk") or "unknown").strip().lower()
    pricing_quality = str(inputs.get("pricing_quality") or "unknown").strip().lower()
    iv_rank_pct = _float(inputs.get("iv_rank_pct"))
    atm_iv = _float(inputs.get("atm_iv"))
    session_return_pct = _float(inputs.get("session_return_pct"))
    equity_return_pct = _float(inputs.get("equity_return_pct"))

    if event_risk not in {"", "none", "normal", "unknown"}:
        return _vol_result(
            "event_vol_unreliable",
            0.72,
            [f"event_risk_{event_risk}", "normal_vol_read_unreliable"],
            ["stand_down_or_reduce_size", "wait_for_post_event_pricing_reset"],
        )

    missing = []
    if iv_rank_pct is None:
        missing.append("iv_rank_pct")
    if pricing_quality in {"unknown", "missing", "insufficient_data", ""}:
        missing.append("pricing_quality")
    if missing:
        return _vol_result(
            "stale_unclear_vol",
            0.25,
            ["missing_" + field for field in missing],
            ["do_not_size_from_vol_read", "wait_for_fresh_iv_and_pricing"],
        )

    if pricing_quality in {"degraded", "untradable", "stale"}:
        return _vol_result(
            "vol_crush_premium_trap",
            0.70,
            ["pricing_quality_" + pricing_quality, "premium_execution_risk"],
            ["avoid_chasing_debits", "prefer_defined_risk_or_stand_down"],
        )

    if iv_rank_pct >= 65:
        return _vol_result(
            "vol_crush_premium_trap",
            0.68,
            ["iv_rank_rich", "premium_can_overwhelm_direction"],
            ["avoid_overpaying_for_direction", "require_fast_follow_through"],
        )

    if iv_rank_pct <= 15 and abs(session_return_pct or equity_return_pct or 0.0) < 0.20:
        return _vol_result(
            "low_vol_dead_zone",
            0.66,
            ["iv_rank_low", "realized_move_small"],
            ["avoid_overtrading", "require_structure_and_flow_confirmation"],
        )

    if 45 <= iv_rank_pct < 65 or (atm_iv is not None and atm_iv >= 0.25):
        return _vol_result(
            "high_vol_opportunity",
            0.70,
            ["iv_rank_elevated", "movement_available", "pricing_quality_tradeable"],
            ["favor_clean_momentum_or_defined_risk", "control_spread_and_entry"],
        )

    return _vol_result(
        "fair_premium_tradable_vol",
        0.72,
        ["iv_rank_fair", "pricing_quality_tradeable"],
        ["trade_only_when_regime_and_flow_agree", "normal_position_sizing_rules"],
    )


def classify_flow(inputs: dict[str, Any]) -> dict[str, Any]:
    call_premium = _float(inputs.get("call_premium")) or 0.0
    put_premium = _float(inputs.get("put_premium")) or 0.0
    total_premium = _float(inputs.get("total_premium")) or (call_premium + put_premium)
    ask_aggressor_premium = _float(inputs.get("ask_aggressor_premium")) or 0.0
    bid_aggressor_premium = _float(inputs.get("bid_aggressor_premium")) or 0.0
    sweep_ratio = _float(inputs.get("sweep_ratio")) or 0.0
    sweep_quality_status = str(inputs.get("sweep_quality_status") or "unknown").strip().lower()
    aggressor_quality_status = str(inputs.get("aggressor_quality_status") or "unknown").strip().lower()
    regime = str(inputs.get("regime") or inputs.get("regime_label") or "").strip().lower()

    if total_premium <= 0 or sweep_quality_status not in {"ready", "available"} or aggressor_quality_status not in {"ready", "available"}:
        missing = []
        if total_premium <= 0:
            missing.append("option_flow")
        if sweep_quality_status not in {"ready", "available"}:
            missing.append("sweep_classification")
        if aggressor_quality_status not in {"ready", "available"}:
            missing.append("aggressor_classification")
        return _flow_result(
            "weak_or_stale_flow",
            0.25,
            ["missing_" + field for field in missing],
            ["do_not_trade_from_flow", "wait_for_clean_flow_classification"],
        )

    dominant = _dominant_side(call_premium, put_premium)
    ask_share = _ratio(ask_aggressor_premium, ask_aggressor_premium + bid_aggressor_premium) or 0.0
    bid_share = _ratio(bid_aggressor_premium, ask_aggressor_premium + bid_aggressor_premium) or 0.0
    urgent = sweep_ratio >= 0.10

    if urgent:
        return _flow_result(
            "sweep_driven_urgency",
            0.66,
            ["sweep_ratio_elevated", "urgent_flow_can_be_noisy"],
            ["reduce_size_until_follow_through", "confirm_with_price_and_structure"],
        )

    bullish_regime = regime in {"bullish_trend_day", "opening_drive"}
    bearish_regime = regime == "bearish_trend_day"
    if dominant == "CALL" and ask_share >= 0.55:
        if bearish_regime:
            return _flow_result(
                "flow_divergence",
                0.62,
                ["call_flow_against_bearish_regime", "ask_aggressor_call_pressure"],
                ["do_not_chase_until_regime_reclaims", "watch_for_flip_or_failed_breakdown"],
            )
        return _flow_result(
            "bullish_flow_confirmed",
            0.74 if bullish_regime else 0.68,
            ["call_premium_dominance", "ask_aggressor_premium_leads"],
            ["favor_bullish_setups_only_if_regime_and_vol_allow", "invalidate_on_put_flow_takeover"],
        )

    if dominant == "PUT" and bid_share >= 0.55:
        if bullish_regime:
            return _flow_result(
                "flow_divergence",
                0.62,
                ["put_flow_against_bullish_regime", "bid_aggressor_put_pressure"],
                ["avoid_bullish_chase", "watch_for_vwap_or_pivot_loss"],
            )
        return _flow_result(
            "bearish_flow_confirmed",
            0.74 if bearish_regime else 0.68,
            ["put_premium_dominance", "bid_aggressor_premium_leads"],
            ["favor_bearish_setups_only_if_regime_and_vol_allow", "invalidate_on_call_flow_takeover"],
        )

    if dominant and ((dominant == "CALL" and bid_share >= 0.55) or (dominant == "PUT" and ask_share >= 0.55)):
        return _flow_result(
            "flow_divergence",
            0.58,
            ["premium_side_conflicts_with_aggressor_side"],
            ["wait_for_clean_aggressor_confirmation"],
        )

    return _flow_result(
        "mixed_flow_no_edge",
        0.52,
        ["call_put_flow_balanced", "no_clean_aggressor_edge"],
        ["wait_for_side_dominance", "avoid_forcing_direction"],
    )


def classify_price_location(inputs: dict[str, Any]) -> dict[str, Any]:
    equity_first = _float(inputs.get("equity_first_price"))
    equity_last = _float(inputs.get("equity_last_price") or inputs.get("underlying_mid"))
    session_open = _float(inputs.get("session_open_price"))
    equity_vwap = _float(inputs.get("equity_vwap"))
    structural_pivot = _float(inputs.get("structural_pivot"))
    distance_to_vwap_pct = _float(inputs.get("distance_to_vwap_pct"))
    distance_to_call_wall_pct = _float(inputs.get("distance_to_call_wall_pct"))
    distance_to_put_wall_pct = _float(inputs.get("distance_to_put_wall_pct"))
    distance_to_gamma_flip_pct = _float(inputs.get("distance_to_gamma_flip_pct"))
    failed_wall_acceptance = bool(inputs.get("failed_wall_acceptance"))

    missing = []
    for field, value in {
        "equity_last_price": equity_last,
        "equity_vwap": equity_vwap,
        "structural_pivot": structural_pivot,
    }.items():
        if value is None:
            missing.append(field)
    if missing:
        return _location_result(
            "price_location_insufficient",
            0.20,
            ["missing_" + field for field in missing],
            ["do_not_time_entries_from_location"],
        )

    near_call_wall = distance_to_call_wall_pct is not None and 0 <= distance_to_call_wall_pct <= 0.25
    near_put_wall = distance_to_put_wall_pct is not None and 0 <= distance_to_put_wall_pct <= 0.25
    near_gamma_flip = distance_to_gamma_flip_pct is not None and abs(distance_to_gamma_flip_pct) <= 0.15
    near_vwap = distance_to_vwap_pct is not None and abs(distance_to_vwap_pct) <= 0.15
    above_vwap = equity_last >= equity_vwap
    above_pivot = equity_last >= structural_pivot
    opened_below_pivot = (
        (session_open is not None and session_open < structural_pivot)
        or (equity_first is not None and equity_first < structural_pivot)
    )
    opened_above_pivot = (
        (session_open is not None and session_open > structural_pivot)
        or (equity_first is not None and equity_first > structural_pivot)
    )

    if failed_wall_acceptance and near_call_wall:
        return _location_result(
            "resistance_test",
            0.74,
            ["near_call_wall", "failed_wall_acceptance"],
            ["avoid_chasing_calls_into_resistance", "require_clean_acceptance_above_resistance"],
        )
    if failed_wall_acceptance and near_put_wall:
        return _location_result(
            "support_test",
            0.74,
            ["near_put_wall", "failed_wall_acceptance"],
            ["avoid_chasing_puts_into_support", "require_clean_acceptance_below_support"],
        )
    if near_call_wall:
        return _location_result(
            "near_call_wall",
            0.70,
            ["approaching_structural_resistance"],
            ["take_partial_profits_or_wait_for_acceptance", "do_not_chase_late_calls"],
        )
    if near_put_wall:
        return _location_result(
            "near_put_wall",
            0.70,
            ["approaching_structural_support"],
            ["take_partial_profits_or_wait_for_breakdown", "do_not_chase_late_puts"],
        )
    if near_gamma_flip:
        return _location_result(
            "near_gamma_flip",
            0.68,
            ["near_regime_boundary"],
            ["reduce_size_until_price_accepts_one_side", "expect_whipsaw_near_flip"],
        )
    if opened_below_pivot and above_pivot and above_vwap:
        return _location_result(
            "pivot_reclaim",
            0.76,
            ["opened_below_pivot", "reclaimed_pivot", "holding_above_vwap"],
            ["favor_bullish_setups_if_flow_confirms", "invalidate_on_pivot_loss"],
        )
    if opened_above_pivot and not above_pivot and not above_vwap:
        return _location_result(
            "failed_reclaim",
            0.76,
            ["lost_pivot", "below_vwap"],
            ["avoid_bullish_entries", "favor_bearish_setups_if_flow_confirms"],
        )
    if above_pivot and above_vwap:
        return _location_result(
            "breakout_acceptance",
            0.70,
            ["accepted_above_pivot", "accepted_above_vwap"],
            ["favor_continuation_only_before_wall", "invalidate_on_vwap_loss"],
        )
    if not above_pivot and not above_vwap:
        return _location_result(
            "breakdown_acceptance",
            0.70,
            ["accepted_below_pivot", "accepted_below_vwap"],
            ["favor_downside_only_before_support", "invalidate_on_vwap_reclaim"],
        )
    if near_vwap:
        return _location_result(
            "inside_range_chop",
            0.58,
            ["near_vwap", "no_clean_location_edge"],
            ["wait_for_acceptance_away_from_vwap", "avoid_forcing_direction"],
        )
    return _location_result(
        "mixed_location",
        0.52,
        ["price_between_key_references"],
        ["wait_for_pivot_vwap_or_wall_resolution"],
    )


def classify_trade_quality(inputs: dict[str, Any]) -> dict[str, Any]:
    pricing_quality = str(inputs.get("pricing_quality") or "unknown").strip().lower()
    event_risk = str(inputs.get("event_risk") or "unknown").strip().lower()
    regime = str(inputs.get("regime") or inputs.get("regime_label") or "").strip().lower()
    volatility = str(inputs.get("volatility_label") or "").strip().lower()
    flow = str(inputs.get("flow_label") or "").strip().lower()
    location = str(inputs.get("price_location_label") or "").strip().lower()
    total_premium = _float(inputs.get("total_premium")) or 0.0
    sweep_quality_status = str(inputs.get("sweep_quality_status") or "unknown").strip().lower()
    aggressor_quality_status = str(inputs.get("aggressor_quality_status") or "unknown").strip().lower()

    missing = []
    if total_premium <= 0:
        missing.append("option_flow")
    if pricing_quality in {"unknown", "missing", "insufficient_data", ""}:
        missing.append("pricing_quality")
    if flow in {"", "weak_or_stale_flow"} or sweep_quality_status not in {"ready", "available"} or aggressor_quality_status not in {"ready", "available"}:
        missing.append("clean_flow_quality")
    if location in {"", "price_location_insufficient"}:
        missing.append("price_location")
    if missing:
        return _quality_result(
            "data_insufficient",
            0.22,
            ["missing_" + field for field in missing],
            ["stand_down_until_required_reads_are_fresh"],
        )

    if event_risk not in {"", "none", "normal", "unknown"}:
        return _quality_result(
            "watch_only",
            0.55,
            [f"event_risk_{event_risk}", "market_read_temporarily_unreliable"],
            ["reduce_size_or_stand_down", "wait_for_clean_post_event_window"],
        )

    if pricing_quality in {"degraded", "untradable", "stale"} or volatility == "vol_crush_premium_trap":
        return _quality_result(
            "bad_pricing",
            0.72,
            ["premium_or_execution_quality_bad", "direction_can_be_right_but_trade_can_fail"],
            ["avoid_debit_chase", "require_better_entry_or_defined_risk"],
        )

    if location in {"near_call_wall", "near_put_wall", "resistance_test", "support_test"}:
        return _quality_result(
            "late_chase_risk",
            0.68,
            ["price_near_structural_wall", "reward_to_risk_compressed"],
            ["do_not_chase_into_wall", "wait_for_acceptance_or_pullback"],
        )

    if location == "near_gamma_flip" or flow in {"mixed_flow_no_edge", "flow_divergence"} or regime in {"conflicted_no_trade_day", "regime_flip_mid_session"}:
        return _quality_result(
            "conflicted_low_edge",
            0.64,
            ["key_reads_do_not_agree", "edge_not_clean_enough"],
            ["wait_for_regime_flow_location_alignment"],
        )

    bullish_clean = (
        regime in {"bullish_trend_day", "opening_drive"}
        and flow == "bullish_flow_confirmed"
        and location in {"pivot_reclaim", "breakout_acceptance"}
    )
    bearish_clean = (
        regime in {"bearish_trend_day", "opening_drive"}
        and flow == "bearish_flow_confirmed"
        and location in {"failed_reclaim", "breakdown_acceptance"}
    )
    vol_ok = volatility in {"fair_premium_tradable_vol", "high_vol_opportunity"}
    if vol_ok and (bullish_clean or bearish_clean):
        return _quality_result(
            "clean_setup",
            0.78,
            ["regime_flow_location_aligned", "pricing_quality_tradeable"],
            ["consider_only_rule_based_entry", "invalidate_if_alignment_breaks"],
        )

    return _quality_result(
        "watch_only",
        0.52,
        ["some_reads_are_constructive_but_not_aligned"],
        ["wait_for_a_cleaner_window", "avoid_forcing_trade"],
    )


def classify_invalidation(inputs: dict[str, Any]) -> dict[str, Any]:
    event_risk = str(inputs.get("event_risk") or "unknown").strip().lower()
    regime = str(inputs.get("regime") or inputs.get("regime_label") or "").strip().lower()
    flow = str(inputs.get("flow_label") or "").strip().lower()
    location = str(inputs.get("price_location_label") or "").strip().lower()
    quality = str(inputs.get("trade_quality_label") or "").strip().lower()
    equity_last = _float(inputs.get("equity_last_price") or inputs.get("underlying_mid"))
    equity_vwap = _float(inputs.get("equity_vwap"))
    structural_pivot = _float(inputs.get("structural_pivot"))

    if event_risk not in {"", "none", "normal", "unknown"}:
        return _invalidation_result(
            "event_invalidated",
            0.76,
            [f"event_risk_{event_risk}", "normal_structure_read_unreliable"],
            ["stand_down_until_post_event_window", "rebuild_thesis_after_event"],
        )

    missing = []
    if equity_last is None:
        missing.append("equity_last_price")
    if equity_vwap is None:
        missing.append("equity_vwap")
    if structural_pivot is None:
        missing.append("structural_pivot")
    if flow in {"", "weak_or_stale_flow"}:
        missing.append("flow_read")
    if quality in {"", "data_insufficient"}:
        missing.append("trade_quality")
    if missing:
        return _invalidation_result(
            "risk_unclear",
            0.25,
            ["missing_" + field for field in missing],
            ["do_not_define_risk_from_stale_reads"],
        )

    if location in {"near_call_wall", "near_put_wall", "resistance_test", "support_test"} or quality == "late_chase_risk":
        return _invalidation_result(
            "wall_rejection_risk",
            0.72,
            ["price_at_structural_wall", "acceptance_required"],
            ["invalidate_if_wall_rejects", "wait_for_acceptance_or_pullback"],
        )

    if flow in {"flow_divergence", "mixed_flow_no_edge"} or quality == "conflicted_low_edge":
        return _invalidation_result(
            "flow_reversal_risk",
            0.66,
            ["flow_not_confirming_thesis"],
            ["invalidate_if_opposite_flow_takes_control", "wait_for_clean_flow_confirmation"],
        )

    bullish = regime in {"bullish_trend_day", "opening_drive"} and flow == "bullish_flow_confirmed"
    bearish = regime in {"bearish_trend_day", "opening_drive"} and flow == "bearish_flow_confirmed"
    if bullish and location == "pivot_reclaim":
        return _invalidation_result(
            "pivot_loss_risk",
            0.74,
            ["bullish_read_depends_on_pivot_hold"],
            ["invalidate_on_pivot_loss", "reduce_if_vwap_fails"],
        )
    if bullish:
        return _invalidation_result(
            "vwap_loss_risk",
            0.70,
            ["bullish_read_depends_on_vwap_hold"],
            ["invalidate_on_vwap_loss", "watch_for_put_flow_takeover"],
        )
    if bearish and location == "failed_reclaim":
        return _invalidation_result(
            "pivot_reclaim_risk",
            0.74,
            ["bearish_read_depends_on_pivot_rejection"],
            ["invalidate_on_pivot_reclaim", "reduce_if_call_flow_takes_over"],
        )
    if bearish:
        return _invalidation_result(
            "vwap_reclaim_risk",
            0.70,
            ["bearish_read_depends_on_vwap_rejection"],
            ["invalidate_on_vwap_reclaim", "watch_for_call_flow_takeover"],
        )

    return _invalidation_result(
        "risk_unclear",
        0.44,
        ["no_clear_directional_invalidation"],
        ["wait_for_price_flow_or_structure_to_resolve"],
    )


def classify_timing(inputs: dict[str, Any]) -> dict[str, Any]:
    window_label = str(inputs.get("window_label") or "").strip().lower()
    event_risk = str(inputs.get("event_risk") or "unknown").strip().lower()
    quality = str(inputs.get("trade_quality_label") or "").strip().lower()
    invalidation = str(inputs.get("invalidation_label") or "").strip().lower()
    location = str(inputs.get("price_location_label") or "").strip().lower()

    if event_risk not in {"", "none", "normal", "unknown"} or invalidation == "event_invalidated":
        return _timing_result(
            "wait_window",
            0.70,
            ["event_risk_active", "timing_not_reliable"],
            ["stand_down_until_clean_post_event_window"],
        )

    if quality in {"", "data_insufficient"} or invalidation in {"", "risk_unclear"} or location in {"", "price_location_insufficient"}:
        return _timing_result(
            "timing_unclear",
            0.25,
            ["missing_required_timing_reads"],
            ["do_not_force_entry_timing"],
        )

    if quality == "late_chase_risk" or invalidation == "wall_rejection_risk" or location in {"near_call_wall", "near_put_wall"}:
        return _timing_result(
            "too_late_to_chase",
            0.72,
            ["price_too_close_to_wall", "reward_to_risk_compressed"],
            ["wait_for_pullback_or_acceptance", "do_not_start_new_chase_entry"],
        )

    if invalidation in {"flow_reversal_risk", "vwap_loss_risk", "vwap_reclaim_risk"} and quality != "clean_setup":
        return _timing_result(
            "exit_risk_window",
            0.66,
            ["invalidation_pressure_rising"],
            ["manage_or_reduce_existing_exposure", "avoid_new_entries"],
        )

    if quality in {"bad_pricing", "conflicted_low_edge", "watch_only"}:
        return _timing_result(
            "wait_window",
            0.58,
            ["setup_not_clean_enough_for_entry"],
            ["wait_for_quality_and_invalidation_to_improve"],
        )

    early_windows = {"10:00", "10:30", "11:00", "w0930_1000", "w1000_1030", "w1030_1100"}
    if quality == "clean_setup" and window_label in early_windows:
        return _timing_result(
            "entry_window",
            0.76,
            ["clean_setup_inside_primary_entry_window"],
            ["entry_only_if_personal_rules_confirm", "invalidate_if_timing_read_degrades"],
        )

    if quality == "clean_setup":
        return _timing_result(
            "manage_window",
            0.64,
            ["clean_read_after_primary_entry_window"],
            ["manage_existing_exposure", "avoid_late_new_entries_without_pullback"],
        )

    return _timing_result(
        "timing_unclear",
        0.40,
        ["no_clear_timing_edge"],
        ["wait_for_next_completed_window"],
    )


def classify_liquidity_execution(inputs: dict[str, Any]) -> dict[str, Any]:
    pricing_quality = str(inputs.get("pricing_quality") or "unknown").strip().lower()
    final_quote_status = str(inputs.get("final_quote_status") or "").strip().lower()
    tradability_bucket = str(inputs.get("tradability_bucket") or inputs.get("worst_tradability_bucket") or "").strip().lower()
    max_quote_age_ms = _float(inputs.get("max_quote_age_ms"))
    lead_quote_age_seconds = _float(inputs.get("lead_quote_age_seconds"))
    total_premium = _float(inputs.get("total_premium")) or 0.0

    if total_premium <= 0 or pricing_quality in {"", "unknown", "missing", "insufficient_data"}:
        return _execution_result(
            "execution_unclear",
            0.25,
            ["missing_pricing_or_flow_context"],
            ["do_not_treat_signal_as_fillable"],
        )
    if final_quote_status in {"missing_quote_asof_window", "blocked_missing_quote_asof_window"}:
        return _execution_result(
            "stale_quote",
            0.70,
            ["missing_quote_reference"],
            ["wait_for_fresh_quote_before_considering_entry"],
        )
    if final_quote_status in {"stale_quote_asof_window", "blocked_stale_quote_asof_window"} or (lead_quote_age_seconds is not None and lead_quote_age_seconds > 30) or (max_quote_age_ms is not None and max_quote_age_ms > 30_000):
        return _execution_result(
            "stale_quote",
            0.72,
            ["quote_reference_stale"],
            ["wait_for_fresh_quote_before_considering_entry"],
        )
    if pricing_quality in {"untradable"} or tradability_bucket in {"poor", "thin", "untradable", "blocked"}:
        return _execution_result(
            "thin_liquidity",
            0.70,
            ["contract_liquidity_poor"],
            ["avoid_market_orders", "prefer_stand_down_or_smaller_defined_risk"],
        )
    if pricing_quality in {"degraded", "stale"} or tradability_bucket in {"wide", "degraded"}:
        return _execution_result(
            "wide_spread",
            0.66,
            ["execution_cost_elevated"],
            ["use_limit_discipline", "avoid_chasing_mid_to_ask"],
        )
    return _execution_result(
        "tradable",
        0.74,
        ["pricing_context_usable"],
        ["limit_order_discipline_still_required"],
    )


def classify_event_reliability(inputs: dict[str, Any]) -> dict[str, Any]:
    event_risk = str(inputs.get("event_risk") or "unknown").strip().lower()
    daily_context_status = str(inputs.get("daily_context_status") or "").strip().lower()
    readiness_status = str(inputs.get("readiness_status") or "").strip().lower()
    replay_gate_status = str(inputs.get("replay_gate_status") or "").strip().lower()
    missing_inputs = inputs.get("replay_missing_inputs") or inputs.get("replay_missing_inputs_json") or inputs.get("missing_inputs")

    if event_risk not in {"", "none", "normal", "unknown"}:
        return _reliability_result(
            "event_active",
            0.76,
            [f"event_risk_{event_risk}", "structure_temporarily_unreliable"],
            ["reduce_size_or_stand_down", "wait_for_post_event_reprice"],
        )
    if readiness_status in {"unhealthy", "blocked"} or daily_context_status in {"unhealthy", "missing", "blocked"}:
        return _reliability_result(
            "data_unreliable",
            0.72,
            ["required_context_unhealthy"],
            ["do_not_use_live_read_for_entry"],
        )
    if replay_gate_status.startswith("blocked") or (missing_inputs not in (None, "", [], "[]")):
        return _reliability_result(
            "data_unreliable",
            0.66,
            ["required_inputs_missing"],
            ["wait_for_pipeline_recovery"],
        )
    if event_risk in {"unknown"}:
        return _reliability_result(
            "event_watch",
            0.52,
            ["event_state_unknown"],
            ["avoid_full_confidence_until_event_context_is_clear"],
        )
    return _reliability_result(
        "normal_reliability",
        0.74,
        ["event_context_normal", "required_reads_available"],
        ["normal_reading_rules_apply"],
    )


def classify_position_management(inputs: dict[str, Any]) -> dict[str, Any]:
    timing = str(inputs.get("timing_label") or "").strip().lower()
    invalidation = str(inputs.get("invalidation_label") or "").strip().lower()
    quality = str(inputs.get("trade_quality_label") or "").strip().lower()
    execution = str(inputs.get("liquidity_execution_label") or "").strip().lower()
    reliability = str(inputs.get("event_reliability_label") or "").strip().lower()

    if reliability in {"event_active", "data_unreliable"} or quality in {"data_insufficient", "bad_pricing"}:
        return _management_result(
            "stand_down",
            0.72,
            ["market_or_data_not_reliable_for_management"],
            ["avoid_new_risk", "review_existing_exposure_conservatively"],
        )
    if timing == "exit_risk_window" or invalidation in {"flow_reversal_risk", "event_invalidated"}:
        return _management_result(
            "exit_risk",
            0.70,
            ["invalidation_pressure_rising"],
            ["reduce_or_exit_if_personal_rules_confirm"],
        )
    if timing == "too_late_to_chase" or invalidation == "wall_rejection_risk":
        return _management_result(
            "trim",
            0.68,
            ["price_near_exhaustion_or_wall"],
            ["take_partial_or_wait_for_acceptance"],
        )
    if timing == "manage_window" and execution in {"tradable", "wide_spread"}:
        return _management_result(
            "trail",
            0.64,
            ["setup_still_valid_after_entry_window"],
            ["trail_by_structure_and_invalidation_read"],
        )
    if quality == "clean_setup" and timing == "entry_window":
        return _management_result(
            "hold",
            0.62,
            ["setup_clean_but_requires_entry_rules"],
            ["hold_plan_only_while_reads_remain_aligned"],
        )
    return _management_result(
        "stand_down",
        0.50,
        ["no_clear_management_edge"],
        ["wait_for_clearer_read"],
    )


def _result(regime: str, confidence: float, reason_codes: list[str], invalidations: list[str]) -> dict[str, Any]:
    return {
        "regime": regime,
        "confidence": round(confidence, 4),
        "reason_codes": reason_codes,
        "invalidations": invalidations,
        "maturity": "candidate",
    }


def _vol_result(label: str, confidence: float, reason_codes: list[str], strategy_implications: list[str]) -> dict[str, Any]:
    return {
        "volatility_label": label,
        "confidence": round(confidence, 4),
        "reason_codes": reason_codes,
        "strategy_implications": strategy_implications,
        "maturity": "candidate",
    }


def _flow_result(label: str, confidence: float, reason_codes: list[str], strategy_implications: list[str]) -> dict[str, Any]:
    return {
        "flow_label": label,
        "confidence": round(confidence, 4),
        "reason_codes": reason_codes,
        "strategy_implications": strategy_implications,
        "maturity": "candidate",
    }


def _location_result(label: str, confidence: float, reason_codes: list[str], strategy_implications: list[str]) -> dict[str, Any]:
    return {
        "price_location_label": label,
        "confidence": round(confidence, 4),
        "reason_codes": reason_codes,
        "strategy_implications": strategy_implications,
        "maturity": "candidate",
    }


def _quality_result(label: str, confidence: float, reason_codes: list[str], strategy_implications: list[str]) -> dict[str, Any]:
    return {
        "trade_quality_label": label,
        "confidence": round(confidence, 4),
        "reason_codes": reason_codes,
        "strategy_implications": strategy_implications,
        "maturity": "candidate",
    }


def _invalidation_result(label: str, confidence: float, reason_codes: list[str], strategy_implications: list[str]) -> dict[str, Any]:
    return {
        "invalidation_label": label,
        "confidence": round(confidence, 4),
        "reason_codes": reason_codes,
        "strategy_implications": strategy_implications,
        "maturity": "candidate",
    }


def _timing_result(label: str, confidence: float, reason_codes: list[str], strategy_implications: list[str]) -> dict[str, Any]:
    return {
        "timing_label": label,
        "confidence": round(confidence, 4),
        "reason_codes": reason_codes,
        "strategy_implications": strategy_implications,
        "maturity": "candidate",
    }


def _execution_result(label: str, confidence: float, reason_codes: list[str], strategy_implications: list[str]) -> dict[str, Any]:
    return {
        "liquidity_execution_label": label,
        "confidence": round(confidence, 4),
        "reason_codes": reason_codes,
        "strategy_implications": strategy_implications,
        "maturity": "candidate",
    }


def _reliability_result(label: str, confidence: float, reason_codes: list[str], strategy_implications: list[str]) -> dict[str, Any]:
    return {
        "event_reliability_label": label,
        "confidence": round(confidence, 4),
        "reason_codes": reason_codes,
        "strategy_implications": strategy_implications,
        "maturity": "candidate",
    }


def _management_result(label: str, confidence: float, reason_codes: list[str], strategy_implications: list[str]) -> dict[str, Any]:
    return {
        "position_management_label": label,
        "confidence": round(confidence, 4),
        "reason_codes": reason_codes,
        "strategy_implications": strategy_implications,
        "maturity": "candidate",
    }
