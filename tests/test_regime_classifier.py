from sigmatiq_nexus.regime_classifier import (
    classify_event_reliability,
    classify_flow,
    classify_invalidation,
    classify_liquidity_execution,
    classify_position_management,
    classify_price_location,
    classify_regime,
    classify_timing,
    classify_trade_quality,
    classify_volatility,
)


def _base_inputs(**overrides):
    inputs = {
        "window_label": "w1000_1030",
        "call_premium": 120_000,
        "put_premium": 110_000,
        "total_premium": 230_000,
        "equity_first_price": 514.0,
        "equity_last_price": 515.0,
        "equity_vwap": 514.5,
        "distance_to_vwap_pct": 0.10,
        "net_gex": 1_000_000,
        "pinning_score": 0.65,
        "acceleration_risk_score": 0.10,
        "structural_pivot": 514.0,
        "distance_to_call_wall_pct": 0.75,
        "distance_to_put_wall_pct": 1.25,
        "distance_to_gamma_flip_pct": 0.35,
        "pricing_quality": "ready",
        "event_risk": "normal",
        "iv_rank_pct": 30,
        "session_open_price": 514.0,
        "session_return_pct": 0.20,
        "consecutive_trend_windows": 1,
        "failed_wall_acceptance": False,
        "sweep_ratio": 0.02,
        "sweep_quality_status": "ready",
        "ask_aggressor_premium": 130_000,
        "bid_aggressor_premium": 90_000,
        "aggressor_quality_status": "ready",
    }
    inputs.update(overrides)
    return inputs


def test_classifies_range_pin_day_when_positive_gamma_inside_walls_and_flow_mixed():
    result = classify_regime(_base_inputs())

    assert result["regime"] == "range_pin_day"
    assert result["confidence"] == 0.74
    assert "inside_walls" in result["reason_codes"]


def test_classifies_opening_drive_for_first_window_directional_impulse():
    result = classify_regime(_base_inputs(
        window_label="w0930_1000",
        call_premium=260_000,
        put_premium=100_000,
        total_premium=360_000,
        equity_first_price=510.0,
        equity_last_price=511.2,
        equity_vwap=510.7,
        structural_pivot=510.5,
    ))

    assert result["regime"] == "opening_drive"
    assert "first_window_directional_impulse" in result["reason_codes"]


def test_event_risk_overrides_directional_regime():
    result = classify_regime(_base_inputs(
        call_premium=300_000,
        put_premium=50_000,
        event_risk="macro_event",
    ))

    assert result["regime"] == "event_news_shock"
    assert "event_risk_macro_event" in result["reason_codes"]


def test_classifies_bullish_grind_pivot_reclaim_when_price_and_flow_align_up():
    result = classify_regime(_base_inputs(
        call_premium=300_000,
        put_premium=100_000,
        total_premium=400_000,
        equity_last_price=516.0,
        equity_vwap=514.0,
        structural_pivot=515.0,
        distance_to_vwap_pct=0.39,
        pinning_score=0.20,
    ))

    assert result["regime"] == "bullish_trend_day"
    assert "pivot_reclaim" in result["reason_codes"]
    assert result["invalidations"] == ["lose_vwap", "lose_structural_pivot", "put_flow_dominates"]


def test_classifies_bearish_trend_when_below_vwap_pivot_with_put_flow_and_negative_gamma():
    result = classify_regime(_base_inputs(
        call_premium=100_000,
        put_premium=300_000,
        total_premium=400_000,
        equity_last_price=512.0,
        equity_vwap=514.0,
        structural_pivot=513.0,
        net_gex=-1_000_000,
        pinning_score=0.10,
        acceleration_risk_score=0.55,
        distance_to_vwap_pct=-0.39,
    ))

    assert result["regime"] == "bearish_trend_day"
    assert "negative_gamma_or_acceleration" in result["reason_codes"]


def test_classifies_regime_flip_when_price_is_near_gamma_flip():
    result = classify_regime(_base_inputs(
        distance_to_gamma_flip_pct=0.05,
        call_premium=250_000,
        put_premium=100_000,
    ))

    assert result["regime"] == "regime_flip_mid_session"
    assert "near_gamma_flip" in result["reason_codes"]


def test_classifies_trend_exhaustion_when_extended_move_reaches_wall():
    result = classify_regime(_base_inputs(
        equity_first_price=510.0,
        equity_last_price=512.1,
        distance_to_call_wall_pct=0.18,
        call_premium=180_000,
        put_premium=170_000,
        distance_to_gamma_flip_pct=0.50,
        session_open_price=507.5,
        session_return_pct=0.91,
        consecutive_trend_windows=4,
    ))

    assert result["regime"] == "trend_exhaustion_reversal_risk"
    assert "extended_move_into_wall" in result["reason_codes"]
    assert "4_window_trend_stretch" in result["reason_codes"]


def test_classifies_trend_exhaustion_when_wall_acceptance_fails():
    result = classify_regime(_base_inputs(
        equity_first_price=512.0,
        equity_last_price=511.8,
        distance_to_call_wall_pct=0.10,
        call_premium=160_000,
        put_premium=150_000,
        session_return_pct=0.55,
        consecutive_trend_windows=2,
        failed_wall_acceptance=True,
    ))

    assert result["regime"] == "trend_exhaustion_reversal_risk"
    assert "failed_wall_acceptance" in result["reason_codes"]


def test_classifies_vol_crush_premium_trap_for_bad_pricing_or_high_iv():
    degraded = classify_regime(_base_inputs(pricing_quality="degraded"))
    high_iv = classify_regime(_base_inputs(iv_rank_pct=72))

    assert degraded["regime"] == "vol_crush_premium_trap"
    assert high_iv["regime"] == "vol_crush_premium_trap"


def test_classifies_conflicted_no_trade_when_inputs_do_not_align():
    result = classify_regime(_base_inputs(
        call_premium=290_000,
        put_premium=100_000,
        equity_last_price=512.0,
        equity_vwap=514.0,
        structural_pivot=513.0,
        net_gex=500_000,
        pinning_score=0.10,
        distance_to_vwap_pct=-0.39,
    ))

    assert result["regime"] == "conflicted_no_trade_day"
    assert "no_clean_regime_edge" in result["reason_codes"]


def test_classifies_data_insufficient_when_structural_inputs_are_missing():
    result = classify_regime(_base_inputs(equity_vwap=None, structural_pivot=None))

    assert result["regime"] == "data_insufficient"
    assert "missing_equity_vwap" in result["reason_codes"]
    assert "missing_structural_pivot" in result["reason_codes"]


def test_classifies_low_vol_dead_zone():
    result = classify_volatility(_base_inputs(iv_rank_pct=9, pricing_quality="ready", session_return_pct=0.08))

    assert result["volatility_label"] == "low_vol_dead_zone"
    assert "iv_rank_low" in result["reason_codes"]


def test_classifies_fair_premium_tradable_vol():
    result = classify_volatility(_base_inputs(iv_rank_pct=32, pricing_quality="ready"))

    assert result["volatility_label"] == "fair_premium_tradable_vol"
    assert "iv_rank_fair" in result["reason_codes"]


def test_classifies_high_vol_opportunity():
    result = classify_volatility(_base_inputs(iv_rank_pct=52, atm_iv=0.22, pricing_quality="ready"))

    assert result["volatility_label"] == "high_vol_opportunity"
    assert "movement_available" in result["reason_codes"]


def test_classifies_vol_crush_premium_trap_from_rich_or_bad_pricing():
    rich = classify_volatility(_base_inputs(iv_rank_pct=72, pricing_quality="ready"))
    bad_pricing = classify_volatility(_base_inputs(iv_rank_pct=30, pricing_quality="degraded"))

    assert rich["volatility_label"] == "vol_crush_premium_trap"
    assert bad_pricing["volatility_label"] == "vol_crush_premium_trap"


def test_classifies_event_vol_unreliable():
    result = classify_volatility(_base_inputs(event_risk="macro_event", iv_rank_pct=40, pricing_quality="ready"))

    assert result["volatility_label"] == "event_vol_unreliable"
    assert "event_risk_macro_event" in result["reason_codes"]


def test_classifies_stale_unclear_vol():
    result = classify_volatility(_base_inputs(iv_rank_pct=None, pricing_quality="unknown"))

    assert result["volatility_label"] == "stale_unclear_vol"
    assert "missing_iv_rank_pct" in result["reason_codes"]


def test_classifies_bullish_flow_confirmed():
    result = classify_flow(_base_inputs(call_premium=300_000, put_premium=100_000, regime="bullish_trend_day"))

    assert result["flow_label"] == "bullish_flow_confirmed"
    assert "call_premium_dominance" in result["reason_codes"]


def test_classifies_bearish_flow_confirmed():
    result = classify_flow(_base_inputs(
        call_premium=100_000,
        put_premium=300_000,
        ask_aggressor_premium=80_000,
        bid_aggressor_premium=160_000,
        regime="bearish_trend_day",
    ))

    assert result["flow_label"] == "bearish_flow_confirmed"
    assert "put_premium_dominance" in result["reason_codes"]


def test_classifies_mixed_flow_no_edge():
    result = classify_flow(_base_inputs(call_premium=130_000, put_premium=120_000, ask_aggressor_premium=110_000, bid_aggressor_premium=105_000))

    assert result["flow_label"] == "mixed_flow_no_edge"


def test_classifies_sweep_driven_urgency():
    result = classify_flow(_base_inputs(call_premium=300_000, put_premium=100_000, sweep_ratio=0.18))

    assert result["flow_label"] == "sweep_driven_urgency"
    assert "sweep_ratio_elevated" in result["reason_codes"]


def test_classifies_flow_divergence_against_regime():
    result = classify_flow(_base_inputs(call_premium=300_000, put_premium=100_000, regime="bearish_trend_day"))

    assert result["flow_label"] == "flow_divergence"
    assert "call_flow_against_bearish_regime" in result["reason_codes"]


def test_classifies_weak_or_stale_flow():
    result = classify_flow(_base_inputs(total_premium=0, call_premium=0, put_premium=0, sweep_quality_status="unknown"))

    assert result["flow_label"] == "weak_or_stale_flow"
    assert "missing_option_flow" in result["reason_codes"]


def test_classifies_pivot_reclaim_location():
    result = classify_price_location(_base_inputs(
        equity_first_price=513.0,
        equity_last_price=515.5,
        equity_vwap=514.8,
        structural_pivot=514.0,
    ))

    assert result["price_location_label"] == "pivot_reclaim"
    assert "reclaimed_pivot" in result["reason_codes"]


def test_classifies_failed_reclaim_location():
    result = classify_price_location(_base_inputs(
        equity_first_price=515.0,
        equity_last_price=513.0,
        equity_vwap=514.0,
        structural_pivot=514.0,
    ))

    assert result["price_location_label"] == "failed_reclaim"
    assert "lost_pivot" in result["reason_codes"]


def test_classifies_near_call_wall_location():
    result = classify_price_location(_base_inputs(distance_to_call_wall_pct=0.12))

    assert result["price_location_label"] == "near_call_wall"
    assert "approaching_structural_resistance" in result["reason_codes"]


def test_classifies_near_gamma_flip_location():
    result = classify_price_location(_base_inputs(distance_to_gamma_flip_pct=-0.08))

    assert result["price_location_label"] == "near_gamma_flip"
    assert "near_regime_boundary" in result["reason_codes"]


def test_classifies_clean_trade_quality_when_reads_align():
    result = classify_trade_quality(_base_inputs(
        regime_label="bullish_trend_day",
        volatility_label="fair_premium_tradable_vol",
        flow_label="bullish_flow_confirmed",
        price_location_label="pivot_reclaim",
        pricing_quality="ready",
    ))

    assert result["trade_quality_label"] == "clean_setup"
    assert "regime_flow_location_aligned" in result["reason_codes"]


def test_classifies_bad_pricing_trade_quality():
    result = classify_trade_quality(_base_inputs(
        regime_label="bullish_trend_day",
        volatility_label="vol_crush_premium_trap",
        flow_label="bullish_flow_confirmed",
        price_location_label="pivot_reclaim",
        pricing_quality="ready",
    ))

    assert result["trade_quality_label"] == "bad_pricing"
    assert "premium_or_execution_quality_bad" in result["reason_codes"]


def test_classifies_late_chase_trade_quality_near_wall():
    result = classify_trade_quality(_base_inputs(
        regime_label="bullish_trend_day",
        volatility_label="fair_premium_tradable_vol",
        flow_label="bullish_flow_confirmed",
        price_location_label="near_call_wall",
        pricing_quality="ready",
    ))

    assert result["trade_quality_label"] == "late_chase_risk"


def test_classifies_pivot_loss_invalidation_for_bullish_reclaim():
    result = classify_invalidation(_base_inputs(
        regime_label="bullish_trend_day",
        flow_label="bullish_flow_confirmed",
        price_location_label="pivot_reclaim",
        trade_quality_label="clean_setup",
    ))

    assert result["invalidation_label"] == "pivot_loss_risk"
    assert "bullish_read_depends_on_pivot_hold" in result["reason_codes"]


def test_classifies_flow_reversal_invalidation_for_divergence():
    result = classify_invalidation(_base_inputs(
        regime_label="bullish_trend_day",
        flow_label="flow_divergence",
        price_location_label="pivot_reclaim",
        trade_quality_label="conflicted_low_edge",
    ))

    assert result["invalidation_label"] == "flow_reversal_risk"


def test_classifies_event_invalidation():
    result = classify_invalidation(_base_inputs(
        event_risk="macro_event",
        regime_label="bullish_trend_day",
        flow_label="bullish_flow_confirmed",
        price_location_label="pivot_reclaim",
        trade_quality_label="clean_setup",
    ))

    assert result["invalidation_label"] == "event_invalidated"


def test_classifies_entry_timing_for_clean_early_setup():
    result = classify_timing(_base_inputs(
        window_label="w1000_1030",
        trade_quality_label="clean_setup",
        invalidation_label="pivot_loss_risk",
        price_location_label="pivot_reclaim",
    ))

    assert result["timing_label"] == "entry_window"


def test_classifies_too_late_timing_near_wall():
    result = classify_timing(_base_inputs(
        trade_quality_label="late_chase_risk",
        invalidation_label="wall_rejection_risk",
        price_location_label="near_call_wall",
    ))

    assert result["timing_label"] == "too_late_to_chase"


def test_classifies_exit_risk_timing_when_flow_reverses():
    result = classify_timing(_base_inputs(
        trade_quality_label="watch_only",
        invalidation_label="flow_reversal_risk",
        price_location_label="pivot_reclaim",
    ))

    assert result["timing_label"] == "exit_risk_window"


def test_classifies_manage_timing_after_primary_entry_window():
    result = classify_timing(_base_inputs(
        window_label="w1200_1230",
        trade_quality_label="clean_setup",
        invalidation_label="vwap_loss_risk",
        price_location_label="breakout_acceptance",
    ))

    assert result["timing_label"] == "manage_window"


def test_classifies_tradable_liquidity_execution():
    result = classify_liquidity_execution(_base_inputs(
        pricing_quality="usable",
        final_quote_status="fresh_quote_asof_window",
        worst_tradability_bucket="good",
        max_quote_age_ms=250,
    ))

    assert result["liquidity_execution_label"] == "tradable"
    assert "pricing_context_usable" in result["reason_codes"]


def test_classifies_stale_quote_liquidity_execution():
    result = classify_liquidity_execution(_base_inputs(
        pricing_quality="usable",
        final_quote_status="stale_quote_asof_window",
        max_quote_age_ms=45_000,
    ))

    assert result["liquidity_execution_label"] == "stale_quote"
    assert "quote_reference_stale" in result["reason_codes"]


def test_classifies_event_active_reliability():
    result = classify_event_reliability(_base_inputs(event_risk="fomc"))

    assert result["event_reliability_label"] == "event_active"
    assert "event_risk_fomc" in result["reason_codes"]


def test_classifies_data_unreliable_reliability():
    result = classify_event_reliability(_base_inputs(
        event_risk="normal",
        readiness_status="blocked",
        daily_context_status="healthy",
    ))

    assert result["event_reliability_label"] == "data_unreliable"
    assert "required_context_unhealthy" in result["reason_codes"]


def test_classifies_trim_position_management_near_wall():
    result = classify_position_management(_base_inputs(
        trade_quality_label="late_chase_risk",
        invalidation_label="wall_rejection_risk",
        timing_label="too_late_to_chase",
        liquidity_execution_label="tradable",
        event_reliability_label="normal_reliability",
    ))

    assert result["position_management_label"] == "trim"
    assert "price_near_exhaustion_or_wall" in result["reason_codes"]


def test_classifies_trail_position_management_after_entry_window():
    result = classify_position_management(_base_inputs(
        trade_quality_label="clean_setup",
        invalidation_label="vwap_loss_risk",
        timing_label="manage_window",
        liquidity_execution_label="tradable",
        event_reliability_label="normal_reliability",
    ))

    assert result["position_management_label"] == "trail"
    assert "setup_still_valid_after_entry_window" in result["reason_codes"]
