from sigmatiq_nexus.strategy_read import classify_strategy_read


def test_strategy_read_classifies_candidate_capability():
    result = classify_strategy_read(
        "etf_confluence_sniper",
        "INTERMEDIATE",
        "BULLISH",
        "confluence_alignment",
        confidence=0.74,
    )

    assert result["strategy_read_label"] == "confluence_alignment_candidate"
    assert result["strategy_action"] == "candidate"
    assert result["strategy_capability"] == "confluence_alignment"
    assert result["strategy_direction"] == "bullish"
    assert result["strategy_confidence"] == 0.74


def test_strategy_read_classifies_blocked_inputs():
    result = classify_strategy_read(
        "etf_flow_specialist",
        "BLOCKED",
        None,
        "live_feature_quality_gate_closed",
        feature_failures={"iv_rank": "missing", "net_gex": "stale"},
    )

    assert result["strategy_read_label"] == "flow_confirmation_blocked"
    assert result["strategy_action"] == "blocked"
    assert result["strategy_blockers"] == ["iv_rank", "net_gex"]
    assert "required_strategy_inputs_unavailable" in result["strategy_reason_codes"]


def test_strategy_read_classifies_spread_bet_vehicle():
    result = classify_strategy_read(
        "etf_put_credit_open30_spread",
        "BET",
        "BULLISH",
        "candidate_promoted_to_bet",
    )

    assert result["strategy_read_label"] == "defined_risk_bullish_credit_bet"
    assert result["strategy_action"] == "bet"
    assert result["strategy_vehicle"] == "put_credit_spread"
