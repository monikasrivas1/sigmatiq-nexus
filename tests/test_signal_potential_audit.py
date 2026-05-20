from sigmatiq_nexus.signal_potential_audit import AuditConfig, classify, normalize_database_url


def _config() -> AuditConfig:
    return AuditConfig(
        database_url="postgres://example",
        session_date="2026-05-20",
        symbols=("SPY",),
        min_window_premium=200_000.0,
        premium_overrides={},
        output="summary",
        csv_path=None,
    )


def _row(**overrides):
    row = {
        "strategy_key": "etf_open_specialist",
        "symbol": "SPY",
        "window_label": "w0930_1000",
        "entry_time": "10:00",
        "option_trade_count": 100,
        "equity_total_volume": 100_000,
        "readiness_status": "ready",
        "daily_context_status": "ready",
        "total_premium": 300_000.0,
        "call_premium": 260_000.0,
        "put_premium": 20_000.0,
        "lead_contract_raw_symbol": "SPY   260520C00740000",
        "lead_quote_ts_utc": "2026-05-20T14:00:00+00:00",
        "lead_mid": 1.25,
        "final_quote_status": "fresh_quote_asof_window",
        "quote_age_at_window_end_sec": 5.0,
        "quote_age_at_materialization_sec": 5.0,
        "pricing_quality": "ready",
        "bet_count": 0,
        "spread_signal_count": 0,
        "final_block_count": 0,
        "intermediate_count": 0,
        "window_view_count": 0,
        "audit_count": 0,
    }
    row.update(overrides)
    return row


def test_normalize_database_url_accepts_npgsql_style_connection_string():
    normalized = normalize_database_url(
        "Host=db.example.com;Database=shared;Username=app;Password=secret;SSL Mode=Require"
    )

    assert "host=db.example.com" in normalized
    assert "dbname=shared" in normalized
    assert "user=app" in normalized
    assert "password=secret" in normalized
    assert "sslmode=require" in normalized


def test_classify_candidate_without_actual_output_needs_review():
    result = classify(_row(), _config())

    assert result["potential"] == "candidate_with_unmaterialized_gate_inputs"
    assert result["warnings"] == ["iv_rank"]
    assert result["verdict"] == "needs_review_no_nexus_output"


def test_classify_uses_materialized_iv_rank_when_present():
    result = classify(_row(iv_rank=42.0, iv_rank_pct=42.0), _config())

    assert result["potential"] == "candidate_should_have_reached_final_quote_gate"
    assert result["warnings"] == []


def test_classify_below_premium_as_explainable_no_bet():
    result = classify(_row(total_premium=10_000.0, call_premium=8_000.0), _config())

    assert result["potential"] == "blocked_by_data_or_obvious_gate"
    assert "premium_below_threshold" in result["block_reasons"]
    assert result["verdict"] == "correct_no_bet_or_view_only"


def test_classify_strategy_not_scheduled_for_window():
    result = classify(_row(entry_time="15:30", window_label="w1500_1530"), _config())

    assert result["potential"] == "not_applicable"
    assert "strategy_not_scheduled_for_window" in result["block_reasons"]
