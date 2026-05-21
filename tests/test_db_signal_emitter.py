import json

from sigmatiq_nexus.db_signal_emitter import (
    DEFAULT_STRATEGIES,
    EmitterConfig,
    build_candidate_sql,
    build_emit_sql,
    build_relaxed_watch_candidate_sql,
    build_relaxed_watch_emit_sql,
    confidence_for_open_call_dominance,
    parse_args,
)


def _config(**overrides) -> EmitterConfig:
    config = {
        "database_url": "postgres://example",
        "session_date": "2026-05-20",
        "symbols": ("SPY", "QQQ"),
        "strategies": DEFAULT_STRATEGIES,
        "min_window_premium": 200_000.0,
        "premium_overrides": {},
        "open_call_dominance": 1.35,
        "side_dominance": 2.0,
        "max_sweep_ratio": 0.10,
        "spread_max_iv_rank": 30.0,
        "include_relaxed_watch": False,
        "relaxed_watch_only": False,
        "relaxed_flow_sweep_ratio": 0.0001,
        "relaxed_momentum_max_iv_rank": 45.0,
        "relaxed_spread_max_iv_rank": 45.0,
        "dry_run": False,
        "limit": None,
    }
    config.update(overrides)
    return EmitterConfig(**config)


def test_confidence_for_open_call_dominance_is_bounded():
    assert confidence_for_open_call_dominance(0, 0) == 0.0
    assert confidence_for_open_call_dominance(200_000, 400_000) == 0.65
    assert confidence_for_open_call_dominance(390_000, 400_000) == 0.95


def test_candidate_sql_keeps_db_replay_read_only_and_conservative():
    sql = build_candidate_sql(_config())

    assert "FROM live.prepared_nexus_window_context" in sql
    assert "c.window_label IN ('w0930_1000', 'w1000_1030', 'w1030_1100', 'w1100_1130', 'w1130_1200')" in sql
    assert "c.readiness_status = 'ready'" in sql
    assert "c.daily_context_status IN ('ready', 'healthy')" in sql
    assert "c.final_quote_status = 'fresh_quote_asof_window'" in sql
    assert "'etf_confluence_sniper'::text AS strategy_key" in sql
    assert "bw.pricing_quality = 'ready'" in sql
    assert "bw.cheap_side IN ('C', 'P')" in sql
    assert "bw.call_premium > bw.put_premium * 1.35::numeric" in sql
    assert "'etf_low_sweep_core'::text AS strategy_key" in sql
    assert "bw.sweep_quality_status = 'ready'" in sql
    assert "bw.sweep_ratio <= 0.1::numeric" in sql
    assert "'etf_flow_specialist'::text AS strategy_key" in sql
    assert "bw.aggressor_quality_status = 'ready'" in sql
    assert "bw.lead_delta IS NOT NULL" in sql
    assert "bw.lead_gamma IS NOT NULL" in sql
    assert "'etf_momentum_specialist'::text AS strategy_key" in sql
    assert "bw.iv_rank_pct < 40" in sql
    assert "'etf_put_credit_open30_spread'::text AS strategy_key" in sql
    assert "bw.symbol IN ('SPY', 'QQQ')" in sql
    assert "'etf_call_credit_open30_spread'::text AS strategy_key" in sql
    assert "bw.symbol = 'SPY'" in sql
    assert "bw.iv_rank_pct < 30.0::numeric" in sql
    assert "INSERT INTO live.nexus_strategy_signal" not in sql


def test_emit_sql_uses_idempotent_ledger_before_append_only_signal_insert():
    sql = build_emit_sql(_config(premium_overrides={"IWM": 75_000.0}, limit=3))

    assert "INSERT INTO live.nexus_db_signal_emission" in sql
    assert "ON CONFLICT (symbol, session_date, strategy_key, entry_time, decision) DO NOTHING" in sql
    assert "INSERT INTO live.nexus_strategy_signal" in sql
    assert "'INTERMEDIATE'" in sql
    assert "'sigmatiq_nexus_db_replay'" in sql
    assert "p.strategy_key" in sql
    assert "model_status" in sql
    assert "not_run_db_replay_intermediate_only" in sql
    assert "spread_status" in sql
    assert "not_constructed_db_replay_intermediate_only" in sql
    assert "replay_scope" in sql
    assert "LIMIT 3" in sql
    assert json.dumps({"IWM": 75_000.0}) in sql


def test_relaxed_watch_sql_is_separate_watch_only_path():
    sql = build_relaxed_watch_candidate_sql(_config(limit=5))

    assert "'etf_flow_specialist'::text AS strategy_key" in sql
    assert "relaxed_flow_sweep_threshold" in sql
    assert "bw.sweep_ratio > 0.0001::numeric" in sql
    assert "bw.sweep_ratio <= 0.1::numeric" in sql
    assert "relaxed_momentum_iv_rank_threshold" in sql
    assert "bw.iv_rank_pct >= 40" in sql
    assert "bw.iv_rank_pct < 45.0::numeric" in sql
    assert "relaxed_put_credit_spread_iv_rank_threshold" in sql
    assert "relaxed_watch_with_outcome" in sql
    assert "proxy_return_30m_pct" in sql
    assert "INSERT INTO live.nexus_strategy_signal" not in sql
    assert "LIMIT 5" in sql


def test_relaxed_watch_emit_sql_marks_rows_as_watch_not_intermediate():
    sql = build_relaxed_watch_emit_sql(_config())

    assert "'sigmatiq_nexus_db_replay_relaxed'" in sql
    assert "'WATCH'" in sql
    assert "'decision', 'INTERMEDIATE'" not in sql
    assert "stage', 0" in sql
    assert "risk_note" in sql
    assert "failed_gate" in sql
    assert "counterfactual" in sql
    assert "not_constructed_db_replay_watch_only" in sql
    assert "ON CONFLICT (symbol, session_date, strategy_key, entry_time, decision) DO NOTHING" in sql


def test_parse_args_normalizes_symbols_and_requires_iso_date():
    config = parse_args([
        "--database-url",
        "Host=db;Database=shared;Username=u;Password=p;SSL Mode=Require",
        "--session-date",
        "2026-05-20",
        "--symbols",
        "spy, qqq",
        "--strategies",
        "etf_open_specialist",
        "--spread-max-iv-rank",
        "28",
        "--include-relaxed-watch",
        "--relaxed-flow-sweep-ratio",
        "0.0002",
        "--dry-run",
    ])

    assert config.symbols == ("SPY", "QQQ")
    assert config.strategies == ("etf_open_specialist",)
    assert config.session_date == "2026-05-20"
    assert config.dry_run is True
    assert config.spread_max_iv_rank == 28
    assert config.include_relaxed_watch is True
    assert config.relaxed_flow_sweep_ratio == 0.0002
    assert "host=db" in config.database_url


def test_parse_args_rejects_unsupported_strategy():
    try:
        parse_args([
            "--database-url",
            "postgres://example",
            "--strategies",
            "etf_allday_specialist",
        ])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("expected unsupported strategy to fail")
