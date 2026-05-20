import json

from sigmatiq_nexus.db_signal_emitter import (
    EmitterConfig,
    build_candidate_sql,
    build_emit_sql,
    confidence_for_open_call_dominance,
    parse_args,
)


def _config(**overrides) -> EmitterConfig:
    config = {
        "database_url": "postgres://example",
        "session_date": "2026-05-20",
        "symbols": ("SPY", "QQQ"),
        "strategies": ("etf_open_specialist", "etf_low_sweep_core"),
        "min_window_premium": 200_000.0,
        "premium_overrides": {},
        "open_call_dominance": 1.35,
        "side_dominance": 2.0,
        "max_sweep_ratio": 0.10,
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
    assert "c.window_label IN ('w0930_1000', 'w1000_1030')" in sql
    assert "c.readiness_status = 'ready'" in sql
    assert "c.daily_context_status IN ('ready', 'healthy')" in sql
    assert "c.final_quote_status = 'fresh_quote_asof_window'" in sql
    assert "bw.call_premium > bw.put_premium * 1.35::numeric" in sql
    assert "'etf_low_sweep_core'::text AS strategy_key" in sql
    assert "bw.sweep_quality_status = 'ready'" in sql
    assert "bw.sweep_ratio <= 0.1::numeric" in sql
    assert "INSERT INTO live.nexus_strategy_signal" not in sql


def test_emit_sql_uses_idempotent_ledger_before_append_only_signal_insert():
    sql = build_emit_sql(_config(premium_overrides={"IWM": 75_000.0}, limit=3))

    assert "INSERT INTO live.nexus_db_signal_emission" in sql
    assert "ON CONFLICT (symbol, session_date, strategy_key, entry_time, decision) DO NOTHING" in sql
    assert "INSERT INTO live.nexus_strategy_signal" in sql
    assert "'INTERMEDIATE'" in sql
    assert "'sigmatiq_nexus_db_replay'" in sql
    assert "p.strategy_key" in sql
    assert "LIMIT 3" in sql
    assert json.dumps({"IWM": 75_000.0}) in sql


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
        "--dry-run",
    ])

    assert config.symbols == ("SPY", "QQQ")
    assert config.strategies == ("etf_open_specialist",)
    assert config.session_date == "2026-05-20"
    assert config.dry_run is True
    assert "host=db" in config.database_url


def test_parse_args_rejects_unsupported_strategy():
    try:
        parse_args([
            "--database-url",
            "postgres://example",
            "--strategies",
            "etf_flow_specialist",
        ])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("expected unsupported strategy to fail")
