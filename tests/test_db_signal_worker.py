from datetime import datetime
from zoneinfo import ZoneInfo

from sigmatiq_nexus.db_signal_worker import current_session_date, parse_args, run_once


def test_current_session_date_uses_new_york_date():
    assert current_session_date(datetime(2026, 5, 21, 2, 0, tzinfo=ZoneInfo("UTC"))) == "2026-05-20"


def test_parse_args_defaults_to_relaxed_watch_included_for_worker():
    config = parse_args([
        "--database-url",
        "Host=db;Database=shared;Username=u;Password=p;SSL Mode=Require",
        "--session-date",
        "2026-05-20",
        "--symbols",
        "spy,qqq",
        "--once",
    ])

    assert config.once is True
    assert config.session_date_override == "2026-05-20"
    assert config.emitter.symbols == ("SPY", "QQQ")
    assert config.emitter.include_relaxed_watch is True
    assert config.emitter.relaxed_watch_only is False
    assert config.emitter.max_realtime_lag_seconds == 300
    assert "host=db" in config.emitter.database_url


def test_run_once_rebuilds_emitter_for_current_session():
    seen = []

    config = parse_args([
        "--database-url",
        "postgres://example",
        "--session-date",
        "2026-05-20",
        "--symbols",
        "SPY",
        "--once",
        "--dry-run",
    ])

    def runner(emitter):
        seen.append(emitter)
        return [{"symbol": "SPY"}]

    rows = run_once(config, runner=runner)

    assert rows == [{"symbol": "SPY"}]
    assert seen[0].session_date == "2026-05-20"
    assert seen[0].dry_run is True
    assert seen[0].include_relaxed_watch is True
    assert seen[0].max_realtime_lag_seconds == 300
