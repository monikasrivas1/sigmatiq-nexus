from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable
from zoneinfo import ZoneInfo

from sigmatiq_nexus.db_signal_emitter import (
    DEFAULT_MAX_SWEEP_RATIO,
    DEFAULT_MIN_WINDOW_PREMIUM,
    DEFAULT_OPEN_CALL_DOMINANCE,
    DEFAULT_RELAXED_FLOW_SWEEP_RATIO,
    DEFAULT_RELAXED_MOMENTUM_MAX_IV_RANK,
    DEFAULT_RELAXED_SPREAD_MAX_IV_RANK,
    DEFAULT_SIDE_DOMINANCE,
    DEFAULT_SPREAD_MAX_IV_RANK,
    DEFAULT_STRATEGIES,
    DEFAULT_SYMBOLS,
    EmitterConfig,
    run as run_emitter,
)
from sigmatiq_nexus.signal_potential_audit import normalize_database_url, parse_overrides


NY = ZoneInfo("America/New_York")
DEFAULT_POLL_SECONDS = 30


@dataclass(frozen=True)
class WorkerConfig:
    emitter: EmitterConfig
    poll_seconds: int
    once: bool
    session_date_override: str | None


def current_session_date(now: datetime | None = None) -> str:
    now = now or datetime.now(tz=NY)
    return now.astimezone(NY).date().isoformat()


def log_event(event: str, **fields: Any) -> None:
    print(json.dumps({"event": event, "tsUtc": datetime.utcnow().isoformat() + "Z", **fields}, default=str), flush=True)


def run_once(config: WorkerConfig, runner: Callable[[EmitterConfig], list[dict[str, Any]]] = run_emitter) -> list[dict[str, Any]]:
    session_date = config.session_date_override or current_session_date()
    emitter = EmitterConfig(
        database_url=config.emitter.database_url,
        session_date=session_date,
        symbols=config.emitter.symbols,
        strategies=config.emitter.strategies,
        min_window_premium=config.emitter.min_window_premium,
        premium_overrides=config.emitter.premium_overrides,
        open_call_dominance=config.emitter.open_call_dominance,
        side_dominance=config.emitter.side_dominance,
        max_sweep_ratio=config.emitter.max_sweep_ratio,
        spread_max_iv_rank=config.emitter.spread_max_iv_rank,
        include_relaxed_watch=config.emitter.include_relaxed_watch,
        relaxed_watch_only=config.emitter.relaxed_watch_only,
        relaxed_flow_sweep_ratio=config.emitter.relaxed_flow_sweep_ratio,
        relaxed_momentum_max_iv_rank=config.emitter.relaxed_momentum_max_iv_rank,
        relaxed_spread_max_iv_rank=config.emitter.relaxed_spread_max_iv_rank,
        dry_run=config.emitter.dry_run,
        limit=config.emitter.limit,
        max_realtime_lag_seconds=config.emitter.max_realtime_lag_seconds,
    )
    started = time.monotonic()
    rows = runner(emitter)
    log_event(
        "db_signal_worker_poll_completed",
        sessionDate=session_date,
        dryRun=emitter.dry_run,
        includeRelaxedWatch=emitter.include_relaxed_watch,
        relaxedWatchOnly=emitter.relaxed_watch_only,
        emittedOrCandidateCount=len(rows),
        elapsedMs=round((time.monotonic() - started) * 1000, 2),
    )
    return rows


def run_loop(config: WorkerConfig, runner: Callable[[EmitterConfig], list[dict[str, Any]]] = run_emitter) -> int:
    log_event(
        "db_signal_worker_started",
        symbols=list(config.emitter.symbols),
        strategies=list(config.emitter.strategies),
        pollSeconds=config.poll_seconds,
        dryRun=config.emitter.dry_run,
        includeRelaxedWatch=config.emitter.include_relaxed_watch,
        relaxedWatchOnly=config.emitter.relaxed_watch_only,
        sessionDateOverride=config.session_date_override,
    )
    while True:
        try:
            run_once(config, runner=runner)
        except Exception as exc:  # pragma: no cover - exercised by runtime logs
            log_event("db_signal_worker_poll_failed", error=str(exc), errorType=type(exc).__name__)
        if config.once:
            return 0
        time.sleep(config.poll_seconds)


def parse_args(argv: list[str]) -> WorkerConfig:
    parser = argparse.ArgumentParser(
        description="Poll DB-materialized Nexus strategy inputs and emit DB replay signals."
    )
    parser.add_argument("--database-url", default=os.environ.get("SHARED_DATABASE_URL") or os.environ.get("DATABASE_URL"))
    parser.add_argument("--session-date", default=os.environ.get("NEXUS_DB_SIGNAL_SESSION_DATE"))
    parser.add_argument("--symbols", default=os.environ.get("NEXUS_DB_SIGNAL_SYMBOLS") or os.environ.get("NEXUS_SYMBOLS", ",".join(DEFAULT_SYMBOLS)))
    parser.add_argument("--strategies", default=os.environ.get("NEXUS_DB_SIGNAL_STRATEGIES", ",".join(DEFAULT_STRATEGIES)))
    parser.add_argument("--poll-seconds", type=int, default=int(os.environ.get("NEXUS_DB_SIGNAL_POLL_SECONDS", DEFAULT_POLL_SECONDS)))
    parser.add_argument("--min-window-premium", type=float, default=float(os.environ.get("NEXUS_DB_REPLAY_MIN_WINDOW_PREMIUM", DEFAULT_MIN_WINDOW_PREMIUM)))
    parser.add_argument("--min-window-premium-overrides", default=os.environ.get("NEXUS_MIN_WINDOW_PREMIUM_OVERRIDES", ""))
    parser.add_argument("--open-call-dominance", type=float, default=float(os.environ.get("NEXUS_DB_REPLAY_OPEN_CALL_DOMINANCE", DEFAULT_OPEN_CALL_DOMINANCE)))
    parser.add_argument("--side-dominance", type=float, default=float(os.environ.get("NEXUS_DB_REPLAY_SIDE_DOMINANCE", DEFAULT_SIDE_DOMINANCE)))
    parser.add_argument("--max-sweep-ratio", type=float, default=float(os.environ.get("NEXUS_DB_REPLAY_MAX_SWEEP_RATIO", DEFAULT_MAX_SWEEP_RATIO)))
    parser.add_argument("--spread-max-iv-rank", type=float, default=float(os.environ.get("NEXUS_DB_REPLAY_SPREAD_MAX_IV_RANK", DEFAULT_SPREAD_MAX_IV_RANK)))
    parser.add_argument("--include-relaxed-watch", action="store_true", default=os.environ.get("NEXUS_DB_REPLAY_INCLUDE_RELAXED_WATCH", "true").lower() in {"1", "true", "yes"})
    parser.add_argument("--relaxed-watch-only", action="store_true", default=os.environ.get("NEXUS_DB_REPLAY_RELAXED_WATCH_ONLY", "").lower() in {"1", "true", "yes"})
    parser.add_argument("--relaxed-flow-sweep-ratio", type=float, default=float(os.environ.get("NEXUS_DB_REPLAY_RELAXED_FLOW_SWEEP_RATIO", DEFAULT_RELAXED_FLOW_SWEEP_RATIO)))
    parser.add_argument("--relaxed-momentum-max-iv-rank", type=float, default=float(os.environ.get("NEXUS_DB_REPLAY_RELAXED_MOMENTUM_MAX_IV_RANK", DEFAULT_RELAXED_MOMENTUM_MAX_IV_RANK)))
    parser.add_argument("--relaxed-spread-max-iv-rank", type=float, default=float(os.environ.get("NEXUS_DB_REPLAY_RELAXED_SPREAD_MAX_IV_RANK", DEFAULT_RELAXED_SPREAD_MAX_IV_RANK)))
    parser.add_argument("--dry-run", action="store_true", default=os.environ.get("NEXUS_DB_SIGNAL_DRY_RUN", "").lower() in {"1", "true", "yes"})
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--max-realtime-lag-seconds", type=int, default=int(os.environ.get("NEXUS_DB_SIGNAL_MAX_WINDOW_LAG_SECONDS", "300")))
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)

    if not args.database_url:
        parser.error("--database-url or SHARED_DATABASE_URL/DATABASE_URL is required")
    if args.session_date:
        try:
            session_date_override = date.fromisoformat(args.session_date).isoformat()
        except ValueError:
            parser.error("--session-date must be YYYY-MM-DD")
    else:
        session_date_override = None
    if args.poll_seconds < 5:
        parser.error("--poll-seconds must be at least 5")
    if args.max_realtime_lag_seconds < 0:
        parser.error("--max-realtime-lag-seconds must be zero or greater")

    symbols = tuple(sym.strip().upper() for sym in args.symbols.split(",") if sym.strip())
    strategies = tuple(strategy.strip() for strategy in args.strategies.split(",") if strategy.strip())
    unsupported = sorted(set(strategies) - set(DEFAULT_STRATEGIES))
    if unsupported:
        parser.error(f"unsupported strategies for DB replay worker: {', '.join(unsupported)}")

    emitter = EmitterConfig(
        database_url=normalize_database_url(args.database_url),
        session_date=session_date_override or current_session_date(),
        symbols=symbols,
        strategies=strategies,
        min_window_premium=args.min_window_premium,
        premium_overrides=parse_overrides(args.min_window_premium_overrides),
        open_call_dominance=args.open_call_dominance,
        side_dominance=args.side_dominance,
        max_sweep_ratio=args.max_sweep_ratio,
        spread_max_iv_rank=args.spread_max_iv_rank,
        include_relaxed_watch=args.include_relaxed_watch,
        relaxed_watch_only=args.relaxed_watch_only,
        relaxed_flow_sweep_ratio=args.relaxed_flow_sweep_ratio,
        relaxed_momentum_max_iv_rank=args.relaxed_momentum_max_iv_rank,
        relaxed_spread_max_iv_rank=args.relaxed_spread_max_iv_rank,
        dry_run=args.dry_run,
        limit=args.limit,
        max_realtime_lag_seconds=args.max_realtime_lag_seconds,
    )
    return WorkerConfig(
        emitter=emitter,
        poll_seconds=args.poll_seconds,
        once=args.once,
        session_date_override=session_date_override,
    )


def main(argv: list[str] | None = None) -> int:
    return run_loop(parse_args(argv or sys.argv[1:]))


if __name__ == "__main__":
    raise SystemExit(main())
