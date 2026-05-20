from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import date
from typing import Any

from sigmatiq_nexus.signal_potential_audit import normalize_database_url, parse_overrides, psql_json


DEFAULT_SYMBOLS = ("SPY", "QQQ", "IWM", "UVXY")
DEFAULT_STRATEGIES = ("etf_open_specialist", "etf_low_sweep_core")
DEFAULT_MIN_WINDOW_PREMIUM = 200_000.0
DEFAULT_OPEN_CALL_DOMINANCE = 1.35
DEFAULT_SIDE_DOMINANCE = 2.0
DEFAULT_MAX_SWEEP_RATIO = 0.10
SOURCE_NAME = "sigmatiq_nexus_db_replay"


@dataclass(frozen=True)
class EmitterConfig:
    database_url: str
    session_date: str
    symbols: tuple[str, ...]
    strategies: tuple[str, ...]
    min_window_premium: float
    premium_overrides: dict[str, float]
    open_call_dominance: float
    side_dominance: float
    max_sweep_ratio: float
    dry_run: bool
    limit: int | None


def confidence_for_open_call_dominance(call_premium: float, total_premium: float) -> float:
    return confidence_from_share(call_premium, total_premium)


def confidence_from_share(side_premium: float, total_premium: float) -> float:
    if total_premium <= 0:
        return 0.0
    side_share = max(0.0, min(1.0, side_premium / total_premium))
    return round(min(0.99, max(0.60, 0.65 + min(side_share - 0.50, 0.30))), 4)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _json_literal(value: Any) -> str:
    return _sql_literal(json.dumps(value))


def _strategy_filter(config: EmitterConfig) -> str:
    return "SELECT value::text AS strategy_key FROM jsonb_array_elements_text(" + _json_literal(list(config.strategies)) + "::jsonb)"


def _base_ctes(config: EmitterConfig) -> str:
    symbols_json = _json_literal(list(config.symbols))
    overrides_json = _json_literal(config.premium_overrides)
    session_date = _sql_literal(config.session_date)

    return f"""
WITH requested_symbols AS (
    SELECT upper(value::text) AS symbol
    FROM jsonb_array_elements_text({symbols_json}::jsonb)
),
requested_strategies AS (
    {_strategy_filter(config)}
),
premium_overrides AS (
    SELECT upper(key) AS symbol, value::numeric AS min_window_premium
    FROM jsonb_each_text({overrides_json}::jsonb)
),
base_windows AS (
    SELECT
        c.symbol,
        c.session_date,
        c.window_label,
        CASE c.window_label
            WHEN 'w0930_1000' THEN '10:00'
            WHEN 'w1000_1030' THEN '10:30'
            ELSE c.window_label
        END AS entry_time,
        c.window_start_utc,
        c.window_end_utc,
        c.total_premium,
        c.call_premium,
        c.put_premium,
        c.option_trade_count,
        c.lead_contract_raw_symbol,
        substring(c.lead_contract_raw_symbol from '([CP])[0-9]{{8}}$') AS lead_option_side,
        c.lead_quote_ts_utc,
        c.lead_quote_mid,
        c.lead_quote_age_seconds,
        c.iv_rank_pct,
        c.atm_iv,
        c.net_gex,
        c.sweep_ratio,
        c.sweep_quality_status,
        c.aggressor_quality_status,
        c.daily_context_status,
        c.readiness_status,
        c.final_quote_status,
        c.replay_gate_status,
        c.replay_missing_inputs_json,
        c.materialized_at_utc,
        coalesce(po.min_window_premium, {config.min_window_premium}::numeric) AS min_window_premium
    FROM live.prepared_nexus_window_context c
    JOIN requested_symbols s ON s.symbol = c.symbol
    LEFT JOIN premium_overrides po ON po.symbol = c.symbol
    WHERE c.session_date = {session_date}::date
      AND c.window_label IN ('w0930_1000', 'w1000_1030')
      AND c.readiness_status = 'ready'
      AND c.daily_context_status IN ('ready', 'healthy')
      AND c.final_quote_status = 'fresh_quote_asof_window'
      AND c.lead_contract_raw_symbol IS NOT NULL
      AND c.lead_quote_mid IS NOT NULL
),
strategy_candidates AS (
    SELECT
        'etf_open_specialist'::text AS strategy_key,
        bw.symbol,
        bw.session_date,
        bw.window_label,
        bw.entry_time,
        bw.window_start_utc,
        bw.window_end_utc,
        bw.total_premium,
        bw.call_premium,
        bw.put_premium,
        bw.option_trade_count,
        bw.lead_contract_raw_symbol,
        bw.lead_option_side,
        bw.lead_quote_ts_utc,
        bw.lead_quote_mid,
        bw.lead_quote_age_seconds,
        bw.iv_rank_pct,
        bw.atm_iv,
        bw.net_gex,
        bw.sweep_ratio,
        bw.sweep_quality_status,
        bw.aggressor_quality_status,
        bw.daily_context_status,
        bw.readiness_status,
        bw.final_quote_status,
        bw.replay_gate_status,
        bw.replay_missing_inputs_json,
        bw.materialized_at_utc,
        bw.min_window_premium,
        'BULLISH'::text AS sentiment,
        'open30_call_dominance'::text AS replay_reason,
        bw.call_premium AS signal_side_premium,
        round(least(0.99, greatest(0.60, 0.65 + least((bw.call_premium / nullif(bw.total_premium, 0)) - 0.50, 0.30))), 4) AS confidence
    FROM base_windows bw
    WHERE bw.window_label = 'w0930_1000'
      AND bw.lead_option_side = 'C'
      AND bw.total_premium >= bw.min_window_premium
      AND bw.call_premium >= bw.min_window_premium
      AND bw.call_premium > bw.put_premium * {config.open_call_dominance}::numeric

    UNION ALL

    SELECT
        'etf_low_sweep_core'::text AS strategy_key,
        bw.symbol,
        bw.session_date,
        bw.window_label,
        bw.entry_time,
        bw.window_start_utc,
        bw.window_end_utc,
        bw.total_premium,
        bw.call_premium,
        bw.put_premium,
        bw.option_trade_count,
        bw.lead_contract_raw_symbol,
        bw.lead_option_side,
        bw.lead_quote_ts_utc,
        bw.lead_quote_mid,
        bw.lead_quote_age_seconds,
        bw.iv_rank_pct,
        bw.atm_iv,
        bw.net_gex,
        bw.sweep_ratio,
        bw.sweep_quality_status,
        bw.aggressor_quality_status,
        bw.daily_context_status,
        bw.readiness_status,
        bw.final_quote_status,
        bw.replay_gate_status,
        bw.replay_missing_inputs_json,
        bw.materialized_at_utc,
        bw.min_window_premium,
        CASE WHEN bw.put_premium > bw.call_premium * {config.side_dominance}::numeric THEN 'BEARISH' ELSE 'BULLISH' END AS sentiment,
        CASE WHEN bw.put_premium > bw.call_premium * {config.side_dominance}::numeric THEN 'low_sweep_put_dominance' ELSE 'low_sweep_call_dominance' END AS replay_reason,
        CASE WHEN bw.put_premium > bw.call_premium * {config.side_dominance}::numeric THEN bw.put_premium ELSE bw.call_premium END AS signal_side_premium,
        round(
            least(
                0.99,
                greatest(
                    0.60,
                    0.65 + least(
                        (CASE WHEN bw.put_premium > bw.call_premium * {config.side_dominance}::numeric THEN bw.put_premium ELSE bw.call_premium END / nullif(bw.total_premium, 0)) - 0.50,
                        0.30
                    )
                )
            ),
            4
        ) AS confidence
    FROM base_windows bw
    WHERE bw.window_label IN ('w0930_1000', 'w1000_1030')
      AND bw.total_premium >= bw.min_window_premium
      AND bw.sweep_quality_status = 'ready'
      AND bw.sweep_ratio IS NOT NULL
      AND bw.sweep_ratio <= {config.max_sweep_ratio}::numeric
      AND (
          (bw.entry_time IN ('10:00', '10:30')
           AND bw.call_premium > bw.put_premium * {config.side_dominance}::numeric
           AND bw.lead_option_side = 'C')
          OR
          (bw.entry_time = '10:30'
           AND bw.put_premium > bw.call_premium * {config.side_dominance}::numeric
           AND bw.lead_option_side = 'P')
      )
),
eligible AS (
    SELECT sc.*
    FROM strategy_candidates sc
    JOIN requested_strategies rs ON rs.strategy_key = sc.strategy_key
    WHERE NOT EXISTS (
        SELECT 1
        FROM live.nexus_strategy_signal existing
        WHERE existing.symbol = sc.symbol
          AND existing.session_date = sc.session_date
          AND existing.strategy_key = sc.strategy_key
          AND existing.entry_time = sc.entry_time
          AND existing.decision IN ('INTERMEDIATE', 'BET')
    )
    ORDER BY sc.symbol, sc.entry_time, sc.strategy_key
"""


def build_candidate_sql(config: EmitterConfig) -> str:
    limit_clause = f"LIMIT {int(config.limit)}" if config.limit else ""
    return f"""
{_base_ctes(config)}
    {limit_clause}
)
SELECT COALESCE(jsonb_agg(to_jsonb(eligible) ORDER BY symbol, entry_time, strategy_key), '[]'::jsonb)
FROM eligible;
"""


def build_emit_sql(config: EmitterConfig) -> str:
    limit_clause = f"LIMIT {int(config.limit)}" if config.limit else ""
    source = _sql_literal(SOURCE_NAME)

    return f"""
{_base_ctes(config)}
    {limit_clause}
),
payloads AS (
    SELECT
        e.*,
        ('dbreplay:' || e.session_date::text || ':' || e.symbol || ':' || e.strategy_key || ':' || e.entry_time || ':' || e.lead_contract_raw_symbol) AS signal_id,
        jsonb_build_object(
            'message_id', 'dbreplay:' || md5(e.symbol || e.session_date::text || e.strategy_key || e.entry_time || e.lead_contract_raw_symbol || now()::text),
            'signal_id', 'dbreplay:' || e.session_date::text || ':' || e.symbol || ':' || e.strategy_key || ':' || e.entry_time || ':' || e.lead_contract_raw_symbol,
            'position_id', 'dbreplay:' || e.session_date::text || ':' || e.symbol || ':' || e.strategy_key || ':' || e.entry_time || ':' || e.lead_contract_raw_symbol,
            'strategy', e.strategy_key,
            'symbol', e.symbol,
            'stage', 1,
            'decision', 'INTERMEDIATE',
            'sentiment', e.sentiment,
            'confidence', e.confidence,
            'timestamp', now(),
            'source', {source},
            'session_date', e.session_date::text,
            'entry_time', e.entry_time,
            'window_start', e.window_start_utc,
            'window_end', e.window_end_utc,
            'raw_symbol', e.lead_contract_raw_symbol,
            'entry_quote', jsonb_build_object(
                'option_mid', e.lead_quote_mid,
                'quote_ts', e.lead_quote_ts_utc,
                'quote_age_at_window_end_seconds', e.lead_quote_age_seconds,
                'quote_freshness', e.final_quote_status
            ),
            'db_replay', jsonb_build_object(
                'materialized_at_utc', e.materialized_at_utc,
                'window_label', e.window_label,
                'replay_reason', e.replay_reason,
                'total_premium', e.total_premium,
                'call_premium', e.call_premium,
                'put_premium', e.put_premium,
                'signal_side_premium', e.signal_side_premium,
                'min_window_premium', e.min_window_premium,
                'open_call_dominance', {config.open_call_dominance},
                'side_dominance', {config.side_dominance},
                'max_sweep_ratio', {config.max_sweep_ratio},
                'sweep_ratio', e.sweep_ratio,
                'sweep_quality_status', e.sweep_quality_status,
                'aggressor_quality_status', e.aggressor_quality_status,
                'daily_context_status', e.daily_context_status,
                'readiness_status', e.readiness_status,
                'replay_gate_status', e.replay_gate_status,
                'replay_missing_inputs', e.replay_missing_inputs_json,
                'iv_rank_pct', e.iv_rank_pct,
                'atm_iv', e.atm_iv,
                'net_gex', e.net_gex
            ),
            'reason_summary', 'DB replay found a strategy candidate with persisted window flow, EOD context, and quote-as-of-window evidence. This is an audit candidate, not a Redis live-overlay BET.'
        ) AS payload_json
    FROM eligible e
),
emissions AS (
    INSERT INTO live.nexus_db_signal_emission (
        symbol, session_date, strategy_key, entry_time, decision,
        source_window_label, source_materialized_at_utc, payload_json
    )
    SELECT
        p.symbol,
        p.session_date,
        p.strategy_key,
        p.entry_time,
        'INTERMEDIATE',
        p.window_label,
        p.materialized_at_utc,
        p.payload_json
    FROM payloads p
    ON CONFLICT (symbol, session_date, strategy_key, entry_time, decision) DO NOTHING
    RETURNING *
),
signals AS (
    INSERT INTO live.nexus_strategy_signal (
        strategy_key, symbol, session_date, ts_utc, redis_key, stage, decision,
        sentiment, confidence, entry_time, window_start, window_end, source,
        worker_name, worker_version, payload_json
    )
    SELECT
        p.strategy_key,
        p.symbol,
        p.session_date,
        now(),
        'nexus_db_replay_intermediate:' || p.symbol || ':' || p.strategy_key || ':' || p.entry_time,
        1,
        'INTERMEDIATE',
        p.sentiment,
        p.confidence,
        p.entry_time,
        p.window_start_utc::text,
        p.window_end_utc::text,
        {source},
        'sigmatiq_nexus_db_signal_emitter',
        'v1',
        p.payload_json
    FROM payloads p
    JOIN emissions e
      ON e.symbol = p.symbol
     AND e.session_date = p.session_date
     AND e.strategy_key = p.strategy_key
     AND e.entry_time = p.entry_time
     AND e.decision = 'INTERMEDIATE'
    RETURNING symbol, session_date::text, strategy_key, entry_time, decision, confidence, payload_json
)
SELECT COALESCE(jsonb_agg(to_jsonb(signals) ORDER BY symbol, entry_time, strategy_key), '[]'::jsonb)
FROM signals;
"""


def run(config: EmitterConfig) -> list[dict[str, Any]]:
    sql = build_candidate_sql(config) if config.dry_run else build_emit_sql(config)
    return psql_json(config.database_url, sql)


def parse_args(argv: list[str]) -> EmitterConfig:
    parser = argparse.ArgumentParser(
        description="Emit DB-proven Nexus replay candidates into live.nexus_strategy_signal."
    )
    parser.add_argument("--database-url", default=os.environ.get("SHARED_DATABASE_URL") or os.environ.get("DATABASE_URL"))
    parser.add_argument("--session-date", default=date.today().isoformat())
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--strategies", default=",".join(DEFAULT_STRATEGIES))
    parser.add_argument("--min-window-premium", type=float, default=DEFAULT_MIN_WINDOW_PREMIUM)
    parser.add_argument("--min-window-premium-overrides", default=os.environ.get("NEXUS_MIN_WINDOW_PREMIUM_OVERRIDES", ""))
    parser.add_argument("--open-call-dominance", type=float, default=float(os.environ.get("NEXUS_DB_REPLAY_OPEN_CALL_DOMINANCE", DEFAULT_OPEN_CALL_DOMINANCE)))
    parser.add_argument("--side-dominance", type=float, default=float(os.environ.get("NEXUS_DB_REPLAY_SIDE_DOMINANCE", DEFAULT_SIDE_DOMINANCE)))
    parser.add_argument("--max-sweep-ratio", type=float, default=float(os.environ.get("NEXUS_DB_REPLAY_MAX_SWEEP_RATIO", DEFAULT_MAX_SWEEP_RATIO)))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args(argv)
    if not args.database_url:
        parser.error("--database-url or SHARED_DATABASE_URL/DATABASE_URL is required")
    try:
        session_date = date.fromisoformat(args.session_date).isoformat()
    except ValueError:
        parser.error("--session-date must be YYYY-MM-DD")
    symbols = tuple(sym.strip().upper() for sym in args.symbols.split(",") if sym.strip())
    strategies = tuple(strategy.strip() for strategy in args.strategies.split(",") if strategy.strip())
    unsupported = sorted(set(strategies) - set(DEFAULT_STRATEGIES))
    if unsupported:
        parser.error(f"unsupported strategies for DB replay v1: {', '.join(unsupported)}")
    return EmitterConfig(
        database_url=normalize_database_url(args.database_url),
        session_date=session_date,
        symbols=symbols,
        strategies=strategies,
        min_window_premium=args.min_window_premium,
        premium_overrides=parse_overrides(args.min_window_premium_overrides),
        open_call_dominance=args.open_call_dominance,
        side_dominance=args.side_dominance,
        max_sweep_ratio=args.max_sweep_ratio,
        dry_run=args.dry_run,
        limit=args.limit,
    )


def main(argv: list[str] | None = None) -> int:
    config = parse_args(argv or sys.argv[1:])
    rows = run(config)
    print(json.dumps({
        "sessionDate": config.session_date,
        "symbols": list(config.symbols),
        "strategies": list(config.strategies),
        "dryRun": config.dry_run,
        "emittedCount": 0 if config.dry_run else len(rows),
        "candidateCount": len(rows),
        "rows": rows,
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
