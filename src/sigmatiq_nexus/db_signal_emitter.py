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
DEFAULT_MIN_WINDOW_PREMIUM = 200_000.0
DEFAULT_OPEN_CALL_DOMINANCE = 1.35
SOURCE_NAME = "sigmatiq_nexus_db_replay"
STRATEGY_KEY = "etf_open_specialist"
ENTRY_TIME = "10:00"
WINDOW_LABEL = "w0930_1000"


@dataclass(frozen=True)
class EmitterConfig:
    database_url: str
    session_date: str
    symbols: tuple[str, ...]
    min_window_premium: float
    premium_overrides: dict[str, float]
    open_call_dominance: float
    dry_run: bool
    limit: int | None


def confidence_for_open_call_dominance(call_premium: float, total_premium: float) -> float:
    if total_premium <= 0:
        return 0.0
    call_share = max(0.0, min(1.0, call_premium / total_premium))
    return round(min(0.99, max(0.60, 0.65 + min(call_share - 0.50, 0.30))), 4)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _json_literal(value: Any) -> str:
    return _sql_literal(json.dumps(value))


def build_candidate_sql(config: EmitterConfig) -> str:
    symbols_json = _json_literal(list(config.symbols))
    overrides_json = _json_literal(config.premium_overrides)
    limit_clause = f"LIMIT {int(config.limit)}" if config.limit else ""
    session_date = _sql_literal(config.session_date)

    return f"""
WITH requested_symbols AS (
    SELECT upper(value::text) AS symbol
    FROM jsonb_array_elements_text({symbols_json}::jsonb)
),
premium_overrides AS (
    SELECT upper(key) AS symbol, value::numeric AS min_window_premium
    FROM jsonb_each_text({overrides_json}::jsonb)
),
base_candidates AS (
    SELECT
        c.symbol,
        c.session_date,
        c.window_label,
        '{ENTRY_TIME}'::text AS entry_time,
        c.window_start_utc,
        c.window_end_utc,
        c.total_premium,
        c.call_premium,
        c.put_premium,
        c.option_trade_count,
        c.lead_contract_raw_symbol,
        c.lead_quote_ts_utc,
        c.lead_quote_mid,
        c.lead_quote_age_seconds,
        c.iv_rank_pct,
        c.atm_iv,
        c.net_gex,
        c.daily_context_status,
        c.readiness_status,
        c.final_quote_status,
        c.replay_gate_status,
        c.replay_missing_inputs_json,
        c.materialized_at_utc,
        coalesce(po.min_window_premium, {config.min_window_premium}::numeric) AS min_window_premium,
        CASE WHEN c.total_premium > 0
            THEN round(least(0.99, greatest(0.60, 0.65 + least((c.call_premium / nullif(c.total_premium, 0)) - 0.50, 0.30))), 4)
            ELSE 0.0
        END AS confidence
    FROM live.prepared_nexus_window_context c
    JOIN requested_symbols s ON s.symbol = c.symbol
    LEFT JOIN premium_overrides po ON po.symbol = c.symbol
    WHERE c.session_date = {session_date}::date
      AND c.window_label = '{WINDOW_LABEL}'
      AND c.readiness_status = 'ready'
      AND c.daily_context_status IN ('ready', 'healthy')
      AND c.final_quote_status = 'fresh_quote_asof_window'
      AND c.lead_contract_raw_symbol IS NOT NULL
      AND c.lead_quote_mid IS NOT NULL
),
eligible AS (
    SELECT *
    FROM base_candidates bc
    WHERE bc.total_premium >= bc.min_window_premium
      AND bc.call_premium >= bc.min_window_premium
      AND bc.call_premium > bc.put_premium * {config.open_call_dominance}::numeric
      AND NOT EXISTS (
          SELECT 1
          FROM live.nexus_strategy_signal existing
          WHERE existing.symbol = bc.symbol
            AND existing.session_date = bc.session_date
            AND existing.strategy_key = '{STRATEGY_KEY}'
            AND existing.entry_time = bc.entry_time
            AND existing.decision IN ('INTERMEDIATE', 'BET')
      )
    ORDER BY bc.symbol
    {limit_clause}
)
SELECT COALESCE(jsonb_agg(to_jsonb(eligible) ORDER BY symbol), '[]'::jsonb)
FROM eligible;
"""


def build_emit_sql(config: EmitterConfig) -> str:
    symbols_json = _json_literal(list(config.symbols))
    overrides_json = _json_literal(config.premium_overrides)
    limit_clause = f"LIMIT {int(config.limit)}" if config.limit else ""
    session_date = _sql_literal(config.session_date)
    source = _sql_literal(SOURCE_NAME)

    return f"""
WITH requested_symbols AS (
    SELECT upper(value::text) AS symbol
    FROM jsonb_array_elements_text({symbols_json}::jsonb)
),
premium_overrides AS (
    SELECT upper(key) AS symbol, value::numeric AS min_window_premium
    FROM jsonb_each_text({overrides_json}::jsonb)
),
base_candidates AS (
    SELECT
        c.symbol,
        c.session_date,
        c.window_label,
        '{ENTRY_TIME}'::text AS entry_time,
        c.window_start_utc,
        c.window_end_utc,
        c.total_premium,
        c.call_premium,
        c.put_premium,
        c.option_trade_count,
        c.lead_contract_raw_symbol,
        c.lead_quote_ts_utc,
        c.lead_quote_mid,
        c.lead_quote_age_seconds,
        c.iv_rank_pct,
        c.atm_iv,
        c.net_gex,
        c.daily_context_status,
        c.readiness_status,
        c.final_quote_status,
        c.replay_gate_status,
        c.replay_missing_inputs_json,
        c.materialized_at_utc,
        coalesce(po.min_window_premium, {config.min_window_premium}::numeric) AS min_window_premium,
        CASE WHEN c.total_premium > 0
            THEN round(least(0.99, greatest(0.60, 0.65 + least((c.call_premium / nullif(c.total_premium, 0)) - 0.50, 0.30))), 4)
            ELSE 0.0
        END AS confidence
    FROM live.prepared_nexus_window_context c
    JOIN requested_symbols s ON s.symbol = c.symbol
    LEFT JOIN premium_overrides po ON po.symbol = c.symbol
    WHERE c.session_date = {session_date}::date
      AND c.window_label = '{WINDOW_LABEL}'
      AND c.readiness_status = 'ready'
      AND c.daily_context_status IN ('ready', 'healthy')
      AND c.final_quote_status = 'fresh_quote_asof_window'
      AND c.lead_contract_raw_symbol IS NOT NULL
      AND c.lead_quote_mid IS NOT NULL
),
eligible AS (
    SELECT *
    FROM base_candidates bc
    WHERE bc.total_premium >= bc.min_window_premium
      AND bc.call_premium >= bc.min_window_premium
      AND bc.call_premium > bc.put_premium * {config.open_call_dominance}::numeric
      AND NOT EXISTS (
          SELECT 1
          FROM live.nexus_strategy_signal existing
          WHERE existing.symbol = bc.symbol
            AND existing.session_date = bc.session_date
            AND existing.strategy_key = '{STRATEGY_KEY}'
            AND existing.entry_time = bc.entry_time
            AND existing.decision IN ('INTERMEDIATE', 'BET')
      )
    ORDER BY bc.symbol
    {limit_clause}
),
payloads AS (
    SELECT
        e.*,
        ('dbreplay:' || e.session_date::text || ':' || e.symbol || ':{STRATEGY_KEY}:{ENTRY_TIME}:' || e.lead_contract_raw_symbol) AS signal_id,
        jsonb_build_object(
            'message_id', 'dbreplay:' || md5(e.symbol || e.session_date::text || e.lead_contract_raw_symbol || now()::text),
            'signal_id', 'dbreplay:' || e.session_date::text || ':' || e.symbol || ':{STRATEGY_KEY}:{ENTRY_TIME}:' || e.lead_contract_raw_symbol,
            'position_id', 'dbreplay:' || e.session_date::text || ':' || e.symbol || ':{STRATEGY_KEY}:{ENTRY_TIME}:' || e.lead_contract_raw_symbol,
            'strategy', '{STRATEGY_KEY}',
            'symbol', e.symbol,
            'stage', 1,
            'decision', 'INTERMEDIATE',
            'sentiment', 'BULLISH',
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
                'total_premium', e.total_premium,
                'call_premium', e.call_premium,
                'put_premium', e.put_premium,
                'min_window_premium', e.min_window_premium,
                'open_call_dominance', {config.open_call_dominance},
                'daily_context_status', e.daily_context_status,
                'readiness_status', e.readiness_status,
                'replay_gate_status', e.replay_gate_status,
                'replay_missing_inputs', e.replay_missing_inputs_json,
                'iv_rank_pct', e.iv_rank_pct,
                'atm_iv', e.atm_iv,
                'net_gex', e.net_gex
            ),
            'reason_summary', 'DB replay found an open30 call-dominance candidate with persisted window flow, EOD context, and quote-as-of-window evidence. This is an audit candidate, not a Redis live-overlay BET.'
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
        '{STRATEGY_KEY}',
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
        '{STRATEGY_KEY}',
        p.symbol,
        p.session_date,
        now(),
        'nexus_db_replay_intermediate:' || p.symbol || ':{STRATEGY_KEY}:{ENTRY_TIME}',
        1,
        'INTERMEDIATE',
        'BULLISH',
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
     AND e.strategy_key = '{STRATEGY_KEY}'
     AND e.entry_time = p.entry_time
     AND e.decision = 'INTERMEDIATE'
    RETURNING symbol, session_date::text, strategy_key, entry_time, decision, confidence, payload_json
)
SELECT COALESCE(jsonb_agg(to_jsonb(signals) ORDER BY symbol), '[]'::jsonb)
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
    parser.add_argument("--min-window-premium", type=float, default=DEFAULT_MIN_WINDOW_PREMIUM)
    parser.add_argument("--min-window-premium-overrides", default=os.environ.get("NEXUS_MIN_WINDOW_PREMIUM_OVERRIDES", ""))
    parser.add_argument("--open-call-dominance", type=float, default=DEFAULT_OPEN_CALL_DOMINANCE)
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
    return EmitterConfig(
        database_url=normalize_database_url(args.database_url),
        session_date=session_date,
        symbols=symbols,
        min_window_premium=args.min_window_premium,
        premium_overrides=parse_overrides(args.min_window_premium_overrides),
        open_call_dominance=args.open_call_dominance,
        dry_run=args.dry_run,
        limit=args.limit,
    )


def main(argv: list[str] | None = None) -> int:
    config = parse_args(argv or sys.argv[1:])
    rows = run(config)
    print(json.dumps({
        "sessionDate": config.session_date,
        "symbols": list(config.symbols),
        "dryRun": config.dry_run,
        "emittedCount": 0 if config.dry_run else len(rows),
        "candidateCount": len(rows),
        "rows": rows,
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
