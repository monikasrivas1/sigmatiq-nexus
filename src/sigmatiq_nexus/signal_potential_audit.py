from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any


DEFAULT_SYMBOLS = ("SPY", "QQQ", "IWM", "UVXY")
DEFAULT_MIN_WINDOW_PREMIUM = 200_000.0
OPEN_CALL_DOMINANCE = 1.35

STRATEGY_RULES: dict[str, dict[str, Any]] = {
    "etf_confluence_sniper": {
        "entries": {"10:00", "10:30", "11:00", "11:30", "12:00"},
        "needs_model": False,
        "needs_pricing": True,
    },
    "etf_open_specialist": {
        "entries": {"10:00"},
        "needs_model": False,
        "unknown_db_inputs": ("iv_rank",),
    },
    "etf_low_sweep_core": {
        "entries": {"10:00", "10:30"},
        "needs_model": False,
        "unknown_db_inputs": ("sweep_ratio",),
    },
    "etf_flow_specialist": {
        "entries": {"10:00", "10:30", "11:00"},
        "needs_model": True,
        "unknown_db_inputs": ("sweep_ratio", "atm_iv", "net_gex", "iv_rank"),
    },
    "etf_momentum_specialist": {
        "entries": {"11:00"},
        "needs_model": True,
        "unknown_db_inputs": ("minute_price_persistence", "iv_rank"),
    },
    "etf_put_credit_open30_spread": {
        "entries": {"10:00"},
        "symbols": {"SPY", "QQQ"},
        "needs_model": False,
        "spread_side": "P",
        "unknown_db_inputs": ("iv_rank", "spread_candidate_quotes"),
    },
    "etf_call_credit_open30_spread": {
        "entries": {"10:00"},
        "symbols": {"SPY"},
        "needs_model": False,
        "spread_side": "C",
        "unknown_db_inputs": ("iv_rank", "spread_candidate_quotes"),
    },
}


WINDOW_TO_ENTRY = {
    "w0930_1000": "10:00",
    "w1000_1030": "10:30",
    "w1030_1100": "11:00",
    "w1100_1130": "11:30",
    "w1130_1200": "12:00",
    "w1200_1230": "12:30",
    "w1230_1300": "13:00",
    "w1300_1330": "13:30",
    "w1330_1400": "14:00",
    "w1400_1430": "14:30",
    "w1430_1500": "15:00",
    "w1500_1530": "15:30",
    "w1530_1600": "16:00",
    "w1600_1615": "16:15",
}


@dataclass(frozen=True)
class AuditConfig:
    database_url: str
    session_date: str
    symbols: tuple[str, ...]
    min_window_premium: float
    premium_overrides: dict[str, float]
    output: str
    csv_path: Path | None


def normalize_database_url(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return raw
    if "://" in raw:
        return raw

    parts: dict[str, str] = {}
    for chunk in raw.split(";"):
        if not chunk or "=" not in chunk:
            continue
        key, value = chunk.split("=", 1)
        parts[key.strip().lower()] = value.strip()

    host = parts.get("host") or parts.get("server")
    database = parts.get("database") or parts.get("initial catalog")
    user = parts.get("username") or parts.get("user id") or parts.get("user")
    password = parts.get("password")
    if not host or not database or not user:
        return raw

    psql_parts = [f"host={host}", f"dbname={database}", f"user={user}"]
    if password:
        psql_parts.append(f"password={password}")
    if parts.get("ssl mode", parts.get("sslmode", "")).lower() in {"require", "true", "verify-ca", "verify-full"}:
        psql_parts.append("sslmode=require")
    return " ".join(psql_parts)


def parse_overrides(raw: str | None) -> dict[str, float]:
    overrides: dict[str, float] = {}
    if not raw:
        return overrides
    for item in raw.split(","):
        if not item.strip():
            continue
        symbol, value = item.split("=", 1)
        overrides[symbol.strip().upper()] = float(value.strip())
    return overrides


def psql_json(database_url: str, sql: str) -> list[dict[str, Any]]:
    cmd = [
        "psql",
        normalize_database_url(database_url),
        "-X",
        "-A",
        "-t",
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(f"psql query failed: {proc.stderr.strip()}")
    text = proc.stdout.strip()
    return json.loads(text) if text else []


def fetch_rows(config: AuditConfig) -> list[dict[str, Any]]:
    symbols_json = json.dumps(list(config.symbols))
    window_labels_json = json.dumps(list(WINDOW_TO_ENTRY))
    sql = f"""
WITH requested_symbols AS (
    SELECT upper(value::text) AS symbol
    FROM jsonb_array_elements_text('{symbols_json}'::jsonb)
),
audited_windows AS (
    SELECT value::text AS window_label
    FROM jsonb_array_elements_text('{window_labels_json}'::jsonb)
),
prepared AS (
    SELECT
        c.symbol,
        c.session_date::text AS session_date,
        c.window_label,
        CASE c.window_label
            WHEN 'w0930_1000' THEN '10:00'
            WHEN 'w1000_1030' THEN '10:30'
            WHEN 'w1030_1100' THEN '11:00'
            WHEN 'w1100_1130' THEN '11:30'
            WHEN 'w1130_1200' THEN '12:00'
            WHEN 'w1200_1230' THEN '12:30'
            WHEN 'w1230_1300' THEN '13:00'
            WHEN 'w1300_1330' THEN '13:30'
            WHEN 'w1330_1400' THEN '14:00'
            WHEN 'w1400_1430' THEN '14:30'
            WHEN 'w1430_1500' THEN '15:00'
            WHEN 'w1500_1530' THEN '15:30'
            WHEN 'w1530_1600' THEN '16:00'
            WHEN 'w1600_1615' THEN '16:15'
            ELSE c.window_label
        END AS entry_time,
        c.window_start_utc,
        c.window_end_utc,
        c.total_premium::float8 AS total_premium,
        c.call_premium::float8 AS call_premium,
        c.put_premium::float8 AS put_premium,
        c.option_trade_count,
        c.lead_contract_raw_symbol,
        c.lead_contract_premium::float8 AS lead_contract_premium,
        c.equity_total_volume,
        c.unusual_trade_count,
        c.unusual_total_premium::float8 AS unusual_total_premium,
        c.pricing_quality,
        c.daily_context_status,
        c.readiness_status,
        c.missing_inputs_json,
        c.iv_rank_pct::float8 AS iv_rank_pct,
        c.atm_iv::float8 AS atm_iv,
        c.net_gex::float8 AS net_gex,
        c.sweep_ratio::float8 AS sweep_ratio,
        c.sweep_quality_status,
        c.aggressor_quality_status,
        c.final_quote_status,
        c.replay_gate_status,
        c.replay_missing_inputs_json,
        c.materialized_at_utc,
        COALESCE(c.lead_quote_ts_utc, cs.quote_ts_utc) AS lead_quote_ts_utc,
        COALESCE(c.lead_quote_mid, cs.mid)::float8 AS lead_mid,
        COALESCE(c.lead_delta, cs.delta)::float8 AS lead_delta,
        COALESCE(c.lead_gamma, cs.gamma)::float8 AS lead_gamma,
        cs.quote_status AS lead_quote_status,
        cs.enrichment_status AS lead_enrichment_status,
        c.lead_quote_age_seconds::float8 AS quote_age_at_window_end_sec,
        EXTRACT(EPOCH FROM (c.materialized_at_utc - cs.quote_ts_utc))::float8 AS quote_age_at_materialization_sec
    FROM live.prepared_nexus_window_context c
    JOIN requested_symbols s ON s.symbol = c.symbol
    JOIN audited_windows w ON w.window_label = c.window_label
    LEFT JOIN live.prepared_contract_state cs
        ON cs.symbol = c.symbol
       AND cs.session_date = c.session_date
       AND cs.raw_symbol = c.lead_contract_raw_symbol
    WHERE c.session_date = '{config.session_date}'::date
),
actual_window AS (
    SELECT
        strategy_key,
        symbol,
        entry_time,
        count(*) AS window_view_count,
        max(persisted_at_utc) AS latest_window_view_at,
        (array_agg(decision ORDER BY persisted_at_utc DESC))[1] AS latest_window_decision,
        (array_agg(reason ORDER BY persisted_at_utc DESC))[1] AS latest_window_reason,
        (array_agg(sentiment ORDER BY persisted_at_utc DESC))[1] AS latest_window_sentiment
    FROM live.nexus_window_view
    WHERE session_date = '{config.session_date}'::date
    GROUP BY strategy_key, symbol, entry_time
),
actual_audit AS (
    SELECT
        strategy_key,
        symbol,
        entry_time,
        count(*) AS audit_count,
        max(persisted_at_utc) AS latest_audit_at,
        (array_agg(decision ORDER BY persisted_at_utc DESC))[1] AS latest_audit_decision,
        (array_agg(reason ORDER BY persisted_at_utc DESC))[1] AS latest_audit_reason
    FROM live.nexus_strategy_audit
    WHERE session_date = '{config.session_date}'::date
    GROUP BY strategy_key, symbol, entry_time
),
actual_signal AS (
    SELECT
        strategy_key,
        symbol,
        entry_time,
        count(*) FILTER (WHERE decision IN ('BET','FINAL','SIGNAL')) AS bet_count,
        count(*) FILTER (WHERE decision = 'INTERMEDIATE') AS intermediate_count,
        count(*) FILTER (WHERE decision IN ('FINAL_BLOCKED','BLOCKED')) AS final_block_count,
        max(persisted_at_utc) AS latest_signal_at,
        (array_agg(decision ORDER BY persisted_at_utc DESC))[1] AS latest_signal_decision
    FROM live.nexus_strategy_signal
    WHERE session_date = '{config.session_date}'::date
    GROUP BY strategy_key, symbol, entry_time
),
actual_spread AS (
    SELECT
        strategy_key,
        symbol,
        entry_time,
        count(*) AS spread_signal_count,
        max(persisted_at_utc) AS latest_spread_at
    FROM live.nexus_spread_signal
    WHERE session_date = '{config.session_date}'::date
    GROUP BY strategy_key, symbol, entry_time
),
strategies(strategy_key) AS (
    VALUES
        ('etf_confluence_sniper'),
        ('etf_open_specialist'),
        ('etf_low_sweep_core'),
        ('etf_flow_specialist'),
        ('etf_momentum_specialist'),
        ('etf_put_credit_open30_spread'),
        ('etf_call_credit_open30_spread')
)
SELECT COALESCE(jsonb_agg(to_jsonb(row_data) ORDER BY symbol, window_label, strategy_key), '[]'::jsonb)
FROM (
    SELECT
        p.*,
        st.strategy_key,
        aw.window_view_count,
        aw.latest_window_view_at,
        aw.latest_window_decision,
        aw.latest_window_reason,
        aw.latest_window_sentiment,
        aa.audit_count,
        aa.latest_audit_at,
        aa.latest_audit_decision,
        aa.latest_audit_reason,
        sig.bet_count,
        sig.intermediate_count,
        sig.final_block_count,
        sig.latest_signal_at,
        sig.latest_signal_decision,
        sp.spread_signal_count,
        sp.latest_spread_at
    FROM prepared p
    CROSS JOIN strategies st
    LEFT JOIN actual_window aw
        ON aw.symbol = p.symbol
       AND aw.entry_time = p.entry_time
       AND aw.strategy_key = st.strategy_key
    LEFT JOIN actual_audit aa
        ON aa.symbol = p.symbol
       AND aa.entry_time = p.entry_time
       AND aa.strategy_key = st.strategy_key
    LEFT JOIN actual_signal sig
        ON sig.symbol = p.symbol
       AND sig.entry_time = p.entry_time
       AND sig.strategy_key = st.strategy_key
    LEFT JOIN actual_spread sp
        ON sp.symbol = p.symbol
       AND sp.entry_time = p.entry_time
       AND sp.strategy_key = st.strategy_key
) row_data;
"""
    return psql_json(config.database_url, sql)


def threshold_for(row: dict[str, Any], config: AuditConfig) -> float:
    return config.premium_overrides.get(str(row["symbol"]).upper(), config.min_window_premium)


def dominant_side(row: dict[str, Any]) -> str | None:
    call_premium = float(row.get("call_premium") or 0)
    put_premium = float(row.get("put_premium") or 0)
    if call_premium > put_premium:
        return "C"
    if put_premium > call_premium:
        return "P"
    return None


def add_missing_warnings(row: dict[str, Any], warnings: list[str], fields: tuple[str, ...]) -> None:
    for field in fields:
        if field == "sweep_ratio":
            if row.get("sweep_quality_status") != "ready" or row.get("sweep_ratio") is None:
                warnings.append("sweep_ratio_not_materialized")
        elif field == "spread_candidate_quotes":
            warnings.append(field)
        elif field == "iv_rank":
            if row.get("iv_rank") is None and row.get("iv_rank_pct") is None:
                warnings.append(field)
        elif row.get(field) is None:
            warnings.append(field)


def classify(row: dict[str, Any], config: AuditConfig) -> dict[str, Any]:
    strategy = row["strategy_key"]
    symbol = str(row["symbol"]).upper()
    entry_time = row.get("entry_time")
    rule = STRATEGY_RULES[strategy]
    min_premium = threshold_for(row, config)
    reasons: list[str] = []
    warnings: list[str] = []

    applicable = True
    if entry_time not in rule["entries"]:
        applicable = False
        reasons.append("strategy_not_scheduled_for_window")
    allowed_symbols = rule.get("symbols")
    if allowed_symbols and symbol not in allowed_symbols:
        applicable = False
        reasons.append("strategy_not_applicable_to_symbol")

    raw_ready = int(row.get("option_trade_count") or 0) > 0 and int(row.get("equity_total_volume") or 0) > 0
    materialized_ready = row.get("readiness_status") == "ready"
    premium_ready = float(row.get("total_premium") or 0) >= min_premium
    daily_context_ready = row.get("daily_context_status") in {"ready", "healthy"}
    quote_ready = (
        row.get("lead_contract_raw_symbol")
        and row.get("lead_quote_ts_utc")
        and row.get("lead_mid") is not None
        and (
            row.get("final_quote_status") == "fresh_quote_asof_window"
            or (
                row.get("quote_age_at_window_end_sec") is not None
                and float(row["quote_age_at_window_end_sec"]) <= 30
            )
        )
    )

    if applicable and not raw_ready:
        reasons.append("raw_data_insufficient")
    if applicable and not materialized_ready:
        reasons.append("materialized_context_not_ready")
    if applicable and not premium_ready:
        reasons.append("premium_below_threshold")
    if applicable and not daily_context_ready:
        reasons.append("daily_context_not_ready")
    if applicable and not quote_ready:
        reasons.append("lead_contract_quote_not_fresh_or_missing")

    side = dominant_side(row)
    call_premium = float(row.get("call_premium") or 0)
    put_premium = float(row.get("put_premium") or 0)
    heuristic = "unknown"
    possible_sentiment: str | None = None

    if applicable and raw_ready and materialized_ready and premium_ready:
        if strategy == "etf_open_specialist":
            if call_premium >= min_premium and call_premium > put_premium * OPEN_CALL_DOMINANCE:
                heuristic = "passes_obvious_call_dominance"
                possible_sentiment = "BULLISH"
                add_missing_warnings(row, warnings, rule.get("unknown_db_inputs", ()))
            else:
                heuristic = "fails_open_call_dominance"
                reasons.append("open_specialist_call_dominance_not_met")
        elif strategy == "etf_low_sweep_core":
            if side == "C" and entry_time in {"10:00", "10:30"}:
                heuristic = "possible_call_dominance_sweep_unknown"
                possible_sentiment = "BULLISH"
                add_missing_warnings(row, warnings, ("sweep_ratio",))
            elif side == "P" and entry_time == "10:30":
                heuristic = "possible_put_dominance_sweep_unknown"
                possible_sentiment = "BEARISH"
                add_missing_warnings(row, warnings, ("sweep_ratio",))
            else:
                heuristic = "fails_low_sweep_side_window"
                reasons.append("low_sweep_side_window_not_met")
        elif strategy == "etf_flow_specialist":
            if side:
                heuristic = "possible_flow_alignment_context_values_unknown"
                possible_sentiment = "BULLISH" if side == "C" else "BEARISH"
                add_missing_warnings(row, warnings, rule.get("unknown_db_inputs", ()))
            else:
                heuristic = "fails_flow_no_dominant_side"
                reasons.append("flow_no_dominant_side")
        elif strategy == "etf_momentum_specialist":
            heuristic = "requires_minute_price_persistence_and_model"
            add_missing_warnings(row, warnings, rule.get("unknown_db_inputs", ()))
        elif strategy == "etf_confluence_sniper":
            if row.get("pricing_quality") == "ready":
                heuristic = "possible_pricing_context_ready"
                warnings.append("momentum_heuristic_requires_trade_sequence")
            else:
                heuristic = "pricing_not_ready"
                reasons.append("pricing_lag_not_ready")
        elif strategy == "etf_put_credit_open30_spread":
            if call_premium > put_premium * OPEN_CALL_DOMINANCE:
                heuristic = "passes_open_call_dominance_put_credit_context"
                possible_sentiment = "BULLISH"
                add_missing_warnings(row, warnings, rule.get("unknown_db_inputs", ()))
            else:
                heuristic = "fails_put_credit_open30_call_dominance"
                reasons.append("put_credit_open30_call_dominance_not_met")
        elif strategy == "etf_call_credit_open30_spread":
            if put_premium > call_premium * OPEN_CALL_DOMINANCE:
                heuristic = "passes_open_put_dominance_call_credit_context"
                possible_sentiment = "BEARISH"
                add_missing_warnings(row, warnings, rule.get("unknown_db_inputs", ()))
            else:
                heuristic = "fails_call_credit_open30_put_dominance"
                reasons.append("call_credit_open30_put_dominance_not_met")

    actual = "none"
    if int(row.get("bet_count") or 0) > 0 or int(row.get("spread_signal_count") or 0) > 0:
        actual = "bet_or_spread_signal"
    elif int(row.get("final_block_count") or 0) > 0:
        actual = "final_blocked"
    elif int(row.get("intermediate_count") or 0) > 0:
        actual = "intermediate_only"
    elif int(row.get("window_view_count") or 0) > 0:
        actual = "window_view"
    elif int(row.get("audit_count") or 0) > 0:
        actual = "audit_only"

    if not applicable:
        potential = "not_applicable"
    elif reasons:
        potential = "blocked_by_data_or_obvious_gate"
    elif rule.get("needs_model"):
        potential = "candidate_requires_model_probability"
    elif warnings:
        potential = "candidate_with_unmaterialized_gate_inputs"
    else:
        potential = "candidate_should_have_reached_final_quote_gate"

    if potential.startswith("candidate") and actual == "none":
        verdict = "needs_review_no_nexus_output"
    elif potential.startswith("candidate") and actual in {"window_view", "audit_only"}:
        verdict = "nexus_view_only_no_signal_attempt"
    elif potential == "blocked_by_data_or_obvious_gate" and actual in {"none", "window_view", "audit_only"}:
        verdict = "correct_no_bet_or_view_only"
    elif potential == "not_applicable" and actual in {"none", "window_view", "audit_only"}:
        verdict = "correct_not_applicable_or_explainable"
    elif actual in {"bet_or_spread_signal", "final_blocked", "intermediate_only"}:
        verdict = "nexus_attempted_signal_path"
    else:
        verdict = "review"

    return {
        **row,
        "min_window_premium_required": min_premium,
        "dominant_side": side,
        "raw_ready": raw_ready,
        "materialized_ready": materialized_ready,
        "premium_ready": premium_ready,
        "daily_context_ready": daily_context_ready,
        "lead_quote_ready": bool(quote_ready),
        "heuristic_summary": heuristic,
        "possible_sentiment": possible_sentiment,
        "potential": potential,
        "actual": actual,
        "verdict": verdict,
        "block_reasons": reasons,
        "warnings": sorted(set(warnings)),
    }


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_verdict: dict[str, int] = {}
    by_potential: dict[str, int] = {}
    for row in rows:
        by_verdict[row["verdict"]] = by_verdict.get(row["verdict"], 0) + 1
        by_potential[row["potential"]] = by_potential.get(row["potential"], 0) + 1
    needs_review = [row for row in rows if row["verdict"] == "needs_review_no_nexus_output"]
    attempted = [row for row in rows if row["actual"] in {"bet_or_spread_signal", "final_blocked", "intermediate_only"}]
    return {
        "row_count": len(rows),
        "by_verdict": by_verdict,
        "by_potential": by_potential,
        "needs_review_count": len(needs_review),
        "attempted_signal_path_count": len(attempted),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "session_date",
        "symbol",
        "window_label",
        "entry_time",
        "strategy_key",
        "potential",
        "actual",
        "verdict",
        "block_reasons",
        "warnings",
        "total_premium",
        "call_premium",
        "put_premium",
        "option_trade_count",
        "readiness_status",
        "daily_context_status",
        "pricing_quality",
        "lead_contract_raw_symbol",
        "lead_quote_ready",
        "quote_age_at_materialization_sec",
        "quote_age_at_window_end_sec",
        "final_quote_status",
        "replay_gate_status",
        "iv_rank_pct",
        "atm_iv",
        "net_gex",
        "sweep_quality_status",
        "aggressor_quality_status",
        "latest_window_decision",
        "latest_window_reason",
        "latest_audit_decision",
        "latest_audit_reason",
        "latest_signal_decision",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            flat = {field: row.get(field) for field in fields}
            flat["block_reasons"] = ";".join(row.get("block_reasons") or [])
            flat["warnings"] = ";".join(row.get("warnings") or [])
            writer.writerow(flat)


def parse_args(argv: list[str]) -> AuditConfig:
    parser = argparse.ArgumentParser(description="Audit whether raw and materialized live DB data could have supported Nexus signals.")
    parser.add_argument("--database-url", default=os.environ.get("SHARED_DATABASE_URL") or os.environ.get("DATABASE_URL"))
    parser.add_argument("--session-date", default=date.today().isoformat())
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--min-window-premium", type=float, default=DEFAULT_MIN_WINDOW_PREMIUM)
    parser.add_argument("--min-window-premium-overrides", default=os.environ.get("NEXUS_MIN_WINDOW_PREMIUM_OVERRIDES", ""))
    parser.add_argument("--output", choices=("json", "summary"), default="summary")
    parser.add_argument("--csv", type=Path, default=None)
    args = parser.parse_args(argv)
    if not args.database_url:
        parser.error("--database-url or SHARED_DATABASE_URL/DATABASE_URL is required")
    symbols = tuple(sym.strip().upper() for sym in args.symbols.split(",") if sym.strip())
    return AuditConfig(
        database_url=args.database_url,
        session_date=args.session_date,
        symbols=symbols,
        min_window_premium=args.min_window_premium,
        premium_overrides=parse_overrides(args.min_window_premium_overrides),
        output=args.output,
        csv_path=args.csv,
    )


def main(argv: list[str] | None = None) -> int:
    config = parse_args(argv or sys.argv[1:])
    raw_rows = fetch_rows(config)
    rows = [classify(row, config) for row in raw_rows]
    summary = summarize(rows)
    result = {
        "sessionDate": config.session_date,
        "symbols": list(config.symbols),
        "summary": summary,
        "rows": rows,
    }
    if config.csv_path:
        write_csv(config.csv_path, rows)
        result["csvPath"] = str(config.csv_path)
    if config.output == "json":
        print(json.dumps(result, indent=2, default=str))
    else:
        print(json.dumps({k: v for k, v in result.items() if k != "rows"}, indent=2, default=str))
        review_rows = [row for row in rows if row["verdict"] == "needs_review_no_nexus_output"]
        if review_rows:
            print("\nNeeds review:")
            for row in review_rows[:30]:
                print(
                    f"- {row['symbol']} {row['window_label']} {row['strategy_key']}: "
                    f"{row['potential']} actual={row['actual']} warnings={','.join(row['warnings']) or '-'}"
                )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
