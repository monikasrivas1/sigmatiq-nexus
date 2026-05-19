"""Nexus Input Readiness Audit

Checks every Redis key Nexus depends on for existence, field correctness,
and freshness. Produces a clear pass/fail report per symbol.

Usage:
    python -m sigmatiq_nexus.input_readiness_audit
    python -m sigmatiq_nexus.input_readiness_audit --symbol SPY
    python -m sigmatiq_nexus.input_readiness_audit --redis-url redis://your-host:6379/0
    python -m sigmatiq_nexus.input_readiness_audit --json  # machine-readable output
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as redis_lib

from sigmatiq_nexus import nexus_worker as nw


# ── key definitions ──────────────────────────────────────────────────────────

CONTEXT_KEYS: list[dict[str, Any]] = [
    {
        "name": "iv_surface",
        "template": nw.IV_SURFACE_KEY_TEMPLATE,
        "required_fields": ["atmIv"],
        "optional_fields": ["putSkew25d", "callSkew25d", "termSlope", "termState", "surfaceQuality"],
        "max_age_seconds": nw.VOL_CONTEXT_MAX_AGE_SECONDS,
        "writer": "sigmatiq-options-stream (Options LiveWorker)",
        "refresh_cadence": "~60s",
    },
    {
        "name": "gex",
        "template": nw.GEX_KEY_TEMPLATE,
        "required_fields": ["netGex"],
        "optional_fields": ["gammaFlip", "callWall", "putWall", "gammaRegime", "grossGex"],
        "max_age_seconds": nw.GEX_CONTEXT_MAX_AGE_SECONDS,
        "writer": "sigmatiq-options-stream (Options LiveWorker)",
        "refresh_cadence": "~30s",
    },
    {
        "name": "vrp",
        "template": nw.VRP_KEY_TEMPLATE,
        "required_fields": ["ivRank"],
        "optional_fields": ["atmIv", "rv30d", "vrp30d", "percentile", "zScore", "vrpRegime"],
        "max_age_seconds": nw.VOL_CONTEXT_MAX_AGE_SECONDS,
        "writer": "sigmatiq-options-stream (Options LiveWorker)",
        "refresh_cadence": "~60s (intraday), 1x/day (EOD percentile)",
    },
    {
        "name": "equity_context",
        "template": nw.EQUITY_CONTEXT_KEY_TEMPLATE,
        "required_fields": ["price"],
        "optional_fields": ["lastTradePrice", "vwap", "warmupComplete", "priceDataStale", "distanceToVwapPct"],
        "max_age_seconds": nw.UNDERLYING_MAX_AGE_SECONDS,
        "writer": "sigmatiq-equity-stream (Equity LiveWorker)",
        "refresh_cadence": "per bar/trade (~1s-60s)",
    },
]

TRADE_STREAM_TEMPLATE = "md:{symbol}:options:trades"


# ── helpers ──────────────────────────────────────────────────────────────────

async def _read_json_key(client: Any, key: str) -> dict[str, Any] | None:
    raw = await client.get(key)
    if not raw:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None


def _extract_timestamp(payload: dict[str, Any]) -> datetime | None:
    return nw._payload_datetime(payload, nw.CONTEXT_TIMESTAMP_FIELDS)


def _age_seconds(ts: datetime | None) -> float | None:
    if ts is None:
        return None
    now = datetime.now(timezone.utc)
    return (now - ts).total_seconds()


def _check_field(payload: dict[str, Any], field: str) -> dict[str, Any]:
    value = payload.get(field)
    if value is None:
        # try snake_case / camelCase alternates
        alternates = {
            "atmIv": ["atm_iv"],
            "netGex": ["net_gex"],
            "ivRank": ["iv_rank"],
            "lastTradePrice": ["last_trade_price", "close"],
            "price": ["lastTradePrice", "last_trade_price", "close"],
        }
        for alt in alternates.get(field, []):
            value = payload.get(alt)
            if value is not None:
                return {"present": True, "value": value, "source_field": alt}
        return {"present": False, "value": None}
    return {"present": True, "value": value, "source_field": field}


# ── audit functions ──────────────────────────────────────────────────────────

async def audit_context_key(
    client: Any, symbol: str, key_def: dict[str, Any],
) -> dict[str, Any]:
    key = key_def["template"].format(symbol=symbol)
    payload = await _read_json_key(client, key)

    result: dict[str, Any] = {
        "key": key,
        "name": key_def["name"],
        "writer": key_def["writer"],
        "refresh_cadence": key_def["refresh_cadence"],
        "max_age_seconds": key_def["max_age_seconds"],
    }

    # 1. Key exists?
    if payload is None:
        result["exists"] = False
        result["verdict"] = "FAIL"
        result["fail_reason"] = "key_missing_or_empty"
        return result
    result["exists"] = True

    # 2. Timestamp field present?
    ts = _extract_timestamp(payload)
    found_ts_field = None
    for field_name in nw.CONTEXT_TIMESTAMP_FIELDS:
        if field_name in payload and payload[field_name]:
            found_ts_field = field_name
            break
    result["timestamp_field"] = found_ts_field
    result["timestamp_value"] = ts.isoformat() if ts else None

    if found_ts_field is None:
        result["verdict"] = "FAIL" if nw.NEXUS_REQUIRE_CONTEXT_TIMESTAMPS else "WARN"
        result["fail_reason"] = (
            "no_recognized_timestamp_field"
            f" (checked: {', '.join(nw.CONTEXT_TIMESTAMP_FIELDS)})"
        )
        if nw.NEXUS_REQUIRE_CONTEXT_TIMESTAMPS:
            result["nexus_status"] = "unknown_freshness → BLOCKED"
        else:
            result["nexus_status"] = "available (timestamps not required)"

    # 3. Freshness?
    age = _age_seconds(ts)
    result["age_seconds"] = round(age, 1) if age is not None else None
    if age is not None:
        result["fresh"] = age <= key_def["max_age_seconds"]
        if not result["fresh"]:
            result["verdict"] = "FAIL"
            result["fail_reason"] = (
                f"stale: {round(age, 1)}s old > {key_def['max_age_seconds']}s threshold"
            )
            result["nexus_status"] = "stale → BLOCKED"
    elif found_ts_field is not None:
        # Field exists but couldn't parse
        result["fresh"] = None
        result["verdict"] = "WARN"
        result["fail_reason"] = f"timestamp field '{found_ts_field}' present but not parseable"

    # 4. Required data fields?
    field_checks: dict[str, Any] = {}
    missing_required: list[str] = []
    for field in key_def["required_fields"]:
        check = _check_field(payload, field)
        field_checks[field] = check
        if not check["present"]:
            missing_required.append(field)

    for field in key_def.get("optional_fields", []):
        field_checks[field] = _check_field(payload, field)

    result["fields"] = field_checks
    result["missing_required"] = missing_required

    if missing_required:
        result["verdict"] = "FAIL"
        result["fail_reason"] = f"missing required fields: {', '.join(missing_required)}"
        result["nexus_status"] = "missing data → default/blocked"

    # 5. Set verdict if not already set
    if "verdict" not in result:
        result["verdict"] = "PASS"
        result["nexus_status"] = "available"

    return result


async def audit_trade_stream(client: Any, symbol: str) -> dict[str, Any]:
    stream_key = TRADE_STREAM_TEMPLATE.format(symbol=symbol)
    result: dict[str, Any] = {
        "key": stream_key,
        "name": "options_trade_stream",
        "writer": "pilot-ingestion-python (Databento OPRA)",
    }

    # Check stream exists and has entries
    try:
        stream_len = await client.xlen(stream_key)
    except Exception:
        stream_len = 0

    result["exists"] = stream_len > 0
    result["stream_length"] = stream_len

    if stream_len == 0:
        result["verdict"] = "FAIL"
        result["fail_reason"] = "stream_empty_or_missing"
        result["latest_entry_age_seconds"] = None
        return result

    # Check latest entry age
    try:
        entries = await client.xrevrange(stream_key, count=1)
        if entries:
            msg_id, data = entries[0]
            if isinstance(msg_id, bytes):
                msg_id = msg_id.decode("utf-8")
            # Redis stream IDs are millisecond timestamps
            ts_ms = int(msg_id.split("-")[0])
            ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            age = _age_seconds(ts)
            result["latest_entry_id"] = msg_id
            result["latest_entry_utc"] = ts.isoformat()
            result["latest_entry_age_seconds"] = round(age, 1) if age else None

            # Check if stream is actively receiving data (< 5 min old)
            if age and age > 300:
                result["verdict"] = "WARN"
                result["fail_reason"] = f"latest entry is {round(age, 1)}s old (>5 min)"
            else:
                result["verdict"] = "PASS"
        else:
            result["verdict"] = "FAIL"
            result["fail_reason"] = "stream_exists_but_no_entries_readable"
    except Exception as e:
        result["verdict"] = "FAIL"
        result["fail_reason"] = f"error reading stream: {e}"

    # Check Nexus stream offset (has Nexus consumed from this stream?)
    offset_key = nw.STREAM_OFFSET_KEY_TEMPLATE.format(stream=stream_key)
    try:
        offset_raw = await client.get(offset_key)
        if offset_raw:
            result["nexus_offset_key"] = offset_key
            result["nexus_last_offset"] = offset_raw.decode("utf-8") if isinstance(offset_raw, bytes) else str(offset_raw)
        else:
            result["nexus_offset_key"] = offset_key
            result["nexus_last_offset"] = None
            if result["verdict"] == "PASS":
                result["verdict"] = "WARN"
                result["fail_reason"] = "Nexus has never consumed from this stream (no offset)"
    except Exception:
        result["nexus_last_offset"] = None

    return result


async def audit_symbol(client: Any, symbol: str) -> dict[str, Any]:
    # Run all checks in parallel
    context_tasks = [audit_context_key(client, symbol, kd) for kd in CONTEXT_KEYS]
    stream_task = audit_trade_stream(client, symbol)

    results = await asyncio.gather(*context_tasks, stream_task)

    context_results = {r["name"]: r for r in results[:-1]}
    stream_result = results[-1]

    # Compute symbol-level verdict
    all_verdicts = [r["verdict"] for r in results]
    if any(v == "FAIL" for v in all_verdicts):
        symbol_verdict = "BLOCKED"
    elif any(v == "WARN" for v in all_verdicts):
        symbol_verdict = "DEGRADED"
    else:
        symbol_verdict = "READY"

    # Which strategies are blocked by these failures?
    blocked_features: set[str] = set()
    for r in results[:-1]:
        if r["verdict"] == "FAIL":
            if r["name"] == "iv_surface":
                blocked_features.add("atm_iv")
            elif r["name"] == "gex":
                blocked_features.add("net_gex")
            elif r["name"] == "vrp":
                blocked_features.add("iv_rank")
            elif r["name"] == "equity_context":
                blocked_features.add("underlying_mid")

    if stream_result["verdict"] == "FAIL":
        blocked_features.add("trades")

    # Map to strategies
    strategy_impact: dict[str, str] = {}
    for strategy, required in nw.STRATEGY_REQUIRED_FEATURES.items():
        missing = blocked_features & set(required)
        if "trades" in blocked_features:
            strategy_impact[strategy] = "BLOCKED (no trade data)"
        elif missing:
            strategy_impact[strategy] = f"BLOCKED (missing: {', '.join(sorted(missing))})"
        else:
            strategy_impact[strategy] = "UNBLOCKED by input readiness"

    return {
        "symbol": symbol,
        "verdict": symbol_verdict,
        "context_keys": context_results,
        "trade_stream": stream_result,
        "blocked_features": sorted(blocked_features),
        "strategy_impact": strategy_impact,
    }


# ── pretty printer ───────────────────────────────────────────────────────────

def _verdict_marker(verdict: str) -> str:
    return {"PASS": "[OK]", "FAIL": "[FAIL]", "WARN": "[WARN]"}.get(verdict, f"[{verdict}]")


def print_report(results: list[dict[str, Any]]) -> None:
    print()
    print("=" * 80)
    print("  NEXUS INPUT READINESS AUDIT")
    print(f"  Time: {datetime.now(timezone.utc).isoformat()}")
    print(f"  Symbols: {', '.join(r['symbol'] for r in results)}")
    print(f"  NEXUS_REQUIRE_CONTEXT_TIMESTAMPS: {nw.NEXUS_REQUIRE_CONTEXT_TIMESTAMPS}")
    print("=" * 80)

    for sym_result in results:
        symbol = sym_result["symbol"]
        verdict = sym_result["verdict"]
        print()
        print(f"  {symbol}  {'=' * 60}")
        print(f"  Overall: {verdict}")
        print()

        # Context keys
        for name, r in sym_result["context_keys"].items():
            marker = _verdict_marker(r["verdict"])
            print(f"    {marker} {r['key']}")
            if not r.get("exists"):
                print(f"           Key does not exist in Redis")
                print(f"           Writer: {r['writer']} (refresh: {r['refresh_cadence']})")
            else:
                ts_field = r.get("timestamp_field")
                age = r.get("age_seconds")
                max_age = r["max_age_seconds"]
                if ts_field:
                    fresh_label = "FRESH" if r.get("fresh") else "STALE"
                    print(f"           Timestamp: {ts_field} = {r.get('timestamp_value', '?')}")
                    print(f"           Age: {age}s (threshold: {max_age}s) → {fresh_label}")
                else:
                    print(f"           Timestamp: MISSING (no recognized field)")
                    print(f"           Checked: {', '.join(nw.CONTEXT_TIMESTAMP_FIELDS)}")

                # Required fields
                for field, check in r.get("fields", {}).items():
                    if field in (r.get("missing_required") or []):
                        print(f"           Field {field}: MISSING (required)")
                    elif check["present"]:
                        val = check["value"]
                        if isinstance(val, float):
                            val = round(val, 6)
                        src = check.get("source_field", field)
                        tag = " (required)" if field in (r.get("required_fields") or []) else ""
                        print(f"           Field {src}: {val}{tag}")

            if r.get("fail_reason"):
                print(f"           Reason: {r['fail_reason']}")
            print()

        # Trade stream
        sr = sym_result["trade_stream"]
        marker = _verdict_marker(sr["verdict"])
        print(f"    {marker} {sr['key']}")
        if sr.get("exists"):
            print(f"           Stream length: {sr.get('stream_length', '?')}")
            print(f"           Latest entry: {sr.get('latest_entry_utc', '?')} ({sr.get('latest_entry_age_seconds', '?')}s ago)")
            if sr.get("nexus_last_offset"):
                print(f"           Nexus offset: {sr['nexus_last_offset']}")
            else:
                print(f"           Nexus offset: NOT SET (Nexus may not have consumed)")
        else:
            print(f"           Stream is empty or does not exist")
            print(f"           Writer: {sr['writer']}")
        if sr.get("fail_reason"):
            print(f"           Reason: {sr['fail_reason']}")
        print()

        # Strategy impact
        print(f"    Strategy Impact:")
        for strategy, impact in sym_result["strategy_impact"].items():
            blocked = "BLOCKED" in impact
            marker = "[FAIL]" if blocked else "[ OK ]"
            print(f"      {marker} {strategy}: {impact}")
        print()

    # Summary
    print("=" * 80)
    print("  SUMMARY")
    print("=" * 80)
    for sym_result in results:
        symbol = sym_result["symbol"]
        verdict = sym_result["verdict"]
        blocked = sym_result.get("blocked_features", [])
        total_strategies = len(sym_result["strategy_impact"])
        blocked_strategies = sum(1 for v in sym_result["strategy_impact"].values() if "BLOCKED" in v)
        print(f"  {symbol}: {verdict} — {blocked_strategies}/{total_strategies} strategies blocked")
        if blocked:
            print(f"         Blocked by: {', '.join(blocked)}")
    print()


# ── main ─────────────────────────────────────────────────────────────────────

async def async_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Audit all Redis keys Nexus depends on for input readiness.",
    )
    parser.add_argument(
        "--symbol",
        default=None,
        help="Audit a single symbol (default: all NEXUS_SYMBOLS)",
    )
    parser.add_argument(
        "--redis-url",
        default=os.environ.get("REDIS_URL", nw.REDIS_URL if hasattr(nw, "REDIS_URL") else "redis://localhost:6379/0"),
    )
    parser.add_argument(
        "--cluster",
        action="store_true",
        default=nw.REDIS_CLUSTER if hasattr(nw, "REDIS_CLUSTER") else False,
    )
    parser.add_argument("--json", action="store_true", help="Output JSON instead of human-readable report")
    args = parser.parse_args(argv)

    symbols = [args.symbol.upper()] if args.symbol else sorted(nw.SYMBOLS)

    if args.cluster:
        from redis.asyncio.cluster import RedisCluster
        client = RedisCluster.from_url(args.redis_url, decode_responses=False)
    else:
        client = await redis_lib.from_url(args.redis_url, decode_responses=False)

    try:
        results = await asyncio.gather(*[audit_symbol(client, s) for s in symbols])
        results = list(results)

        if args.json:
            print(json.dumps(results, indent=2, default=str))
        else:
            print_report(results)

        # Exit code: 1 if any symbol is BLOCKED
        if any(r["verdict"] == "BLOCKED" for r in results):
            return 1
        return 0
    finally:
        close = getattr(client, "aclose", None) or getattr(client, "close", None)
        if close:
            await close()


def main() -> None:
    raise SystemExit(asyncio.run(async_main()))


if __name__ == "__main__":
    main()
