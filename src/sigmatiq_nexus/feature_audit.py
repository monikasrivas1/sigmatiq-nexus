from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from typing import Any

import msgpack
import redis.asyncio as redis

from sigmatiq_nexus import nexus_worker as nw


@dataclass(frozen=True)
class FeatureCheck:
    name: str
    status: str
    source: str | None = None
    reason: str | None = None

    @property
    def ok(self) -> bool:
        return self.status in nw.FRESH_STATUSES

    @property
    def degraded(self) -> bool:
        return False


STRATEGY_REQUIREMENTS: dict[str, tuple[str, ...]] = {
    "etf_open_specialist": (
        "ts_utc",
        "symbol",
        "side",
        "premium",
        "iv_rank",
    ),
    "etf_low_sweep_core": (
        "ts_utc",
        "symbol",
        "raw_symbol",
        "side",
        "premium",
        "is_sweep",
    ),
    "etf_flow_specialist": (
        "ts_utc",
        "symbol",
        "raw_symbol",
        "side",
        "premium",
        "is_sweep",
        "aggressor",
        "delta",
        "gamma",
        "iv_rank",
        "atm_iv",
        "net_gex",
    ),
    "etf_momentum_specialist": (
        "ts_utc",
        "symbol",
        "side",
        "premium",
        "underlying_mid",
        "iv_rank",
    ),
    "etf_confluence_sniper": (
        "ts_utc",
        "raw_symbol",
        "side",
        "premium",
        "underlying_mid",
        "delta",
        "option_mid",
        "iv_rank",
    ),
    "etf_put_credit_open30_spread": (
        "ts_utc",
        "symbol",
        "raw_symbol",
        "side",
        "premium",
        "delta",
        "option_mid",
        "iv_rank",
    ),
    "etf_call_credit_open30_spread": (
        "ts_utc",
        "symbol",
        "raw_symbol",
        "side",
        "premium",
        "delta",
        "option_mid",
        "iv_rank",
    ),
}


def _decode_bytes(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _decode_mapping(data: dict[Any, Any]) -> dict[str, Any]:
    decoded: dict[str, Any] = {}
    for key, value in data.items():
        decoded_key = str(_decode_bytes(key))
        if isinstance(value, bytes) and decoded_key != "data":
            value = value.decode("utf-8")
        decoded[decoded_key] = value
    return decoded


def unwrap_stream_entry(data: dict[Any, Any]) -> dict[str, Any]:
    decoded = _decode_mapping(data)
    if "payload" in decoded:
        raw = decoded["payload"]
        return json.loads(raw)
    if "data" in decoded:
        raw = decoded["data"]
        if isinstance(raw, str):
            raw = raw.encode("latin1")
        unpacked = msgpack.unpackb(raw, raw=False)
        return _decode_mapping(unpacked)
    return decoded


def _has_any(payload: dict[str, Any], *names: str) -> str | None:
    for name in names:
        if name in payload and payload[name] not in (None, ""):
            return name
    return None


def _positive_number(payload: dict[str, Any], *names: str) -> str | None:
    for name in names:
        try:
            if name in payload and float(payload[name]) > 0:
                return name
        except (TypeError, ValueError):
            continue
    return None


def audit_event_features(raw_payload: dict[str, Any]) -> dict[str, FeatureCheck]:
    normalized = nw.normalize_trade_payload(raw_payload)
    runtime_status = normalized.get("_feature_status") or {}
    checks: dict[str, FeatureCheck] = {}

    ts_source = _has_any(raw_payload, "ts_utc", "timestamp")
    if ts_source:
        checks["ts_utc"] = FeatureCheck("ts_utc", "available", ts_source)
    elif _has_any(raw_payload, "ts_event_ns"):
        checks["ts_utc"] = FeatureCheck("ts_utc", "derived", "ts_event_ns")
    else:
        checks["ts_utc"] = FeatureCheck("ts_utc", "missing", reason="no timestamp field")

    symbol_source = _has_any(raw_payload, "symbol", "underlying")
    checks["symbol"] = (
        FeatureCheck("symbol", "available", symbol_source)
        if symbol_source
        else FeatureCheck("symbol", "missing", reason="no symbol or underlying field")
    )

    raw_symbol_source = _has_any(raw_payload, "raw_symbol", "rawSymbol")
    checks["raw_symbol"] = (
        FeatureCheck("raw_symbol", "available", raw_symbol_source)
        if raw_symbol_source
        else FeatureCheck("raw_symbol", "missing", reason="no raw option symbol")
    )

    side_source = _has_any(raw_payload, "side")
    if side_source:
        checks["side"] = FeatureCheck("side", "available", side_source)
    elif normalized.get("side"):
        checks["side"] = FeatureCheck("side", "derived", "raw_symbol")
    else:
        checks["side"] = FeatureCheck("side", "missing", reason="no side and cannot parse raw_symbol")

    premium_source = _positive_number(raw_payload, "premium")
    if premium_source:
        checks["premium"] = FeatureCheck("premium", "available", premium_source)
    elif _positive_number(raw_payload, "price") and _positive_number(raw_payload, "size", "contracts"):
        checks["premium"] = FeatureCheck("premium", "derived", "price*size*100")
    else:
        checks["premium"] = FeatureCheck("premium", "missing", reason="no premium or usable price/size")

    sweep_source = _has_any(raw_payload, "is_sweep", "isSweep")
    if runtime_status.get("is_sweep") == "derived":
        checks["is_sweep"] = FeatureCheck("is_sweep", "derived", "quote_derived")
    elif sweep_source:
        checks["is_sweep"] = FeatureCheck("is_sweep", "available", sweep_source)
    elif runtime_status.get("is_sweep") in nw.FRESH_STATUSES:
        checks["is_sweep"] = FeatureCheck("is_sweep", runtime_status["is_sweep"], "quote_derived")
    else:
        checks["is_sweep"] = FeatureCheck("is_sweep", "missing", reason="missing sweep classifier; default false is unsafe")

    aggressor_source = _has_any(raw_payload, "aggressor", "trade_side", "tradeSide")
    if runtime_status.get("aggressor") == "derived":
        checks["aggressor"] = FeatureCheck("aggressor", "derived", "quote_derived")
    elif aggressor_source:
        checks["aggressor"] = FeatureCheck("aggressor", "available", aggressor_source)
    elif runtime_status.get("aggressor") in nw.FRESH_STATUSES:
        checks["aggressor"] = FeatureCheck("aggressor", runtime_status["aggressor"], "quote_derived")
    else:
        checks["aggressor"] = FeatureCheck("aggressor", "missing", reason="missing aggressor classifier")

    for greek in ("delta", "gamma"):
        source = _has_any(raw_payload, greek)
        checks[greek] = (
            FeatureCheck(greek, runtime_status.get(greek, "available"), source)
            if source
            else FeatureCheck(greek, "missing", reason=f"missing {greek}")
        )

    underlying_source = _has_any(raw_payload, "underlying_mid", "underlyingMid", "underlying_price", "underlyingPrice")
    checks["underlying_mid"] = (
        FeatureCheck("underlying_mid", runtime_status.get("underlying_mid", "available"), underlying_source)
        if underlying_source
        else FeatureCheck("underlying_mid", "missing", reason="missing underlying price on event")
    )

    option_mid_source = _has_any(raw_payload, "option_mid", "optionMid")
    if option_mid_source:
        checks["option_mid"] = FeatureCheck("option_mid", runtime_status.get("option_mid", "available"), option_mid_source)
    elif _positive_number(raw_payload, "price"):
        checks["option_mid"] = FeatureCheck(
            "option_mid",
            "missing",
            "price",
            "trade price is not a quote-mid substitute for pricing-lag logic",
        )
    else:
        checks["option_mid"] = FeatureCheck(
            "option_mid",
            "missing",
            reason="missing option_mid and price",
        )

    return checks


def _context_status(context: dict[str, Any], payload_key: str, feature: str) -> str:
    payload = context.get(payload_key)
    if not isinstance(payload, dict) or not payload:
        payload = context
    if isinstance(payload, dict):
        feature_time = nw._payload_datetime(payload, nw.CONTEXT_TIMESTAMP_FIELDS)
        if not feature_time and nw.NEXUS_REQUIRE_CONTEXT_TIMESTAMPS:
            return "unknown_freshness"
        reference_time = context.get("_reference_time")
        return nw._freshness_status(reference_time, feature_time, nw.FEATURE_MAX_AGE_SECONDS[feature], missing_is_stale=nw.NEXUS_REQUIRE_CONTEXT_TIMESTAMPS)
    if nw.NEXUS_REQUIRE_CONTEXT_TIMESTAMPS:
        return "unknown_freshness"
    return "available"


def audit_context_features(context: dict[str, Any] | None = None) -> dict[str, FeatureCheck]:
    context = context or {}
    checks: dict[str, FeatureCheck] = {}

    if context.get("iv_rank") is not None:
        checks["iv_rank"] = FeatureCheck("iv_rank", _context_status(context, "_iv_rank_payload", "iv_rank"), "iv_rank")
    elif context.get("ivRank") is not None:
        checks["iv_rank"] = FeatureCheck("iv_rank", _context_status(context, "_vrp_payload", "iv_rank"), "vrp")
    else:
        checks["iv_rank"] = FeatureCheck("iv_rank", "missing", reason="missing canonical IV rank")

    if context.get("atm_iv") is not None or context.get("atmIv") is not None:
        checks["atm_iv"] = FeatureCheck("atm_iv", _context_status(context, "_iv_surface_payload", "atm_iv"), "iv_surface")
    else:
        checks["atm_iv"] = FeatureCheck("atm_iv", "missing", reason="missing ATM IV")

    if context.get("net_gex") is not None or context.get("netGex") is not None:
        checks["net_gex"] = FeatureCheck("net_gex", _context_status(context, "_gex_payload", "net_gex"), "gex")
    else:
        checks["net_gex"] = FeatureCheck("net_gex", "missing", reason="missing net GEX")

    return checks


def audit_strategies(
    feature_checks: dict[str, FeatureCheck],
    symbol: str | None = None,
    entry_label: str | None = None,
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for strategy, required in STRATEGY_REQUIREMENTS.items():
        # Check window applicability when symbol/entry_label are provided
        if symbol and entry_label:
            slot = {"entry_label": entry_label}
            if not nw.strategy_window_view_applicable(strategy, symbol, slot):
                output[strategy] = {"status": "not_applicable", "missing": [], "warnings": []}
                continue
        missing = [name for name in required if not feature_checks.get(name, FeatureCheck(name, "missing")).ok]
        warnings = [
            f"{name}: {feature_checks[name].reason}"
            for name in required
            if name in feature_checks and feature_checks[name].degraded and feature_checks[name].reason
        ]
        status = "blocked" if missing else "degraded" if warnings else "ready"
        output[strategy] = {"status": status, "missing": missing, "warnings": warnings}
    return output


def audit_payload(
    raw_payload: dict[str, Any],
    context: dict[str, Any] | None = None,
    entry_label: str | None = None,
) -> dict[str, Any]:
    context = dict(context or {})
    context["_reference_time"] = nw.parse_event_datetime(raw_payload)
    event_checks = audit_event_features(raw_payload)
    context_checks = audit_context_features(context)
    checks = {**event_checks, **context_checks}
    symbol = str(raw_payload.get("symbol") or raw_payload.get("underlying") or "").upper()
    return {
        "symbol": symbol,
        "raw_symbol": raw_payload.get("raw_symbol") or raw_payload.get("rawSymbol"),
        "features": {name: check.__dict__ for name, check in sorted(checks.items())},
        "strategies": audit_strategies(checks, symbol=symbol, entry_label=entry_label),
    }


async def _read_json_key(client: Any, key: str) -> dict[str, Any]:
    raw = await client.get(key)
    if not raw:
        return {}
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        try:
            return {"value": float(raw)}
        except ValueError:
            return {}


async def read_context(client: Any, symbol: str) -> dict[str, Any]:
    iv_surface_key = nw.IV_SURFACE_KEY_TEMPLATE.format(symbol=symbol)
    vrp_key = nw.VRP_KEY_TEMPLATE.format(symbol=symbol)
    gex_key = nw.GEX_KEY_TEMPLATE.format(symbol=symbol)

    iv_surface, vrp, gex = await asyncio.gather(
        _read_json_key(client, iv_surface_key),
        _read_json_key(client, vrp_key),
        _read_json_key(client, gex_key),
    )
    context: dict[str, Any] = {}
    if iv_surface.get("atmIv") is not None:
        context["atmIv"] = iv_surface["atmIv"]
        context["_iv_surface_payload"] = iv_surface
    if vrp.get("ivRank") is not None:
        context["ivRank"] = vrp["ivRank"]
        context["_vrp_payload"] = vrp
    if gex.get("netGex") is not None:
        context["netGex"] = gex["netGex"]
        context["_gex_payload"] = gex
    return context


async def sample_and_audit(client: Any, symbol: str, stream: str | None = None, limit: int = 5) -> dict[str, Any]:
    stream_name = stream or f"md:{symbol}:options:trades"
    entries = await client.xrevrange(stream_name, count=limit)
    context = await read_context(client, symbol)
    audits = []
    for msg_id, data in entries:
        raw_payload = unwrap_stream_entry(data)
        raw_payload = await nw.enrich_trade_payload_from_redis(nw.normalize_trade_payload(raw_payload), client)
        audits.append(
            {
                "message_id": msg_id.decode("utf-8") if isinstance(msg_id, bytes) else str(msg_id),
                "audit": audit_payload(raw_payload, context),
            }
        )
    return {"symbol": symbol, "stream": stream_name, "sample_count": len(audits), "context": context, "samples": audits}


async def _connect_redis(redis_url: str, cluster: bool) -> Any:
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    if cluster:
        return worker._connect_cluster(redis_url)
    return await redis.from_url(worker._redis_url(redis_url), decode_responses=False)


async def async_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit live Nexus feature availability for implemented strategies.")
    parser.add_argument("--symbol", default=next(iter(sorted(nw.SYMBOLS))) if nw.SYMBOLS else "SPY")
    parser.add_argument("--stream", default=None)
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--redis-url", default=nw.REDIS_URL)
    parser.add_argument("--cluster", action="store_true", default=nw.REDIS_CLUSTER)
    parser.add_argument("--payload-json", default=None, help="Audit one local payload instead of reading Redis.")
    parser.add_argument("--context-json", default=None, help="Optional local context JSON for --payload-json.")
    args = parser.parse_args(argv)

    if args.payload_json:
        payload = json.loads(args.payload_json)
        context = json.loads(args.context_json) if args.context_json else {}
        print(json.dumps(audit_payload(payload, context), indent=2, sort_keys=True))
        return 0

    client = await _connect_redis(args.redis_url, args.cluster)
    try:
        result = await sample_and_audit(client, args.symbol.upper(), args.stream, args.limit)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    finally:
        close = getattr(client, "aclose", None)
        if close:
            await close()


def main() -> None:
    raise SystemExit(asyncio.run(async_main()))


if __name__ == "__main__":
    main()
