from __future__ import annotations

import asyncio
import json
from collections import deque
from datetime import date, datetime, timezone

import msgpack
import polars as pl

from sigmatiq_nexus import feature_audit as fa
from sigmatiq_nexus import nexus_worker as nw


def row(ts: str, side: str, premium: float, sweep: bool = False) -> dict:
    return {
        "ts_utc": ts,
        "symbol": "SPY",
        "side": side,
        "premium": premium,
        "is_sweep": sweep,
        "delta": 0.5,
        "gamma": 0.01,
        "aggressor": "A",
    }


def test_decision_slot_uses_event_time_in_new_york():
    slot = nw.decision_slot(datetime(2026, 5, 5, 14, 5, tzinfo=timezone.utc))

    assert slot is not None
    assert slot["entry_label"] == "10:00"
    assert slot["window_start"].isoformat() == "09:30:00"
    assert slot["window_end"].isoformat() == "10:00:00"


def test_decision_slot_includes_1130_completed_window():
    slot = nw.decision_slot(datetime(2026, 5, 5, 15, 35, tzinfo=timezone.utc))

    assert slot is not None
    assert slot["entry_label"] == "11:30"
    assert slot["window_start"].isoformat() == "11:00:00"
    assert slot["window_end"].isoformat() == "11:30:00"


def test_window_df_for_slot_uses_completed_window_not_current_tick_window():
    df = pl.DataFrame([
        row("2026-05-05T13:35:00Z", "C", 150_000),  # 09:35 ET, included for 10:00 entry
        row("2026-05-05T13:59:59Z", "C", 75_000),
        row("2026-05-05T14:05:00Z", "P", 500_000),  # 10:05 ET, excluded
    ])

    filtered = nw.window_df_for_slot(df, nw.DECISION_SLOTS[0])

    assert filtered.height == 2
    assert filtered["premium"].sum() == 225_000


def test_window_df_for_slot_filters_session_date():
    df = pl.DataFrame([
        row("2026-05-04T13:35:00Z", "C", 999_000),
        row("2026-05-05T13:35:00Z", "C", 150_000),
    ])

    filtered = nw.window_df_for_slot(df, nw.DECISION_SLOTS[0], date(2026, 5, 5))

    assert filtered.height == 1
    assert filtered["premium"].sum() == 150_000


def test_live_session_event_guard_rejects_prior_session_events():
    old_event = datetime(2026, 5, 15, 16, 34, tzinfo=timezone.utc)
    active_session = date(2026, 5, 19)

    assert nw.is_current_live_session_event(old_event, active_session) is False


def test_live_session_event_guard_accepts_active_session_events():
    active_event = datetime(2026, 5, 19, 14, 34, tzinfo=timezone.utc)
    active_session = date(2026, 5, 19)

    assert nw.is_current_live_session_event(active_event, active_session) is True


def test_window_stats_normalizes_option_side_case():
    df = pl.DataFrame([
        row("2026-05-05T13:35:00Z", "c", 300_000),
        row("2026-05-05T13:36:00Z", "P", 100_000, sweep=True),
    ])

    stats = nw.window_stats(df)

    assert stats["total_p"] == 400_000
    assert stats["call_p"] == 300_000
    assert stats["put_p"] == 100_000
    assert stats["sweep"] == 0.5
    assert nw.dominant_side(stats) == "C"


def test_assess_open_specialist_window_returns_decision_evidence():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)

    async def fake_get_context(symbol):
        assert symbol == "SPY"
        return 22.0, 0.25, -1_500_000_000.0

    worker.get_context = fake_get_context
    df = pl.DataFrame([
        row("2026-05-05T13:35:00Z", "C", 300_000),
        row("2026-05-05T13:36:00Z", "P", 100_000),
    ])

    sentiment, reason, evidence = asyncio.run(worker.assess_open_specialist_window(df, "SPY", nw.DECISION_SLOTS[0]))

    assert sentiment == "BULLISH"
    assert reason == "call_dominance"
    assert evidence["iv_rank"] == 22.0
    assert evidence["total_premium"] == 400_000.0
    assert evidence["call_premium"] == 300_000.0
    assert evidence["put_premium"] == 100_000.0
    assert evidence["call_premium_share"] == 0.75
    assert evidence["put_premium_share"] == 0.25
    assert evidence["premium_put_call_ratio"] == 100_000.0 / 300_000.0
    assert evidence["dominant_side"] == "C"
    assert evidence["min_window_premium_required"] == nw.MIN_WINDOW_PREMIUM
    assert evidence["open_call_dominance_threshold"] == nw.OPEN_CALL_DOMINANCE


def test_decode_msgpack_stream_entry_derives_live_trade_fields():
    payload = {
        "underlying": "SPY",
        "raw_symbol": "SPY   260505P00719000",
        "price": 1.25,
        "size": 4,
        "ts_event_ns": 1_778_012_099_996_787_889,
    }

    decoded = nw.decode_stream_entry({b"data": msgpack.packb(payload, use_bin_type=True)})

    assert decoded["symbol"] == "SPY"
    assert decoded["side"] == "P"
    assert decoded["premium"] == 500.0
    assert decoded["is_sweep"] is False
    assert decoded["ts_utc"].startswith("2026-")


def test_parse_optional_datetime_accepts_dotnet_seven_digit_fractional_seconds():
    parsed = nw.parse_optional_datetime("2026-05-08T16:22:19.2635803+00:00")

    assert parsed is not None
    assert parsed.isoformat() == "2026-05-08T16:22:19.263580+00:00"


def test_freshness_allows_context_newer_than_reference_time():
    reference = datetime(2026, 5, 5, 14, 0, tzinfo=timezone.utc)
    feature_time = datetime(2026, 5, 5, 14, 20, tzinfo=timezone.utc)

    assert nw._freshness_status(reference, feature_time, 120, missing_is_stale=True) == "available"


def test_underlying_mid_bar_context_stays_available_within_two_minutes():
    reference = datetime(2026, 5, 5, 14, 1, 30, tzinfo=timezone.utc)
    payload = {
        "underlying_mid": 700.25,
        "underlying_ts_utc": "2026-05-05T14:00:00Z",
        "underlying_warmup_complete": True,
        "underlying_data_stale": False,
    }

    assert nw._event_freshness_status(payload, reference, "underlying_mid") == "available"


def test_underlying_mid_bar_context_turns_stale_after_two_minutes():
    reference = datetime(2026, 5, 5, 14, 2, 1, tzinfo=timezone.utc)
    payload = {
        "underlying_mid": 700.25,
        "underlying_ts_utc": "2026-05-05T14:00:00Z",
        "underlying_warmup_complete": True,
        "underlying_data_stale": False,
    }

    assert nw._event_freshness_status(payload, reference, "underlying_mid") == "stale"


def test_decode_msgpack_databento_trade_side_does_not_override_option_side():
    payload = {
        "underlying": "SPY",
        "raw_symbol": "SPY   260508C00737000",
        "price": 1.25,
        "size": 4,
        "side": 78,
        "ts_event_ns": int(datetime(2026, 5, 8, 14, 10, tzinfo=timezone.utc).timestamp() * 1_000_000_000),
    }

    decoded = nw.decode_stream_entry({b"data": msgpack.packb(payload, use_bin_type=True)})

    assert decoded["side"] == "C"
    assert decoded["aggressor"] == "M"
    assert decoded["is_sweep"] is False
    assert decoded["_feature_status"]["side"] == "available"
    assert decoded["_feature_status"]["aggressor"] == "available"
    assert decoded["_feature_status"]["is_sweep"] == "derived"


def test_decode_msgpack_databento_ask_side_trade_preserves_option_put_side():
    payload = {
        "underlying": "SPY",
        "raw_symbol": "SPY   260508P00736000",
        "price": 1.25,
        "size": 1000,
        "side": 65,
        "ts_event_ns": int(datetime(2026, 5, 8, 14, 10, tzinfo=timezone.utc).timestamp() * 1_000_000_000),
    }

    decoded = nw.decode_stream_entry({b"data": msgpack.packb(payload, use_bin_type=True)})

    assert decoded["side"] == "P"
    assert decoded["aggressor"] == "A"
    assert decoded["is_sweep"] is True
    assert decoded["premium"] == 125_000.0


def test_contract_details_from_raw_symbol_parses_expiry_strike_and_side():
    details = nw._contract_details_from_raw_symbol("SPY   260505P00719000")

    assert details == {
        "expiry_date": "2026-05-05",
        "strike": 719.0,
        "side": "P",
    }


def test_raw_symbol_with_strike_rewrites_opra_strike():
    assert nw._raw_symbol_with_strike("SPY   260505P00715000", 710.0) == "SPY260505P00710000"
    assert nw._raw_symbol_with_strike("SPY   260505C00715000", 720.0) == "SPY260505C00720000"


def test_raw_symbol_key_variants_include_compact_and_padded_opra_keys():
    assert nw._raw_symbol_key_variants("SPY260505P00715000") == [
        "SPY260505P00715000",
        "SPY   260505P00715000",
    ]
    assert nw._raw_symbol_key_variants("SPY   260505P00715000") == [
        "SPY   260505P00715000",
        "SPY260505P00715000",
    ]


def test_feature_audit_blocks_raw_trade_payload_missing_strategy_fields():
    payload = {
        "underlying": "SPY",
        "raw_symbol": "SPY   260505P00719000",
        "price": 1.25,
        "size": 4,
        "ts_event_ns": 1_778_012_099_996_787_889,
    }

    audit = fa.audit_payload(payload, {})

    assert audit["features"]["premium"]["status"] == "derived"
    assert audit["features"]["side"]["status"] == "derived"
    assert audit["features"]["is_sweep"]["status"] == "missing"
    assert audit["strategies"]["etf_low_sweep_core"]["status"] == "blocked"
    assert "is_sweep" in audit["strategies"]["etf_low_sweep_core"]["missing"]
    assert "iv_rank" in audit["strategies"]["etf_open_specialist"]["missing"]
    assert "delta" in audit["strategies"]["etf_flow_specialist"]["missing"]
    assert "underlying_mid" in audit["strategies"]["etf_momentum_specialist"]["missing"]
    assert "option_mid" in audit["strategies"]["etf_put_credit_open30_spread"]["missing"]


def test_feature_audit_marks_enriched_payload_ready_for_all_strategies():
    payload = {
        "symbol": "SPY",
        "raw_symbol": "SPY   260505C00719000",
        "ts_utc": "2026-05-05T14:00:00Z",
        "side": "C",
        "price": 1.25,
        "size": 10,
        "premium": 1_250.0,
        "is_sweep": False,
        "aggressor": "A",
        "delta": 0.52,
        "gamma": 0.018,
        "greeks_ts_utc": "2026-05-05T14:00:00Z",
        "underlying_mid": 700.25,
        "underlying_ts_utc": "2026-05-05T14:00:00Z",
        "option_mid": 1.24,
        "quote_ts_utc": "2026-05-05T14:00:00Z",
    }

    audit = fa.audit_payload(payload, {"iv_rank": 24.0, "atm_iv": 0.18, "net_gex": 1_000_000, "asOf": "2026-05-05T14:00:00Z"})

    assert {result["status"] for result in audit["strategies"].values()} == {"ready"}


def test_feature_audit_blocks_confluence_when_only_trade_price_exists():
    payload = {
        "symbol": "SPY",
        "raw_symbol": "SPY   260505C00719000",
        "ts_utc": "2026-05-05T14:00:00Z",
        "side": "C",
        "price": 1.25,
        "size": 10,
        "premium": 1_250.0,
        "is_sweep": False,
        "aggressor": "A",
        "delta": 0.52,
        "gamma": 0.018,
        "greeks_ts_utc": "2026-05-05T14:00:00Z",
        "underlying_mid": 700.25,
        "underlying_ts_utc": "2026-05-05T14:00:00Z",
    }

    audit = fa.audit_payload(payload, {"iv_rank": 24.0, "atm_iv": 0.18, "net_gex": 1_000_000, "asOf": "2026-05-05T14:00:00Z"})

    confluence = audit["strategies"]["etf_confluence_sniper"]
    assert confluence["status"] == "blocked"
    assert confluence["missing"] == ["option_mid"]


def test_open_specialist_uses_cheap_call_dominance_for_1000_entry():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis({"options:live:vrp:SPY": json.dumps({"ivRank": 24.0, "asOf": "2026-05-05T14:00:00Z"})})
    df = pl.DataFrame([
        row("2026-05-05T13:35:00Z", "C", 210_000, sweep=True),
        row("2026-05-05T13:50:00Z", "P", 100_000, sweep=True),
    ])

    assert asyncio.run(worker.check_open_specialist_heuristic(df, "SPY", nw.DECISION_SLOTS[0])) == ("BULLISH", True)
    assert asyncio.run(worker.check_open_specialist_heuristic(df, "SPY", nw.DECISION_SLOTS[1])) == (None, False)


def test_context_uses_canonical_live_keys():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis({
        "options:live:vrp:SPY": json.dumps({
            "symbol": "SPY",
            "tsUtc": "2026-05-05T14:00:00Z",
            "ivRank": 24.0,
        }),
        "options:live:iv_surface:SPY": json.dumps({
            "symbol": "SPY",
            "tsUtc": "2026-05-05T14:00:00Z",
            "atmIv": 0.42,
        }),
        "options:live:gex:SPY": json.dumps({
            "symbol": "SPY",
            "tsUtc": "2026-05-05T14:00:00Z",
            "netGex": -1250000,
        }),
    })

    iv_rank, atm_iv, net_gex, status = asyncio.run(
        worker.get_context_with_quality("SPY", datetime(2026, 5, 5, 14, 0, 30, tzinfo=timezone.utc))
    )

    assert iv_rank == 24.0
    assert atm_iv == 0.42
    assert net_gex == -1250000
    assert status == {"iv_rank": "available", "atm_iv": "available", "net_gex": "available"}


def test_vrp_payload_without_rank_does_not_satisfy_iv_rank():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis({
        "options:live:vrp:SPY": json.dumps({
            "symbol": "SPY",
            "tsUtc": "2026-05-05T14:00:00Z",
            "atmIv": 0.42,
            "vrp30d": 0.11,
            "vrpRegime": "unknown",
        })
    })

    iv_rank, _, _, status = asyncio.run(
        worker.get_context_with_quality("SPY", datetime(2026, 5, 5, 14, 0, 30, tzinfo=timezone.utc))
    )

    assert iv_rank == 50.0
    assert status["iv_rank"] == "missing"


def test_low_sweep_core_allows_only_research_windows_and_sides():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    call_df = pl.DataFrame([row("2026-05-05T13:35:00Z", "C", 250_000, sweep=False)])
    put_df = pl.DataFrame([row("2026-05-05T13:35:00Z", "P", 250_000, sweep=False)])

    assert asyncio.run(worker.calculate_low_sweep_heuristic(call_df, nw.DECISION_SLOTS[0])) == ("BULLISH", True)
    assert asyncio.run(worker.calculate_low_sweep_heuristic(put_df, nw.DECISION_SLOTS[0])) == (None, False)
    assert asyncio.run(worker.calculate_low_sweep_heuristic(call_df, nw.DECISION_SLOTS[1])) == ("BULLISH", True)
    assert asyncio.run(worker.calculate_low_sweep_heuristic(put_df, nw.DECISION_SLOTS[1])) == ("BEARISH", True)
    assert asyncio.run(worker.calculate_low_sweep_heuristic(call_df, nw.DECISION_SLOTS[2])) == (None, False)


def test_low_sweep_core_rejects_high_sweep_windows():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    df = pl.DataFrame([row("2026-05-05T13:35:00Z", "C", 250_000, sweep=True)])

    assert asyncio.run(worker.calculate_low_sweep_heuristic(df, nw.DECISION_SLOTS[0])) == (None, False)


def test_confluence_pricing_lag_detects_cheap_option_after_underlying_move():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    df = pl.DataFrame([
        {
            **row("2026-05-05T14:00:00Z", "C", 125_000),
            "raw_symbol": "SPY   260505C00700000",
            "option_mid": 1.00,
            "underlying_mid": 700.0,
            "delta": 0.50,
            "quote_ts_utc": "2026-05-05T14:00:00Z",
        },
        {
            **row("2026-05-05T14:06:00Z", "C", 125_000),
            "raw_symbol": "SPY   260505C00700000",
            "option_mid": 1.10,
            "underlying_mid": 701.0,
            "delta": 0.50,
            "quote_ts_utc": "2026-05-05T14:06:00Z",
        },
    ])

    lag = worker.calculate_pricing_lag(df, "BULLISH")

    assert lag is not None
    assert lag < -0.05


def test_workflow_uses_configured_redis_host_variable():
    text = open(".github/workflows/deploy-nexus-prod.yml", encoding="utf-8").read()

    assert "needs: test" in text
    assert "actions/setup-python@v5" in text
    assert 'python -m pip install -e ".[dev]"' in text
    assert "python -m pytest -q" in text
    assert "CORE_REDIS_HOST" not in text
    assert "rg-sigmatiq-prod" in text
    assert "secrets.NEXUS_REDIS_URL" in text
    assert "NEXUS_REDIS_CLUSTER=true" in text
    assert "NEXUS_SYMBOLS=SPY,QQQ,IWM,UVXY" in text
    assert "NEXUS_FIRST_TRIGGER_SCOPE=symbol" in text
    assert "NEXUS_GROUP_LOCK_STRATEGIES=etf_confluence_sniper" in text


def test_cluster_connection_does_not_disable_tls_verification():
    text = open("src/sigmatiq_nexus/nexus_worker.py", encoding="utf-8").read()

    assert "ssl_cert_reqs=None" not in text
    assert "ssl_cert_reqs\": ssl.CERT_REQUIRED" in text
    assert "ssl_check_hostname\": False" in text


def test_default_symbols_and_scope_cover_combined_etf_sniper():
    assert {"SPY", "QQQ", "IWM", "UVXY"}.issubset(nw.SYMBOLS)
    assert {"SPY", "QQQ", "IWM", "UVXY"}.issubset(nw.NEXUS_HEALTH_SYMBOLS)
    assert nw.FIRST_TRIGGER_SCOPE == "symbol"
    assert "etf_confluence_sniper" in nw.GROUP_LOCK_STRATEGIES


def test_nexus_health_payload_reports_inputs_outputs_and_blocked_reasons():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.health_state = worker._empty_health_state()

    event_time = datetime(2026, 5, 12, 13, 31, tzinfo=timezone.utc)
    worker._record_nexus_input("SPY", "md:SPY:options:trades", msg_id="171-0", event_dt_utc=event_time)
    worker._record_nexus_output(
        "nexus_final_block:SPY:etf_open_specialist:10:00",
        {
            "symbol": "SPY",
            "decision": "FINAL_BLOCKED",
            "reason": "strategy_final_blocked_by_quote",
        },
    )

    payload = worker._build_health_payload(datetime(2026, 5, 12, 13, 32, tzinfo=timezone.utc))

    assert payload["component"] == "nexus"
    assert payload["healthKey"] == "health:nexus"
    assert "SPY" in payload["activeSymbols"]
    assert payload["perSymbol"]["SPY"]["input"]["stream"] == "md:SPY:options:trades"
    assert payload["perSymbol"]["SPY"]["input"]["lastOffset"] == "171-0"
    assert payload["perSymbol"]["SPY"]["outputs"]["final_block"]["count"] == 1
    assert payload["perSymbol"]["SPY"]["blocked"]["by_reason"]["strategy_final_blocked_by_quote"] == 1
    assert "missing_symbol_activity" in payload["degradedReasons"]


def test_append_persistence_event_publishes_nexus_health_key():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis({
        "options:live:gex:SPY": json.dumps({
            "netGex": -2_500_000_000.0,
            "asOf": "2026-05-08T13:59:00Z",
        }),
    })
    worker.health_state = worker._empty_health_state()

    asyncio.run(
        worker._append_persistence_event_for_key(
            "nexus_window_view:SPY:etf_open_specialist:10:00",
            {
                "symbol": "SPY",
                "decision": "BLOCKED",
                "block_reason": "live_feature_quality_gate_closed",
            },
        )
    )

    assert nw.NEXUS_HEALTH_KEY in worker.redis.values
    payload = json.loads(worker.redis.values[nw.NEXUS_HEALTH_KEY])
    assert payload["perSymbol"]["SPY"]["outputs"]["window_view"]["count"] == 1
    assert payload["perSymbol"]["SPY"]["blocked"]["by_reason"]["live_feature_quality_gate_closed"] == 1
    assert worker.redis.xadds[-1][0] == nw.LIVE_PERSISTENCE_EVENT_STREAM

class FakeRedis:
    def __init__(self, values=None):
        self.values = values or {}
        self.expires = {}
        self.sets = []
        self.publishes = []
        self.xadds = []
        self.xrevranges = {}
        self.index_sets = {}

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.values:
            return False
        self.values[key] = value
        if ex is not None:
            self.expires[key] = ex
        self.sets.append((key, value))
        return True

    async def delete(self, key):
        self.values.pop(key, None)
        self.index_sets.pop(key, None)
        return 1

    async def publish(self, channel, value):
        self.publishes.append((channel, value))

    async def xadd(self, name, fields, maxlen=None, approximate=True):
        self.xadds.append((name, fields, maxlen, approximate))
        return "1-0"

    async def xrevrange(self, name, count=None):
        return self.xrevranges.get(name, [])[:count]

    async def sadd(self, key, *members):
        bucket = self.index_sets.setdefault(key, set())
        added = 0
        for member in members:
            if member not in bucket:
                bucket.add(member)
                added += 1
        return added

    async def srem(self, key, *members):
        bucket = self.index_sets.get(key)
        if not bucket:
            return 0
        removed = 0
        for member in members:
            if member in bucket:
                bucket.discard(member)
                removed += 1
        if not bucket:
            self.index_sets.pop(key, None)
        return removed

    async def smembers(self, key):
        return set(self.index_sets.get(key, set()))

    async def expire(self, key, ttl_seconds):
        if key in self.values or key in self.index_sets:
            self.expires[key] = ttl_seconds
            return 1
        return 0


class FakeXReadRedis:
    def __init__(self):
        self.calls = []

    async def xread(self, streams, count=None, block=None):
        self.calls.append((streams, count, block))
        stream_name = next(iter(streams))
        return [(stream_name, [(f"{len(self.calls)}-0", {b"data": b"{}"})])]


class FakeClusterPublishRedis:
    def __init__(self):
        self.commands = []

    async def execute_command(self, *args):
        self.commands.append(args)
        return 1


def test_publish_falls_back_to_execute_command_for_cluster_client():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeClusterPublishRedis()

    asyncio.run(worker._publish("signal:test", "payload"))

    assert worker.redis.commands == [("PUBLISH", "signal:test", "payload")]


def test_window_df_for_symbol_falls_back_to_redis_stream_when_buffer_empty():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.buffers = {"SPY": deque()}
    worker.redis = FakeRedis()
    worker._log = lambda *args, **kwargs: None
    stream_rows = [
        {
            "underlying": "SPY",
            "raw_symbol": "SPY   260505C00720000",
            "price": 10.0,
            "size": 200,
            "side": 65,
            "ts_event_ns": int(datetime(2026, 5, 5, 13, 40, tzinfo=timezone.utc).timestamp() * 1_000_000_000),
        },
        {
            "underlying": "SPY",
            "raw_symbol": "SPY   260505P00710000",
            "price": 5.0,
            "size": 10,
            "side": 65,
            "ts_event_ns": int(datetime(2026, 5, 5, 14, 10, tzinfo=timezone.utc).timestamp() * 1_000_000_000),
        },
    ]
    worker.redis.xrevranges["md:SPY:options:trades"] = [
        (f"{idx}-0", {b"data": msgpack.packb(payload, use_bin_type=True)})
        for idx, payload in enumerate(stream_rows)
    ]

    df = asyncio.run(worker._window_df_for_symbol("SPY", nw.DECISION_SLOTS[0], date(2026, 5, 5)))

    assert df.height == 1
    assert df["side"].to_list() == ["C"]
    assert df["premium"].sum() == 200_000


def test_window_stream_fallback_enriches_rows_from_live_contract_state():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.buffers = {"SPY": deque()}
    worker.redis = FakeRedis({
        "equity:live:context:SPY": json.dumps({
            "price": 700.25,
            "lastPriceUtc": "2026-05-05T14:00:00Z",
            "warmupComplete": True,
            "priceDataStale": False,
        }),
        "options:live:contract_state:SPY   260505C00720000": json.dumps({
            "optionMid": 1.24,
            "bid": 1.23,
            "ask": 1.25,
            "underlyingMid": 700.30,
            "delta": 0.52,
            "gamma": 0.018,
            "asOfUtc": "2026-05-05T14:00:00.1234567+00:00",
            "greeksTsUtc": "2026-05-05T14:00:00.1234567+00:00",
        }),
    })
    worker._log = lambda *args, **kwargs: None
    worker.redis.xrevranges["md:SPY:options:trades"] = [
        ("1-0", {b"data": msgpack.packb({
            "underlying": "SPY",
            "raw_symbol": "SPY   260505C00720000",
            "price": 1.25,
            "size": 10,
            "side": 65,
            "ts_event_ns": int(datetime(2026, 5, 5, 13, 40, tzinfo=timezone.utc).timestamp() * 1_000_000_000),
        }, use_bin_type=True)}),
    ]

    df = asyncio.run(worker._window_df_for_symbol(
        "SPY",
        nw.DECISION_SLOTS[0],
        date(2026, 5, 5),
        datetime(2026, 5, 5, 14, 0, tzinfo=timezone.utc),
    ))

    assert df.height == 1
    row = df.to_dicts()[0]
    assert row["option_mid"] == 1.24
    assert row["underlying_mid"] == 700.30
    assert row["delta"] == 0.52
    assert row["gamma"] == 0.018
    assert row["_feature_status"]["option_mid"] == "available"
    assert row["_feature_status"]["underlying_mid"] == "available"
    assert row["_feature_status"]["delta"] == "available"


def test_window_buffer_refreshes_stale_rows_from_live_contract_state():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    stale_trade = nw.normalize_trade_payload({
        "symbol": "SPY",
        "raw_symbol": "SPY   260505C00720000",
        "side": "C",
        "price": 1.0,
        "size": 10,
        "ts_utc": "2026-05-05T13:40:00Z",
        "option_mid": 1.0,
        "underlying_mid": 699.0,
        "delta": 0.40,
        "gamma": 0.010,
        "quote_ts_utc": "2026-05-05T13:40:00Z",
        "underlying_ts_utc": "2026-05-05T13:40:00Z",
        "greeks_ts_utc": "2026-05-05T13:40:00Z",
    })
    worker.buffers = {"SPY": deque([stale_trade])}
    worker.redis = FakeRedis({
        "equity:live:context:SPY": json.dumps({
            "price": 700.25,
            "lastPriceUtc": "2026-05-05T14:00:00Z",
            "warmupComplete": True,
            "priceDataStale": False,
        }),
        "options:live:contract_state:SPY   260505C00720000": json.dumps({
            "optionMid": 1.24,
            "bid": 1.23,
            "ask": 1.25,
            "underlyingMid": 700.30,
            "delta": 0.52,
            "gamma": 0.018,
            "asOfUtc": "2026-05-05T14:00:00Z",
            "greeksTsUtc": "2026-05-05T14:00:00Z",
        }),
    })
    worker._log = lambda *args, **kwargs: None

    df = asyncio.run(worker._window_df_for_symbol(
        "SPY",
        nw.DECISION_SLOTS[0],
        date(2026, 5, 5),
        datetime(2026, 5, 5, 14, 0, tzinfo=timezone.utc),
    ))

    assert df.height == 1
    row = df.to_dicts()[0]
    assert row["option_mid"] == 1.24
    assert row["underlying_mid"] == 700.30
    assert row["delta"] == 0.52
    assert row["gamma"] == 0.018
    assert row["quote_ts_utc"] == "2026-05-05T14:00:00Z"
    assert row["greeks_ts_utc"] == "2026-05-05T14:00:00Z"
    assert row["_feature_status"]["option_mid"] == "available"
    assert row["_feature_status"]["underlying_mid"] == "available"
    assert row["_feature_status"]["delta"] == "available"
    assert row["_feature_status"]["gamma"] == "available"


def test_window_stream_fallback_enriches_rows_from_compact_contract_state_key():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.buffers = {"SPY": deque()}
    worker.redis = FakeRedis({
        "equity:live:context:SPY": json.dumps({
            "price": 700.25,
            "lastPriceUtc": "2026-05-05T14:00:00Z",
            "warmupComplete": True,
            "priceDataStale": False,
        }),
        "options:live:contract_state:SPY260505C00720000": json.dumps({
            "optionMid": 1.24,
            "bid": 1.23,
            "ask": 1.25,
            "underlyingMid": 700.30,
            "delta": 0.52,
            "gamma": 0.018,
            "asOfUtc": "2026-05-05T14:00:00Z",
            "greeksTsUtc": "2026-05-05T14:00:00Z",
        }),
    })
    worker._log = lambda *args, **kwargs: None
    worker.redis.xrevranges["md:SPY:options:trades"] = [
        ("1-0", {b"data": msgpack.packb({
            "underlying": "SPY",
            "raw_symbol": "SPY   260505C00720000",
            "price": 1.25,
            "size": 10,
            "side": 65,
            "ts_event_ns": int(datetime(2026, 5, 5, 13, 59, tzinfo=timezone.utc).timestamp() * 1_000_000_000),
        }, use_bin_type=True)}),
    ]

    df = asyncio.run(worker._window_df_for_symbol(
        "SPY",
        nw.DECISION_SLOTS[0],
        date(2026, 5, 5),
        datetime(2026, 5, 5, 14, 0, tzinfo=timezone.utc),
    ))

    assert df.height == 1
    row = df.to_dicts()[0]
    assert row["option_mid"] == 1.24
    assert row["delta"] == 0.52
    assert row["_feature_status"]["option_mid"] == "available"


def test_cluster_mode_reads_each_input_stream_separately(monkeypatch):
    monkeypatch.setattr(nw, "REDIS_CLUSTER", True)
    redis_client = FakeXReadRedis()
    streams = {
        "md:QQQ:options:trades": "$",
        "md:SPY:options:trades": "$",
    }

    replies = asyncio.run(nw.read_input_streams(redis_client, streams))

    assert len(redis_client.calls) == 2
    assert [call[0] for call in redis_client.calls] == [
        {"md:QQQ:options:trades": "$"},
        {"md:SPY:options:trades": "$"},
    ]
    assert all(call[1:] == (10, 250) for call in redis_client.calls)
    assert [reply[0] for reply in replies] == ["md:QQQ:options:trades", "md:SPY:options:trades"]


def test_non_cluster_mode_uses_single_multi_stream_xread(monkeypatch):
    monkeypatch.setattr(nw, "REDIS_CLUSTER", False)
    redis_client = FakeXReadRedis()
    streams = {
        "md:QQQ:options:trades": "$",
        "md:SPY:options:trades": "$",
    }

    asyncio.run(nw.read_input_streams(redis_client, streams))

    assert redis_client.calls == [(streams, 10, 1000)]


def test_input_streams_default_to_persistent_start_id(monkeypatch):
    monkeypatch.setattr(nw, "INPUT_STREAM", None)
    monkeypatch.setattr(nw, "SYMBOLS", {"SPY"})
    monkeypatch.setattr(nw, "STREAM_START_ID", "0-0")

    assert nw.input_streams() == {"md:SPY:options:trades": "0-0"}


def test_restore_and_persist_stream_offsets():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis({nw.redis_stream_offset_key("md:SPY:options:trades"): "171-0"})
    streams = {"md:SPY:options:trades": "0-0", "md:QQQ:options:trades": "0-0"}

    asyncio.run(worker.restore_stream_offsets(streams))
    asyncio.run(worker.persist_stream_offset("md:QQQ:options:trades", b"172-0"))

    assert streams == {"md:SPY:options:trades": "171-0", "md:QQQ:options:trades": "0-0"}
    assert worker.redis.values[nw.redis_stream_offset_key("md:QQQ:options:trades")] == "172-0"


def test_publish_final_appends_live_persistence_event():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = set()
    worker.redis = FakeRedis({
        "options:live:contract_state:SPY   260505C00720000": json.dumps({
            "optionMid": 1.24,
            "bid": 1.20,
            "ask": 1.28,
            "asOfUtc": "2026-05-05T14:00:00Z",
            "tradable": True,
        }),
        "options:live:tradability:SPY   260505C00720000": json.dumps({
            "quoteAgeMs": 700,
            "tradabilityBucket": "tradable",
        }),
    })

    asyncio.run(worker._publish_final("etf_low_sweep_core", "SPY", "BULLISH", 1.0, 1.24, datetime(2026, 5, 5).date(), nw.DECISION_SLOTS[0], "SPY   260505C00720000"))

    overlay_sets = [(key, value) for key, value in worker.redis.sets if key == "nexus_live_overlay:SPY"]
    assert len(overlay_sets) == 1
    assert len(worker.redis.xadds) == 3
    contract_refs = [fields for _, fields, _, _ in worker.redis.xadds if fields.get("source") == "sigmatiq_nexus_contract_reference"]
    assert {fields["redis_key"] for fields in contract_refs} == {
        "options:live:contract_state:SPY   260505C00720000",
        "options:live:tradability:SPY   260505C00720000",
    }
    assert all(json.loads(fields["payload_json"])["symbol"] == "SPY" for fields in contract_refs)
    assert all(json.loads(fields["payload_json"])["raw_symbol"] == "SPY   260505C00720000" for fields in contract_refs)
    stream, fields, maxlen, approximate = next(
        entry for entry in worker.redis.xadds if entry[1]["redis_key"] == "nexus_live_overlay:SPY"
    )
    assert stream == "live:persistence:events"
    assert fields["redis_key"] == "nexus_live_overlay:SPY"
    payload = json.loads(fields["payload_json"])
    assert payload["strategy"] == "etf_low_sweep_core"
    assert payload["stage"] == 2
    assert payload["decision"] == "BET"
    assert payload["signal_id"].startswith("sig_")
    assert payload["position_id"] == payload["signal_id"]
    assert payload["raw_symbol"] == "SPY   260505C00720000"
    assert payload["expiry_date"] == "2026-05-05"
    assert payload["strike"] == 720.0
    assert payload["option_side"] == "C"
    assert payload["risk"]["stop_loss_pct"] == nw.STOP_LOSS_PCT
    assert payload["risk"]["guard_activate_pct"] == nw.GUARD_ACTIVATE_PCT
    assert payload["risk"]["guard_floor_pct"] == nw.GUARD_FLOOR_PCT
    assert payload["quote_freshness"] == "available"
    assert payload["quote_valid_until"] == "2026-05-05T14:00:15+00:00"
    assert payload["entry_quote"]["option_mid"] == 1.24
    assert payload["entry_quote"]["option_bid"] == 1.2
    assert payload["entry_quote"]["option_ask"] == 1.28
    assert payload["entry_quote"]["tradability_bucket"] == "tradable"
    assert payload["execution"]["order_type"] == "limit"
    assert payload["execution"]["price_reference"] == "option_mid"
    assert payload["execution"]["reference_price"] == 1.24
    assert payload["execution"]["max_slippage_pct"] == nw.EXECUTION_MAX_SLIPPAGE_PCT
    assert payload["execution"]["quote_freshness"] == "available"
    assert worker.redis.values[nw.redis_symbol_lock_key(datetime(2026, 5, 5).date(), "SPY")] == payload["signal_id"]
    assert json.loads(worker.redis.values[nw.redis_active_position_key(datetime(2026, 5, 5).date(), "SPY")])["entry_price"] == 1.24
    assert maxlen == 10000
    assert approximate is True


def test_publish_final_blocks_when_quote_is_missing():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = set()
    worker.active_positions = {}
    worker.redis = FakeRedis()

    asyncio.run(worker._publish_final("etf_low_sweep_core", "SPY", "BULLISH", 1.0, 1.24, datetime(2026, 5, 5).date(), nw.DECISION_SLOTS[0], "SPY   260505C00720000"))

    assert not any(key == "nexus_live_overlay:SPY" for key, _ in worker.redis.sets)
    assert not worker.already_signaled(datetime(2026, 5, 5).date(), "SPY", "etf_low_sweep_core")
    block = json.loads(worker.redis.values["nexus_final_block:SPY:etf_low_sweep_core:10:00"])
    assert block["decision"] == "FINAL_BLOCKED"
    assert block["confidence"] == 1.0
    assert block["reason"] == "strategy_final_blocked_by_quote"
    assert block["raw_symbol"] == "SPY   260505C00720000"
    assert worker.redis.index_sets[nw.nexus_index_key(date(2026, 5, 5), "SPY", "final_block")] == {"etf_low_sweep_core:10:00"}
    assert worker.redis.xadds[-1][1]["redis_key"] == "nexus_final_block:SPY:etf_low_sweep_core:10:00"
    assert worker.redis.publishes[-1][0] == "signal:final_block:etf_low_sweep_core"


def test_publish_final_blocks_when_redis_lock_exists():
    session_date = datetime(2026, 5, 5).date()
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = set()
    worker.active_positions = {}
    worker.redis = FakeRedis({
        nw.redis_symbol_lock_key(session_date, "SPY"): "sig_existing",
        "options:live:contract_state:SPY   260505C00720000": json.dumps({
            "optionMid": 1.24,
            "bid": 1.20,
            "ask": 1.28,
            "asOfUtc": "2026-05-05T14:00:00Z",
            "quoteAgeMs": 700,
            "tradable": True,
            "executable": True,
            "tradabilityBucket": "tradable",
        }),
    })

    asyncio.run(worker._publish_final("etf_low_sweep_core", "SPY", "BULLISH", 1.0, 1.24, session_date, nw.DECISION_SLOTS[0], "SPY   260505C00720000"))

    assert not any(key == "nexus_live_overlay:SPY" for key, _ in worker.redis.sets)
    assert nw.redis_active_position_key(session_date, "SPY") not in worker.redis.values


def test_restore_active_positions_loads_current_session_symbol_state(monkeypatch):
    monkeypatch.setattr(nw, "SYMBOLS", {"SPY", "QQQ"})
    session_date = datetime(2026, 5, 5).date()
    position = {
        "entry_price": 1.24,
        "is_guarded": False,
        "side": "BULLISH",
        "raw_symbol": "SPY   260505C00720000",
        "signal_id": "sig_restore",
        "position_id": "sig_restore",
        "session_date": str(session_date),
    }
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.last_reset_session_date = session_date
    worker.active_positions = {}
    worker.redis = FakeRedis({nw.redis_active_position_key(session_date, "SPY"): json.dumps(position)})

    asyncio.run(worker.restore_active_positions())

    assert worker.active_positions == {"SPY": position}


def test_publish_intermediate_sets_key_and_appends_persistence_event():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis()

    session_date = datetime(2026, 5, 5).date()
    asyncio.run(worker._publish_intermediate("etf_low_sweep_core", "SPY", "BULLISH", 0.91, nw.DECISION_SLOTS[0], session_date, "SPY   260505C00720000"))

    assert worker.redis.sets[0][0] == "nexus_intermediate:SPY:etf_low_sweep_core:10:00"
    payload = json.loads(worker.redis.sets[0][1])
    assert payload["strategy"] == "etf_low_sweep_core"
    assert payload["stage"] == 1
    assert payload["decision"] == "INTERMEDIATE"
    assert payload["sentiment"] == "BULLISH"
    assert payload["confidence"] == 0.91
    assert payload["signal_id"].startswith("sig_")
    assert payload["position_id"] == payload["signal_id"]
    assert payload["raw_symbol"] == "SPY   260505C00720000"
    assert worker.redis.xadds[0][1]["redis_key"] == "nexus_intermediate:SPY:etf_low_sweep_core:10:00"
    assert worker.redis.publishes[0] == ("nexus_intermediate:updates", "SPY")
    assert worker.redis.publishes[1][0] == "signal:intermediate:etf_low_sweep_core"

    index_key = nw.nexus_index_key(session_date, "SPY", "intermediate")
    assert asyncio.run(worker.redis.smembers(index_key)) == {"etf_low_sweep_core:10:00"}
    assert worker.redis.expires[index_key] == nw.NEXUS_INDEX_TTL_SECONDS


def test_index_sadd_swallows_redis_failures_and_logs():
    class FailingRedis:
        async def sadd(self, *args, **kwargs):
            raise RuntimeError("boom")

        async def expire(self, *args, **kwargs):
            raise RuntimeError("boom")

    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FailingRedis()
    logged: list[tuple[str, dict]] = []
    worker._log = lambda event, **kwargs: logged.append((event, kwargs))

    # Must not raise — publish path should not be blocked by index errors.
    asyncio.run(worker._index_sadd("nexus:index:test", "member"))
    assert logged and logged[0][0] == "nexus_index_add_failed"


def test_index_helpers_round_trip_via_fake_redis():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis()
    key = "nexus:index:2026-05-05:SPY:window_view"

    asyncio.run(worker._index_sadd(key, "etf_flow_specialist:10:30"))
    asyncio.run(worker._index_sadd(key, "etf_momentum_specialist:11:00"))
    members = asyncio.run(worker.redis.smembers(key))
    assert members == {"etf_flow_specialist:10:30", "etf_momentum_specialist:11:00"}

    asyncio.run(worker._index_srem(key, "etf_flow_specialist:10:30"))
    assert asyncio.run(worker.redis.smembers(key)) == {"etf_momentum_specialist:11:00"}


def test_publish_window_view_sets_key_and_appends_persistence_event():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis()
    worker.window_views_reported = set()
    worker._current_window_df = pl.DataFrame([
        nw.normalize_trade_payload({
            "symbol": "SPY",
            "raw_symbol": "SPY   260505C00720000",
            "ts_utc": "2026-05-05T13:35:00Z",
            "side": "C",
            "price": 12.5,
            "size": 300,
            "premium": 375_000,
            "is_sweep": False,
            "aggressor": "A",
            "delta": 0.5,
            "gamma": 0.01,
            "underlying_mid": 720.0,
            "option_mid": 12.5,
        }),
        nw.normalize_trade_payload({
            "symbol": "SPY",
            "raw_symbol": "SPY   260505P00700000",
            "ts_utc": "2026-05-05T13:36:00Z",
            "side": "P",
            "price": 5.0,
            "size": 100,
            "premium": 50_000,
            "is_sweep": False,
            "aggressor": "A",
            "delta": -0.4,
            "gamma": 0.01,
            "underlying_mid": 719.5,
            "option_mid": 5.0,
        }),
    ])
    worker._current_window_pricing_summary = worker._window_pricing_summary(worker._current_window_df)

    asyncio.run(worker._publish_window_view(
        "etf_low_sweep_core",
        "SPY",
        "BULLISH",
        "low_sweep_call_dominance",
        nw.DECISION_SLOTS[0],
        datetime(2026, 5, 5).date(),
    ))

    assert worker.redis.sets[0][0] == "nexus_window_view:SPY:etf_low_sweep_core:10:00"
    payload = json.loads(worker.redis.sets[0][1])
    assert payload["decision"] == "WINDOW_VIEW"
    assert payload["sentiment"] == "BULLISH"
    assert payload["reason"] == "low_sweep_call_dominance"
    assert payload["lead_contract_raw_symbol"] == "SPY   260505C00720000"
    assert payload["lead_contract_expiry_date"] == "2026-05-05"
    assert payload["lead_contract_strike"] == 720.0
    assert payload["lead_contract_side"] == "C"
    assert "lead_contract_pricing_lag" in payload
    assert "lead_contract_cheapness_score" in payload
    assert worker.redis.xadds[0][1]["redis_key"] == "nexus_window_view:SPY:etf_low_sweep_core:10:00"
    assert worker.redis.publishes[0][0] == "signal:window_view:etf_low_sweep_core"


def test_publish_window_view_includes_open_specialist_evidence():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis()
    worker.window_views_reported = set()
    worker._current_window_df = pl.DataFrame([
        nw.normalize_trade_payload({
            "symbol": "SPY",
            "raw_symbol": "SPY   260505C00720000",
            "ts_utc": "2026-05-05T13:35:00Z",
            "side": "C",
            "price": 12.5,
            "size": 300,
            "premium": 375_000,
            "is_sweep": False,
            "aggressor": "A",
            "delta": 0.5,
            "gamma": 0.01,
            "underlying_mid": 720.0,
            "option_mid": 12.5,
        }),
    ])
    worker._current_window_pricing_summary = worker._window_pricing_summary(worker._current_window_df)

    asyncio.run(worker._publish_window_view(
        "etf_open_specialist",
        "SPY",
        "CHOP",
        "cheap_vol_or_premium_filter_not_met",
        nw.DECISION_SLOTS[0],
        datetime(2026, 5, 5).date(),
        evidence={
            "iv_rank": 42.0,
            "total_premium": 375_000.0,
            "call_premium_share": 1.0,
            "put_premium_share": 0.0,
            "premium_put_call_ratio": 0.0,
            "dominant_side": "C",
        },
    ))

    payload = json.loads(worker.redis.sets[0][1])
    assert payload["iv_rank"] == 42.0
    assert payload["total_premium"] == 375_000.0
    assert payload["call_premium_share"] == 1.0
    assert payload["put_premium_share"] == 0.0
    assert payload["premium_put_call_ratio"] == 0.0
    assert payload["dominant_side"] == "C"


def test_publish_window_assessments_writes_no_bet_strategy_audit():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis({
        "options:live:vrp:QQQ": json.dumps({"ivRank": 22, "asOf": "2026-05-05T14:00:00Z"}),
        "options:live:iv_surface:QQQ": json.dumps({"atmIv": 0.22, "asOf": "2026-05-05T14:00:00Z"}),
        "options:live:gex:QQQ": json.dumps({"netGex": 1_000_000, "asOf": "2026-05-05T14:00:00Z"}),
    })
    worker.strategy_audits_reported = set()
    worker.window_views_reported = set()
    worker.feature_blocks_reported = set()
    worker.health_state = worker._empty_health_state()
    worker._log = lambda *args, **kwargs: None
    df = pl.DataFrame([
        nw.normalize_trade_payload({
            "symbol": "QQQ",
            "raw_symbol": "QQQ   260505C00500000",
            "ts_utc": "2026-05-05T13:35:00Z",
            "side": "C",
            "price": 2.0,
            "size": 1000,
            "premium": 200_000,
            "is_sweep": False,
            "aggressor": "A",
            "delta": 0.50,
            "gamma": 0.01,
            "greeks_ts_utc": "2026-05-05T13:35:00Z",
            "underlying_mid": 500.0,
            "underlying_ts_utc": "2026-05-05T13:35:00Z",
            "option_mid": 2.0,
            "quote_age_ms": 100,
        }),
        nw.normalize_trade_payload({
            "symbol": "QQQ",
            "raw_symbol": "QQQ   260505P00500000",
            "ts_utc": "2026-05-05T13:36:00Z",
            "side": "P",
            "price": 1.9,
            "size": 1000,
            "premium": 190_000,
            "is_sweep": False,
            "aggressor": "A",
            "delta": -0.50,
            "gamma": 0.01,
            "greeks_ts_utc": "2026-05-05T13:36:00Z",
            "underlying_mid": 500.0,
            "underlying_ts_utc": "2026-05-05T13:36:00Z",
            "option_mid": 1.9,
            "quote_age_ms": 100,
        }),
    ])

    asyncio.run(worker.publish_window_assessments(df, "QQQ", nw.DECISION_SLOTS[0], datetime(2026, 5, 5).date()))

    audit = json.loads(worker.redis.values["nexus_strategy_audit:QQQ:etf_open_specialist:10:00"])
    assert audit["kind"] == "nexus_strategy_decision_audit"
    assert audit["decision"] == "NO_BET"
    assert audit["sentiment"] == "CHOP"
    assert audit["reason"] == "no_open_dominance"
    assert audit["inputs"]["iv_rank"] == 22.0
    assert audit["inputs"]["call_premium"] == 200_000.0
    assert audit["inputs"]["put_premium"] == 190_000.0
    assert audit["inputs"]["open_call_dominance_threshold"] == nw.OPEN_CALL_DOMINANCE
    assert [gate["name"] for gate in audit["gates"]] == [
        "strategy_window_applicability",
        "live_feature_quality",
        "strategy_signal_logic",
        "bet_escalation",
    ]
    signal_gate = next(gate for gate in audit["gates"] if gate["name"] == "strategy_signal_logic")
    assert signal_gate["severity"] == "soft"
    assert signal_gate["status"] == "failed"
    assert signal_gate["hard_block"] is False
    assert signal_gate["reason"] == "no_open_dominance"
    assert worker.redis.index_sets[nw.nexus_index_key(date(2026, 5, 5), "QQQ", "strategy_audit")]
    assert any(fields["redis_key"] == "nexus_strategy_audit:QQQ:etf_open_specialist:10:00" for _, fields, _, _ in worker.redis.xadds)
    assert ("signal:strategy_audit", json.dumps(audit)) in worker.redis.publishes
    view = json.loads(worker.redis.values["nexus_window_view:QQQ:etf_open_specialist:10:00"])
    assert view["decision"] == "WINDOW_VIEW"
    assert view["sentiment"] == "CHOP"
    assert view["reason"] == "no_open_dominance"
    assert view["gates"] == audit["gates"]


def test_publish_window_assessments_audits_not_applicable_strategy_lanes():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis({
        "options:live:vrp:IWM": json.dumps({"ivRank": 20, "asOf": "2026-05-05T14:05:00Z"}),
        "options:live:iv_surface:IWM": json.dumps({"atmIv": 0.22, "asOf": "2026-05-05T14:05:00Z"}),
        "options:live:gex:IWM": json.dumps({"netGex": 1_000_000, "asOf": "2026-05-05T14:05:00Z"}),
    })
    worker.strategy_audits_reported = set()
    worker.window_views_reported = set()
    worker.feature_blocks_reported = set()
    worker.health_state = worker._empty_health_state()
    worker._log = lambda *args, **kwargs: None
    df = pl.DataFrame([
        nw.normalize_trade_payload({
            "symbol": "IWM",
            "raw_symbol": "IWM   260505C00220000",
            "ts_utc": "2026-05-05T13:35:00Z",
            "side": "C",
            "price": 2.0,
            "size": 1000,
            "premium": 200_000,
            "is_sweep": False,
            "aggressor": "A",
            "delta": 0.50,
            "gamma": 0.01,
            "greeks_ts_utc": "2026-05-05T13:35:00Z",
            "underlying_mid": 220.0,
            "underlying_ts_utc": "2026-05-05T13:35:00Z",
            "option_mid": 2.0,
            "quote_age_ms": 100,
        })
    ])

    asyncio.run(worker.publish_window_assessments(df, "IWM", nw.DECISION_SLOTS[0], datetime(2026, 5, 5).date()))

    audit = json.loads(worker.redis.values["nexus_strategy_audit:IWM:etf_put_credit_open30_spread:10:00"])
    assert audit["decision"] == "SKIPPED"
    assert audit["reason"] == "strategy_not_applicable_to_symbol_or_window"
    assert audit["inputs"]["total_premium"] == 200_000.0
    assert audit["gates"] == [
        {
            "name": "strategy_window_applicability",
            "scope": "strategy",
            "severity": "observability",
            "status": "skipped",
            "hard_block": False,
            "reason": "strategy_not_applicable_to_symbol_or_window",
            "details": {
                "symbol": "IWM",
                "entry_time": "10:00",
                "allowed_symbols": ["QQQ", "SPY"],
                "allowed_entry_times": ["10:00"],
            },
        }
    ]
    view = json.loads(worker.redis.values["nexus_window_view:IWM:etf_put_credit_open30_spread:10:00"])
    assert view["gates"] == audit["gates"]


def test_publish_window_pricing_sets_key_and_side_summary():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis()
    worker.window_pricing_reported = set()
    df = pl.DataFrame([
        nw.normalize_trade_payload({
            "symbol": "SPY",
            "raw_symbol": "SPY   260505C00720000",
            "ts_utc": "2026-05-05T13:35:00Z",
            "side": "C",
            "price": 10.0,
            "size": 200,
            "premium": 200_000,
            "is_sweep": False,
            "aggressor": "A",
            "delta": 0.50,
            "gamma": 0.01,
            "underlying_mid": 720.0,
            "option_mid": 10.0,
            "quote_ts_utc": "2026-05-05T13:35:00Z",
        }),
        nw.normalize_trade_payload({
            "symbol": "SPY",
            "raw_symbol": "SPY   260505C00720000",
            "ts_utc": "2026-05-05T13:41:00Z",
            "side": "C",
            "price": 10.4,
            "size": 200,
            "premium": 208_000,
            "is_sweep": False,
            "aggressor": "A",
            "delta": 0.50,
            "gamma": 0.01,
            "underlying_mid": 721.5,
            "option_mid": 10.4,
            "quote_ts_utc": "2026-05-05T13:41:00Z",
        }),
        nw.normalize_trade_payload({
            "symbol": "SPY",
            "raw_symbol": "SPY   260505P00700000",
            "ts_utc": "2026-05-05T13:35:00Z",
            "side": "P",
            "price": 8.0,
            "size": 150,
            "premium": 120_000,
            "is_sweep": False,
            "aggressor": "A",
            "delta": -0.45,
            "gamma": 0.01,
            "underlying_mid": 720.0,
            "option_mid": 8.0,
            "quote_ts_utc": "2026-05-05T13:35:00Z",
        }),
        nw.normalize_trade_payload({
            "symbol": "SPY",
            "raw_symbol": "SPY   260505P00700000",
            "ts_utc": "2026-05-05T13:41:00Z",
            "side": "P",
            "price": 7.4,
            "size": 150,
            "premium": 111_000,
            "is_sweep": False,
            "aggressor": "A",
            "delta": -0.45,
            "gamma": 0.01,
            "underlying_mid": 721.5,
            "option_mid": 7.4,
            "quote_ts_utc": "2026-05-05T13:41:00Z",
        }),
    ])

    asyncio.run(worker.publish_window_pricing(df, "SPY", nw.DECISION_SLOTS[0], datetime(2026, 5, 5).date()))

    payload = json.loads(worker.redis.sets[0][1])
    assert worker.redis.sets[0][0] == "nexus_window_pricing:SPY:10:00"
    assert payload["decision"] == "WINDOW_PRICING"
    assert payload["cheap_contract_raw_symbol"] is not None
    assert payload["costly_contract_raw_symbol"] is not None
    assert payload["cheap_side"] in {"C", "P"}
    assert payload["costly_side"] in {"C", "P"}
    assert payload["pricing_quality"] == "usable"
    assert payload["pricing_quality_reason"] == "point_in_time_pricing_profiles"
    assert worker.redis.publishes[0][0] == "signal:window_pricing"


def test_window_pricing_uses_trade_baseline_with_later_live_quote_snapshot():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    df = pl.DataFrame([
        nw.normalize_trade_payload({
            "symbol": "SPY",
            "raw_symbol": "SPY   260505C00720000",
            "ts_utc": "2026-05-05T13:35:00Z",
            "side": "C",
            "price": 10.0,
            "size": 200,
            "premium": 200_000,
            "is_sweep": False,
            "aggressor": "A",
            "delta": 0.50,
            "underlying_mid": 720.0,
            "option_mid": 10.4,
            "quote_ts_utc": "2026-05-05T14:00:00Z",
        }),
        nw.normalize_trade_payload({
            "symbol": "SPY",
            "raw_symbol": "SPY   260505P00700000",
            "ts_utc": "2026-05-05T13:35:00Z",
            "side": "P",
            "price": 8.0,
            "size": 150,
            "premium": 120_000,
            "is_sweep": False,
            "aggressor": "A",
            "delta": -0.45,
            "underlying_mid": 721.5,
            "option_mid": 7.4,
            "quote_ts_utc": "2026-05-05T14:00:00Z",
        }),
    ])

    summary = worker._window_pricing_summary(df)

    assert summary["evaluated_contract_count"] == 2
    assert summary["cheap_contract"] is not None
    assert summary["costly_contract"] is not None
    assert summary["pricing_quality"] == "usable"
    assert summary["pricing_quality_reason"] == "point_in_time_pricing_profiles"


def test_window_pricing_requires_current_quote_snapshot():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    df = pl.DataFrame([
        nw.normalize_trade_payload({
            "symbol": "SPY",
            "raw_symbol": "SPY   260505C00720000",
            "ts_utc": "2026-05-05T13:35:00Z",
            "side": "C",
            "price": 10.0,
            "size": 200,
            "premium": 200_000,
            "is_sweep": False,
            "aggressor": "A",
            "delta": 0.50,
            "underlying_mid": 720.0,
        }),
    ])

    summary = worker._window_pricing_summary(df)

    assert summary["evaluated_contract_count"] == 0
    assert summary["cheap_contract"] is None
    assert summary["cheap_side"] is None
    assert summary["pricing_quality"] == "unknown"
    assert summary["pricing_quality_reason"] == "no_reliable_pricing_profiles"


def test_normalize_trade_payload_derives_price_from_premium_and_size():
    normalized = nw.normalize_trade_payload({
        "symbol": "SPY",
        "raw_symbol": "SPY   260505C00720000",
        "ts_utc": "2026-05-05T13:35:00Z",
        "side": "C",
        "size": 200,
        "premium": 200_000,
        "is_sweep": False,
        "aggressor": "A",
    })

    assert normalized["price"] == 10.0
    assert normalized["premium"] == 200_000.0


def test_window_pricing_uses_derived_trade_price_from_premium_and_size():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    df = pl.DataFrame([
        nw.normalize_trade_payload({
            "symbol": "SPY",
            "raw_symbol": "SPY   260505C00720000",
            "ts_utc": "2026-05-05T13:35:00Z",
            "side": "C",
            "size": 200,
            "premium": 200_000,
            "is_sweep": False,
            "aggressor": "A",
            "delta": 0.50,
            "underlying_mid": 720.0,
            "option_mid": 10.4,
            "quote_ts_utc": "2026-05-05T14:00:00Z",
        }),
    ])

    profiles = worker._contract_pricing_profiles(df)

    assert len(profiles) == 1
    assert profiles[0]["raw_symbol"] == "SPY   260505C00720000"


def _contract_state(mid: float, bid: float, ask: float) -> str:
    return json.dumps({
        "optionMid": mid,
        "bid": bid,
        "ask": ask,
        "asOfUtc": "2026-05-05T14:00:00Z",
        "quoteAgeMs": 500,
        "tradable": True,
        "executable": True,
        "tradabilityBucket": "tradable",
    })


def test_put_credit_open30_spread_publishes_paper_bet():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = set()
    worker.active_positions = {}
    worker.redis = FakeRedis({
        "options:live:vrp:SPY": json.dumps({"ivRank": 20, "asOf": "2026-05-05T14:00:00Z"}),
        "options:live:contract_state:SPY   260505P00715000": _contract_state(0.575, 0.55, 0.60),
        "options:live:contract_state:SPY   260505P00710000": _contract_state(0.175, 0.15, 0.20),
    })
    df = pl.DataFrame([
        {
            **row("2026-05-05T13:35:00Z", "C", 250_000),
            "raw_symbol": "SPY   260505C00725000",
            "delta": 0.50,
            "option_mid": 2.0,
            "quote_age_ms": 500,
        },
        {
            **row("2026-05-05T13:50:00Z", "P", 20_000),
            "raw_symbol": "SPY   260505P00715000",
            "delta": -0.16,
            "option_mid": 0.575,
            "quote_age_ms": 500,
        },
    ])

    asyncio.run(worker.evaluate_put_credit_open30_spread(df, "SPY", nw.DECISION_SLOTS[0], datetime(2026, 5, 5).date()))

    spread_payloads = [
        json.loads(value)
        for key, value in worker.redis.sets
        if key == "nexus_spread_overlay:SPY:etf_put_credit_open30_spread:10:00"
    ]
    assert len(spread_payloads) == 1
    payload = spread_payloads[0]
    assert payload["decision"] == "BET"
    assert payload["instrument_type"] == "vertical_credit_spread"
    assert payload["paper_only"] is True
    assert payload["entry_credit"] == 0.35
    assert payload["quote_freshness"] == "available"
    assert payload["spread"]["spread_type"] == "put_credit"
    assert payload["spread"]["max_loss_assumption"] == "AT_EXPIRATION_NO_ASSIGNMENT"
    assert [(leg["action"], leg["raw_symbol"]) for leg in payload["legs"]] == [
        ("SELL", "SPY260505P00715000"),
        ("BUY", "SPY260505P00710000"),
    ]
    assert payload["execution"]["price_reference"] == "net_credit"
    assert "SPY" not in worker.active_positions
    assert not worker.already_signaled(datetime(2026, 5, 5).date(), "SPY", "etf_open_specialist")
    assert worker.redis.xadds[-1][1]["redis_key"] == "nexus_spread_overlay:SPY:etf_put_credit_open30_spread:10:00"
    assert worker.redis.publishes[-1][0] == "signal:spread:etf_put_credit_open30_spread"


def test_put_credit_open30_spread_uses_live_contract_state_when_window_rows_lack_delta_and_option_mid():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = set()
    worker.active_positions = {}
    worker.redis = FakeRedis({
        "options:live:vrp:SPY": json.dumps({"ivRank": 20, "asOf": "2026-05-05T14:00:00Z"}),
        "options:live:contract_state:SPY260505P00715000": json.dumps({
            "optionMid": 0.575,
            "bid": 0.55,
            "ask": 0.60,
            "delta": -0.16,
            "asOfUtc": "2026-05-05T14:00:00Z",
            "quoteAgeMs": 500,
            "tradable": True,
            "executable": True,
            "tradabilityBucket": "tradable",
        }),
        "options:live:contract_state:SPY260505P00710000": json.dumps({
            "optionMid": 0.175,
            "bid": 0.15,
            "ask": 0.20,
            "delta": -0.10,
            "asOfUtc": "2026-05-05T14:00:00Z",
            "quoteAgeMs": 500,
            "tradable": True,
            "executable": True,
            "tradabilityBucket": "tradable",
        }),
    })
    df = pl.DataFrame([
        {
            **row("2026-05-05T13:35:00Z", "C", 250_000),
            "raw_symbol": "SPY   260505C00725000",
        },
        {
            **row("2026-05-05T13:50:00Z", "P", 20_000),
            "raw_symbol": "SPY   260505P00715000",
        },
    ])

    asyncio.run(worker.evaluate_put_credit_open30_spread(df, "SPY", nw.DECISION_SLOTS[0], datetime(2026, 5, 5).date()))

    spread_payloads = [
        json.loads(value)
        for key, value in worker.redis.sets
        if key == "nexus_spread_overlay:SPY:etf_put_credit_open30_spread:10:00"
    ]
    assert len(spread_payloads) == 1
    payload = spread_payloads[0]
    assert payload["decision"] == "BET"
    assert payload["spread"]["spread_type"] == "put_credit"
    assert payload["entry_credit"] == 0.35


def test_call_credit_open30_spread_is_spy_only_and_publishes():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = set()
    worker.active_positions = {}
    worker.redis = FakeRedis({
        "options:live:vrp:SPY": json.dumps({"ivRank": 20, "asOf": "2026-05-05T13:50:00Z"}),
        "options:live:contract_state:SPY260505C00725000": _contract_state(0.575, 0.55, 0.60),
        "options:live:contract_state:SPY260505C00730000": _contract_state(0.175, 0.15, 0.20),
    })
    df = pl.DataFrame([
        {
            **row("2026-05-05T13:35:00Z", "P", 250_000),
            "raw_symbol": "SPY   260505P00715000",
            "delta": -0.50,
            "option_mid": 2.0,
            "quote_age_ms": 500,
        },
        {
            **row("2026-05-05T13:50:00Z", "C", 20_000),
            "raw_symbol": "SPY   260505C00725000",
            "delta": 0.16,
            "option_mid": 0.575,
            "quote_age_ms": 500,
        },
    ])

    assert asyncio.run(worker.check_call_credit_open30_spread(df, "QQQ", nw.DECISION_SLOTS[0])) == (None, False)
    asyncio.run(worker.evaluate_call_credit_open30_spread(df, "SPY", nw.DECISION_SLOTS[0], datetime(2026, 5, 5).date()))

    payload = json.loads([
        value
        for key, value in worker.redis.sets
        if key == "nexus_spread_overlay:SPY:etf_call_credit_open30_spread:10:00"
    ][0])
    assert payload["sentiment"] == "BEARISH"
    assert payload["spread"]["spread_type"] == "call_credit"
    assert [(leg["action"], leg["raw_symbol"]) for leg in payload["legs"]] == [
        ("SELL", "SPY260505C00725000"),
        ("BUY", "SPY260505C00730000"),
    ]


def test_spread_candidate_rejects_low_credit():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = set()
    worker.active_positions = {}
    worker.redis = FakeRedis({
        "options:live:vrp:SPY": json.dumps({"ivRank": 20, "asOf": "2026-05-05T13:50:00Z"}),
        "options:live:contract_state:SPY260505P00715000": _contract_state(0.575, 0.25, 0.60),
        "options:live:contract_state:SPY260505P00710000": _contract_state(0.175, 0.15, 0.20),
    })
    df = pl.DataFrame([
        {
            **row("2026-05-05T13:35:00Z", "C", 250_000),
            "raw_symbol": "SPY   260505C00725000",
            "delta": 0.50,
            "option_mid": 2.0,
            "quote_age_ms": 500,
        },
        {
            **row("2026-05-05T13:50:00Z", "P", 20_000),
            "raw_symbol": "SPY   260505P00715000",
            "delta": -0.16,
            "option_mid": 0.575,
            "quote_age_ms": 500,
        },
    ])

    asyncio.run(worker.evaluate_put_credit_open30_spread(df, "SPY", nw.DECISION_SLOTS[0], datetime(2026, 5, 5).date()))

    assert not any(key.startswith("nexus_spread_overlay:") for key, _ in worker.redis.sets)


def test_spread_final_blocks_when_leg_quote_is_stale():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis()
    worker._log = lambda *args, **kwargs: None
    stale_quote = {
        "option_mid": 0.575,
        "option_bid": 0.55,
        "option_ask": 0.60,
        "quote_ts": "2026-05-05T13:55:00Z",
        "quote_age_ms": 20_000,
        "tradability_bucket": "tradable",
    }
    candidate = {
        "structure": "credit_spread",
        "spread_type": "put_credit",
        "right": "P",
        "short_raw_symbol": "SPY260505P00715000",
        "long_raw_symbol": "SPY260505P00710000",
        "short_strike": 715.0,
        "long_strike": 710.0,
        "expiry_date": "2026-05-05",
        "net_credit": 0.35,
        "strike_width": 5.0,
        "max_loss": 4.65,
        "max_loss_assumption": "AT_EXPIRATION_NO_ASSIGNMENT",
        "legs": [
            {
                "raw_symbol": "SPY260505P00715000",
                "action": "SELL",
                "ratio": -1,
                "side": "P",
                "strike": 715.0,
                "expiry_date": "2026-05-05",
                "quote": stale_quote,
            },
            {
                "raw_symbol": "SPY260505P00710000",
                "action": "BUY",
                "ratio": 1,
                "side": "P",
                "strike": 710.0,
                "expiry_date": "2026-05-05",
                "quote": stale_quote,
            },
        ],
    }

    asyncio.run(worker._publish_spread_final(
        "etf_put_credit_open30_spread",
        "SPY",
        "BULLISH",
        1.0,
        datetime(2026, 5, 5).date(),
        nw.DECISION_SLOTS[0],
        candidate,
    ))

    assert not any(key.startswith("nexus_spread_overlay:") for key, _ in worker.redis.sets)
    block = json.loads(worker.redis.values["nexus_final_block:SPY:etf_put_credit_open30_spread:10:00"])
    assert block["decision"] == "FINAL_BLOCKED"
    assert block["confidence"] == 1.0
    assert block["reason"] == "strategy_spread_final_blocked_by_quote"
    assert worker.redis.publishes[-1][0] == "signal:final_block:etf_put_credit_open30_spread"


def test_spread_open30_rejects_high_iv_rank():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis({"options:live:vrp:SPY": json.dumps({"ivRank": 35, "asOf": "2026-05-05T13:50:00Z"})})
    df = pl.DataFrame([
        {
            **row("2026-05-05T13:35:00Z", "C", 250_000),
            "raw_symbol": "SPY   260505C00725000",
            "delta": 0.50,
            "option_mid": 2.0,
            "quote_age_ms": 500,
        },
        {
            **row("2026-05-05T13:50:00Z", "P", 20_000),
            "raw_symbol": "SPY   260505P00715000",
            "delta": -0.16,
            "option_mid": 0.575,
            "quote_age_ms": 500,
        },
    ])

    assert asyncio.run(worker.check_put_credit_open30_spread(df, "SPY", nw.DECISION_SLOTS[0])) == (None, False)


def test_enrich_trade_payload_from_redis_merges_underlying_and_contract_state():
    payload = nw.normalize_trade_payload({
        "symbol": "SPY",
        "raw_symbol": "SPY   260505C00700000",
        "ts_utc": "2026-05-05T14:00:00Z",
        "side": "C",
        "price": 1.25,
        "size": 10,
        "premium": 1250.0,
    })
    redis_client = FakeRedis({
        "equity:live:context:SPY": json.dumps({"price": 700.25, "lastPriceUtc": "2026-05-05T14:00:00Z"}),
        "options:live:contract_state:SPY   260505C00700000": json.dumps({
            "mid": 1.24,
            "underlyingMid": 700.30,
            "quoteAgeMs": 250,
            "delta": 0.52,
            "gamma": 0.018,
            "asOfUtc": "2026-05-05T14:00:00Z",
        }),
    })

    enriched = asyncio.run(nw.enrich_trade_payload_from_redis(payload, redis_client))

    assert enriched["underlying_mid"] == 700.30
    assert enriched["option_mid"] == 1.24
    assert enriched["delta"] == 0.52
    assert enriched["gamma"] == 0.018
    assert enriched["_feature_status"]["underlying_mid"] == "available"
    assert enriched["_feature_status"]["option_mid"] == "available"


def test_runtime_gate_blocks_low_sweep_when_sweep_classifier_is_missing():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = set()
    worker.redis = FakeRedis()
    payload = {
        "underlying": "SPY",
        "raw_symbol": "SPY   260505C00700000",
        "price": 12.5,
        "size": 200,
        "ts_event_ns": 1_778_012_099_996_787_889,
    }
    df = pl.DataFrame([nw.decode_stream_entry({b"data": msgpack.packb(payload, use_bin_type=True)})])

    asyncio.run(worker.evaluate_low_sweep_core(df, "SPY", nw.DECISION_SLOTS[0], datetime(2026, 5, 5).date()))

    assert worker.redis.sets[0][0] == "nexus_window_view:SPY:etf_low_sweep_core:10:00"
    assert len(worker.redis.xadds) == 1
    blocked = json.loads(worker.redis.xadds[0][1]["payload_json"])
    assert blocked["decision"] == "BLOCKED"
    assert blocked["block_reason"] == "live_feature_quality_gate_closed"
    assert blocked["missing_features"] == ["is_sweep"]
    assert blocked["feature_failures"] == {"is_sweep": "missing"}
    assert [gate["name"] for gate in blocked["gates"]] == [
        "strategy_window_applicability",
        "live_feature_quality",
    ]
    feature_gate = next(gate for gate in blocked["gates"] if gate["name"] == "live_feature_quality")
    assert feature_gate["severity"] == "hard"
    assert feature_gate["status"] == "failed"
    assert feature_gate["hard_block"] is True
    assert feature_gate["reason"] == "live_feature_quality_gate_closed"
    assert feature_gate["details"]["missing_features"] == ["is_sweep"]
    assert json.loads(worker.redis.sets[0][1]) == blocked
    assert worker.redis.publishes[0][0] == "signal:window_view:etf_low_sweep_core"


def test_runtime_gate_blocks_flow_when_live_context_is_missing():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = set()
    worker.redis = FakeRedis()
    df = pl.DataFrame([
        {
            **row("2026-05-05T14:00:00Z", "C", 250_000, sweep=True),
            "raw_symbol": "SPY   260505C00700000",
        }
    ])

    asyncio.run(worker.evaluate_flow_specialist(df, "SPY", nw.DECISION_SLOTS[1], datetime(2026, 5, 5).date()))

    assert worker.redis.sets[0][0] == "nexus_window_view:SPY:etf_flow_specialist:10:30"
    blocked = json.loads(worker.redis.xadds[0][1]["payload_json"])
    assert blocked["decision"] == "BLOCKED"
    assert blocked["missing_features"] == ["atm_iv", "iv_rank", "net_gex"]


def test_runtime_gate_blocks_confluence_without_option_mid():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = set()
    worker.redis = FakeRedis({"options:live:vrp:SPY": json.dumps({"ivRank": 20, "asOf": "2026-05-05T14:00:00Z"})})
    df = pl.DataFrame([
        {
            **row("2026-05-05T14:00:00Z", "C", 250_000, sweep=True),
            "raw_symbol": "SPY   260505C00700000",
            "price": 1.25,
            "underlying_mid": 700.0,
        }
    ])

    asyncio.run(worker.evaluate_confluence_sniper(df, "SPY", nw.DECISION_SLOTS[1], datetime(2026, 5, 5).date()))

    assert worker.redis.sets[0][0] == "nexus_window_view:SPY:etf_confluence_sniper:10:30"
    blocked = json.loads(worker.redis.xadds[0][1]["payload_json"])
    assert blocked["decision"] == "BLOCKED"
    assert blocked["missing_features"] == ["option_mid"]
    assert blocked["feature_failures"] == {"option_mid": "missing"}


def test_runtime_gate_blocks_stale_option_mid_for_confluence():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = set()
    worker.redis = FakeRedis({"options:live:vrp:SPY": json.dumps({"ivRank": 20, "asOf": "2026-05-05T14:00:00Z"})})
    payload = {
        "symbol": "SPY",
        "raw_symbol": "SPY   260505C00700000",
        "ts_utc": "2026-05-05T14:00:00Z",
        "side": "C",
        "price": 1.25,
        "size": 10,
        "premium": 250_000,
        "is_sweep": True,
        "aggressor": "A",
        "delta": 0.52,
        "gamma": 0.018,
        "greeks_ts_utc": "2026-05-05T14:00:00Z",
        "underlying_mid": 700.25,
        "underlying_ts_utc": "2026-05-05T14:00:00Z",
        "option_mid": 1.24,
        "quote_age_ms": 20_000,
    }
    df = pl.DataFrame([nw.normalize_trade_payload(payload)])

    asyncio.run(worker.evaluate_confluence_sniper(df, "SPY", nw.DECISION_SLOTS[1], datetime(2026, 5, 5).date()))

    blocked = json.loads(worker.redis.xadds[0][1]["payload_json"])
    assert blocked["decision"] == "BLOCKED"
    assert blocked["feature_failures"] == {"option_mid": "stale"}


def test_runtime_gate_blocks_stale_live_context_for_flow():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = set()
    worker.redis = FakeRedis({
        "options:live:iv_surface:SPY": json.dumps({"atmIv": 0.22, "asOf": "2026-05-05T13:55:00Z"}),
        "options:live:gex:SPY": json.dumps({"netGex": -1_500_000_000, "asOf": "2026-05-05T13:55:00Z"}),
        "options:live:vrp:SPY": json.dumps({"ivRank": 20, "asOf": "2026-05-05T13:55:00Z"}),
    })
    payload = {
        "symbol": "SPY",
        "raw_symbol": "SPY   260505C00700000",
        "ts_utc": "2026-05-05T14:00:00Z",
        "side": "C",
        "price": 1.25,
        "size": 10,
        "premium": 250_000,
        "is_sweep": True,
        "aggressor": "A",
        "delta": 0.52,
        "gamma": 0.018,
        "greeks_ts_utc": "2026-05-05T14:00:00Z",
    }
    df = pl.DataFrame([nw.normalize_trade_payload(payload)])

    asyncio.run(worker.evaluate_flow_specialist(df, "SPY", nw.DECISION_SLOTS[1], datetime(2026, 5, 5).date()))

    blocked = json.loads(worker.redis.xadds[0][1]["payload_json"])
    assert blocked["decision"] == "BLOCKED"
    assert blocked["feature_failures"] == {"atm_iv": "stale", "iv_rank": "stale", "net_gex": "stale"}


def test_runtime_gate_accepts_fresh_live_context_for_flow():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis({
        "options:live:iv_surface:SPY": json.dumps({"atmIv": 0.22, "asOf": "2026-05-05T14:00:00Z"}),
        "options:live:gex:SPY": json.dumps({"netGex": -1_500_000_000, "asOf": "2026-05-05T14:00:00Z"}),
        "options:live:vrp:SPY": json.dumps({"ivRank": 20, "asOf": "2026-05-05T14:00:00Z"}),
    })
    payload = {
        "symbol": "SPY",
        "raw_symbol": "SPY   260505C00700000",
        "ts_utc": "2026-05-05T14:00:00Z",
        "side": "C",
        "price": 1.25,
        "size": 10,
        "premium": 250_000,
        "is_sweep": True,
        "aggressor": "A",
        "delta": 0.52,
        "gamma": 0.018,
        "greeks_ts_utc": "2026-05-05T14:00:00Z",
    }
    df = pl.DataFrame([nw.normalize_trade_payload(payload)])

    assert asyncio.run(worker._missing_strategy_features("etf_flow_specialist", "SPY", df)) == []


def test_enrich_trade_payload_uses_equity_context_and_contract_tradability():
    raw_symbol = "SPY   260505C00700000"
    redis = FakeRedis({
        "equity:live:context:SPY": json.dumps({
            "symbol": "SPY",
            "price": 700.25,
            "lastPriceUtc": "2026-05-05T14:00:00Z",
            "warmupComplete": True,
            "priceDataStale": False,
        }),
        f"options:live:tradability:{raw_symbol}": json.dumps({
            "symbol": "SPY",
            "rawSymbol": raw_symbol,
            "asOfUtc": "2026-05-05T14:00:00Z",
            "available": True,
            "stale": False,
            "tradable": True,
            "executable": True,
            "tradabilityScore": 0.91,
            "tradabilityBucket": "tradable",
            "option": {
                "bid": 1.20,
                "ask": 1.28,
                "mid": 1.24,
                "bidSize": 25,
                "askSize": 31,
                "spreadPct": 0.064516,
            },
        }),
    })
    payload = nw.normalize_trade_payload({
        "symbol": "SPY",
        "raw_symbol": raw_symbol,
        "ts_utc": "2026-05-05T14:00:00Z",
        "side": "C",
        "price": 1.25,
        "size": 10,
        "premium": 1_250.0,
        "is_sweep": True,
        "aggressor": "A",
    })

    enriched = asyncio.run(nw.enrich_trade_payload_from_redis(payload, redis))

    assert enriched["underlying_mid"] == 700.25
    assert enriched["underlying_ts_utc"] == "2026-05-05T14:00:00Z"
    assert enriched["option_mid"] == 1.24
    assert enriched["quote_ts_utc"] == "2026-05-05T14:00:00Z"
    assert enriched["tradability_bucket"] == "tradable"
    assert enriched["_feature_status"]["underlying_mid"] == "available"
    assert enriched["_feature_status"]["option_mid"] == "available"
    assert enriched["_feature_status"]["delta"] == "missing"


def test_normalize_trade_payload_keeps_missing_greeks_missing():
    payload = nw.normalize_trade_payload({
        "symbol": "SPY",
        "raw_symbol": "SPY   260505C00700000",
        "ts_utc": "2026-05-05T14:00:00Z",
        "side": "C",
        "price": 1.25,
        "size": 10,
        "premium": 1_250.0,
        "is_sweep": True,
        "aggressor": "A",
    })

    assert "delta" not in payload
    assert "gamma" not in payload
    assert payload["_feature_status"]["delta"] == "missing"
    assert payload["_feature_status"]["gamma"] == "missing"


def test_enrich_trade_payload_uses_compact_contract_key_for_padded_raw_symbol():
    raw_symbol = "SPY   260505C00700000"
    redis = FakeRedis({
        "equity:live:context:SPY": json.dumps({
            "price": 700.25,
            "lastPriceUtc": "2026-05-05T14:00:00Z",
            "warmupComplete": True,
            "priceDataStale": False,
        }),
        "options:live:contract_state:SPY260505C00700000": json.dumps({
            "symbol": "SPY",
            "rawSymbol": "SPY260505C00700000",
            "asOfUtc": "2026-05-05T14:00:00Z",
            "optionMid": 1.24,
            "bid": 1.20,
            "ask": 1.28,
            "tradable": True,
            "executable": True,
            "tradabilityBucket": "tradable",
            "delta": 0.52,
            "gamma": 0.018,
            "greeksTsUtc": "2026-05-05T14:00:00Z",
        }),
    })
    payload = nw.normalize_trade_payload({
        "symbol": "SPY",
        "raw_symbol": raw_symbol,
        "ts_utc": "2026-05-05T14:00:00Z",
        "side": "C",
        "price": 1.25,
        "size": 10,
        "premium": 1_250.0,
        "is_sweep": True,
        "aggressor": "A",
    })

    enriched = asyncio.run(nw.enrich_trade_payload_from_redis(payload, redis))

    assert enriched["option_mid"] == 1.24
    assert enriched["delta"] == 0.52
    assert enriched["gamma"] == 0.018
    assert enriched["_feature_status"]["option_mid"] == "available"
    assert enriched["_feature_status"]["delta"] == "available"


def test_normalize_trade_payload_derives_aggressor_and_sweep_from_fresh_quote():
    payload = {
        "symbol": "SPY",
        "raw_symbol": "SPY   260505C00700000",
        "ts_utc": "2026-05-05T14:00:00Z",
        "side": "C",
        "price": 1.28,
        "size": 250,
        "option_bid": 1.20,
        "option_ask": 1.28,
        "option_mid": 1.24,
        "quote_ts_utc": "2026-05-05T14:00:00Z",
        "option_tradable": True,
        "option_executable": True,
        "tradability_bucket": "tradable",
    }

    normalized = nw.normalize_trade_payload(payload)

    assert normalized["aggressor"] == "A"
    assert normalized["is_sweep"] is True
    assert normalized["_feature_status"]["aggressor"] == "derived"
    assert normalized["_feature_status"]["is_sweep"] == "derived"


def test_raw_prod_shape_enriches_aggressor_sweep_from_contract_state():
    raw_symbol = "SPY   260505C00700000"
    redis = FakeRedis({
        "equity:live:context:SPY": json.dumps({
            "price": 700.25,
            "lastPriceUtc": "2026-05-05T14:00:00Z",
            "warmupComplete": True,
            "priceDataStale": False,
        }),
        f"options:live:contract_state:{raw_symbol}": json.dumps({
            "symbol": "SPY",
            "rawSymbol": raw_symbol,
            "asOfUtc": "2026-05-05T14:00:00Z",
            "available": True,
            "stale": False,
            "tradable": True,
            "executable": True,
            "bid": 1.20,
            "ask": 1.28,
            "optionMid": 1.24,
            "tradabilityBucket": "tradable",
            "underlyingMid": 700.25,
            "delta": 0.52,
            "gamma": 0.018,
            "greeksTsUtc": "2026-05-05T14:00:00Z",
        }),
    })
    raw_payload = {
        "underlying": "SPY",
        "raw_symbol": raw_symbol,
        "price": 1.28,
        "size": 250,
        "ts_event_ns": int(datetime(2026, 5, 5, 14, 0, tzinfo=timezone.utc).timestamp() * 1_000_000_000),
    }

    decoded = nw.decode_stream_entry({b"data": msgpack.packb(raw_payload, use_bin_type=True)})
    enriched = asyncio.run(nw.enrich_trade_payload_from_redis(decoded, redis))

    assert enriched["aggressor"] == "A"
    assert enriched["is_sweep"] is True
    assert enriched["_feature_status"]["aggressor"] == "derived"
    assert enriched["_feature_status"]["is_sweep"] == "derived"
    assert enriched["_feature_status"]["delta"] == "available"
    assert enriched["_feature_status"]["gamma"] == "available"


def test_feature_audit_marks_quote_derived_aggressor_and_sweep_ready():
    payload = nw.normalize_trade_payload({
        "symbol": "SPY",
        "raw_symbol": "SPY   260505C00700000",
        "ts_utc": "2026-05-05T14:00:00Z",
        "side": "C",
        "price": 1.28,
        "size": 250,
        "option_bid": 1.20,
        "option_ask": 1.28,
        "option_mid": 1.24,
        "quote_ts_utc": "2026-05-05T14:00:00Z",
        "option_tradable": True,
        "option_executable": True,
        "tradability_bucket": "tradable",
        "delta": 0.52,
        "gamma": 0.018,
        "greeks_ts_utc": "2026-05-05T14:00:00Z",
        "underlying_mid": 700.25,
        "underlying_ts_utc": "2026-05-05T14:00:00Z",
    })

    audit = fa.audit_payload(payload, {"iv_rank": 24.0, "atm_iv": 0.18, "net_gex": 1_000_000, "asOf": "2026-05-05T14:00:00Z"})

    assert audit["features"]["aggressor"]["status"] == "derived"
    assert audit["features"]["is_sweep"]["status"] == "derived"
    assert {result["status"] for result in audit["strategies"].values()} == {"ready"}


def test_enrich_trade_payload_blocks_untradable_contract_mid():
    raw_symbol = "SPY   260505C00700000"
    redis = FakeRedis({
        f"options:live:tradability:{raw_symbol}": json.dumps({
            "symbol": "SPY",
            "rawSymbol": raw_symbol,
            "asOfUtc": "2026-05-05T14:00:00Z",
            "available": True,
            "stale": False,
            "tradable": False,
            "executable": False,
            "tradabilityBucket": "reject",
            "option": {"mid": 1.24, "bid": 1.00, "ask": 1.48},
        }),
    })
    payload = nw.normalize_trade_payload({
        "symbol": "SPY",
        "raw_symbol": raw_symbol,
        "ts_utc": "2026-05-05T14:00:00Z",
        "side": "C",
        "price": 1.25,
        "size": 10,
        "premium": 1_250.0,
        "is_sweep": True,
        "aggressor": "A",
    })

    enriched = asyncio.run(nw.enrich_trade_payload_from_redis(payload, redis))

    assert enriched["option_mid"] == 1.24
    assert enriched["_feature_status"]["option_mid"] == "untradable"


def test_contract_state_can_satisfy_confluence_required_features():
    raw_symbol = "SPY   260505C00700000"
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis({
        "options:live:vrp:SPY": json.dumps({"ivRank": 20, "asOf": "2026-05-05T14:00:00Z"}),
    })
    redis = FakeRedis({
        "equity:live:context:SPY": json.dumps({
            "price": 700.25,
            "lastPriceUtc": "2026-05-05T14:00:00Z",
            "warmupComplete": True,
            "priceDataStale": False,
        }),
        f"options:live:contract_state:{raw_symbol}": json.dumps({
            "symbol": "SPY",
            "rawSymbol": raw_symbol,
            "asOf": "2026-05-05T14:00:00Z",
            "optionMid": 1.24,
            "tradable": True,
            "executable": True,
            "tradabilityBucket": "tradable",
            "delta": 0.52,
            "gamma": 0.018,
            "greeksTsUtc": "2026-05-05T14:00:00Z",
        }),
    })
    payload = nw.normalize_trade_payload({
        "symbol": "SPY",
        "raw_symbol": raw_symbol,
        "ts_utc": "2026-05-05T14:00:00Z",
        "side": "C",
        "price": 1.25,
        "size": 10,
        "premium": 1_250.0,
        "is_sweep": True,
        "aggressor": "A",
    })
    enriched = asyncio.run(nw.enrich_trade_payload_from_redis(payload, redis))

    assert asyncio.run(worker._missing_strategy_features("etf_confluence_sniper", "SPY", pl.DataFrame([enriched]))) == []


def test_runtime_gate_allows_enriched_low_sweep_signal():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = set()
    worker.active_positions = {}
    worker.redis = FakeRedis({
        "options:live:contract_state:SPY   260505C00700000": json.dumps({
            "optionMid": 12.4,
            "bid": 12.3,
            "ask": 12.5,
            "asOfUtc": "2026-05-05T13:35:00Z",
            "quoteAgeMs": 100,
            "tradable": True,
            "executable": True,
            "tradabilityBucket": "tradable",
        }),
    })
    payload = {
        "symbol": "SPY",
        "raw_symbol": "SPY   260505C00700000",
        "ts_utc": "2026-05-05T13:35:00Z",
        "side": "C",
        "price": 12.5,
        "size": 200,
        "premium": 250_000,
        "is_sweep": False,
        "aggressor": "A",
        "delta": 0.50,
        "gamma": 0.01,
    }
    df = pl.DataFrame([nw.normalize_trade_payload(payload)])

    asyncio.run(worker.evaluate_low_sweep_core(df, "SPY", nw.DECISION_SLOTS[0], datetime(2026, 5, 5).date()))

    overlay_sets = [(key, value) for key, value in worker.redis.sets if key == "nexus_live_overlay:SPY"]
    assert len(overlay_sets) == 1
    assert len(worker.redis.xadds) == 3
    contract_refs = [fields for _, fields, _, _ in worker.redis.xadds if fields.get("source") == "sigmatiq_nexus_contract_reference"]
    assert [fields["redis_key"] for fields in contract_refs] == ["options:live:contract_state:SPY   260505C00700000"]
    assert json.loads(contract_refs[0]["payload_json"])["symbol"] == "SPY"
    assert json.loads(contract_refs[0]["payload_json"])["raw_symbol"] == "SPY   260505C00700000"
    assert worker.redis.sets[0][0] == "nexus_intermediate:SPY:etf_low_sweep_core:10:00"
    final = json.loads(overlay_sets[0][1])
    assert final["strategy"] == "etf_low_sweep_core"
    assert final["decision"] == "BET"
    assert final["raw_symbol"] == "SPY   260505C00700000"


def test_active_position_mid_uses_tracked_contract_not_unrelated_symbol_trade():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = set()
    worker.active_positions = {
        "SPY": {
            "entry_price": 10.0,
            "is_guarded": False,
            "side": "BULLISH",
            "raw_symbol": "SPY   260505C00700000",
            "signal_id": "sig_test_position",
            "position_id": "sig_test_position",
        }
    }
    worker.redis = FakeRedis({
        "options:live:contract_state:SPY   260505C00700000": json.dumps({
            "optionMid": 4.9,
            "asOfUtc": "2026-05-05T14:06:00Z",
            "tradable": True,
        }),
    })
    worker.last_reset_session_date = datetime(2026, 5, 5).date()
    worker.buffers = {"SPY": deque(maxlen=10)}
    worker.feature_blocks_reported = set()
    worker.window_views_reported = set()
    worker.window_pricing_reported = set()

    unrelated_payload = {
        "symbol": "SPY",
        "raw_symbol": "SPY   260505C00710000",
        "ts_utc": "2026-05-05T14:06:00Z",
        "side": "C",
        "price": 2.0,
        "size": 10,
        "premium": 2_000.0,
        "is_sweep": False,
        "aggressor": "A",
        "delta": 0.50,
        "gamma": 0.01,
        "option_mid": 20.0,
    }

    asyncio.run(worker.process_message({"payload": json.dumps(unrelated_payload)}))

    assert "SPY" not in worker.active_positions
    liquidate = next(json.loads(value) for _, value in worker.redis.sets if json.loads(value).get("decision") == "LIQUIDATE")
    assert liquidate["decision"] == "LIQUIDATE"
    assert liquidate["reason"] == "STOP_LOSS"
    assert liquidate["signal_id"] == "sig_test_position"
    assert liquidate["position_id"] == "sig_test_position"
    assert liquidate["raw_symbol"] == "SPY   260505C00700000"
    assert liquidate["exit_price"] == 4.9
    assert liquidate["quote_freshness"] == "stale"
    assert liquidate["quote_valid_until"] == "2026-05-05T14:06:15+00:00"
    assert liquidate["execution"]["order_type"] == "limit"
    assert liquidate["execution"]["reference_price"] == 4.9
    assert liquidate["execution"]["quote_freshness"] == "stale"


def test_process_message_waits_for_window_grace_before_evaluating(monkeypatch):
    monkeypatch.setattr(nw, "WINDOW_EVALUATION_GRACE_SECONDS", 15)
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = set()
    worker.active_positions = {}
    worker.redis = FakeRedis()
    worker.last_reset_session_date = datetime(2026, 5, 5).date()
    worker.buffers = {"SPY": deque(maxlen=10)}
    worker.feature_blocks_reported = set()
    worker.window_views_reported = set()
    worker.window_pricing_reported = set()
    worker.evaluated_windows = set()
    calls = []

    async def fake_evaluate(symbol, slot, event_dt_utc):
        calls.append((symbol, slot["entry_label"]))

    worker.evaluate_strategy = fake_evaluate
    payload_before_grace = {
        "symbol": "SPY",
        "raw_symbol": "SPY   260505C00700000",
        "ts_utc": "2026-05-05T14:00:05Z",
        "side": "C",
        "price": 1.0,
        "size": 1,
        "premium": 100.0,
    }
    payload_after_grace = {**payload_before_grace, "ts_utc": "2026-05-05T14:00:16Z"}

    asyncio.run(worker.process_message({"payload": json.dumps(payload_before_grace)}))
    asyncio.run(worker.process_message({"payload": json.dumps(payload_after_grace)}))

    assert calls == [("SPY", "10:00")]
    assert nw.window_eval_key(datetime(2026, 5, 5).date(), "SPY", "10:00") in worker.evaluated_windows


def test_market_context_windows_continue_after_strategy_windows():
    assert nw.MARKET_CONTEXT_WINDOWS[-1]["entry_label"] == "w1600_1615"
    assert nw.MARKET_CONTEXT_WINDOWS[-1]["window_end"].isoformat() == "16:15:00"
    assert nw.DECISION_SLOTS[-1]["entry_label"] == "12:00"


def test_publish_option_market_context_sets_latest_and_publishes():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis()
    worker.option_market_context_reported = set()
    worker.late_window_impacts = {}
    worker.buffers = {"SPY": deque(maxlen=10)}
    rows = [
        {
            "ts_utc": "2026-05-05T13:35:00Z",
            "symbol": "SPY",
            "raw_symbol": "SPY   260505C00700000",
            "side": "C",
            "premium": 300_000.0,
            "is_sweep": True,
            "underlying_mid": 700.0,
            "delta": 0.5,
            "option_mid": 10.0,
            "option_bid": 9.9,
            "option_ask": 10.1,
            "quote_ts_utc": "2026-05-05T13:35:00Z",
        },
        {
            "ts_utc": "2026-05-05T13:42:00Z",
            "symbol": "SPY",
            "raw_symbol": "SPY   260505C00700000",
            "side": "C",
            "premium": 150_000.0,
            "is_sweep": False,
            "underlying_mid": 702.0,
            "delta": 0.5,
            "option_mid": 10.7,
            "option_bid": 10.6,
            "option_ask": 10.8,
            "quote_ts_utc": "2026-05-05T13:42:00Z",
        },
        {
            "ts_utc": "2026-05-05T13:50:00Z",
            "symbol": "SPY",
            "raw_symbol": "SPY   260505P00695000",
            "side": "P",
            "premium": 50_000.0,
            "is_sweep": False,
            "underlying_mid": 702.0,
            "delta": -0.4,
            "option_mid": 7.0,
            "option_bid": 6.9,
            "option_ask": 7.1,
        },
    ]
    worker.buffers["SPY"].extend(rows)

    asyncio.run(worker.publish_option_market_context_for_slot("SPY", nw.MARKET_CONTEXT_WINDOWS[0], datetime(2026, 5, 5).date()))

    key = "nexus_option_market_context:SPY:w0930_1000"
    assert key in worker.redis.values
    assert "nexus_option_market_context:SPY:latest" in worker.redis.values
    assert worker.redis.expires[key] == 48 * 3600
    assert worker.redis.expires["nexus_option_market_context:SPY:latest"] == 8 * 3600
    msg = json.loads(worker.redis.values[key])
    assert msg["schema_version"] == nw.NEXUS_SCHEMA_VERSION
    assert msg["worker_name"] == nw.NEXUS_WORKER_NAME
    assert msg["worker_version"] == nw.NEXUS_WORKER_VERSION
    assert msg["session_date"] == "2026-05-05"
    assert msg["window_id"] == "w0930_1000"
    assert msg["premium"]["net_premium_bias"] == "call_heavy"
    assert msg["activity"]["trade_count"] == 3
    assert msg["activity"]["contract_count"] == 2
    assert msg["pricing_profile_count"] == 1
    assert msg["most_traded_contracts"][0]["raw_symbol"] == "SPY   260505C00700000"
    assert msg["cheap_side"] == "balanced"
    assert msg["pricing_quality"] == "degraded"
    assert msg["pricing_quality_reason"] == "pricing_lag_range_too_small"
    assert msg["data_quality"]["status"] == "degraded"
    assert "PRICING_QUALITY_DEGRADED" in msg["data_quality"]["reason_codes"]
    assert "pricing_quality_degraded" in msg["data_quality"]["degraded"]
    assert worker.redis.xadds[0][1]["redis_key"] == key
    assert worker.redis.publishes[0][0] == "signal:option_market_context"
    assert ("2026-05-05", "SPY", "w0930_1000") in worker.option_market_context_reported


def test_publish_option_market_context_marks_missing_pricing_profiles():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis()
    worker.option_market_context_reported = set()
    worker.late_window_impacts = {}
    worker.buffers = {"SPY": deque(maxlen=10)}
    rows = [
        {
            "ts_utc": "2026-05-05T13:35:00Z",
            "symbol": "SPY",
            "raw_symbol": "SPY   260505C00700000",
            "side": "C",
            "premium": 300_000.0,
            "is_sweep": True,
            "underlying_mid": 700.0,
            "delta": 0.5,
            "option_mid": 10.0,
        },
        {
            "ts_utc": "2026-05-05T13:42:00Z",
            "symbol": "SPY",
            "raw_symbol": "SPY   260505C00700000",
            "side": "C",
            "premium": 150_000.0,
            "is_sweep": False,
            "underlying_mid": 702.0,
            "delta": 0.5,
            "option_mid": 10.7,
        },
    ]
    worker.buffers["SPY"].extend(rows)

    asyncio.run(worker.publish_option_market_context_for_slot("SPY", nw.MARKET_CONTEXT_WINDOWS[0], datetime(2026, 5, 5).date()))

    msg = json.loads(worker.redis.values["nexus_option_market_context:SPY:w0930_1000"])
    assert msg["pricing_profile_count"] == 0
    assert msg["pricing_quality"] == "unknown"
    assert msg["pricing_quality_reason"] == "no_reliable_pricing_profiles"
    assert msg["data_quality"]["status"] == "unknown"
    assert "pricing_profiles" in msg["data_quality"]["missing"]
    assert "NO_RELIABLE_PRICING_PROFILES" in msg["data_quality"]["reason_codes"]


def test_publish_option_market_context_does_not_mark_reported_when_write_fails():
    class FailingRedis(FakeRedis):
        async def set(self, key, value, nx=False, ex=None):
            raise RuntimeError("redis unavailable")

    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FailingRedis()
    worker.option_market_context_reported = set()
    worker.late_window_impacts = {}
    worker.buffers = {"SPY": deque(maxlen=10)}
    worker.buffers["SPY"].append(
        {
            "ts_utc": "2026-05-05T13:35:00Z",
            "symbol": "SPY",
            "raw_symbol": "SPY   260505C00700000",
            "side": "C",
            "premium": 300_000.0,
            "is_sweep": True,
            "underlying_mid": 700.0,
            "delta": 0.5,
            "option_mid": 10.0,
            "option_bid": 9.9,
            "option_ask": 10.1,
            "quote_ts_utc": "2026-05-05T13:35:00Z",
        }
    )

    try:
        asyncio.run(worker.publish_option_market_context_for_slot("SPY", nw.MARKET_CONTEXT_WINDOWS[0], date(2026, 5, 5)))
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected Redis write failure")

    assert ("2026-05-05", "SPY", "w0930_1000") not in worker.option_market_context_reported


def test_late_event_after_window_evaluation_publishes_audit_event():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = set()
    worker.active_positions = {}
    worker.redis = FakeRedis()
    worker.last_reset_session_date = datetime(2026, 5, 5).date()
    worker.buffers = {"SPY": deque(maxlen=10)}
    worker.feature_blocks_reported = set()
    worker.window_views_reported = set()
    worker.window_pricing_reported = set()
    worker.evaluated_windows = {nw.window_eval_key(datetime(2026, 5, 5).date(), "SPY", "10:00")}
    worker.late_window_impacts = {}

    payload = {
        "symbol": "SPY",
        "raw_symbol": "SPY   260505P00700000",
        "ts_utc": "2026-05-05T13:59:58Z",
        "side": "P",
        "price": 15.0,
        "size": 200,
        "premium": 300_000.0,
    }

    asyncio.run(worker.process_message({"payload": json.dumps(payload)}))

    key = "nexus_window_late_event:SPY:10:00"
    msg = json.loads(worker.redis.values[key])
    assert msg["decision"] == "WINDOW_LATE_EVENT"
    assert msg["entry_time"] == "10:00"
    assert msg["raw_symbol"] == "SPY   260505P00700000"
    assert msg["side"] == "P"
    assert msg["late_event_count"] == 1
    assert msg["late_put_premium"] == 300_000.0
    assert worker.redis.xadds[0][1]["redis_key"] == key
    assert worker.redis.publishes[0][0] == "signal:window_late_event"


def test_event_before_window_evaluation_does_not_publish_late_audit():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = set()
    worker.active_positions = {}
    worker.redis = FakeRedis()
    worker.last_reset_session_date = datetime(2026, 5, 5).date()
    worker.buffers = {"SPY": deque(maxlen=10)}
    worker.feature_blocks_reported = set()
    worker.window_views_reported = set()
    worker.window_pricing_reported = set()
    worker.evaluated_windows = set()
    worker.late_window_impacts = {}

    payload = {
        "symbol": "SPY",
        "raw_symbol": "SPY   260505C00700000",
        "ts_utc": "2026-05-05T13:59:58Z",
        "side": "C",
        "price": 10.0,
        "size": 200,
        "premium": 200_000.0,
    }

    asyncio.run(worker.process_message({"payload": json.dumps(payload)}))

    assert "nexus_window_late_event:SPY:10:00" not in worker.redis.values
    assert worker.redis.xadds == []
    assert worker.redis.publishes == []


def test_evaluate_strategy_publishes_window_views_even_when_trade_locked():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = {nw.signal_key(datetime(2026, 5, 5).date(), "SPY", "*")}
    worker.window_views_reported = set()
    worker.feature_blocks_reported = set()
    worker.active_positions = {}
    worker.redis = FakeRedis({
        "options:live:vrp:SPY": json.dumps({"ivRank": 20, "asOf": "2026-05-05T14:05:00Z"}),
        "options:live:iv_surface:SPY": json.dumps({"atmIv": 0.22, "asOf": "2026-05-05T14:05:00Z"}),
        "options:live:gex:SPY": json.dumps({"netGex": 1_000_000, "asOf": "2026-05-05T14:05:00Z"}),
    })
    worker.buffers = {
        "SPY": [
            nw.normalize_trade_payload({
                "symbol": "SPY",
                "raw_symbol": "SPY   260505C00700000",
                "ts_utc": "2026-05-05T13:35:00Z",
                "side": "C",
                "price": 12.5,
                "size": 200,
                "premium": 250_000,
                "is_sweep": False,
                "aggressor": "A",
                "delta": 0.50,
                "gamma": 0.01,
                "greeks_ts_utc": "2026-05-05T13:35:00Z",
                "underlying_mid": 700.25,
                "underlying_ts_utc": "2026-05-05T13:35:00Z",
                "option_mid": 12.4,
                "quote_age_ms": 100,
            })
        ]
    }

    asyncio.run(worker.evaluate_strategy("SPY", nw.DECISION_SLOTS[0], datetime(2026, 5, 5, 14, 5, tzinfo=timezone.utc)))

    decisions = [json.loads(value)["decision"] for key, value in worker.redis.sets if key != nw.NEXUS_HEALTH_KEY]
    assert "WINDOW_VIEW" in decisions
    assert "BET" not in decisions


def test_publish_window_assessments_publishes_skipped_spread_window_views_for_unsupported_symbol():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis({
        "options:live:vrp:IWM": json.dumps({"ivRank": 20, "asOf": "2026-05-05T14:05:00Z"}),
        "options:live:iv_surface:IWM": json.dumps({"atmIv": 0.22, "asOf": "2026-05-05T14:05:00Z"}),
        "options:live:gex:IWM": json.dumps({"netGex": 1_000_000, "asOf": "2026-05-05T14:05:00Z"}),
    })
    worker.window_views_reported = set()
    worker.feature_blocks_reported = set()
    worker._log = lambda *args, **kwargs: None
    df = pl.DataFrame([
        nw.normalize_trade_payload({
            "symbol": "IWM",
            "raw_symbol": "IWM   260505C00220000",
            "ts_utc": "2026-05-05T13:35:00Z",
            "side": "C",
            "price": 2.0,
            "size": 1000,
            "premium": 200_000,
            "is_sweep": False,
            "aggressor": "A",
            "delta": 0.50,
            "gamma": 0.01,
            "greeks_ts_utc": "2026-05-05T13:35:00Z",
            "underlying_mid": 220.0,
            "underlying_ts_utc": "2026-05-05T13:35:00Z",
            "option_mid": 2.0,
            "quote_age_ms": 100,
        })
    ])

    asyncio.run(worker.publish_window_assessments(df, "IWM", nw.DECISION_SLOTS[0], datetime(2026, 5, 5).date()))

    put_view = json.loads(worker.redis.values["nexus_window_view:IWM:etf_put_credit_open30_spread:10:00"])
    call_view = json.loads(worker.redis.values["nexus_window_view:IWM:etf_call_credit_open30_spread:10:00"])
    assert put_view["decision"] == "SKIPPED"
    assert put_view["reason"] == "strategy_not_applicable_to_symbol_or_window"
    assert call_view["decision"] == "SKIPPED"
    assert call_view["reason"] == "strategy_not_applicable_to_symbol_or_window"


def test_publish_window_assessments_publishes_skipped_spread_window_views_outside_open30_slot():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis({
        "options:live:vrp:SPY": json.dumps({"ivRank": 20, "asOf": "2026-05-05T14:35:00Z"}),
        "options:live:iv_surface:SPY": json.dumps({"atmIv": 0.22, "asOf": "2026-05-05T14:35:00Z"}),
        "options:live:gex:SPY": json.dumps({"netGex": 1_000_000, "asOf": "2026-05-05T14:35:00Z"}),
    })
    worker.window_views_reported = set()
    worker.feature_blocks_reported = set()
    worker._log = lambda *args, **kwargs: None
    df = pl.DataFrame([
        nw.normalize_trade_payload({
            "symbol": "SPY",
            "raw_symbol": "SPY   260505C00700000",
            "ts_utc": "2026-05-05T14:05:00Z",
            "side": "C",
            "price": 2.0,
            "size": 1000,
            "premium": 200_000,
            "is_sweep": False,
            "aggressor": "A",
            "delta": 0.50,
            "gamma": 0.01,
            "greeks_ts_utc": "2026-05-05T14:05:00Z",
            "underlying_mid": 700.0,
            "underlying_ts_utc": "2026-05-05T14:05:00Z",
            "option_mid": 2.0,
            "quote_age_ms": 100,
        })
    ])

    asyncio.run(worker.publish_window_assessments(df, "SPY", nw.DECISION_SLOTS[1], datetime(2026, 5, 5).date()))

    put_view = json.loads(worker.redis.values["nexus_window_view:SPY:etf_put_credit_open30_spread:10:30"])
    call_view = json.loads(worker.redis.values["nexus_window_view:SPY:etf_call_credit_open30_spread:10:30"])
    assert put_view["decision"] == "SKIPPED"
    assert put_view["reason"] == "strategy_not_applicable_to_symbol_or_window"
    assert call_view["decision"] == "SKIPPED"
    assert call_view["reason"] == "strategy_not_applicable_to_symbol_or_window"


def test_default_first_trigger_scope_prevents_second_strategy_final_publish():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = set()
    worker.redis = FakeRedis({
        "options:live:contract_state:QQQ   260505C00500000": json.dumps({
            "optionMid": 12.4,
            "bid": 12.3,
            "ask": 12.5,
            "asOfUtc": "2026-05-05T13:35:00Z",
            "quoteAgeMs": 100,
            "tradable": True,
            "executable": True,
            "tradabilityBucket": "tradable",
        }),
    })
    worker.window_views_reported = set()
    worker.feature_blocks_reported = set()
    worker.active_positions = {}
    worker.buffers = {
        "QQQ": [
            nw.normalize_trade_payload({
                "symbol": "QQQ",
                "raw_symbol": "QQQ   260505C00500000",
                "ts_utc": "2026-05-05T13:35:00Z",
                "side": "C",
                "price": 12.5,
                "size": 200,
                "premium": 250_000,
                "is_sweep": False,
                "aggressor": "A",
                "delta": 0.50,
                "gamma": 0.01,
                "greeks_ts_utc": "2026-05-05T13:35:00Z",
                "underlying_mid": 500.25,
                "underlying_ts_utc": "2026-05-05T13:35:00Z",
                "option_mid": 12.4,
                "quote_age_ms": 100,
            }),
        ]
    }
    calls = []

    async def confluence(df, symbol, slot, session_date, *args):
        calls.append("confluence")

    async def open_specialist(df, symbol, slot, session_date, *args):
        calls.append("open")
        await worker._publish_final("etf_open_specialist", symbol, "BULLISH", 0.95, 0.0, session_date, slot, "QQQ   260505C00500000")

    async def hybrid(df, symbol, slot, session_date, *args):
        calls.append("hybrid")

    worker.evaluate_confluence_sniper = confluence
    worker.evaluate_open_specialist = open_specialist
    worker.evaluate_low_sweep_core = hybrid
    worker.evaluate_flow_specialist = hybrid

    asyncio.run(worker.evaluate_strategy("QQQ", nw.DECISION_SLOTS[0], datetime(2026, 5, 5, 14, 5, tzinfo=timezone.utc)))

    assert calls == ["confluence", "open"]
    bet_messages = [
        json.loads(value)
        for key, value in worker.redis.sets
        if key.startswith("nexus_live_overlay:")
    ]
    assert len(bet_messages) == 1


def test_symbol_lane_does_not_lock_other_symbol():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = set()
    worker.active_positions = {}
    worker.redis = FakeRedis({
        "options:live:contract_state:QQQ   260505C00500000": json.dumps({
            "optionMid": 12.4,
            "bid": 12.3,
            "ask": 12.5,
            "asOfUtc": "2026-05-05T14:00:00Z",
            "quoteAgeMs": 100,
            "tradable": True,
            "executable": True,
            "tradabilityBucket": "tradable",
        }),
    })
    session_date = datetime(2026, 5, 5).date()

    asyncio.run(worker._publish_final("etf_open_specialist", "QQQ", "BULLISH", 0.95, 0.0, session_date, nw.DECISION_SLOTS[0], "QQQ   260505C00500000"))

    assert not worker.already_signaled(session_date, "SPY", "etf_open_specialist")
    assert worker.already_signaled(session_date, "QQQ", "etf_open_specialist")


def test_confluence_group_lock_blocks_other_symbol_confluence_only():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.signaled_today = set()
    worker.active_positions = {}
    worker.redis = FakeRedis({
        "options:live:contract_state:QQQ   260505C00500000": json.dumps({
            "optionMid": 12.4,
            "bid": 12.3,
            "ask": 12.5,
            "asOfUtc": "2026-05-05T14:00:00Z",
            "quoteAgeMs": 100,
            "tradable": True,
            "executable": True,
            "tradabilityBucket": "tradable",
        }),
    })
    session_date = datetime(2026, 5, 5).date()

    asyncio.run(worker._publish_final("etf_confluence_sniper", "QQQ", "BULLISH", 0.95, 0.0, session_date, nw.DECISION_SLOTS[0], "QQQ   260505C00500000"))

    assert worker.already_signaled(session_date, "SPY", "etf_confluence_sniper")
    assert not worker.already_signaled(session_date, "SPY", "etf_open_specialist")


def test_context_requires_canonical_vrp_iv_rank():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis({
        "options:live:iv_surface:SPY": json.dumps({"atmIv": 0.22}),
        "options:live:gex:SPY": json.dumps({"netGex": -1_500_000_000}),
        "options:live:vrp:SPY": json.dumps({"vrpRegime": "cheap"}),
    })

    assert asyncio.run(worker.get_context("SPY")) == (50.0, 0.22, -1_500_000_000.0)


def test_momentum_specialist_requires_underlying_mid_and_uses_iv_gate():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis({"options:live:vrp:SPY": json.dumps({"ivRank": 20})})
    rows = []
    for minute in range(10):
        rows.append({
            **row(f"2026-05-05T13:{30 + minute:02d}:00Z", "C", 25_000),
            "underlying_mid": 700.0 + minute,
        })
        rows.append({
            **row(f"2026-05-05T13:{30 + minute:02d}:30Z", "C", 25_000),
            "underlying_mid": 700.5 + minute,
        })

    sentiment, valid, p_feat = asyncio.run(worker.check_momentum_heuristics(pl.DataFrame(rows), "SPY"))

    assert (sentiment, valid) == ("BULLISH", True)
    assert p_feat[:2] == [10.0, 0.0]


def test_momentum_specialist_sorts_rows_before_dynamic_grouping():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis({"options:live:vrp:IWM": json.dumps({"ivRank": 20})})
    rows = []
    for minute in range(10):
        rows.append({
            **row(f"2026-05-05T13:{30 + minute:02d}:30Z", "C", 25_000),
            "symbol": "IWM",
            "underlying_mid": 220.5 + minute,
        })
        rows.append({
            **row(f"2026-05-05T13:{30 + minute:02d}:00Z", "C", 25_000),
            "symbol": "IWM",
            "underlying_mid": 220.0 + minute,
        })

    sentiment, valid, p_feat = asyncio.run(worker.check_momentum_heuristics(pl.DataFrame(list(reversed(rows))), "IWM"))

    assert (sentiment, valid) == ("BULLISH", True)
    assert p_feat[:2] == [10.0, 0.0]


# ---------------------------------------------------------------------------
# Participant Flow Context — integration tests
# ---------------------------------------------------------------------------


def test_publish_participant_flow_context_sets_keys_and_publishes():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis({
        "options:live:gex:SPY": json.dumps({
            "netGex": -2_500_000_000.0,
            "asOf": "2026-05-08T13:59:00Z",
        }),
    })
    worker.participant_flow_reported = set()
    worker.buffers = {"SPY": deque(maxlen=100)}
    worker.buffers["SPY"].extend([
        {
            "ts_utc": "2026-05-08T13:35:00Z",
            "symbol": "SPY",
            "raw_symbol": "SPY   260508C00560000",
            "side": "C",
            "premium": 300_000.0,
            "is_sweep": True,
            "aggressor": "A",
            "delta": 0.5,
            "option_mid": 10.0,
            "option_bid": 9.9,
            "option_ask": 10.1,
            "underlying_mid": 560.0,
        },
        {
            "ts_utc": "2026-05-08T13:50:00Z",
            "symbol": "SPY",
            "raw_symbol": "SPY   260508P00555000",
            "side": "P",
            "premium": 50_000.0,
            "is_sweep": False,
            "aggressor": "A",
            "delta": -0.4,
            "option_mid": 7.0,
            "option_bid": 6.9,
            "option_ask": 7.1,
            "underlying_mid": 560.0,
        },
    ])

    slot = nw.MARKET_CONTEXT_WINDOWS[0]  # w0930_1000
    asyncio.run(worker.publish_participant_flow_context_for_slot("SPY", slot, date(2026, 5, 8)))

    redis_key = f"nexus_participant_flow_context:SPY:{slot['entry_label']}"
    latest_key = "nexus_participant_flow_context:SPY:latest"

    # Verify both keys were set
    assert redis_key in worker.redis.values
    assert latest_key in worker.redis.values

    # Verify payload structure
    msg = json.loads(worker.redis.values[redis_key])
    assert msg["schema_version"] == 1
    assert msg["symbol"] == "SPY"
    assert msg["window_key"] == slot["entry_label"]
    assert msg["source"] == "sigmatiq_nexus"
    assert msg["window_side_read"]["dominant_side"] == "calls"
    assert msg["window_side_read"]["premium_bias"] in ("call_heavy", "put_heavy", "balanced")
    assert msg["dealer_inferred_pressure"]["underlying_hedge_direction"] == "buy_underlying"
    assert msg["dealer_inferred_pressure"]["impact_state"] == "destabilizing"
    assert msg["data_quality"]["status"] in ("usable", "degraded", "thin", "stale", "unknown")
    assert "opening_or_closing_unknown" not in msg["data_quality"]["degraded"]

    # Verify persistence stream
    assert len(worker.redis.xadds) == 1
    assert worker.redis.xadds[0][1]["redis_key"] == redis_key

    # Verify pub/sub
    assert len(worker.redis.publishes) == 1
    assert worker.redis.publishes[0][0] == "signal:participant_flow_context"


def test_publish_participant_flow_context_dedup_guard():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis()
    worker.participant_flow_reported = set()
    worker.buffers = {"SPY": deque(maxlen=100)}
    worker.buffers["SPY"].append({
        "ts_utc": "2026-05-08T13:35:00Z",
        "symbol": "SPY",
        "raw_symbol": "SPY   260508C00560000",
        "side": "C",
        "premium": 100_000.0,
        "is_sweep": False,
        "aggressor": "A",
        "delta": 0.5,
        "option_mid": 10.0,
        "option_bid": 9.9,
        "option_ask": 10.1,
    })

    slot = nw.MARKET_CONTEXT_WINDOWS[0]
    asyncio.run(worker.publish_participant_flow_context_for_slot("SPY", slot, date(2026, 5, 8)))
    first_set_count = len(worker.redis.sets)

    asyncio.run(worker.publish_participant_flow_context_for_slot("SPY", slot, date(2026, 5, 8)))
    # Second call should be a no-op
    assert len(worker.redis.sets) == first_set_count


# ---------------------------------------------------------------------------
# etf_allday_specialist — tests
# ---------------------------------------------------------------------------


def test_evaluate_allday_alert_publishes_intermediate_and_final():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis({
        "options:live:contract_state:SPY260508C00560000": json.dumps({
            "optionMid": 5.0, "bid": 4.90, "ask": 5.10,
            "asOfUtc": "2026-05-08T14:15:00Z", "quoteAgeMs": 100,
            "tradable": True, "executable": True, "tradabilityBucket": "tradable",
        }),
        "options:live:vrp:SPY": json.dumps({"ivRank": 22.0, "asOf": "2026-05-08T14:15:00Z"}),
    })
    worker.signaled_today = set()
    worker.active_positions = {}
    worker.buffers = {"SPY": deque(maxlen=100)}
    worker.buffers["SPY"].extend([
        {
            "ts_utc": "2026-05-08T14:10:00Z",
            "symbol": "SPY",
            "raw_symbol": "SPY260508C00560000",
            "side": "C",
            "premium": 300_000.0,
            "is_sweep": True,
            "aggressor": "A",
            "delta": 0.5,
            "option_mid": 5.0,
            "option_bid": 4.90,
            "option_ask": 5.10,
            "underlying_mid": 560.0,
        },
    ])
    worker.last_reset_session_date = date(2026, 5, 8)
    worker.feature_blocks_reported = set()
    worker.window_views_reported = set()
    worker.window_pricing_reported = set()
    worker.option_market_context_reported = set()
    worker.participant_flow_reported = set()
    worker.evaluated_windows = set()
    worker.late_window_impacts = {}

    signal = {
        "symbol": "SPY",
        "direction": "BULLISH",
        "horizon": "0DTE",
        "session_date": "2026-05-08",
        "timestamp": "2026-05-08T14:15:00+00:00",
    }

    asyncio.run(worker.evaluate_allday_alert(signal))

    # Should have published intermediate + final (SET + XADD + PUBLISH each)
    keys_set = [s[0] for s in worker.redis.sets]
    assert any("nexus_intermediate:SPY:etf_allday_specialist" in k for k in keys_set)


def test_evaluate_allday_alert_ignores_wrong_symbol():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis()
    worker.signaled_today = set()
    worker.active_positions = {}
    worker.buffers = {}
    worker.last_reset_session_date = date(2026, 5, 8)

    signal = {"symbol": "AAPL", "direction": "BULLISH", "horizon": "0DTE", "session_date": "2026-05-08"}
    asyncio.run(worker.evaluate_allday_alert(signal))
    assert len(worker.redis.sets) == 0


def test_evaluate_allday_alert_ignores_wrong_horizon():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis()
    worker.signaled_today = set()
    worker.active_positions = {}
    worker.buffers = {"SPY": deque(maxlen=100)}
    worker.last_reset_session_date = date(2026, 5, 8)

    signal = {"symbol": "SPY", "direction": "BULLISH", "horizon": "2W", "session_date": "2026-05-08"}
    asyncio.run(worker.evaluate_allday_alert(signal))
    assert len(worker.redis.sets) == 0


def test_evaluate_allday_alert_respects_session_lock():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.redis = FakeRedis({
        "options:live:contract_state:SPY260508C00560000": json.dumps({
            "optionMid": 5.0, "bid": 4.90, "ask": 5.10,
            "asOfUtc": "2026-05-08T14:15:00Z", "quoteAgeMs": 100,
            "tradable": True, "executable": True, "tradabilityBucket": "tradable",
        }),
        "options:live:vrp:SPY": json.dumps({"ivRank": 22.0, "asOf": "2026-05-08T14:15:00Z"}),
    })
    worker.signaled_today = set()
    worker.active_positions = {}
    worker.buffers = {"SPY": deque(maxlen=100)}
    worker.buffers["SPY"].append({
        "ts_utc": "2026-05-08T14:10:00Z", "symbol": "SPY", "raw_symbol": "SPY260508C00560000",
        "side": "C", "premium": 300_000.0, "is_sweep": True, "aggressor": "A",
        "delta": 0.5, "option_mid": 5.0, "option_bid": 4.90, "option_ask": 5.10, "underlying_mid": 560.0,
    })
    worker.last_reset_session_date = date(2026, 5, 8)
    worker.feature_blocks_reported = set()
    worker.window_views_reported = set()
    worker.window_pricing_reported = set()
    worker.option_market_context_reported = set()
    worker.participant_flow_reported = set()
    worker.evaluated_windows = set()
    worker.late_window_impacts = {}

    signal = {"symbol": "SPY", "direction": "BULLISH", "horizon": "0DTE", "session_date": "2026-05-08"}

    # First call should publish
    asyncio.run(worker.evaluate_allday_alert(signal))
    first_count = len(worker.redis.sets)
    assert first_count > 0

    # Second call should be locked
    asyncio.run(worker.evaluate_allday_alert(signal))
    assert len(worker.redis.sets) == first_count


def test_latest_due_slot_returns_only_most_recent_due_window():
    reference = datetime(2026, 5, 8, 16, 40, tzinfo=timezone.utc)  # 12:40 ET

    slot = nw.latest_due_slot(nw.MARKET_CONTEXT_WINDOWS, reference, 0)

    assert slot is not None
    assert slot["entry_label"] == "w1200_1230"


def test_evaluate_due_windows_does_not_backfill_all_missed_windows_after_restart():
    worker = nw.SigmatiqNexus.__new__(nw.SigmatiqNexus)
    worker.buffers = {"SPY": deque([{"symbol": "SPY"}], maxlen=10)}
    worker.option_market_context_reported = set()
    worker.participant_flow_reported = set()
    worker.evaluated_windows = set()
    calls: list[tuple[str, str]] = []

    async def publish_option_market_context(symbol, slot, session_date):
        calls.append(("market", slot["entry_label"]))

    async def publish_participant_flow_context(symbol, slot, session_date):
        calls.append(("participant", slot["entry_label"]))

    async def evaluate_strategy(symbol, slot, reference_dt_utc):
        calls.append(("strategy", slot["entry_label"]))

    worker.publish_option_market_context_for_slot = publish_option_market_context
    worker.publish_participant_flow_context_for_slot = publish_participant_flow_context
    worker.evaluate_strategy = evaluate_strategy

    asyncio.run(worker.evaluate_due_windows(datetime(2026, 5, 8, 16, 40, tzinfo=timezone.utc)))

    assert calls == [
        ("market", "w1200_1230"),
        ("participant", "w1200_1230"),
        ("strategy", "12:00"),
    ]
