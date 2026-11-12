"""Unified 1m bar contract shared by Flink output, Kafka ``market.bars.1m`` and the trader.

``quant_edai.quant.market_replay.kafka_message_to_bar`` reads these field names, so Flink
(sim or PyFlink) must emit them. Legacy aliases (``window_start``, ``vwap_1m``) are kept
so existing consumers/tests keep working.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping

BAR_SCHEMA_VERSION = "bar_1m.v1"

BAR_REQUIRED_FIELDS = (
    "ticker_symbol",
    "event_timestamp",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "vwap",
)
BAR_OPTIONAL_FIELDS = ("trade_count", "window_start", "vwap_1m", "source_layer", "dataset_id", "bar_schema_version")


def normalize_bar_record(rec: Mapping[str, Any]) -> Dict[str, Any]:
    """Return ``rec`` with unified field names filled from legacy aliases."""
    out: Dict[str, Any] = dict(rec)
    out.setdefault("ticker_symbol", out.get("ticker"))
    ts = out.get("event_timestamp") or out.get("window_start") or out.get("bar_timestamp") or out.get("timestamp")
    out["event_timestamp"] = ts
    out.setdefault("window_start", ts)
    vwap = out.get("vwap")
    if vwap is None:
        vwap = out.get("vwap_1m")
    out["vwap"] = vwap
    out.setdefault("vwap_1m", vwap)
    for new, old in (
        ("open_price", "open"),
        ("high_price", "high"),
        ("low_price", "low"),
        ("close_price", "close"),
    ):
        if out.get(new) is None and out.get(old) is not None:
            out[new] = out[old]
    out["bar_schema_version"] = BAR_SCHEMA_VERSION
    return out


def missing_bar_fields(rec: Mapping[str, Any]) -> list[str]:
    return [f for f in BAR_REQUIRED_FIELDS if rec.get(f) is None]
