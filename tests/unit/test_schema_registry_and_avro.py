"""
Unit tests for Confluent Avro Wire Format and Schema Evolution.
"""

from pathlib import Path
import fastavro
import pytest

from quant_edai.governance.schema_registry_client import SchemaRegistryClient, CONFLUENT_MAGIC_BYTE
from quant_edai.governance.verify_schema_evolution import load_schema_file, verify_schema_evolution


def test_avro_schemas_validity():
    v1 = load_schema_file("schemas/avro/market_tick_v1.avsc")
    v2 = load_schema_file("schemas/avro/market_tick_v2.avsc")

    parsed_v1 = fastavro.parse_schema(v1)
    parsed_v2 = fastavro.parse_schema(v2)

    assert parsed_v1["name"] == "com.quant.edai.market.MarketTick"
    assert parsed_v2["name"] == "com.quant.edai.market.MarketTick"

    # v2 has 10 fields (8 baseline + 2 optional)
    assert len(parsed_v1["fields"]) == 8
    assert len(parsed_v2["fields"]) == 10


def test_confluent_wire_format_framing():
    client = SchemaRegistryClient()
    v1 = load_schema_file("schemas/avro/market_tick_v1.avsc")

    record = {
        "trade_id": "trd_123",
        "ticker_symbol": "MSFT",
        "event_timestamp": "2024-06-01T10:00:00Z",
        "price": 420.0,
        "size": 200,
        "exchange": "NASDAQ",
        "is_late": False,
        "is_duplicate": False,
    }

    # Encode
    payload = client.encode_wire_format(record, v1, schema_id=42)

    # Validate header
    assert len(payload) > 5
    assert payload[0] == CONFLUENT_MAGIC_BYTE
    assert int.from_bytes(payload[1:5], byteorder="big") == 42

    # Decode
    schema_id, decoded = client.decode_wire_format(payload, reader_schema=v1)
    assert schema_id == 42
    assert decoded["ticker_symbol"] == "MSFT"
    assert decoded["price"] == 420.0
    assert decoded["size"] == 200


def test_verify_schema_evolution_offline(tmp_path):
    report_file = tmp_path / "schema_report.json"
    report = verify_schema_evolution(registry_url="http://localhost:9999", out_file=str(report_file))

    assert report["status"] == "PASSED"
    assert report["breaking_change_blocked"] is True
    assert report["wire_format_serialization_tested"] is True
    assert report_file.exists()
