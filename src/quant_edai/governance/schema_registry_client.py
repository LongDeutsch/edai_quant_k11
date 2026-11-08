"""
Confluent Schema Registry Client & Avro Wire Format Serializer.

Handles:
1. Confluent Wire Format framing (Magic Byte 0x00 + 4-byte Schema ID + Binary Avro).
2. Schema registration and versioning under subject names (e.g., `market.ticks-value`).
3. Compatibility checks (BACKWARD_TRANSITIVE) and rejection of breaking schema changes.
"""

from __future__ import annotations

import io
import json
import os
import struct
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import fastavro
import requests


CONFLUENT_MAGIC_BYTE = 0


class SchemaRegistryClient:
    """Client for Confluent Schema Registry with Wire Format serialization."""

    def __init__(self, registry_url: Optional[str] = None):
        self.registry_url = (registry_url or os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8085")).rstrip("/")
        self._schema_cache: Dict[int, Dict[str, Any]] = {}
        self._id_cache: Dict[str, int] = {}

    def is_reachable(self) -> bool:
        """Checks if Schema Registry service is reachable."""
        try:
            resp = requests.get(f"{self.registry_url}/subjects", timeout=2)
            return resp.status_code == 200
        except Exception:
            return False

    def set_compatibility(self, subject: Optional[str] = None, level: str = "BACKWARD_TRANSITIVE") -> bool:
        """Sets compatibility level globally or for a specific subject."""
        url = f"{self.registry_url}/config/{subject}" if subject else f"{self.registry_url}/config"
        try:
            resp = requests.put(url, json={"compatibility": level}, timeout=3)
            return resp.status_code == 200
        except Exception as e:
            print(f"[SchemaRegistry] Failed to set compatibility ({e})")
            return False

    def register_schema(self, subject: str, schema_dict: Dict[str, Any]) -> int:
        """
        Registers schema under given subject. Returns integer schema_id.
        Raises ValueError if Schema Registry rejects incompatible schema.
        """
        url = f"{self.registry_url}/subjects/{subject}/versions"
        payload = {"schema": json.dumps(schema_dict)}
        resp = requests.post(url, json=payload, headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}, timeout=5)

        if resp.status_code == 200:
            schema_id = resp.json()["id"]
            self._schema_cache[schema_id] = schema_dict
            self._id_cache[subject] = schema_id
            return schema_id
        elif resp.status_code == 409:
            raise ValueError(f"Schema Registry 409 Conflict: Incompatible schema evolution for subject '{subject}'. {resp.text}")
        else:
            raise RuntimeError(f"Schema Registry registration error ({resp.status_code}): {resp.text}")

    def check_compatibility(self, subject: str, schema_dict: Dict[str, Any], version: str = "latest") -> bool:
        """Checks if new schema is compatible with the specified subject version."""
        url = f"{self.registry_url}/compatibility/subjects/{subject}/versions/{version}"
        payload = {"schema": json.dumps(schema_dict)}
        try:
            resp = requests.post(url, json=payload, headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}, timeout=3)
            if resp.status_code == 200:
                return bool(resp.json().get("is_compatible", False))
            return False
        except Exception:
            return False

    def get_latest_schema(self, subject: str) -> Tuple[int, Dict[str, Any]]:
        """Retrieves latest schema_id and parsed schema dictionary."""
        url = f"{self.registry_url}/subjects/{subject}/versions/latest"
        resp = requests.get(url, timeout=3)
        if resp.status_code == 200:
            data = resp.json()
            schema_id = data["id"]
            schema_dict = json.loads(data["schema"])
            self._schema_cache[schema_id] = schema_dict
            return schema_id, schema_dict
        raise RuntimeError(f"Failed to fetch latest schema for '{subject}': {resp.text}")

    def encode_wire_format(self, record: Dict[str, Any], schema: Dict[str, Any], schema_id: int = 1) -> bytes:
        """
        Encodes record into Confluent Wire Format:
        [1 byte: 0x00][4 bytes: schema_id (big-endian)][Avro binary payload]
        """
        buf = io.BytesIO()
        # 1. Magic byte
        buf.write(struct.pack(">b", CONFLUENT_MAGIC_BYTE))
        # 2. Schema ID
        buf.write(struct.pack(">I", schema_id))
        # 3. Binary Avro payload
        fastavro.schemaless_writer(buf, schema, record)
        return buf.getvalue()

    def decode_wire_format(self, payload: bytes, reader_schema: Optional[Dict[str, Any]] = None) -> Tuple[int, Dict[str, Any]]:
        """
        Decodes Confluent Wire Format bytes into (schema_id, record_dict).
        """
        if len(payload) < 5:
            raise ValueError(f"Payload too short for Confluent Wire Format ({len(payload)} bytes)")

        magic_byte, schema_id = struct.unpack(">bI", payload[:5])
        if magic_byte != CONFLUENT_MAGIC_BYTE:
            raise ValueError(f"Invalid magic byte: {magic_byte} (expected {CONFLUENT_MAGIC_BYTE})")

        buf = io.BytesIO(payload[5:])
        schema = reader_schema or self._schema_cache.get(schema_id)
        if not schema:
            # Fallback to local default v1 schema
            v1_path = Path("schemas/avro/market_tick_v1.avsc")
            if v1_path.exists():
                with open(v1_path, "r") as f:
                    schema = json.load(f)
            else:
                raise ValueError(f"Schema ID {schema_id} not found in cache and no reader schema provided.")

        record = fastavro.schemaless_reader(buf, schema)
        return schema_id, record
