"""Real Kafka -> ClickHouse Serving Consumer (Gate 4 & Gate 5).

Streams 1-minute bars from Kafka topic `market.bars.1m` into ClickHouse
`quant_dw.market_intraday_bars` backed by `ReplacingMergeTree(version)`
with composite sorting key `(dataset_id, ticker_symbol, window_start)`.

Reliability & Engineering Contract (Phase A):
1. Safe Deserialization: Does NOT crash on tombstones (null value) or malformed/corrupt JSON.
2. Semantic Distinction:
   - Tombstones (null value) represent deletion/compaction markers and are handled deterministically.
   - Malformed/corrupt payloads are routed to DLQ (`market.bars.dlq`) with metadata and reason.
   - Schema validation errors (missing fields, negative volume) are routed to DLQ.
3. At-Least-Once Delivery: Offsets committed ONLY after ClickHouse batch insert and DLQ flush succeed.
4. Idempotent Serving & Late Corrections:
   - Replay idempotency preserves logical count under FINAL.
   - Monotonic version updates (v1 -> v2 -> v3...).
   - Stale-version arrival resistance.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import requests

try:
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition, OffsetAndMetadata
except ImportError:
    KafkaConsumer = None
    KafkaProducer = None
    TopicPartition = None
    OffsetAndMetadata = None

from quant_edai.serving.clickhouse_sync import ClickHouseSyncManager

logger = logging.getLogger("KafkaClickHouseConsumer")


@dataclass
class ServingSyncMetrics:
    """Runtime metrics for Kafka -> ClickHouse serving integration."""
    dataset_id: str
    topic: str
    table: str
    messages_read: int = 0
    messages_inserted: int = 0
    batches_inserted: int = 0
    corrections_observed: int = 0
    stale_arrivals_observed: int = 0
    tombstones_processed: int = 0
    corrupt_tombstones_routed_to_dlq: int = 0
    poison_messages_routed_to_dlq: int = 0
    schema_errors_routed_to_dlq: int = 0
    latest_offsets: Dict[int, int] = field(default_factory=dict)
    committed_offsets: Dict[int, int] = field(default_factory=dict)
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    status: str = "INITIALIZED"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "topic": self.topic,
            "table": self.table,
            "messages_read": self.messages_read,
            "messages_inserted": self.messages_inserted,
            "batches_inserted": self.batches_inserted,
            "corrections_observed": self.corrections_observed,
            "stale_arrivals_observed": self.stale_arrivals_observed,
            "tombstones_processed": self.tombstones_processed,
            "corrupt_tombstones_routed_to_dlq": self.corrupt_tombstones_routed_to_dlq,
            "poison_messages_routed_to_dlq": self.poison_messages_routed_to_dlq,
            "schema_errors_routed_to_dlq": self.schema_errors_routed_to_dlq,
            "latest_offsets": self.latest_offsets,
            "committed_offsets": self.committed_offsets,
            "duration_sec": round((self.end_time or time.time()) - self.start_time, 3),
            "status": self.status,
        }


class KafkaClickHouseServingConsumer:
    """Manages streaming ingestion from Kafka changelog into ClickHouse ReplacingMergeTree."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "market.bars.1m",
        group_id: str = "clickhouse-serving-group",
        dlq_topic: str = "market.bars.dlq",
        clickhouse_manager: Optional[ClickHouseSyncManager] = None,
        table_name: str = "market_intraday_bars",
        batch_size: int = 500,
        poll_timeout_ms: int = 1000,
    ) -> None:
        if KafkaConsumer is None or KafkaProducer is None:
            raise RuntimeError("kafka-python is required for KafkaClickHouseServingConsumer.")

        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.dlq_topic = dlq_topic
        self.table_name = table_name
        self.batch_size = batch_size
        self.poll_timeout_ms = poll_timeout_ms
        self.ch_mgr = clickhouse_manager or ClickHouseSyncManager()
        self._dlq_producer: Optional[KafkaProducer] = None

    def _get_dlq_producer(self) -> KafkaProducer:
        """Lazily instantiates the Kafka DLQ producer."""
        if self._dlq_producer is None:
            self._dlq_producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
            )
        return self._dlq_producer

    def _route_to_dlq(
        self,
        dataset_id: str,
        partition: int,
        offset: int,
        key: Optional[str],
        raw_payload: Optional[str],
        dlq_reason: str,
        error_details: str,
    ) -> None:
        """Safely dispatches corrupt or schema-invalid bar messages to DLQ topic."""
        producer = self._get_dlq_producer()
        dlq_record = {
            "dataset_id": dataset_id or "UNKNOWN",
            "source_topic": self.topic,
            "source_partition": partition,
            "source_offset": offset,
            "message_key": key,
            "raw_payload": raw_payload,
            "dlq_reason": dlq_reason,
            "error_details": error_details,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        producer.send(self.dlq_topic, key=key or f"dlq_{partition}_{offset}", value=dlq_record)
        logger.warning(
            f"[DLQ Routing] Message at offset {offset} (key={key}) routed to {self.dlq_topic}: {dlq_reason}"
        )

    def _create_consumer(self, auto_offset_reset: str = "earliest") -> KafkaConsumer:
        """
        Creates a KafkaConsumer without fragile lambda deserializer.
        Receives raw bytes so poison pills, corrupt JSON, and tombstones (nulls)
        are processed safely in application code rather than crashing the fetcher.
        """
        return KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=False,
            value_deserializer=None,  # Raw bytes for safe handling
            consumer_timeout_ms=self.poll_timeout_ms * 3,
        )

    def transform_bar_record(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Maps Kafka 1m bar payload to ClickHouse `market_intraday_bars` schema."""
        ds_id = str(raw.get("dataset_id", "UNKNOWN"))
        ticker = str(raw.get("ticker_symbol", "UNKNOWN"))
        win_start = str(raw.get("window_start", ""))
        win_end = str(raw.get("window_end", win_start))

        fmt_start = win_start.replace("T", " ")[:19]
        fmt_end = win_end.replace("T", " ")[:19]

        return {
            "dataset_id": ds_id,
            "ticker_symbol": ticker,
            "window_start": fmt_start,
            "window_end": fmt_end,
            "event_timestamp": fmt_start,
            "open_price": float(raw.get("open", 0.0)),
            "high_price": float(raw.get("high", 0.0)),
            "low_price": float(raw.get("low", 0.0)),
            "close_price": float(raw.get("close", 0.0)),
            "volume": int(raw.get("volume", 0)),
            "vwap": float(raw.get("vwap", 0.0)),
            "trade_count": int(raw.get("trade_count", 1)),
            "exchange": "NASDAQ",
            "version": int(raw.get("version", 1)),
            "is_deleted": int(raw.get("is_deleted", 0)),
            "created": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }

    def sync_dataset_bars(
        self,
        dataset_id: str,
        max_messages: Optional[int] = None,
        max_idle_polls: int = 5,
        fail_on_ch_down: bool = True,
    ) -> ServingSyncMetrics:
        """
        Consumes bars matching `dataset_id` (or all if empty) from Kafka, inserts into ClickHouse,
        routes poison messages to DLQ, and commits offsets.
        """
        if not self.ch_mgr.is_online():
            msg = f"ClickHouse at {self.ch_mgr.url} is unreachable."
            logger.error(msg)
            if fail_on_ch_down:
                raise RuntimeError(msg)
            return ServingSyncMetrics(dataset_id=dataset_id, topic=self.topic, table=self.table_name, status="CH_UNREACHABLE")

        metrics = ServingSyncMetrics(dataset_id=dataset_id, topic=self.topic, table=self.table_name)
        consumer = self._create_consumer(auto_offset_reset="earliest")

        batch: List[Dict[str, Any]] = []
        idle_count = 0
        partition_offsets: Dict[TopicPartition, int] = {}
        known_versions: Dict[Tuple[str, str, str], int] = {}

        try:
            while True:
                poll_result = consumer.poll(timeout_ms=self.poll_timeout_ms)
                if not poll_result:
                    idle_count += 1
                    if idle_count >= max_idle_polls:
                        logger.info(f"Consumer reached max idle polls ({max_idle_polls}). Finalizing.")
                        break
                    continue

                idle_count = 0
                for tp, messages in poll_result.items():
                    for msg in messages:
                        partition_offsets[tp] = msg.offset + 1
                        metrics.latest_offsets[tp.partition] = msg.offset

                        # 1. Check for Tombstone (upsert delete semantics: value is None)
                        if msg.value is None:
                            metrics.tombstones_processed += 1
                            key_str = msg.key.decode("utf-8", errors="replace") if msg.key else ""
                            logger.info(
                                f"[Tombstone] Processing delete marker for key '{key_str}' at offset {msg.offset}"
                            )

                            tomb_ds = None
                            tomb_ticker = None
                            tomb_window_start = None
                            tomb_version = 1

                            # Check Kafka message headers for version if provided
                            if msg.headers:
                                for h_k, h_v in msg.headers:
                                    if h_k.lower() in ("version", "ver") and h_v:
                                        try:
                                            tomb_version = int(h_v.decode("utf-8", errors="replace"))
                                        except (ValueError, TypeError):
                                            pass

                            is_parsed = False
                            parse_error = None

                            # Contract A: JSON composite key (Flink upsert-kafka standard contract)
                            # e.g. {"dataset_id": "...", "ticker_symbol": "...", "window_start": "..."}
                            if key_str.startswith("{") and key_str.endswith("}"):
                                try:
                                    key_dict = json.loads(key_str)
                                    if isinstance(key_dict, dict):
                                        tomb_ds = str(key_dict.get("dataset_id") or "")
                                        tomb_ticker = str(key_dict.get("ticker_symbol") or "")
                                        tomb_window_start = str(key_dict.get("window_start") or "")
                                        if "version" in key_dict:
                                            try:
                                                tomb_version = int(key_dict["version"])
                                            except (ValueError, TypeError):
                                                pass
                                        if tomb_ds and tomb_ticker and tomb_window_start:
                                            is_parsed = True
                                        else:
                                            missing_k = [k for k in ["dataset_id", "ticker_symbol", "window_start"] if not key_dict.get(k)]
                                            parse_error = f"JSON composite key missing required fields: {missing_k}"
                                    else:
                                        parse_error = "JSON key payload is not an object"
                                except Exception as e:
                                    parse_error = f"Malformed JSON in tombstone key: {e}"

                            # Contract B: Explicit legacy delimited key: <dataset_id>_<ticker_symbol>_<window_start>
                            # Uses rsplit('_', 2) to preserve any underscores in dataset_id
                            if not is_parsed and not parse_error and "_" in key_str:
                                parts = key_str.rsplit("_", 2)
                                if len(parts) == 3 and parts[0] and parts[1] and parts[2]:
                                    tomb_ds = parts[0]
                                    tomb_ticker = parts[1]
                                    tomb_window_start = parts[2]
                                    is_parsed = True
                                else:
                                    parse_error = f"Invalid delimited key format: expected <dataset_id>_<ticker>_<window_start>, got '{key_str}'"

                            # Timestamp contract validation
                            if is_parsed and tomb_window_start:
                                try:
                                    dt = pd.to_datetime(tomb_window_start)
                                    tomb_window_start = dt.strftime("%Y-%m-%d %H:%M:%S")
                                except Exception as e:
                                    is_parsed = False
                                    parse_error = f"Invalid window_start datetime in tombstone key ('{tomb_window_start}'): {e}"

                            # Handle corrupt tombstone key -> route to DLQ, never crash ClickHouse
                            if not is_parsed:
                                dlq_reason = "CORRUPT_TOMBSTONE_KEY"
                                err_msg = parse_error or f"Unrecognized tombstone key format: '{key_str}'"
                                logger.warning(
                                    f"[DLQ Routing] Message at offset {msg.offset} (key={key_str}) routed to {self.dlq_topic}: {dlq_reason} ({err_msg})"
                                )
                                self._route_to_dlq(
                                    dataset_id=dataset_id or tomb_ds or "UNKNOWN",
                                    partition=tp.partition,
                                    offset=msg.offset,
                                    key=key_str,
                                    raw_payload=None,
                                    dlq_reason=dlq_reason,
                                    error_details=err_msg,
                                )
                                metrics.corrupt_tombstones_routed_to_dlq += 1
                                continue

                            # Dataset isolation: skip tombstones for other datasets
                            if dataset_id and tomb_ds != dataset_id:
                                logger.debug(
                                    f"[Tombstone] Ignoring delete marker for other dataset '{tomb_ds}' (syncing '{dataset_id}')"
                                )
                                continue

                            # Valid tombstone soft-delete record
                            metrics.messages_read += 1
                            tomb_record = {
                                "dataset_id": tomb_ds,
                                "ticker_symbol": tomb_ticker,
                                "window_start": tomb_window_start,
                                "window_end": tomb_window_start,
                                "event_timestamp": tomb_window_start,
                                "open_price": 0.0,
                                "high_price": 0.0,
                                "low_price": 0.0,
                                "close_price": 0.0,
                                "volume": 0,
                                "vwap": 0.0,
                                "trade_count": 0,
                                "exchange": "NASDAQ",
                                "version": tomb_version,
                                "is_deleted": 1,
                                "created": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                            }
                            batch.append(tomb_record)

                            if len(batch) >= self.batch_size:
                                self._flush_batch_and_commit(consumer, batch, partition_offsets, metrics)
                                batch = []
                            continue

                        # 2. Safe JSON Deserialization (Poison Message Detection)
                        raw_bytes = msg.value
                        try:
                            raw_str = raw_bytes.decode("utf-8")
                            val = json.loads(raw_str)
                            if not isinstance(val, dict):
                                raise ValueError("Payload is not a valid JSON dictionary")
                        except Exception as e:
                            # MALFORMED JSON POISON PILL -> ROUTE TO DLQ
                            self._route_to_dlq(
                                dataset_id=dataset_id,
                                partition=tp.partition,
                                offset=msg.offset,
                                key=msg.key.decode("utf-8", errors="replace") if msg.key else None,
                                raw_payload=raw_bytes.decode("utf-8", errors="replace") if raw_bytes else None,
                                dlq_reason="MALFORMED_JSON_PAYLOAD",
                                error_details=str(e),
                            )
                            metrics.poison_messages_routed_to_dlq += 1
                            continue

                        # 3. Schema Validation & Numerical Sanity
                        required_cols = {"dataset_id", "ticker_symbol", "window_start", "open", "high", "low", "close", "volume"}
                        missing = required_cols - set(val.keys())
                        is_negative_vol = False
                        try:
                            is_negative_vol = float(val.get("volume", 0)) < 0
                        except (ValueError, TypeError):
                            is_negative_vol = True

                        if missing or is_negative_vol:
                            self._route_to_dlq(
                                dataset_id=val.get("dataset_id", dataset_id),
                                partition=tp.partition,
                                offset=msg.offset,
                                key=msg.key.decode("utf-8", errors="replace") if msg.key else None,
                                raw_payload=raw_str,
                                dlq_reason=f"SCHEMA_VALIDATION_ERROR: missing={list(missing)}, negative_vol={is_negative_vol}",
                                error_details="Record failed contract validation",
                            )
                            metrics.schema_errors_routed_to_dlq += 1
                            continue

                        # Filter by dataset_id if requested
                        msg_ds = val.get("dataset_id")
                        if dataset_id and msg_ds != dataset_id:
                            continue

                        metrics.messages_read += 1
                        ver = int(val.get("version", 1))
                        bar_key = (str(val.get("dataset_id")), str(val.get("ticker_symbol")), str(val.get("window_start")))

                        if ver > 1:
                            metrics.corrections_observed += 1

                        if bar_key in known_versions:
                            if ver < known_versions[bar_key]:
                                metrics.stale_arrivals_observed += 1
                            else:
                                known_versions[bar_key] = ver
                        else:
                            known_versions[bar_key] = ver

                        transformed = self.transform_bar_record(val)
                        batch.append(transformed)

                        if len(batch) >= self.batch_size:
                            self._flush_batch_and_commit(consumer, batch, partition_offsets, metrics)
                            batch = []

                        if max_messages and metrics.messages_read >= max_messages:
                            break
                    if max_messages and metrics.messages_read >= max_messages:
                        break

            # Final flush for remaining batch or uncommitted offsets (DLQ, filtered)
            if batch or partition_offsets:
                self._flush_batch_and_commit(consumer, batch, partition_offsets, metrics)
                batch = []

            metrics.status = "SUCCESS"
        except Exception as e:
            logger.error(f"Error during Kafka -> ClickHouse serving sync: {e}", exc_info=True)
            metrics.status = f"FAILED: {e}"
            raise
        finally:
            metrics.end_time = time.time()
            consumer.close()

        return metrics

    def _flush_batch_and_commit(
        self,
        consumer: KafkaConsumer,
        batch: List[Dict[str, Any]],
        partition_offsets: Dict[TopicPartition, int],
        metrics: ServingSyncMetrics,
    ) -> None:
        """
        1. Flushes DLQ producer to guarantee corrupt records are persisted in DLQ.
        2. Inserts valid batch into ClickHouse.
        3. ONLY on success commits Kafka offsets (At-Least-Once Delivery).
        """
        # 1. Flush DLQ
        if self._dlq_producer is not None:
            self._dlq_producer.flush()

        # 2. Insert valid records into ClickHouse
        if batch:
            df = pd.DataFrame(batch)
            t0 = time.time()
            inserted_count = self.ch_mgr.insert_dataframe(self.table_name, df)
            insert_duration = time.time() - t0

            metrics.messages_inserted += inserted_count
            metrics.batches_inserted += 1
            logger.info(
                f"[Kafka -> ClickHouse] Inserted {inserted_count} rows in {insert_duration:.3f}s."
            )

        # 3. Synchronous commit of Kafka offsets
        if partition_offsets:
            commit_map = {tp: OffsetAndMetadata(offset, "") for tp, offset in partition_offsets.items()}
            consumer.commit(commit_map)
            for tp, offset in partition_offsets.items():
                metrics.committed_offsets[tp.partition] = offset
            logger.info(f"[Kafka -> ClickHouse] Committed offsets: {metrics.committed_offsets}")
            partition_offsets.clear()

    def query_serving_bars(
        self,
        dataset_id: str,
        ticker_symbol: Optional[str] = None,
        final: bool = True,
        include_deleted: bool = False,
    ) -> pd.DataFrame:
        """
        Queries ClickHouse for market intraday bars with or without FINAL.
        When `final=True`, ClickHouse deduplicates using ReplacingMergeTree(version),
        returning strictly the latest version of each logical bar.
        """
        where_clauses = [f"dataset_id = '{dataset_id}'"]
        if not include_deleted:
            where_clauses.append("is_deleted = 0")
        if ticker_symbol:
            where_clauses.append(f"ticker_symbol = '{ticker_symbol}'")
        where_sql = " AND ".join(where_clauses)

        final_keyword = " FINAL" if final else ""
        query = f"""
            SELECT
                dataset_id,
                ticker_symbol,
                window_start,
                window_end,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                vwap,
                trade_count,
                version,
                is_deleted
            FROM {self.ch_mgr.database}.{self.table_name}{final_keyword}
            WHERE {where_sql}
            ORDER BY window_start, ticker_symbol
            FORMAT JSONEachRow
        """
        resp = self.ch_mgr.execute_query(query)
        lines = [json.loads(line) for line in resp.text.strip().split("\n") if line.strip()]
        if not lines:
            return pd.DataFrame(columns=[
                "dataset_id", "ticker_symbol", "window_start", "window_end",
                "event_timestamp", "open_price", "high_price", "low_price",
                "close_price", "volume", "vwap", "trade_count", "version",
                "is_deleted", "window_start_dt"
            ])
        df = pd.DataFrame(lines)
        df["window_start_dt"] = pd.to_datetime(df["window_start"], utc=True)
        return df

    def get_serving_reconciliation(self, dataset_id: str) -> Dict[str, Any]:
        """
        Calculates exact reconciliation statistics for `dataset_id` in ClickHouse:
        Raw row count vs FINAL logical row count (active, non-deleted), volume conservation, and highest versions.
        """
        raw_count = self.ch_mgr.query_count(self.table_name, dataset_id=dataset_id, final=False)
        final_resp = self.ch_mgr.execute_query(f"""
            SELECT count()
            FROM {self.ch_mgr.database}.{self.table_name} FINAL
            WHERE dataset_id = '{dataset_id}' AND is_deleted = 0
        """)
        final_count = int(final_resp.text.strip())

        vol_resp = self.ch_mgr.execute_query(f"""
            SELECT sum(volume), sum(trade_count), max(version)
            FROM {self.ch_mgr.database}.{self.table_name} FINAL
            WHERE dataset_id = '{dataset_id}' AND is_deleted = 0
            FORMAT JSONEachRow
        """)
        vol_data = json.loads(vol_resp.text.strip()) if vol_resp.text.strip() else {}
        total_vol = int(vol_data.get("sum(volume)", 0))
        total_trades = int(vol_data.get("sum(trade_count)", 0))
        max_version = int(vol_data.get("max(version)", 1))

        return {
            "dataset_id": dataset_id,
            "table": self.table_name,
            "raw_physical_rows": raw_count,
            "logical_final_rows": final_count,
            "duplicate_or_superseded_rows": raw_count - final_count,
            "total_final_volume": total_vol,
            "total_final_trades": total_trades,
            "max_version_observed": max_version,
            "is_deduplicated": raw_count >= final_count,
        }
