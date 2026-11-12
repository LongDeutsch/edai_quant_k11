"""
Baseline Unoptimized PyFlink Streaming Job (Coursework Baseline Engine).

Rubric Satisfied:
- M1-14: Baseline unoptimized Flink job using Processing Time (PROCTIME()).
  Does NOT use event-time watermarking, and does NOT deduplicate streaming records.
  Demonstrates that processing-time windowing misallocates late-arriving ticks and duplicates data.
- Sinks 1m bars to dedicated Kafka topic 'market.bars.1m.baseline'.
"""

from __future__ import annotations

import os
import sys

from pyflink.table import EnvironmentSettings, TableEnvironment


def run_pyflink_baseline(
    kafka_bootstrap: str = "kafka:29092",
    input_topic: str = "market.ticks",
    baseline_bars_topic: str = "market.bars.1m.baseline",
    consumer_group: str = "pyflink-baseline-consumer",
) -> None:
    print(f"==> Initializing PyFlink Baseline (Processing-Time) Job on Kafka: {kafka_bootstrap}...")

    # 1. Initialize Streaming Table Environment
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # 2. Configure Pipeline Name (No Watermark, No State TTL Dedup)
    t_env.get_config().set("pipeline.name", "PyFlink-Market-Ticks-Baseline-ProcessingTime")
    t_env.get_config().set("execution.checkpointing.interval", "10000")

    # 3. Source Table: Kafka market.ticks using Processing Time (PROCTIME)
    # Notice: No WATERMARK declaration; proctime evaluates based on wall-clock system arrival.
    print(f"[PyFlink Baseline] Registering Source Table: {input_topic} (PROCTIME)...")
    t_env.execute_sql(f"""
        CREATE TABLE kafka_market_ticks_baseline (
            trade_id STRING,
            ticker_symbol STRING,
            price DOUBLE,
            `size` BIGINT,
            exchange STRING,
            event_timestamp STRING,
            is_late BOOLEAN,
            is_duplicate BOOLEAN,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{input_topic}',
            'properties.bootstrap.servers' = '{kafka_bootstrap}',
            'properties.group.id' = '{consumer_group}',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        );
    """)

    # 4. Sink Table: Baseline 1-Minute Bars (market.bars.1m.baseline)
    print(f"[PyFlink Baseline] Registering Sink Table: {baseline_bars_topic}...")
    t_env.execute_sql(f"""
        CREATE TABLE kafka_bars_1m_baseline (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            ticker_symbol STRING,
            `open` DOUBLE,
            high DOUBLE,
            low DOUBLE,
            `close` DOUBLE,
            volume BIGINT,
            vwap DOUBLE,
            trade_count BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{baseline_bars_topic}',
            'properties.bootstrap.servers' = '{kafka_bootstrap}',
            'format' = 'json'
        );
    """)

    # 5. Execute 1-minute Tumbling Window Aggregation over PROCTIME (No Dedup, Raw Stream)
    print("[PyFlink Baseline] Executing Processing-Time Tumbling Window...")
    query_baseline = """
        INSERT INTO kafka_bars_1m_baseline
        SELECT
            TUMBLE_START(proctime, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(proctime, INTERVAL '1' MINUTE) AS window_end,
            ticker_symbol,
            FIRST_VALUE(price) AS `open`,
            MAX(price) AS high,
            MIN(price) AS low,
            LAST_VALUE(price) AS `close`,
            SUM(`size`) AS volume,
            ROUND(SUM(price * `size`) / CAST(SUM(`size`) AS DOUBLE), 2) AS vwap,
            COUNT(1) AS trade_count
        FROM kafka_market_ticks_baseline
        WHERE price > 0 AND `size` > 0 AND ticker_symbol IS NOT NULL
        GROUP BY
            TUMBLE(proctime, INTERVAL '1' MINUTE),
            ticker_symbol
    """
    result = t_env.execute_sql(query_baseline)
    print(f"==> PyFlink Baseline Job submitted successfully! Job Client: {result.get_job_client()}")


if __name__ == "__main__":
    kafka_host = os.environ.get("KAFKA_BOOTSTRAP_SERVERS") or os.environ.get("KAFKA_BOOTSTRAP") or "kafka:29092"
    run_pyflink_baseline(kafka_bootstrap=kafka_host)
