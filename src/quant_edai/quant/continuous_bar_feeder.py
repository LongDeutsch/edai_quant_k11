"""Continuous Market Bar Feeder for Real-Time Paper Trading & Live Ops (P1).

Publishes 1-minute OHLCV market bars to Kafka topic ``market.bars.1m`` continuously.
Provides a realistic data plane feed so `market_replay --mode live` can run continuously
and dashboard Page 1/2 can observe real-time order fills, position changes, and PnL updates.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
import signal
import sys
import time
from typing import Dict, List, Optional

import numpy as np

from quant_edai.common.execution_status import is_integration_mode, normalize_mode
from quant_edai.common.kafka_env import resolve_kafka_bootstrap

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ContinuousBarFeeder")

DEFAULT_BOOTSTRAP = resolve_kafka_bootstrap()  # KAFKA_BOOTSTRAP or KAFKA_BOOTSTRAP_SERVERS
BARS_TOPIC = os.environ.get("PAPER_BARS_TOPIC", "market.bars.1m")


class ContinuousBarFeeder:
    """Streams simulated 1-minute market bars into Kafka continuously (smoke only).

    Bars are labelled ``source_layer=continuous_feeder``; under MODE=integration|full the
    feeder refuses to run and the trader rejects such bars (use Flink ``market.bars.1m``).
    """

    TICKERS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "JPM"]
    BASE_PRICES = {
        "AAPL": 180.0,
        "MSFT": 420.0,
        "NVDA": 120.0,
        "AMZN": 175.0,
        "GOOGL": 155.0,
        "META": 490.0,
        "TSLA": 210.0,
        "JPM": 195.0,
    }

    def __init__(
        self,
        kafka_bootstrap: Optional[str] = None,
        topic: str = BARS_TOPIC,
        interval_sec: float = 1.0,
        max_cycles: int = 0,  # 0 = infinite continuous
    ):
        if is_integration_mode():
            raise RuntimeError(
                f"[MODE={normalize_mode()}] continuous_bar_feeder emits synthetic bars (smoke only); "
                "integration|full must consume Flink-produced market.bars.1m."
            )
        self.bootstrap = resolve_kafka_bootstrap(kafka_bootstrap)
        self.topic = topic
        self.interval = max(0.1, float(interval_sec))
        self.max_cycles = max_cycles
        self.running = True
        self.prices = dict(self.BASE_PRICES)
        self.producer = None

    def _connect_kafka(self) -> bool:
        try:
            from kafka import KafkaProducer
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                acks=1,
                request_timeout_ms=5000,
            )
            logger.info("Connected to Kafka cluster at %s for continuous bar publishing.", self.bootstrap)
            return True
        except Exception as e:
            logger.warning("Could not connect to Kafka at %s: %s (Running in simulated loop mode).", self.bootstrap, e)
            self.producer = None
            return False

    def stop(self, *args) -> None:
        logger.info("Stopping Continuous Bar Feeder gracefully...")
        self.running = False

    def run(self) -> None:
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

        self._connect_kafka()
        logger.info(
            "Continuous Bar Feeder started: streaming 8 tickers to topic '%s' every %ss (max_cycles=%s)...",
            self.topic,
            self.interval,
            "INFINITE" if self.max_cycles <= 0 else self.max_cycles,
        )

        cycle = 0
        total_bars_sent = 0
        start_time = time.time()

        try:
            while self.running:
                cycle += 1
                now_ts = datetime.now(timezone.utc).isoformat()
                bars_this_cycle = []

                for ticker in self.TICKERS:
                    curr_p = self.prices[ticker]
                    # Random walk with slight drift and volatility
                    ret = float(np.random.normal(loc=0.0001, scale=0.0018))
                    new_p = max(1.0, curr_p * (1.0 + ret))
                    self.prices[ticker] = new_p

                    volume = float(np.random.randint(800, 8000))
                    p_ml = float(np.clip(0.50 + 25.0 * ret, 0.05, 0.95))

                    bar_payload = {
                        "ticker_symbol": ticker,
                        "timestamp": now_ts,
                        "open_price": round(new_p * 0.9998, 2),
                        "high_price": round(new_p * 1.0006, 2),
                        "low_price": round(new_p * 0.9994, 2),
                        "close_price": round(new_p, 2),
                        "volume": volume,
                        "vwap": round(new_p * 0.9999, 2),
                        "p_ml": round(p_ml, 4),
                        "source_layer": "continuous_feeder",
                    }
                    bars_this_cycle.append(bar_payload)

                    if self.producer is not None:
                        try:
                            self.producer.send(self.topic, key=ticker, value=bar_payload)
                        except Exception as e:
                            logger.debug("Failed sending bar for %s: %s", ticker, e)

                if self.producer is not None:
                    try:
                        self.producer.flush(timeout=3)
                    except Exception:
                        pass

                total_bars_sent += len(bars_this_cycle)
                if cycle % 10 == 0 or cycle == 1:
                    logger.info(
                        "[Cycle %s] Published %s 1m bars (Total: %s bars) | Sample AAPL: $%.2f, NVDA: $%.2f",
                        cycle,
                        len(bars_this_cycle),
                        total_bars_sent,
                        self.prices["AAPL"],
                        self.prices["NVDA"],
                    )

                if self.max_cycles > 0 and cycle >= self.max_cycles:
                    logger.info("Reached maximum cycles limit (%s). Exiting feeder.", self.max_cycles)
                    break

                time.sleep(self.interval)

        finally:
            if self.producer is not None:
                try:
                    self.producer.close(timeout=3)
                except Exception:
                    pass
            elapsed = time.time() - start_time
            logger.info("Continuous Feeder finished. Total bars published: %s over %.1fs.", total_bars_sent, elapsed)


def main() -> None:
    parser = argparse.ArgumentParser(description="Continuous 1m Market Bar Feeder to Kafka")
    parser.add_argument("--kafka-bootstrap", type=str, default=None, help="Defaults to KAFKA_BOOTSTRAP / KAFKA_BOOTSTRAP_SERVERS")
    parser.add_argument("--topic", type=str, default=BARS_TOPIC)
    parser.add_argument("--interval", type=float, default=1.0, help="Interval in seconds between 1m bar cycles")
    parser.add_argument("--max-cycles", type=int, default=0, help="Max cycles (0 = infinite)")
    args = parser.parse_args()

    feeder = ContinuousBarFeeder(
        kafka_bootstrap=args.kafka_bootstrap,
        topic=args.topic,
        interval_sec=args.interval,
        max_cycles=args.max_cycles,
    )
    feeder.run()


if __name__ == "__main__":
    main()
