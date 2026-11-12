"""Dual-mode Paper Trading runner (M1-38).

Modes
-----
* ``live``   — **Kafka consumer only** on ``market.bars.1m``.
  Data must already be on Kafka (e.g. ``make trader-publish-bronze`` which
  reads Bronze then publishes, or Flink aggregating ``market.ticks``).
  The trader process itself never opens Bronze/Parquet files.
* ``replay`` — offline **synthetic** 1m bars for unit/smoke tests only
  (explicitly not a Parquet lakehouse reader).

MODE=integration|full (P1.4):
* only ``live`` (Flink ``market.bars.1m``) + ``--alpha-source store`` (ML signals) are allowed;
  synthetic replay / inline gate raise instead of silently running.
* bars from synthetic / bronze-expansion publishers are rejected.
* every order & fill must carry model_version, dataset_id, feature_source (else FAIL).
* EOD flatten is 15:55 America/New_York (``common.market_time``).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import time
from typing import Any, Dict, Generator, Optional

import numpy as np

from quant_edai.common.execution_status import build_status, is_integration_mode, normalize_mode
from quant_edai.common.kafka_env import resolve_kafka_bootstrap
from quant_edai.common.market_time import MARKET_TZ, bar_timestamp_policy
from quant_edai.common.ml_provenance import PROVENANCE_FIELDS, is_real_provenance_value
from quant_edai.quant.paper_db import PaperDatabase
from quant_edai.quant.paper_trading import MarketBar, PaperTradingEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("MarketReplay")

DEFAULT_BOOTSTRAP = resolve_kafka_bootstrap()  # KAFKA_BOOTSTRAP or KAFKA_BOOTSTRAP_SERVERS
DEFAULT_TOPIC = os.environ.get("PAPER_BARS_TOPIC", "market.bars.1m")

# Bars from these producers are never valid input for integration|full trading.
NON_PRODUCTION_SOURCE_LAYERS = frozenset({"synthetic", "synthetic_replay", "continuous_feeder", "bronze"})


def kafka_message_to_bar(payload: Dict[str, Any]) -> Optional[MarketBar]:
    """Map Kafka JSON (bars or tick-like) into MarketBar."""
    ticker = payload.get("ticker_symbol") or payload.get("ticker")
    if not ticker:
        return None
    ts = (
        payload.get("event_timestamp")
        or payload.get("timestamp")
        or payload.get("bar_timestamp")
        or payload.get("window_start")  # PyFlink / Flink-sim tumbling window start
        or datetime.now(timezone.utc).isoformat()
    )
    prov = {
        "model_version": payload.get("model_version"),
        "dataset_id": payload.get("dataset_id"),
        "feature_source": payload.get("feature_source"),
        "source_layer": payload.get("source_layer"),
    }
    # Tick payload: price/size only → synthesize a 1-tick bar
    if "close_price" not in payload and "price" in payload:
        px = float(payload["price"])
        return MarketBar(
            ticker_symbol=str(ticker),
            timestamp=str(ts),
            open_price=px,
            high_price=px,
            low_price=px,
            close_price=px,
            volume=float(payload.get("size", payload.get("volume", 1.0))),
            vwap=px,
            p_ml=float(payload["p_ml"]) if payload.get("p_ml") is not None else None,
            **prov,
        )
    close_px = float(payload.get("close_price", payload.get("close", 0.0)))
    if close_px <= 0:
        return None
    return MarketBar(
        ticker_symbol=str(ticker),
        timestamp=str(ts),
        open_price=float(payload.get("open_price", payload.get("open", close_px))),
        high_price=float(payload.get("high_price", payload.get("high", close_px))),
        low_price=float(payload.get("low_price", payload.get("low", close_px))),
        close_price=close_px,
        volume=float(payload.get("volume", 1000.0)),
        vwap=(
            float(payload["vwap"])
            if payload.get("vwap") is not None
            else (float(payload["vwap_1m"]) if payload.get("vwap_1m") is not None else None)
        ),
        p_ml=float(payload["p_ml"]) if payload.get("p_ml") is not None else None,
        **prov,
    )


class MarketReplayRunner:
    """Feeds bars to PaperTradingEngine via Kafka live or synthetic replay."""

    def __init__(
        self,
        mode: str = "replay",
        speed: float = 20.0,
        reset_db: bool = False,
        max_bars: int = 500,
        output_dir: str = "evidence/phase6",
        profile: str = "intraday",
        alpha_source: str = "store",
        sl_pct: Optional[float] = 0.05,
        kafka_bootstrap: Optional[str] = None,
        kafka_topic: str = DEFAULT_TOPIC,
        kafka_timeout_sec: float = 45.0,
        consumer_group: Optional[str] = None,
        db: Optional[PaperDatabase] = None,
        model_version: Optional[str] = None,
        dataset_id: Optional[str] = None,
        feature_source: Optional[str] = None,
    ):
        self.mode = mode.lower()
        self.run_mode = normalize_mode()
        if is_integration_mode(self.run_mode):
            # No synthetic default gate: Flink bars + ML (store) signals only.
            if self.mode != "live":
                raise RuntimeError(
                    f"[MODE={self.run_mode}] trader requires --mode live (Flink market.bars.1m); "
                    f"synthetic '{self.mode}' replay is smoke-only."
                )
            if str(alpha_source).lower() != "store":
                raise RuntimeError(
                    f"[MODE={self.run_mode}] trader requires --alpha-source store (ML signals); "
                    f"'{alpha_source}' inline gate is smoke-only."
                )
        self.speed = max(1.0, float(speed))
        self.reset_db = reset_db
        self.max_bars = max_bars
        self.output_dir = Path(output_dir)
        self.profile = profile
        self.alpha_source = alpha_source
        self.sl_pct = sl_pct
        self.kafka_bootstrap = resolve_kafka_bootstrap(kafka_bootstrap)
        self.kafka_topic = kafka_topic
        self.kafka_timeout_sec = kafka_timeout_sec
        self.consumer_group = consumer_group or f"paper-trader-{int(time.time())}"

        self.db = db if db is not None else PaperDatabase()
        if self.reset_db:
            logger.info("Resetting paper trading database tables (--reset-db)...")
            self.db.init_db(reset=True)
        else:
            self.db.init_db(reset=False)

        self.engine = PaperTradingEngine(
            db=self.db,
            starting_cash=100_000.0,
            trade_notional=10_000.0,
            profile=self.profile,
            alpha_source=self.alpha_source,
            sl_pct=self.sl_pct,
            model_version=model_version,
            dataset_id=dataset_id,
            feature_source=feature_source,
        )
        self.rejected_non_production_bars = 0

    def synthetic_bar_stream(self) -> Generator[MarketBar, None, None]:
        """Offline smoke stream — synthetic only (no Parquet / no Kafka)."""
        logger.info("Generating synthetic 1-minute intraday stream for offline replay...")
        tickers = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "JPM"]
        base_prices = {
            "AAPL": 180.0,
            "MSFT": 420.0,
            "NVDA": 120.0,
            "AMZN": 175.0,
            "GOOGL": 155.0,
            "META": 490.0,
            "TSLA": 210.0,
            "JPM": 195.0,
        }
        bar_count = 0
        minute = 0
        while (self.max_bars <= 0) or (bar_count < self.max_bars):
            # Wrap within a trading-day-like clock for readability
            minute_of_session = (30 + minute) % (6 * 60)
            hour = 9 + minute_of_session // 60
            min_of_hour = minute_of_session % 60
            # Exchange-local (America/New_York) with explicit offset: unambiguous EOD handling.
            ts_str = datetime(2024, 3, 15, hour, min_of_hour, tzinfo=MARKET_TZ).isoformat()
            for t in tickers:
                if self.max_bars > 0 and bar_count >= self.max_bars:
                    return
                volatility = 0.0015
                drift = 0.0001 * np.sin(minute / 30.0)
                ret = float(np.random.normal(loc=drift, scale=volatility))
                curr_price = base_prices[t] * (1.0 + ret)
                base_prices[t] = curr_price
                volume = float(np.random.randint(500, 5000))
                p_ml = float(np.clip(0.50 + 20.0 * ret, 0.1, 0.9))
                yield MarketBar(
                    ticker_symbol=t,
                    timestamp=ts_str,
                    open_price=round(curr_price * 0.9998, 2),
                    high_price=round(curr_price * 1.0008, 2),
                    low_price=round(curr_price * 0.9992, 2),
                    close_price=round(curr_price, 2),
                    volume=volume,
                    vwap=round(curr_price * 0.9999, 2),
                    p_ml=p_ml,
                    source_layer="synthetic_replay",
                )
                bar_count += 1
            minute += 1

    def kafka_bar_stream(self) -> Generator[MarketBar, None, None]:
        """Consume ``market.bars.1m`` (or configured topic) from Kafka only."""
        try:
            from kafka import KafkaConsumer
        except ImportError as e:
            raise RuntimeError(
                "kafka-python is required for live mode. Install project kafka extra."
            ) from e

        logger.info(
            "LIVE mode: consuming Kafka topic '%s' at %s (group=%s, max_bars=%s, timeout=%ss)",
            self.kafka_topic,
            self.kafka_bootstrap,
            self.consumer_group,
            self.max_bars,
            self.kafka_timeout_sec,
        )
        consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_bootstrap,
            group_id=self.consumer_group,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=int(self.kafka_timeout_sec * 1000),
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        )
        yielded = 0
        try:
            for msg in consumer:
                payload = msg.value
                if not isinstance(payload, dict):
                    continue
                bar = kafka_message_to_bar(payload)
                if bar is None:
                    continue
                if is_integration_mode(self.run_mode) and bar.source_layer in NON_PRODUCTION_SOURCE_LAYERS:
                    self.rejected_non_production_bars += 1
                    continue
                yield bar
                yielded += 1
                if self.max_bars > 0 and yielded >= self.max_bars:
                    break
        finally:
            consumer.close()

        if yielded == 0 and self.rejected_non_production_bars:
            raise RuntimeError(
                f"[MODE={self.run_mode}] all {self.rejected_non_production_bars} bars on '{self.kafka_topic}' "
                f"came from non-production sources {sorted(NON_PRODUCTION_SOURCE_LAYERS)}; "
                "run the Flink job (make stream-flink) instead of synthetic/bronze publishers."
            )
        if yielded == 0:
            raise RuntimeError(
                f"No messages consumed from Kafka topic '{self.kafka_topic}'. "
                "Publish Bronze data first: `make trader-publish-bronze` "
                "(or run Flink so market.bars.1m is populated)."
            )
        logger.info("Consumed %s bars from Kafka topic '%s'", yielded, self.kafka_topic)

    def bar_stream_generator(self) -> Generator[MarketBar, None, None]:
        if self.mode == "live":
            yield from self.kafka_bar_stream()
        elif self.mode == "replay":
            yield from self.synthetic_bar_stream()
        else:
            raise ValueError(f"Unsupported mode: {self.mode}")

    def run(self) -> Dict[str, Any]:
        logger.info(
            "Starting Paper Trading (mode=%s, speed=%sx, max_bars=%s)...",
            self.mode,
            self.speed,
            self.max_bars,
        )
        start_wall_time = time.time()
        processed_bars = 0
        events_total = 0
        sleep_interval = 0.0 if self.mode == "live" else ((60.0 / self.speed) if self.speed < 500 else 0.0)

        for bar in self.bar_stream_generator():
            res = self.engine.on_bar(bar)
            processed_bars += 1
            if res and res["events"]:
                events_total += len(res["events"])
                logger.info(
                    "[%s] %s: Events %s | Equity: $%s | Open: %s",
                    bar.timestamp,
                    bar.ticker_symbol,
                    res["events"],
                    f"{res['equity']:,.2f}",
                    res["positions_count"],
                )
            if sleep_interval > 0:
                time.sleep(min(sleep_interval, 0.01))

        wall_duration = time.time() - start_wall_time
        trades = self.db.get_recent_trades(limit=1000)
        positions = self.db.get_positions()
        snapshots = self.db.get_snapshots(limit=1000)

        latencies = np.array(self.engine.latencies_ms) if self.engine.latencies_ms else np.array([0.0])
        p50_lat = float(np.percentile(latencies, 50))
        p95_lat = float(np.percentile(latencies, 95))
        p99_lat = float(np.percentile(latencies, 99))
        mean_lat = float(np.mean(latencies))

        final_equity = self.engine.cash + sum(
            p.current_mark_price * p.quantity for p in self.engine.positions.values()
        )
        total_pnl = final_equity - self.engine.starting_cash
        total_pnl_pct = (total_pnl / self.engine.starting_cash) * 100.0
        filled_trades_count = len(trades)
        winning_trades = sum(1 for tr in trades if float(tr.get("realized_pnl", 0.0)) > 0.0)
        exited_trades = sum(1 for tr in trades if float(tr.get("realized_pnl", 0.0)) != 0.0)
        win_rate = (winning_trades / exited_trades) if exited_trades > 0 else 0.0

        # Provenance evidence over this session's fills (orders carry the same columns)
        session_fills = [tr for tr in trades if tr.get("session_id") in (None, self.engine.session_id)]
        fills_missing = [
            tr.get("trade_id")
            for tr in session_fills
            if not all(is_real_provenance_value(tr.get(f)) for f in PROVENANCE_FIELDS)
        ]
        provenance_evidence = {
            "fills_checked": len(session_fills),
            "fills_missing_provenance": len(fills_missing),
            "missing_trade_ids": [str(t) for t in fills_missing[:20]],
            "dataset_ids": sorted({str(tr.get("dataset_id")) for tr in session_fills if tr.get("dataset_id")}),
            "model_versions": sorted({str(tr.get("model_version")) for tr in session_fills if tr.get("model_version")}),
            "feature_sources": sorted(
                {str(tr.get("feature_source")) for tr in session_fills if tr.get("feature_source")}
            ),
        }
        run_ok = not (is_integration_mode(self.run_mode) and (fills_missing or processed_bars == 0))
        status = build_status(
            ok=run_ok,
            mode=self.run_mode,
            engine="kafka" if self.mode == "live" else "sim",
            degraded_reason=(
                f"{len(fills_missing)} fills missing ML provenance"
                if fills_missing
                else ("no bars processed" if processed_bars == 0 else None)
            ),
        ).as_dict()

        exec_summary = {
            "session_id": self.engine.session_id,
            "mode": self.mode,
            "status": status["status"],
            "execution_mode": status["execution_mode"],
            "run_mode": self.run_mode,
            "dataset_id": self.engine.provenance.get("dataset_id")
            or (provenance_evidence["dataset_ids"][0] if provenance_evidence["dataset_ids"] else None),
            "model_version": self.engine.provenance.get("model_version")
            or (provenance_evidence["model_versions"][0] if provenance_evidence["model_versions"] else None),
            "feature_source": self.engine.provenance.get("feature_source")
            or (provenance_evidence["feature_sources"][0] if provenance_evidence["feature_sources"] else None),
            "provenance": provenance_evidence,
            "eod_flatten": {"timezone": "America/New_York", "cutoff": "15:55", "bar_ts_policy": bar_timestamp_policy(self.run_mode)},
            "rejected_non_production_bars": self.rejected_non_production_bars,
            "kafka_bootstrap_resolved": self.kafka_bootstrap if self.mode == "live" else None,
            "data_plane": "kafka" if self.mode == "live" else "synthetic_replay",
            "kafka_topic": self.kafka_topic if self.mode == "live" else None,
            "kafka_bootstrap": self.kafka_bootstrap if self.mode == "live" else None,
            "speed_multiplier": self.speed if self.mode == "replay" else None,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "execution": {
                "bars_processed": processed_bars,
                "wall_time_seconds": round(wall_duration, 2),
                "bars_per_second": round(processed_bars / wall_duration, 2) if wall_duration > 0 else 0.0,
                "filled_trades_count": filled_trades_count,
                "active_positions_count": len(positions),
                "events_count": events_total,
            },
            "financials": {
                "starting_cash": self.engine.starting_cash,
                "final_equity": round(final_equity, 2),
                "net_pnl": round(total_pnl, 2),
                "net_pnl_pct": round(total_pnl_pct, 4),
                "realized_pnl_cumulative": round(self.engine.realized_pnl_cumulative, 2),
                "win_rate": round(win_rate, 4),
                "max_drawdown_pct": round(
                    float(snapshots["max_drawdown_pct"].min() if len(snapshots) > 0 else 0.0),
                    4,
                ),
            },
            "risk_controls_verified": {
                "position_sizing_notional": self.engine.trade_notional,
                "max_positions_limit": self.engine.max_positions,
                "tp_bps": self.engine.tp_bps,
                "sl_bps": self.engine.sl_bps,
                "slippage_bps": self.engine.slippage_bps,
                "commission_bps": self.engine.commission_bps,
                "daily_circuit_breaker_pct": self.engine.max_daily_loss_pct,
                "eod_flattening_active": True,
            },
            "latency_sla": {
                "target_ms": 50.0,
                "mean_ms": round(mean_lat, 3),
                "p50_ms": round(p50_lat, 3),
                "p95_ms": round(p95_lat, 3),
                "p99_ms": round(p99_lat, 3),
                "sla_met": bool(p95_lat < 50.0),
            },
        }

        latency_report = {
            "target_sla_ms": 50.0,
            "bars_measured": int(len(self.engine.latencies_ms)),
            "mean_ms": round(mean_lat, 3),
            "p50_ms": round(p50_lat, 3),
            "p95_ms": round(p95_lat, 3),
            "p99_ms": round(p99_lat, 3),
            "max_ms": round(float(np.max(latencies)), 3),
            "min_ms": round(float(np.min(latencies)), 3),
            "sla_met": bool(p95_lat < 50.0),
            "mode": self.mode,
        }

        self.output_dir.mkdir(parents=True, exist_ok=True)
        exec_path = self.output_dir / "paper_trading_execution.json"
        lat_path = self.output_dir / "paper_trading_latency.json"
        with open(exec_path, "w", encoding="utf-8") as f:
            json.dump(exec_summary, f, indent=2)
        with open(lat_path, "w", encoding="utf-8") as f:
            json.dump(latency_report, f, indent=2)
        logger.info("Saved execution summary to %s", exec_path)
        logger.info("Saved latency report to %s", lat_path)
        if status["status"] == "FAILED":
            raise RuntimeError(
                f"[MODE={self.run_mode}] paper trading FAILED: {status.get('detail')} "
                f"(evidence: {exec_path})"
            )
        return exec_summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Real-time Paper Trading Runner")
    parser.add_argument("--mode", type=str, default="replay", choices=["replay", "live"])
    parser.add_argument("--speed", type=float, default=50.0, help="Synthetic replay speed only")
    parser.add_argument("--reset-db", action="store_true")
    parser.add_argument("--max-bars", type=int, default=300, help="Max bars to process (0 = infinite continuous stream)")
    parser.add_argument("--output-dir", type=str, default="evidence/phase6")
    parser.add_argument("--profile", type=str, default="intraday", choices=["intraday", "daily", "swing"])
    parser.add_argument("--alpha-source", type=str, default="store", choices=["store", "inline"])
    parser.add_argument(
        "--sl-pct",
        type=float,
        default=0.05,
        help="Stop-loss as fraction of entry (default 0.05 = 5%%). Set <=0 to use profile bps.",
    )
    parser.add_argument("--kafka-bootstrap", type=str, default=None, help="Defaults to KAFKA_BOOTSTRAP / KAFKA_BOOTSTRAP_SERVERS")
    parser.add_argument("--kafka-topic", type=str, default=DEFAULT_TOPIC)
    parser.add_argument("--kafka-timeout-sec", type=float, default=45.0)
    args = parser.parse_args()

    sl_pct = args.sl_pct if args.sl_pct and args.sl_pct > 0 else None
    runner = MarketReplayRunner(
        mode=args.mode,
        speed=args.speed,
        reset_db=args.reset_db,
        max_bars=args.max_bars,
        output_dir=args.output_dir,
        profile=args.profile,
        alpha_source=args.alpha_source,
        sl_pct=sl_pct,
        kafka_bootstrap=args.kafka_bootstrap,
        kafka_topic=args.kafka_topic,
        kafka_timeout_sec=args.kafka_timeout_sec,
    )
    summary = runner.run()

    print("\n" + "=" * 65)
    print("REAL-TIME PAPER TRADING EXECUTION SUMMARY (M1-38)")
    print("=" * 65)
    print(f"  Mode                     : {summary['mode']} ({summary.get('data_plane')})")
    if summary.get("kafka_topic"):
        print(f"  Kafka topic              : {summary['kafka_topic']}")
    print(f"  Bars Processed            : {summary['execution']['bars_processed']}")
    print(f"  Trades Filled             : {summary['execution']['filled_trades_count']}")
    print(f"  Final Equity              : ${summary['financials']['final_equity']:,.2f}")
    print(
        f"  Net PnL                   : ${summary['financials']['net_pnl']:+,.2f} "
        f"({summary['financials']['net_pnl_pct']:+.2f}%)"
    )
    print(f"  Win Rate                  : {summary['financials']['win_rate']*100:.1f}%")
    print(
        f"  Latency p95 (SLA < 50ms)  : {summary['latency_sla']['p95_ms']:.3f} ms "
        f"(SLA Met: {summary['latency_sla']['sla_met']})"
    )
    print("=" * 65)
    print(f"Evidence artifacts saved to {args.output_dir}/\n")


if __name__ == "__main__":
    main()
