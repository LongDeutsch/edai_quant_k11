"""SCALE / offline-online window / corpus replay / DP3-alpha conflict regressions.

Manual smoke (optional):
  make generate-data SCALE=0.1
  make stream-test
  make lakehouse-offline
  make alpha-seed && make alpha-metrics
"""

from __future__ import annotations

import json
from typing import Union

import pandas as pd
import pytest

from quant_edai.generator.data_generator import QuantDataGenerator
from quant_edai.generator.streaming_producer import StreamingTickProducer
from quant_edai.generator.tick_corpus import (
    DEFAULT_OFFLINE_END,
    DEFAULT_OFFLINE_START,
    DEFAULT_ONLINE_END,
    DEFAULT_ONLINE_START,
    materialize_tick_corpus,
)
from quant_edai.processing.features.feature_calculator import run_feature_calculation
from quant_edai.processing.spark.dedup_job import DeduplicationAndSchemaEvolutionJob
from quant_edai.quant.alpha_seed import _load_feature_rows, run_seed


def test_scale_no_longer_scales_ohlcv_volume(tmp_path):
    """SCALE must not multiply OHLCV volume; tick corpus rows grow ~linearly with SCALE."""
    window = dict(start_date="2024-01-02", end_date="2024-02-29", seed=42)
    out_lo = tmp_path / "ohlcv_lo"
    out_hi = tmp_path / "ohlcv_hi"
    gen_lo = QuantDataGenerator(scale=0.1, out_dir=str(out_lo), **window)
    gen_hi = QuantDataGenerator(scale=1.0, out_dir=str(out_hi), **window)
    stats_lo = gen_lo.run()
    stats_hi = gen_hi.run()

    assert stats_lo["total_market_rows"] == stats_hi["total_market_rows"]
    assert stats_lo.get("scale_mode") == "tick_budget"
    assert stats_lo.get("ohlcv_volume_scaled_by_scale") is False
    assert stats_hi.get("ohlcv_volume_scaled_by_scale") is False

    df_lo = pd.read_parquet(gen_lo.out_dir / "raw_market_ohlcv")
    df_hi = pd.read_parquet(gen_hi.out_dir / "raw_market_ohlcv")
    # Identical volumes (same seed/window; SCALE unused for volume)
    assert df_lo["volume"].tolist() == df_hi["volume"].tolist()
    mean_lo = float(df_lo["volume"].mean())
    mean_hi = float(df_hi["volume"].mean())
    assert mean_hi < mean_lo * 2.0  # not ~×10

    tick_lo = materialize_tick_corpus(
        out_dir=str(tmp_path / "ticks_lo"),
        start_date="2025-11-03",
        end_date="2025-11-28",
        scale=0.1,
        seed=42,
        corpus_name="online",
    )
    tick_hi = materialize_tick_corpus(
        out_dir=str(tmp_path / "ticks_hi"),
        start_date="2025-11-03",
        end_date="2025-11-28",
        scale=1.0,
        seed=42,
        corpus_name="online",
    )
    ratio = tick_hi["total_ticks"] / max(1, tick_lo["total_ticks"])
    assert 8.0 <= ratio <= 12.0  # ~×10 with dup inflation noise


def test_offline_online_windows_disjoint():
    offline_start = pd.Timestamp(DEFAULT_OFFLINE_START)
    offline_end = pd.Timestamp(DEFAULT_OFFLINE_END)
    online_start = pd.Timestamp(DEFAULT_ONLINE_START)
    online_end = pd.Timestamp(DEFAULT_ONLINE_END)

    assert offline_end < online_start
    offline_span_years = (offline_end - offline_start).days / 365.25
    online_span_days = (online_end - online_start).days
    assert offline_span_years >= 2.9
    assert online_span_days >= 360


def test_online_corpus_fault_ratios(tmp_path):
    manifest = materialize_tick_corpus(
        out_dir=str(tmp_path),
        start_date="2025-11-01",
        end_date="2026-02-28",
        scale=0.2,
        seed=42,
        corpus_name="online",
    )
    assert 0.04 <= manifest["late_arrival_ratio"] <= 0.06
    assert 0.010 <= manifest["duplicate_ratio"] <= 0.020
    assert 0.75 <= manifest["mega_cap_tick_row_share"] <= 0.85
    from pathlib import Path

    run_root = Path(manifest["run_root"])
    assert (run_root / "raw_market_ticks_online").exists()
    assert not (run_root / "raw_market_ohlcv").exists()


def test_stream_replay_reads_corpus(tmp_path):
    start, end = "2025-11-03", "2025-12-31"
    manifest = materialize_tick_corpus(
        out_dir=str(tmp_path),
        start_date=start,
        end_date=end,
        scale=0.1,
        seed=7,
        corpus_name="online",
    )
    from pathlib import Path

    run_root = Path(manifest["run_root"])
    out_file = run_root / "streaming_ticks.jsonl"
    producer = StreamingTickProducer(seed=7)
    stats = producer.stream_to_kafka_or_disk(
        out_file=str(out_file),
        publish_kafka=False,
        from_corpus=str(run_root / "raw_market_ticks_online"),
        max_ticks=500,
    )
    assert stats["mode"] == "corpus_replay"
    assert stats["total_streaming_ticks"] > 0
    assert out_file.exists()

    rows = [json.loads(line) for line in out_file.read_text().splitlines() if line.strip()]
    assert rows

    def _naive(value: Union[str, pd.Timestamp]) -> pd.Timestamp:
        ts = pd.Timestamp(value)
        if ts.tzinfo is not None:
            return ts.tz_convert("UTC").tz_localize(None)
        return ts

    lo = _naive(start)
    hi = _naive(end) + pd.Timedelta(days=1)
    for row in rows:
        assert lo <= _naive(row["event_timestamp"]) <= hi


def test_lakehouse_offline_no_gold_from_generator(tmp_path):
    from pathlib import Path

    out_dir = tmp_path / "lh"
    gen = QuantDataGenerator(
        seed=3,
        scale=0.01,
        out_dir=str(out_dir),
        start_date=DEFAULT_OFFLINE_START,
        end_date="2023-01-31",  # short slice inside offline window for CI speed
    )
    stats = gen.run()
    run_dir = gen.out_dir
    assert stats.get("gold_features_written") is False
    assert not (run_dir / "gold_features").exists()
    assert stats.get("scale_mode") == "tick_budget"

    DeduplicationAndSchemaEvolutionJob(
        source_dir=str(run_dir),
        target_dir=str(run_dir / "stg_market_ohlcv"),
    ).run(engine="sim")
    gold = run_dir / "gold_features" / "feat_ticker_technical_indicators.parquet"
    run_feature_calculation(
        source_path=str(run_dir / "stg_market_ohlcv"),
        target_path=str(gold),
        engine="sim",
        out_file=None,
    )
    assert gold.exists()
    silver = pd.read_parquet(run_dir / "stg_market_ohlcv")
    feats = pd.read_parquet(gold)
    assert set(feats["ticker_symbol"]).issubset(set(silver["ticker_symbol"]))


def test_alpha_seed_metrics_ignore_tick_corpus(tmp_path):
    """Alpha seed must not read tick parquet; synthetic fallback without gold."""
    from pathlib import Path

    out_dir = tmp_path / "ticks_only"
    manifest = materialize_tick_corpus(
        out_dir=str(out_dir),
        start_date="2025-11-03",
        end_date="2025-11-14",
        scale=0.1,
        seed=1,
        corpus_name="online",
    )
    run_root = Path(manifest["run_root"])
    assert (run_root / "raw_market_ticks_online").exists()
    df_syn, source_syn = _load_feature_rows(str(run_root))
    assert source_syn == "synthetic_panel"
    assert not df_syn.empty

    # With gold present, seed loads gold_features path (not ticks)
    gold_dir = tmp_path / "with_gold"
    gold_dir.mkdir()
    panel = pd.DataFrame(
        [
            {
                "ticker_symbol": "AAPL",
                "event_timestamp": "2024-06-03",
                "close_price": 180.0,
                "sma_20": 178.0,
                "sma_50": 175.0,
                "sma_ratio_20_50": 1.017,
                "rsi_14d": 55.0,
                "macd_histogram": 0.2,
                "rolling_volatility_30d": 0.2,
                "intraday_return_1d": 0.01,
            }
        ]
    )
    man2 = materialize_tick_corpus(
        out_dir=str(gold_dir),
        start_date="2025-11-03",
        end_date="2025-11-07",
        scale=0.1,
        seed=2,
        corpus_name="online",
    )
    gold_run = Path(man2["run_root"])
    (gold_run / "gold_features").mkdir(parents=True, exist_ok=True)
    feat_file = gold_run / "gold_features" / "feat_ticker_technical_indicators.parquet"
    panel.to_parquet(feat_file, index=False)
    df_gold, source_gold = _load_feature_rows(str(gold_run))
    assert "gold_features" in source_gold
    assert "raw_market_ticks" not in source_gold
    assert list(df_gold["ticker_symbol"]) == ["AAPL"]

    db_path = tmp_path / "paper.db"
    result = run_seed(source_dir=str(gold_run), sqlite_path=str(db_path))
    assert result["status"] in {"SUCCESS", "PASSED_SIMULATED"}
