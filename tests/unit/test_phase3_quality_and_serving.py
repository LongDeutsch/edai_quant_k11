"""Unit tests for Phase 3: Airflow, Great Expectations, DataHub Lineage, and Serving layers."""

import json
from pathlib import Path
import pandas as pd
import pytest

from quant_edai.processing.features.feature_calculator import compute_technical_indicators
from quant_edai.quality.gx_runner import run_gx_checkpoint
from quant_edai.quality.datahub_emitter import emit_pipeline_lineage
from quant_edai.serving.clickhouse_sync import ClickHouseSyncManager, sync_clickhouse_gold


def test_feature_calculator_indicators_and_audit_columns(tmp_path: Path):
    """Test feature calculator computes SMA, RSI, MACD, Volatility with event_timestamp and created."""
    # Synthetic OHLCV
    dates = pd.date_range("2026-01-01", periods=60, freq="D")
    df = pd.DataFrame({
        "ticker_symbol": ["AAPL"] * 60,
        "trade_date": dates,
        "open_price": [150.0 + i for i in range(60)],
        "high_price": [155.0 + i for i in range(60)],
        "low_price": [148.0 + i for i in range(60)],
        "close_price": [152.0 + i for i in range(60)],
        "volume": [1000000] * 60,
    })

    df_feats = compute_technical_indicators(df)

    assert "event_timestamp" in df_feats.columns, "Missing event_timestamp (M1-34)"
    assert "created" in df_feats.columns, "Missing created audit column (M1-34)"
    assert "sma_20" in df_feats.columns
    assert "sma_50" in df_feats.columns
    assert "rsi_14d" in df_feats.columns
    assert "macd_12_26" in df_feats.columns
    assert "rolling_volatility_30d" in df_feats.columns
    assert "intraday_return_1d" in df_feats.columns

    # Verify SMA 20 non-null on 20th row
    assert not pd.isna(df_feats.iloc[19]["sma_20"])
    # Verify SMA 50 non-null on 50th row
    assert not pd.isna(df_feats.iloc[49]["sma_50"])


def test_great_expectations_runner_bronze_suite(tmp_path: Path):
    """Test GX runner against bronze OHLCV suite."""
    df = pd.DataFrame({
        "ticker_symbol": ["AAPL", "MSFT", "NVDA"],
        "trade_date": ["2026-01-01", "2026-01-02", "2026-01-03"],
        "open_price": [150.0, 151.0, 152.0],
        "high_price": [155.0, 156.0, 157.0],
        "low_price": [149.0, 150.0, 151.0],
        "close_price": [153.0, 154.0, 155.0],
        "volume": [10000, 20000, 30000],
        "exchange": ["NASDAQ", "NASDAQ", "NYSE"],
    })
    p_file = tmp_path / "ohlcv.parquet"
    df.to_parquet(p_file)

    suite_path = "configs/quality/suite_raw_market_data.json"
    doc_out = tmp_path / "report.md"

    res = run_gx_checkpoint(data_path=str(p_file), suite_path=suite_path, doc_out_path=str(doc_out))
    assert res["success"] is True
    assert res["evaluated_expectations"] > 0
    assert doc_out.exists()


def test_datahub_lineage_emitter(tmp_path: Path):
    """Test DataHub lineage emitter generates structured graph and markdown mermaid."""
    out_json = tmp_path / "lineage.json"
    out_md = tmp_path / "lineage.md"

    lineage = emit_pipeline_lineage(out_json=str(out_json), out_md=str(out_md))

    assert "nodes" in lineage
    assert "edges" in lineage
    assert len(lineage["nodes"]) >= 4
    assert out_json.exists()
    assert out_md.exists()

    content = out_md.read_text()
    assert "mermaid" in content
    assert "flowchart LR" in content


def test_clickhouse_sync_sim_mode(tmp_path: Path):
    """Test ClickHouse sync execution under sim mode."""
    # Create mock dim_companies and OHLCV
    dim_dir = tmp_path / "dim_companies"
    dim_dir.mkdir(parents=True)
    df_dim = pd.DataFrame({
        "company_key": [1, 2],
        "ticker_symbol": ["AAPL", "MSFT"],
        "company_name": ["Apple", "Microsoft"],
        "sector": ["Tech", "Tech"],
        "industry": ["Hardware", "Software"],
        "market_cap_category": ["Mega", "Mega"],
        "valid_from_ts": ["2026-01-01 00:00:00", "2026-01-01 00:00:00"],
        "valid_to_ts": ["2099-12-31 23:59:59", "2099-12-31 23:59:59"],
        "is_current": [1, 1],
    })
    df_dim.to_parquet(dim_dir / "dim_companies.parquet")

    # Create mock raw_market_ohlcv for intraday bars
    ohlcv_dir = tmp_path / "raw_market_ohlcv" / "trade_date=2026-01-01"
    ohlcv_dir.mkdir(parents=True)
    df_ohlcv = pd.DataFrame({
        "ticker_symbol": ["AAPL", "AAPL"],
        "open_price": [150.0, 151.0],
        "high_price": [152.0, 153.0],
        "low_price": [149.0, 150.0],
        "close_price": [151.0, 152.0],
        "volume": [1000, 2000],
        "vwap": [150.5, 151.5],
        "trade_count": [10, 20],
        "ingestion_timestamp": ["2026-01-01 09:30:00", "2026-01-01 09:31:00"],
    })
    df_ohlcv.to_parquet(ohlcv_dir / "part.parquet")

    out_metrics = tmp_path / "ch_metrics.json"
    metrics = sync_clickhouse_gold(
        source_dir=str(tmp_path),
        engine="sim",
        out_file=str(out_metrics),
    )

    assert metrics["engine"] == "simulation_fallback"
    assert "dim_companies" in metrics["synced_tables"]
    assert "market_intraday_bars" in metrics["synced_tables"]
    assert metrics["synced_tables"]["market_intraday_bars"]["rows"] == 2
    assert out_metrics.exists()
