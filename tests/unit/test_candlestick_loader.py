"""Unit tests for candlestick loader (ClickHouse primary, Lakehouse Parquet fallback)."""

import io
import json
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

from quant_edai.serving.candlestick_loader import load_candlestick_data


def test_load_candlestick_data_clickhouse_intraday_success():
    """Verify that when ClickHouse returns intraday bars, it is returned directly."""
    mock_ch_payload = {
        "meta": [{"name": "event_timestamp", "type": "DateTime"}],
        "data": [
            {
                "event_timestamp": "2024-03-15 09:30:00",
                "open_price": 180.0,
                "high_price": 182.0,
                "low_price": 179.5,
                "close_price": 181.0,
                "volume": 50000,
                "vwap": 180.5,
            },
            {
                "event_timestamp": "2024-03-15 09:31:00",
                "open_price": 181.0,
                "high_price": 183.0,
                "low_price": 180.5,
                "close_price": 182.5,
                "volume": 60000,
                "vwap": 181.8,
            },
        ],
        "rows": 2,
    }

    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps(mock_ch_payload).encode("utf-8")
    mock_resp.__enter__.return_value = mock_resp

    with patch("urllib.request.urlopen", return_value=mock_resp) as mock_urlopen:
        df, source = load_candlestick_data("AAPL")

        assert "ClickHouse OLAP" in source
        assert "market_intraday_bars" in source
        assert len(df) == 2
        assert list(df["close_price"]) == [181.0, 182.5]
        # Confirm HTTP request was made to ClickHouse port :8123
        call_req = mock_urlopen.call_args[0][0]
        assert "8123" in call_req.full_url


def test_load_candlestick_data_clickhouse_daily_fallback():
    """Verify fallback to daily table if intraday table is empty in ClickHouse."""
    mock_empty_payload = {"data": [], "rows": 0}
    mock_daily_payload = {
        "data": [
            {
                "event_timestamp": "2024-03-15",
                "open_price": 170.0,
                "high_price": 175.0,
                "low_price": 169.0,
                "close_price": 174.0,
                "volume": 1000000,
                "vwap": 172.5,
            }
        ],
        "rows": 1,
    }

    mock_resp_empty = MagicMock()
    mock_resp_empty.read.return_value = json.dumps(mock_empty_payload).encode("utf-8")
    mock_resp_empty.__enter__.return_value = mock_resp_empty

    mock_resp_daily = MagicMock()
    mock_resp_daily.read.return_value = json.dumps(mock_daily_payload).encode("utf-8")
    mock_resp_daily.__enter__.return_value = mock_resp_daily

    with patch("urllib.request.urlopen", side_effect=[mock_resp_empty, mock_resp_daily]):
        df, source = load_candlestick_data("MSFT")

        assert "ClickHouse OLAP" in source
        assert "market_daily_ohlcv" in source
        assert len(df) == 1
        assert df.iloc[0]["close_price"] == 174.0


def test_load_candlestick_data_offline_fallback(tmp_path):
    """Verify graceful degradation to parquet or simulated data when ClickHouse is offline."""
    # Simulate network failure to ClickHouse
    with patch("urllib.request.urlopen", side_effect=Exception("Connection refused")):
        df, source = load_candlestick_data("NVDA")
        assert len(df) > 0
        assert source in [
            "Lakehouse Storage (/tmp/lakehouse_source)",
            "Paper DB Session Store (SQLite)",
            "Paper DB Session Store (PostgreSQL)",
            "Simulated Real-time Feed",
        ]
        assert "close_price" in df.columns
        assert "open_price" in df.columns
