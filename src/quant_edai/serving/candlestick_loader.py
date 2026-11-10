"""Candlestick and market bar data loader for dashboards and serving.

Prioritizes ClickHouse OLAP serving (quant_dw.market_intraday_bars, M1-19).
Falls back to Lakehouse raw parquet if ClickHouse is offline,
and to deterministic simulation if neither is available.
"""

from __future__ import annotations

import glob
import json
import os
import urllib.request
from typing import Tuple

import numpy as np
import pandas as pd


def load_candlestick_data(ticker: str) -> Tuple[pd.DataFrame, str]:
    """Fetch candlestick OHLCV data for given ticker with multi-tier priority.

    Priority:
    1. ClickHouse OLAP (quant_dw.market_intraday_bars or market_daily_ohlcv) via HTTP :8123
    2. Lakehouse Raw Parquet (/tmp/lakehouse_source/raw_market_ohlcv/**/*.parquet)
    3. Deterministic Synthetic fallback
    """
    # 1. Try ClickHouse HTTP Interface (:8123)
    ch_host = os.getenv("CLICKHOUSE_HOST", "localhost")
    ch_port = os.getenv("CLICKHOUSE_PORT", "8123")
    ch_user = os.getenv("CLICKHOUSE_USER", "quant")
    ch_password = os.getenv("CLICKHOUSE_PASSWORD", "quant_password")
    ch_url = f"http://{ch_host}:{ch_port}/"

    ch_query = (
        f"SELECT event_timestamp, open_price, high_price, low_price, close_price, volume, vwap "
        f"FROM quant_dw.market_intraday_bars "
        f"WHERE ticker_symbol = '{ticker}' "
        f"ORDER BY event_timestamp DESC LIMIT 150 "
        f"FORMAT JSON"
    )
    try:
        url_with_auth = f"{ch_url}?user={ch_user}&password={ch_password}"
        req = urllib.request.Request(
            url_with_auth,
            data=ch_query.encode("utf-8"),
            headers={"Content-Type": "text/plain"},
        )
        with urllib.request.urlopen(req, timeout=1.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if "data" in data and len(data["data"]) > 0:
                df = pd.DataFrame(data["data"]).sort_values("event_timestamp")
                return df, f"ClickHouse OLAP ({ch_host}:{ch_port} - market_intraday_bars)"

            # Fallback to daily table if intraday is empty
            ch_daily_query = (
                f"SELECT toString(trade_date) AS event_timestamp, open_price, high_price, low_price, close_price, volume, vwap "
                f"FROM quant_dw.market_daily_ohlcv "
                f"WHERE ticker_symbol = '{ticker}' "
                f"ORDER BY trade_date DESC LIMIT 150 "
                f"FORMAT JSON"
            )
            req_daily = urllib.request.Request(
                url_with_auth,
                data=ch_daily_query.encode("utf-8"),
                headers={"Content-Type": "text/plain"},
            )
            with urllib.request.urlopen(req_daily, timeout=1.0) as resp_d:
                data_d = json.loads(resp_d.read().decode("utf-8"))
                if "data" in data_d and len(data_d["data"]) > 0:
                    df = pd.DataFrame(data_d["data"]).sort_values("event_timestamp")
                    return df, f"ClickHouse OLAP ({ch_host}:{ch_port} - market_daily_ohlcv)"
    except Exception:
        pass

    # 2. Try Lakehouse Raw Parquet Store
    parquet_pattern = "/tmp/lakehouse_source/raw_market_ohlcv/**/*.parquet"
    files = glob.glob(parquet_pattern, recursive=True)
    if files:
        df_list = []
        for f in files[:10]:
            try:
                sub = pd.read_parquet(f)
                if "ticker_symbol" in sub.columns:
                    sub_t = sub[sub["ticker_symbol"] == ticker]
                    if not sub_t.empty:
                        df_list.append(sub_t)
            except Exception:
                continue
        if df_list:
            df = pd.concat(df_list, ignore_index=True)
            ts_col = "ingestion_timestamp" if "ingestion_timestamp" in df.columns else "event_timestamp"
            if ts_col in df.columns:
                df["event_timestamp"] = df[ts_col]
            df = df.sort_values("event_timestamp").head(150)
            return df, "Lakehouse Storage (/tmp/lakehouse_source)"

    # 3. Deterministic Synthetic fallback
    np.random.seed(hash(ticker) % 2**32)
    base_price = 150.0 + (hash(ticker) % 200)
    dates = pd.date_range("2024-03-15 09:30:00", periods=100, freq="min")
    prices = [base_price]
    for _ in range(99):
        prices.append(prices[-1] * (1.0 + np.random.normal(0, 0.0015)))
    prices = np.array(prices)

    df_synth = pd.DataFrame({
        "event_timestamp": dates.strftime("%Y-%m-%d %H:%M:%S"),
        "open_price": prices * 0.9995,
        "high_price": prices * 1.0010,
        "low_price": prices * 0.9990,
        "close_price": prices,
        "volume": np.random.randint(1000, 8000, size=100),
        "vwap": prices * 0.9998,
    })
    return df_synth, "Simulated Real-time Feed"
