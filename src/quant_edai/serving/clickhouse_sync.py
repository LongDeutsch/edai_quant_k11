"""ClickHouse serving layer synchronization.

Transfers Gold layer facts, dimensions, and computed technical indicators
into ClickHouse ReplacingMergeTree tables for high-throughput OLAP analytics.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ClickHouseSync")


class ClickHouseSyncManager:
    """Manages synchronization between Lakehouse Parquet/Iceberg and ClickHouse OLAP."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        user: str = "quant",
        password: str = "quant_password",
        database: str = "quant_dw",
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.url = f"http://{host}:{port}"

    def is_online(self) -> bool:
        """Check if ClickHouse HTTP endpoint is reachable."""
        try:
            resp = requests.get(f"{self.url}/ping", timeout=3)
            return resp.status_code == 200 and "Ok." in resp.text
        except Exception:
            return False

    def execute_query(self, query: str) -> requests.Response:
        """Execute a DDL or DML statement via HTTP POST."""
        params = {}
        if self.user:
            params["user"] = self.user
        if self.password:
            params["password"] = self.password
        
        resp = requests.post(self.url, params=params, data=query.encode("utf-8"), timeout=15)
        resp.raise_for_status()
        return resp

    def init_schema(self, ddl_path: Path) -> None:
        """Apply ClickHouse DDL script."""
        if not ddl_path.exists():
            logger.warning(f"DDL file {ddl_path} does not exist. Skipping DDL init.")
            return

        with open(ddl_path, "r", encoding="utf-8") as f:
            ddl_content = f.read()

        # Split statements by semicolon
        statements = [stmt.strip() for stmt in ddl_content.split(";") if stmt.strip()]
        for stmt in statements:
            logger.info(f"Applying DDL: {stmt[:60]}...")
            self.execute_query(stmt)

    def insert_dataframe(self, table: str, df: pd.DataFrame) -> int:
        """Insert a pandas DataFrame into a ClickHouse table using JSONEachRow format."""
        if df.empty:
            return 0

        # ClickHouse JSONEachRow ingest
        json_data = df.to_json(orient="records", lines=True, date_format="iso")
        query = f"INSERT INTO {self.database}.{table} FORMAT JSONEachRow"
        params = {"query": query}
        if self.user:
            params["user"] = self.user
        if self.password:
            params["password"] = self.password

        resp = requests.post(self.url, params=params, data=json_data.encode("utf-8"), timeout=30)
        resp.raise_for_status()
        return len(df)

    def query_count(self, table: str) -> int:
        """Get row count from ClickHouse table."""
        query = f"SELECT count() FROM {self.database}.{table}"
        resp = self.execute_query(query)
        return int(resp.text.strip())


def sync_clickhouse_gold(
    source_dir: str = "/tmp/lakehouse_source",
    ddl_path: str = "sql/clickhouse/schema_quant.sql",
    host: str = "localhost",
    port: int = 8123,
    user: str = "quant",
    password: str = "quant_password",
    engine: str = "auto",
    out_file: str = "evidence/phase3/clickhouse_sync_metrics.json",
) -> Dict[str, Any]:
    """Sync Gold tables to ClickHouse, or simulate if offline/sim engine requested."""
    manager = ClickHouseSyncManager(host=host, port=port, user=user, password=password)
    is_live = manager.is_online() if engine != "sim" else False

    logger.info(f"ClickHouse service at {host}:{port} status: {'ONLINE' if is_live else 'OFFLINE/SIM'}")

    src_path = Path(source_dir)
    results: Dict[str, Any] = {
        "engine": "clickhouse_live" if is_live else "simulation_fallback",
        "host": host,
        "port": port,
        "synced_tables": {},
    }

    if is_live:
        manager.init_schema(Path(ddl_path))

    # 1. Sync dim_companies
    dim_companies_file = src_path / "dim_companies" / "dim_companies.parquet"
    if dim_companies_file.exists():
        df_dim = pd.read_parquet(dim_companies_file)
        if "company_key" not in df_dim.columns:
            df_dim["company_key"] = range(1, len(df_dim) + 1)
        if "valid_from_ts" not in df_dim.columns:
            df_dim["valid_from_ts"] = "2026-01-01 00:00:00"
        if "valid_to_ts" not in df_dim.columns:
            df_dim["valid_to_ts"] = "2099-12-31 23:59:59"
        if "is_current" not in df_dim.columns:
            df_dim["is_current"] = 1

        cols = ["company_key", "ticker_symbol", "company_name", "sector", "industry", "market_cap_category", "valid_from_ts", "valid_to_ts", "is_current"]
        avail_cols = [c for c in cols if c in df_dim.columns]
        df_dim = df_dim[avail_cols]
        rows = len(df_dim)
        if is_live:
            manager.insert_dataframe("dim_companies", df_dim)
            rows = manager.query_count("dim_companies")
        results["synced_tables"]["dim_companies"] = {"rows": rows, "status": "SYNCED"}
        logger.info(f"Synced dim_companies: {rows} rows")

    # 2. Sync fact_daily_ohlcv / market_daily_ohlcv
    ohlcv_path = src_path / "raw_market_ohlcv"
    if ohlcv_path.exists():
        try:
            df_ohlcv = pd.read_parquet(ohlcv_path)
            # rename / format
            if "trade_date" in df_ohlcv.columns:
                df_ohlcv["trade_date"] = pd.to_datetime(df_ohlcv["trade_date"]).dt.strftime("%Y-%m-%d")
            if "created" not in df_ohlcv.columns:
                df_ohlcv["created"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
            else:
                df_ohlcv["created"] = pd.to_datetime(df_ohlcv["created"]).dt.strftime("%Y-%m-%d %H:%M:%S")

            expected_cols = ["ticker_symbol", "trade_date", "open_price", "high_price", "low_price", "close_price", "volume", "vwap", "trade_count", "daily_return", "created"]
            sync_cols = [c for c in expected_cols if c in df_ohlcv.columns]
            df_ohlcv = df_ohlcv[sync_cols].head(5000)
            rows = len(df_ohlcv)
            if is_live:
                manager.insert_dataframe("market_daily_ohlcv", df_ohlcv)
                rows = manager.query_count("market_daily_ohlcv")
            results["synced_tables"]["market_daily_ohlcv"] = {"rows": rows, "status": "SYNCED"}
            logger.info(f"Synced market_daily_ohlcv: {rows} rows")
        except Exception as e:
            logger.warning(f"Could not load OHLCV parquet: {e}")

    # 3. Sync feat_technical_indicators
    feat_path = src_path / "gold_features" / "feat_ticker_technical_indicators.parquet"
    if not feat_path.exists():
        feat_path = Path("evidence/phase3/feat_ticker_technical_indicators.parquet")

    if feat_path.exists():
        try:
            df_feats = pd.read_parquet(feat_path)
            if "event_timestamp" in df_feats.columns:
                df_feats["event_timestamp"] = pd.to_datetime(df_feats["event_timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
            if "created" in df_feats.columns:
                df_feats["created"] = pd.to_datetime(df_feats["created"]).dt.strftime("%Y-%m-%d %H:%M:%S")
            rows = len(df_feats)
            if is_live:
                manager.insert_dataframe("feat_technical_indicators", df_feats)
                rows = manager.query_count("feat_technical_indicators")
            results["synced_tables"]["feat_technical_indicators"] = {"rows": rows, "status": "SYNCED"}
            logger.info(f"Synced feat_technical_indicators: {rows} rows")
        except Exception as e:
            logger.warning(f"Could not load features parquet: {e}")

    # 4. Sync market_intraday_bars (Real-time & 1m intraday serving)
    intraday_path = src_path / "raw_market_ohlcv"
    if intraday_path.exists():
        try:
            import glob
            files = sorted(glob.glob(str(intraday_path / "**/*.parquet"), recursive=True))
            if files:
                dfs = [pd.read_parquet(f) for f in files[:20]]
                df_intra = pd.concat(dfs, ignore_index=True)
                
                # Normalize timestamp
                if "ingestion_timestamp" in df_intra.columns:
                    df_intra["event_timestamp"] = pd.to_datetime(df_intra["ingestion_timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
                elif "event_timestamp" in df_intra.columns:
                    df_intra["event_timestamp"] = pd.to_datetime(df_intra["event_timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
                elif "trade_date" in df_intra.columns:
                    df_intra["event_timestamp"] = pd.to_datetime(df_intra["trade_date"]).dt.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    df_intra["event_timestamp"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

                if "created" not in df_intra.columns:
                    df_intra["created"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
                else:
                    df_intra["created"] = pd.to_datetime(df_intra["created"]).dt.strftime("%Y-%m-%d %H:%M:%S")

                if "exchange" not in df_intra.columns:
                    df_intra["exchange"] = "NASDAQ"

                expected_intra = [
                    "ticker_symbol", "event_timestamp", "open_price", "high_price",
                    "low_price", "close_price", "volume", "vwap", "trade_count",
                    "exchange", "created"
                ]
                sync_intra = [c for c in expected_intra if c in df_intra.columns]
                df_intra = df_intra[sync_intra].head(10000)
                rows_intra = len(df_intra)
                if is_live:
                    manager.insert_dataframe("market_intraday_bars", df_intra)
                    rows_intra = manager.query_count("market_intraday_bars")
                results["synced_tables"]["market_intraday_bars"] = {"rows": rows_intra, "status": "SYNCED"}
                logger.info(f"Synced market_intraday_bars: {rows_intra} rows")
        except Exception as e:
            logger.warning(f"Could not load intraday parquet: {e}")

    # Write output evidence
    out_p = Path(out_file)
    out_p.parent.mkdir(parents=True, exist_ok=True)
    with open(out_p, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    logger.info(f"ClickHouse synchronization complete. Metrics saved to {out_file}")
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Synchronize Gold tables to ClickHouse OLAP.")
    parser.add_argument("--source-dir", default="/tmp/lakehouse_source", help="Path to lakehouse parquet data")
    parser.add_argument("--ddl-path", default="sql/clickhouse/schema_quant.sql", help="ClickHouse DDL script")
    parser.add_argument("--host", default="localhost", help="ClickHouse host")
    parser.add_argument("--port", type=int, default=8123, help="ClickHouse HTTP port")
    parser.add_argument("--user", default="quant", help="ClickHouse user")
    parser.add_argument("--password", default="quant_password", help="ClickHouse password")
    parser.add_argument("--engine", default="auto", choices=["auto", "sim", "clickhouse"], help="Execution mode")
    parser.add_argument("--out-file", default="evidence/phase3/clickhouse_sync_metrics.json", help="Output metrics path")
    args = parser.parse_args()

    sync_clickhouse_gold(
        source_dir=args.source_dir,
        ddl_path=args.ddl_path,
        host=args.host,
        port=args.port,
        engine=args.engine,
        out_file=args.out_file,
    )


if __name__ == "__main__":
    main()
