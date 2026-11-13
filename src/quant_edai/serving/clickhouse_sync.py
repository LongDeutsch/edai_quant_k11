"""ClickHouse serving layer synchronization.

Transfers Gold layer facts, dimensions, and computed technical indicators
into ClickHouse ReplacingMergeTree tables for high-throughput OLAP analytics.

Execution contract (P1.2):
* MODE=integration|full  -> a real ClickHouse is REQUIRED (``CLICKHOUSE_HOST``; probes
  ``localhost`` then ``clickhouse``). If it is down, or a Gold/Silver source is missing,
  the sync writes FAILED evidence and raises. ENGINE=sim is ignored.
* MODE=smoke + ENGINE=sim (or ClickHouse offline) -> ``PASSED_SIMULATED`` / ``SIMULATED``,
  never a bare SUCCESS.
* Sources resolve through ``resolve_dataset_root(source_dir, dataset_id)`` (Gold, then Silver;
  Bronze is smoke-only). Frames are aligned to ``sql/clickhouse/schema_quant.sql`` before insert.
* ``.head(N)`` row caps exist only behind ``smoke_cap=True`` (and never in integration|full).
* Evidence carries ``dataset_id``, ``execution_mode`` and a live query-back count per table.
"""

from __future__ import annotations

import argparse
import glob
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

from quant_edai.common.dataset_paths import resolve_dataset_root
from quant_edai.common.execution_status import build_status, is_integration_mode, normalize_mode
from quant_edai.common.ml_provenance import dataset_id_from_path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ClickHouseSync")

DEFAULT_CH_HOSTS = ("localhost", "clickhouse")
INSERT_CHUNK_ROWS = 50_000
DT_FMT = "%Y-%m-%d %H:%M:%S"

# Column specs mirror sql/clickhouse/schema_quant.sql (name, kind). dataset_id is added by DDL ALTERs.
# kinds: str | date | dt | float | uint
TABLE_SCHEMAS: Dict[str, Dict[str, Any]] = {
    "dim_companies": {
        "columns": [
            ("company_key", "uint"), ("ticker_symbol", "str"), ("company_name", "str"), ("sector", "str"),
            ("industry", "str"), ("market_cap_category", "str"), ("valid_from_ts", "dt"),
            ("valid_to_ts", "dt"), ("is_current", "uint"),
        ],
        "key": ["dataset_id", "company_key"],
        "required": ["company_key", "ticker_symbol"],
    },
    "market_daily_ohlcv": {
        "columns": [
            ("ticker_symbol", "str"), ("trade_date", "date"), ("open_price", "float"), ("high_price", "float"),
            ("low_price", "float"), ("close_price", "float"), ("volume", "uint"), ("vwap", "float"),
            ("trade_count", "uint"), ("daily_return", "float"), ("split_id", "str"), ("version", "uint"), ("created", "dt"),
        ],
        "key": ["dataset_id", "ticker_symbol", "trade_date"],
        "required": ["ticker_symbol", "trade_date", "open_price", "high_price", "low_price", "close_price"],
    },
    "feat_technical_indicators": {
        "columns": [
            ("ticker_symbol", "str"), ("event_timestamp", "dt"), ("created", "dt"), ("close_price", "float"),
            ("sma_20", "float"), ("sma_50", "float"), ("sma_ratio_20_50", "float"), ("rsi_14d", "float"),
            ("macd_12_26", "float"), ("macd_signal_9", "float"), ("macd_histogram", "float"),
            ("rolling_volatility_30d", "float"), ("intraday_return_1d", "float"), ("split_id", "str"),
        ],
        "key": ["dataset_id", "ticker_symbol", "event_timestamp"],
        "required": ["ticker_symbol", "event_timestamp", "close_price", "sma_20", "rsi_14d"],
    },
    "market_intraday_bars": {
        "columns": [
            ("ticker_symbol", "str"), ("window_start", "dt"), ("window_end", "dt"), ("event_timestamp", "dt"),
            ("open_price", "float"), ("high_price", "float"), ("low_price", "float"), ("close_price", "float"),
            ("volume", "uint"), ("vwap", "float"), ("trade_count", "uint"), ("exchange", "str"),
            ("split_id", "str"), ("version", "uint"), ("created", "dt"),
        ],
        "key": ["dataset_id", "ticker_symbol", "window_start"],
        "required": ["ticker_symbol", "event_timestamp", "open_price", "high_price", "low_price", "close_price"],
    },
}


def resolve_clickhouse_host(explicit: Optional[str] = None) -> Optional[str]:
    """explicit > CLICKHOUSE_HOST env; None means 'probe the defaults' (localhost, clickhouse)."""
    return explicit or os.environ.get("CLICKHOUSE_HOST") or None


class ClickHouseSyncManager:
    """Manages synchronization between Lakehouse Parquet/Iceberg and ClickHouse OLAP."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: str = "quant_dw",
    ) -> None:
        self.host = resolve_clickhouse_host(host) or DEFAULT_CH_HOSTS[0]
        self.port = int(port or os.environ.get("CLICKHOUSE_PORT", 8123))
        self.user = user if user is not None else os.environ.get("CLICKHOUSE_USER", "quant")
        self.password = password if password is not None else os.environ.get("CLICKHOUSE_PASSWORD", "quant_password")
        self.database = database
        self.url = f"http://{self.host}:{self.port}"

    def is_online(self) -> bool:
        """Check if ClickHouse HTTP endpoint is reachable."""
        try:
            resp = requests.get(f"{self.url}/ping", timeout=3)
            return resp.status_code == 200 and "Ok." in resp.text
        except Exception:
            return False

    def _auth_params(self) -> Dict[str, str]:
        params: Dict[str, str] = {}
        if self.user:
            params["user"] = self.user
        if self.password:
            params["password"] = self.password
        return params

    def execute_query(self, query: str) -> requests.Response:
        """Execute a DDL or DML statement via HTTP POST."""
        resp = requests.post(self.url, params=self._auth_params(), data=query.encode("utf-8"), timeout=15)
        resp.raise_for_status()
        return resp

    def init_schema(self, ddl_path: Path) -> None:
        """Apply ClickHouse DDL script."""
        resolved = ddl_path
        if not resolved.exists():
            root = Path(os.environ.get("QUANT_EDAI_ROOT", "/opt/quant_edai"))
            cand = root / ddl_path
            if cand.exists():
                resolved = cand
        if not resolved.exists():
            logger.warning(f"DDL file {ddl_path} does not exist. Skipping DDL init.")
            return

        with open(resolved, "r", encoding="utf-8") as f:
            ddl_content = f.read()

        # Split statements by semicolon
        statements = [stmt.strip() for stmt in ddl_content.split(";") if stmt.strip()]
        for stmt in statements:
            logger.info(f"Applying DDL: {stmt[:60]}...")
            self.execute_query(stmt)

    def insert_dataframe(self, table: str, df: pd.DataFrame) -> int:
        """Insert a pandas DataFrame into a ClickHouse table using JSONEachRow format (chunked)."""
        if df.empty:
            return 0
        query = f"INSERT INTO {self.database}.{table} FORMAT JSONEachRow"
        params = {"query": query, **self._auth_params()}
        inserted = 0
        for start in range(0, len(df), INSERT_CHUNK_ROWS):
            chunk = df.iloc[start : start + INSERT_CHUNK_ROWS]
            json_data = chunk.to_json(orient="records", lines=True, date_format="iso")
            resp = requests.post(self.url, params=params, data=json_data.encode("utf-8"), timeout=120)
            resp.raise_for_status()
            inserted += len(chunk)
        return inserted

    def query_count(self, table: str, dataset_id: Optional[str] = None, final: bool = False) -> int:
        """Row count (optionally restricted to a dataset_id, optionally deduplicated via FINAL)."""
        where = ""
        if dataset_id is not None:
            safe = dataset_id.replace("\\", "\\\\").replace("'", "\\'")
            where = f" WHERE dataset_id = '{safe}'"
        query = f"SELECT count() FROM {self.database}.{table}{' FINAL' if final else ''}{where}"
        resp = self.execute_query(query)
        return int(resp.text.strip())


def _probe_host(user: Optional[str], password: Optional[str], port: Optional[int], explicit: Optional[str]) -> Tuple[ClickHouseSyncManager, List[str]]:
    """Return the first reachable manager (or the last tried one) and the hosts tried."""
    hosts = [explicit] if explicit else list(DEFAULT_CH_HOSTS)
    tried: List[str] = []
    mgr = ClickHouseSyncManager(host=hosts[0], port=port, user=user, password=password)
    for h in hosts:
        mgr = ClickHouseSyncManager(host=h, port=port, user=user, password=password)
        tried.append(f"{mgr.host}:{mgr.port}")
        if mgr.is_online():
            break
    return mgr, tried


# ---------------------------------------------------------------------------
# Schema alignment
# ---------------------------------------------------------------------------
def align_to_schema(
    df: pd.DataFrame, table: str, dataset_id: Optional[str]
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Coerce ``df`` to the ClickHouse DDL for ``table`` (names, order, dtypes)."""
    spec = TABLE_SCHEMAS[table]
    cols: List[Tuple[str, str]] = spec["columns"]
    df = df.copy()
    report: Dict[str, Any] = {"filled": [], "dropped": [], "missing_required": [], "rows_dropped_invalid": 0}

    # Common source aliases
    for old, new in (("ticker", "ticker_symbol"), ("date", "trade_date")):
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})

    if table == "dim_companies":
        if "company_key" not in df.columns:
            df["company_key"] = range(1, len(df) + 1)
            report["filled"].append("company_key")
        defaults = {"valid_from_ts": "2026-01-01 00:00:00", "valid_to_ts": "2099-12-31 23:59:59", "is_current": 1}
        for c, v in defaults.items():
            if c not in df.columns:
                df[c] = v
                report["filled"].append(c)

    if table == "market_intraday_bars" and "event_timestamp" not in df.columns:
        for src in ("bar_timestamp", "window_start", "ingestion_timestamp", "trade_date"):
            if src in df.columns:
                df["event_timestamp"] = df[src]
                report["filled"].append(f"event_timestamp<-{src}")
                break

    if table == "market_daily_ohlcv":
        if "trade_date" not in df.columns:
            for src in ("event_timestamp", "ingestion_timestamp", "bar_timestamp"):
                if src in df.columns:
                    df["trade_date"] = pd.to_datetime(df[src], utc=True, format="mixed").dt.strftime("%Y-%m-%d")
                    report["filled"].append(f"trade_date<-{src}")
                    break
        if "daily_return" not in df.columns and {"ticker_symbol", "trade_date", "close_price"} <= set(df.columns):
            df = df.sort_values(["ticker_symbol", "trade_date"])
            df["daily_return"] = df.groupby("ticker_symbol")["close_price"].pct_change().fillna(0.0)
            report["filled"].append("daily_return(computed)")

    if "vwap" in {c for c, _ in cols} and "vwap" not in df.columns and "vwap_1m" in df.columns:
        df["vwap"] = df["vwap_1m"]
    if "vwap" in {c for c, _ in cols} and "vwap" not in df.columns and "close_price" in df.columns:
        df["vwap"] = df["close_price"]
        report["filled"].append("vwap<-close_price")
    if "trade_count" in {c for c, _ in cols} and "trade_count" not in df.columns:
        df["trade_count"] = 1
        report["filled"].append("trade_count=1")
    if "exchange" in {c for c, _ in cols} and "exchange" not in df.columns:
        df["exchange"] = "NASDAQ"
        report["filled"].append("exchange=NASDAQ")
    if "created" in {c for c, _ in cols} and "created" not in df.columns:
        df["created"] = pd.Timestamp.now(tz="UTC").strftime(DT_FMT)
        report["filled"].append("created=now")
    if "version" in {c for c, _ in cols} and "version" not in df.columns:
        df["version"] = 1
        report["filled"].append("version=1")
    if "window_start" in {c for c, _ in cols} and "window_start" not in df.columns and "event_timestamp" in df.columns:
        df["window_start"] = df["event_timestamp"]
        report["filled"].append("window_start<-event_timestamp")
    if "window_end" in {c for c, _ in cols} and "window_end" not in df.columns and "window_start" in df.columns:
        ts_start = pd.to_datetime(df["window_start"], utc=True, format="mixed")
        df["window_end"] = (ts_start + pd.to_timedelta(1, unit="m")).dt.strftime(DT_FMT)
        report["filled"].append("window_end(computed)")

    if "split_id" in {c for c, _ in cols} and ("split_id" not in df.columns or df["split_id"].isna().all() or (df["split_id"].astype(str) == "").all()):
        date_col = None
        for cand in ("trade_date", "window_start", "event_timestamp", "bar_timestamp"):
            if cand in df.columns:
                date_col = cand
                break
        if date_col:
            dt_s = pd.to_datetime(df[date_col], errors="coerce", utc=True)
            # Use tz-aware timestamps for comparison to avoid tz-naive vs tz-aware errors
            is_mask = dt_s <= pd.Timestamp("2025-04-30", tz="UTC")
            oos_mask = dt_s <= pd.Timestamp("2026-04-30", tz="UTC")
            df["split_id"] = np.where(is_mask, "IS", np.where(oos_mask, "OOS", "TRADING"))
        else:
            df["split_id"] = "IS"
        report["filled"].append("split_id(derived)")

    report["missing_required"] = [c for c in spec["required"] if c not in df.columns]
    if report["missing_required"]:
        empty = pd.DataFrame(columns=[c for c, _ in cols] + ["dataset_id"])
        return empty, report

    for c, kind in cols:
        if c not in df.columns:
            df[c] = 0 if kind in {"float", "uint"} else ""
            report["filled"].append(f"{c}=default")
        if kind == "date":
            s = df[c].astype(str).str.replace(r"^9999-", "2099-", regex=True)
            df[c] = pd.to_datetime(s, errors="coerce").dt.strftime("%Y-%m-%d")
        elif kind == "dt":
            s = df[c].astype(str).str.replace(r"^9999-", "2099-", regex=True)
            ts = pd.to_datetime(s, utc=True, format="mixed", errors="coerce")
            df[c] = ts.dt.strftime(DT_FMT)
        elif kind == "float":
            df[c] = pd.to_numeric(df[c], errors="coerce")
        elif kind == "uint":
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).clip(lower=0).astype("int64")
        else:
            df[c] = df[c].astype(str)

    required_float = [c for c, k in cols if k == "float" and c in spec["required"]]
    before = len(df)
    if required_float:
        df = df.dropna(subset=required_float)
    report["rows_dropped_invalid"] = before - len(df)
    for c, kind in cols:
        if kind == "float":
            df[c] = df[c].fillna(0.0)

    out = df[[c for c, _ in cols]].copy()
    report["dropped"] = sorted(set(df.columns) - {c for c, _ in cols})
    out["dataset_id"] = dataset_id or ""
    return out, report


def _unique_key_count(df: pd.DataFrame, table: str) -> int:
    return int(len(df.drop_duplicates(subset=TABLE_SCHEMAS[table]["key"]))) if len(df) else 0


# ---------------------------------------------------------------------------
# Source discovery (Gold > Silver; Bronze only in smoke)
# ---------------------------------------------------------------------------
def _first_existing(root: Path, names: List[str]) -> Optional[Path]:
    for n in names:
        p = root / n
        if p.exists():
            return p
    return None


def _read_parquet_any(path: Path, max_files: Optional[int] = None) -> pd.DataFrame:
    if path.is_file():
        return pd.read_parquet(path)
    if not max_files:
        try:
            return pd.read_parquet(path)
        except Exception:
            pass
    files = sorted(glob.glob(str(path / "**" / "*.parquet"), recursive=True))
    if not files:
        raise FileNotFoundError(f"No parquet files under {path}")
    if max_files:
        files = files[:max_files]
    dfs = []
    for f in files:
        df = pd.read_parquet(f)
        for part in Path(f).parent.parts:
            if "=" in part:
                k, v = part.split("=", 1)
                if k not in df.columns:
                    df[k] = v
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


def _locate_sources(root: Path, integration: bool, tables: Optional[List[str]] = None) -> Dict[str, Tuple[Optional[Path], str]]:
    """table -> (path or None, zone)."""
    dim = _first_existing(root, ["gold_dim_companies/dim_companies.parquet", "gold_dim_companies", "dim_companies/dim_companies.parquet", "dim_companies"])
    daily = _first_existing(root, ["gold_market_bars_daily"])
    daily_zone = "gold"
    if daily is None:
        daily, daily_zone = _first_existing(root, ["stg_market_ohlcv"]), "silver"
    if daily is None and not integration:
        daily, daily_zone = _first_existing(root, ["raw_market_ohlcv"]), "bronze"
    feats = _first_existing(root, ["gold_features/feat_ticker_technical_indicators.parquet", "gold_features"])
    feats_zone = "gold"
    if feats is None and not integration:
        legacy = Path("evidence/phase3/feat_ticker_technical_indicators.parquet")
        feats, feats_zone = (legacy if legacy.exists() else None), "evidence_legacy"
    intra = _first_existing(root, ["gold_market_bars_1m"])
    intra_zone = "gold"
    if intra is None and not integration:
        intra, intra_zone = _first_existing(root, ["raw_market_ohlcv"]), "bronze"
    all_sources = {
        "dim_companies": (dim, "gold"),
        "market_daily_ohlcv": (daily, daily_zone),
        "feat_technical_indicators": (feats, feats_zone),
        "market_intraday_bars": (intra, intra_zone),
    }
    if tables is not None:
        return {t: all_sources[t] for t in tables if t in all_sources}
    return all_sources


def _write_evidence(results: Dict[str, Any], out_file: str) -> None:
    out_p = Path(out_file)
    out_p.parent.mkdir(parents=True, exist_ok=True)
    with open(out_p, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)


def sync_clickhouse_gold(
    source_dir: str = "/tmp/lakehouse_source",
    ddl_path: str = "sql/clickhouse/schema_quant.sql",
    host: Optional[str] = None,
    port: Optional[int] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    engine: str = "auto",
    out_file: str = "evidence/phase3/clickhouse_sync_metrics.json",
    dataset_id: Optional[str] = None,
    mode: Optional[str] = None,
    smoke_cap: bool = False,
    smoke_cap_rows: int = 5000,
    tables: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Sync Gold tables to ClickHouse (live) or simulate (smoke only).

    Raises RuntimeError under MODE=integration|full when ClickHouse is down, a required
    Gold/Silver source is missing, or query-back counts do not reconcile.
    """
    run_mode = normalize_mode(mode)
    integration = is_integration_mode(run_mode)
    engine_req = (engine or "auto").strip().lower()
    sim_requested = engine_req == "sim"
    if integration and sim_requested:
        logger.warning(f"ENGINE=sim ignored under MODE={run_mode}: real ClickHouse is required.")
        sim_requested = False
    cap_active = bool(smoke_cap) and not integration

    manager = ClickHouseSyncManager(host=host, port=port, user=user, password=password)
    tried: List[str] = [f"{manager.host}:{manager.port}"]
    if not sim_requested:
        manager, tried = _probe_host(user, password, port, resolve_clickhouse_host(host))
    is_live = (not sim_requested) and manager.is_online()
    logger.info(f"ClickHouse {tried} status: {'ONLINE' if is_live else 'OFFLINE/SIM'} (MODE={run_mode})")

    # Dataset-scoped source root: OUT_DIR/runs/{dataset_id}
    try:
        src_path = resolve_dataset_root(source_dir, dataset_id)
    except FileNotFoundError:
        src_path = Path(source_dir)
    resolved_dataset_id = dataset_id or dataset_id_from_path(src_path) or os.environ.get("DATASET_ID")

    results: Dict[str, Any] = {
        "engine": "clickhouse_live" if is_live else "simulation_fallback",
        "mode": run_mode,
        "dataset_id": resolved_dataset_id,
        "source_root": str(src_path),
        "host": manager.host,
        "port": manager.port,
        "hosts_probed": tried,
        "tables_requested": tables,
        "smoke_cap": {"active": cap_active, "rows": smoke_cap_rows if cap_active else None},
        "synced_tables": {},
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    def _finish(ok: bool, reason: Optional[str] = None) -> Dict[str, Any]:
        if ok and is_live:
            st = {"status": "SUCCESS", "execution_mode": "LIVE"}  # real ClickHouse wrote + queried back
        else:
            st = build_status(
                ok=ok,
                mode=run_mode,
                engine="sim" if not is_live else "clickhouse",
                degraded_reason=reason,
                forced_simulated=not is_live,
            ).as_dict()
        results["status"] = st["status"]
        results["execution_mode"] = st["execution_mode"]
        if reason:
            results["detail"] = reason
        _write_evidence(results, out_file)
        logger.info(f"ClickHouse synchronization {st['status']} ({st['execution_mode']}). Metrics saved to {out_file}")
        return results

    def _fail(reason: str) -> None:
        _finish(False, reason)
        raise RuntimeError(f"[MODE={run_mode}] ClickHouse sync FAILED: {reason}")

    if integration and not is_live:
        _fail(f"ClickHouse not reachable (tried {', '.join(tried)}; set CLICKHOUSE_HOST). No simulation in integration|full.")
    if is_live:
        try:
            manager.init_schema(Path(ddl_path))
        except Exception as e:
            if integration:
                _fail(f"DDL apply failed: {e}")
            logger.warning(f"DDL apply failed in smoke: {e}")

    sources = _locate_sources(src_path, integration, tables=tables)
    problems: List[str] = []

    for table, (path, zone) in sources.items():
        if path is None:
            msg = f"no {table} source under {src_path}"
            if integration:
                _fail(msg + " (Gold/Silver required)")
            logger.info(f"Skipping {table}: {msg}")
            continue
        try:
            max_files = 20 if (cap_active and table == "market_intraday_bars") else None
            df_raw = _read_parquet_any(path, max_files=max_files)
            df, report = align_to_schema(df_raw, table, resolved_dataset_id)
            if report["missing_required"]:
                raise ValueError(f"missing required columns {report['missing_required']}")
            if cap_active:
                cap = smoke_cap_rows * (2 if table == "market_intraday_bars" else 1)
                df = df.head(cap)
            expected_unique = _unique_key_count(df, table)
            entry: Dict[str, Any] = {
                "rows": int(len(df)),
                "rows_read": int(len(df_raw)),
                "unique_keys": expected_unique,
                "source": str(path),
                "source_zone": zone,
                "schema_alignment": report,
                "status": "SIMULATED",
            }
            if is_live:
                before = manager.query_count(table)
                manager.insert_dataframe(table, df)
                after_total = manager.query_count(table)
                dataset_rows = manager.query_count(table, resolved_dataset_id or "", final=True)
                entry.update(
                    {
                        "rows": int(len(df)),
                        "rows_inserted": int(len(df)),
                        "queryback": {
                            "table_rows_before": before,
                            "table_rows_after": after_total,
                            "dataset_rows_final": dataset_rows,
                            "expected_unique_keys": expected_unique,
                            "reconciled": dataset_rows >= expected_unique,
                        },
                        "status": "SYNCED" if dataset_rows >= expected_unique else "COUNT_MISMATCH",
                    }
                )
                if dataset_rows < expected_unique:
                    problems.append(f"{table}: query-back {dataset_rows} < expected {expected_unique}")
            results["synced_tables"][table] = entry
            logger.info(f"{table}: {entry['status']} rows={entry['rows']} ({zone}: {path})")
        except Exception as e:
            if integration:
                results["synced_tables"][table] = {"status": "FAILED", "source": str(path), "error": str(e)}
                _fail(f"{table} sync failed: {e}")
            logger.warning(f"Could not sync {table}: {e}")
            results["synced_tables"][table] = {"status": "ERROR", "source": str(path), "error": str(e)}

    if integration and problems:
        _fail("; ".join(problems))
    return _finish(True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Synchronize Gold tables to ClickHouse OLAP.")
    parser.add_argument("--source-dir", default=os.environ.get("OUT_DIR", "/tmp/lakehouse_source"), help="OUT_DIR (dataset resolved via CURRENT_DATASET)")
    parser.add_argument("--dataset-id", default=os.environ.get("DATASET_ID"), help="Explicit dataset_id under OUT_DIR/runs/")
    parser.add_argument("--ddl-path", default="sql/clickhouse/schema_quant.sql", help="ClickHouse DDL script")
    parser.add_argument("--host", default=None, help="ClickHouse host (default: CLICKHOUSE_HOST, else probe localhost/clickhouse)")
    parser.add_argument("--port", type=int, default=None, help="ClickHouse HTTP port (default: CLICKHOUSE_PORT or 8123)")
    parser.add_argument("--user", default=None, help="ClickHouse user (default: CLICKHOUSE_USER or quant)")
    parser.add_argument("--password", default=None, help="ClickHouse password (default: CLICKHOUSE_PASSWORD)")
    parser.add_argument("--engine", default=os.environ.get("ENGINE", "auto"), help="auto|sim|clickhouse (ignored=live under MODE=integration|full)")
    parser.add_argument("--tables", nargs="*", default=None, help="Specific tables to sync")
    parser.add_argument("--smoke-cap", action="store_true", help="Cap rows (smoke only; ignored in integration|full)")
    parser.add_argument("--smoke-cap-rows", type=int, default=5000)
    parser.add_argument("--out-file", default="evidence/phase3/clickhouse_sync_metrics.json", help="Output metrics path")
    args = parser.parse_args()

    try:
        sync_clickhouse_gold(
            source_dir=args.source_dir,
            ddl_path=args.ddl_path,
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            engine=args.engine,
            out_file=args.out_file,
            dataset_id=args.dataset_id,
            smoke_cap=args.smoke_cap,
            smoke_cap_rows=args.smoke_cap_rows,
            tables=args.tables,
        )
    except RuntimeError as e:
        logger.error(str(e))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
