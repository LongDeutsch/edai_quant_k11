"""Database access layer for Paper Trading ACID persistence and idempotency."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
import sqlite3
from typing import Any, Dict, List, Optional
import pandas as pd

from quant_edai.common.ml_provenance import PROVENANCE_FIELDS, require_provenance

logger = logging.getLogger(__name__)

# Tables whose rows are ML-derived and must carry model_version / dataset_id / feature_source.
PROVENANCE_TABLES = ("alpha_registry", "alpha_signals_daily", "paper_orders", "paper_trades")

# Matches infra/docker-compose.yml service ``postgres`` (user/password/db).
DEFAULT_PG_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://quant:quant_password@localhost:5432/quant_db",
)
DEFAULT_SQLITE_PATH = os.environ.get("SQLITE_PAPER_DB", "paper_trading.db")


class PaperDatabase:
    """Manages paper trading persistence across PostgreSQL and SQLite fallback."""

    def __init__(
        self,
        db_url: Optional[str] = None,
        sqlite_path: Optional[str] = None,
        require_postgres: bool = False,
    ):
        self.db_url = db_url or DEFAULT_PG_URL
        self.sqlite_path = sqlite_path or DEFAULT_SQLITE_PATH
        self.require_postgres = require_postgres
        self.is_postgres = False
        self._pg_conn = None
        self._sqlite_conn = None
        # An explicit sqlite file is a test or local database. It must not open production Postgres.
        self._sqlite_only = sqlite_path is not None or os.environ.get("USE_SQLITE_ONLY") == "1"

        self._connect()

    def _connect(self) -> None:
        """Attempt PostgreSQL connection first, fallback to SQLite."""
        if not self._sqlite_only:
            try:
                import psycopg2
                conn = psycopg2.connect(self.db_url, connect_timeout=1)
                conn.autocommit = False
                self._pg_conn = conn
                self.is_postgres = True
                logger.info("Connected to PostgreSQL database for Paper Trading.")
                self._migrate_existing_provenance()
                return
            except Exception as e:
                if self.require_postgres:
                    raise ConnectionError(f"PostgreSQL is required for official replay: {e}") from e
                logger.debug(f"PostgreSQL connection unavailable ({e}); using SQLite backend.")

        if self.require_postgres:
            raise ConnectionError("PostgreSQL is required for official replay.")

        # Fallback to SQLite
        self.is_postgres = False
        self._sqlite_conn = sqlite3.connect(self.sqlite_path, check_same_thread=False)
        self._sqlite_conn.row_factory = sqlite3.Row
        logger.info(f"Using SQLite database at {self.sqlite_path} for Paper Trading.")
        self._migrate_existing_provenance()

    def _migrate_existing_provenance(self) -> None:
        """Best-effort: add provenance columns to tables created before P1 (no-op when absent)."""
        conn = self._pg_conn if self.is_postgres else self._sqlite_conn
        cur = conn.cursor()
        try:
            self._ensure_provenance_columns(cur)
            self._ensure_replay_history(cur)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.debug(f"Provenance column migration skipped: {e}")
        finally:
            cur.close()

    def init_db(self, reset: bool = False) -> None:
        """Initialize or reset tables."""
        if reset and self.is_postgres and os.environ.get("ALLOW_PG_PAPER_RESET") != "1":
            raise RuntimeError(
                "Refusing to reset PostgreSQL paper tables. Tests must pass sqlite_path so they stay off the production database."
            )
        if self.is_postgres:
            self._init_postgres(reset=reset)
        else:
            self._init_sqlite(reset=reset)

    def _init_postgres(self, reset: bool) -> None:
        cur = self._pg_conn.cursor()
        try:
            if reset:
                cur.execute("""
                    DROP TABLE IF EXISTS paper_trades CASCADE;
                    DROP TABLE IF EXISTS paper_orders CASCADE;
                    DROP TABLE IF EXISTS paper_positions CASCADE;
                    DROP TABLE IF EXISTS paper_portfolio_snapshots CASCADE;
                """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS paper_orders (
                    order_id VARCHAR(64) PRIMARY KEY,
                    session_id VARCHAR(64) NOT NULL,
                    ticker_symbol VARCHAR(16) NOT NULL,
                    order_type VARCHAR(16) NOT NULL,
                    direction VARCHAR(8) NOT NULL,
                    quantity NUMERIC(14, 4) NOT NULL,
                    requested_price NUMERIC(14, 4) NOT NULL,
                    status VARCHAR(16) NOT NULL,
                    bar_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    CONSTRAINT uq_order_idempotency UNIQUE (session_id, ticker_symbol, bar_timestamp, direction, order_type)
                );
                CREATE TABLE IF NOT EXISTS paper_trades (
                    trade_id VARCHAR(64) PRIMARY KEY,
                    order_id VARCHAR(64) REFERENCES paper_orders(order_id),
                    session_id VARCHAR(64) NOT NULL,
                    ticker_symbol VARCHAR(16) NOT NULL,
                    direction VARCHAR(8) NOT NULL,
                    fill_price NUMERIC(14, 4) NOT NULL,
                    fill_quantity NUMERIC(14, 4) NOT NULL,
                    slippage_bps NUMERIC(8, 2) NOT NULL DEFAULT 5.0,
                    commission NUMERIC(10, 4) NOT NULL DEFAULT 0.0,
                    realized_pnl NUMERIC(14, 4) NOT NULL DEFAULT 0.0,
                    executed_at TIMESTAMP WITH TIME ZONE NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_positions (
                    session_id VARCHAR(64) NOT NULL,
                    ticker_symbol VARCHAR(16) NOT NULL,
                    side VARCHAR(8) NOT NULL,
                    quantity NUMERIC(14, 4) NOT NULL,
                    average_entry_price NUMERIC(14, 4) NOT NULL,
                    current_mark_price NUMERIC(14, 4) NOT NULL,
                    unrealized_pnl NUMERIC(14, 4) NOT NULL DEFAULT 0.0,
                    unrealized_pnl_pct NUMERIC(8, 4) NOT NULL DEFAULT 0.0,
                    stop_loss_price NUMERIC(14, 4),
                    take_profit_price NUMERIC(14, 4),
                    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    PRIMARY KEY (session_id, ticker_symbol)
                );
                CREATE TABLE IF NOT EXISTS paper_portfolio_snapshots (
                    snapshot_id BIGSERIAL PRIMARY KEY,
                    session_id VARCHAR(64) NOT NULL,
                    cash_balance NUMERIC(16, 4) NOT NULL,
                    invested_equity NUMERIC(16, 4) NOT NULL,
                    total_portfolio_value NUMERIC(16, 4) NOT NULL,
                    realized_pnl_cumulative NUMERIC(16, 4) NOT NULL,
                    unrealized_pnl_current NUMERIC(16, 4) NOT NULL,
                    rolling_sharpe_ratio NUMERIC(8, 4) NOT NULL DEFAULT 0.0,
                    max_drawdown_pct NUMERIC(8, 4) NOT NULL DEFAULT 0.0,
                    active_positions_count INT NOT NULL DEFAULT 0,
                    latency_ms NUMERIC(8, 3) NOT NULL DEFAULT 0.0,
                    recorded_at TIMESTAMP WITH TIME ZONE NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_market_bars (
                    session_id VARCHAR(64) NOT NULL,
                    ticker_symbol VARCHAR(16) NOT NULL,
                    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    open_price NUMERIC(14, 4) NOT NULL,
                    high_price NUMERIC(14, 4) NOT NULL,
                    low_price NUMERIC(14, 4) NOT NULL,
                    close_price NUMERIC(14, 4) NOT NULL,
                    volume NUMERIC(16, 2) NOT NULL,
                    vwap NUMERIC(14, 4),
                    p_ml NUMERIC(8, 4),
                    PRIMARY KEY (session_id, ticker_symbol, event_timestamp)
                );
                CREATE TABLE IF NOT EXISTS alpha_registry (
                    alpha_id VARCHAR(64) NOT NULL,
                    version VARCHAR(32) NOT NULL,
                    alpha_type VARCHAR(16) NOT NULL,
                    status VARCHAR(24) NOT NULL,
                    description TEXT,
                    config_json TEXT,
                    metrics_snapshot TEXT,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    promoted_at TIMESTAMP WITH TIME ZONE,
                    promoted_by VARCHAR(64),
                    retired_at TIMESTAMP WITH TIME ZONE,
                    PRIMARY KEY (alpha_id, version)
                );
                CREATE TABLE IF NOT EXISTS alpha_signals_daily (
                    signal_id VARCHAR(64) PRIMARY KEY,
                    alpha_id VARCHAR(64) NOT NULL,
                    version VARCHAR(32) NOT NULL,
                    ticker_symbol VARCHAR(16) NOT NULL,
                    asof_date DATE NOT NULL,
                    effective_date DATE NOT NULL,
                    target_side VARCHAR(8) NOT NULL,
                    target_weight NUMERIC(8, 4) NOT NULL,
                    confidence NUMERIC(8, 4) NOT NULL,
                    raw_score NUMERIC(10, 4) NOT NULL,
                    regime_detected VARCHAR(32),
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    CONSTRAINT uq_daily_signal UNIQUE (alpha_id, version, ticker_symbol, effective_date)
                );
                CREATE TABLE IF NOT EXISTS pm_governance_audit (
                    audit_id SERIAL PRIMARY KEY,
                    action_type VARCHAR(32) NOT NULL,
                    alpha_id VARCHAR(64),
                    version VARCHAR(32),
                    executed_by VARCHAR(64) NOT NULL,
                    reason TEXT NOT NULL,
                    details TEXT,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS alpha_ensemble_config (
                    ensemble_id VARCHAR(64) NOT NULL,
                    horizon VARCHAR(16) NOT NULL,
                    selected_alpha_ids TEXT NOT NULL,
                    booksize NUMERIC(16, 2) NOT NULL DEFAULT 100000.0,
                    top_k_tickers INT NOT NULL DEFAULT 5,
                    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    updated_by VARCHAR(64),
                    PRIMARY KEY (ensemble_id, horizon)
                );
            """)
            self._ensure_provenance_columns(cur)
            self._pg_conn.commit()
        except Exception as e:
            self._pg_conn.rollback()
            raise e
        finally:
            cur.close()

    def _init_sqlite(self, reset: bool) -> None:
        cur = self._sqlite_conn.cursor()
        if reset:
            cur.executescript("""
                DROP TABLE IF EXISTS paper_trades;
                DROP TABLE IF EXISTS paper_orders;
                DROP TABLE IF EXISTS paper_positions;
                DROP TABLE IF EXISTS paper_portfolio_snapshots;
            """)
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS paper_orders (
                order_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                ticker_symbol TEXT NOT NULL,
                order_type TEXT NOT NULL,
                direction TEXT NOT NULL,
                quantity REAL NOT NULL,
                requested_price REAL NOT NULL,
                status TEXT NOT NULL,
                bar_timestamp TEXT NOT NULL,
                created_at TEXT NOT NULL,
                CONSTRAINT uq_order_idempotency UNIQUE (session_id, ticker_symbol, bar_timestamp, direction, order_type)
            );
            CREATE TABLE IF NOT EXISTS paper_trades (
                trade_id TEXT PRIMARY KEY,
                order_id TEXT,
                session_id TEXT NOT NULL,
                ticker_symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                fill_price REAL NOT NULL,
                fill_quantity REAL NOT NULL,
                slippage_bps REAL NOT NULL DEFAULT 5.0,
                commission REAL NOT NULL DEFAULT 0.0,
                realized_pnl REAL NOT NULL DEFAULT 0.0,
                executed_at TEXT NOT NULL,
                FOREIGN KEY (order_id) REFERENCES paper_orders(order_id)
            );
            CREATE TABLE IF NOT EXISTS paper_positions (
                session_id TEXT NOT NULL,
                ticker_symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                quantity REAL NOT NULL,
                average_entry_price REAL NOT NULL,
                current_mark_price REAL NOT NULL,
                unrealized_pnl REAL NOT NULL DEFAULT 0.0,
                unrealized_pnl_pct REAL NOT NULL DEFAULT 0.0,
                stop_loss_price REAL,
                take_profit_price REAL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (session_id, ticker_symbol)
            );
            CREATE TABLE IF NOT EXISTS paper_portfolio_snapshots (
                snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                cash_balance REAL NOT NULL,
                invested_equity REAL NOT NULL,
                total_portfolio_value REAL NOT NULL,
                realized_pnl_cumulative REAL NOT NULL,
                unrealized_pnl_current REAL NOT NULL,
                rolling_sharpe_ratio REAL NOT NULL DEFAULT 0.0,
                max_drawdown_pct REAL NOT NULL DEFAULT 0.0,
                active_positions_count INTEGER NOT NULL DEFAULT 0,
                latency_ms REAL NOT NULL DEFAULT 0.0,
                recorded_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS paper_market_bars (
                session_id TEXT NOT NULL,
                ticker_symbol TEXT NOT NULL,
                event_timestamp TEXT NOT NULL,
                open_price REAL NOT NULL,
                high_price REAL NOT NULL,
                low_price REAL NOT NULL,
                close_price REAL NOT NULL,
                volume REAL NOT NULL,
                vwap REAL,
                p_ml REAL,
                PRIMARY KEY (session_id, ticker_symbol, event_timestamp)
            );
            CREATE TABLE IF NOT EXISTS alpha_registry (
                alpha_id TEXT NOT NULL,
                version TEXT NOT NULL,
                alpha_type TEXT NOT NULL,
                status TEXT NOT NULL,
                description TEXT,
                config_json TEXT,
                metrics_snapshot TEXT,
                created_at TEXT NOT NULL,
                promoted_at TEXT,
                promoted_by TEXT,
                retired_at TEXT,
                PRIMARY KEY (alpha_id, version)
            );
            CREATE TABLE IF NOT EXISTS alpha_signals_daily (
                signal_id TEXT PRIMARY KEY,
                alpha_id TEXT NOT NULL,
                version TEXT NOT NULL,
                ticker_symbol TEXT NOT NULL,
                asof_date TEXT NOT NULL,
                effective_date TEXT NOT NULL,
                target_side TEXT NOT NULL,
                target_weight REAL NOT NULL,
                confidence REAL NOT NULL,
                raw_score REAL NOT NULL,
                regime_detected TEXT,
                created_at TEXT NOT NULL,
                CONSTRAINT uq_daily_signal UNIQUE (alpha_id, version, ticker_symbol, effective_date)
            );
            CREATE TABLE IF NOT EXISTS pm_governance_audit (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                action_type TEXT NOT NULL,
                alpha_id TEXT,
                version TEXT,
                executed_by TEXT NOT NULL,
                reason TEXT NOT NULL,
                details TEXT,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS alpha_ensemble_config (
                ensemble_id TEXT NOT NULL,
                horizon TEXT NOT NULL,
                selected_alpha_ids TEXT NOT NULL,
                booksize REAL NOT NULL DEFAULT 100000.0,
                top_k_tickers INTEGER NOT NULL DEFAULT 5,
                updated_at TEXT NOT NULL,
                updated_by TEXT,
                PRIMARY KEY (ensemble_id, horizon)
            );
        """)
        self._ensure_provenance_columns(cur)
        self._sqlite_conn.commit()

    def _ensure_provenance_columns(self, cur) -> None:
        """Additive migration: model_version / dataset_id / feature_source on ML-derived tables."""
        for table in PROVENANCE_TABLES:
            for col in PROVENANCE_FIELDS:
                if self.is_postgres:
                    cur.execute(f"ALTER TABLE IF EXISTS {table} ADD COLUMN IF NOT EXISTS {col} TEXT")
                else:
                    existing = {r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()}
                    if existing and col not in existing:
                        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} TEXT")

        # Alpha Lifecycle & Approved Ensemble Additive Columns
        extra_schema = [
            ("alpha_registry", "out_of_sample_metrics", "TEXT"),
            ("alpha_registry", "split_id", "TEXT"),
            ("alpha_backtest_results", "split_id", "TEXT"),
            ("alpha_ensemble_config", "dataset_id", "TEXT"),
            ("alpha_ensemble_config", "model_version", "TEXT"),
            ("alpha_ensemble_config", "weights_json", "TEXT"),
            ("alpha_ensemble_config", "approval_status", "TEXT"),
            ("alpha_ensemble_config", "approved_by", "TEXT"),
            ("alpha_ensemble_config", "approved_at", "TIMESTAMP WITH TIME ZONE" if self.is_postgres else "TEXT"),
            ("paper_trades", "alpha_id", "TEXT"),
            ("paper_positions", "entry_time", "TEXT"),
        ]
        for tbl, col, ctype in extra_schema:
            try:
                if self.is_postgres:
                    cur.execute(f"ALTER TABLE IF EXISTS {tbl} ADD COLUMN IF NOT EXISTS {col} {ctype}")
                else:
                    existing = {r[1] for r in cur.execute(f"PRAGMA table_info({tbl})").fetchall()}
                    if existing and col not in existing:
                        cur.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {ctype}")
            except Exception:
                pass

    def _ensure_replay_history(self, cur) -> None:
        """Additive tables for score history, position marks, and closed-trade lifecycle."""
        if self.is_postgres:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS paper_alpha_scores (
                    session_id TEXT NOT NULL,
                    ticker_symbol TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    alpha_id TEXT NOT NULL,
                    version TEXT NOT NULL,
                    z_score DOUBLE PRECISION,
                    action TEXT NOT NULL,
                    PRIMARY KEY (session_id, ticker_symbol, event_time, alpha_id, version)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS paper_position_lifecycle (
                    session_id TEXT NOT NULL,
                    ticker_symbol TEXT NOT NULL,
                    entry_time TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity DOUBLE PRECISION NOT NULL,
                    entry_price DOUBLE PRECISION NOT NULL,
                    last_mark_time TEXT,
                    last_mark_price DOUBLE PRECISION,
                    unrealized_pnl DOUBLE PRECISION NOT NULL DEFAULT 0,
                    last_open_unrealized DOUBLE PRECISION NOT NULL DEFAULT 0,
                    exit_time TEXT,
                    exit_price DOUBLE PRECISION,
                    exit_reason TEXT,
                    realized_pnl DOUBLE PRECISION,
                    status TEXT NOT NULL,
                    PRIMARY KEY (session_id, ticker_symbol, entry_time)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS paper_position_marks (
                    session_id TEXT NOT NULL,
                    ticker_symbol TEXT NOT NULL,
                    entry_time TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    mark_price DOUBLE PRECISION NOT NULL,
                    unrealized_pnl DOUBLE PRECISION NOT NULL,
                    quantity DOUBLE PRECISION NOT NULL,
                    side TEXT NOT NULL,
                    PRIMARY KEY (session_id, ticker_symbol, entry_time, event_time)
                )
                """
            )
        else:
            cur.executescript(
                """
                CREATE TABLE IF NOT EXISTS paper_alpha_scores (
                    session_id TEXT NOT NULL,
                    ticker_symbol TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    alpha_id TEXT NOT NULL,
                    version TEXT NOT NULL,
                    z_score REAL,
                    action TEXT NOT NULL,
                    PRIMARY KEY (session_id, ticker_symbol, event_time, alpha_id, version)
                );
                CREATE TABLE IF NOT EXISTS paper_position_lifecycle (
                    session_id TEXT NOT NULL,
                    ticker_symbol TEXT NOT NULL,
                    entry_time TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    last_mark_time TEXT,
                    last_mark_price REAL,
                    unrealized_pnl REAL NOT NULL DEFAULT 0,
                    last_open_unrealized REAL NOT NULL DEFAULT 0,
                    exit_time TEXT,
                    exit_price REAL,
                    exit_reason TEXT,
                    realized_pnl REAL,
                    status TEXT NOT NULL,
                    PRIMARY KEY (session_id, ticker_symbol, entry_time)
                );
                CREATE TABLE IF NOT EXISTS paper_position_marks (
                    session_id TEXT NOT NULL,
                    ticker_symbol TEXT NOT NULL,
                    entry_time TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    mark_price REAL NOT NULL,
                    unrealized_pnl REAL NOT NULL,
                    quantity REAL NOT NULL,
                    side TEXT NOT NULL,
                    PRIMARY KEY (session_id, ticker_symbol, entry_time, event_time)
                );
                """
            )

    def record_alpha_scores(self, session_id: str, ticker: str, event_time: str, scores: List[Dict[str, Any]]) -> None:
        """Store evaluated alpha scores. A later bar does not erase them."""
        rows = []
        for score in scores:
            alpha_id = score.get("alpha_id")
            if not alpha_id:
                continue
            z_score = score.get("z_score")
            rows.append((
                session_id, ticker, str(event_time), str(alpha_id), str(score.get("version") or ""),
                None if z_score is None else float(z_score), str(score.get("action") or ""),
            ))
        if not rows:
            return
        sql = """
            INSERT INTO paper_alpha_scores (
                session_id, ticker_symbol, event_time, alpha_id, version, z_score, action
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        if self.is_postgres:
            sql = sql.replace("?", "%s") + " ON CONFLICT DO NOTHING"
            cur = self._pg_conn.cursor()
            try:
                cur.executemany(sql, rows)
                self._pg_conn.commit()
            finally:
                cur.close()
        else:
            cur = self._sqlite_conn.cursor()
            cur.executemany(sql + " ON CONFLICT DO NOTHING", rows)
            self._sqlite_conn.commit()

    def open_position_lifecycle(self, pos: Dict[str, Any]) -> None:
        sql = """
            INSERT INTO paper_position_lifecycle (
                session_id, ticker_symbol, entry_time, side, quantity, entry_price,
                last_mark_time, last_mark_price, unrealized_pnl, last_open_unrealized, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN')
        """
        params = (
            pos["session_id"], pos["ticker_symbol"], str(pos["entry_time"]), pos["side"],
            float(pos["quantity"]), float(pos["average_entry_price"]),
            str(pos["entry_time"]), float(pos["current_mark_price"]),
            float(pos["unrealized_pnl"]), float(pos["unrealized_pnl"]),
        )
        self._write_history(sql, params, conflict="DO NOTHING")
        self.record_position_mark(pos, pos["entry_time"])

    def record_position_mark(self, pos: Dict[str, Any], event_time: str) -> None:
        mark_sql = """
            INSERT INTO paper_position_marks (
                session_id, ticker_symbol, entry_time, event_time, mark_price, unrealized_pnl, quantity, side
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        mark_params = (
            pos["session_id"], pos["ticker_symbol"], str(pos["entry_time"]), str(event_time),
            float(pos["current_mark_price"]), float(pos["unrealized_pnl"]),
            float(pos["quantity"]), pos["side"],
        )
        self._write_history(mark_sql, mark_params, conflict="DO NOTHING")
        update_sql = """
            UPDATE paper_position_lifecycle
            SET last_mark_time = ?, last_mark_price = ?, unrealized_pnl = ?, last_open_unrealized = ?
            WHERE session_id = ? AND ticker_symbol = ? AND entry_time = ? AND status = 'OPEN'
        """
        update_params = (
            str(event_time), float(pos["current_mark_price"]), float(pos["unrealized_pnl"]),
            float(pos["unrealized_pnl"]), pos["session_id"], pos["ticker_symbol"], str(pos["entry_time"]),
        )
        self._write_history(update_sql, update_params, conflict=None)

    def close_position_lifecycle(self, pos: Dict[str, Any], exit_time: str, exit_price: float, reason: str, realized_pnl: float) -> None:
        sql = """
            UPDATE paper_position_lifecycle
            SET exit_time = ?, exit_price = ?, exit_reason = ?, realized_pnl = ?,
                unrealized_pnl = 0, status = 'CLOSED'
            WHERE session_id = ? AND ticker_symbol = ? AND entry_time = ? AND status = 'OPEN'
        """
        params = (
            str(exit_time), float(exit_price), reason, float(realized_pnl),
            pos["session_id"], pos["ticker_symbol"], str(pos["entry_time"]),
        )
        self._write_history(sql, params, conflict=None)

    def _write_history(self, sql: str, params: tuple, conflict: Optional[str]) -> None:
        statement = sql
        if conflict:
            statement = sql + f" ON CONFLICT {conflict}"
        if self.is_postgres:
            statement = statement.replace("?", "%s")
            cur = self._pg_conn.cursor()
            try:
                cur.execute(statement, params)
                self._pg_conn.commit()
            finally:
                cur.close()
        else:
            cur = self._sqlite_conn.cursor()
            cur.execute(statement, params)
            self._sqlite_conn.commit()

    def record_order(self, order: Dict[str, Any]) -> bool:
        """Insert order with idempotency check. Returns True if inserted, False if duplicate."""
        order = require_provenance(order, kind="order")
        bar_ts = str(order.get("bar_timestamp") or order.get("created_at"))
        created_at = str(order.get("created_at") or bar_ts)
        sql = """
            INSERT INTO paper_orders (
                order_id, session_id, ticker_symbol, order_type, direction,
                quantity, requested_price, status, bar_timestamp, created_at,
                model_version, dataset_id, feature_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            order["order_id"], order["session_id"], order["ticker_symbol"],
            order["order_type"], order["direction"], float(order["quantity"]),
            float(order["requested_price"]), order["status"], bar_ts, created_at,
            *[str(order[f]) for f in PROVENANCE_FIELDS],
        )
        if self.is_postgres:
            pg_sql = (
                sql.replace("?", "%s")
                + " ON CONFLICT (session_id, ticker_symbol, bar_timestamp, direction, order_type) DO NOTHING"
            )
            cur = self._pg_conn.cursor()
            try:
                cur.execute(pg_sql, params)
                inserted = cur.rowcount > 0
                self._pg_conn.commit()
                return inserted
            except Exception as e:
                self._pg_conn.rollback()
                logger.error(f"Error inserting order: {e}")
                return False
            finally:
                cur.close()
        else:
            cur = self._sqlite_conn.cursor()
            try:
                cur.execute(sql.replace("INSERT INTO", "INSERT OR IGNORE INTO"), params)
                inserted = cur.rowcount > 0
                self._sqlite_conn.commit()
                return inserted
            except Exception as e:
                logger.error(f"SQLite error inserting order: {e}")
                return False

    def record_trade(self, trade: Dict[str, Any]) -> None:
        """Insert executed trade (fill). integration|full: provenance is mandatory."""
        trade = require_provenance(trade, kind="fill")
        sql = """
            INSERT INTO paper_trades (
                trade_id, order_id, session_id, ticker_symbol, direction,
                fill_price, fill_quantity, slippage_bps, commission, realized_pnl, executed_at,
                model_version, dataset_id, feature_source, alpha_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        alpha_id = trade.get("alpha_id")
        params = (
            trade["trade_id"], trade.get("order_id"), trade["session_id"], trade["ticker_symbol"],
            trade["direction"], float(trade["fill_price"]), float(trade["fill_quantity"]),
            float(trade.get("slippage_bps", 5.0)), float(trade.get("commission", 0.0)),
            float(trade.get("realized_pnl", 0.0)), str(trade["executed_at"]),
            *[str(trade[f]) for f in PROVENANCE_FIELDS],
            None if alpha_id is None or str(alpha_id).strip() == "" else str(alpha_id),
        )
        if self.is_postgres:
            cur = self._pg_conn.cursor()
            try:
                cur.execute(sql.replace("?", "%s"), params)
                self._pg_conn.commit()
            except Exception as e:
                self._pg_conn.rollback()
                logger.error(f"Error recording trade: {e}")
            finally:
                cur.close()
        else:
            cur = self._sqlite_conn.cursor()
            cur.execute(sql, params)
            self._sqlite_conn.commit()

    def upsert_position(self, pos: Dict[str, Any]) -> None:
        """Upsert active open position."""
        if self.is_postgres:
            sql = """
                INSERT INTO paper_positions (
                    session_id, ticker_symbol, side, quantity, average_entry_price,
                    current_mark_price, unrealized_pnl, unrealized_pnl_pct, stop_loss_price,
                    take_profit_price, updated_at, entry_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (session_id, ticker_symbol) DO UPDATE SET
                    side = EXCLUDED.side,
                    quantity = EXCLUDED.quantity,
                    average_entry_price = EXCLUDED.average_entry_price,
                    current_mark_price = EXCLUDED.current_mark_price,
                    unrealized_pnl = EXCLUDED.unrealized_pnl,
                    unrealized_pnl_pct = EXCLUDED.unrealized_pnl_pct,
                    stop_loss_price = EXCLUDED.stop_loss_price,
                    take_profit_price = EXCLUDED.take_profit_price,
                    updated_at = EXCLUDED.updated_at,
                    entry_time = EXCLUDED.entry_time
            """
            cur = self._pg_conn.cursor()
            try:
                cur.execute(sql, (
                    pos["session_id"], pos["ticker_symbol"], pos["side"],
                    float(pos["quantity"]), float(pos["average_entry_price"]),
                    float(pos["current_mark_price"]), float(pos["unrealized_pnl"]),
                    float(pos["unrealized_pnl_pct"]), pos.get("stop_loss_price"),
                    pos.get("take_profit_price"), str(pos["updated_at"]),
                    str(pos.get("entry_time") or pos["updated_at"]),
                ))
                self._pg_conn.commit()
            finally:
                cur.close()
        else:
            sql = """
                INSERT INTO paper_positions (
                    session_id, ticker_symbol, side, quantity, average_entry_price,
                    current_mark_price, unrealized_pnl, unrealized_pnl_pct, stop_loss_price,
                    take_profit_price, updated_at, entry_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (session_id, ticker_symbol) DO UPDATE SET
                    side = excluded.side,
                    quantity = excluded.quantity,
                    average_entry_price = excluded.average_entry_price,
                    current_mark_price = excluded.current_mark_price,
                    unrealized_pnl = excluded.unrealized_pnl,
                    unrealized_pnl_pct = excluded.unrealized_pnl_pct,
                    stop_loss_price = excluded.stop_loss_price,
                    take_profit_price = excluded.take_profit_price,
                    updated_at = excluded.updated_at,
                    entry_time = excluded.entry_time
            """
            cur = self._sqlite_conn.cursor()
            cur.execute(sql, (
                pos["session_id"], pos["ticker_symbol"], pos["side"],
                float(pos["quantity"]), float(pos["average_entry_price"]),
                float(pos["current_mark_price"]), float(pos["unrealized_pnl"]),
                float(pos["unrealized_pnl_pct"]), pos.get("stop_loss_price"),
                pos.get("take_profit_price"), str(pos["updated_at"]),
                str(pos.get("entry_time") or pos["updated_at"]),
            ))
            self._sqlite_conn.commit()

    def remove_position(self, ticker_symbol: str, session_id: Optional[str] = None) -> None:
        """Remove closed position."""
        if session_id:
            sql = "DELETE FROM paper_positions WHERE ticker_symbol = ? AND session_id = ?"
            params: tuple = (ticker_symbol, session_id)
        else:
            sql = "DELETE FROM paper_positions WHERE ticker_symbol = ?"
            params = (ticker_symbol,)
        if self.is_postgres:
            cur = self._pg_conn.cursor()
            try:
                cur.execute(sql.replace("?", "%s"), params)
                self._pg_conn.commit()
            finally:
                cur.close()
        else:
            cur = self._sqlite_conn.cursor()
            cur.execute(sql, params)
            self._sqlite_conn.commit()

    def record_snapshot(self, snap: Dict[str, Any]) -> None:
        """Record portfolio equity & risk metrics snapshot."""
        sql = """
            INSERT INTO paper_portfolio_snapshots (
                session_id, cash_balance, invested_equity, total_portfolio_value,
                realized_pnl_cumulative, unrealized_pnl_current, rolling_sharpe_ratio,
                max_drawdown_pct, active_positions_count, latency_ms, recorded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            snap["session_id"], float(snap["cash_balance"]), float(snap["invested_equity"]),
            float(snap["total_portfolio_value"]), float(snap["realized_pnl_cumulative"]),
            float(snap["unrealized_pnl_current"]), float(snap.get("rolling_sharpe_ratio", 0.0)),
            float(snap.get("max_drawdown_pct", 0.0)), int(snap.get("active_positions_count", 0)),
            float(snap.get("latency_ms", 0.0)), str(snap["recorded_at"])
        )
        if self.is_postgres:
            cur = self._pg_conn.cursor()
            try:
                cur.execute(sql.replace("?", "%s"), params)
                self._pg_conn.commit()
            finally:
                cur.close()
        else:
            cur = self._sqlite_conn.cursor()
            cur.execute(sql, params)
            self._sqlite_conn.commit()

    def close(self) -> None:
        if self._pg_conn is not None:
            self._pg_conn.close()
            self._pg_conn = None
        if self._sqlite_conn is not None:
            self._sqlite_conn.close()
            self._sqlite_conn = None

    def session_has_activity(self, session_id: str) -> bool:
        """True when this session already has trades or portfolio snapshots."""
        sql = """
            SELECT
                (SELECT COUNT(*) FROM paper_trades WHERE session_id = ?)
              + (SELECT COUNT(*) FROM paper_portfolio_snapshots WHERE session_id = ?)
        """
        params = (session_id, session_id)
        if self.is_postgres:
            cur = self._pg_conn.cursor()
            try:
                cur.execute(sql.replace("?", "%s"), params)
                count = int(cur.fetchone()[0])
            finally:
                cur.close()
        else:
            cur = self._sqlite_conn.cursor()
            cur.execute(sql, params)
            count = int(cur.fetchone()[0])
        return count > 0

    def get_latest_session_id(self) -> Optional[str]:
        """Fetch the most recently active session ID."""
        sql = "SELECT session_id FROM paper_portfolio_snapshots ORDER BY snapshot_id DESC LIMIT 1"
        try:
            if self.is_postgres:
                df = pd.read_sql(sql, self._pg_conn)
            else:
                df = pd.read_sql(sql, self._sqlite_conn)
            if not df.empty:
                return str(df.iloc[0]["session_id"])
        except Exception:
            pass
        return None

    def get_positions(self, session_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch currently open positions (optionally filtered by session_id)."""
        if session_id:
            sql = f"SELECT * FROM paper_positions WHERE session_id = '{session_id}' ORDER BY ticker_symbol"
        else:
            sql = "SELECT * FROM paper_positions ORDER BY ticker_symbol"
        if self.is_postgres:
            df = pd.read_sql(sql, self._pg_conn)
            return df.to_dict(orient="records")
        else:
            df = pd.read_sql(sql, self._sqlite_conn)
            return df.to_dict(orient="records")

    def get_recent_trades(self, session_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """Fetch recent executed trades (optionally filtered by session_id)."""
        if session_id:
            sql = f"SELECT * FROM paper_trades WHERE session_id = '{session_id}' ORDER BY executed_at DESC LIMIT {limit}"
        else:
            sql = f"SELECT * FROM paper_trades ORDER BY executed_at DESC LIMIT {limit}"
        if self.is_postgres:
            df = pd.read_sql(sql, self._pg_conn)
            return df.to_dict(orient="records")
        else:
            df = pd.read_sql(sql, self._sqlite_conn)
            return df.to_dict(orient="records")

    def get_snapshots(self, session_id: Optional[str] = None, limit: int = 1000) -> pd.DataFrame:
        """Fetch the most recent portfolio snapshots as DataFrame for charting in chronological order."""
        if session_id:
            sub = f"SELECT * FROM paper_portfolio_snapshots WHERE session_id = '{session_id}' ORDER BY snapshot_id DESC LIMIT {limit}"
        else:
            sub = f"SELECT * FROM paper_portfolio_snapshots ORDER BY snapshot_id DESC LIMIT {limit}"
        sql = f"SELECT * FROM ({sub}) AS sub_snap ORDER BY snapshot_id ASC"
        if self.is_postgres:
            return pd.read_sql(sql, self._pg_conn)
        else:
            # SQLite does not strictly require alias or handles subqueries cleanly
            sql_sqlite = f"SELECT * FROM ({sub}) ORDER BY snapshot_id ASC"
            return pd.read_sql(sql_sqlite, self._sqlite_conn)

    def record_bar(self, bar: Dict[str, Any]) -> None:
        """Record a market bar into paper_market_bars."""
        sql = """
            INSERT INTO paper_market_bars (
                session_id, ticker_symbol, event_timestamp, open_price,
                high_price, low_price, close_price, volume, vwap, p_ml
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            bar["session_id"], bar["ticker_symbol"], str(bar["event_timestamp"]),
            float(bar["open_price"]), float(bar["high_price"]), float(bar["low_price"]),
            float(bar["close_price"]), float(bar.get("volume", 0.0)),
            float(bar["vwap"]) if bar.get("vwap") is not None else None,
            float(bar["p_ml"]) if bar.get("p_ml") is not None else None,
        )
        if self.is_postgres:
            pg_sql = (
                sql.replace("?", "%s")
                + " ON CONFLICT (session_id, ticker_symbol, event_timestamp) DO UPDATE SET "
                + "open_price = EXCLUDED.open_price, high_price = EXCLUDED.high_price, "
                + "low_price = EXCLUDED.low_price, close_price = EXCLUDED.close_price, "
                + "volume = EXCLUDED.volume, vwap = EXCLUDED.vwap, p_ml = EXCLUDED.p_ml"
            )
            cur = self._pg_conn.cursor()
            try:
                cur.execute(pg_sql, params)
                self._pg_conn.commit()
            except Exception as e:
                self._pg_conn.rollback()
                logger.error(f"Error recording bar: {e}")
            finally:
                cur.close()
        else:
            sqlite_sql = sql.replace("INSERT INTO", "INSERT OR REPLACE INTO")
            cur = self._sqlite_conn.cursor()
            cur.execute(sqlite_sql, params)
            self._sqlite_conn.commit()

    def record_bars_batch(self, bars: List[Dict[str, Any]]) -> int:
        """Batch insert market bars."""
        if not bars:
            return 0
        inserted = 0
        for b in bars:
            try:
                self.record_bar(b)
                inserted += 1
            except Exception:
                continue
        return inserted

    def get_market_bars(
        self,
        ticker_symbol: str,
        session_id: Optional[str] = None,
        session_date: Optional[str] = None,
        limit: int = 500,
    ) -> pd.DataFrame:
        """Fetch market bars for a specific ticker and session/date."""
        conditions = ["ticker_symbol = ?"]
        params: List[Any] = [ticker_symbol]

        if session_id:
            conditions.append("session_id = ?")
            params.append(session_id)
        if session_date:
            conditions.append("substr(event_timestamp, 1, 10) = ?")
            params.append(session_date)

        where_clause = " WHERE " + " AND ".join(conditions)
        sql = f"SELECT * FROM paper_market_bars {where_clause} ORDER BY event_timestamp ASC LIMIT {limit}"

        conn = self._pg_conn if self.is_postgres else self._sqlite_conn
        if self.is_postgres:
            pg_sql = sql.replace("?", "%s")
            try:
                return pd.read_sql(pg_sql, conn, params=params)
            except Exception as e:
                logger.debug(f"Postgres get_market_bars fallback: {e}")
                return pd.DataFrame()
        else:
            try:
                return pd.read_sql(sql, conn, params=params)
            except Exception as e:
                logger.debug(f"SQLite get_market_bars error: {e}")
                return pd.DataFrame()

    def get_sessions(self) -> List[Dict[str, Any]]:
        """Fetch all available trading sessions with metadata."""
        sql = """
            SELECT 
                session_id,
                MIN(executed_at) AS session_start,
                MAX(executed_at) AS session_end,
                SUBSTR(MIN(executed_at), 1, 10) AS session_date,
                COUNT(trade_id) AS total_trades,
                COUNT(DISTINCT ticker_symbol) AS symbols_traded
            FROM paper_trades
            GROUP BY session_id
            ORDER BY session_start DESC
        """
        conn = self._pg_conn if self.is_postgres else self._sqlite_conn
        try:
            df = pd.read_sql(sql, conn)
            return df.to_dict(orient="records")
        except Exception:
            return []

    def get_trades_with_orders(
        self,
        ticker_symbol: Optional[str] = None,
        session_id: Optional[str] = None,
        limit: int = 500,
    ) -> pd.DataFrame:
        """Fetch trades joined with orders for full execution and attribution details."""
        conditions = []
        params: List[Any] = []
        if ticker_symbol:
            conditions.append("t.ticker_symbol = ?")
            params.append(ticker_symbol)
        if session_id:
            conditions.append("t.session_id = ?")
            params.append(session_id)

        where_str = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = f"""
            SELECT 
                t.trade_id,
                t.order_id,
                t.session_id,
                t.ticker_symbol,
                t.direction,
                t.fill_price,
                t.fill_quantity,
                t.slippage_bps,
                t.commission,
                t.realized_pnl,
                t.executed_at,
                t.model_version,
                t.dataset_id,
                t.feature_source,
                o.order_type,
                o.requested_price,
                o.bar_timestamp
            FROM paper_trades t
            LEFT JOIN paper_orders o ON t.order_id = o.order_id
            {where_str}
            ORDER BY t.executed_at ASC
            LIMIT {limit}
        """
        conn = self._pg_conn if self.is_postgres else self._sqlite_conn
        if self.is_postgres:
            pg_sql = sql.replace("?", "%s")
            try:
                return pd.read_sql(pg_sql, conn, params=params)
            except Exception:
                return pd.DataFrame()
        else:
            try:
                return pd.read_sql(sql, conn, params=params)
            except Exception:
                return pd.DataFrame()

    def upsert_alpha(self, alpha: Dict[str, Any]) -> None:
        """Register or update an alpha model in alpha_registry (provenance required in integration)."""
        alpha = require_provenance(alpha, kind="alpha_registry row")
        oos_metrics = alpha.get("out_of_sample_metrics", {})
        oos_str = json.dumps(oos_metrics) if isinstance(oos_metrics, (dict, list)) else str(oos_metrics or "{}")
        split_id = alpha.get("split_id", "IS")
        sql = """
            INSERT INTO alpha_registry (
                alpha_id, version, alpha_type, status, description,
                config_json, metrics_snapshot, created_at, promoted_at,
                promoted_by, retired_at, model_version, dataset_id, feature_source,
                out_of_sample_metrics, split_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            alpha["alpha_id"],
            alpha["version"],
            alpha["alpha_type"],
            alpha["status"],
            alpha.get("description", ""),
            json.dumps(alpha.get("config_json", {})) if isinstance(alpha.get("config_json"), dict) else str(alpha.get("config_json", "{}")),
            json.dumps(alpha.get("metrics_snapshot", {})) if isinstance(alpha.get("metrics_snapshot"), dict) else str(alpha.get("metrics_snapshot", "{}")),
            str(alpha.get("created_at") or datetime.now(timezone.utc).isoformat()),
            str(alpha.get("promoted_at")) if alpha.get("promoted_at") else None,
            alpha.get("promoted_by"),
            str(alpha.get("retired_at")) if alpha.get("retired_at") else None,
            *[str(alpha[f]) for f in PROVENANCE_FIELDS],
            oos_str,
            split_id,
        )
        if self.is_postgres:
            pg_sql = (
                sql.replace("?", "%s")
                + """ ON CONFLICT (alpha_id, version) DO UPDATE SET
                    alpha_type = EXCLUDED.alpha_type,
                    status = EXCLUDED.status,
                    description = EXCLUDED.description,
                    config_json = EXCLUDED.config_json,
                    metrics_snapshot = EXCLUDED.metrics_snapshot,
                    promoted_at = EXCLUDED.promoted_at,
                    promoted_by = EXCLUDED.promoted_by,
                    retired_at = EXCLUDED.retired_at,
                    model_version = EXCLUDED.model_version,
                    dataset_id = EXCLUDED.dataset_id,
                    feature_source = EXCLUDED.feature_source,
                    out_of_sample_metrics = EXCLUDED.out_of_sample_metrics,
                    split_id = EXCLUDED.split_id
                """
            )
            cur = self._pg_conn.cursor()
            try:
                cur.execute(pg_sql, params)
                self._pg_conn.commit()
            except Exception as e:
                self._pg_conn.rollback()
                logger.error(f"Error upserting alpha in Postgres: {e}")
            finally:
                cur.close()
        else:
            sqlite_sql = sql.replace("INSERT INTO", "INSERT OR REPLACE INTO")
            cur = self._sqlite_conn.cursor()
            cur.execute(sqlite_sql, params)
            self._sqlite_conn.commit()

    def get_alpha(self, alpha_id: str, version: str) -> Optional[Dict[str, Any]]:
        """Fetch specific alpha version metadata."""
        sql = "SELECT * FROM alpha_registry WHERE alpha_id = ? AND version = ?"
        conn = self._pg_conn if self.is_postgres else self._sqlite_conn
        if self.is_postgres:
            df = pd.read_sql(sql.replace("?", "%s"), conn, params=[alpha_id, version])
        else:
            df = pd.read_sql(sql, conn, params=[alpha_id, version])
        if df.empty:
            return None
        return df.iloc[0].to_dict()

    def list_alphas(
        self, status: Optional[str] = None, alpha_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List registered alphas, optionally filtered by status or type."""
        conditions = []
        params = []
        if status:
            conditions.append("status = ?")
            params.append(status)
        if alpha_type:
            conditions.append("alpha_type = ?")
            params.append(alpha_type)

        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = f"SELECT * FROM alpha_registry {where} ORDER BY created_at DESC"
        conn = self._pg_conn if self.is_postgres else self._sqlite_conn
        if self.is_postgres:
            df = pd.read_sql(sql.replace("?", "%s"), conn, params=params)
        else:
            df = pd.read_sql(sql, conn, params=params)
        return df.to_dict(orient="records")

    def update_alpha_status(
        self,
        alpha_id: str,
        version: str,
        status: str,
        promoted_by: Optional[str] = None,
    ) -> None:
        """Update alpha lifecycle status."""
        now_ts = datetime.now(timezone.utc).isoformat()
        if status in ["PROMOTED_ACTIVE", "PAPER_TRIAL"]:
            sql = "UPDATE alpha_registry SET status = ?, promoted_at = ?, promoted_by = ? WHERE alpha_id = ? AND version = ?"
            params = (status, now_ts, promoted_by or "PM", alpha_id, version)
        elif status == "RETIRED":
            sql = "UPDATE alpha_registry SET status = ?, retired_at = ? WHERE alpha_id = ? AND version = ?"
            params = (status, now_ts, alpha_id, version)
        else:
            sql = "UPDATE alpha_registry SET status = ? WHERE alpha_id = ? AND version = ?"
            params = (status, alpha_id, version)

        conn = self._pg_conn if self.is_postgres else self._sqlite_conn
        if self.is_postgres:
            cur = conn.cursor()
            try:
                cur.execute(sql.replace("?", "%s"), params)
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Postgres error updating alpha status: {e}")
            finally:
                cur.close()
        else:
            cur = conn.cursor()
            cur.execute(sql, params)
            conn.commit()

    def record_daily_signals(self, signals: List[Dict[str, Any]]) -> int:
        """Batch record daily signals for target effective date."""
        if not signals:
            return 0
        sql = """
            INSERT INTO alpha_signals_daily (
                signal_id, alpha_id, version, ticker_symbol, asof_date,
                effective_date, target_side, target_weight, confidence,
                raw_score, regime_detected, created_at,
                model_version, dataset_id, feature_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        inserted = 0
        now_ts = datetime.now(timezone.utc).isoformat()
        conn = self._pg_conn if self.is_postgres else self._sqlite_conn

        for s in signals:
            s = require_provenance(s, kind="alpha signal")
            target_side = s.get("target_side")
            if not target_side:
                target_side = "BUY" if float(s.get("raw_score", 0.0)) >= 0 else "SELL"

            params = (
                s["signal_id"],
                s["alpha_id"],
                s["version"],
                s["ticker_symbol"],
                str(s["asof_date"]),
                str(s["effective_date"]),
                target_side,
                float(s["target_weight"]),
                float(s.get("confidence", 1.0)),
                float(s.get("raw_score", 0.0)),
                s.get("regime_detected", "NORMAL"),
                str(s.get("created_at") or now_ts),
                *[str(s[f]) for f in PROVENANCE_FIELDS],
            )
            if self.is_postgres:
                pg_sql = sql.replace("?", "%s") + " ON CONFLICT (alpha_id, version, ticker_symbol, effective_date) DO UPDATE SET target_side = EXCLUDED.target_side, target_weight = EXCLUDED.target_weight, confidence = EXCLUDED.confidence, raw_score = EXCLUDED.raw_score, model_version = EXCLUDED.model_version, dataset_id = EXCLUDED.dataset_id, feature_source = EXCLUDED.feature_source"
                cur = conn.cursor()
                try:
                    cur.execute(pg_sql, params)
                    conn.commit()
                    inserted += 1
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error inserting daily signal: {e}")
                finally:
                    cur.close()
            else:
                sqlite_sql = sql.replace("INSERT INTO", "INSERT OR REPLACE INTO")
                cur = conn.cursor()
                cur.execute(sqlite_sql, params)
                conn.commit()
                inserted += 1

        return inserted

    def get_daily_signals(
        self,
        effective_date: str,
        alpha_id: Optional[str] = None,
        version: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetch daily signals for a given execution date."""
        conditions = ["effective_date = ?"]
        params = [effective_date]
        if alpha_id:
            conditions.append("alpha_id = ?")
            params.append(alpha_id)
        if version:
            conditions.append("version = ?")
            params.append(version)

        where = " WHERE " + " AND ".join(conditions)
        sql = f"SELECT * FROM alpha_signals_daily {where} ORDER BY ticker_symbol ASC"
        conn = self._pg_conn if self.is_postgres else self._sqlite_conn
        if self.is_postgres:
            return pd.read_sql(sql.replace("?", "%s"), conn, params=params)
        else:
            return pd.read_sql(sql, conn, params=params)

    def record_pm_audit(self, audit: Dict[str, Any]) -> None:
        """Log a PM manual action into pm_governance_audit."""
        sql = """
            INSERT INTO pm_governance_audit (
                action_type, alpha_id, version, executed_by, reason, details, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        now_ts = datetime.now(timezone.utc).isoformat()
        action_type = audit.get("action_type") or audit.get("action", "PM_ACTION")
        reason = audit.get("reason", "PM manual action")
        params = (
            action_type,
            audit.get("alpha_id"),
            audit.get("version"),
            audit.get("executed_by", "PM"),
            reason,
            json.dumps(audit.get("details", {})) if isinstance(audit.get("details"), dict) else str(audit.get("details", "{}")),
            str(audit.get("created_at") or now_ts),
        )
        conn = self._pg_conn if self.is_postgres else self._sqlite_conn
        if self.is_postgres:
            cur = conn.cursor()
            try:
                cur.execute(sql.replace("?", "%s"), params)
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Error recording PM audit: {e}")
            finally:
                cur.close()
        else:
            cur = conn.cursor()
            cur.execute(sql, params)
            conn.commit()

    def get_pm_audits(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Fetch recent PM governance and override actions."""
        sql = f"SELECT * FROM pm_governance_audit ORDER BY created_at DESC LIMIT {limit}"
        conn = self._pg_conn if self.is_postgres else self._sqlite_conn
        try:
            df = pd.read_sql(sql, conn)
            return df.to_dict(orient="records")
        except Exception:
            return []

    def upsert_ensemble_config(self, config: Dict[str, Any]) -> None:
        """Persist Pick-5 ensemble config for a horizon (INTRADAY|DAILY) with immutable approval metadata."""
        writer = str(config.get("updated_by") or config.get("approved_by") or "").strip().lower()
        if self.is_postgres and writer in {"test", "pytest"}:
            raise RuntimeError(
                "A test writer cannot update the production ensemble. Use an explicit sqlite database."
            )
        selected = config.get("selected_alpha_ids", [])
        if isinstance(selected, list):
            selected_json = json.dumps(selected)
        else:
            selected_json = str(selected or "[]")

        weights = config.get("weights") or [0.2] * len(selected)
        weights_json = json.dumps(weights) if isinstance(weights, (list, dict)) else str(weights)

        now_ts = datetime.now(timezone.utc).isoformat()
        dataset_id = config.get("dataset_id")
        model_version = config.get("model_version")
        approval_status = config.get("approval_status", "APPROVED_FROZEN")
        approved_by = config.get("approved_by") or config.get("updated_by", "ADMIN")
        approved_at = config.get("approved_at") or now_ts

        sql = """
            INSERT INTO alpha_ensemble_config (
                ensemble_id, horizon, selected_alpha_ids, booksize,
                top_k_tickers, updated_at, updated_by, dataset_id,
                model_version, weights_json, approval_status, approved_by, approved_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            str(config.get("ensemble_id") or "default"),
            str(config.get("horizon") or "INTRADAY").upper(),
            selected_json,
            float(config.get("booksize", 100_000.0)),
            int(config.get("top_k_tickers", 5)),
            str(config.get("updated_at") or now_ts),
            config.get("updated_by", "system"),
            dataset_id,
            model_version,
            weights_json,
            approval_status,
            approved_by,
            approved_at,
        )
        if self.is_postgres:
            pg_sql = (
                sql.replace("?", "%s")
                + """ ON CONFLICT (ensemble_id, horizon) DO UPDATE SET
                    selected_alpha_ids = EXCLUDED.selected_alpha_ids,
                    booksize = EXCLUDED.booksize,
                    top_k_tickers = EXCLUDED.top_k_tickers,
                    updated_at = EXCLUDED.updated_at,
                    updated_by = EXCLUDED.updated_by,
                    dataset_id = EXCLUDED.dataset_id,
                    model_version = EXCLUDED.model_version,
                    weights_json = EXCLUDED.weights_json,
                    approval_status = EXCLUDED.approval_status,
                    approved_by = EXCLUDED.approved_by,
                    approved_at = EXCLUDED.approved_at
                """
            )
            cur = self._pg_conn.cursor()
            try:
                cur.execute(pg_sql, params)
                self._pg_conn.commit()
            except Exception as e:
                self._pg_conn.rollback()
                logger.error(f"Error upserting ensemble config: {e}")
            finally:
                cur.close()
        else:
            sqlite_sql = sql.replace("INSERT INTO", "INSERT OR REPLACE INTO")
            cur = self._sqlite_conn.cursor()
            cur.execute(sqlite_sql, params)
            self._sqlite_conn.commit()

    def get_ensemble_config(
        self,
        horizon: str = "INTRADAY",
        ensemble_id: str = "default",
    ) -> Optional[Dict[str, Any]]:
        """Fetch saved ensemble config; returns None if absent."""
        sql = """
            SELECT * FROM alpha_ensemble_config
            WHERE ensemble_id = ? AND horizon = ?
        """
        conn = self._pg_conn if self.is_postgres else self._sqlite_conn
        try:
            if self.is_postgres:
                df = pd.read_sql(sql.replace("?", "%s"), conn, params=[ensemble_id, horizon.upper()])
            else:
                df = pd.read_sql(sql, conn, params=[ensemble_id, horizon.upper()])
        except Exception:
            return None
        if df.empty:
            return None
        row = df.iloc[0].to_dict()
        raw_ids = row.get("selected_alpha_ids", "[]")
        if isinstance(raw_ids, str):
            try:
                row["selected_alpha_ids"] = json.loads(raw_ids)
            except Exception:
                row["selected_alpha_ids"] = []
        return row

    def list_ensemble_configs(self, horizon: Optional[str] = None) -> List[Dict[str, Any]]:
        """List ensemble configs, optionally filtered by horizon."""
        if horizon:
            sql = "SELECT * FROM alpha_ensemble_config WHERE horizon = ? ORDER BY updated_at DESC"
            params: List[Any] = [horizon.upper()]
        else:
            sql = "SELECT * FROM alpha_ensemble_config ORDER BY updated_at DESC"
            params = []
        conn = self._pg_conn if self.is_postgres else self._sqlite_conn
        try:
            if self.is_postgres and params:
                df = pd.read_sql(sql.replace("?", "%s"), conn, params=params)
            elif params:
                df = pd.read_sql(sql, conn, params=params)
            else:
                df = pd.read_sql(sql, conn)
        except Exception:
            return []
        records = df.to_dict(orient="records")
        for row in records:
            raw_ids = row.get("selected_alpha_ids", "[]")
            if isinstance(raw_ids, str):
                try:
                    row["selected_alpha_ids"] = json.loads(raw_ids)
                except Exception:
                    row["selected_alpha_ids"] = []
        return records

    def close(self) -> None:
        """Close active database connections."""
        if self._pg_conn:
            self._pg_conn.close()
        if self._sqlite_conn:
            self._sqlite_conn.close()
