"""Database access layer for Paper Trading ACID persistence and idempotency."""

from __future__ import annotations

import logging
import os
import sqlite3
from typing import Any, Dict, List, Optional
import pandas as pd

logger = logging.getLogger(__name__)

# Matches infra/docker-compose.yml service ``postgres`` (user/password/db).
DEFAULT_PG_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://quant:quant_password@localhost:5432/quant_db",
)
DEFAULT_SQLITE_PATH = os.environ.get("SQLITE_PAPER_DB", "paper_trading.db")


class PaperDatabase:
    """Manages paper trading persistence across PostgreSQL and SQLite fallback."""

    def __init__(self, db_url: Optional[str] = None, sqlite_path: Optional[str] = None):
        self.db_url = db_url or DEFAULT_PG_URL
        self.sqlite_path = sqlite_path or DEFAULT_SQLITE_PATH
        self.is_postgres = False
        self._pg_conn = None
        self._sqlite_conn = None

        self._connect()

    def _connect(self) -> None:
        """Attempt PostgreSQL connection first, fallback to SQLite."""
        # Check if Postgres should be attempted
        if os.environ.get("USE_SQLITE_ONLY") != "1":
            try:
                import psycopg2
                conn = psycopg2.connect(self.db_url, connect_timeout=1)
                conn.autocommit = False
                self._pg_conn = conn
                self.is_postgres = True
                logger.info("Connected to PostgreSQL database for Paper Trading.")
                return
            except Exception as e:
                logger.debug(f"PostgreSQL connection unavailable ({e}); using SQLite backend.")

        # Fallback to SQLite
        self.is_postgres = False
        self._sqlite_conn = sqlite3.connect(self.sqlite_path, check_same_thread=False)
        self._sqlite_conn.row_factory = sqlite3.Row
        logger.info(f"Using SQLite database at {self.sqlite_path} for Paper Trading.")

    def init_db(self, reset: bool = False) -> None:
        """Initialize or reset tables."""
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
            """)
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
        """)
        self._sqlite_conn.commit()

    def record_order(self, order: Dict[str, Any]) -> bool:
        """Insert order with idempotency check. Returns True if inserted, False if duplicate."""
        bar_ts = str(order.get("bar_timestamp") or order.get("created_at"))
        created_at = str(order.get("created_at") or bar_ts)
        sql = """
            INSERT INTO paper_orders (
                order_id, session_id, ticker_symbol, order_type, direction,
                quantity, requested_price, status, bar_timestamp, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            order["order_id"], order["session_id"], order["ticker_symbol"],
            order["order_type"], order["direction"], float(order["quantity"]),
            float(order["requested_price"]), order["status"], bar_ts, created_at,
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
        """Insert executed trade."""
        sql = """
            INSERT INTO paper_trades (
                trade_id, order_id, session_id, ticker_symbol, direction,
                fill_price, fill_quantity, slippage_bps, commission, realized_pnl, executed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            trade["trade_id"], trade.get("order_id"), trade["session_id"], trade["ticker_symbol"],
            trade["direction"], float(trade["fill_price"]), float(trade["fill_quantity"]),
            float(trade.get("slippage_bps", 5.0)), float(trade.get("commission", 0.0)),
            float(trade.get("realized_pnl", 0.0)), str(trade["executed_at"])
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
                    take_profit_price, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (session_id, ticker_symbol) DO UPDATE SET
                    side = EXCLUDED.side,
                    quantity = EXCLUDED.quantity,
                    average_entry_price = EXCLUDED.average_entry_price,
                    current_mark_price = EXCLUDED.current_mark_price,
                    unrealized_pnl = EXCLUDED.unrealized_pnl,
                    unrealized_pnl_pct = EXCLUDED.unrealized_pnl_pct,
                    stop_loss_price = EXCLUDED.stop_loss_price,
                    take_profit_price = EXCLUDED.take_profit_price,
                    updated_at = EXCLUDED.updated_at
            """
            cur = self._pg_conn.cursor()
            try:
                cur.execute(sql, (
                    pos["session_id"], pos["ticker_symbol"], pos["side"],
                    float(pos["quantity"]), float(pos["average_entry_price"]),
                    float(pos["current_mark_price"]), float(pos["unrealized_pnl"]),
                    float(pos["unrealized_pnl_pct"]), pos.get("stop_loss_price"),
                    pos.get("take_profit_price"), str(pos["updated_at"])
                ))
                self._pg_conn.commit()
            finally:
                cur.close()
        else:
            sql = """
                INSERT INTO paper_positions (
                    session_id, ticker_symbol, side, quantity, average_entry_price,
                    current_mark_price, unrealized_pnl, unrealized_pnl_pct, stop_loss_price,
                    take_profit_price, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (session_id, ticker_symbol) DO UPDATE SET
                    side = excluded.side,
                    quantity = excluded.quantity,
                    average_entry_price = excluded.average_entry_price,
                    current_mark_price = excluded.current_mark_price,
                    unrealized_pnl = excluded.unrealized_pnl,
                    unrealized_pnl_pct = excluded.unrealized_pnl_pct,
                    stop_loss_price = excluded.stop_loss_price,
                    take_profit_price = excluded.take_profit_price,
                    updated_at = excluded.updated_at
            """
            cur = self._sqlite_conn.cursor()
            cur.execute(sql, (
                pos["session_id"], pos["ticker_symbol"], pos["side"],
                float(pos["quantity"]), float(pos["average_entry_price"]),
                float(pos["current_mark_price"]), float(pos["unrealized_pnl"]),
                float(pos["unrealized_pnl_pct"]), pos.get("stop_loss_price"),
                pos.get("take_profit_price"), str(pos["updated_at"])
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

    def get_positions(self) -> List[Dict[str, Any]]:
        """Fetch all currently open positions."""
        sql = "SELECT * FROM paper_positions ORDER BY ticker_symbol"
        if self.is_postgres:
            df = pd.read_sql(sql, self._pg_conn)
            return df.to_dict(orient="records")
        else:
            df = pd.read_sql(sql, self._sqlite_conn)
            return df.to_dict(orient="records")

    def get_recent_trades(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Fetch recent executed trades."""
        sql = f"SELECT * FROM paper_trades ORDER BY executed_at DESC LIMIT {limit}"
        if self.is_postgres:
            df = pd.read_sql(sql, self._pg_conn)
            return df.to_dict(orient="records")
        else:
            df = pd.read_sql(sql, self._sqlite_conn)
            return df.to_dict(orient="records")

    def get_snapshots(self, limit: int = 1000) -> pd.DataFrame:
        """Fetch portfolio snapshots as DataFrame for charting."""
        sql = f"SELECT * FROM paper_portfolio_snapshots ORDER BY recorded_at ASC LIMIT {limit}"
        if self.is_postgres:
            return pd.read_sql(sql, self._pg_conn)
        else:
            return pd.read_sql(sql, self._sqlite_conn)

    def close(self) -> None:
        """Close active database connections."""
        if self._pg_conn:
            self._pg_conn.close()
        if self._sqlite_conn:
            self._sqlite_conn.close()
