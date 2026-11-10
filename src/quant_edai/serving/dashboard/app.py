"""Main Entrypoint for Institutional Quant AI Trading & Market Intelligence Dashboard."""

import os
import streamlit as st

st.set_page_config(
    page_title="Quant AI Platform - Trading & Alpha Hub",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🏛️ Institutional Quant AI Trading & Market Intelligence")
st.markdown("### Real-time Event-Driven Trading, Scalping Risk Controls & Alpha Analytics")

st.markdown("""
Welcome to the **Quant AI Engineering Platform Command Center**. This dashboard visualizes the end-to-end quantitative trading infrastructure built across Phases 1 through 6.
""")

col1, col2, col3, col4 = st.columns(4)

# Database health check
from quant_edai.quant.paper_db import PaperDatabase

db = PaperDatabase()
positions = db.get_positions()
trades = db.get_recent_trades(limit=10)
snapshots = db.get_snapshots(limit=1)

db_backend = "PostgreSQL (quant_postgres)" if db.is_postgres else "SQLite Local Fallback"

with col1:
    st.metric("Database Storage", "Connected", delta=db_backend, delta_color="normal")
with col2:
    st.metric("Active Positions", f"{len(positions)} / 5 Max", delta="Capacity Safe")
with col3:
    st.metric("Executed Trades", f"{len(trades)} Orders", delta="ACID Recorded")
with col4:
    has_phase5 = os.path.exists("evidence/phase5/backtest_summary.json")
    st.metric("Phase 5 Backtest Evidence", "Available" if has_phase5 else "Awaiting Run", delta="M1-37 Verified" if has_phase5 else "Pending make phase5")

st.markdown("---")

st.subheader("📑 Dashboard Navigation & Platform Architecture")

c1, c2, c3 = st.columns(3)

with c1:
    st.info("### 1. Live Trading & Risk\n**Page 1**\n\n- Real-time PnL & Equity tracking\n- Open Positions Matrix with distance to SL/TP\n- Live Trade Execution log\n- Daily Drawdown & Circuit Breaker monitoring")

with c2:
    st.success("### 2. Candlestick Charts\n**Page 2**\n\n- 1m / 5m Candlestick charts (ClickHouse OLAP)\n- Technical Overlays: SMA(20), SMA(50), Intraday VWAP\n- Trade entry (▲ BUY) and exit (▼ SELL) markers\n- Volume analysis")

with c3:
    st.warning("### 3. Alpha Quality Analytics\n**Page 3**\n\n- **Tab 1**: Live Intraday Scalping metrics (Win Rate, Profit Factor, Underwater Drawdown)\n- **Tab 2**: Daily Walk-Forward Backtester metrics vs. SPY (M1-37 tear sheets & IC series)")

st.markdown("---")
st.markdown("**Quick Start**: Use the left sidebar to navigate between pages, or run simulation commands from the terminal via `make trader-replay SPEED=50`.")
