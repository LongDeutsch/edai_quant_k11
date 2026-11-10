"""Page 1: Live Trading & Risk Management Dashboard (M1-38)."""

import os
import time
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from quant_edai.quant.paper_db import PaperDatabase

st.set_page_config(page_title="Live Trading & Risk | Quant AI", page_icon="⚡", layout="wide")

st.title("⚡ Live Trading & Risk Management")
st.markdown("Real-time monitoring of intraday orders, positions, and institutional risk limits.")

db = PaperDatabase()

# Top KPI Header
positions = db.get_positions()
recent_trades = db.get_recent_trades(limit=100)
snapshots_df = db.get_snapshots(limit=500)

if not snapshots_df.empty:
    latest_snap = snapshots_df.iloc[-1]
    net_worth = float(latest_snap["total_portfolio_value"])
    cash_bal = float(latest_snap["cash_balance"])
    realized_pnl = float(latest_snap["realized_pnl_cumulative"])
    unrealized_pnl = float(latest_snap["unrealized_pnl_current"])
    mdd_pct = float(latest_snap["max_drawdown_pct"]) * 100.0
    latency_ms = float(latest_snap.get("latency_ms", 0.0))
else:
    net_worth = 100_000.0
    cash_bal = 100_000.0
    realized_pnl = 0.0
    unrealized_pnl = 0.0
    mdd_pct = 0.0
    latency_ms = 0.0

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.metric("Total Equity", f"${net_worth:,.2f}", delta=f"${(net_worth - 100000.0):+,.2f}")
with col2:
    st.metric("Cash Balance", f"${cash_bal:,.2f}")
with col3:
    st.metric("Realized PnL", f"${realized_pnl:+,.2f}", delta_color="normal")
with col4:
    st.metric("Unrealized PnL", f"${unrealized_pnl:+,.2f}", delta_color="normal")
with col5:
    st.metric("Max Drawdown", f"{mdd_pct:.2f}%", delta=f"Latency: {latency_ms:.2f}ms")

st.markdown("---")

# Section 1: Open Positions Matrix
st.subheader(f"📊 Active Open Positions ({len(positions)} / 5 Max)")

if positions:
    df_pos = pd.DataFrame(positions)
    # Format display columns
    display_cols = [
        "ticker_symbol", "side", "quantity", "average_entry_price",
        "current_mark_price", "unrealized_pnl", "unrealized_pnl_pct",
        "stop_loss_price", "take_profit_price", "updated_at"
    ]
    df_pos_clean = df_pos[[c for c in display_cols if c in df_pos.columns]].copy()
    df_pos_clean.rename(columns={
        "ticker_symbol": "Ticker", "side": "Side", "quantity": "Quantity",
        "average_entry_price": "Entry Price ($)", "current_mark_price": "Mark Price ($)",
        "unrealized_pnl": "Unrealized ($)", "unrealized_pnl_pct": "Unrealized (%)",
        "stop_loss_price": "Stop Loss ($)", "take_profit_price": "Take Profit ($)",
        "updated_at": "Last Updated"
    }, inplace=True)
    st.dataframe(df_pos_clean, use_container_width=True)
else:
    st.info("No active open positions. Portfolio is currently in cash or waiting for signal entry.")

st.markdown("---")

# Section 2: Intraday Equity Curve & Drawdown Chart
if not snapshots_df.empty:
    st.subheader("📈 Intraday Equity Progression")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=pd.to_datetime(snapshots_df["recorded_at"], format="ISO8601", utc=True),
        y=snapshots_df["total_portfolio_value"],
        mode="lines",
        name="Portfolio Equity ($)",
        line=dict(color="#00CC96", width=2.5),
    ))
    fig.add_hline(y=100_000.0, line_dash="dash", line_color="gray", annotation_text="Starting Capital ($100,000)")
    fig.update_layout(height=350, margin=dict(l=20, r=20, t=30, b=20), xaxis_title="Time", yaxis_title="Total Equity ($)")
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# Section 3: Recent Executed Trades Log
st.subheader("📜 Recent Executed Orders & Trades Log")
if recent_trades:
    df_trades = pd.DataFrame(recent_trades)
    trade_cols = [
        "trade_id", "ticker_symbol", "direction", "fill_price",
        "fill_quantity", "slippage_bps", "commission", "realized_pnl", "executed_at"
    ]
    df_trades_clean = df_trades[[c for c in trade_cols if c in df_trades.columns]].copy()
    df_trades_clean.rename(columns={
        "trade_id": "Trade ID", "ticker_symbol": "Ticker", "direction": "Action",
        "fill_price": "Fill Price ($)", "fill_quantity": "Quantity", "slippage_bps": "Slippage (bps)",
        "commission": "Commission ($)", "realized_pnl": "Realized PnL ($)", "executed_at": "Timestamp"
    }, inplace=True)
    st.dataframe(df_trades_clean, use_container_width=True)
else:
    st.info("No trades executed yet. Run `make trader-replay SPEED=50` to simulate live trading.")
