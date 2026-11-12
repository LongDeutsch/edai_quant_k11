"""Page 3: Real-Time Trading Simulation & Execution Terminal (M1-38).

Features:
- Streaming Market Replay Controller (Start / Pause / Resume / Stop) running in safe background worker.
- Active Top-5 Alphas ensemble signal matrix with ML directional probability (KServe / Feast).
- Institutional intraday scalping risk controls: Stop Loss (-12 bps), Take Profit (+20 bps), Max 5 positions, Circuit Breaker (-2.0%).
- Real-time PnL & Portfolio valuation, live open positions, recent trade execution fills in PostgreSQL.
- ACID idempotency preventing duplicate orders upon session replay or service restarts.
"""

from __future__ import annotations

import logging
import time
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

logger = logging.getLogger("DashboardTrading")

from quant_edai.serving.dashboard.dashboard_service import (
    AlphaResearchService,
    MarketDataService,
    ReplayController,
    get_postgres_connection,
)

st.set_page_config(
    page_title="Real-Time Trading | Quant Terminal",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("⚡ Real-Time Trading Simulation & Risk Terminal")
st.caption("Event-driven paper execution engine, live streaming alpha signals, and institutional risk management backed by PostgreSQL ACID storage.")

# -----------------------------------------------------------------------------
# 1. Replay Control Strip (Top Toolbar)
# -----------------------------------------------------------------------------
st.subheader("🕹️ Market Replay Controller")

state = ReplayController.get_state()

c_sess, c_ds, c_spd, c_act = st.columns([2, 3, 1.5, 3.5])

with c_sess:
    sess_id = st.text_input("Session ID", value=state["session_id"] or "sess_live_01")
with c_ds:
    adv_mode = st.checkbox("⚙️ Advanced Mode", value=False, help="Show raw ClickHouse test/idempotency datasets.")
    ds_options = MarketDataService.get_catalog_options(advanced_mode=adv_mode)
    selected_preset = st.selectbox("Market Dataset Preset", ds_options, index=0)
    selected_ds = MarketDataService.resolve_dataset_id(selected_preset)
    st.caption(f"Resolved Dataset: `{selected_ds}`")
with c_spd:
    speed = st.selectbox("Replay Speed", [1, 5, 10, 25, 50], index=2)
with c_act:
    st.write("Controls")
    btn_c1, btn_c2, btn_c3 = st.columns(3)
    with btn_c1:
        if not state["is_running"]:
            if st.button("▶ Start", type="primary", use_container_width=True):
                saved_alphas = AlphaResearchService.get_latest_top5_ensemble() or ["composite_eq"]
                ReplayController.start_replay(
                    session_id=sess_id,
                    dataset_id=selected_ds,
                    speed=speed,
                    selected_alphas=saved_alphas,
                )
                st.rerun()
        else:
            if st.button("⏹ Stop", type="secondary", use_container_width=True):
                ReplayController.stop_replay()
                st.rerun()
    with btn_c2:
        if state["is_running"] and not state["is_paused"]:
            if st.button("⏸ Pause", use_container_width=True):
                ReplayController.pause_replay()
                st.rerun()
        elif state["is_running"] and state["is_paused"]:
            if st.button("⏯ Resume", use_container_width=True):
                ReplayController.resume_replay()
                st.rerun()
    with btn_c3:
        if st.button("🔄 Refresh", use_container_width=True):
            st.rerun()

# Replay Progress Status Strip
status_color = "🟢 RUNNING" if state["is_running"] and not state["is_paused"] else ("🟡 PAUSED" if state["is_paused"] else "⚪ IDLE")
prog = (state["processed_bars"] / max(1, state["total_bars"])) if state["total_bars"] > 0 else 0.0

st.progress(min(1.0, prog), text=f"Status: {status_color} | Progress: {state['processed_bars']}/{state['total_bars']} bars | Time: {state['current_time'] or 'N/A'}")

st.markdown("---")

# -----------------------------------------------------------------------------
# 2. Portfolio KPIs & Risk Guard Strip
# -----------------------------------------------------------------------------
def get_portfolio_kpis(session_id: str):
    """Retrieve live equity, cash, realized PnL and drawdown from PostgreSQL paper_portfolio_snapshots."""
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT cash_balance, invested_equity, total_portfolio_value, realized_pnl_cumulative, unrealized_pnl_current, max_drawdown_pct
            FROM paper_portfolio_snapshots
            WHERE session_id = %s
            ORDER BY recorded_at DESC LIMIT 1;
        """, (session_id,))
        row = cur.fetchone()
        conn.close()
        if row:
            return {
                "cash": float(row[0]),
                "invested": float(row[1]),
                "equity": float(row[2]),
                "realized_pnl": float(row[3]),
                "unrealized_pnl": float(row[4]),
                "drawdown": float(row[5]),
                "status": "LIVE",
            }
        else:
            # Clean initial session before first snapshot (no trades executed yet)
            return {
                "cash": 100000.0,
                "invested": 0.0,
                "equity": 100000.0,
                "realized_pnl": 0.0,
                "unrealized_pnl": 0.0,
                "drawdown": 0.0,
                "status": "INITIAL",
            }
    except Exception as e:
        logger.error(f"PostgreSQL error querying portfolio KPIs for session '{session_id}': {e}")
        st.error(f"❌ PostgreSQL Portfolio Snapshots Query Error: {e}")
        return {
            "cash": 0.0,
            "invested": 0.0,
            "equity": 0.0,
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "drawdown": 0.0,
            "status": "ERROR",
            "error_msg": str(e),
        }

kpis = get_portfolio_kpis(sess_id)

k1, k2, k3, k4, k5 = st.columns(5)
total_pnl = kpis["realized_pnl"] + kpis["unrealized_pnl"]
pnl_pct = (total_pnl / 100000.0) * 100.0
k1.metric("Total Equity", f"${kpis['equity']:,.2f}", delta=f"{pnl_pct:+.2f}%")
k2.metric("Cash Balance", f"${kpis['cash']:,.2f}")
k3.metric("Realized PnL", f"${kpis['realized_pnl']:+,.2f}")
k4.metric("Unrealized PnL", f"${kpis['unrealized_pnl']:+,.2f}")

circuit_breaker = kpis["drawdown"] <= -2.0
cb_label = "🚨 TRIPPED (-2.0%)" if circuit_breaker else "✅ HEALTHY"
k5.metric("Current Drawdown", f"{kpis['drawdown']:.2f}%", delta=cb_label, delta_color="inverse" if circuit_breaker else "normal")

# -----------------------------------------------------------------------------
# 3. Active Top-5 Ensemble Signals & Model Telemetry
# -----------------------------------------------------------------------------
st.subheader("🎯 Active Top-5 Ensemble & ML Directional Signals")
st.caption("Bar-closed execution signals: Signals are evaluated strictly after bar closure to eliminate look-ahead leakage.")

saved_alphas = AlphaResearchService.get_latest_top5_ensemble() or ["composite_eq", "momentum_1m", "vwap_dev", "rsi_reversal", "breakout_vol"]
latest_sigs = state.get("latest_signals", {})

sig_rows = []
for tkr in ["AAPL", "NVDA", "MSFT", "AMZN", "GOOGL"]:
    data = latest_sigs.get(tkr, {
        "price": 150.0,
        "p_ml": 0.50,
        "z_combined": 0.0,
        "action": "HOLD",
        "status": "WAITING: No bar received yet",
        "position": "FLAT",
        "unrealized_pnl": 0.0,
        "last_bar_time": "N/A",
    })
    sig_rows.append({
        "Ticker": tkr,
        "Mark Price": f"${data['price']:.2f}",
        "ML Direction P(Up)": f"{data['p_ml']:.4f}",
        "Combined Z-Score": f"{data['z_combined']:+.2f}",
        "Signal Action": data["action"],
        "Execution / Rejection Status": data.get("status", "EVALUATING"),
        "Position": data.get("position", "FLAT"),
        "Unrealized PnL": f"${data.get('unrealized_pnl', 0.0):+,.2f}",
        "Last Event Time": data.get("last_bar_time", "N/A"),
    })

st.dataframe(pd.DataFrame(sig_rows), use_container_width=True, hide_index=True)

# -----------------------------------------------------------------------------
# 4. Open Positions & Executed Orders (PostgreSQL ACID Ledger)
# -----------------------------------------------------------------------------
p_col, t_col = st.columns([1, 1])

with p_col:
    st.subheader("📌 Open Positions Matrix")
    try:
        conn = get_postgres_connection()
        pos_df = pd.read_sql("SELECT ticker_symbol, side, quantity, average_entry_price, current_mark_price, unrealized_pnl, stop_loss_price, take_profit_price FROM paper_positions WHERE session_id = %s", conn, params=[sess_id])
        conn.close()
        if not pos_df.empty:
            st.dataframe(pos_df, use_container_width=True, hide_index=True)
        else:
            st.info("No open positions. Waiting for entry signals...")
    except Exception as e:
        st.warning(f"Positions query: {e}")

with t_col:
    st.subheader("📜 Recent Fills (paper_trades)")
    try:
        conn = get_postgres_connection()
        trades_df = pd.read_sql("SELECT trade_id, ticker_symbol, direction, fill_price, fill_quantity, slippage_bps, realized_pnl, executed_at FROM paper_trades WHERE session_id = %s ORDER BY executed_at DESC LIMIT 8", conn, params=[sess_id])
        conn.close()
        if not trades_df.empty:
            st.dataframe(trades_df, use_container_width=True, hide_index=True)
        else:
            st.info("No trades executed in this session yet.")
    except Exception as e:
        st.warning(f"Trades query: {e}")

# Auto-refresh loop while running
if state["is_running"] and not state["is_paused"]:
    time.sleep(1.0)
    st.rerun()
