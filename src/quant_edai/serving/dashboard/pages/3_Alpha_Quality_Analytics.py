"""Page 3: Alpha Quality Analytics - Intraday Live vs. Daily Walk-Forward Backtest."""

import json
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from quant_edai.quant.paper_db import PaperDatabase

st.set_page_config(page_title="Alpha Quality Analytics | Quant AI", page_icon="🔬", layout="wide")

st.title("🔬 Alpha Quality & Factor Attribution Analytics")
st.markdown("Comparative quality evaluation across **Live Intraday Scalping (Phase 6)** and **Daily Walk-Forward Factor Backtesting (Phase 5)**.")

tab_intraday, tab_daily = st.tabs(["⚡ Tab 1: Live Intraday Scalping Alpha (Phase 6)", "📅 Tab 2: Daily Alpha Metrics & Tear Sheets (Phase 5 - M1-37)"])

# ==============================================================================
# TAB 1: Live Intraday Alpha Metrics (PostgreSQL / SQLite)
# ==============================================================================
with tab_intraday:
    st.subheader("Live Intraday Factor Performance & Execution Quality")
    db = PaperDatabase()
    trades = db.get_recent_trades(limit=1000)
    snapshots_df = db.get_snapshots(limit=1000)

    if trades:
        df_tr = pd.DataFrame(trades)
        closed_trades = df_tr[df_tr["direction"] == "SELL"].copy()

        if not closed_trades.empty:
            closed_trades["realized_pnl"] = closed_trades["realized_pnl"].astype(float)
            winning = closed_trades[closed_trades["realized_pnl"] > 0]
            losing = closed_trades[closed_trades["realized_pnl"] < 0]

            gross_profit = winning["realized_pnl"].sum()
            gross_loss = abs(losing["realized_pnl"].sum())
            profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (gross_profit if gross_profit > 0 else 1.0)
            win_rate = (len(winning) / len(closed_trades)) * 100.0
            avg_win = winning["realized_pnl"].mean() if not winning.empty else 0.0
            avg_loss = abs(losing["realized_pnl"].mean()) if not losing.empty else 0.0
            win_loss_ratio = (avg_win / avg_loss) if avg_loss > 0 else avg_win

            c1, c2, c3, c4 = st.columns(4)
            with c1:
                st.metric("Realized Win Rate", f"{win_rate:.1f}%", f"{len(winning)}W / {len(losing)}L")
            with c2:
                st.metric("Profit Factor", f"{profit_factor:.2f}", f"GP: ${gross_profit:,.2f}")
            with c3:
                st.metric("Avg Win / Avg Loss", f"{win_loss_ratio:.2f}", f"Avg Win: ${avg_win:.2f}")
            with c4:
                st.metric("Total Closed PnL", f"${(gross_profit - gross_loss):+,.2f}")

            st.markdown("---")

            col_hist, col_dd = st.columns(2)
            with col_hist:
                st.markdown("#### Trade PnL Distribution")
                fig_hist = px.histogram(
                    closed_trades, x="realized_pnl", nbins=20,
                    color_discrete_sequence=["#1f77b4"],
                    labels={"realized_pnl": "Realized PnL per Trade ($)"}
                )
                fig_hist.update_layout(height=300, margin=dict(l=20, r=20, t=20, b=20))
                st.plotly_chart(fig_hist, use_container_width=True)

            with col_dd:
                st.markdown("#### Intraday Drawdown Underwater Chart")
                if not snapshots_df.empty:
                    fig_dd = go.Figure()
                    fig_dd.add_trace(go.Scatter(
                        x=pd.to_datetime(snapshots_df["recorded_at"], format="ISO8601", utc=True),
                        y=snapshots_df["max_drawdown_pct"] * 100.0,
                        fill="tozeroy",
                        mode="lines",
                        line=dict(color="#d62728"),
                        name="Drawdown (%)"
                    ))
                    fig_dd.update_layout(height=300, margin=dict(l=20, r=20, t=20, b=20), yaxis_title="Drawdown (%)")
                    st.plotly_chart(fig_dd, use_container_width=True)
        else:
            st.info("No closed trades recorded yet. Run `make trader-replay SPEED=50` to generate trade activity.")
    else:
        st.info("No paper trades found in database. Run `make trader-replay` to simulate intraday trading.")

# ==============================================================================
# TAB 2: Daily Alpha Metrics (Backtest Phase 5 - M1-37 JSON Contract)
# ==============================================================================
with tab_daily:
    st.subheader("Daily Factor Walk-Forward Backtester Scorecard (M1-37)")
    phase5_json_path = "evidence/phase5/backtest_summary.json"

    if not os.path.exists(phase5_json_path):
        st.warning(
            "⚠️ **Awaiting Phase 5 Backtest Run (`make phase5`)** — "
            "Evidence file `evidence/phase5/backtest_summary.json` was not found. "
            "Please run `make phase5` in your terminal to execute the walk-forward alpha factor evaluation and generate tear sheets."
        )
    else:
        with open(phase5_json_path, "r", encoding="utf-8") as f:
            bt_data = json.load(f)

        m = bt_data["metrics"]
        d_range = bt_data.get("date_range", {})

        st.success(f"✅ Loaded Phase 5 Walk-Forward Backtest Evidence ({d_range.get('trading_days', 0)} trading days: {d_range.get('start_date')} to {d_range.get('end_date')})")

        # Top KPI Scorecard
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            st.metric("Information Coefficient (IC)", f"{m['ic_mean']:+.4f}", f"Rank IC: {m['rank_ic_mean']:+.4f}")
        with k2:
            st.metric("Strategy Sharpe (periods=252)", f"{m['sharpe']:.2f}", f"vs SPY: {m['spy_sharpe']:.2f} (+{m['sharpe'] - m['spy_sharpe']:.2f})")
        with k3:
            st.metric("Cumulative Return", f"{m['cum_return']*100:+.2f}%", f"SPY: {m['spy_cum_return']*100:+.2f}%")
        with k4:
            st.metric("Max Drawdown", f"{m['max_drawdown']*100:.2f}%", f"SPY: {m['spy_max_drawdown']*100:.2f}%")

        st.markdown("---")

        # Comparative Table
        st.markdown("#### Quantitative Performance vs S&P 500 (SPY) Benchmark")
        comp_df = pd.DataFrame([
            {"Metric": "Cumulative Return", "Quant Alpha Strategy (M1-37)": f"{m['cum_return']*100:+.2f}%", "S&P 500 ETF (SPY)": f"{m['spy_cum_return']*100:+.2f}%", "Alpha Excess": f"{m['excess_alpha_vs_spy']*100:+.2f}%"},
            {"Metric": "Annualized Return", "Quant Alpha Strategy (M1-37)": f"{m['annualized_return']*100:+.2f}%", "S&P 500 ETF (SPY)": f"{m['spy_annualized_return']*100:+.2f}%", "Alpha Excess": f"{(m['annualized_return'] - m['spy_annualized_return'])*100:+.2f}%"},
            {"Metric": "Annualized Volatility", "Quant Alpha Strategy (M1-37)": f"{m['annualized_volatility']*100:.2f}%", "S&P 500 ETF (SPY)": f"{m['spy_annualized_volatility']*100:.2f}%", "Alpha Excess": f"{(m['annualized_volatility'] - m['spy_annualized_volatility'])*100:+.2f}%"},
            {"Metric": "Sharpe Ratio (Rf=4%, p=252)", "Quant Alpha Strategy (M1-37)": f"{m['sharpe']:.2f}", "S&P 500 ETF (SPY)": f"{m['spy_sharpe']:.2f}", "Alpha Excess": f"{m['sharpe'] - m['spy_sharpe']:+.2f}"},
            {"Metric": "Sortino Ratio", "Quant Alpha Strategy (M1-37)": f"{m['sortino']:.2f}", "S&P 500 ETF (SPY)": "-", "Alpha Excess": "-"},
            {"Metric": "Calmar Ratio", "Quant Alpha Strategy (M1-37)": f"{m['calmar']:.2f}", "S&P 500 ETF (SPY)": "-", "Alpha Excess": "-"},
            {"Metric": "Max Drawdown (MDD)", "Quant Alpha Strategy (M1-37)": f"{m['max_drawdown']*100:.2f}%", "S&P 500 ETF (SPY)": f"{m['spy_max_drawdown']*100:.2f}%", "Alpha Excess": f"{(m['max_drawdown'] - m['spy_max_drawdown'])*100:+.2f}%"},
            {"Metric": "Information Ratio (IC_IR)", "Quant Alpha Strategy (M1-37)": f"{m['ic_ir']:.3f}", "S&P 500 ETF (SPY)": "-", "Alpha Excess": "-"},
        ])
        st.dataframe(comp_df, use_container_width=True)

        st.markdown("---")

        # Interactive Charts from JSON Series
        series_data = bt_data.get("series", {})

        if "equity_curve" in series_data and series_data["equity_curve"]:
            st.markdown("#### Walk-Forward Cumulative Equity Curve vs SPY Benchmark")
            df_eq = pd.DataFrame(series_data["equity_curve"])
            fig_eq = go.Figure()
            fig_eq.add_trace(go.Scatter(x=df_eq["date"], y=df_eq["strategy_equity"], name="Quant Alpha Strategy", line=dict(color="#1f77b4", width=2.5)))
            fig_eq.add_trace(go.Scatter(x=df_eq["date"], y=df_eq["spy_equity"], name="S&P 500 Benchmark (SPY)", line=dict(color="#7f7f7f", width=1.5, dash="dash")))
            fig_eq.update_layout(height=380, margin=dict(l=20, r=20, t=20, b=20), xaxis_title="Date", yaxis_title="Equity Value ($1.0 Base)")
            st.plotly_chart(fig_eq, use_container_width=True)

        col_ic, col_rs = st.columns(2)
        with col_ic:
            if "ic_series" in series_data and series_data["ic_series"]:
                st.markdown("#### Daily Information Coefficient (IC) Series")
                df_ic = pd.DataFrame(series_data["ic_series"])
                fig_ic = px.line(df_ic, x="date", y=["ic", "rank_ic"], labels={"value": "IC Score", "date": "Date"}, color_discrete_map={"ic": "#2ca02c", "rank_ic": "#1f77b4"})
                fig_ic.add_hline(y=0.0, line_dash="solid", line_color="black")
                fig_ic.update_layout(height=300, margin=dict(l=20, r=20, t=20, b=20))
                st.plotly_chart(fig_ic, use_container_width=True)

        with col_rs:
            if "rolling_sharpe" in series_data and series_data["rolling_sharpe"]:
                st.markdown("#### 63-Day Rolling Annualized Sharpe Ratio")
                df_rs = pd.DataFrame(series_data["rolling_sharpe"])
                fig_rs = px.line(df_rs, x="date", y=["rolling_sharpe", "spy_rolling_sharpe"], labels={"value": "Sharpe", "date": "Date"}, color_discrete_map={"rolling_sharpe": "#1f77b4", "spy_rolling_sharpe": "#7f7f7f"})
                fig_rs.add_hline(y=1.0, line_dash="dot", line_color="#2ca02c")
                fig_rs.update_layout(height=300, margin=dict(l=20, r=20, t=20, b=20))
                st.plotly_chart(fig_rs, use_container_width=True)

        st.markdown("---")

        # Static High-Resolution PNG Tear Sheets from Evidence
        st.markdown("#### Institutional PNG Tear Sheet Artifacts")
        png_col1, png_col2 = st.columns(2)
        with png_col1:
            p1 = "evidence/phase5/cumulative_returns_vs_benchmark.png"
            if os.path.exists(p1):
                st.image(p1, caption="Cumulative Returns & Underwater Drawdown vs. SPY", use_container_width=True)
        with png_col2:
            p2 = "evidence/phase5/factor_ic_distribution.png"
            if os.path.exists(p2):
                st.image(p2, caption="Information Coefficient Distribution (Pearson & Rank IC)", use_container_width=True)

        # Novel Idea 2 Report Markdown
        novel_report_path = "evidence/phase5/novel_idea_attribution_report.md"
        if os.path.exists(novel_report_path):
            with st.expander("📄 View Novel Idea 2: Factor Attribution & Risk Parity Report", expanded=False):
                with open(novel_report_path, "r", encoding="utf-8") as rf:
                    st.markdown(rf.read())
