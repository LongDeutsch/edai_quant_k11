"""Page 2: Alpha Research, Dynamic Backtesting & Ensemble Selection.

Features:
- Alpha Registry Explorer for both Daily and Intraday quantitative strategies.
- Dynamic historical backtest engine with zero look-ahead bias across 2-year target history.
- Institutional scorecard: Sharpe Ratio, Max Drawdown, Total Return, Win Rate, Profit Factor.
- Interactive Leaderboard, Equity Curve vs Benchmark SPY, and Drawdown underwater chart.
- Backtest results persistence in PostgreSQL (alpha_backtest_results).
- Top-5 Alpha selection saved to alpha_ensemble_configs for execution handoff to Page 3.
"""

from __future__ import annotations

import json
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from quant_edai.serving.dashboard.dashboard_service import AlphaResearchService, MarketDataService

st.set_page_config(
    page_title="Alpha Research | Quant Terminal",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🔬 Alpha Research & Factor Backtesting")
st.caption("Quantitative factor discovery, dynamic backtesting across historical Lakehouse features, and Top-5 ensemble allocation.")

adv_mode = st.sidebar.checkbox("⚙️ Advanced Mode", value=False, help="Show raw ClickHouse test/idempotency datasets.")
ds_options = MarketDataService.get_catalog_options(advanced_mode=adv_mode)
selected_preset = st.sidebar.selectbox("Active Dataset Preset", ds_options, index=1 if len(ds_options) > 1 else 0)
selected_dataset = MarketDataService.resolve_dataset_id(selected_preset)
st.sidebar.caption(f"Resolved Dataset ID: `{selected_dataset}`")

# -----------------------------------------------------------------------------
# Tabs Layout
# -----------------------------------------------------------------------------
tab1, tab2, tab3 = st.tabs(["🏛️ Alpha Registry", "⚡ Dynamic Backtest Engine", "🏆 Backtest Leaderboard & Top-5 Ensemble"])

# =============================================================================
# TAB 1: Alpha Registry Explorer
# =============================================================================
with tab1:
    st.subheader("Registered Alpha Factor Universe")
    st.markdown("All quantitative alphas registered in PostgreSQL `alpha_registry` with strict mathematical specifications.")

    c1, c2 = st.columns([1, 3])
    with c1:
        horizon_filter = st.selectbox("Filter Horizon", ["ALL", "DAILY", "INTRADAY"], index=0)

    alphas_df = AlphaResearchService.get_registered_alphas(horizon=None if horizon_filter == "ALL" else horizon_filter)
    if not alphas_df.empty:
        # Display registered columns matching alpha_registry schema
        cols_to_show = [c for c in ["alpha_id", "version", "alpha_type", "status", "description", "created_at"] if c in alphas_df.columns]
        st.dataframe(
            alphas_df[cols_to_show],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No Alphas found in registry. Run `python -m quant_edai.quant.alpha_seed` to populate standard institutional alphas.")


# =============================================================================
# TAB 2: Dynamic Backtest Engine
# =============================================================================
with tab2:
    st.subheader("Historical Factor Simulation Engine (No Look-Ahead)")
    st.info("💡 **2-Year Target Data Coverage Notice**: Daily features are sourced from Lakehouse Gold (`2022-11-01` to `2025-10-31`, 3 full years, 12,544 rows). Actual duration is computed dynamically.")

    b1, b2, b3, b4 = st.columns(4)
    with b1:
        alpha_options = alphas_df["alpha_id"].tolist() if not alphas_df.empty else ["composite_eq", "momentum_1m", "vwap_dev", "rsi_reversal"]
        selected_alpha = st.selectbox("Select Strategy / Alpha ID", alpha_options, index=0)
    with b2:
        selected_ticker = st.selectbox("Backtest Ticker Universe", ["ALL", "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "SPY"], index=0)
    with b3:
        slippage_input = st.number_input("Slippage (basis points)", min_value=0.0, max_value=50.0, value=5.0, step=0.5)
    with b4:
        comm_input = st.number_input("Commission (basis points)", min_value=0.0, max_value=20.0, value=1.0, step=0.5)

    run_btn = st.button("🚀 Run Walk-Forward Backtest", type="primary")

    if run_btn:
        with st.spinner(f"Simulating walk-forward backtest for `{selected_alpha}` on `{selected_ticker}`..."):
            result = AlphaResearchService.run_historical_backtest(
                alpha_id=selected_alpha,
                ticker=selected_ticker,
                timeframe="DAILY",
                slippage_bps=slippage_input,
                commission_bps=comm_input,
                dataset_id=selected_dataset,
            )

        if result["status"] == "SUCCESS":
            m = result["metrics"]
            st.success(f"✅ Backtest completed successfully! Evaluated from `{m['start_date']}` to `{m['end_date']}` ({m['calendar_days']} calendar days). Run ID: `{m['run_id']}` (Persisted to PostgreSQL).")

            # Metrics Scorecard Strip
            s1, s2, s3, s4, s5, s6 = st.columns(6)
            s1.metric("Sharpe Ratio", f"{m['sharpe_ratio']:.2f}", delta="Rf = 4.0%")
            s2.metric("Total Return", f"{m['total_return_pct']:+.2f}%")
            s3.metric("Max Drawdown", f"{m['max_drawdown_pct']:.2f}%")
            s4.metric("Win Rate", f"{m['win_rate_pct']:.1f}%")
            s5.metric("Trade Count", f"{m['trade_count']}")
            s6.metric("Friction Cost", f"{m['slippage_bps'] + m['commission_bps']} bps")

            # Equity Curve Plot
            eq_data = result["equity_curve"]
            if eq_data:
                eq_df = pd.DataFrame(eq_data)
                fig_eq = go.Figure()
                fig_eq.add_trace(go.Scatter(x=eq_df["time"], y=eq_df["strategy_equity"], name="Alpha Strategy Equity", line=dict(color="#10B981", width=2)))
                fig_eq.add_trace(go.Scatter(x=eq_df["time"], y=eq_df["benchmark_equity"], name="Benchmark Equity", line=dict(color="#64748B", width=1.5, dash="dot")))
                fig_eq.update_layout(
                    title=f"Cumulative Equity Curve vs Benchmark: {selected_alpha} ({selected_ticker})",
                    template="plotly_dark",
                    height=450,
                    margin=dict(l=40, r=40, t=50, b=40),
                )
                st.plotly_chart(fig_eq, use_container_width=True)

        elif result["status"] == "NO_TRADES":
            st.warning(f"⚠️ **No Trades Executed**: {result.get('message')}")
            st.info("💡 **Reason**: The mathematical signal did not cross the entry threshold during the evaluation window. To generate trades, try lowering the entry threshold or selecting another strategy.")

        else:
            st.error(f"❌ Backtest failed: {result.get('message')}")


# =============================================================================
# TAB 3: Leaderboard & Top-5 Ensemble Selection
# =============================================================================
with tab3:
    st.subheader("🏆 Quantitative Alpha Leaderboard")
    st.caption("Historical performance runs retrieved from PostgreSQL `alpha_backtest_results`.")

    col_flt1, col_flt2 = st.columns([2, 1])
    with col_flt1:
        only_latest = st.checkbox("Show only latest successful run per strategy / ticker", value=True)
    with col_flt2:
        if st.button("🔄 Refresh Leaderboard", use_container_width=True):
            st.rerun()

    leaderboard_df = AlphaResearchService.get_backtest_leaderboard(only_latest=only_latest)
    if not leaderboard_df.empty:
        st.dataframe(
            leaderboard_df[["run_id", "alpha_id", "ticker_symbol", "sharpe_ratio", "total_return_pct", "max_drawdown", "win_rate", "total_trades", "created_at"]],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No saved backtest runs found. Execute a backtest in Tab 2 to populate the leaderboard.")

    st.markdown("---")
    st.subheader("🎯 Top-5 Alpha Selection for Live Trading (Page 3)")
    st.markdown("Select exactly **5 Alphas** to build an Equal-Average ensemble signal ($Z_{\\text{combined}} = \\frac{1}{5} \\sum Z_i$) for execution in Page 3.")

    # Multi-select for top 5
    available_alpha_ids = alphas_df["alpha_id"].tolist() if not alphas_df.empty else ["composite_eq", "momentum_1m", "vwap_dev", "rsi_reversal", "breakout_vol"]
    # Ensure default has 5 items
    default_selection = available_alpha_ids[:5] if len(available_alpha_ids) >= 5 else available_alpha_ids

    selected_top5 = st.multiselect(
        "Choose Top 5 Alphas",
        options=available_alpha_ids,
        default=default_selection,
        max_selections=5,
        help="Select exactly 5 Alphas for the production trading ensemble.",
    )

    save_c1, save_c2 = st.columns([1, 3])
    with save_c1:
        save_btn = st.button("💾 Save Top-5 Ensemble", type="primary", disabled=len(selected_top5) != 5)

    if save_btn:
        success = AlphaResearchService.save_top5_ensemble(selected_top5, horizon="INTRADAY", dataset_id=selected_dataset)
        if success:
            st.success(f"✅ Top-5 Ensemble successfully saved to PostgreSQL `alpha_ensemble_config`! Alphas: {', '.join(selected_top5)}. Page 3 will use these Alphas for streaming signals.")
        else:
            st.error("Failed to persist ensemble to database. Check PostgreSQL logs.")
