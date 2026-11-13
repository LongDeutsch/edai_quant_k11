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
# -----------------------------------------------------------------------------
# Tabs Layout
# -----------------------------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "🏛️ Alpha Registry",
    "⚡ In-Sample Factor Backtesting",
    "🛡️ Out-of-Sample Admin Validation",
    "🏆 Leaderboard & Top-5 Ensemble"
])

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
# TAB 2: In-Sample Factor Backtesting
# =============================================================================
with tab2:
    st.subheader("In-Sample Factor Simulation Engine (IS Split: 2022-11 to 2025-04)")
    st.info("💡 **In-Sample Research Boundary**: Research strictly evaluates against In-Sample (IS) split data (`2022-11-01` to `2025-04-30`). Out-of-Sample (OOS) data is reserved for Admin Validation in Tab 3. TRADING replay data is strictly isolated and inaccessible here.")

    b1, b2, b3, b4 = st.columns(4)
    with b1:
        alpha_options = alphas_df["alpha_id"].tolist() if not alphas_df.empty else ["alpha_sma_cross_daily", "alpha_mom_z_daily", "alpha_vwap_z_daily"]
        selected_alpha = st.selectbox("Select Strategy / Alpha ID", alpha_options, index=0)
    with b2:
        selected_ticker = st.selectbox("Backtest Ticker Universe", ["ALL", "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "SPY"], index=0)
    with b3:
        slippage_input = st.number_input("Slippage (basis points)", min_value=0.0, max_value=50.0, value=5.0, step=0.5)
    with b4:
        comm_input = st.number_input("Commission (basis points)", min_value=0.0, max_value=20.0, value=1.0, step=0.5)

    run_btn = st.button("🚀 Run In-Sample Backtest", type="primary")

    if run_btn:
        with st.spinner(f"Simulating In-Sample walk-forward backtest for `{selected_alpha}` on `{selected_ticker}`..."):
            result = AlphaResearchService.run_historical_backtest(
                alpha_id=selected_alpha,
                ticker=selected_ticker,
                timeframe="DAILY",
                slippage_bps=slippage_input,
                commission_bps=comm_input,
                dataset_id=selected_dataset,
                split_id="IS",
            )

        if result["status"] == "SUCCESS":
            m = result["metrics"]
            st.success(f"✅ In-Sample Backtest completed successfully! Evaluated from `{m['start_date']}` to `{m['end_date']}` ({m['calendar_days']} calendar days). Run ID: `{m['run_id']}` (Persisted to PostgreSQL).")

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
                    title=f"Cumulative In-Sample Equity Curve vs Benchmark: {selected_alpha} ({selected_ticker})",
                    template="plotly_dark",
                    height=450,
                    margin=dict(l=40, r=40, t=50, b=40),
                )
                st.plotly_chart(fig_eq, use_container_width=True)

        elif result["status"] == "INELIGIBLE":
            st.error(f"🚫 **Eligibility Block**: {result.get('message')}")

        elif result["status"] == "NO_TRADES":
            st.warning(f"⚠️ **No Trades Executed**: {result.get('message')}")
            st.info("💡 **Reason**: The mathematical signal did not cross the entry threshold during the evaluation window. To generate trades, try lowering the entry threshold or selecting another strategy.")

        else:
            st.error(f"❌ Backtest failed: {result.get('message')}")


# =============================================================================
# TAB 3: Out-of-Sample Admin Validation
# =============================================================================
with tab3:
    st.subheader("🛡️ Independent Out-of-Sample (OOS) Validation & Quality Gate")
    st.markdown("Strictly evaluated on held-out **OOS Split** (`2025-05-01` to `2026-04-30`). Admin reviews degradation across held-out market conditions before approving and freezing Top-5 ensembles.")

    oos_df = AlphaResearchService.get_alpha_validation_comparison()
    if not oos_df.empty:
        col_ov1, col_ov2, col_ov3 = st.columns(3)
        col_ov1.metric("Validated Alphas Count", len(oos_df))
        approved_count = len(oos_df[oos_df["status"] == "APPROVED_FROZEN"])
        col_ov2.metric("Approved Frozen Alphas", approved_count)
        avg_deg = oos_df["degradation_pct"].mean()
        col_ov3.metric("Average Factor Degradation", f"{avg_deg:.1f}%")

        st.dataframe(
            oos_df[["alpha_id", "horizon", "status", "is_sharpe", "oos_sharpe", "is_mdd", "oos_mdd", "degradation_pct", "oos_win_rate"]],
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("---")
        st.subheader("🔒 Admin Top-5 Approval & Freeze")
        st.markdown("PM / Admin approves the Top-5 Ensemble based on OOS validation. This sets `status = APPROVED_FROZEN` in PostgreSQL and updates `alpha_ensemble_config` with cryptographic provenance.")

        horizon_approve = st.radio("Target Serving Horizon", ["INTRADAY", "DAILY"], horizontal=True)
        sub_oos = oos_df[oos_df["horizon"] == horizon_approve]
        top_candidates = sub_oos.sort_values(by="oos_sharpe", ascending=False)["alpha_id"].tolist()
        default_top5 = top_candidates[:5] if len(top_candidates) >= 5 else top_candidates

        admin_top5 = st.multiselect(
            f"Select 5 Validated {horizon_approve} Alphas for Approval",
            options=top_candidates,
            default=default_top5,
            max_selections=5,
            key=f"admin_approve_{horizon_approve}"
        )

        admin_user = st.text_input("Approving Admin User ID", value="ADMIN_PM")

        if st.button(f"🔒 Approve & Freeze Top-5 {horizon_approve} Ensemble", type="primary", disabled=len(admin_top5) != 5):
            success = AlphaResearchService.save_top5_ensemble(
                alpha_ids=admin_top5,
                horizon=horizon_approve,
                dataset_id=selected_dataset,
                approved_by=admin_user,
                freeze=True,
            )
            if success:
                st.success(f"✅ Successfully approved and frozen Top-5 {horizon_approve} Ensemble ({', '.join(admin_top5)})! Status: `APPROVED_FROZEN`. Live trading will use this verified ensemble.")
                st.rerun()
            else:
                st.error("Failed to approve ensemble. Check database connection.")
    else:
        st.info("No validation comparison available. Please refresh alpha metrics with splits.")


# =============================================================================
# TAB 4: Leaderboard & Top-5 Ensemble Selection
# =============================================================================
with tab4:
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
    st.subheader("📋 Active Production Ensemble Handoff")
    active_intraday_ens = AlphaResearchService.get_latest_top5_ensemble(horizon="INTRADAY")
    active_daily_ens = AlphaResearchService.get_latest_top5_ensemble(horizon="DAILY")
    col_e1, col_e2 = st.columns(2)
    with col_e1:
        st.markdown("**Active Intraday Ensemble (Page 3 Streaming)**")
        if active_intraday_ens:
            st.success(f"Status: `APPROVED_FROZEN`\n\nAlphas: {', '.join(active_intraday_ens)}")
        else:
            st.warning("No intraday ensemble configured.")
    with col_e2:
        st.markdown("**Active Daily Ensemble (Daily Factor Allocation)**")
        if active_daily_ens:
            st.success(f"Status: `APPROVED_FROZEN`\n\nAlphas: {', '.join(active_daily_ens)}")
        else:
            st.warning("No daily ensemble configured.")
