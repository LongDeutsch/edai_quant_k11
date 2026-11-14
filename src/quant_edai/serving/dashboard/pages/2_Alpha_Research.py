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

from quant_edai.quant.metrics import format_sharpe
from quant_edai.serving.dashboard.dashboard_service import AlphaResearchService, MarketDataService


def _eval_sharpe(status, value, reason, trades) -> str:
    if status != "COMPLETED":
        return "N/A"
    trade_count = None if pd.isna(trades) else int(trades)
    return format_sharpe(None if pd.isna(value) else float(value), reason, trade_count=trade_count)


def _eval_percent(status, value) -> str:
    if status != "COMPLETED" or pd.isna(value):
        return "N/A"
    return f"{float(value) * 100.0:.1f}%"


def _eval_rate(status, value) -> str:
    if status != "COMPLETED" or pd.isna(value):
        return "N/A"
    return f"{float(value) * 100.0:.1f}%"

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
            s1.metric(
                "Sharpe Ratio",
                format_sharpe(m.get("sharpe_ratio"), m.get("sharpe_reason"), trade_count=m.get("trade_count")),
                delta="Rf = 4.0%",
            )
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
    st.markdown(
        "Each alpha is backtested on its own candles. "
        "In-Sample is `2022-11-01` to `2025-04-30`. "
        "Out-of-Sample is `2025-05-01` to `2026-04-30`. "
        "The trading split is not used here."
    )

    oos_df = AlphaResearchService.get_alpha_validation_comparison(dataset_id=selected_dataset)
    if not oos_df.empty:
        completed = oos_df[(oos_df["is_status"] == "COMPLETED") & (oos_df["oos_status"] == "COMPLETED")]
        comparable = oos_df["degradation_pct"].dropna()
        col_ov1, col_ov2, col_ov3 = st.columns(3)
        col_ov1.metric("Completed IS and OOS", int(len(completed)))
        approved_count = int((oos_df["lifecycle_status"] == "APPROVED_FROZEN").sum())
        col_ov2.metric("Approved Frozen Alphas", approved_count)
        if comparable.empty:
            col_ov3.metric("Average Factor Degradation", "N/A")
        else:
            col_ov3.metric("Average Factor Degradation", f"{float(comparable.mean()):.1f}%")
        st.caption(
            f"Average degradation uses {len(comparable)} of {len(oos_df)} registered alphas. "
            "A row is included only when both Sharpes are defined and the In-Sample Sharpe is above 0.10. "
            "Missing Out-of-Sample metrics stay N/A."
        )

        display = oos_df.copy()
        display["IS Sharpe"] = [
            _eval_sharpe(status, value, reason, trades)
            for status, value, reason, trades in zip(
                display["is_status"], display["is_sharpe"], display["is_sharpe_reason"], display["is_trades"]
            )
        ]
        display["OOS Sharpe"] = [
            _eval_sharpe(status, value, reason, trades)
            for status, value, reason, trades in zip(
                display["oos_status"], display["oos_sharpe"], display["oos_sharpe_reason"], display["oos_trades"]
            )
        ]
        display["IS MDD"] = [_eval_percent(status, value) for status, value in zip(display["is_status"], display["is_mdd"])]
        display["OOS MDD"] = [_eval_percent(status, value) for status, value in zip(display["oos_status"], display["oos_mdd"])]
        display["Degradation"] = display["degradation_pct"].map(lambda value: "N/A" if pd.isna(value) else f"{float(value):.1f}%")
        display["Sharpe change"] = display["sharpe_change"].map(lambda value: "N/A" if pd.isna(value) else f"{float(value):+.2f}")
        display["OOS win rate"] = [
            _eval_rate(status, value) for status, value in zip(display["oos_status"], display["oos_win_rate"])
        ]
        display["IS trades"] = display["is_trades"].map(lambda value: "N/A" if pd.isna(value) else int(value))
        display["OOS trades"] = display["oos_trades"].map(lambda value: "N/A" if pd.isna(value) else int(value))
        st.dataframe(
            display[[
                "alpha_id", "version", "horizon", "lifecycle_status", "timeframe",
                "is_status", "oos_status", "IS Sharpe", "OOS Sharpe", "Sharpe change", "Degradation",
                "IS MDD", "OOS MDD", "IS trades", "OOS trades", "OOS win rate",
                "is_run_id", "oos_run_id", "note",
            ]].rename(columns={
                "lifecycle_status": "lifecycle",
                "is_status": "IS eval",
                "oos_status": "OOS eval",
                "is_run_id": "IS run",
                "oos_run_id": "OOS run",
            }),
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("---")
        st.subheader("🔒 Admin Top-5 Approval & Freeze")
        from quant_edai.quant.alpha_split_evaluation import (
            POLICY_DOCUMENT,
            POLICY_NAME,
            is_admin_approver,
            rank_ensemble_candidates,
            weight_block_reason,
        )

        st.caption(f"Policy `{POLICY_NAME}`: {POLICY_DOCUMENT}")
        horizon_approve = st.radio("Target Serving Horizon", ["INTRADAY", "DAILY"], horizontal=True)
        ranked = rank_ensemble_candidates(oos_df, horizon_approve)
        eligible = ranked[ranked["eligible"] == True].sort_values("rank")
        blocked = ranked[ranked["eligible"] != True]
        st.caption(
            f"{len(eligible)} {horizon_approve} versions on `{selected_dataset}` have completed IS and OOS runs with a defined Sharpe."
        )
        if horizon_approve == "INTRADAY":
            st.caption("Daily alphas are excluded from this list and are not used to fill a short Intraday set.")

        candidate_view = ranked.copy()
        for column, status_column in (("IS Sharpe", "is_status"), ("OOS Sharpe", "oos_status")):
            source = "is_sharpe" if column.startswith("IS") else "oos_sharpe"
            reason = "is_sharpe_reason" if column.startswith("IS") else "oos_sharpe_reason"
            trades = "is_trades" if column.startswith("IS") else "oos_trades"
            candidate_view[column] = [
                _eval_sharpe(status, value, why, count)
                for status, value, why, count in zip(
                    candidate_view[status_column], candidate_view[source], candidate_view[reason], candidate_view[trades]
                )
            ]
        candidate_view["IS MDD"] = [
            _eval_percent(status, value) for status, value in zip(candidate_view["is_status"], candidate_view["is_mdd"])
        ]
        candidate_view["OOS MDD"] = [
            _eval_percent(status, value) for status, value in zip(candidate_view["oos_status"], candidate_view["oos_mdd"])
        ]
        candidate_view["IS trades"] = candidate_view["is_trades"].map(lambda value: "N/A" if pd.isna(value) else str(int(value)))
        candidate_view["OOS trades"] = candidate_view["oos_trades"].map(lambda value: "N/A" if pd.isna(value) else str(int(value)))
        candidate_view["Rank"] = candidate_view["rank"].map(lambda value: "" if pd.isna(value) else int(value))
        st.dataframe(
            candidate_view[[
                "Rank", "alpha_id", "version", "eligible", "exclusion_reason",
                "is_status", "oos_status", "IS Sharpe", "OOS Sharpe", "IS MDD", "OOS MDD",
                "IS trades", "OOS trades", "is_run_id", "oos_run_id",
            ]].rename(columns={
                "alpha_id": "Alpha",
                "version": "Version",
                "eligible": "Eligible",
                "exclusion_reason": "Why excluded",
                "is_status": "IS eval",
                "oos_status": "OOS eval",
                "is_run_id": "IS run",
                "oos_run_id": "OOS run",
            }),
            use_container_width=True,
            hide_index=True,
        )

        if len(eligible) < 5:
            st.error(
                f"Approval blocked: {len(eligible)} eligible {horizon_approve} versions. Exactly five are required."
            )
            for row in blocked.itertuples(index=False):
                st.write(f"`{row.alpha_id}@{row.version}`: {row.exclusion_reason}")

        option_labels = [f"{row.alpha_id}@{row.version}" for row in eligible.itertuples(index=False)]
        suggested = option_labels[:5]
        if suggested:
            st.caption("Suggested by the policy: " + ", ".join(suggested))
        admin_top5 = st.multiselect(
            f"Select 5 validated {horizon_approve} alpha versions",
            options=option_labels,
            default=suggested if len(eligible) >= 5 else [],
            max_selections=5,
            key=f"admin_approve_{horizon_approve}",
        )
        chosen_weights: list[float] = []
        if len(admin_top5) == 5:
            weight_columns = st.columns(5)
            for index, label in enumerate(admin_top5):
                chosen_weights.append(float(weight_columns[index].number_input(
                    f"Weight {label}",
                    min_value=0.0,
                    max_value=1.0,
                    value=0.20,
                    step=0.05,
                    format="%.2f",
                    key=f"weight_{horizon_approve}_{label}",
                )))
            entered_weight_reason = weight_block_reason(chosen_weights)
            if entered_weight_reason:
                st.error(entered_weight_reason)
        else:
            entered_weight_reason = "Select exactly five versions."
        admin_user = st.text_input("Approving Admin User ID", value="ADMIN_PM")

        selection_ready = (
            len(eligible) >= 5
            and len(admin_top5) == 5
            and entered_weight_reason is None
            and is_admin_approver(admin_user)
        )
        if st.button(
            f"🔒 Approve & Freeze Top-5 {horizon_approve} Ensemble",
            type="primary",
            disabled=not selection_ready,
        ):
            chosen_ids = [item.split("@", 1)[0] for item in admin_top5]
            chosen_versions = [item.split("@", 1)[1] for item in admin_top5]
            success = AlphaResearchService.save_top5_ensemble(
                alpha_ids=chosen_ids,
                horizon=horizon_approve,
                dataset_id=selected_dataset,
                approved_by=admin_user,
                freeze=True,
                versions=chosen_versions,
                weights=chosen_weights,
            )
            if success:
                st.success(f"Approved and froze the {horizon_approve} Top-5.")
                st.rerun()
            else:
                from quant_edai.quant.alpha_split_evaluation import approval_block_reason

                st.error(approval_block_reason(
                    chosen_ids, horizon_approve, admin_user, selected_dataset, chosen_versions,
                ) or "The ensemble was not saved.")
    else:
        st.info("No registered alphas were found.")


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
    from quant_edai.quant.alpha_split_evaluation import latest_frozen_ensemble

    active_intraday_ens = latest_frozen_ensemble(horizon="INTRADAY", dataset_id=selected_dataset)
    active_daily_ens = AlphaResearchService.get_latest_top5_ensemble(horizon="DAILY")
    col_e1, col_e2 = st.columns(2)
    with col_e1:
        st.markdown("**Active Intraday Ensemble (Page 3 Streaming)**")
        if active_intraday_ens:
            members = [
                f"{alpha_id}@{version} ({weight:.2f})"
                for alpha_id, version, weight in zip(
                    active_intraday_ens["selected_alpha_ids"],
                    active_intraday_ens["alpha_versions"],
                    active_intraday_ens["weights"],
                )
            ]
            st.success(
                f"Status: `APPROVED_FROZEN` version {active_intraday_ens['ensemble_version']}\n\n"
                f"Policy: `{active_intraday_ens['policy_name']}`\n\n"
                f"Approved by {active_intraday_ens['approved_by']} at {active_intraday_ens['approved_at']}\n\n"
                + "\n".join(members)
            )
        else:
            st.warning("No immutable Intraday approval for this dataset.")
    with col_e2:
        st.markdown("**Active Daily Ensemble (Daily Factor Allocation)**")
        if active_daily_ens:
            st.success(f"Status: `APPROVED_FROZEN`\n\nAlphas: {', '.join(active_daily_ens)}")
        else:
            st.warning("No daily ensemble configured.")
