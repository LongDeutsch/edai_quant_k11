"""P1.2: ClickHouse sync — live in integration, SIMULATED in smoke, schema aligned, evidence lineage."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from quant_edai.common.dataset_paths import write_current_pointer
from quant_edai.serving import clickhouse_sync as ch

DS = "ds_ch_test"


def _daily(n: int) -> pd.DataFrame:
    dates = pd.date_range("2000-01-01", periods=n, freq="D").strftime("%Y-%m-%d")
    return pd.DataFrame(
        {
            "ticker_symbol": ["AAPL"] * n,
            "trade_date": dates,
            "open_price": 1.0, "high_price": 2.0, "low_price": 0.5, "close_price": 1.5,
            "volume": 100, "vwap": 1.2, "daily_return": 0.01,
            "junk_extra_column": "dropme",
        }
    )


@pytest.fixture
def lake(tmp_path):
    out = tmp_path / "lakehouse"
    root = out / "runs" / DS
    (root / "gold_market_bars_daily").mkdir(parents=True)
    _daily(6000).to_parquet(root / "gold_market_bars_daily" / "part.parquet", index=False)
    (root / "dim_companies").mkdir()
    pd.DataFrame({"ticker_symbol": ["AAPL"], "company_name": ["Apple"], "sector": ["Tech"]}).to_parquet(
        root / "dim_companies" / "dim_companies.parquet", index=False
    )
    (root / "gold_features").mkdir()
    pd.DataFrame(
        {
            "ticker_symbol": ["AAPL"] * 3,
            "event_timestamp": ["2024-01-02 16:00:00", "2024-01-03 16:00:00", "2024-01-04 16:00:00"],
            "close_price": 1.0, "sma_20": 1.0, "rsi_14d": 50.0,
        }
    ).to_parquet(root / "gold_features" / "feat_ticker_technical_indicators.parquet", index=False)
    (root / "gold_market_bars_1m").mkdir()
    pd.DataFrame(
        {
            "ticker_symbol": ["AAPL"] * 2,
            "event_timestamp": ["2024-01-02T14:30:00Z", "2024-01-02T14:31:00Z"],
            "open_price": 1.0, "high_price": 2.0, "low_price": 0.5, "close_price": 1.5, "volume": 10,
        }
    ).to_parquet(root / "gold_market_bars_1m" / "part.parquet", index=False)
    write_current_pointer(out, DS)
    return out


class FakeCH:
    """Records inserts; answers query-back from them."""

    inserted: dict = {}

    def __init__(self, host=None, port=None, user=None, password=None, **_):
        self.host, self.port = host or "fake-ch", port or 8123

    def is_online(self):
        return True

    def init_schema(self, *_a, **_k):
        return True

    def insert_dataframe(self, table, df, *_a, **_k):
        FakeCH.inserted.setdefault(table, []).append(df.copy())
        return len(df)

    def query_count(self, table, dataset_id=None, final=False):
        return sum(len(d) for d in FakeCH.inserted.get(table, []))


@pytest.mark.p1
def test_integration_fails_hard_when_clickhouse_down(integration_env, lake, tmp_path) -> None:
    out_file = tmp_path / "ev.json"
    with pytest.raises(RuntimeError, match="ClickHouse"):
        ch.sync_clickhouse_gold(
            source_dir=str(lake), host="127.0.0.1", port=1, out_file=str(out_file), mode="integration"
        )
    ev = json.loads(out_file.read_text())
    assert ev["status"] == "FAILED" and ev["dataset_id"] == DS and ev["execution_mode"] != "SIMULATED"


@pytest.mark.p1
def test_smoke_sim_is_labelled_simulated(smoke_env, lake, tmp_path) -> None:
    res = ch.sync_clickhouse_gold(source_dir=str(lake), engine="sim", out_file=str(tmp_path / "ev.json"), mode="smoke")
    assert res["status"] == "PASSED_SIMULATED" and res["execution_mode"] == "SIMULATED"
    assert res["engine"] == "simulation_fallback" and res["dataset_id"] == DS


@pytest.mark.p1
def test_integration_rejects_sim_engine(integration_env, lake, tmp_path) -> None:
    with pytest.raises(RuntimeError):
        ch.sync_clickhouse_gold(source_dir=str(lake), engine="sim", out_file=str(tmp_path / "ev.json"), mode="integration")


@pytest.mark.p1
def test_smoke_cap_is_explicit_only(smoke_env, lake, tmp_path) -> None:
    capped = ch.sync_clickhouse_gold(source_dir=str(lake), engine="sim", out_file=str(tmp_path / "a.json"), mode="smoke", smoke_cap=True)
    full = ch.sync_clickhouse_gold(source_dir=str(lake), engine="sim", out_file=str(tmp_path / "b.json"), mode="smoke")
    assert capped["synced_tables"]["market_daily_ohlcv"]["rows"] == 5000 and capped["smoke_cap"]["active"] is True
    assert full["synced_tables"]["market_daily_ohlcv"]["rows"] == 6000 and full["smoke_cap"]["active"] is False


@pytest.mark.p1
def test_live_sync_aligns_schema_and_records_queryback(integration_env, lake, tmp_path, monkeypatch) -> None:
    FakeCH.inserted = {}
    monkeypatch.setattr(ch, "ClickHouseSyncManager", FakeCH)
    # smoke_cap must never apply in integration, even if requested
    res = ch.sync_clickhouse_gold(
        source_dir=str(lake), host="fake-ch", out_file=str(tmp_path / "ev.json"), mode="integration", smoke_cap=True
    )
    assert res["status"] == "SUCCESS" and res["execution_mode"] == "LIVE" and res["engine"] == "clickhouse_live"
    assert res["dataset_id"] == DS and res["mode"] == "integration" and res["smoke_cap"]["active"] is False

    t = res["synced_tables"]["market_daily_ohlcv"]
    assert t["rows"] == 6000 and t["source_zone"] == "gold"
    assert t["queryback"]["dataset_rows_final"] == 6000

    inserted = pd.concat(FakeCH.inserted["market_daily_ohlcv"])
    assert "junk_extra_column" not in inserted.columns  # aligned to DDL
    assert (inserted["dataset_id"] == DS).all()
    assert json.loads((tmp_path / "ev.json").read_text())["dataset_id"] == DS


@pytest.mark.p1
def test_align_to_schema_drops_extras_and_derives_date() -> None:
    df = pd.DataFrame({"ticker_symbol": ["A"], "event_timestamp": ["2024-01-02T10:00:00Z"], "open_price": [1.0], "high_price": [1.0], "low_price": [1.0], "close_price": [1.0], "zzz": [1]})
    out, report = ch.align_to_schema(df, "market_daily_ohlcv", "d")
    assert "zzz" not in out.columns and out["dataset_id"].iloc[0] == "d"
    assert str(out["trade_date"].iloc[0])[:10] == "2024-01-02"


@pytest.mark.p1
def test_ddl_declares_dataset_id() -> None:
    ddl = (Path(__file__).resolve().parents[2] / "sql" / "clickhouse" / "schema_quant.sql").read_text()
    assert ddl.count("ADD COLUMN IF NOT EXISTS dataset_id") == 4
