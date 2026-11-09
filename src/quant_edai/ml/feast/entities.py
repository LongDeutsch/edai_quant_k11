"""Feast Entity Definitions for US Equities Market Intelligence."""

from feast import Entity

# Primary Entity: Ticker Symbol (e.g. AAPL, MSFT, NVDA)
ticker_symbol = Entity(
    name="ticker_symbol",
    join_keys=["ticker_symbol"],
    description="Stock ticker symbol for US Equities (e.g. AAPL, NVDA, MSFT)",
)
