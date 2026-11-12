# Great Expectations Data Docs: suite_gold_market_data
> Evaluated at: `2026-11-12T07:14:52.339382+00:00` | Status: **PASSED**

- **Evaluated Records**: 3,488
- **Scorecard**: 6 / 6 expectations (100.0%)

## Detailed Results
| Expectation Type | Column | Parameters | Status | Details |
|---|---|---|---|---|
| `expect_column_to_exist` | `ticker_symbol` | {} | ✅ PASSED | {"observed_value": true} |
| `expect_column_values_to_not_be_null` | `ticker_symbol` | {} | ✅ PASSED | {"unexpected_count": 0, "unexpected_percent": 0.0} |
| `expect_column_values_to_not_be_null` | `trade_date` | {} | ✅ PASSED | {"unexpected_count": 0, "unexpected_percent": 0.0} |
| `expect_column_values_to_be_between` | `close_price` | {"min_value": 0.01, "max_value": 100000.0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 67.9422, "max_observed": 540.0663} |
| `expect_column_values_to_be_between` | `volume` | {"min_value": 0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 10.0, "max_observed": 8184.0} |
| `expect_column_values_to_be_in_set` | `exchange` | {"value_set": ["NASDAQ", "NYSE", "BATS", "ARCA"]} | ✅ PASSED | {"unexpected_values": []} |
