# Great Expectations Data Docs: suite_raw_market_data
> Evaluated at: `2026-11-14T03:37:33.301701+00:00` | Status: **PASSED**

- **Evaluated Records**: 16,704
- **Scorecard**: 9 / 9 expectations (100.0%)

## Detailed Results
| Expectation Type | Column | Parameters | Status | Details |
|---|---|---|---|---|
| `expect_column_to_exist` | `ticker_symbol` | {} | ✅ PASSED | {"observed_value": true} |
| `expect_column_values_to_not_be_null` | `ticker_symbol` | {} | ✅ PASSED | {"unexpected_count": 0, "unexpected_percent": 0.0} |
| `expect_column_values_to_not_be_null` | `trade_date` | {} | ✅ PASSED | {"unexpected_count": 0, "unexpected_percent": 0.0} |
| `expect_column_values_to_be_between` | `open_price` | {"min_value": 0.01, "max_value": 100000.0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 33.37, "max_observed": 543.45} |
| `expect_column_values_to_be_between` | `high_price` | {"min_value": 0.01, "max_value": 100000.0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 33.46, "max_observed": 543.51} |
| `expect_column_values_to_be_between` | `low_price` | {"min_value": 0.01, "max_value": 100000.0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 33.35, "max_observed": 543.45} |
| `expect_column_values_to_be_between` | `close_price` | {"min_value": 0.01, "max_value": 100000.0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 33.4, "max_observed": 543.47} |
| `expect_column_values_to_be_between` | `volume` | {"min_value": 0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 10.0, "max_observed": 15909.0} |
| `expect_column_values_to_be_in_set` | `exchange` | {"value_set": ["NASDAQ", "NYSE", "BATS", "ARCA"]} | ✅ PASSED | {"unexpected_values": []} |
