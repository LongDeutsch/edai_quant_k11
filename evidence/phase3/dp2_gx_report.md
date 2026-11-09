# Great Expectations Data Docs: suite_gold_market_data
> Evaluated at: `2026-11-09T00:50:58.425740+00:00` | Status: **PASSED**

- **Evaluated Records**: 38,368
- **Scorecard**: 6 / 6 expectations (100.0%)

## Detailed Results
| Expectation Type | Column | Parameters | Status | Details |
|---|---|---|---|---|
| `expect_column_to_exist` | `ticker_symbol` | {} | ✅ PASSED | {"observed_value": true} |
| `expect_column_values_to_not_be_null` | `ticker_symbol` | {} | ✅ PASSED | {"unexpected_count": 0, "unexpected_percent": 0.0} |
| `expect_column_values_to_not_be_null` | `trade_date` | {} | ✅ PASSED | {"unexpected_count": 0, "unexpected_percent": 0.0} |
| `expect_column_values_to_be_between` | `close_price` | {"min_value": 0.01, "max_value": 100000.0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 44.41, "max_observed": 625.35} |
| `expect_column_values_to_be_between` | `volume` | {"min_value": 0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 307077.0, "max_observed": 43155693.0} |
| `expect_column_values_to_be_in_set` | `exchange` | {"value_set": ["NASDAQ", "NYSE", "BATS", "ARCA"]} | ✅ PASSED | {"unexpected_values": []} |
