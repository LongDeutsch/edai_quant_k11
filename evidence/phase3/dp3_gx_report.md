# Great Expectations Data Docs: suite_features
> Evaluated at: `2026-11-12T07:14:57.713372+00:00` | Status: **PASSED**

- **Evaluated Records**: 3,488
- **Scorecard**: 7 / 7 expectations (100.0%)

## Detailed Results
| Expectation Type | Column | Parameters | Status | Details |
|---|---|---|---|---|
| `expect_column_to_exist` | `ticker_symbol` | {} | ✅ PASSED | {"observed_value": true} |
| `expect_column_values_to_not_be_null` | `event_timestamp` | {} | ✅ PASSED | {"unexpected_count": 0, "unexpected_percent": 0.0} |
| `expect_column_values_to_not_be_null` | `created` | {} | ✅ PASSED | {"unexpected_count": 0, "unexpected_percent": 0.0} |
| `expect_column_values_to_be_between` | `close_price` | {"min_value": 0.01, "max_value": 100000.0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 67.9422, "max_observed": 540.0663} |
| `expect_column_values_to_be_between` | `rsi_14d` | {"min_value": 0.0, "max_value": 100.0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 0.0, "max_observed": 98.15950920239622} |
| `expect_column_values_to_be_between` | `rolling_volatility_30d` | {"min_value": 0.0, "max_value": 10.0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 0.00026341647322303095, "max_observed": 0.25302783083526353} |
| `expect_column_values_to_be_between` | `sma_ratio_20_50` | {"min_value": 0.1, "max_value": 10.0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 0.9661310127178849, "max_observed": 1.0070818615918502} |
