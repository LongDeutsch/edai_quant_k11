# Great Expectations Data Docs: suite_features
> Evaluated at: `2026-11-08T16:28:41.718577+00:00` | Status: **PASSED**

- **Evaluated Records**: 42,684
- **Scorecard**: 7 / 7 expectations (100.0%)

## Detailed Results
| Expectation Type | Column | Parameters | Status | Details |
|---|---|---|---|---|
| `expect_column_to_exist` | `ticker_symbol` | {} | ✅ PASSED | {"observed_value": true} |
| `expect_column_values_to_not_be_null` | `event_timestamp` | {} | ✅ PASSED | {"unexpected_count": 0, "unexpected_percent": 0.0} |
| `expect_column_values_to_not_be_null` | `created` | {} | ✅ PASSED | {"unexpected_count": 0, "unexpected_percent": 0.0} |
| `expect_column_values_to_be_between` | `close_price` | {"min_value": 0.01, "max_value": 100000.0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 44.41, "max_observed": 625.35} |
| `expect_column_values_to_be_between` | `rsi_14d` | {"min_value": 0.0, "max_value": 100.0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 0.0, "max_observed": 99.70149253731286} |
| `expect_column_values_to_be_between` | `rolling_volatility_30d` | {"min_value": 0.0, "max_value": 10.0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 0.0, "max_observed": 0.42097807375525864} |
| `expect_column_values_to_be_between` | `sma_ratio_20_50` | {"min_value": 0.1, "max_value": 10.0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 0.9088737527470278, "max_observed": 1.0767332589662983} |
