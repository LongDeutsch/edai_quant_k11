# Great Expectations Data Docs: suite_features
> Evaluated at: `2026-11-14T01:15:24.219544+00:00` | Status: **PASSED**

- **Evaluated Records**: 16,704
- **Scorecard**: 7 / 7 expectations (100.0%)

## Detailed Results
| Expectation Type | Column | Parameters | Status | Details |
|---|---|---|---|---|
| `expect_column_to_exist` | `ticker_symbol` | {} | ✅ PASSED | {"observed_value": true} |
| `expect_column_values_to_not_be_null` | `event_timestamp` | {} | ✅ PASSED | {"unexpected_count": 0, "unexpected_percent": 0.0} |
| `expect_column_values_to_not_be_null` | `created` | {} | ✅ PASSED | {"unexpected_count": 0, "unexpected_percent": 0.0} |
| `expect_column_values_to_be_between` | `close_price` | {"min_value": 0.01, "max_value": 100000.0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 33.4005, "max_observed": 543.4732} |
| `expect_column_values_to_be_between` | `rsi_14d` | {"min_value": 0.0, "max_value": 100.0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 0.0, "max_observed": 97.98545539850988} |
| `expect_column_values_to_be_between` | `rolling_volatility_30d` | {"min_value": 0.0, "max_value": 10.0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 0.0039426894421518205, "max_observed": 0.2} |
| `expect_column_values_to_be_between` | `sma_ratio_20_50` | {"min_value": 0.1, "max_value": 10.0} | ✅ PASSED | {"unexpected_count": 0, "min_observed": 0.9420319084169466, "max_observed": 1.0254633345160613} |
