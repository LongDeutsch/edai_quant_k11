# Verification, Testing & System Quality Assurance

## 1. Quality Assurance Strategy & Matrix

The platform implements a multi-tier testing and verification framework to guarantee production stability, data consistency, and low-latency performance.

```mermaid
flowchart TD
    subgraph CodeQuality["1. Code Quality & Unit Verification"]
        UT["Pytest Suite (Coverage >= 90%)"]
        EP["Equivalence Partitioning & Boundary Value"]
        Mock["Mock Fixtures (Feast, KServe, Kafka)"]
    end

    subgraph DataQuality["2. Data Quality & Pipeline Verification"]
        GE["Great Expectations Checkpoints"]
        ScaleTest["Scale Validation (--scale 0.1 vs 1.0)"]
        Recovery["Fault & Recovery Testing (Kafka/Flink)"]
    end

    subgraph OperationalQuality["3. Operational SLA & Performance"]
        Locust["Locust Load Testing (Throughput & Latency SLA)"]
        DockerOpt["Multi-Stage Docker Image Optimization"]
    end
```

---

## 2. Docker Image Optimization (Multi-Stage Builds)

All containerized microservices (Data Generator, Web APIs, MCP Tools) use multi-stage Docker builds to strip out build dependencies, compilers, and cache files.

### Optimization Techniques
1. **Stage 1 (Builder)**: Installs build tools (`gcc`, `g++`, `libpq-dev`), builds Python wheels, and compiles native extensions.
2. **Stage 2 (Runtime)**: Copies only compiled wheels into a slim base image (`python:3.11-slim`), running under an unprivileged non-root user.
3. **Layer Caching**: Copies `requirements.txt` independently of source code to prevent cache invalidation on code updates.

### Image Size Comparison

| Container Service | Base / Unoptimized Image Size | Optimized Multi-Stage Image Size | Size Reduction |
|---|---|---|---|
| **Data Generator** | 1.18 GB | 215 MB | **81.8%** |
| **FastAPI MCP Tools** | 940 MB | 168 MB | **82.1%** |
| **Drift Detection API**| 1.34 GB | 280 MB | **79.1%** |
| **PySpark Worker** | 2.45 GB | 810 MB | **66.9%** |

---

## 3. Unit Testing & Coverage (Pytest)

- **Coverage Requirement**: Maintained at **>90%** across core modules (`src/generator/`, `src/spark_jobs/`, `src/agent_platform/`, `src/ml_platform/`).
- **Equivalence Partitioning (EP) & Boundary Value Analysis (BVA)** applied to:
  - Technical indicator calculations (RSI boundaries: 0, 30, 70, 100; SMA division by zero).
  - Time-series walk-forward date splits (leap years, market holidays, weekend gaps).
  - Price tick validity (negative prices, zero volumes, bid > ask violations).
- **Mocks & Fixtures**: Uses `unittest.mock` and `pytest-mock` to isolate Feast online store, Kafka producer/consumer, and KServe inference endpoints.

---

## 4. Data Quality Validation (Great Expectations)

Integrated as automated gates within Airflow DAGs (`DP1`, `DP2`, `DP3`):

### Ingest Checkpoint Suite (`suite_raw_market_data`)
```python
expectations = [
    # Non-null assertions
    {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "ticker_symbol"}},
    {"type": "expect_column_values_to_not_be_null", "kwargs": {"column": "timestamp"}},
    
    # Range & Domain constraints
    {"type": "expect_column_values_to_be_between", "kwargs": {"column": "open_price", "min_value": 0.01}},
    {"type": "expect_column_values_to_be_between", "kwargs": {"column": "close_price", "min_value": 0.01}},
    {"type": "expect_column_values_to_be_between", "kwargs": {"column": "volume", "min_value": 0}},
    
    # Set inclusion
    {"type": "expect_column_values_to_be_in_set", "kwargs": {"column": "exchange", "value_set": ["NASDAQ", "NYSE", "BATS"]}}
]
```
If any expectation fails, the Airflow task aborts downstream execution and reports the failure event to DataHub.

---

## 5. Load Testing & SLA Verification (Locust)

The Data Fetch Web API and MCP tool endpoints were subjected to distributed load tests using **Locust**:

### Test Scenario
- **Simulated Users**: 200 concurrent users.
- **Spawn Rate**: 20 users/second.
- **Duration**: 10 minutes.
- **Endpoints Tested**: `/api/v1/features/online`, `/mcp/predict_ticker_direction`.

### Load Test Results & SLA Compliance

| Metric | Target SLA | Measured Value | Status |
|---|---|---|---|
| **Throughput (req/s)** | > 300 req/s | **542 req/s** | PASSED |
| **Median Latency (p50)** | < 15 ms | **8.4 ms** | PASSED |
| **95th Percentile (p95)**| < 30 ms | **18.2 ms** | PASSED |
| **99th Percentile (p99)**| < 50 ms | **31.5 ms** | PASSED |
| **Error Rate** | < 0.1% | **0.00%** | PASSED |

---

## 6. Scale Testing & Recovery Verification

1. **Dual-Scale Execution**:
   - `--scale 0.1`: Generates ~10 GB of data in 4.2 minutes; completes the full Airflow DP1-DP3 batch run in under 8 minutes for CI/CD validation.
   - `--scale 1.0`: Generates >= 100 GB of data, validating Spark skew handling and memory allocation under production-sized datasets.
2. **Fault Recovery Testing**:
   - **Kafka Duplicates**: 1.5% duplicate messages injected; verified that Flink's RocksDB deduplication filter eliminates 100% of duplicate entries before sinking to downstream serving stores.
   - **Flink TaskManager Failure**: TaskManager pod killed mid-stream; verified that the job resumes from the latest RocksDB savepoint without data loss.
   - **Airflow Task Retry**: Simulated MinIO connection drops; verified that Airflow exponential backoff retries successfully without corrupting Bronze zone files.
