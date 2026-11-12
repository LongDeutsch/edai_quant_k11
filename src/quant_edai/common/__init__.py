"""Shared contracts: dataset identity, path resolution, execution status."""

from quant_edai.common.dataset_paths import (
    CURRENT_POINTER_NAME,
    generate_dataset_id,
    read_current_dataset_id,
    resolve_dataset_root,
    write_current_pointer,
)
from quant_edai.common.execution_status import (
    ExecutionResult,
    assert_integration_ready,
    build_status,
    is_integration_mode,
    is_simulated_engine,
)

__all__ = [
    "CURRENT_POINTER_NAME",
    "ExecutionResult",
    "assert_integration_ready",
    "build_status",
    "generate_dataset_id",
    "is_integration_mode",
    "is_simulated_engine",
    "read_current_dataset_id",
    "resolve_dataset_root",
    "write_current_pointer",
]
