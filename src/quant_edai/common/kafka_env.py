"""Single resolver for Kafka bootstrap env vars (KAFKA_BOOTSTRAP / KAFKA_BOOTSTRAP_SERVERS)."""

from __future__ import annotations

import os
from typing import Optional

DEFAULT_KAFKA_BOOTSTRAP = "localhost:9092"


def resolve_kafka_bootstrap(explicit: Optional[str] = None, default: str = DEFAULT_KAFKA_BOOTSTRAP) -> str:
    """explicit > KAFKA_BOOTSTRAP > KAFKA_BOOTSTRAP_SERVERS > default (read at call time)."""
    return (
        explicit
        or os.environ.get("KAFKA_BOOTSTRAP")
        or os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
        or default
    )
