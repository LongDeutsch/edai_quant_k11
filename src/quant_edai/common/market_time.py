"""Exchange-time helpers: one rule for bar timestamps → America/New_York.

Policy (``BAR_TIMESTAMP_TZ``):
    utc  Naive and ``Z`` timestamps are true UTC (default for MODE=integration|full).
    et   Naive and ``Z`` timestamps are exchange wall-clock labelled ``Z``
         (legacy smoke/unit fixtures such as ``2024-03-15T15:55:00Z``).
Timestamps with an explicit numeric offset (``-04:00``) are always honoured.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from quant_edai.common.execution_status import is_integration_mode

MARKET_TZ = ZoneInfo("America/New_York")
EOD_FLAT_HOUR = 15
EOD_FLAT_MINUTE = 55
MARKET_OPEN = (9, 30)


def bar_timestamp_policy(mode: Optional[str] = None) -> str:
    explicit = os.environ.get("BAR_TIMESTAMP_TZ", "").strip().lower()
    if explicit in {"utc", "et"}:
        return explicit
    return "utc" if is_integration_mode(mode) else "et"


def parse_bar_timestamp(ts: str, policy: Optional[str] = None) -> Optional[datetime]:
    """Parse a bar timestamp into a tz-aware America/New_York datetime (None if unparseable)."""
    policy = policy or bar_timestamp_policy()
    text = str(ts).strip()
    is_z = text.endswith(("Z", "z"))
    if is_z:
        text = text[:-1]
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None

    if dt.tzinfo is not None:  # explicit numeric offset
        return dt.astimezone(MARKET_TZ)
    if policy == "utc":
        return dt.replace(tzinfo=timezone.utc).astimezone(MARKET_TZ)
    return dt.replace(tzinfo=MARKET_TZ)


def to_utc_iso(dt_et: datetime) -> str:
    return dt_et.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def is_eod_flat_time(dt_et: datetime) -> bool:
    """15:55 ET onward (including after the close) → flatten."""
    return (dt_et.hour, dt_et.minute) >= (EOD_FLAT_HOUR, EOD_FLAT_MINUTE)


def is_market_open_window(dt_et: datetime) -> bool:
    return dt_et.hour == MARKET_OPEN[0] and MARKET_OPEN[1] <= dt_et.minute <= MARKET_OPEN[1] + 5
