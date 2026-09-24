"""Rule id → callable registry."""

from __future__ import annotations

from typing import Callable

from src.validation.rules.nulls import not_null_reasons
from src.validation.rules.ranges import numeric_range_reasons, timestamp_range_reasons
from src.validation.rules.schema import check_schema
from src.validation.rules.timestamps import timestamp_order_reasons
from src.validation.rules.uniqueness import apply_primary_key

RULES: dict[str, Callable] = {
    "schema": check_schema,
    "not_null": not_null_reasons,
    "numeric_range": numeric_range_reasons,
    "timestamp_range": timestamp_range_reasons,
    "timestamp_order": timestamp_order_reasons,
    "primary_key": apply_primary_key,
}
