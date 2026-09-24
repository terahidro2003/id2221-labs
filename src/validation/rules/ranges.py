"""numeric_range and timestamp_range row rules."""

from __future__ import annotations

from typing import Any, Optional

from pyspark.sql import Column, DataFrame, functions as F


def _range_violation(
    column: str,
    *,
    min_value: Any = None,
    max_value: Any = None,
    max_exclusive: bool = False,
    as_timestamp: bool = False,
) -> Column:
    """True when a non-null value falls outside [min, max] (or [min, max))."""
    col = F.col(column)
    dirty = F.lit(False)

    if min_value is not None:
        lower = F.to_timestamp(F.lit(min_value)) if as_timestamp else F.lit(min_value)
        dirty = dirty | (col.isNotNull() & (col < lower))

    if max_value is not None:
        upper = F.to_timestamp(F.lit(max_value)) if as_timestamp else F.lit(max_value)
        if max_exclusive:
            dirty = dirty | (col.isNotNull() & (col >= upper))
        else:
            dirty = dirty | (col.isNotNull() & (col > upper))

    return dirty


def numeric_range_reasons(
    df: DataFrame,
    column: Optional[str] = None,
    min: Any = None,
    max: Any = None,
    **_: Any,
) -> list[Column]:
    if not column or column not in df.columns:
        return []
    bad = _range_violation(column, min_value=min, max_value=max)
    return [F.when(bad, F.lit(f"range:{column}"))]


def timestamp_range_reasons(
    df: DataFrame,
    column: Optional[str] = None,
    min: Any = None,
    max: Any = None,
    **_: Any,
) -> list[Column]:
    """Out of range → reject. Matches notebook: max is exclusive (>= max rejects)."""
    if not column or column not in df.columns:
        return []
    bad = _range_violation(
        column,
        min_value=min,
        max_value=max,
        max_exclusive=True,
        as_timestamp=True,
    )
    return [F.when(bad, F.lit(f"ts_range:{column}"))]
