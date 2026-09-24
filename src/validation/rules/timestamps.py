"""timestamp_order row rule (before must be <= after)."""

from __future__ import annotations

from typing import Any, Optional

from pyspark.sql import Column, DataFrame, functions as F


def timestamp_order_reasons(
    df: DataFrame,
    before: Optional[str] = None,
    after: Optional[str] = None,
    **_: Any,
) -> list[Column]:
    if not before or not after:
        return []
    if before not in df.columns or after not in df.columns:
        return []
    bad = (
        F.col(before).isNotNull()
        & F.col(after).isNotNull()
        & (F.col(before) > F.col(after))
    )
    return [F.when(bad, F.lit(f"ts_order:{before}>{after}"))]
