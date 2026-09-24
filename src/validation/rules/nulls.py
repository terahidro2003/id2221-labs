"""not_null row rule."""

from __future__ import annotations

from typing import Any, Iterable

from pyspark.sql import Column, DataFrame, functions as F


def not_null_reasons(df: DataFrame, columns: Iterable[str] | None = None, **_: Any) -> list[Column]:
    reasons: list[Column] = []
    for col in columns or []:
        if col in df.columns:
            reasons.append(F.when(F.col(col).isNull(), F.lit(f"null:{col}")))
    return reasons
