"""primary_key / uniqueness: keep first row per key, duplicates become rejects."""

from __future__ import annotations

from typing import Any, Iterable

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


def apply_primary_key(
    good: DataFrame,
    rejects: DataFrame,
    columns: Iterable[str] | None = None,
    **_: Any,
) -> tuple[DataFrame, DataFrame]:
    """First-row-wins dedupe; duplicate PK rows get _reject_reason=duplicate_pk."""
    pk = [c for c in (columns or []) if c in good.columns]
    if not pk:
        return good, rejects

    ranked = good.withColumn(
        "_rn",
        # No preferred sort key: keep an arbitrary first row per PK.
        F.row_number().over(Window.partitionBy(*pk).orderBy(F.lit(1))),
    )
    dup_rejects = (
        ranked.filter(F.col("_rn") > 1)
        .drop("_rn")
        .withColumn("_reject_reason", F.lit("duplicate_pk"))
    )
    good = ranked.filter(F.col("_rn") == 1).drop("_rn")
    rejects = rejects.unionByName(dup_rejects, allowMissingColumns=True)
    return good, rejects
