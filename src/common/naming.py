
from __future__ import annotations

from pyspark.sql import Column, DataFrame, functions as F

LOCAL_TZ = "America/New_York"


def utc_to_local(col: Column | str) -> Column:
    return F.from_utc_timestamp(col, LOCAL_TZ)


def rename_columns(df: DataFrame, mapping: dict) -> DataFrame:
    out = df
    for src, dst in mapping.items():
        if src in out.columns and src != dst:
            out = out.withColumnRenamed(src, dst)
    return out
