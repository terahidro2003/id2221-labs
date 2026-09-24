"""Ingestion / data-product metadata helpers (ported from data_products.ipynb)."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from pyspark.sql import DataFrame, SparkSession, functions as F


def get_last_commit_timestamp(spark: SparkSession, delta_path: Path) -> datetime:
    if not os.path.exists(delta_path):
        return datetime.min
    try:
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forPath(spark, str(delta_path))
        latest_commit = delta_table.history(1).collect()[0]
        return latest_commit["timestamp"]
    except Exception:
        return datetime.min


def should_refresh(
    spark: SparkSession,
    target_product_path: Path,
    upstream_paths: Iterable[Path],
) -> bool:
    """True if target missing or any upstream Delta commit is newer."""
    target_ts = get_last_commit_timestamp(spark, target_product_path)
    if target_ts == datetime.min:
        return True

    for src in upstream_paths:
        src_ts = get_last_commit_timestamp(spark, src)
        if src_ts > target_ts:
            return True
    return False


def with_metadata(
    df: DataFrame,
    schema_ver: str = "1.0",
    data_source: str = "gold/integrated_taxi_trips",
) -> DataFrame:
    now = F.current_timestamp()
    return (
        df.withColumn("_data_source", F.lit(data_source))
        .withColumn("_created_at", now)
        .withColumn("_refreshed_at", now)
        .withColumn("_schema_version", F.lit(schema_ver))
    )


def safe_write_delta(
    df: DataFrame,
    target_path: Path,
    partition_by: Optional[list[str]] = None,
    schema_ver: str = "1.0",
    data_source: str = "gold/integrated_taxi_trips",
) -> Path:
    """Write Delta with product metadata columns (mergeSchema overwrite)."""
    enriched = with_metadata(df, schema_ver=schema_ver, data_source=data_source)
    writer = (
        enriched.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
    )
    parts = [c for c in (partition_by or []) if c in enriched.columns]
    if parts:
        writer = writer.partitionBy(*parts)
    writer.save(str(target_path))
    return target_path
