
from __future__ import annotations

from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, functions as F

from src.spark import project_root

ROOT = project_root()
RAW = ROOT / "data" / "raw"
BRONZE = ROOT / "data" / "lake" / "bronze"
SILVER = ROOT / "data" / "lake" / "silver"
GOLD = ROOT / "data" / "lake" / "gold"
OPS = ROOT / "data" / "lake" / "ops"

for _path in (BRONZE, SILVER, GOLD, OPS):
    _path.mkdir(parents=True, exist_ok=True)


_DELTA_UNSAFE = str.maketrans({ch: "_" for ch in " ,;{}()\n\t="})


def delta_safe_name(name: str) -> str:
    return name.strip().translate(_DELTA_UNSAFE)


def with_delta_safe_columns(df: DataFrame) -> DataFrame:
    out = df
    for col in df.columns:
        safe = delta_safe_name(col)
        if safe != col:
            out = out.withColumnRenamed(col, safe)
    return out


def write_delta(
    df: DataFrame,
    path: Path,
    partition_by: Optional[list[str]] = None,
    *,
    mode: str = "overwrite",
) -> Path:
    writer = df.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    else:
        # Incremental appends may introduce evolved columns (humidity, aqi, …).
        writer = writer.option("mergeSchema", "true")
    parts = [c for c in (partition_by or []) if c in df.columns]
    if parts:
        writer = writer.partitionBy(*parts)
    writer.save(str(path))
    return path


def read_delta(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.format("delta").load(str(path))


def align_to_delta_schema(df: DataFrame, path: Path) -> DataFrame:
    """Cast overlapping columns to the existing Delta table types before append.

    Avoids DELTA_FAILED_TO_MERGE_FIELDS (e.g. TimestampType vs TimestampNTZType).
    New columns are left unchanged so mergeSchema can add them.
    """
    if not (path / "_delta_log").is_dir():
        return df
    spark = df.sparkSession
    existing = {f.name: f.dataType for f in read_delta(spark, path).schema.fields}
    out = df
    for name, dtype in existing.items():
        if name not in out.columns:
            continue
        if out.schema[name].dataType != dtype:
            out = out.withColumn(name, F.col(name).cast(dtype))
    return out


def show_delta(spark: SparkSession, path: Path, n: int = 3) -> None:
    df = read_delta(spark, path)
    try:
        rel = path.relative_to(ROOT)
    except ValueError:
        rel = path
    print(f"{path.name}: {df.count()} rows @ {rel}")
    df.show(n, truncate=False)


def write_bronze(
    df: DataFrame,
    table_name: str,
    partition_by: Optional[list[str]] = None,
    *,
    mode: str = "overwrite",
) -> Path:
    path = BRONZE / table_name
    framed = with_delta_safe_columns(df).withColumn("_ingested_at", F.current_timestamp())
    if mode == "append":
        framed = align_to_delta_schema(framed, path)
    return write_delta(framed, path, partition_by, mode=mode)


def write_silver(
    df: DataFrame,
    table_name: str,
    partition_by: Optional[list[str]] = None,
) -> Path:
    return write_delta(df, SILVER / table_name, partition_by)


def write_gold(
    df: DataFrame,
    table_name: str,
    partition_by: Optional[list[str]] = None,
) -> Path:
    return write_delta(df, GOLD / table_name, partition_by)
