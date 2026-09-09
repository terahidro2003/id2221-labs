"""Delta Lake paths and read/write helpers shared by the notebooks."""

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

for _path in (BRONZE, SILVER, GOLD):
    _path.mkdir(parents=True, exist_ok=True)


def delta_safe_name(name: str) -> str:
    return (
        name.strip()
        .replace(" ", "_")
        .replace(",", "_")
        .replace(";", "_")
        .replace("{", "_")
        .replace("}", "_")
        .replace("(", "_")
        .replace(")", "_")
        .replace("\n", "_")
        .replace("\t", "_")
        .replace("=", "_")
    )


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
) -> Path:
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    parts = [c for c in (partition_by or []) if c in df.columns]
    if parts:
        writer = writer.partitionBy(*parts)
    writer.save(str(path))
    return path


def read_delta(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.format("delta").load(str(path))


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
) -> Path:
    framed = with_delta_safe_columns(df).withColumn("_ingested_at", F.current_timestamp())
    return write_delta(framed, BRONZE / table_name, partition_by)


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
