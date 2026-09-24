"""Silver promote: transform → row validation → write silver + rejects."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession, functions as F

from src.common.config import list_dataset_configs, load_dataset_config
from src.lake import BRONZE, SILVER, write_silver
from src.silver.transforms import transform_dataset
from src.validation import run_row_checks


def write_rejects(df: DataFrame, table_name: str) -> None:
    path = BRONZE / f"{table_name}_rejects"
    (
        df.withColumn("_rejected_at", F.current_timestamp())
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(path))
    )


def promote_dataset(spark: SparkSession, name: str) -> None:
    cfg = load_dataset_config(name)
    silver = cfg.get("silver") or {}
    table = silver.get("table", name)

    transformed = transform_dataset(spark, name)
    checked = run_row_checks(transformed, silver)

    write_rejects(checked.rejects_df, table)
    write_silver(checked.good_df, table, partition_by=silver.get("partition_by") or [])

    n_good = spark.read.format("delta").load(str(SILVER / table)).count()
    n_bad = spark.read.format("delta").load(str(BRONZE / f"{table}_rejects")).count()
    print(f"[silver/{table}] kept={n_good:,}  rejected={n_bad:,}")


def promote_all(spark: SparkSession, datasets: Optional[list[str]] = None) -> None:
    names = datasets or list_dataset_configs()
    for name in names:
        promote_dataset(spark, name)
