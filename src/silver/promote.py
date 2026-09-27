
from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession, functions as F

from src.bronze.ingest import _prep_bronze, _read_raw, has_incremental_update
from src.common.config import list_dataset_configs, load_dataset_config
from src.lake import BRONZE, SILVER, write_silver
from src.monitoring import record_run
from src.silver.transforms import transform_dataset
from src.validation import run_row_checks


def write_rejects(df: DataFrame, table_name: str) -> None:
    path = BRONZE / f"{table_name}_rejects"
    (
        df.withColumn("_rejected_at", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(str(path))
    )


def promote_dataset(spark: SparkSession, name: str) -> None:
    cfg = load_dataset_config(name)
    silver = cfg.get("silver") or {}
    table = silver.get("table", name)
    schema_version = str(cfg.get("schema_version", "1.0"))

    with record_run(
        spark, layer="silver", dataset=name, schema_version=schema_version
    ) as run:
        silver_path = SILVER / table
        incremental = has_incremental_update(name) and (
            BRONZE / table / "_delta_log"
        ).exists()
        if incremental:
            update_raw = _read_raw(spark, cfg, name, only_update=True)
            update_bronze = _prep_bronze(update_raw, cfg)
            transformed = transform_dataset(spark, name, source_df=update_bronze)
        else:
            transformed = transform_dataset(spark, name)
        checked = run_row_checks(transformed, silver)

        n_good = checked.good_df.count() if checked.good_df is not None else 0
        n_bad = checked.rejects_df.count() if checked.rejects_df is not None else 0

        write_rejects(checked.rejects_df, table)
        if silver_path.exists() and (silver_path / "_delta_log").exists():
            from delta.tables import DeltaTable

            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
            
            pk = silver.get("primary_key") or []
            partitions = silver.get("partition_by") or []
            merge_cols = list(dict.fromkeys(pk + partitions))
            match_cols = [c for c in merge_cols if c in checked.good_df.columns]
            
            if match_cols:
                from pyspark.sql.window import Window
                
                pk_cols = [c for c in pk if c in checked.good_df.columns] or match_cols
                window_spec = Window.partitionBy(*pk_cols).orderBy(F.lit(1))
                deduped_update = (
                    checked.good_df.withColumn("_rn", F.row_number().over(window_spec))
                    .filter(F.col("_rn") == 1)
                    .drop("_rn")
                )
                
                silver_target = DeltaTable.forPath(spark, str(silver_path))
                
                merge_cond = " AND ".join([f"target.`{c}` = source.`{c}`" for c in match_cols])
                (
                    silver_target.alias("target")
                    .merge(deduped_update.alias("source"), merge_cond)
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                (
                    checked.good_df.write.format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .save(str(silver_path))
                )
        else:
            write_silver(
                checked.good_df, table, partition_by=silver.get("partition_by") or []
            )

        run.set_counts(
            processed=n_good + n_bad,
            inserted=n_good,
            rejected=n_bad,
            validation_failures=n_bad,
            validation_ok=n_bad == 0,
        )
        print(f"[silver/{table}] kept={n_good:,}  rejected={n_bad:,}")


def promote_all(spark: SparkSession, datasets: Optional[list[str]] = None) -> None:
    names = datasets or list_dataset_configs()
    for name in names:
        promote_dataset(spark, name)
