"""Spark SQL answers for operational pipeline questions.

Each question returns one row per *source* dataset family
(taxi_trips, weather, air_quality, taxi_zones, …) with:
- overall aggregates across all layers (bronze/silver/gold products rolled in)
- bronze / silver / gold breakdown columns
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src.lake import read_delta
from src.monitoring.recorder import PIPELINE_RUNS

# Gold products / tables map back onto the bronze/silver source they belong to.
_SOURCE_DATASET_CASE = """
    CASE dataset
        WHEN 'integrated_taxi_trips' THEN 'taxi_trips'
        WHEN 'daily_borough_mobility' THEN 'taxi_trips'
        WHEN 'taxi_zone_monthly_demand' THEN 'taxi_trips'
        WHEN 'zone_weather_sensitivity' THEN 'taxi_trips'
        WHEN 'weather_impact_summary' THEN 'weather'
        WHEN 'air_quality_demand_summary' THEN 'air_quality'
        ELSE dataset
    END
"""


def load_pipeline_runs(spark: SparkSession) -> DataFrame:
    """Load ops metrics and register temp views for SQL."""
    df = read_delta(spark, PIPELINE_RUNS)
    df.createOrReplaceTempView("pipeline_runs")
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW pipeline_runs_by_source AS
        SELECT
            *,
            {_SOURCE_DATASET_CASE} AS source_dataset
        FROM pipeline_runs
        """
    )
    return df


def execution_times(spark: SparkSession) -> DataFrame:
    """How long each pipeline step took (one row per dataset/layer run)."""
    return spark.sql(
        """
        SELECT
            started_at,
            layer,
            source_dataset AS dataset,
            dataset AS recorded_as,
            ROUND(duration_seconds, 2) AS execution_time_sec,
            processed_records,
            inserted_records,
            rejected_records,
            schema_version,
            status
        FROM pipeline_runs_by_source
        ORDER BY started_at
        """
    )


def execution_time_by_layer(spark: SparkSession) -> DataFrame:
    """Total / average wall time rolled up by medallion layer."""
    return spark.sql(
        """
        SELECT
            layer,
            COUNT(*) AS steps,
            ROUND(SUM(duration_seconds), 2) AS total_execution_time_sec,
            ROUND(AVG(duration_seconds), 2) AS avg_execution_time_sec,
            ROUND(MAX(duration_seconds), 2) AS max_execution_time_sec
        FROM pipeline_runs
        GROUP BY layer
        ORDER BY total_execution_time_sec DESC
        """
    )


def all_pipeline_runs(spark: SparkSession) -> DataFrame:
    """Full per-execution metrics (time, counts, schema version, rejects)."""
    return execution_times(spark)


def datasets_failing_validation_most(spark: SparkSession) -> DataFrame:
    """Which source dataset fails validation most frequently?

    Rolls gold products into their source family (taxi_trips / weather / …).
    """
    return spark.sql(
        """
        SELECT
            source_dataset AS dataset,
            COUNT(*) AS failed_runs,
            COALESCE(SUM(validation_failures), 0) AS total_validation_failures,
            COALESCE(SUM(rejected_records), 0) AS total_rejected_records,
            SUM(CASE WHEN layer = 'bronze' THEN 1 ELSE 0 END) AS bronze_failed_runs,
            COALESCE(SUM(CASE WHEN layer = 'bronze' THEN validation_failures ELSE 0 END), 0)
                AS bronze_validation_failures,
            COALESCE(SUM(CASE WHEN layer = 'bronze' THEN rejected_records ELSE 0 END), 0)
                AS bronze_rejected_records,
            SUM(CASE WHEN layer = 'silver' THEN 1 ELSE 0 END) AS silver_failed_runs,
            COALESCE(SUM(CASE WHEN layer = 'silver' THEN validation_failures ELSE 0 END), 0)
                AS silver_validation_failures,
            COALESCE(SUM(CASE WHEN layer = 'silver' THEN rejected_records ELSE 0 END), 0)
                AS silver_rejected_records,
            SUM(CASE WHEN layer = 'gold' THEN 1 ELSE 0 END) AS gold_failed_runs,
            COALESCE(SUM(CASE WHEN layer = 'gold' THEN validation_failures ELSE 0 END), 0)
                AS gold_validation_failures,
            COALESCE(SUM(CASE WHEN layer = 'gold' THEN rejected_records ELSE 0 END), 0)
                AS gold_rejected_records
        FROM pipeline_runs_by_source
        WHERE validation_failures > 0 OR validation_ok = false
        GROUP BY source_dataset
        ORDER BY failed_runs DESC, total_validation_failures DESC
        """
    )


def datasets_longest_processing(spark: SparkSession) -> DataFrame:
    """Which source dataset requires the longest processing time?

    One row per source family with overall + per-layer avg/max/runs.
    """
    return spark.sql(
        """
        SELECT
            source_dataset AS dataset,
            ROUND(AVG(duration_seconds), 2) AS avg_execution_time_sec,
            ROUND(MAX(duration_seconds), 2) AS max_execution_time_sec,
            ROUND(SUM(duration_seconds), 2) AS total_execution_time_sec,
            ROUND(AVG(processed_records), 0) AS avg_processed_records,
            COUNT(*) AS runs,
            ROUND(AVG(CASE WHEN layer = 'bronze' THEN duration_seconds END), 2)
                AS bronze_avg_sec,
            ROUND(MAX(CASE WHEN layer = 'bronze' THEN duration_seconds END), 2)
                AS bronze_max_sec,
            SUM(CASE WHEN layer = 'bronze' THEN 1 ELSE 0 END) AS bronze_runs,
            ROUND(AVG(CASE WHEN layer = 'silver' THEN duration_seconds END), 2)
                AS silver_avg_sec,
            ROUND(MAX(CASE WHEN layer = 'silver' THEN duration_seconds END), 2)
                AS silver_max_sec,
            SUM(CASE WHEN layer = 'silver' THEN 1 ELSE 0 END) AS silver_runs,
            ROUND(AVG(CASE WHEN layer = 'gold' THEN duration_seconds END), 2)
                AS gold_avg_sec,
            ROUND(MAX(CASE WHEN layer = 'gold' THEN duration_seconds END), 2)
                AS gold_max_sec,
            SUM(CASE WHEN layer = 'gold' THEN 1 ELSE 0 END) AS gold_runs
        FROM pipeline_runs_by_source
        GROUP BY source_dataset
        ORDER BY total_execution_time_sec DESC
        """
    )


def rejects_per_execution(spark: SparkSession) -> DataFrame:
    """How many records were rejected, aggregated by source dataset family?"""
    return spark.sql(
        """
        SELECT
            source_dataset AS dataset,
            COUNT(*) AS runs,
            COALESCE(SUM(processed_records), 0) AS total_processed_records,
            COALESCE(SUM(inserted_records), 0) AS total_inserted_records,
            COALESCE(SUM(rejected_records), 0) AS total_rejected_records,
            COALESCE(SUM(validation_failures), 0) AS total_validation_failures,
            COALESCE(SUM(CASE WHEN layer = 'bronze' THEN processed_records ELSE 0 END), 0)
                AS bronze_processed,
            COALESCE(SUM(CASE WHEN layer = 'bronze' THEN inserted_records ELSE 0 END), 0)
                AS bronze_inserted,
            COALESCE(SUM(CASE WHEN layer = 'bronze' THEN rejected_records ELSE 0 END), 0)
                AS bronze_rejected,
            COALESCE(SUM(CASE WHEN layer = 'silver' THEN processed_records ELSE 0 END), 0)
                AS silver_processed,
            COALESCE(SUM(CASE WHEN layer = 'silver' THEN inserted_records ELSE 0 END), 0)
                AS silver_inserted,
            COALESCE(SUM(CASE WHEN layer = 'silver' THEN rejected_records ELSE 0 END), 0)
                AS silver_rejected,
            COALESCE(SUM(CASE WHEN layer = 'gold' THEN processed_records ELSE 0 END), 0)
                AS gold_processed,
            COALESCE(SUM(CASE WHEN layer = 'gold' THEN inserted_records ELSE 0 END), 0)
                AS gold_inserted,
            COALESCE(SUM(CASE WHEN layer = 'gold' THEN rejected_records ELSE 0 END), 0)
                AS gold_rejected
        FROM pipeline_runs_by_source
        GROUP BY source_dataset
        ORDER BY total_rejected_records DESC, dataset
        """
    )


def processing_time_over_executions(spark: SparkSession) -> DataFrame:
    """How has processing time changed, aggregated by source dataset family?"""
    return spark.sql(
        """
        SELECT
            source_dataset AS dataset,
            COUNT(*) AS runs,
            ROUND(AVG(duration_seconds), 2) AS avg_execution_time_sec,
            ROUND(MIN(duration_seconds), 2) AS min_execution_time_sec,
            ROUND(MAX(duration_seconds), 2) AS max_execution_time_sec,
            ROUND(SUM(duration_seconds), 2) AS total_execution_time_sec,
            ROUND(
                AVG(CASE WHEN rn_asc = 1 THEN duration_seconds END), 2
            ) AS first_execution_time_sec,
            ROUND(
                AVG(CASE WHEN rn_desc = 1 THEN duration_seconds END), 2
            ) AS last_execution_time_sec,
            ROUND(AVG(CASE WHEN layer = 'bronze' THEN duration_seconds END), 2)
                AS bronze_avg_sec,
            ROUND(AVG(CASE WHEN layer = 'silver' THEN duration_seconds END), 2)
                AS silver_avg_sec,
            ROUND(AVG(CASE WHEN layer = 'gold' THEN duration_seconds END), 2)
                AS gold_avg_sec,
            MIN(started_at) AS first_started_at,
            MAX(started_at) AS last_started_at
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY source_dataset ORDER BY started_at ASC
                ) AS rn_asc,
                ROW_NUMBER() OVER (
                    PARTITION BY source_dataset ORDER BY started_at DESC
                ) AS rn_desc
            FROM pipeline_runs_by_source
        )
        GROUP BY source_dataset
        ORDER BY total_execution_time_sec DESC
        """
    )


def _print_time_summary(spark: SparkSession) -> None:
    """Plain-text list so execution time is obvious even if tables scroll."""
    rows = (
        spark.sql(
            """
            SELECT layer, dataset, ROUND(duration_seconds, 2) AS sec, status
            FROM pipeline_runs
            ORDER BY started_at
            """
        )
        .collect()
    )
    print("\n=== Execution time (each pipeline step) ===")
    if not rows:
        print("(no runs recorded yet)")
        return
    print(f"{'layer':<8} {'dataset':<28} {'time_sec':>10}  status")
    print("-" * 60)
    for r in rows:
        print(f"{r['layer']:<8} {r['dataset']:<28} {r['sec']:>10.2f}  {r['status']}")
    total = sum(float(r["sec"] or 0) for r in rows)
    print("-" * 60)
    print(f"{'TOTAL':<8} {'(all recorded steps)':<28} {total:>10.2f}")


def print_monitoring_report(spark: SparkSession) -> None:
    try:
        alive = spark is not None and spark.sparkContext._jsc is not None
    except Exception:
        alive = False
    if not alive:
        raise RuntimeError(
            "Spark session is stopped. Recreate it with create_spark(...) "
            "before running the monitoring report."
        )

    load_pipeline_runs(spark)

    _print_time_summary(spark)

    print("\n=== Execution time by layer ===")
    execution_time_by_layer(spark).show(20, truncate=False)

    print("=== Execution time + counts (every step) ===")
    execution_times(spark).show(200, truncate=False)

    print("=== Source datasets failing validation (family + per layer) ===")
    datasets_failing_validation_most(spark).show(50, truncate=False)

    print("=== Source datasets with longest processing (family + per layer) ===")
    datasets_longest_processing(spark).show(50, truncate=False)

    print("=== Rejected / inserted / processed by source dataset (+ per layer) ===")
    rejects_per_execution(spark).show(100, truncate=False)

    print("=== Processing time by source dataset (+ per layer) ===")
    processing_time_over_executions(spark).show(100, truncate=False)
