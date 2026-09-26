"""Spark SQL answers for operational pipeline questions."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src.lake import read_delta
from src.monitoring.recorder import PIPELINE_RUNS


def load_pipeline_runs(spark: SparkSession) -> DataFrame:
    """Load ops metrics and register a temp view for SQL."""
    df = read_delta(spark, PIPELINE_RUNS)
    df.createOrReplaceTempView("pipeline_runs")
    return df


def execution_times(spark: SparkSession) -> DataFrame:
    """How long each pipeline step took (one row per dataset/layer run)."""
    return spark.sql(
        """
        SELECT
            started_at,
            layer,
            dataset,
            ROUND(duration_seconds, 2) AS execution_time_sec,
            processed_records,
            inserted_records,
            rejected_records,
            schema_version,
            status
        FROM pipeline_runs
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
    """Which dataset fails validation most frequently?"""
    return spark.sql(
        """
        SELECT
            dataset,
            COUNT(*) AS failed_runs,
            SUM(validation_failures) AS total_validation_failures,
            SUM(rejected_records) AS total_rejected_records
        FROM pipeline_runs
        WHERE validation_failures > 0 OR validation_ok = false
        GROUP BY dataset
        ORDER BY failed_runs DESC, total_validation_failures DESC
        """
    )


def datasets_longest_processing(spark: SparkSession) -> DataFrame:
    """Which dataset requires the longest processing time? (avg + max)."""
    return spark.sql(
        """
        SELECT
            dataset,
            ROUND(AVG(duration_seconds), 2) AS avg_execution_time_sec,
            ROUND(MAX(duration_seconds), 2) AS max_execution_time_sec,
            ROUND(AVG(processed_records), 0) AS avg_processed_records,
            COUNT(*) AS runs
        FROM pipeline_runs
        GROUP BY dataset
        ORDER BY avg_execution_time_sec DESC
        """
    )


def rejects_per_execution(spark: SparkSession) -> DataFrame:
    """How many records were rejected during each execution?"""
    return spark.sql(
        """
        SELECT
            started_at,
            layer,
            dataset,
            schema_version,
            processed_records,
            inserted_records,
            rejected_records,
            validation_failures,
            ROUND(duration_seconds, 2) AS execution_time_sec,
            status
        FROM pipeline_runs
        ORDER BY started_at
        """
    )


def processing_time_over_executions(spark: SparkSession) -> DataFrame:
    """How has processing time changed over multiple executions?"""
    return spark.sql(
        """
        SELECT
            dataset,
            layer,
            started_at,
            schema_version,
            ROUND(duration_seconds, 2) AS execution_time_sec,
            processed_records,
            inserted_records,
            rejected_records,
            status
        FROM pipeline_runs
        ORDER BY dataset, started_at
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

    print("=== Datasets failing validation most frequently ===")
    datasets_failing_validation_most(spark).show(50, truncate=False)

    print("=== Datasets with longest processing time ===")
    datasets_longest_processing(spark).show(50, truncate=False)

    print("=== Rejected / inserted / processed records per execution ===")
    rejects_per_execution(spark).show(100, truncate=False)

    print("=== Processing time over executions (by dataset) ===")
    processing_time_over_executions(spark).show(100, truncate=False)
