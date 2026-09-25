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


def datasets_failing_validation_most(spark: SparkSession) -> DataFrame:
    """Which dataset fails validation most frequently?"""
    return spark.sql(
        """
        SELECT
            dataset,
            COUNT(*) AS failed_runs,
            SUM(validation_failures) AS total_validation_failures
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
            ROUND(AVG(duration_seconds), 2) AS avg_duration_seconds,
            ROUND(MAX(duration_seconds), 2) AS max_duration_seconds,
            COUNT(*) AS runs
        FROM pipeline_runs
        GROUP BY dataset
        ORDER BY avg_duration_seconds DESC
        """
    )


def rejects_per_execution(spark: SparkSession) -> DataFrame:
    """How many records were rejected during each execution?"""
    return spark.sql(
        """
        SELECT
            run_id,
            layer,
            dataset,
            started_at,
            rejected_records,
            validation_failures,
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
            ROUND(duration_seconds, 2) AS duration_seconds,
            processed_records,
            status
        FROM pipeline_runs
        ORDER BY dataset, started_at
        """
    )


def print_monitoring_report(spark: SparkSession) -> None:
    load_pipeline_runs(spark)

    print("\n=== 1. Datasets failing validation most frequently ===")
    datasets_failing_validation_most(spark).show(50, truncate=False)

    print("=== 2. Datasets with longest processing time ===")
    datasets_longest_processing(spark).show(50, truncate=False)

    print("=== 3. Rejected records per execution ===")
    rejects_per_execution(spark).show(100, truncate=False)

    print("=== 4. Processing time over executions ===")
    processing_time_over_executions(spark).show(100, truncate=False)
