"""Pipeline execution monitoring (ops Delta + Spark SQL reports)."""

from src.monitoring.recorder import PIPELINE_RUNS, RunContext, record_run

__all__ = ["PIPELINE_RUNS", "RunContext", "record_run"]
