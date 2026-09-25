"""Append-only pipeline run metrics to data/lake/ops/pipeline_runs."""

from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator, Optional

from pyspark.sql import Row, SparkSession
from pyspark.sql import types as T

from src.lake import OPS

PIPELINE_RUNS = OPS / "pipeline_runs"

_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("layer", T.StringType(), False),
        T.StructField("dataset", T.StringType(), False),
        T.StructField("started_at", T.TimestampType(), False),
        T.StructField("finished_at", T.TimestampType(), False),
        T.StructField("duration_seconds", T.DoubleType(), False),
        T.StructField("processed_records", T.LongType(), True),
        T.StructField("inserted_records", T.LongType(), True),
        T.StructField("rejected_records", T.LongType(), True),
        T.StructField("schema_version", T.StringType(), True),
        T.StructField("validation_failures", T.LongType(), True),
        T.StructField("validation_ok", T.BooleanType(), True),
        T.StructField("status", T.StringType(), False),
        T.StructField("error_message", T.StringType(), True),
    ]
)


@dataclass
class RunContext:
    """Mutable metrics for one pipeline execution."""

    spark: SparkSession
    layer: str
    dataset: str
    schema_version: str = "1.0"
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    processed_records: Optional[int] = None
    inserted_records: Optional[int] = None
    rejected_records: Optional[int] = None
    validation_failures: Optional[int] = None
    validation_ok: Optional[bool] = None
    error_message: Optional[str] = None

    def set_counts(
        self,
        *,
        processed: Optional[int] = None,
        inserted: Optional[int] = None,
        rejected: Optional[int] = None,
        validation_failures: Optional[int] = None,
        validation_ok: Optional[bool] = None,
    ) -> None:
        if processed is not None:
            self.processed_records = int(processed)
        if inserted is not None:
            self.inserted_records = int(inserted)
        if rejected is not None:
            self.rejected_records = int(rejected)
        if validation_failures is not None:
            self.validation_failures = int(validation_failures)
        if validation_ok is not None:
            self.validation_ok = validation_ok


def append_pipeline_run(
    spark: SparkSession,
    *,
    run_id: str,
    layer: str,
    dataset: str,
    started_at: datetime,
    finished_at: datetime,
    duration_seconds: float,
    processed_records: Optional[int],
    inserted_records: Optional[int],
    rejected_records: Optional[int],
    schema_version: str,
    validation_failures: Optional[int],
    validation_ok: Optional[bool],
    status: str,
    error_message: Optional[str],
) -> None:
    OPS.mkdir(parents=True, exist_ok=True)
    row = Row(
        run_id=run_id,
        layer=layer,
        dataset=dataset,
        started_at=started_at.replace(tzinfo=None) if started_at.tzinfo else started_at,
        finished_at=finished_at.replace(tzinfo=None) if finished_at.tzinfo else finished_at,
        duration_seconds=float(duration_seconds),
        processed_records=processed_records,
        inserted_records=inserted_records,
        rejected_records=rejected_records,
        schema_version=schema_version,
        validation_failures=validation_failures,
        validation_ok=validation_ok,
        status=status,
        error_message=error_message,
    )
    df = spark.createDataFrame([row], schema=_SCHEMA)
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("layer")
        .save(str(PIPELINE_RUNS))
    )


@contextmanager
def record_run(
    spark: SparkSession,
    *,
    layer: str,
    dataset: str,
    schema_version: str = "1.0",
) -> Iterator[RunContext]:
    """Time a pipeline step, append one ops row, re-raise on failure."""
    ctx = RunContext(
        spark=spark,
        layer=layer,
        dataset=dataset,
        schema_version=schema_version,
    )
    t0 = time.perf_counter()
    status = "success"
    try:
        yield ctx
    except Exception as exc:
        status = "failed"
        if ctx.error_message is None:
            ctx.error_message = str(exc)[:2000]
        if ctx.validation_ok is None:
            ctx.validation_ok = False
        raise
    finally:
        finished = datetime.now(timezone.utc)
        duration = time.perf_counter() - t0
        if ctx.validation_ok is None:
            ctx.validation_ok = status == "success" and not (ctx.validation_failures or 0)
        try:
            append_pipeline_run(
                spark,
                run_id=ctx.run_id,
                layer=ctx.layer,
                dataset=ctx.dataset,
                started_at=ctx.started_at,
                finished_at=finished,
                duration_seconds=duration,
                processed_records=ctx.processed_records,
                inserted_records=ctx.inserted_records,
                rejected_records=ctx.rejected_records,
                schema_version=ctx.schema_version,
                validation_failures=ctx.validation_failures,
                validation_ok=ctx.validation_ok,
                status=status,
                error_message=ctx.error_message,
            )
        except Exception as write_exc:
            print(f"[ops] failed to append pipeline_runs: {write_exc}")
