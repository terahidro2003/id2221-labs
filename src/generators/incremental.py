"""Incremental raw update generators (taxi parquet + weather/AQ CSV)."""

from __future__ import annotations

import csv
import io
import math
import os
import random
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from src.spark import project_root


def estimate_row_count(path: str) -> int:
    """Estimate rows from file size / average line length (no full parse)."""
    size = os.path.getsize(path)
    if size == 0:
        return 0
    with open(path, "rb") as f:
        sample = f.read(1 << 20)
    avg_line = len(sample) / max(sample.count(b"\n"), 1)
    return max(0, int(size / avg_line) - 1)


def read_csv_tail(path: str, n_rows: int, header: list) -> list:
    """Return the last n_rows as dicts by seeking from EOF."""
    if n_rows <= 0:
        return []

    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        if size == 0:
            return []

        buf, pos = b"", size
        while pos > 0 and buf.count(b"\n") < n_rows + 2:
            step = min(1 << 20, pos)
            pos -= step
            f.seek(pos)
            buf = f.read(step) + buf

    lines = buf.decode("utf-8-sig", errors="replace").splitlines()
    if pos > 0 and lines:
        lines = lines[1:]
    if lines and lines[0].startswith(header[0].lstrip("\ufeff")):
        lines = lines[1:]

    lines = lines[-n_rows:]
    rows = list(csv.reader(io.StringIO("\n".join(lines) + "\n")))
    return [dict(zip(header, row)) for row in rows if row]


def load_recent_csv_window(path: str, update_fraction: float, datetime_builder):
    """Return header, source row estimate, recent rows, latest timestamp, cutoff."""
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        header = next(csv.reader(f))

    n_rows = estimate_row_count(path)
    n_read = max(1, math.ceil(n_rows * min(0.05, update_fraction * 5)))
    dated = []
    for row in read_csv_tail(path, n_read, header):
        dt = datetime_builder(row)
        if dt is not None:
            dated.append((dt, row))

    if not dated:
        return header, n_rows, [], None, None

    latest = max(dt for dt, _ in dated)
    hours = max(1, math.ceil(8760 * update_fraction))
    cutoff = latest - timedelta(hours=hours)
    window = [row for dt, row in dated if dt > cutoff]
    if not window:
        window = [row for dt, row in dated if dt == latest]
        cutoff = latest

    return header, n_rows, window, latest, cutoff


def write_csv(path: str, header: list, rows: list) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def jitter(value, scale=0.03, lo=None, hi=None):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return value
    if math.isnan(number):
        return value
    number *= 1.0 + random.uniform(-scale, scale)
    if lo is not None:
        number = max(lo, number)
    if hi is not None:
        number = min(hi, number)
    return number


def format_number(value):
    if isinstance(value, float):
        return f"{value:.3f}".rstrip("0").rstrip(".")
    return value


def generate_incremental_csv_update(
    source_path: str,
    update_path: str,
    update_fraction: float,
    datetime_builder,
    datetime_formatter,
    transform_row=None,
    extra_columns: list = None,
    time_step: timedelta = timedelta(hours=1),
    numeric_jitter_columns: dict = None,
):
    header, n_source, window, latest, cutoff = load_recent_csv_window(
        source_path, update_fraction, datetime_builder
    )
    if not window or latest is None:
        raise ValueError(f"No valid records or timestamps found in {source_path}")

    time_shift = latest + time_step - cutoff
    jitter_cols = numeric_jitter_columns or {}
    out_rows = []

    for row in window:
        row = dict(row)
        orig_dt = datetime_builder(row)
        if orig_dt is not None:
            row = datetime_formatter(row, orig_dt + time_shift, time_shift)

        for col, (lo, hi) in jitter_cols.items():
            if row.get(col) not in (None, "", "NULL"):
                row[col] = format_number(jitter(row[col], lo=lo, hi=hi))

        if transform_row is not None:
            row = transform_row(row)
        out_rows.append(row)

    write_csv(update_path, list(header) + list(extra_columns or []), out_rows)
    return n_source, len(out_rows), datetime_builder(out_rows[0]), datetime_builder(out_rows[-1])


def _shift_ts(col, seconds):
    from pyspark.sql import functions as F

    return (F.unix_timestamp(F.col(col).cast("timestamp")) + F.lit(seconds)).cast(
        "timestamp"
    )


def _noise(col, seed, scale=0.06):
    from pyspark.sql import functions as F

    return F.round(F.col(col) * (1.0 + (F.rand(seed) - 0.5) * scale), 2)


def _write_single_parquet(df, path: str) -> str:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.parent / f".tmp_{out.stem}"
    if tmp.exists():
        shutil.rmtree(tmp)
    if out.exists():
        shutil.rmtree(out) if out.is_dir() else out.unlink()

    df.coalesce(1).write.mode("overwrite").parquet(str(tmp))
    parts = list(tmp.glob("*.parquet"))
    if not parts:
        raise RuntimeError(f"No parquet parts written under {tmp}")
    shutil.move(str(parts[0]), str(out))
    shutil.rmtree(tmp)
    return str(out)


def generate_taxi_trips_update(
    source_path: str,
    update_path: str,
    new_fraction: float = 0.07,
    duplicate_fraction: float = 0.015,
    seed: int = 42,
):
    from pyspark.sql import functions as F
    from src.spark import create_spark

    if not 0.05 <= new_fraction <= 0.10:
        raise ValueError("new_fraction must be between 0.05 and 0.10")
    if not 0.01 <= duplicate_fraction <= 0.02:
        raise ValueError("duplicate_fraction must be between 0.01 and 0.02")

    spark = create_spark("data-generator-taxi")
    trips = spark.read.parquet(source_path)

    valid = trips.filter(
        (F.col("tpep_pickup_datetime") >= F.lit("2024-01-01").cast("timestamp"))
        & (F.col("tpep_pickup_datetime") < F.lit("2025-01-01").cast("timestamp"))
    )

    n_source = valid.count()
    latest = valid.agg(F.max("tpep_pickup_datetime")).first()[0]
    if latest is None:
        spark.stop()
        raise ValueError(f"No valid pickup timestamps in {source_path}")

    sample = valid.sample(False, new_fraction, seed).cache()
    n_new = sample.count()
    sample_min = sample.agg(F.min("tpep_pickup_datetime")).first()[0]
    shift_s = int((latest - sample_min).total_seconds()) + 3600

    new_trips = (
        sample.withColumn("tpep_pickup_datetime", _shift_ts("tpep_pickup_datetime", shift_s))
        .withColumn("tpep_dropoff_datetime", _shift_ts("tpep_dropoff_datetime", shift_s))
        .withColumn("trip_distance", _noise("trip_distance", seed))
        .withColumn("fare_amount", _noise("fare_amount", seed + 1))
        .withColumn("total_amount", _noise("total_amount", seed + 2))
    )

    n_dups = max(1, math.ceil(n_source * duplicate_fraction))
    duplicates = valid.orderBy(F.rand(seed + 3)).limit(n_dups)
    out_path = _write_single_parquet(new_trips.unionByName(duplicates), update_path)

    new_min, new_max = new_trips.agg(
        F.min("tpep_pickup_datetime"), F.max("tpep_pickup_datetime")
    ).first()

    sample.unpersist()
    spark.stop()
    return n_source, n_new, n_dups, latest, new_min, new_max, out_path


def weather_dt_builder(row: dict):
    try:
        return datetime(
            int(row["year"]),
            int(row["month"]),
            int(row["day"]),
            int(float(row["hour"])),
        )
    except (KeyError, ValueError, TypeError):
        return None


def weather_dt_formatter(row: dict, new_dt: datetime, time_shift: timedelta) -> dict:
    row["year"] = str(new_dt.year)
    row["month"] = str(new_dt.month)
    row["day"] = str(new_dt.day)
    row["hour"] = str(new_dt.hour)
    return row


def weather_transform(row: dict) -> dict:
    raw = row.get("rhum")
    try:
        humidity = float(raw)
    except (TypeError, ValueError):
        humidity = random.uniform(40.0, 85.0)
    humidity = humidity * (1.0 + random.uniform(-0.02, 0.02))
    humidity = min(100.0, max(20.0, humidity))
    row["humidity"] = f"{humidity:.1f}"
    return row


AQI_BREAKPOINTS = [
    (0.0, 9.0, 0, 50),
    (9.1, 35.4, 51, 100),
    (35.5, 55.4, 101, 150),
    (55.5, 125.4, 151, 200),
    (125.5, 225.4, 201, 300),
    (225.5, 500.4, 301, 500),
]


def pm25_to_aqi(measurement) -> str:
    if measurement is None or str(measurement).strip() == "":
        return ""
    try:
        concentration = max(0.0, float(measurement))
    except (TypeError, ValueError):
        return ""

    for c_lo, c_hi, a_lo, a_hi in AQI_BREAKPOINTS:
        if concentration <= c_hi:
            aqi = ((a_hi - a_lo) / (c_hi - c_lo)) * (concentration - c_lo) + a_lo
            return str(int(min(500, max(0, round(aqi)))))
    return "500"


def air_quality_dt_builder(row):
    try:
        return datetime.strptime(
            f"{row['Date Local']} {row['Time Local']}", "%Y-%m-%d %H:%M"
        )
    except (KeyError, ValueError):
        return None


def air_quality_dt_formatter(row, new_dt, time_shift):
    row["Date Local"] = new_dt.strftime("%Y-%m-%d")
    row["Time Local"] = new_dt.strftime("%H:%M")
    if row.get("Date GMT") and row.get("Time GMT"):
        try:
            orig_gmt = datetime.strptime(
                f"{row['Date GMT']} {row['Time GMT']}", "%Y-%m-%d %H:%M"
            )
            new_gmt = orig_gmt + time_shift
            row["Date GMT"] = new_gmt.strftime("%Y-%m-%d")
            row["Time GMT"] = new_gmt.strftime("%H:%M")
        except ValueError:
            pass
    return row


def air_quality_transform(row):
    if row.get("Sample Measurement") not in (None, ""):
        jittered = jitter(row["Sample Measurement"], scale=0.05, lo=0.0)
        row["Sample Measurement"] = format_number(jittered)
    row["aqi"] = pm25_to_aqi(row.get("Sample Measurement", ""))
    return row


def run_all_generators() -> None:
    root = project_root()

    taxi_source = str(root / "data/raw/taxi_trips/yellow")
    taxi_update = str(root / "data/raw/taxi_trips/yellow_tripdata_update.parquet")
    print("Generating taxi trips incremental update...")
    (
        source_row_count,
        new_records,
        duplicates,
        latest_pickup,
        period_start,
        period_end,
        out_path,
    ) = generate_taxi_trips_update(
        source_path=taxi_source,
        update_path=taxi_update,
        new_fraction=0.07,
        duplicate_fraction=0.015,
    )
    print("=== TAXI TRIPS GENERATION REPORT ===")
    print(f"Source directory:       {taxi_source}")
    print(f"Source records:         {source_row_count:,}")
    print(f"Output update file:     {out_path}")
    print(f"New records written:    {new_records:,}")
    print(f"Duplicates injected:    {duplicates:,}")
    print(f"Original latest pickup: {latest_pickup}")
    print(f"New trip period:        {period_start} to {period_end}")

    weather_source = str(root / "data/raw/weather/weather.csv")
    weather_update = str(root / "data/raw/weather/weather_update.csv")
    source_row_count, new_records, start_period, end_period = generate_incremental_csv_update(
        source_path=weather_source,
        update_path=weather_update,
        update_fraction=0.01,
        datetime_builder=weather_dt_builder,
        datetime_formatter=weather_dt_formatter,
        transform_row=weather_transform,
        extra_columns=["humidity"],
        numeric_jitter_columns={
            "temp": (-50.0, 50.0),
            "wspd": (0.0, 80.0),
            "prcp": (0.0, 50.0),
            "pres": (950.0, 1050.0),
            "rhum": (20.0, 100.0),
        },
    )
    print("=== WEATHER GENERATION REPORT ===")
    print(f"Source file:            {weather_source}")
    print(f"Source records:         {source_row_count:,}")
    print(f"Output update file:     {weather_update}")
    print(f"New records written:    {new_records:,}")
    print(f"Time period covered:    {start_period} to {end_period}")

    aq_source = str(root / "data/raw/air_quality/hourly_88101_2024.csv")
    aq_update = str(root / "data/raw/air_quality/hourly_88101_update.csv")
    print("Generating air quality incremental update...")
    source_row_count, new_records, start_period, end_period = generate_incremental_csv_update(
        source_path=aq_source,
        update_path=aq_update,
        update_fraction=0.01,
        datetime_builder=air_quality_dt_builder,
        datetime_formatter=air_quality_dt_formatter,
        transform_row=air_quality_transform,
        extra_columns=["aqi"],
    )
    print("=== AIR QUALITY GENERATION REPORT ===")
    print(f"Source file:            {aq_source}")
    print(f"Source records:         {source_row_count:,}")
    print(f"Output update file:     {aq_update}")
    print(f"New records written:    {new_records:,}")
    print(f"Time period covered:    {start_period} to {end_period}")
