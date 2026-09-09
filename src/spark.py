"""Local Spark session with Delta Lake, matching the ingestion notebook setup."""

from __future__ import annotations

import glob
import os
import subprocess
import sys
from pathlib import Path


def project_root() -> Path:
    root = Path.cwd().resolve()
    if (root / "src").is_dir() or (root / "data" / "raw").exists():
        return root
    parent = root.parent
    if (parent / "src").is_dir() or (parent / "data" / "raw").exists():
        return parent
    return root


def _configure_java() -> Path:
    for key in ("JAVA_HOME", "SPARK_HOME", "PYSPARK_SUBMIT_ARGS"):
        os.environ.pop(key, None)

    candidates = (
        Path("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"),
        Path("/opt/homebrew/opt/openjdk@17"),
        Path("/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"),
    )
    java_home = next((p for p in candidates if (p / "bin" / "java").is_file()), None)
    if java_home is None:
        raise RuntimeError("OpenJDK 17 not found. Install with: brew install openjdk@17")

    os.environ["JAVA_HOME"] = str(java_home)
    os.environ["PATH"] = f"{java_home / 'bin'}:" + os.environ.get("PATH", "")
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
    subprocess.run(
        [str(java_home / "bin" / "java"), "-version"],
        check=True,
        capture_output=True,
    )
    return java_home


def _prefer_venv_site_packages(root: Path) -> None:
    site_pkgs = sorted(glob.glob(str(root / ".venv" / "lib" / "python*" / "site-packages")))
    if not site_pkgs:
        raise RuntimeError("Missing .venv packages. Run: .venv/bin/pip install -r requirements.txt")
    site = site_pkgs[-1]
    if site in sys.path:
        sys.path.remove(site)
    sys.path.insert(0, site)


def create_spark(app_name: str = "urban-data-platform"):
    root = project_root()
    _configure_java()
    _prefer_venv_site_packages(root)

    from delta import configure_spark_with_delta_pip
    from pyspark import SparkContext
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()
    if SparkContext._active_spark_context is not None:
        SparkContext._active_spark_context.stop()

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
