"""Schema rule: require expected columns and type compatibility."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import types as T

from src.common.config import load_compatible_types


def _compatible_types() -> dict[str, tuple]:
    return {
        logical: tuple(getattr(T, type_name) for type_name in spark_names)
        for logical, spark_names in load_compatible_types().items()
    }


def check_schema(df: DataFrame, expected: dict[str, str], **_: Any) -> list[str]:
    """Return error strings; empty list means ok. Extra columns are allowed."""
    actual = {field.name: field.dataType for field in df.schema.fields}
    compatible = _compatible_types()
    errors: list[str] = []

    for column, expected_type in expected.items():
        if column not in actual:
            errors.append(f"missing column: {column}")
            continue

        allowed = compatible.get(expected_type)
        if allowed and not isinstance(actual[column], allowed):
            errors.append(
                f"{column}: got {actual[column].simpleString()}, expected {expected_type}"
            )
    return errors
