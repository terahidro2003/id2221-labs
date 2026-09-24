"""Config-driven Spark DataFrame validation (schema + row DQ)."""

from src.validation.runner import run_row_checks, run_schema_checks
from src.validation.types import ValidationResult

__all__ = ["ValidationResult", "run_schema_checks", "run_row_checks"]
