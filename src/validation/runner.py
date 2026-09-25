"""Run schema (bronze) and row (silver) validation from YAML rule lists."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from pyspark.sql import Column, DataFrame, functions as F

from src.validation.registry import RULES
from src.validation.types import ValidationResult

_ROW_REASON_RULES = frozenset(
    {
        "not_null",
        "numeric_range",
        "timestamp_range",
        "timestamp_order",
    }
)


def _as_rule_dicts(items: Sequence[Mapping[str, Any]]) -> list[dict]:
    return [dict(item) for item in items]


def _rule_items(config: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None) -> list[dict]:
    """Normalize a validation list, layer config, or single rule into rule dicts."""
    if config is None:
        return []

    if isinstance(config, Sequence) and not isinstance(config, (str, bytes)):
        return _as_rule_dicts(config)

    if not isinstance(config, Mapping):
        return []

    if "validation" in config:
        return _as_rule_dicts(config.get("validation") or [])

    if "rule" in config:
        return [dict(config)]

    for layer in ("bronze", "silver"):
        nested = config.get(layer)
        if isinstance(nested, Mapping) and nested.get("validation") is not None:
            return _as_rule_dicts(nested["validation"])

    return []


def _rule_params(item: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in item.items() if key != "rule"}


def run_schema_checks(
    df: DataFrame,
    config: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None,
    dataset: str = "",
) -> ValidationResult:
    """Schema validation. Returns ValidationResult; does not raise (caller decides)."""
    errors: list[str] = []
    for item in _rule_items(config):
        if item.get("rule") != "schema":
            continue
        errors.extend(RULES["schema"](df, **_rule_params(item)))

    label = dataset or "dataset"
    ok = not errors
    print(f"[{label}] schema ok={ok}")
    if errors:
        print("  errors:", errors)
    return ValidationResult(ok=ok, errors=errors, stats={"dataset": label})


def _collect_reject_reasons(
    df: DataFrame,
    items: list[dict],
) -> tuple[list[Column], dict[str, Any] | None]:
    """Build reject-reason columns; return primary_key params separately if present."""
    reasons: list[Column] = []
    pk_params: dict[str, Any] | None = None

    for item in items:
        rule_id = item.get("rule")
        if rule_id == "primary_key":
            pk_params = _rule_params(item)
            continue
        if rule_id == "schema":
            continue
        if rule_id not in RULES:
            raise KeyError(f"Unknown validation rule: {rule_id}")
        if rule_id not in _ROW_REASON_RULES:
            raise KeyError(f"Rule {rule_id} is not a row reason rule")
        reasons.extend(RULES[rule_id](df, **_rule_params(item)))

    return reasons, pk_params


def _split_by_reject_reason(
    df: DataFrame,
    reasons: list[Column],
) -> tuple[DataFrame, DataFrame]:
    if not reasons:
        empty_rejects = df.limit(0).withColumn("_reject_reason", F.lit(""))
        return df, empty_rejects

    flagged = df.withColumn("_reject_reason", F.concat_ws(",", *reasons))
    good = flagged.filter(F.col("_reject_reason") == "").drop("_reject_reason")
    rejects = flagged.filter(F.col("_reject_reason") != "")
    return good, rejects


def run_row_checks(
    df: DataFrame,
    config: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None,
) -> ValidationResult:
    """
    Apply row DQ rules then primary_key dedupe (first-row-wins).
    Returns good_df / rejects_df with _reject_reason on rejects.
    """
    reasons, pk_params = _collect_reject_reasons(df, _rule_items(config))
    good, rejects = _split_by_reject_reason(df, reasons)

    if pk_params is not None:
        good, rejects = RULES["primary_key"](good, rejects, **pk_params)

    return ValidationResult(ok=True, good_df=good, rejects_df=rejects)
