
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from pyspark.sql import DataFrame


@dataclass
class ValidationResult:

    ok: bool = True
    errors: list[str] = field(default_factory=list)
    good_df: Optional[DataFrame] = None
    rejects_df: Optional[DataFrame] = None
    stats: dict[str, Any] = field(default_factory=dict)
