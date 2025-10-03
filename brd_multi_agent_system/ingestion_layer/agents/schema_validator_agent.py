from __future__ import annotations
from typing import Iterable
import pandas as pd

from ..libs.contracts import ValidationResult


class SchemaValidatorAgent:
    """Validates required fields for normalized CSVs."""

    def validate(self, df: pd.DataFrame, required_fields: Iterable[str]) -> ValidationResult:
        errors: list[str] = []
        cols = set(df.columns)
        for req in required_fields:
            if req not in cols:
                errors.append(f"Missing required field: {req}")
        # Basic semantic checks for the task-specified fields
        for name in ["invoice_date", "gst_rate", "state_code"]:
            if name not in cols:
                errors.append(f"Missing required field: {name}")
        success = len(errors) == 0
        return ValidationResult(success=success, errors=errors)
