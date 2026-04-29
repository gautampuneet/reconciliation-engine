from __future__ import annotations

from typing import Any


class DataContractViolationError(Exception):
    """Raised when upstream records violate the data contract."""

    def __init__(self, message: str, *, manifest: Any | None = None) -> None:
        super().__init__(message)
        self.manifest = manifest


class VarianceDetectedError(Exception):
    """Raised when strict reconciliation mode detects true variances."""

