class DataContractViolationError(Exception):
    """Raised when upstream records violate the data contract."""


class VarianceDetectedError(Exception):
    """Raised when strict reconciliation mode detects true variances."""


# Legacy module wrapper: delegate to the production implementation under `src/`.
from src.engine.exceptions import (  # noqa: E402
    DataContractViolationError as DataContractViolationError,
    VarianceDetectedError as VarianceDetectedError,
)

