class DataContractViolationError(Exception):
    """Raised when upstream records violate the data contract."""


class VarianceDetectedError(Exception):
    """Raised when strict reconciliation mode detects true variances."""
