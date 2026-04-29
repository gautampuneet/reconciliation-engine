from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Annotated

from pydantic import BaseModel, ConfigDict, Field, BeforeValidator
from pydantic_extra_types.currency_code import ISO4217

AMOUNT_QUANTIZER: Decimal = Decimal("0.0001")

TRANSACTION_ID_PATTERN = r"^[A-Z0-9][A-Z0-9_-]{7,63}$"
LEDGER_ID_PATTERN = r"^[A-Z0-9][A-Z0-9_-]{7,63}$"
ACCOUNT_CODE_PATTERN = r"^[A-Z0-9][A-Z0-9_-]{1,63}$"


def _validate_id_regex(pattern: str, value: str) -> str:
    if not re.fullmatch(pattern, value):
        raise ValueError("id does not match the required regex pattern")
    return value


def validate_transaction_id(value: Any) -> str:
    """
    Validates and normalizes a transaction id.

    Args:
        value: Raw input.

    Returns:
        A validated transaction id string.
    """

    if not isinstance(value, str):
        raise TypeError("transaction_id must be a string")
    return _validate_id_regex(TRANSACTION_ID_PATTERN, value)


def validate_ledger_id(value: Any) -> str:
    """
    Validates and normalizes a ledger id.

    Args:
        value: Raw input.

    Returns:
        A validated ledger id string.
    """

    if not isinstance(value, str):
        raise TypeError("ledger_id must be a string")
    return _validate_id_regex(LEDGER_ID_PATTERN, value)


def validate_account_code(value: Any) -> str:
    """
    Validates an account code.

    Args:
        value: Raw input.

    Returns:
        A validated account code string.
    """

    if not isinstance(value, str):
        raise TypeError("account_code must be a string")
    return _validate_id_regex(ACCOUNT_CODE_PATTERN, value)


def validate_amount_4dp(value: Any) -> Decimal:
    """
    Enforces exact 4-decimal precision for monetary amounts.

    Financial logic note:
        We require the payload to contain exactly 4 decimal places rather than
        silently rounding, so that reconciliation is deterministic and
        auditable.

    Args:
        value: Raw amount input.

    Returns:
        The validated Decimal amount (exact 4dp).
    """

    if isinstance(value, float):
        # Avoid float binary artifacts by converting through string.
        value = str(value)
    if not isinstance(value, Decimal):
        value = Decimal(str(value))

    quantized = value.quantize(AMOUNT_QUANTIZER, rounding=ROUND_HALF_UP)
    if quantized != value:
        raise ValueError("amount must contain exactly 4 decimal places")
    return value


def validate_posting_date_aware(value: Any) -> datetime:
    """
    Enforces timezone-aware posting_date and normalizes it to UTC.

    Args:
        value: Raw posting_date input, either an ISO 8601 string or datetime.

    Returns:
        Timezone-normalized datetime in UTC.
    """

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        # datetime.fromisoformat supports ISO 8601 with offsets in Python 3.11+.
        dt = datetime.fromisoformat(value)
    else:
        raise TypeError("posting_date must be an ISO 8601 string or datetime")

    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        raise ValueError("posting_date must be timezone-aware ISO 8601 string")
    return dt.astimezone(timezone.utc)


class Transaction(BaseModel):
    """
    Transaction data contract (source of truth).

    This contract is designed to be used in AWS Glue pipelines where raw rows
    arrive as dictionaries (e.g., from Spark or a CSV/Parquet reader).
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    transaction_id: Annotated[str, BeforeValidator(validate_transaction_id)] = Field()
    account_code: Annotated[str, BeforeValidator(validate_account_code)] = Field()

    amount: Annotated[Decimal, BeforeValidator(validate_amount_4dp)] = Field()
    currency: ISO4217
    posting_date: Annotated[datetime, BeforeValidator(validate_posting_date_aware)] = Field()

    metadata: dict[str, Any] = Field(default_factory=dict)


class LedgerEntry(BaseModel):
    """
    Ledger entry data contract.

    Aggregate matching assumes ledger entries represent totals for groups
    keyed by (posting_date, account_code, currency).
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    ledger_id: Annotated[str, BeforeValidator(validate_ledger_id)] = Field()
    transaction_id: Annotated[str, BeforeValidator(validate_transaction_id)] = Field()
    account_code: Annotated[str, BeforeValidator(validate_account_code)] = Field()

    amount: Annotated[Decimal, BeforeValidator(validate_amount_4dp)] = Field()
    currency: ISO4217
    posting_date: Annotated[datetime, BeforeValidator(validate_posting_date_aware)] = Field()

