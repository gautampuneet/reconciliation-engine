from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_extra_types.currency_code import ISO4217

AMOUNT_QUANTIZER = Decimal("0.0001")
LEDGER_ID_PATTERN = r"^[A-Z0-9][A-Z0-9_-]{7,63}$"


class LedgerEntry(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    ledger_id: str = Field(pattern=LEDGER_ID_PATTERN)
    transaction_id: str = Field(pattern=LEDGER_ID_PATTERN)
    amount: Decimal
    currency: ISO4217
    posting_date: datetime

    @field_validator("amount")
    @classmethod
    def validate_amount_precision(cls, value: Decimal) -> Decimal:
        quantized = value.quantize(AMOUNT_QUANTIZER, rounding=ROUND_HALF_UP)
        if quantized != value:
            raise ValueError("amount must contain exactly 4 decimal places")
        return value

    @field_validator("posting_date")
    @classmethod
    def validate_posting_date_is_tz_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            raise ValueError("posting_date must be timezone-aware ISO timestamp")
        return value.astimezone(timezone.utc)
