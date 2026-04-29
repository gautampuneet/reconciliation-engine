from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Generic, Sequence, TypeVar

from pydantic import BaseModel, ValidationError

T = TypeVar("T", bound=BaseModel)


@dataclass(frozen=True)
class QuarantinedRecord:
    """
    Represents a single payload quarantined due to contract violations.

    Attributes:
        record_index: Index of the record within the input batch.
        payload: Original payload dictionary (as received).
        error_codes: Machine-readable codes describing why validation failed.
    """

    record_index: int
    payload: dict[str, Any]
    error_codes: list[str]

    def to_dlq_json(self) -> dict[str, Any]:
        """Converts the record into a JSON-serializable dict for S3/Athena DLQ."""
        return {
            "record_index": self.record_index,
            "payload_json": json.dumps(self.payload, default=str, ensure_ascii=False),
            "error_codes": self.error_codes,
        }


@dataclass(frozen=True)
class DQManifest:
    """
    Data Quality manifest output from the DQGate.

    This object is designed to be logged and persisted alongside DLQ records.

    Attributes:
        total_records: Number of records observed in the batch.
        valid_records: Number of records that passed strict validation.
        quarantined_records: Detailed list of records that failed validation.
    """

    total_records: int
    valid_records: int
    quarantined_records: list[QuarantinedRecord] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """Returns True when no records are quarantined."""
        return len(self.quarantined_records) == 0

    def to_dlq_rows(self) -> list[dict[str, Any]]:
        """
        Returns DLQ rows suitable for writing to S3 (e.g., JSON/CSV/Parquet).

        Note:
            Athena can query over semi-structured DLQ files when payloads and
            error codes are stored in JSON columns.
        """

        return [r.to_dlq_json() for r in self.quarantined_records]


class DQGate(Generic[T]):
    """
    Data contract validation gate (strict) for AWS Glue batch pipelines.

    Integration context:
        In an AWS Glue job, raw inputs typically arrive as dictionaries
        (e.g., from parsing CSV/JSON, reading from S3, or converting Spark
        DataFrames to Python records). This gate validates each record against
        a Pydantic v2 model and produces a manifest.

        Quarantined records should be written to an S3 Dead Letter prefix,
        for example:
            s3://<bucket>/<dlq_prefix>/transactions/dt=YYYY-MM-DD/batch=<execution_id>.json

    Financial logic:
        This gate enforces deterministic precision rules so reconciliation
        behavior is auditable and consistent across retries.
    """

    def __init__(self, model_cls: type[T]) -> None:
        """
        Args:
            model_cls: Pydantic v2 model class used as the contract source of truth.
        """

        self.model_cls = model_cls

    def process_batch(self, raw_records: Sequence[dict[str, Any]]) -> tuple[list[T], DQManifest]:
        """
        Validates a batch of raw payloads.

        Args:
            raw_records: Sequence of raw dictionaries.

        Returns:
            A tuple of (valid_models, manifest).
        """

        valid: list[T] = []
        quarantined: list[QuarantinedRecord] = []

        for idx, payload in enumerate(raw_records):
            try:
                model = self.model_cls.model_validate(payload)
                valid.append(model)
            except ValidationError as exc:
                quarantined.append(
                    QuarantinedRecord(
                        record_index=idx,
                        payload=payload,
                        error_codes=self._extract_error_codes(exc),
                    )
                )

        manifest = DQManifest(
            total_records=len(raw_records),
            valid_records=len(valid),
            quarantined_records=quarantined,
        )
        return valid, manifest

    def _extract_error_codes(self, exc: ValidationError) -> list[str]:
        """
        Extracts a stable set of error codes from a Pydantic ValidationError.

        Args:
            exc: Pydantic ValidationError.

        Returns:
            A list of error code strings.
        """

        codes: set[str] = set()
        for err in exc.errors():
            loc = err.get("loc", [])
            field = str(loc[0]) if loc else ""
            msg = str(err.get("msg", "")).lower()

            if field in {"transaction_id", "ledger_id"} or "id does not match" in msg:
                codes.add("invalid_id")
            elif field == "amount" or "decimal places" in msg or "4 decimal" in msg:
                codes.add("amount_precision_mismatch")
            elif field == "currency":
                codes.add("invalid_currency")
            elif field == "posting_date":
                codes.add("invalid_posting_date")
            elif field == "account_code":
                codes.add("invalid_account_code")
            else:
                codes.add("contract_violation")
        return sorted(codes)

