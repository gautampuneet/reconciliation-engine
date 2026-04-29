from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class QuarantinedRecord:
    record_index: int
    payload: dict[str, Any]
    errors: list[dict[str, Any]]

    def to_athena_row(self) -> dict[str, str]:
        # Athena expects flat, typed columns; for flexible storage we keep
        # JSON blobs as string fields.
        payload_json = json.dumps(self.payload, default=str, ensure_ascii=False)
        errors_json = json.dumps(self.errors, default=str, ensure_ascii=False)

        tx_id = self.payload.get("transaction_id")
        return {
            "record_index": str(self.record_index),
            "transaction_id": "" if tx_id is None else str(tx_id),
            "payload_json": payload_json,
            "errors_json": errors_json,
        }


@dataclass
class ValidationManifest:
    quarantined: list[QuarantinedRecord] = field(default_factory=list)

    def add(self, record_index: int, payload: dict[str, Any], errors: list[dict[str, Any]]) -> None:
        self.quarantined.append(QuarantinedRecord(record_index=record_index, payload=payload, errors=errors))

    def is_valid(self) -> bool:
        return not self.quarantined

    def export_quarantined_for_athena(self) -> list[dict[str, str]]:
        """
        Returns rows ready to be written to S3 (e.g. JSON/CSV/Parquet) and queried
        from Athena.
        """

        return [q.to_athena_row() for q in self.quarantined]

