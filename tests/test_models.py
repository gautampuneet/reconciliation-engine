from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from engine.exceptions import DataContractViolationError
from engine.reconciliation import ReconciliationEngine
from models import SourceSystem


def test_amount_with_wrong_precision_raises_data_contract_violation() -> None:
    engine = ReconciliationEngine(source_system=SourceSystem.CARDS)
    now = datetime.now(timezone.utc).isoformat()

    with pytest.raises(DataContractViolationError):
        engine.run(
            transactions=[
                {
                    "transaction_id": "TXN_PREC_001",
                    "amount": Decimal("100.12345"),
                    "currency": "USD",
                    "posting_date": now,
                }
            ],
            ledger_entries=[],
        )
