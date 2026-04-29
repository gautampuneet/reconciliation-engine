from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from engine.reconciliation import ReconciliationEngine
from models import SourceSystem


def test_many_to_one_batch_settlement() -> None:
    """
    Verifies many-to-one aggregate settlement reconciliation.

    Scenario:
        - Three card transactions for the same posting_date + account_code:
            $20, $30, $50 (sum = $100)
        - One ERP ledger entry containing the aggregated total:
            $100

    Expected behavior:
        - The engine aggregates transactions and matches them to the single
          ledger entry with a 100% match (no variances).
    """

    engine = ReconciliationEngine(source_system=SourceSystem.CARDS)
    posting = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)

    report = engine.run(
        transactions=[
            {
                "transaction_id": "TXN_SET_001",
                "account_code": "ACCT_010",
                "amount": Decimal("20.0000"),
                "currency": "USD",
                "posting_date": posting.isoformat(),
                "metadata": {"source_system": "card"},
            },
            {
                "transaction_id": "TXN_SET_002",
                "account_code": "ACCT_010",
                "amount": Decimal("30.0000"),
                "currency": "USD",
                "posting_date": posting.isoformat(),
                "metadata": {"source_system": "card"},
            },
            {
                "transaction_id": "TXN_SET_003",
                "account_code": "ACCT_010",
                "amount": Decimal("50.0000"),
                "currency": "USD",
                "posting_date": posting.isoformat(),
                "metadata": {"source_system": "card"},
            },
        ],
        ledger_entries=[
            {
                "ledger_id": "LEDGER_ERP_001",
                "transaction_id": "TXN_AGG_ERP_001",
                "account_code": "ACCT_010",
                "amount": Decimal("100.0000"),
                "currency": "USD",
                "posting_date": posting.isoformat(),
            }
        ],
    )

    assert len(report.matched) == 3
    assert len(report.timing_differences) == 0
    assert len(report.variances) == 0
    assert report.match_rate == Decimal("1")

