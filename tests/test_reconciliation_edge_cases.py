from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from models import SourceSystem
from engine.reconciliation import ReconciliationEngine


def test_match_exactly_at_24_hour_limit_is_timing_difference() -> None:
    engine = ReconciliationEngine(source_system=SourceSystem.CARDS)
    base_time = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)

    report = engine.run(
        transactions=[
            {
                "transaction_id": "TXN_12345678",
                "amount": Decimal("100.0000"),
                "currency": "USD",
                "account_code": "ACCT_001",
                "posting_date": base_time.isoformat(),
            }
        ],
        ledger_entries=[
            {
                "ledger_id": "LEDGER_12345",
                "transaction_id": "TXN_OTHER_1",
                "amount": Decimal("100.0000"),
                "currency": "USD",
                "account_code": "ACCT_001",
                "posting_date": (base_time + timedelta(hours=24)).isoformat(),
            }
        ],
    )

    assert len(report.timing_differences) == 1
    assert len(report.variances) == 0


def test_amount_difference_exactly_point_zero_one_is_fuzzy_match() -> None:
    engine = ReconciliationEngine(source_system=SourceSystem.CARDS)
    base_time = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)

    report = engine.run(
        transactions=[
            {
                "transaction_id": "TXN_87654321",
                "amount": Decimal("100.0000"),
                "currency": "USD",
                "account_code": "ACCT_001",
                "posting_date": base_time.isoformat(),
            }
        ],
        ledger_entries=[
            {
                "ledger_id": "LEDGER_98765",
                "transaction_id": "TXN_OTHER_2",
                "amount": Decimal("100.0100"),
                "currency": "USD",
                "account_code": "ACCT_001",
                "posting_date": (base_time + timedelta(hours=1)).isoformat(),
            }
        ],
    )

    assert len(report.matched) == 1
    assert len(report.timing_differences) == 0
    assert len(report.variances) == 0


def test_currency_mismatch_is_true_variance() -> None:
    engine = ReconciliationEngine(source_system=SourceSystem.CARDS)
    base_time = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)

    report = engine.run(
        transactions=[
            {
                "transaction_id": "TXN_CURR_01",
                "amount": Decimal("250.0000"),
                "currency": "USD",
                "account_code": "ACCT_001",
                "posting_date": base_time.isoformat(),
            }
        ],
        ledger_entries=[
            {
                "ledger_id": "LEDGER_CURR1",
                "transaction_id": "TXN_OTHER_3",
                "amount": Decimal("250.0000"),
                "currency": "EUR",
                "account_code": "ACCT_001",
                "posting_date": base_time.isoformat(),
            }
        ],
    )

    assert len(report.variances) == 1
    assert report.variances[0].transaction_id == "TXN_CURR_01"
