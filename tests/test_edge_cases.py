from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from engine.reconciliation import ReconciliationEngine
from models import SourceSystem


def test_amount_at_0_0001_boundary_is_fuzzy_matched() -> None:
    engine = ReconciliationEngine(source_system=SourceSystem.CARDS)
    posting = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)

    # 100.0000 vs 100.0001 has a delta of exactly 0.0001 (still within ±0.01).
    report = engine.run(
        transactions=[
            {
                "transaction_id": "TXN_ROUND_01",
                "account_code": "ACCT_001",
                "amount": Decimal("100.0000"),
                "currency": "USD",
                "posting_date": posting.isoformat(),
            }
        ],
        ledger_entries=[
            {
                "ledger_id": "LEDGER_ROUND_01",
                "transaction_id": "TXN_ROUND_01",
                "account_code": "ACCT_001",
                "amount": Decimal("100.0001"),
                "currency": "USD",
                "posting_date": posting.isoformat(),
            }
        ],
    )

    assert len(report.matched) == 1
    assert len(report.timing_differences) == 0
    assert len(report.variances) == 0


def test_time_matching_at_exact_23_59_59_is_within_fuzzy_window() -> None:
    engine = ReconciliationEngine(source_system=SourceSystem.CARDS)
    base_time = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)

    # Exactly 23:59:59 apart is within the 24h time tolerance.
    report = engine.run(
        transactions=[
            {
                "transaction_id": "TXN_TIME_01",
                "account_code": "ACCT_001",
                "amount": Decimal("100.0000"),
                "currency": "USD",
                "posting_date": base_time.isoformat(),
            }
        ],
        ledger_entries=[
            {
                "ledger_id": "LEDGER_TIME_01",
                "transaction_id": "TXN_TIME_OTHER_01",
                "account_code": "ACCT_001",
                "amount": Decimal("100.0000"),
                "currency": "USD",
                "posting_date": (base_time + timedelta(hours=23, minutes=59, seconds=59)).isoformat(),
            }
        ],
    )

    assert len(report.matched) == 0
    assert len(report.timing_differences) == 1
    assert len(report.variances) == 0


def test_many_to_one_aggregate_variance_when_sum_off_by_0_01() -> None:
    engine = ReconciliationEngine(source_system=SourceSystem.CARDS)
    posting = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)

    report = engine.run(
        transactions=[
            {
                "transaction_id": "TXN_AGG_01",
                "account_code": "ACCT_002",
                "amount": Decimal("60.0000"),
                "currency": "USD",
                "posting_date": posting.isoformat(),
            },
            {
                "transaction_id": "TXN_AGG_02",
                "account_code": "ACCT_002",
                "amount": Decimal("40.0000"),
                "currency": "USD",
                "posting_date": posting.isoformat(),
            },
        ],
        ledger_entries=[
            {
                "ledger_id": "LEDGER_AGG_01",
                "transaction_id": "TXN_LEDGER_PLACEHOLDER",
                "account_code": "ACCT_002",
                # Sum of transactions = 100.0000, ledger total = 100.0100 (off by 0.01)
                "amount": Decimal("100.0100"),
                "currency": "USD",
                "posting_date": posting.isoformat(),
            }
        ],
    )

    assert len(report.matched) == 0
    assert len(report.timing_differences) == 0
    assert len(report.variances) == 2
    assert {t.transaction_id for t in report.variances} == {"TXN_AGG_01", "TXN_AGG_02"}

