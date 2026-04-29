"""
Example: run reconciliation with data quality gate.

This script demonstrates a typical production flow:
1. Validate raw payloads with :class:`engine.DQGate` (strict contracts).
2. Pass only valid records to :class:`engine.ReconciliationEngine`.
3. Print a concise summary of reconciliation outputs.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from engine import DQGate, ReconciliationEngine
from models import LedgerEntry, SourceSystem, Transaction


def _print_dq_manifest(name: str, manifest) -> None:
    """Prints a small DQGate manifest summary."""

    print(f"{name}: total={manifest.total_records} valid={manifest.valid_records}")
    if manifest.quarantined_records:
        print(f"{name}: quarantined={len(manifest.quarantined_records)}")
        for q in manifest.quarantined_records:
            print(f"  - index={q.record_index} error_codes={q.error_codes}")


def main() -> None:
    """
    Runs a deterministic + fuzzy reconciliation example.

    Returns:
        None
    """

    engine = ReconciliationEngine(source_system=SourceSystem.CARDS)
    now = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)

    raw_transactions: list[dict] = [
        {
            "transaction_id": "TXN_EX_001",
            "account_code": "ACCT_001",
            "amount": Decimal("100.0000"),
            "currency": "USD",
            "posting_date": now.isoformat(),
            "metadata": {"source_system": "cards"},
        },
        # Invalid: amount has 5 decimal places
        {
            "transaction_id": "TXN_EX_002",
            "account_code": "ACCT_001",
            "amount": Decimal("100.12345"),
            "currency": "USD",
            "posting_date": now.isoformat(),
        },
    ]

    raw_ledger_entries: list[dict] = [
        {
            "ledger_id": "LEDGER_EX_001",
            "transaction_id": "TXN_EX_OTHER_001",
            "account_code": "ACCT_001",
            "amount": Decimal("100.0000"),
            "currency": "USD",
            "posting_date": now.isoformat(),
        }
    ]

    txn_gate = DQGate(Transaction)
    valid_transactions, txn_manifest = txn_gate.process_batch(raw_transactions)
    _print_dq_manifest("transactions", txn_manifest)

    ledger_gate = DQGate(LedgerEntry)
    valid_ledgers, ledger_manifest = ledger_gate.process_batch(raw_ledger_entries)
    _print_dq_manifest("ledger_entries", ledger_manifest)

    # Reconcile only valid payloads.
    report = engine.run(transactions=valid_transactions, ledger_entries=valid_ledgers)

    print("\nReconciliation summary")
    print(f"- Match rate: {report.match_rate}")
    print(f"- SLA breach: {report.sla_breach}")
    print(f"- Variances: {len(report.variances)}")
    if report.variances:
        print("- Variance transaction_ids:", [t.transaction_id for t in report.variances])


if __name__ == "__main__":
    main()

