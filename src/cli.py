from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from decimal import Decimal
from typing import Any
from uuid import UUID

import pandas as pd

from engine import DataContractViolationError, ReconciliationEngine
from engine.reconciliation import ReconciliationReport
from models import SourceSystem, Transaction


def _parse_source_system(value: str) -> SourceSystem:
    """Parses a CLI `--source-system` argument into `SourceSystem`."""
    normalized = value.strip().lower()
    return SourceSystem(normalized)


def _dt_to_iso(value: Any) -> str:
    """JSON serializer for datetimes."""
    return value.isoformat()


def _decimal_to_str(value: Decimal) -> str:
    """JSON serializer for decimals."""
    return str(value)


def _transaction_to_dict(txn: Transaction) -> dict[str, Any]:
    """Serializes a validated `Transaction` model to a JSON-friendly dict."""
    return {
        "transaction_id": txn.transaction_id,
        "account_code": txn.account_code,
        "amount": str(txn.amount),
        "currency": str(txn.currency),
        "posting_date": txn.posting_date.isoformat(),
        "metadata": txn.metadata,
    }


def _ledger_to_dict(ledger: Any) -> dict[str, Any]:
    """Serializes a validated `LedgerEntry` model to a JSON-friendly dict."""
    return {
        "ledger_id": ledger.ledger_id,
        "transaction_id": ledger.transaction_id,
        "account_code": ledger.account_code,
        "amount": str(ledger.amount),
        "currency": str(ledger.currency),
        "posting_date": ledger.posting_date.isoformat(),
    }


def _match_pair_to_dict(pair: Any) -> dict[str, Any]:
    """Serializes a `MatchPair` (transaction+ledger) to a JSON-friendly dict."""
    return {
        "transaction": _transaction_to_dict(pair.transaction),
        "ledger": _ledger_to_dict(pair.ledger_entry),
    }


def _report_to_dict(report: ReconciliationReport) -> dict[str, Any]:
    """Serializes a `ReconciliationReport` to JSON-friendly dict."""
    return {
        "execution_id": str(report.execution_id),
        "match_rate": str(report.match_rate),
        "sla_breach": report.sla_breach,
        "matched": [_match_pair_to_dict(p) for p in report.matched],
        "timing_differences": [_match_pair_to_dict(p) for p in report.timing_differences],
        "variances": [_transaction_to_dict(t) for t in report.variances],
        "unmatched_ledger_entries": [_ledger_to_dict(l) for l in report.unmatched_ledger_entries],
    }


def main(argv: list[str] | None = None) -> None:
    """
    CSV debug CLI for reconciliation.

    Args:
        argv: Optional argv override for testing.

    Returns:
        None. Writes a JSON file to `--output`.

    Raises:
        SystemExit: For CLI argument parsing errors.
    """

    parser = argparse.ArgumentParser(description="Reconciliation Engine CSV debug runner")
    parser.add_argument("--transactions", required=True, help="Path to transactions.csv")
    parser.add_argument("--ledger", required=True, help="Path to ledger.csv")
    parser.add_argument("--output", required=True, help="Path to output JSON file")
    parser.add_argument("--source-system", default="cards", help="SourceSystem (cards|crypto)")
    parser.add_argument("--strict-mode", action="store_true", help="Fail if any variances exist")
    args = parser.parse_args(argv)

    source_system = _parse_source_system(args.source_system)
    engine = ReconciliationEngine(source_system=source_system, strict_mode=args.strict_mode)

    # Read as strings to preserve exact decimal precision (no float rounding).
    tx_df = pd.read_csv(args.transactions, dtype=str).fillna("")
    ledger_df = pd.read_csv(args.ledger, dtype=str).fillna("")

    tx_records: list[dict[str, Any]] = tx_df.to_dict(orient="records")
    ledger_records: list[dict[str, Any]] = ledger_df.to_dict(orient="records")

    try:
        report = engine.run(transactions=tx_records, ledger_entries=ledger_records)
    except DataContractViolationError as exc:
        payload = {
            "error": str(exc),
            "manifest": getattr(exc, "manifest", None).to_dlq_rows()
            if getattr(exc, "manifest", None) is not None
            else None,
        }
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        return

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(_report_to_dict(report), f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    main()

