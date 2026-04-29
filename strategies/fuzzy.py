from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import Sequence

from models import LedgerEntry, Transaction
from strategies.base import MatchPair, MatchingStrategy, StrategyMatchResult


class FuzzyMatchStrategy(MatchingStrategy):
    def __init__(
        self,
        time_tolerance: timedelta = timedelta(hours=24),
        amount_tolerance: Decimal = Decimal("0.01"),
    ) -> None:
        self.time_tolerance = time_tolerance
        self.amount_tolerance = amount_tolerance

    def _is_within_tolerance(self, transaction: Transaction, ledger: LedgerEntry) -> bool:
        if transaction.currency != ledger.currency:
            return False
        amount_delta = abs(transaction.amount - ledger.amount)
        time_delta = abs(transaction.posting_date - ledger.posting_date)
        return amount_delta <= self.amount_tolerance and time_delta <= self.time_tolerance

    def match(
        self,
        transactions: Sequence[Transaction],
        ledger_entries: Sequence[LedgerEntry],
    ) -> StrategyMatchResult:
        matched_pairs: list[MatchPair] = []
        unmatched_transactions: list[Transaction] = []
        consumed_ledger_ids: set[str] = set()

        sorted_ledger_entries = sorted(ledger_entries, key=lambda entry: entry.posting_date)

        for transaction in transactions:
            candidates = [
                ledger
                for ledger in sorted_ledger_entries
                if ledger.ledger_id not in consumed_ledger_ids and self._is_within_tolerance(transaction, ledger)
            ]
            if not candidates:
                unmatched_transactions.append(transaction)
                continue

            best_match = min(
                candidates,
                key=lambda ledger: (
                    abs(transaction.posting_date - ledger.posting_date),
                    abs(transaction.amount - ledger.amount),
                    ledger.ledger_id,
                ),
            )
            consumed_ledger_ids.add(best_match.ledger_id)
            matched_pairs.append(MatchPair(transaction=transaction, ledger_entry=best_match))

        unmatched_ledger_entries = [
            ledger for ledger in ledger_entries if ledger.ledger_id not in consumed_ledger_ids
        ]
        return StrategyMatchResult(
            matched_pairs=matched_pairs,
            unmatched_transactions=unmatched_transactions,
            unmatched_ledger_entries=unmatched_ledger_entries,
        )
