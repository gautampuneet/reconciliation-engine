from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Sequence

from models import LedgerEntry, Transaction
from strategies.base import MatchPair, MatchingStrategy, StrategyMatchResult


class ExactMatchStrategy(MatchingStrategy):
    """
    One-to-one exact matching.

    Optimized for O(N) dictionary lookups:
    - Build an index of ledger entries keyed by (transaction_id, currency, account_code, amount)
    - Greedily consume the first available ledger entry for each transaction.
    """

    @staticmethod
    def _build_key(transaction_id: str, currency: str, account_code: str, amount: Decimal) -> tuple[str, str, str, Decimal]:
        return (transaction_id, currency, account_code, amount)

    def match(
        self,
        transactions: Sequence[Transaction],
        ledger_entries: Sequence[LedgerEntry],
    ) -> StrategyMatchResult:
        index: dict[tuple[str, str, str, Decimal], list[LedgerEntry]] = defaultdict(list)
        for ledger in ledger_entries:
            key = self._build_key(ledger.transaction_id, ledger.currency, ledger.account_code, ledger.amount)
            index[key].append(ledger)

        matched_pairs: list[MatchPair] = []
        unmatched_transactions: list[Transaction] = []
        consumed_ledger_ids: set[str] = set()

        for transaction in transactions:
            key = self._build_key(
                transaction.transaction_id,
                transaction.currency,
                transaction.account_code,
                transaction.amount,
            )
            candidates = index.get(key, [])
            match = next((entry for entry in candidates if entry.ledger_id not in consumed_ledger_ids), None)
            if match is None:
                unmatched_transactions.append(transaction)
                continue

            consumed_ledger_ids.add(match.ledger_id)
            matched_pairs.append(MatchPair(transaction=transaction, ledger_entry=match))

        unmatched_ledger_entries = [ledger for ledger in ledger_entries if ledger.ledger_id not in consumed_ledger_ids]
        return StrategyMatchResult(
            matched_pairs=matched_pairs,
            unmatched_transactions=unmatched_transactions,
            unmatched_ledger_entries=unmatched_ledger_entries,
            variance_transactions=[],
        )

