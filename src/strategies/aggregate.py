from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Sequence

from models import LedgerEntry, Transaction
from strategies.base import MatchPair, MatchingStrategy, StrategyMatchResult


AMOUNT_SCALE = Decimal("10000")  # 0.0001


def _decimal_to_int4(value: Decimal) -> int:
    return int((value * AMOUNT_SCALE).to_integral_value(rounding=ROUND_HALF_UP))


@dataclass(frozen=True)
class _GroupKey:
    posting_date: object  # datetime (hashable)
    account_code: str
    currency: str


class AggregateMatchStrategy(MatchingStrategy):
    """
    Many-to-one aggregate matching.

    For each (posting_date, account_code, currency) group, sum transaction
    amounts and compare to the corresponding ledger totals.

    If the sums reconcile (within tolerance), all transactions in the group
    are marked as matched and all ledger entries for the group are consumed.

    If the sums do not reconcile, transactions are flagged as
    `variance_transactions`.
    """

    def __init__(self, amount_tolerance: Decimal = Decimal("0")) -> None:
        self.amount_tolerance = amount_tolerance
        self._amount_tol_int = _decimal_to_int4(self.amount_tolerance.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))

    @staticmethod
    def _key(txn: Transaction | LedgerEntry) -> _GroupKey:
        return _GroupKey(
            posting_date=txn.posting_date,
            account_code=txn.account_code,
            currency=str(txn.currency),
        )

    def match(
        self,
        transactions: Sequence[Transaction],
        ledger_entries: Sequence[LedgerEntry],
    ) -> StrategyMatchResult:
        matched_pairs: list[MatchPair] = []
        variance_transactions: list[Transaction] = []
        consumed_ledger_ids: set[str] = set()
        matched_txn_ids: set[str] = set()
        variance_txn_ids: set[str] = set()

        ledger_by_key: dict[_GroupKey, list[LedgerEntry]] = defaultdict(list)
        for ledger in ledger_entries:
            ledger_by_key[self._key(ledger)].append(ledger)

        txns_by_key: dict[_GroupKey, list[Transaction]] = defaultdict(list)
        for txn in transactions:
            txns_by_key[self._key(txn)].append(txn)

        # First pass: reconcile groups that have corresponding ledger totals.
        for key, txns in txns_by_key.items():
            ledger_group = ledger_by_key.get(key)
            if not ledger_group:
                continue

            # Many-to-one settlements imply aggregating multiple transactions.
            # For groups that contain only a single transaction, we skip the
            # aggregate reconciliation so the engine can apply exact/fuzzy
            # one-to-one logic instead of labeling it as a variance.
            if len(txns) < 2:
                continue

            sum_txn_int = sum(_decimal_to_int4(t.amount) for t in txns)
            sum_ledger_int = sum(_decimal_to_int4(l.amount) for l in ledger_group)
            if abs(sum_txn_int - sum_ledger_int) <= self._amount_tol_int:
                ledger_entry_for_pair = ledger_group[0]
                for txn in txns:
                    matched_pairs.append(MatchPair(transaction=txn, ledger_entry=ledger_entry_for_pair))
                    matched_txn_ids.add(txn.transaction_id)
                for l in ledger_group:
                    consumed_ledger_ids.add(l.ledger_id)
            else:
                variance_transactions.extend(txns)
                for txn in txns:
                    variance_txn_ids.add(txn.transaction_id)

        unmatched_transactions: list[Transaction] = [
            txn for txn in transactions if txn.transaction_id not in matched_txn_ids and txn.transaction_id not in variance_txn_ids
        ]

        unmatched_ledger_entries: list[LedgerEntry] = [l for l in ledger_entries if l.ledger_id not in consumed_ledger_ids]

        return StrategyMatchResult(
            matched_pairs=matched_pairs,
            unmatched_transactions=unmatched_transactions,
            unmatched_ledger_entries=unmatched_ledger_entries,
            variance_transactions=variance_transactions,
        )

