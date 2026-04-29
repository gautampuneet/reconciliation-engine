from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Sequence

import numpy as np
import pandas as pd

from models import LedgerEntry, Transaction
from strategies.base import MatchPair, MatchingStrategy, StrategyMatchResult


AMOUNT_SCALE = Decimal("10000")  # 0.0001 precision


def _decimal_to_int4(value: Decimal) -> int:
    # Amounts are already quantized to 4 decimal places at model level; this is
    # the matching-time representation.
    return int((value * AMOUNT_SCALE).to_integral_value(rounding=ROUND_HALF_UP))


@dataclass(frozen=True)
class _CurrencyGroup:
    # Sorted ledger arrays for efficient window selection.
    posting_us: np.ndarray
    amount_int: np.ndarray
    ledger_id: np.ndarray
    ledger_idx_to_obj: list[LedgerEntry]
    consumed_mask: np.ndarray


class FuzzyMatchStrategy(MatchingStrategy):
    """
    Vectorized fuzzy matching.

    Avoids generating an O(N^2) candidate list by using time-window slicing
    over a sorted ledger posting_date index, then filtering by:
    - currency equality
    - amount delta within tolerance
    - time delta within tolerance
    """

    def __init__(
        self,
        time_tolerance: timedelta = timedelta(hours=24),
        amount_tolerance: Decimal = Decimal("0.01"),
    ) -> None:
        self.time_tolerance = time_tolerance
        self.amount_tolerance = amount_tolerance

        # Pandas may represent timezone-aware datetimes at microsecond resolution
        # (datetime64[us, UTC]) depending on the environment. To keep window logic
        # consistent and avoid unit conversion issues, we operate in microseconds.
        self._time_tol_us = int(self.time_tolerance.total_seconds() * 1_000_000)
        self._amount_tol_int = _decimal_to_int4(self.amount_tolerance.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))

    def _build_currency_groups(self, ledger_entries: Sequence[LedgerEntry]) -> dict[str, _CurrencyGroup]:
        if not ledger_entries:
            return {}

        # Build and sort per currency. This minimizes cross-currency filtering.
        grouped: dict[str, list[LedgerEntry]] = defaultdict(list)
        for ledger in ledger_entries:
            grouped[str(ledger.currency)].append(ledger)

        out: dict[str, _CurrencyGroup] = {}
        for currency, entries in grouped.items():
            df = pd.DataFrame(
                {
                    "posting_date": [e.posting_date for e in entries],
                    "amount": [e.amount for e in entries],
                    "ledger_id": [e.ledger_id for e in entries],
                }
            )

            df["posting_date"] = pd.to_datetime(df["posting_date"], utc=True)
            # For tz-aware datetimes, Pandas often stores datetime64[us, UTC].
            # `.values.view("int64")` gives microseconds since epoch.
            posting_us = df["posting_date"].values.view("int64")
            amount_int = np.array([_decimal_to_int4(x) for x in df["amount"].to_list()], dtype=np.int64)
            ledger_id = df["ledger_id"].to_numpy(dtype=object)

            order = np.argsort(posting_us, kind="mergesort")
            posting_us = posting_us[order]
            amount_int = amount_int[order]
            ledger_id = ledger_id[order]
            ledger_idx_to_obj = [entries[i] for i in np.array(order, dtype=int)]

            out[currency] = _CurrencyGroup(
                posting_us=posting_us,
                amount_int=amount_int,
                ledger_id=ledger_id,
                ledger_idx_to_obj=ledger_idx_to_obj,
                consumed_mask=np.zeros(len(entries), dtype=bool),
            )

        return out

    def match(
        self,
        transactions: Sequence[Transaction],
        ledger_entries: Sequence[LedgerEntry],
    ) -> StrategyMatchResult:
        matched_pairs: list[MatchPair] = []
        unmatched_transactions: list[Transaction] = []

        currency_groups = self._build_currency_groups(ledger_entries)

        transactions_by_currency: dict[str, list[tuple[int, Transaction]]] = defaultdict(list)
        for i, txn in enumerate(transactions):
            transactions_by_currency[str(txn.currency)].append((i, txn))

        for currency, txns in transactions_by_currency.items():
            group = currency_groups.get(currency)
            if group is None:
                unmatched_transactions.extend([t for _, t in txns])
                continue

            for _, txn in txns:
                # `posting_date` is already tz-aware (normalized to UTC by the model).
                txn_time_us = int(pd.Timestamp(txn.posting_date).value // 1_000)
                txn_amount_int = _decimal_to_int4(txn.amount)

                # Candidate slice by time window (inclusive).
                left_time = txn_time_us - self._time_tol_us
                right_time = txn_time_us + self._time_tol_us
                start = int(group.posting_us.searchsorted(left_time, side="left"))
                end = int(group.posting_us.searchsorted(right_time, side="right"))

                if start >= end:
                    unmatched_transactions.append(txn)
                    continue

                candidate_pos = np.arange(start, end, dtype=np.int64)
                candidate_pos = candidate_pos[~group.consumed_mask[candidate_pos]]
                if candidate_pos.size == 0:
                    unmatched_transactions.append(txn)
                    continue

                time_delta = np.abs(group.posting_us[candidate_pos] - txn_time_us)
                amount_delta = np.abs(group.amount_int[candidate_pos] - txn_amount_int)
                within = amount_delta <= self._amount_tol_int
                candidate_pos = candidate_pos[within]

                if candidate_pos.size == 0:
                    unmatched_transactions.append(txn)
                    continue

                # Pick best: smallest time delta, then smallest amount delta, then ledger_id.
                min_time = int(time_delta[within].min())
                pos_time = candidate_pos[time_delta[within] == min_time]
                amount_delta_time = amount_delta[within][time_delta[within] == min_time]
                min_amt = int(amount_delta_time.min())
                pos_amt = pos_time[amount_delta_time == min_amt]

                best_pos = min(int(p) for p in pos_amt)  # deterministic fallback
                # If there are multiple ties, ledger_id lexicographic order breaks ties.
                if pos_amt.size > 1:
                    best_pos = min(pos_amt.tolist(), key=lambda p: str(group.ledger_id[int(p)]))

                group.consumed_mask[int(best_pos)] = True
                ledger_obj = group.ledger_idx_to_obj[int(best_pos)]
                matched_pairs.append(MatchPair(transaction=txn, ledger_entry=ledger_obj))

        # Collect unmatched ledger entries (across currencies).
        unmatched_ledger_entries: list[LedgerEntry] = []
        for group in currency_groups.values():
            for consumed, ledger_obj in zip(group.consumed_mask.tolist(), group.ledger_idx_to_obj):
                if not consumed:
                    unmatched_ledger_entries.append(ledger_obj)

        return StrategyMatchResult(
            matched_pairs=matched_pairs,
            unmatched_transactions=unmatched_transactions,
            unmatched_ledger_entries=unmatched_ledger_entries,
            variance_transactions=[],
        )

