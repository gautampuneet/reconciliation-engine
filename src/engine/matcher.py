from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Any, Sequence

import pandas as pd

from models import LedgerEntry, MatchCategory, Transaction


@dataclass(frozen=True)
class MatchResult:
    """
    Categorized reconciliation result for demonstration purposes.

    Attributes:
        matched: List of transactions categorized as MATCHED.
        timing_differences: List categorized as TIMING_DIFFERENCE.
        variances: List categorized as VARIANCE.
    """

    matched: list[Transaction]
    timing_differences: list[Transaction]
    variances: list[Transaction]


class ReconciliationEngine:
    """
    Demonstration matching engine for Three Logic Examples (A/B/C).

    This module is intentionally example-focused (not a full reconciliation
    pipeline replacement). It demonstrates how deterministic and fuzzy rules
    can be expressed in pandas via left joins and vectorized comparisons.

    Financial logic note:
        The "24h window" comes from the cards tolerance settings used by the
        production engine. Transactions with the same amount and currency but
        a non-zero posting_date offset within 24 hours are categorized as
        `MatchCategory.TIMING_DIFFERENCE`.
    """

    def __init__(
        self,
        *,
        time_window: timedelta = timedelta(hours=24),
        amount_fuzzy_boundary: Decimal = Decimal("0.01"),
    ) -> None:
        """
        Args:
            time_window: Maximum allowed posting_date offset for timing logic.
            amount_fuzzy_boundary: Amount delta boundary used to flag VARIANCE.
        """

        self.time_window = time_window
        self.amount_fuzzy_boundary = amount_fuzzy_boundary

    def reconcile(
        self,
        transactions: Sequence[dict[str, Any] | Transaction],
        ledger_entries: Sequence[dict[str, Any] | LedgerEntry],
    ) -> MatchResult:
        """
        Reconciles transactions against ledger entries and categorizes results.

        Args:
            transactions: Raw payload dicts or already validated `Transaction` models.
            ledger_entries: Raw payload dicts or already validated `LedgerEntry` models.

        Returns:
            `MatchResult` containing three categorized lists.
        """

        txns: list[Transaction] = [
            t if isinstance(t, Transaction) else Transaction.model_validate(t) for t in transactions
        ]
        ledgers: list[LedgerEntry] = [
            l if isinstance(l, LedgerEntry) else LedgerEntry.model_validate(l) for l in ledger_entries
        ]

        if not txns:
            return MatchResult(matched=[], timing_differences=[], variances=[])
        if not ledgers:
            return MatchResult(matched=[], timing_differences=[], variances=txns)

        tx_df = pd.DataFrame([t.model_dump() for t in txns])
        ld_df = pd.DataFrame([l.model_dump() for l in ledgers])

        # Normalize datetimes for vectorized computations.
        tx_df["posting_date"] = pd.to_datetime(tx_df["posting_date"], utc=True)
        ld_df["posting_date"] = pd.to_datetime(ld_df["posting_date"], utc=True)

        # Example A: Exact match by transaction_id + amount + currency.
        a_join = tx_df.merge(
            ld_df,
            how="inner",
            left_on=["transaction_id", "amount", "currency"],
            right_on=["transaction_id", "amount", "currency"],
            suffixes=("_txn", "_ledger"),
        )
        a_tx_ids = set(a_join["transaction_id"].tolist())

        # Remaining transactions for B/C.
        rem_tx_df = tx_df[~tx_df["transaction_id"].isin(a_tx_ids)].copy()

        # Left join by transaction_id for Examples B and C.
        bc_join = rem_tx_df.merge(
            ld_df,
            how="left",
            left_on=["transaction_id"],
            right_on=["transaction_id"],
            suffixes=("_txn", "_ledger"),
        )

        # Vectorized deltas.
        bc_join["time_delta"] = (bc_join["posting_date_txn"] - bc_join["posting_date_ledger"]).abs()
        bc_join["amount_delta"] = (bc_join["amount_txn"] - bc_join["amount_ledger"]).abs()

        # Example C: transaction_id matches, but currency differs OR amount differs by exactly 0.01.
        # When there is no ledger row, `currency_ledger` is NA; those will be variances.
        currency_diff = bc_join["currency_ledger"].isna() | (bc_join["currency_txn"] != bc_join["currency_ledger"])
        amount_boundary_diff = bc_join["amount_ledger"].notna() & (bc_join["amount_delta"] == self.amount_fuzzy_boundary)
        c_variance_mask = currency_diff | amount_boundary_diff

        # Example B: amount+currency exact, date offset within 24h.
        amount_currency_match = (
            bc_join["amount_ledger"].notna()
            & (bc_join["amount_txn"] == bc_join["amount_ledger"])
            & (bc_join["currency_txn"] == bc_join["currency_ledger"])
        )
        timing_mask = (
            amount_currency_match
            & (bc_join["time_delta"] > pd.Timedelta(seconds=0))
            & (bc_join["time_delta"] <= pd.Timedelta(self.time_window.total_seconds(), unit="s"))
        )

        # Precedence: if Example C applies, categorize as VARIANCE; else if Example B, categorize as TIMING_DIFFERENCE.
        bc_join["category"] = None
        bc_join.loc[c_variance_mask, "category"] = MatchCategory.VARIANCE
        bc_join.loc[~c_variance_mask & timing_mask, "category"] = MatchCategory.TIMING_DIFFERENCE
        bc_join.loc[bc_join["category"].isna(), "category"] = MatchCategory.VARIANCE

        # Map back to transactions.
        by_id: dict[str, Transaction] = {t.transaction_id: t for t in txns}
        matched: list[Transaction] = []
        timing_differences: list[Transaction] = []
        variances: list[Transaction] = []

        for _, row in bc_join.iterrows():
            txn_id = row["transaction_id"]
            cat = row["category"]
            if cat == MatchCategory.TIMING_DIFFERENCE:
                timing_differences.append(by_id[txn_id])
            elif cat == MatchCategory.MATCHED:
                matched.append(by_id[txn_id])
            else:
                variances.append(by_id[txn_id])

        # Include Example A transactions as MATCHED.
        matched.extend([by_id[tid] for tid in a_tx_ids])

        # De-duplicate by transaction_id.
        def _dedupe(ts: list[Transaction]) -> list[Transaction]:
            seen: set[str] = set()
            out: list[Transaction] = []
            for t in ts:
                if t.transaction_id not in seen:
                    seen.add(t.transaction_id)
                    out.append(t)
            return out

        return MatchResult(
            matched=_dedupe(matched),
            timing_differences=_dedupe(timing_differences),
            variances=_dedupe(variances),
        )

