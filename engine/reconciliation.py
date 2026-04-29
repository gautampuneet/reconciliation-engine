from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Any, Sequence

from pydantic import ValidationError

from engine.exceptions import DataContractViolationError, VarianceDetectedError
from engine.logging_config import get_logger
from models import LedgerEntry, MatchCategory, SourceSystem, Transaction
from strategies.base import MatchPair
from strategies.factory import StrategyFactory


@dataclass(frozen=True)
class CategorizedMatch:
    category: MatchCategory
    pair: MatchPair


@dataclass(frozen=True)
class ReconciliationReport:
    matched: list[MatchPair]
    timing_differences: list[MatchPair]
    variances: list[Transaction]
    unmatched_ledger_entries: list[LedgerEntry]
    match_rate: Decimal
    sla_breach: bool


class ReconciliationEngine:
    def __init__(
        self,
        source_system: SourceSystem,
        strict_mode: bool = False,
        sla_time_limit: timedelta = timedelta(hours=4),
    ) -> None:
        self.strict_mode = strict_mode
        self.sla_time_limit = sla_time_limit
        self.exact_strategy = StrategyFactory.create_exact_strategy()
        self.fuzzy_strategy = StrategyFactory.create_fuzzy_strategy(source_system)
        self.logger = get_logger()

    def _validate_transactions(self, raw_transactions: Sequence[dict[str, Any] | Transaction]) -> list[Transaction]:
        validated: list[Transaction] = []
        for row in raw_transactions:
            if isinstance(row, Transaction):
                validated.append(row)
                continue
            try:
                validated.append(Transaction.model_validate(row))
            except ValidationError as exc:
                raise DataContractViolationError(f"Invalid transaction payload: {exc}") from exc
        return validated

    def _validate_ledger_entries(
        self,
        raw_ledger_entries: Sequence[dict[str, Any] | LedgerEntry],
    ) -> list[LedgerEntry]:
        validated: list[LedgerEntry] = []
        for row in raw_ledger_entries:
            if isinstance(row, LedgerEntry):
                validated.append(row)
                continue
            try:
                validated.append(LedgerEntry.model_validate(row))
            except ValidationError as exc:
                raise DataContractViolationError(f"Invalid ledger payload: {exc}") from exc
        return validated

    def _categorize(self, pair: MatchPair) -> MatchCategory:
        same_amount = pair.transaction.amount == pair.ledger_entry.amount
        time_delta = abs(pair.transaction.posting_date - pair.ledger_entry.posting_date)
        if same_amount and time_delta > timedelta(0):
            return MatchCategory.TIMING_DIFFERENCE
        return MatchCategory.MATCHED

    def run(
        self,
        transactions: Sequence[dict[str, Any] | Transaction],
        ledger_entries: Sequence[dict[str, Any] | LedgerEntry],
    ) -> ReconciliationReport:
        txns = self._validate_transactions(transactions)
        ledgers = self._validate_ledger_entries(ledger_entries)

        exact = self.exact_strategy.match(txns, ledgers)
        fuzzy = self.fuzzy_strategy.match(exact.unmatched_transactions, exact.unmatched_ledger_entries)

        matched: list[MatchPair] = list(exact.matched_pairs)
        timing_differences: list[MatchPair] = []
        for pair in fuzzy.matched_pairs:
            if self._categorize(pair) == MatchCategory.TIMING_DIFFERENCE:
                timing_differences.append(pair)
            else:
                matched.append(pair)

        variances = list(fuzzy.unmatched_transactions)
        unmatched_ledger_entries = list(fuzzy.unmatched_ledger_entries)

        total_transactions = Decimal(len(txns))
        match_count = Decimal(len(matched) + len(timing_differences))
        match_rate = Decimal("0") if total_transactions == 0 else match_count / total_transactions

        sla_breach = any(
            abs(pair.transaction.posting_date - pair.ledger_entry.posting_date) > self.sla_time_limit
            for pair in timing_differences
        )

        self.logger.info(
            "reconciliation_complete",
            extra={
                "match_rate": str(match_rate),
                "sla_breach": sla_breach,
                "matched_count": len(matched),
                "timing_count": len(timing_differences),
                "variance_count": len(variances),
            },
        )

        if self.strict_mode and variances:
            raise VarianceDetectedError(f"Detected {len(variances)} variances during reconciliation")

        return ReconciliationReport(
            matched=matched,
            timing_differences=timing_differences,
            variances=variances,
            unmatched_ledger_entries=unmatched_ledger_entries,
            match_rate=match_rate,
            sla_breach=sla_breach,
        )


# Legacy module wrapper: delegate to the production implementation under `src/`.
from src.engine.reconciliation import ReconciliationEngine as ReconciliationEngine  # noqa: E402
from src.engine.reconciliation import ReconciliationReport as ReconciliationReport  # noqa: E402

