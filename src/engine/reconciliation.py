from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Any, Generic, Sequence, TypeVar
from uuid import UUID, uuid4

from pydantic import ValidationError

from .exceptions import DataContractViolationError, VarianceDetectedError
from .logging_config import get_logger
from .persistence import MockPersistenceLayer, PersistenceLayer
from .validation_manifest import ValidationManifest
from models import LedgerEntry, MatchCategory, SourceSystem, Transaction
from strategies.base import MatchPair
from strategies.base import MatchPair
from strategies.factory import StrategyFactory

T = TypeVar("T")


@dataclass(frozen=True)
class ValidationResult(Generic[T]):
    value: T | None
    manifest: ValidationManifest

    @property
    def ok(self) -> bool:
        return self.value is not None and self.manifest.is_valid()


@dataclass(frozen=True)
class CategorizedMatch:
    category: MatchCategory
    pair: MatchPair


@dataclass(frozen=True)
class ReconciliationReport:
    execution_id: UUID
    matched: list[MatchPair]
    timing_differences: list[MatchPair]
    variances: list[Transaction]
    unmatched_ledger_entries: list[LedgerEntry]
    match_rate: Decimal
    sla_breach: bool


class ReconciliationEngine:
    """Production reconciliation pipeline orchestrating exact, aggregate, and fuzzy matching."""

    def __init__(
        self,
        source_system: SourceSystem,
        strict_mode: bool = False,
        sla_time_limit: timedelta = timedelta(hours=4),
        *,
        persistence: PersistenceLayer | None = None,
    ) -> None:
        """
        Initializes the reconciliation engine.

        Args:
            source_system: Source system used to pick tolerance configuration
                (e.g., cards vs crypto).
            strict_mode: If True, raises `VarianceDetectedError` when variances
                are detected.
            sla_time_limit: Maximum allowed posting_date offset for timing
                differences before the report flags an SLA breach.
            persistence: Optional persistence layer used for idempotency.

        Financial logic note:
            - Exact matching is deterministic by ID/amount/currency.
            - Fuzzy matching uses a configured ±24h time window for cards and
              an amount tolerance (cards default: ±0.01).
        """

        self.strict_mode = strict_mode
        self.sla_time_limit = sla_time_limit
        self.exact_strategy = StrategyFactory.create_exact_strategy()
        self.aggregate_strategy = StrategyFactory.create_aggregate_strategy()
        self.fuzzy_strategy = StrategyFactory.create_fuzzy_strategy(source_system)
        self.logger = get_logger()
        self.persistence = persistence or MockPersistenceLayer()

    def _validate_transactions(
        self, raw_transactions: Sequence[dict[str, Any] | Transaction]
    ) -> ValidationResult[list[Transaction]]:
        """
        Validates a batch of transactions against the strict transaction contract.

        Args:
            raw_transactions: Sequence of raw dict payloads or already validated
                `Transaction` models.

        Returns:
            A `ValidationResult` containing either validated models (and a
            manifest with zero quarantines) or `None` with a manifest listing
            quarantined records.
        """

        validated: list[Transaction] = []
        manifest = ValidationManifest()

        for idx, row in enumerate(raw_transactions):
            if isinstance(row, Transaction):
                validated.append(row)
                continue
            try:
                validated.append(Transaction.model_validate(row))
            except ValidationError as exc:
                payload: dict[str, Any]
                if isinstance(row, dict):
                    payload = row
                else:
                    payload = {"value": row}
                manifest.add(record_index=idx, payload=payload, errors=exc.errors())

        if manifest.is_valid():
            return ValidationResult(value=validated, manifest=manifest)
        return ValidationResult(value=None, manifest=manifest)

    def _validate_ledger_entries(
        self, raw_ledger_entries: Sequence[dict[str, Any] | LedgerEntry]
    ) -> list[LedgerEntry]:
        """
        Validates ledger entries against the strict ledger contract.

        Args:
            raw_ledger_entries: Sequence of raw dict payloads or validated
                `LedgerEntry` models.

        Returns:
            Validated ledger entries.

        Raises:
            DataContractViolationError: If ledger validation fails.
        """

        validated: list[LedgerEntry] = []
        for row in raw_ledger_entries:
            if isinstance(row, LedgerEntry):
                validated.append(row)
                continue
            try:
                validated.append(LedgerEntry.model_validate(row))
            except ValidationError as exc:
                # Ledger validation is treated as fail-fast for now; the request
                # explicitly targets _validate_transactions collect-all behavior.
                raise DataContractViolationError(f"Invalid ledger payload: {exc}") from exc
        return validated

    def _categorize(self, pair: MatchPair) -> MatchCategory:
        """
        Categorizes a matched pair as either MATCHED or TIMING_DIFFERENCE.

        Args:
            pair: Matched transaction+ledger pair.

        Returns:
            `TIMING_DIFFERENCE` when amounts match exactly but posting_date
            differs by a non-zero offset; otherwise `MATCHED`.
        """

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
        """
        Runs reconciliation for the provided transaction and ledger batches.

        Args:
            transactions: Raw transaction payload dicts or validated `Transaction`
                models.
            ledger_entries: Raw ledger payload dicts or validated `LedgerEntry`
                models.

        Returns:
            A `ReconciliationReport` with match rate, SLA breach status, matched
            pairs, variances, and unmatched ledger entries.

        Raises:
            DataContractViolationError: When transaction payload validation fails.
            VarianceDetectedError: When strict_mode is enabled and variances exist.
        """

        execution_id = uuid4()

        tx_result = self._validate_transactions(transactions)
        if not tx_result.ok:
            raise DataContractViolationError(
                f"Invalid transaction payloads: {len(tx_result.manifest.quarantined)} rows",
                manifest=tx_result.manifest,
            )

        txns = tx_result.value or []
        ledgers = self._validate_ledger_entries(ledger_entries)

        # Idempotency: skip already matched transaction_ids.
        eligible_txns = [t for t in txns if not self.persistence.is_transaction_matched(t.transaction_id)]
        eligible_txn_ids = {t.transaction_id for t in eligible_txns}

        exact = self.exact_strategy.match(eligible_txns, ledgers)

        aggregate = self.aggregate_strategy.match(exact.unmatched_transactions, exact.unmatched_ledger_entries)

        matched: list[MatchPair] = list(exact.matched_pairs) + list(aggregate.matched_pairs)
        timing_differences: list[MatchPair] = []

        # Continue fuzzy matching only with transactions still eligible after aggregation.
        fuzzy = self.fuzzy_strategy.match(aggregate.unmatched_transactions, aggregate.unmatched_ledger_entries)

        for pair in fuzzy.matched_pairs:
            if self._categorize(pair) == MatchCategory.TIMING_DIFFERENCE:
                timing_differences.append(pair)
            else:
                matched.append(pair)

        variances = list(aggregate.variance_transactions) + list(fuzzy.unmatched_transactions)
        unmatched_ledger_entries = list(fuzzy.unmatched_ledger_entries)

        total_transactions = Decimal(len(eligible_txns))
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

        # Mark idempotency keys for successful reconciliation.
        matched_txn_ids = {p.transaction.transaction_id for p in matched}
        matched_txn_ids |= {p.transaction.transaction_id for p in timing_differences}
        for txn_id in matched_txn_ids:
            if txn_id in eligible_txn_ids:
                self.persistence.mark_transaction_matched(txn_id, execution_id)

        return ReconciliationReport(
            execution_id=execution_id,
            matched=matched,
            timing_differences=timing_differences,
            variances=variances,
            unmatched_ledger_entries=unmatched_ledger_entries,
            match_rate=match_rate,
            sla_breach=sla_breach,
        )

