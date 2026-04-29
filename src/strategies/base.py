from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Sequence

from models import LedgerEntry, Transaction


@dataclass(frozen=True)
class MatchPair:
    transaction: Transaction
    ledger_entry: LedgerEntry


@dataclass(frozen=True)
class StrategyMatchResult:
    """
    Result of applying a matching strategy.

    `variance_transactions` is used by aggregate strategies to indicate groups
    that partially align but do not reconcile exactly (e.g. sums off by > tolerance).
    """

    matched_pairs: list[MatchPair]
    unmatched_transactions: list[Transaction]
    unmatched_ledger_entries: list[LedgerEntry]
    variance_transactions: list[Transaction]


class MatchingStrategy(ABC):
    @abstractmethod
    def match(
        self,
        transactions: Sequence[Transaction],
        ledger_entries: Sequence[LedgerEntry],
    ) -> StrategyMatchResult:
        raise NotImplementedError

