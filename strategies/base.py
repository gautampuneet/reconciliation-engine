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
    matched_pairs: list[MatchPair]
    unmatched_transactions: list[Transaction]
    unmatched_ledger_entries: list[LedgerEntry]


class MatchingStrategy(ABC):
    @abstractmethod
    def match(
        self,
        transactions: Sequence[Transaction],
        ledger_entries: Sequence[LedgerEntry],
    ) -> StrategyMatchResult:
        raise NotImplementedError
