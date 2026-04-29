from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal

from models import SourceSystem
from strategies.exact import ExactMatchStrategy
from strategies.fuzzy import FuzzyMatchStrategy


@dataclass(frozen=True)
class SourceTolerance:
    time_tolerance: timedelta
    amount_tolerance: Decimal


class StrategyFactory:
    _TOLERANCES: dict[SourceSystem, SourceTolerance] = {
        SourceSystem.CARDS: SourceTolerance(
            time_tolerance=timedelta(hours=24),
            amount_tolerance=Decimal("0.01"),
        ),
        SourceSystem.CRYPTO: SourceTolerance(
            time_tolerance=timedelta(hours=48),
            amount_tolerance=Decimal("0.50"),
        ),
    }

    @classmethod
    def create_exact_strategy(cls) -> ExactMatchStrategy:
        return ExactMatchStrategy()

    @classmethod
    def create_fuzzy_strategy(cls, source_system: SourceSystem) -> FuzzyMatchStrategy:
        tolerance = cls._TOLERANCES[source_system]
        return FuzzyMatchStrategy(
            time_tolerance=tolerance.time_tolerance,
            amount_tolerance=tolerance.amount_tolerance,
        )
