from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Set
from uuid import UUID


class PersistenceLayer(ABC):
    @abstractmethod
    def is_transaction_matched(self, transaction_id: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def mark_transaction_matched(self, transaction_id: str, execution_id: UUID) -> None:
        raise NotImplementedError


@dataclass
class MockPersistenceLayer(PersistenceLayer):
    """
    In-memory persistence mock.

    Simulates an RDS-backed idempotency check by storing matched
    transaction_ids in process memory.
    """

    already_matched_transaction_ids: Set[str] = field(default_factory=set)

    def is_transaction_matched(self, transaction_id: str) -> bool:
        return transaction_id in self.already_matched_transaction_ids

    def mark_transaction_matched(self, transaction_id: str, execution_id: UUID) -> None:
        # execution_id is retained for interface completeness; in-memory mock
        # does not store it.
        _ = execution_id
        self.already_matched_transaction_ids.add(transaction_id)

