"""
Legacy module kept for compatibility.

The production source of truth for strict data contracts lives in
`src/models/contracts.py`.
"""

from .contracts import Transaction

__all__ = ["Transaction"]

