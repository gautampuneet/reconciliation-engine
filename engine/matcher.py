"""
Legacy wrapper for the demo matcher.

The production reconciliation pipeline lives under `src/engine/`.
This wrapper exists so imports like `import engine.matcher` work when the
repository root is on `PYTHONPATH`.
"""

from src.engine.matcher import MatchResult, ReconciliationEngine

__all__ = ["MatchResult", "ReconciliationEngine"]

