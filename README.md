# Reconciliation Engine (Python)

Production-oriented repository blueprint implementing core reconciliation logic with strict data contracts, strategy-based matching, and exception-driven remediation.

## Repository Structure

- `models/`: Pydantic v2 data contracts (`Transaction`, `LedgerEntry`) and enums.
- `strategies/`: Strategy Pattern implementation:
  - `MatchingStrategy` abstract base class.
  - `ExactMatchStrategy` (Level 1, hash-map based O(n+m)).
  - `FuzzyMatchStrategy` (Level 2, ±24h and ±0.01 for cards by default).
  - `StrategyFactory` for source-system specific tolerances (cards vs crypto).
- `engine/`: `ReconciliationEngine`, structured logging, custom exceptions.
- `tests/`: pytest edge cases and contract validation tests.

## Data Contracts (TDD Constraints)

Both models validate:

- `transaction_id` (regex): `^[A-Z0-9][A-Z0-9_-]{7,63}$`
- `amount`: `Decimal` with exactly 4 decimal places.
- `currency`: ISO-4217 (`pydantic_extra_types.currency_code.ISO4217`).
- `posting_date`: timezone-aware ISO timestamp, normalized to UTC.

## Reconciliation Flow

1. Validate inbound rows into Pydantic models.
2. **Level 1** exact matching via dictionary lookup for O(n+m) performance.
3. **Level 2** fuzzy matching on unmatched records with:
   - time tolerance (`<= 24h` for cards),
   - amount tolerance (`<= 0.01` for cards),
   - strict currency equality.
4. Categorize results:
   - `Matched`
   - `TimingDifference`
   - `Variance`

## Custom Exceptions

- `DataContractViolationError`: raised when payloads fail model validation.
- `VarianceDetectedError`: raised in strict mode when variances are present.

## Structured Logging

`ReconciliationEngine` logs:

- `match_rate`
- `sla_breach`
- counts for matched, timing differences, and variances

These fields are emitted as structured attributes for observability pipelines.

## AWS Glue Integration

Typical job flow:

1. Glue reads transaction and ledger datasets from S3/Data Catalog partitions.
2. Glue Python transform invokes `ReconciliationEngine.run(...)` per batch.
3. Results are written to S3 in Parquet/JSON for auditability.
4. `DataContractViolationError` rows are redirected to quarantine (DLQ bucket/table).
5. CloudWatch metrics parse structured logs to track `match_rate` and SLA breaches.

## RDS Exception Store Integration

Persist `Variance` (and optionally `TimingDifference`) records into an RDS exception table:

- `exception_id` (idempotent key)
- `transaction_id`
- `ledger_id` (nullable for unmatched transaction)
- `reason_code` (`currency_mismatch`, `amount_out_of_tolerance`, etc.)
- `payload_json`
- `created_at`, `updated_at`

Use idempotent UPSERT semantics so Glue retries do not duplicate exceptions.

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
pytest
```
