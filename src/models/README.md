# Data Contracts (Pydantic v2)

This package uses **strict data contracts** as the source of truth. Contracts are validated with Pydantic v2 and must pass **before** any reconciliation logic runs.

## Contract Summary

| Model | Field | Type | Validation Rules |
|---|---|---|---|
| `Transaction` | `transaction_id` | `str` | Regex: `^[A-Z0-9][A-Z0-9_-]{7,63}$` |
| `Transaction` | `account_code` | `str` | Regex: `^[A-Z0-9][A-Z0-9_-]{1,63}$` |
| `Transaction` | `amount` | `Decimal` | **Exactly 4 decimal places** (no silent rounding) |
| `Transaction` | `currency` | `ISO4217` | Valid ISO-4217 3-letter code (e.g., `USD`, `EUR`) |
| `Transaction` | `posting_date` | `datetime` | Must be a timezone-aware ISO-8601 string; normalized to UTC |
| `Transaction` | `metadata` | `dict[str, Any]` | Optional source-system tags |
| `LedgerEntry` | `ledger_id` | `str` | Regex: `^[A-Z0-9][A-Z0-9_-]{7,63}$` |
| `LedgerEntry` | `transaction_id` | `str` | Regex: `^[A-Z0-9][A-Z0-9_-]{7,63}$` |
| `LedgerEntry` | `account_code` | `str` | Regex: `^[A-Z0-9][A-Z0-9_-]{1,63}$` |
| `LedgerEntry` | `amount` | `Decimal` | **Exactly 4 decimal places** (no silent rounding) |
| `LedgerEntry` | `currency` | `ISO4217` | Valid ISO-4217 3-letter code |
| `LedgerEntry` | `posting_date` | `datetime` | Must be timezone-aware ISO-8601; normalized to UTC |

## Valid vs Invalid Payloads

### `Transaction` example (valid)

```json
{
  "transaction_id": "TXN_87654321",
  "account_code": "ACCT_001",
  "amount": "100.0000",
  "currency": "USD",
  "posting_date": "2026-04-01T12:00:00+00:00",
  "metadata": {"source_system": "cards"}
}
```

### `Transaction` example (invalid: amount precision)

This fails because `amount` has **5 decimal places**.

```json
{
  "transaction_id": "TXN_87654321",
  "account_code": "ACCT_001",
  "amount": "100.12345",
  "currency": "USD",
  "posting_date": "2026-04-01T12:00:00+00:00"
}
```

### `Transaction` example (invalid: naive datetime)

This fails because `posting_date` has **no timezone offset**.

```json
{
  "transaction_id": "TXN_87654321",
  "account_code": "ACCT_001",
  "amount": "100.0000",
  "currency": "USD",
  "posting_date": "2026-04-01T12:00:00"
}
```

