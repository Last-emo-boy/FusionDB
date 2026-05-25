# TASK-088 Verification Context

## Environment

- `CARGO_TARGET_DIR=C:\Users\ES&E\AppData\Local\Temp\fusiondb-target`
- `CARGO_BUILD_JOBS=1`
- `RUSTFLAGS=-C debuginfo=0`

## Coverage

- Formatting and library type checking.
- Primary-key `IN (...)` selection.
- Secondary BTree index `IN (...)` selection with `ORDER BY`.
