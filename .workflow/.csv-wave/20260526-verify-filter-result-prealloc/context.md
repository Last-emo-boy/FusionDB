# TASK-086 Verification Context

## Environment

- `CARGO_TARGET_DIR=C:\Users\ES&E\AppData\Local\Temp\fusiondb-target`
- `CARGO_BUILD_JOBS=1`
- `RUSTFLAGS=-C debuginfo=0`

## Coverage

- Formatting and library type checking.
- Simple WHERE equality and range filtering.
- AND / OR predicate filtering.
- Join local filtering with indexed right-side probe.
