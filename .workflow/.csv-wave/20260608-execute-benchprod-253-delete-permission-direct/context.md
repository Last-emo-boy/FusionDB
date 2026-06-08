# BENCHPROD-253 DELETE Permission Direct Construction

## Goal

Avoid iterator and collection overhead for DELETE permission resolution, where the result can contain at most one table permission entry.

## Implementation

- `src/execution/mod.rs`
  - Replaced `table.into_iter().map(...).collect()` in `Executor::statement_permissions` for `Statement::Delete`.
  - The branch now directly returns `vec![(name, "DELETE")]` for a resolved table and `Vec::new()` otherwise.
  - Added a `DELETE FROM users WHERE id = 1` assertion to the existing statement permission unit test.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test statement_permissions_preserve_preallocated_entries -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_delete_with_where -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

## Result

`BENCHPROD-253` is complete. DELETE permission resolution now directly constructs its zero-or-one result without changing DML behavior.
