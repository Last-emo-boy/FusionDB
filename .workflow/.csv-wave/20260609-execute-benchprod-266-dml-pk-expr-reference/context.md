# BENCHPROD-266 DML Primary-Key Expression Reference Check

## Goal

Avoid temporary `HashSet` allocation while DML primary-key equality fast paths check whether the value side depends on row columns.

## Implementation

- `src/execution/dml/mod.rs`
  - `primary_key_row_id_from_eq_selection` now calls `expr_has_column_reference` directly instead of extracting columns into a temporary set.
  - Removed the unused `HashSet` import.
- `src/execution/scan/index_plan.rs`
  - Expanded `expr_has_column_reference` to cover expression forms already handled by `extract_columns_from_expr`: `EXTRACT`, arrays, compound field access subscript indexes, `SUBSTRING`, `ANY`, and `ALL`.
  - Existing index-planning column-reference checks now share the broader no-allocation predicate.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_update_primary_key_simple_table_fast_path -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_update_qualified_primary_key_uses_point_lookup -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_update_commuted_primary_key_uses_point_lookup -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_delete_primary_key_without_secondary_index_skips_row_decode -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_delete_qualified_primary_key_without_secondary_index_skips_row_decode -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml test_delete_commuted_primary_key_without_secondary_index_skips_row_decode -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_dml -- --nocapture`
  - Passed: 44/44.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_index_cache -- --nocapture`
  - Passed: 38/38.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was previously full.

## Result

`BENCHPROD-266` is complete. DML primary-key equality fast-path value checks now avoid temporary column-set allocation and use the shared no-allocation column-reference predicate.
