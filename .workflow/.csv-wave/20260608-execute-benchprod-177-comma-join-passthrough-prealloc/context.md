# BENCHPROD-177 Comma Join Passthrough Preallocation

## Goal

Avoid first-growth allocation for comma join reorder passthrough entries.

## Implementation

- `src/execution/scan/join.rs`
  - Changed `reorder_comma_join_from` `passthrough` from `Vec::new()` to `Vec::with_capacity(from.len())`.
  - Existing relation analysis, ordering, sorting, and fallback behavior remain unchanged.

## Verification

- `cargo test --test sql_join test_comma_join_reorder_preserves_ldbc_q4_shape_with_deferred_exists -- --nocapture`
  - Passed.
- `cargo test --test sql_join test_implicit_join_where_equi_predicate_matches_chbenchmark_q16_shape -- --nocapture`
  - Passed.
- `cargo test --test sql_join`
  - Passed: 31/31.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-177` is complete. Comma join reorder now sizes passthrough storage from the input relation count.
