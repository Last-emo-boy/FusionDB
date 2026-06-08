# BENCHPROD-174 generate_subscripts Join Preallocation

## Goal

Avoid first-growth allocation for the `generate_subscripts` join specialization output buffer.

## Implementation

- `src/execution/scan/join.rs`
  - Changed `apply_generate_subscripts_join` `joined_rows` from `Vec::new()` to `Vec::with_capacity(left_rows.len().min(4096))`.
  - The capacity is conservative because each left row can expand to multiple generated rows.
  - Existing row generation, row assembly, and projection behavior remain unchanged.

## Verification

- `cargo test --test sql_set_subquery test_generate_subscripts_depends_on_left_row_array -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery test_array_agg_over_generated_subscripts -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery`
  - Passed: 48/48.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-174` is complete. `generate_subscripts` joins now start with a bounded output buffer capacity based on the left input size.
