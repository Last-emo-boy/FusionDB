# BENCHPROD-189 Join Projection Removal Preallocation

## Goal

Avoid implicit growth while building filtered projection lists for join probe key removal.

## Implementation

- `src/execution/scan/join.rs`
  - Replaced collect-based full-schema filtering with `Vec::with_capacity(schema.columns.len().saturating_sub(1))`.
  - Replaced collect-based existing-projection filtering with `Vec::with_capacity(columns.len())`.
  - Column filtering, unresolved-column retention, output order, and projection clone fallback behavior remain unchanged.

## Verification

- `cargo test --test sql_join test_primary_key_join_probe_projection_reuses_right_row_cache -- --nocapture`
  - Passed.
- `cargo test --test sql_join test_join_index_probe_projection_skips_unused_right_column_decode -- --nocapture`
  - Passed.
- `cargo test --test sql_join test_two_hop_join_probe_skips_guaranteed_right_key_decode -- --nocapture`
  - Passed.
- `cargo test --test sql_join`
  - Passed: 31/31.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-189` is complete. Join probe projection removal now sizes filtered projection vectors from known input widths.
