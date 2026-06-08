# BENCHPROD-179 LIKE Prefix Row Id Preallocation

## Goal

Avoid empty `HashSet` initialization plus later reserve for `LIKE 'prefix%'` index row ids.

## Implementation

- `src/execution/scan/index_plan.rs`
  - In the fixed-prefix `LIKE` branch, each primary/secondary index scan now creates its row id set with `HashSet::with_capacity(kv.len())`.
  - Empty prefix results still fall through to later fallback handling.
  - Trigram fallback, exactness, and row id extraction behavior remain unchanged.

## Verification

- `cargo test --test sql_expr_functions test_like_pattern -- --nocapture`
  - Passed.
- `cargo test --test sql_expr_functions test_like_full_patterns -- --nocapture`
  - Passed.
- `cargo test --test sql_join test_or_branch_common_join_key_matches_chbenchmark_q19_shape -- --nocapture`
  - Passed.
- `cargo test --test sql_index_cache`
  - Passed: 37/37.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-179` is complete. `LIKE` fixed-prefix index plans now size row id sets directly from returned prefix scan entries.
