# BENCHPROD-180 OR Branch Preallocation

## Goal

Avoid repeated growth of the temporary OR branch vector used before extracting common conjunctive predicates.

## Implementation

- `src/execution/scan/predicate.rs`
  - Added `disjunctive_predicate_count` to mirror the existing `conjunctive_predicate_count` helper.
  - `extract_common_or_conjunctive_predicates` now initializes the OR branch vector with `Vec::with_capacity(Self::disjunctive_predicate_count(expr))`.
  - Nested expression handling, OR flattening, common predicate lifting, and residual recombination remain unchanged.

## Verification

- `cargo test --lib collect_conjunctive_predicates_lifts_common_or_join_key`
  - Passed.
- `cargo test --test sql_join test_or_branch_common_join_key_matches_chbenchmark_q19_shape -- --nocapture`
  - Passed.
- `cargo test --test sql_join test_implicit_join_common_or_equi_predicate_matches_chbenchmark_q19_shape -- --nocapture`
  - Passed.
- `cargo test --test sql_join`
  - Passed: 31/31.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-180` is complete. OR predicate branch collection now uses the known disjunctive branch count as its initial vector capacity.
