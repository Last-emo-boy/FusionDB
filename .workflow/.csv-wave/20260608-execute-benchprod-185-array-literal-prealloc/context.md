# BENCHPROD-185 Array Literal Preallocation

## Goal

Avoid growth reallocations while evaluating SQL array literals.

## Implementation

- `src/execution/expr/value.rs`
  - Changed `Expr::Array` evaluation to initialize its values vector with `Vec::with_capacity(arr.elem.len())`.
  - Element evaluation order, error propagation, and `Value::Array` contents remain unchanged.

## Verification

- `cargo test --test sql_set_subquery test_generate_subscripts_from_array_literal -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery test_quantified_array_comparison_predicates -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery test_array_agg_over_array_expression_preserves_nested_values -- --nocapture`
  - Passed.
- `cargo test --test sql_set_subquery`
  - Passed: 48/48.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-185` is complete. SQL array literal evaluation now sizes the output vector from the known literal element count.
