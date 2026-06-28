# BENCHPROD-438 Execution Context

## Outcome
Phase 9 perf iteration (workflow-implemented in an isolated worktree, then integrated). Targets the
worst LARGE hot spot, `IN list` (358ms vs 242ms for `=`).

## Implementation
- `src/execution/expr/mod.rs` InList branch: previously called `align_comparison_values(expr, val.clone(), item, ...)`
  once PER LIST ITEM PER ROW, which re-ran `comparison_column_type` -> `resolve_column_index` (a linear
  schema scan) for every item and cloned `val` each time, while the loop already discarded align's coerced
  LEFT value and compared the original `val`. Now resolves the comparison column's data type ONCE per row
  (`comparison_column_type`, made `pub(crate)`), then coerces each item via `coerce_value_to_column_type`.
  Falls back to the original per-item alignment when the LHS has no column type, preserving prior behavior.
- `src/execution/expr/value.rs`: dropped the per-row `ident.value.clone()` in the `Expr::Identifier` eval
  (`resolve_column_index` takes `&str`).

## Verification
- `cargo fmt --check`, `git diff --check`, `cargo check --bins` passed.
- `cargo test --test sql_in_list` (3 new tests) passed; `cargo test --lib` passed.
- Result-preserving: the original `val` is what is compared; only redundant per-item column resolution
  and clones were removed.
