# DECOUPLE-008 Execution Context

## Scope

- `src/execution/expr.rs`
- `src/execution/expr/mod.rs`
- `src/execution/expr/value.rs`
- `src/execution/expr/function.rs`
- `src/execution/expr/pattern.rs`
- `src/execution/expr/subquery.rs`

## Change

- Converted `expr.rs` into the `expr` module directory.
- Kept boolean expression evaluation, aggregate extraction, column extraction, and group expression helpers in `expr/mod.rs`.
- Moved value evaluation/comparison/index-string helpers into `value.rs`.
- Moved SQL function and vector helpers into `function.rs`.
- Moved LIKE/token helpers into `pattern.rs`.
- Moved subquery materialization helpers into `subquery.rs`.

## Resulting Size

- `src/execution/expr/mod.rs`: 469 lines
- `src/execution/expr/value.rs`: 539 lines
- `src/execution/expr/function.rs`: 382 lines
- `src/execution/expr/pattern.rs`: 122 lines
- `src/execution/expr/subquery.rs`: 142 lines
