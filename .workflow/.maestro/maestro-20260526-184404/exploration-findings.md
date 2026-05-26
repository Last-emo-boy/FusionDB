# FusionDB Large-File Decoupling Findings

## Scope And Constraints

- Scope: database core only.
- Out of scope: `dashboard/`, `ui/`, behavior rewrites before plan approval.
- Build artifacts must stay on E drive via `CARGO_TARGET_DIR=E:\Playground\FusionDB\target`.
- Rust verification should also set `CARGO_PROFILE_TEST_DEBUG=0`.

## Current Large Files

| File | Lines | Role |
| --- | ---: | --- |
| `tests/sql_integration.rs` | 5925 | All SQL integration coverage in one target. |
| `src/execution/query.rs` | 3149 | Query orchestration, column-scan fast paths, projection, ORDER BY, GROUP BY, window functions. |
| `src/execution/scan.rs` | 2605 | Base scans, join execution, index scan planning, row fetch helpers. |
| `src/execution/expr.rs` | 1627 | Expression evaluation, SQL functions, vector helpers, subquery materialization. |
| `src/execution/dml.rs` | 1083 | INSERT, DELETE, UPDATE, UPSERT/RETURNING helpers, constraints/defaults. |
| `src/execution/ddl.rs` | 989 | SHOW/DESCRIBE/EXPLAIN, CREATE/DROP/ALTER table/index/view. |
| `src/execution/mod.rs` | 778 | Executor type, statement dispatch, prepared statement/RBAC custom SQL. |

## Execution Query Boundary

`src/execution/query.rs` currently mixes several separable concerns:

- Column-scan aggregate and distinct fast paths: `ColumnAggregateKind`, `ColumnPredicateScanPlan`, `simple_column_aggregate_scan`, `count_distinct_column_scan`, `distinct_column_scan`, `group_by_count_column_scan`, `group_by_column_aggregate_scan`.
- ORDER BY and sort helpers: `ProjectionOrderValueSource`, `SortOrderValueSource`, `SortOrderKey`, `resolve_order_by_projection_index`, `compare_sort_order_keys`, `sort_rows_by_order_keys`.
- Query orchestration: `handle_query`, CTE materialization/cleanup, `handle_query_inner`.
- General aggregate compilation and projection planning: `GroupAggregatePlan`, `compile_group_key_sources`, `compile_group_aggregate_plans`.
- Window function implementation: `compute_window_function`.

Low-risk module boundaries:

1. `src/execution/query/column_scan.rs`: move private column-scan structs and helpers together. This is mostly self-contained and only depends on `Executor`, `TableSchema`, `Value`, `Transaction`, and `sqlparser::ast`.
2. `src/execution/query/order.rs`: move order-key source enums and sorting helpers. This reduces `handle_query_inner` noise without changing behavior.
3. `src/execution/query/window.rs`: move `compute_window_function` and any local helpers after tests cover window behavior.
4. `src/execution/query/aggregate.rs`: later move general group aggregate plan compilation. This has more coupling and should follow the first two moves.

Preferred migration order:

1. Create `src/execution/query/mod.rs` by moving current `query.rs` contents mechanically, then keep `src/execution/query.rs` absent and let `src/execution/mod.rs` use `mod query;`.
2. Extract `column_scan.rs` using `pub(super)` types/functions only where needed by query orchestration.
3. Extract `order.rs`.
4. Extract `window.rs`.
5. Only then consider `aggregate.rs`, because aggregate plans are more entangled with final projection and HAVING behavior.

## Scan Boundary

`src/execution/scan.rs` also mixes independent concerns:

- Base table/view scan and row-cache hydration: `scan_table_base`, `scan_single_table`.
- Join predicate splitting and local predicate pushdown: `collect_conjunctive_predicates`, `take_relation_predicate`, `take_schema_predicate`.
- Join execution: `execute_join`, `apply_join_step`, `project_join_rows`, stage projection helpers.
- Index candidate planning and row-id mapping: `IndexScanPlan`, `try_index_scan`, `row_id_from_key`, `value_to_primary_row_id`, `should_use_index_plan`.
- Row fetch/decode helpers: `fetch_full_row_by_id`, `fetch_rows_by_join_key`, `decode_row_for_projection`.

Low-risk module boundaries:

1. `src/execution/scan/index_plan.rs`: move `IndexScanPlan` and `try_index_scan` plus row-id helpers after preserving current API as `Executor` methods.
2. `src/execution/scan/join.rs`: move join-only helpers and `execute_join`.
3. `src/execution/scan/table.rs`: move `scan_table_base`, `scan_single_table`, and row fetch/decode helpers.
4. `src/execution/scan/predicate.rs`: move conjunctive predicate splitting and relation/schema predicate filters.

Preferred migration order:

1. Convert `scan.rs` into `scan/mod.rs` with all existing code first. This isolates future path changes from behavior changes.
2. Extract predicate helpers because they are pure and easiest to verify.
3. Extract index planning next; it is mostly private except for `try_index_scan`.
4. Extract join code, keeping method names on `Executor` to avoid query call-site churn.
5. Extract table scan code last, because it is used by both query and join paths.

## Expr / DML / DDL Boundary

`expr.rs` has a clearer extraction shape:

- Scalar expression evaluation: `evaluate_expr`, `evaluate_value`, `evaluate_binary_op`.
- Function registry: `evaluate_function`, `evaluate_arg`, `evaluate_arg_expr`, math/string/date/vector functions.
- Pattern/text helpers: `like_match`, `tokenize`, `tokenize_unique`, `like_fixed_prefix`.
- Value conversion and sorting helpers: `sql_value_to_fusion_value`, `json_value_to_fusion_value`, `compare_for_sort`, `value_to_index_string`.
- Subquery materialization: `materialize_subqueries`, `contains_subquery`.

Recommended modules:

- `src/execution/expr/function.rs`
- `src/execution/expr/pattern.rs`
- `src/execution/expr/subquery.rs`
- `src/execution/expr/value.rs`

`dml.rs` should be split after execution/query/scan because DML tests rely heavily on scan/index/cache behavior:

- `src/execution/dml/insert.rs`
- `src/execution/dml/update.rs`
- `src/execution/dml/delete.rs`
- `src/execution/dml/returning.rs`
- `src/execution/dml/constraints.rs`

`ddl.rs` can be split early because handlers are already grouped by operation:

- `src/execution/ddl/show.rs`
- `src/execution/ddl/explain.rs`
- `src/execution/ddl/index.rs`
- `src/execution/ddl/table.rs`
- `src/execution/ddl/view.rs`

## Test Suite Split

`tests/sql_integration.rs` has 181 async tests. Current topical distribution from line ranges:

| Bucket | Tests |
| --- | ---: |
| `select_scan_order_agg` | 32 |
| `set_subquery_ddl` | 30 |
| `expr_aggregate` | 25 |
| `index_cache_pk` | 17 |
| `returning_upsert_vector_rbac` | 14 |
| `view_constraints_show` | 14 |
| `group_aggregate` | 13 |
| `dml_delete_update` | 10 |
| `join` | 8 |
| `ddl_basic` | 7 |
| `functions_agg` | 7 |
| `window` | 4 |

Low-risk split strategy:

1. Create `tests/sql/common.rs` with `setup`, `exec`, `query`, `exec_ok`, `cleanup`.
2. Create new integration targets one bucket at a time using `#[path = "sql/common.rs"] mod common;`.
3. Move tests without changing test bodies except helper imports.
4. Keep `tests/sql_integration.rs` until each moved bucket passes in its new target, then delete moved tests from the large file.

Suggested test targets:

- `tests/sql_ddl.rs`
- `tests/sql_select.rs`
- `tests/sql_dml.rs`
- `tests/sql_group_aggregate.rs`
- `tests/sql_join.rs`
- `tests/sql_index_cache.rs`
- `tests/sql_set_subquery.rs`
- `tests/sql_expr_functions.rs`
- `tests/sql_window.rs`
- `tests/sql_view_show_constraints.rs`
- `tests/sql_returning_upsert_vector_rbac.rs`

## General Verification Commands

Use these patterns for every extraction TASK:

```powershell
$env:CARGO_TARGET_DIR='E:\Playground\FusionDB\target'
$env:CARGO_PROFILE_TEST_DEBUG='0'
cargo fmt -- src/execution/mod.rs <touched-rust-files> <touched-test-files>
cargo test --lib
cargo test <focused-filter> --test <target>
```

For pure module moves, also use:

```powershell
$env:CARGO_TARGET_DIR='E:\Playground\FusionDB\target'
$env:CARGO_PROFILE_TEST_DEBUG='0'
cargo check --lib
```

## Risk Notes

- Start with mechanical file moves before semantic refactors.
- Preserve `impl Executor` method names to keep call sites stable.
- Prefer `pub(super)` over `pub(crate)` inside new submodules.
- Avoid extracting helpers that require broad lifetime/type changes in the same TASK.
- Split tests before or alongside risky execution refactors so validation filters stay focused.
