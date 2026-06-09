# BENCHPROD-376 Cacheable Aggregate Detection Without Display Allocation

## Objective

Avoid allocating an `ObjectName` display string and uppercase `String` while checking whether aggregate queries are eligible for query-result caching.

## Scope

- `src/execution/mod.rs`

## Change

- Added `execution_object_name_eq_ascii`.
- Added `CacheableAggregateFunction` and `cacheable_aggregate_function_kind`.
- Replaced `func.name.to_string().to_uppercase()` in `is_cacheable_aggregate_function`.
- Added a focused helper test for case-insensitive aggregate names and qualified-name rejection.

The cacheability rules remain unchanged: `COUNT` accepts wildcard or column arguments, while `SUM`, `AVG`, `MIN`, `MAX`, `STRING_AGG`, and `GROUP_CONCAT` require a single column argument without `DISTINCT`.

## Verification

| Command | Result |
| --- | --- |
| `cargo test cacheable_aggregate_function_kind -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_group_aggregate test_execute_sql_group_by_aggregate_cache_invalidates_after_insert -- --nocapture` | passed: 1/1 |
| `cargo test --test sql_group_aggregate -- --nocapture` | passed: 50/50 |
| `cargo fmt --check` | passed |
| `git diff --check` | passed |
| `rg 'let func_name = func\.name\.to_string\(\)\.to_uppercase\(\)' src/execution/mod.rs -n` | no matches |

Cargo verification used `.tmp` under the workspace for `TEMP`/`TMP` and set `CARGO_PROFILE_TEST_DEBUG=0`.

`cargo fmt` applied the project-standard import wrap before the final `cargo fmt --check`.

`git diff --check` printed the existing CRLF normalization warning for the edited Rust file while exiting successfully.
