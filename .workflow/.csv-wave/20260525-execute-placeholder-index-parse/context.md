# TASK-071 Placeholder Index Parsing

Scope: `src/execution/expr.rs`, `src/execution/scan.rs`, `tests/sql_integration.rs`

Implemented:
- Added `Executor::placeholder_index` using `strip_prefix('$')`.
- Replaced `p.replace("$", "").parse()` in expression evaluation and indexed FTS MATCH scanning.
- Added unit coverage for placeholder parsing.
- Added integration coverage for `WHERE id = $1` and `MATCH(body) AGAINST($1)`.

Validation is recorded in `.workflow/.csv-wave/20260525-verify-placeholder-index-parse/verification.json`.
