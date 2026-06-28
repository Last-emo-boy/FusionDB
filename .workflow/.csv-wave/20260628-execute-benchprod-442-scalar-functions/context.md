# BENCHPROD-442 Execution Context

## Outcome
SQL-completeness iteration (workflow-implemented in an isolated worktree, then integrated). Adds
commonly-needed scalar functions that previously hard-errored or were missing.

## Implementation (src/execution/expr/{value.rs,function.rs})
- `TRIM` — `Expr::Trim` was parsed but had no handler (hard error). Implemented standard trim:
  default BOTH + whitespace, plus `LEADING`/`TRAILING`/`BOTH` and `... FROM <chars>` character set.
- `POSITION(sub IN str)` — 1-based index of first occurrence, 0 if absent.
- `GREATEST(...)` / `LEAST(...)` — max/min over args with SQL NULL semantics (NULL args ignored;
  all-NULL -> NULL).
- `EXTRACT(QUARTER FROM ts)` and `EXTRACT(WEEK FROM ts)` — extended the existing EXTRACT/datetime
  field machinery; WEEK uses ISO 8601 week (chrono `iso_week().week()`).

## Verification
- `cargo fmt --check`, `git diff --check`, `cargo check --bins` passed.
- `cargo test --test sql_expr_functions` passed (incl. new TRIM/POSITION/GREATEST/LEAST/EXTRACT tests
  + 2 unit tests); `cargo test --lib` passed.
