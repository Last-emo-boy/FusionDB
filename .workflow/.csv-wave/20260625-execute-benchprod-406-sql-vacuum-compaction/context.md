# BENCHPROD-406 Execution Context

## Summary

Added SQL `VACUUM` support as a manual full-database compaction trigger backed by `FusionStorage::compact_now()`.

## Files

- `src/execution/mod.rs`
- `src/server/pg_server.rs`
- `tests/sql_ddl.rs`
- `README.md`
- `ROADMAP.md`

## Behavior

- `VACUUM` and `VACUUM FULL` now execute through the normal parsed statement path.
- FusionStorage runs `compact_now()` and returns a success message indicating whether compaction ran or was skipped because there were not enough SSTables.
- MemoryStorage and other non-Fusion backends return a clear unsupported-backend error.
- Table-specific VACUUM and Redshift-style options other than `FULL` return clear errors.
- Non-legacy users require superuser authorization for VACUUM.
- pgwire command tags map VACUUM success messages to `VACUUM`.

## Verification

- `cargo test --test sql_ddl vacuum -- --nocapture` passed: 3 passed.
- `cargo check --lib` passed.
- `cargo test --test sql_ddl` passed: 37 passed.
- `cargo fmt --check` passed.
- `git diff --check` passed with expected CRLF warnings.
- `cargo test --lib` passed: 302 passed.

