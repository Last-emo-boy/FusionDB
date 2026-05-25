# TASK-062 Index Row Id Set Merge

Scope:
- `src/execution/scan.rs`
- `tests/sql_integration.rs`

Implemented:
- Replaced FTS token `intersection().cloned().collect()` with in-place `retain`.
- Replaced AND index result intersection with in-place retain on the smaller existing set.
- Replaced OR index result union with `extend` into an existing set.
- Marked FTS token index plans exact so key-only projection does not re-evaluate `MATCH` against partial rows.
- Added a multi-token FTS regression test.

Validation is recorded in `.workflow/.csv-wave/20260525-verify-index-rowid-set-merge/verification.json`.
