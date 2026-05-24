# Single-column DDL/DML decode verification

Verification target:
- TASK-017 UNIQUE duplicate checks.
- TASK-018 CREATE INDEX backfill.

Checks:
- `cargo fmt --check`
- `cargo test --test sql_integration test_unique_constraint -- --nocapture`
- `cargo test --test sql_integration test_create_btree_index -- --nocapture`
- `cargo check --lib`

All checks passed in the local workspace.
