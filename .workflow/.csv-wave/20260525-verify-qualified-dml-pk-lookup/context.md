# Verify Context

Verification status: passed.

Checks:
- `cargo fmt --check`
- `cargo test --test sql_integration qualified_primary_key -- --nocapture`
- `cargo test --test sql_integration test_delete_primary_key_without_secondary_index_skips_row_decode -- --nocapture`
- `cargo check --lib`

Notes:
- Qualified DML fast path is limited to the target table name or alias.
- Unqualified primary-key predicates retain the existing point lookup behavior.
