# BENCHPROD-233 SHOW USERS Output Preallocation

## Goal

Avoid implicit vector growth while constructing `SHOW USERS` result rows and permission strings.

## Implementation

- `src/execution/mod.rs`
  - Replaced `SHOW USERS` row `collect()` with `Vec::with_capacity(users.len())`.
  - Preallocated per-user permission string buffers from `record.permissions.len()`.
  - Preallocated per-table privilege name buffers from `privileges.len()`.
  - Preserved `SHOW USERS` columns, user rows, superuser flags, permission formatting, and no-permission display.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_returning_upsert_vector_rbac test_rbac_grant_revoke -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_returning_upsert_vector_rbac test_rbac_create_drop_user -- --nocapture`
  - Passed.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_returning_upsert_vector_rbac`
  - Passed: 14/14.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-233` is complete. `SHOW USERS` result construction now preallocates row and permission buffers from known collection sizes.
