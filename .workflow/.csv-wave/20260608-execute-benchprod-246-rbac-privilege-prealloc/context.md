# BENCHPROD-246 RBAC Privilege Preallocation

## Goal

Avoid implicit vector growth while parsing comma-delimited RBAC privilege lists for `GRANT` and `REVOKE`.

## Implementation

- `src/execution/mod.rs`
  - Replaced `privs_str.split(',').map(...).collect()` in `GRANT` handling with a preallocated vector sized from `privs_str.matches(',').count() + 1`.
  - Applied the same change to `REVOKE` handling.
  - Preserved split, trim, uppercase, and empty-token behavior.

## Verification

- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_returning_upsert_vector_rbac test_rbac_grant_revoke -- --nocapture`
  - Passed: 1/1.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo test --test sql_returning_upsert_vector_rbac -- --nocapture`
  - Passed: 14/14.
- `$env:TEMP=(Resolve-Path '.tmp').Path; $env:TMP=$env:TEMP; cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Environment Note

Cargo/linker verification used an E: workspace temp directory because the default C:/TEMP drive was full.

## Result

`BENCHPROD-246` is complete. RBAC privilege parsing now preallocates the privilege vector from the known comma-delimited token count while preserving existing GRANT and REVOKE semantics.
