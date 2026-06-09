# BENCHPROD-405 Execution Context

## Summary

Optimized `src/execution/expr/value.rs` column fallback resolution by replacing per-column `to_ascii_lowercase()` matching with allocation-free ASCII-insensitive suffix matching.

## Files

- `src/execution/expr/value.rs`

## Behavior Preservation

- Exact column match, exact suffix match, and ASCII-insensitive equality are still checked.
- ASCII-insensitive suffix matching is now byte-level, avoiding temporary lowercase strings and avoiding UTF-8 boundary slicing.
- Unit tests compare the new fallback helper against the previous lowercase-based matching expression.

## Verification

- `cargo test value --lib` passed: 40 passed.
- `cargo test --test sql_select` passed: 27 passed.
- `cargo test --test sql_join join` passed: 30 passed.
- `cargo fmt --check` passed.
- `git diff --check` passed with only the expected CRLF warning.
- `rg -n "fallback_lower|suffix_lower|column\\.name\\.to_ascii_lowercase\\(|to_ascii_lowercase\\(\\)" src\\execution\\expr\\value.rs` shows remaining lowercase calls only in legacy-equivalence tests.
