# BENCHPROD-404 Execution Context

## Summary

Optimized `src/execution/expr/subquery.rs` EXISTS membership cache key construction by replacing per-column `to_ascii_lowercase()` temporary allocations with direct ASCII-lowercase appends into the final key `String`.

## Files

- `src/execution/expr/subquery.rs`

## Behavior Preservation

- Cache key separators and part order are unchanged.
- Column-name lowercase bytes are tested against the previous `to_ascii_lowercase()` output.
- Non-ASCII text is preserved through chunked `push_str`, matching ASCII-only lowercasing semantics.

## Verification

- `cargo test subquery --lib` passed: 6 passed.
- `cargo test --test sql_set_subquery exists` passed: 6 passed.
- `cargo fmt --check` passed.
- `git diff --check` passed with only the expected CRLF warning.
- `rg -n "plan\\.[a-z_]*column\\.to_ascii_lowercase\\(|to_ascii_lowercase\\(\\)" src\\execution\\expr\\subquery.rs` shows remaining matches only in legacy-equivalence tests.
