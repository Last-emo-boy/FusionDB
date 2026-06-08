# BENCHPROD-171 SELECT In-Place Trim

## Goal

Avoid rebuilding ordinary SELECT result rows when applying `LIMIT` and `OFFSET`.

## Implementation

- `src/execution/query/mod.rs`
  - Replaced ordinary SELECT `skip`/`take`/`collect` result trimming with `trim_query_rows_in_place`.
  - Existing `LIMIT`/`OFFSET`, `ORDER BY`, and `COUNT(*)` behavior remain unchanged.

## Verification

- `cargo test --test sql_select test_select_with_limit_offset -- --nocapture`
  - Passed.
- `cargo test --test sql_select test_select_order_by_limit_offset -- --nocapture`
  - Passed.
- `cargo test --test sql_select`
  - Passed: 27/27.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-171` is complete. Ordinary SELECT result windows now trim in place instead of allocating a rebuilt rows vector.
