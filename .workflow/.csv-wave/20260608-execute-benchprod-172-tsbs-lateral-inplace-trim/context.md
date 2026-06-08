# BENCHPROD-172 TSBS Lateral In-Place Trim

## Goal

Avoid rebuilding TSBS lastpoint lateral `DISTINCT ON` result rows when applying outer `LIMIT` and `OFFSET`.

## Implementation

- `src/execution/query/mod.rs`
  - Replaced TSBS lateral `DISTINCT ON` `skip`/`take`/`collect` result trimming with `trim_query_rows_in_place`.
  - Returned the trimmed `distinct_rows` vector directly.
- `tests/sql_join.rs`
  - Extended `test_tsbs_lastpoint_distinct_on_lateral_join` with an outer `LIMIT 1 OFFSET 1` assertion.

## Verification

- `cargo test --test sql_join test_tsbs_lastpoint_distinct_on_lateral_join -- --nocapture`
  - Passed.
- `cargo test --test sql_join`
  - Passed: 31/31.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-172` is complete. TSBS lateral `DISTINCT ON` result windows now trim in place while preserving outer window behavior.
