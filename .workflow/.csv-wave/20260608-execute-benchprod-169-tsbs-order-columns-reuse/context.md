# BENCHPROD-169 TSBS ORDER BY Column Name Reuse

## Goal

Avoid repeated output column-name vector construction while building TSBS lastpoint lateral `ORDER BY` sort keys.

## Implementation

- `src/execution/query/mod.rs`
  - Collected `output_schema` column names once before iterating over `ORDER BY` expressions.
  - Reused that vector for each `resolve_order_value_source` call.
  - Existing sort-key resolution, ordering, `DISTINCT ON`, and limit behavior remain unchanged.

## Verification

- `cargo test --test sql_join test_tsbs_lastpoint_distinct_on_lateral_join -- --nocapture`
  - Passed.
- `cargo test --test sql_join`
  - Passed: 30/30.
- `cargo fmt --check`
  - Passed.
- `git diff --check`
  - Passed.

## Result

`BENCHPROD-169` is complete. TSBS lateral join sort-key construction now avoids rebuilding the same output column-name vector for every order expression.
