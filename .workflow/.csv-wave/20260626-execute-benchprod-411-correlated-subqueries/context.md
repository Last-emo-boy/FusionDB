# BENCHPROD-411 Execution Context

## Outcome

Completed `BENCHPROD-411` by closing the `P2-36` correlated subquery gap.

## Implementation

- Bound outer row references inside subquery `HAVING` clauses and select-list expressions, not only `WHERE`.
- Restricted the optimized EXISTS membership caches to pure row-filter subqueries so `GROUP BY`, `HAVING`, `DISTINCT`, and limiting clauses fall back to full correlated execution instead of ignoring semantics.
- Materialized scalar subqueries during ordinary projection evaluation so per-row scalar subqueries can use the current outer row.
- Included outer columns referenced by projection subqueries in scan projection hints.
- Allowed single-table projection pushdown to resolve qualified column hints such as `p.label` back to base column `label` when unambiguous.
- Updated README and ROADMAP to mark `P2-36` complete.

## Verification

- `cargo test --test sql_set_subquery correlated -- --nocapture` passed.
- `cargo test --test sql_set_subquery -- --nocapture` passed with 50 tests.
- `cargo test --lib` passed with 314 tests.
- `cargo check --bins` passed.
- `cargo fmt --check` passed.
- `git diff --check` passed with expected CRLF warnings.

## Commit

- `0d6d950 feat: 支持相关子查询外层引用`

## Remaining Production Gaps

- `P3-3`: SCRAM-SHA-256, currently blocked by pgwire 0.37 limitations.
- `P4-3`: Cost-based optimizer.
- `P5-1` through `P5-3`: Distributed execution, snapshot transfer, and automatic sharding.
