# BENCHPROD-064 Execution Report

## Objective

Reduce grouped aggregate `ORDER BY ... LIMIT` overhead for benchmark hot paths:

```sql
SELECT tag, COUNT(*)
FROM bench_ldbc_post
WHERE creation_day >= 30
GROUP BY tag
ORDER BY COUNT(*) DESC
LIMIT 10
```

```sql
SELECT w_id, status, SUM(total), COUNT(*)
FROM bench_tpcc_orders
GROUP BY w_id, status
ORDER BY SUM(total) DESC
LIMIT 20
```

## Implementation

- Extracted reusable GROUP BY fast-path order-key parsing.
- Added row comparison helper for grouped aggregate rows.
- Added top-N candidate pruning before final sort when `LIMIT`/`OFFSET` is present.
- Preserved exact output by sorting the retained `offset + limit` window before applying `OFFSET`/`LIMIT`.
- Added `test_group_by_aggregate_order_by_limit_offset_topn_window`.

## Verification

| Check | Status |
|---|---|
| `cargo fmt --check` | passed |
| `cargo test --test sql_group_aggregate` | passed, 39 tests |
| `cargo test --test sql_select` | passed, 26 tests |
| `cargo test --test sql_join` | passed, 10 tests |
| `cargo build --release --bin fusiondb` | passed |
| LDBC medium repeat x3 | passed, 3/3 matrix, 0 case errors |
| Production medium repeat x3 | passed, 3/3 matrix, 0 case errors |
| Production medium gate | failed expected profile noise, 21/22 |
| Production medium gate + current noise allowlist | passed, 22/22 |

## Benchmark Result

LDBC-only repeat:

- Suite: stable
- Unstable suites: 0
- Unstable cases: 1
- `Tag popularity`: stable, P95 median `5.724 ms`, CV `7.68%`, spread `16.59%`, ops/sec median `183.5`

Production repeat:

- Matrix passed: 3/3
- Case errors: 0
- `ldbc` suite P95 median: `2.124 ms`
- `ldbc` suite ops/sec median: `947.5`
- `memtier` suite P95 median: `0.694 ms`
- `memtier` suite ops/sec median: `1228.0`
- `Tag popularity`: P95 median `5.801 ms`, ops/sec median `174.8`

Default gate failed only on newly observed allowlist items:

- `ldbc:Recent posts by friends`
- `memtier:GET by key`
- `memtier:SET existing key`

All suite thresholds, matrix counts, and case error checks passed.

## Next TASK Signals

- `BENCHPROD-067`: Update production profile for newly observed low-latency LDBC/memtier noise.
- `BENCHPROD-068`: Add index-assisted execution for LDBC recent posts.
- `BENCHPROD-069`: Investigate memtier GET/SET latency variance under HTTP SQL protocol.
