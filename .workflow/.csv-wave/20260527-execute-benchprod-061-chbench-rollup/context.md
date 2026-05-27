# BENCHPROD-061 Execution Report

## Objective

Optimize CH-benCHmark `Warehouse revenue rollup`:

```sql
SELECT w_id, status, SUM(total), COUNT(*)
FROM bench_tpcc_orders
GROUP BY w_id, status
ORDER BY SUM(total) DESC
LIMIT 20
```

## Implementation

- Generalized the simple GROUP BY aggregate column-scan fast path from one group column to multiple group columns.
- The CH-benCHmark rollup now uses streaming `scan_prefix_for_each` instead of the generic full-row GROUP BY path.
- Kept the fast path conservative: projection group columns must match the `GROUP BY` prefix, and aggregate arguments must be simple columns or `COUNT(*)`.
- Added `test_multi_column_group_by_aggregates_fast_path_order_by_limit`, including a corrupted unrelated payload column to verify partial decoding.

## Verification

| Check | Status |
|---|---|
| `cargo fmt --check` | passed |
| `cargo test --test sql_group_aggregate` | passed, 38 tests |
| `cargo test --test sql_join` | passed, 10 tests |
| `cargo test --test sql_select` | passed, 26 tests |
| `cargo build --release --bin fusiondb` | passed |
| CH-benCHmark medium repeat x3 | passed, 3/3 matrix, 0 case errors |
| Production medium repeat x3 | passed, 3/3 matrix, 0 case errors |
| Production medium gate | passed, 22/22 |

## Benchmark Result

CH-benCHmark-only repeat:

- Suite: stable
- Unstable suites: 0
- Unstable cases: 0
- `Warehouse revenue rollup`: P95 median `5.080 ms`, CV `9.55%`, spread `20.63%`, ops/sec median `212.3`

Production repeat:

- Matrix passed: 3/3
- Case errors: 0
- `chbench` suite P95 median: `11.939 ms`
- `chbench` suite ops/sec median: `377.1`
- `Warehouse revenue rollup`: P95 median `5.169 ms`, ops/sec median `204.7`
- Gate: passed `22/22`

Compared with the BENCHPROD-060 production repeat baseline for the target case:

- P95 median: `10.131 ms -> 5.169 ms` (`-48.98%`)
- Ops/sec median: `114.3 -> 204.7` (`+79.09%`)
- P95 CV: `22.08% -> 20.84%`
- P95 spread: `53.45% -> 45.64%`

The target case still appears as unstable in the full production repeat due to sample spread, but the absolute latency and throughput improved materially, and the default production gate passed.

## Next TASK Signals

- `BENCHPROD-064`: Stabilize remaining LDBC `Tag popularity` variance in production repeat.
- `BENCHPROD-065`: Add top-N grouped aggregate optimization so `ORDER BY aggregate LIMIT` can avoid sorting all groups.
- `BENCHPROD-066`: Expand multi-column GROUP BY support for less conservative projection and alias shapes.
