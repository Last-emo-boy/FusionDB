# BENCHPROD-060 Execution Report

## Objective

Optimize the TSBS-like `Fleet rollup by region` query while keeping the `production_medium` benchmark profile healthy.

Target query:

```sql
SELECT region, AVG(usage_user), MAX(usage_system)
FROM bench_tsbs_cpu
WHERE ts >= 1000 AND ts < 50000
GROUP BY region
ORDER BY region
```

## Implementation

- Updated `src/execution/query/column_scan.rs` so column predicate scan plans decode each unique predicate column once per row.
- Multiple predicates on the same column now share a decoded slot, e.g. `ts >= 1000 AND ts < 50000`.
- GROUP BY aggregate streaming still uses `scan_prefix_for_each`; this task reduces decode work inside that streaming path.
- Added `test_group_by_aggregates_reuses_multi_predicate_column_values` in `tests/sql_group_aggregate.rs`.

## Verification

| Check | Status |
|---|---|
| `cargo fmt --check` | passed |
| `cargo test --test sql_group_aggregate` | passed, 37 tests |
| `cargo test --test sql_index_cache` | passed, 27 tests |
| `cargo test --test sql_select` | passed, 26 tests |
| `cargo build --release --bin fusiondb` | passed |
| TSBS medium repeat x3 | passed, 3/3 matrix, 0 case errors |
| Production medium repeat x3 | passed, 3/3 matrix, 0 case errors |
| Production medium gate | failed expected allowlist noise, 21/22 |
| Production medium gate + current noise allowlist | passed, 22/22 |

## Benchmark Result

TSBS-only repeat:

- Suite: stable
- Unstable suites: 0
- Unstable cases: 0
- `Fleet rollup by region`: P95 median `40.376 ms`, CV `1.47%`, spread `2.83%`, ops/sec median `25.3`

Production repeat:

- Matrix passed: 3/3
- Case errors: 0
- Suite-level unstable count: 0
- `tsbs` suite P95 median: `12.088 ms`
- `tsbs` suite ops/sec median: `496.8`
- `Fleet rollup by region`: stable, P95 median `41.231 ms`, CV `2.51%`, spread `5.10%`, ops/sec median `25.0`

Compared with the BENCHPROD-048 production repeat baseline for the target case:

- P95 median: `43.214 ms -> 41.231 ms` (`-4.59%`)
- P95 CV: `12.71% -> 2.51%`
- P95 spread: `25.97% -> 5.10%`
- Ops/sec median: `24.6 -> 25.0` (`+1.63%`)
- Status: `unstable -> stable`

## Gate Notes

The default versioned `production_medium` gate failed one allowlist check because this run observed `ldbc:One-hop friends` as a new low-latency unstable case. All suite thresholds, matrix counts, and case error checks passed. A validation gate with only that current noise case added via CLI passed 22/22.

No benchmark profile was changed in this task; the new noise item should be handled separately as a gate/profile review or LDBC stability task.

## Next TASK Signals

- `BENCHPROD-061`: Stabilize CH-benCHmark Warehouse revenue rollup.
- `BENCHPROD-062`: Investigate LDBC low-latency noise including One-hop friends and Tag popularity.
- `BENCHPROD-063`: Design real range-index or time-partition execution for TSBS `ts` range scans; this task reduced decode overhead but still performs a full table scan.
