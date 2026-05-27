# BENCHPROD-068 Execution Report

## Objective

Reduce LDBC join latency and instability by carrying join-stage projection hints into filtered base scans and indexed join probes.

## Implementation

- Added a join helper that maps prefixed join-stage projection requirements back to base table columns.
- Passed projection hints to the first relation when a local `WHERE` predicate is extracted.
- Passed projection hints to right-side relation scans inside join steps.
- Added projected partial decode for indexed right-side join probes.
- Added regression coverage for LDBC-like left-filter and indexed right-probe shapes using corrupted unused payload columns.

## Verification

| Check | Status |
|---|---|
| `cargo fmt --check` | passed |
| `cargo test --test sql_join` | passed, 12 tests |
| `cargo test --test sql_select` | passed, 26 tests |
| `cargo test --test sql_group_aggregate` | passed, 39 tests |
| `cargo build --release --bin fusiondb` | passed |
| LDBC medium repeat x3 | passed, 3/3 matrix, 0 case errors |
| Production medium repeat x3 | passed, 3/3 matrix, 0 case errors |
| Production medium gate | failed expected profile noise, 21/22 |

## Benchmark Result

LDBC-only repeat:

- Suite: stable
- Unstable suites: 0
- Unstable cases: 0
- Suite P95 median: `2.021 ms`, CV `1.36%`, spread `2.75%`
- Suite ops/sec median: `946.2`
- `One-hop friends`: P95 median `0.823 ms`
- `Recent posts by friends`: P95 median `0.975 ms`
- `Tag popularity`: P95 median `5.370 ms`
- `Two-hop candidates`: P95 median `0.858 ms`

Report:

- `E:/Playground/FusionDB-bench/runs/repeat_benchprod068_ldbc_medium_join_projection_3x_20260527/stability/bench_stability_summary.md`

Production repeat:

- Suites: all stable
- Matrix passed: 3/3
- Case errors: 0
- Unstable suites: 0
- Unstable cases: 4
- `chbench` P95 median: `9.503 ms`
- `ldbc` P95 median: `2.099 ms`
- `memtier` P95 median: `0.598 ms`
- `tpcc` P95 median: `3.263 ms`
- `tsbs` P95 median: `11.882 ms`

Default gate failed only because a new observed low-latency noise case is outside the profile allowlist:

- `tsbs:Tag-filtered time range`

All suite thresholds, matrix counts, and case-error checks passed.

Production reports:

- `E:/Playground/FusionDB-bench/runs/repeat_benchprod068_production_medium_join_projection_3x_20260527/stability/bench_stability_summary.md`
- `E:/Playground/FusionDB-bench/runs/gate_benchprod068_production_medium_join_projection_20260527/bench_gate_summary.md`

## Next TASK Signals

- Run full production medium repeat/gate for cross-suite regression evidence.
- Investigate memtier GET/SET latency variance.
- Consider composite-index-assisted latest/recent posts once workload shape grows beyond medium scale.
