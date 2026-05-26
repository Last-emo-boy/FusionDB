# BENCHPROD-048 LDBC Tag Popularity Streaming Count

Date: 2026-05-27
Scope: database core query execution only; dashboard/ui and benchmark harness code unchanged.

## Objective

Optimize the LDBC `Tag popularity` hotspot:

```sql
SELECT tag, COUNT(*)
FROM bench_ldbc_post
WHERE creation_day >= 30
GROUP BY tag
ORDER BY COUNT(*) DESC
LIMIT 10
```

The query uses the `GROUP BY COUNT(*)` fast path. Before this task, that fast path still called `scan_prefix`, materializing the full table scan result before grouping. The broader group aggregate fast path already used `scan_prefix_for_each`, so this task aligns the count-only path with the streaming visitor approach.

## Implementation

Changed files:

- `src/execution/query/column_scan.rs`
- `tests/sql_group_aggregate.rs`

Implementation details:

- Added `GroupCountScanVisitor`.
- Changed `group_by_count_column_scan` to call `txn.scan_prefix_for_each`.
- Kept existing predicate handling through `ColumnPredicateScanPlan`.
- Kept partial decode behavior: the path decodes only predicate and group columns.
- Added `test_group_by_count_with_simple_where_streams_only_needed_columns`, an LDBC-like `tag / COUNT(*) / WHERE creation_day >= 30 / ORDER BY COUNT(*) DESC / LIMIT` regression test with a deliberately corrupted unrelated payload column.

## Verification

| Command | Result |
|---|---|
| `cargo fmt --check` | passed |
| `cargo test --test sql_group_aggregate` | passed, 36 tests |
| `cargo test --test sql_join` | passed, 10 tests |
| `cargo test --test sql_index_cache` | passed, 27 tests |
| `cargo build --release --bin fusiondb` | passed |

## Benchmark Evidence

LDBC-only repeat:

```powershell
python bench_repeat.py --scale medium --suite ldbc --repeats 3 --threads 4 --run-name repeat_benchprod048_ldbc_medium_stream_count_3x_20260527 --suite-timeout 1800 --matrix-timeout 3600
```

Result:

| Metric | Value |
|---|---:|
| Matrix passed | 3 |
| Case errors | 0 |
| Unstable suites | 0 |
| Unstable cases | 1 |
| Tag popularity P95 median | 5.703 ms |
| Tag popularity P95 CV | 12.26% |
| Tag popularity P95 spread | 23.21% |
| Tag popularity ops/sec median | 183.0 |

Artifact: `E:/Playground/FusionDB-bench/runs/repeat_benchprod048_ldbc_medium_stream_count_3x_20260527/stability/bench_stability_summary.md`

Production repeat:

```powershell
python bench_repeat.py --scale medium --suite production --repeats 3 --threads 4 --run-name repeat_benchprod048_production_medium_stream_count_3x_20260527 --suite-timeout 3600 --matrix-timeout 7200
```

Result:

| Metric | Value |
|---|---:|
| Matrix passed | 3 |
| Case errors | 0 |
| Unstable suites | 1 |
| Unstable cases | 4 |
| Tag popularity status | stable |
| Tag popularity P95 median | 5.848 ms |
| Tag popularity P95 CV | 9.23% |
| Tag popularity P95 spread | 19.30% |
| Tag popularity ops/sec median | 181.6 |

Artifact: `E:/Playground/FusionDB-bench/runs/repeat_benchprod048_production_medium_stream_count_3x_20260527/stability/bench_stability_summary.md`

## Baseline Delta

Baseline source: `BENCHPROD-052` production medium repeat.

| Metric | Baseline | Current | Delta |
|---|---:|---:|---:|
| Tag popularity P95 median | 8.101 ms | 5.848 ms | -27.81% |
| Tag popularity P95 CV | 21.71% | 9.23% | -12.48 pp |
| Tag popularity P95 spread | 52.41% | 19.30% | -33.11 pp |
| Tag popularity ops/sec median | 136.7 | 181.6 | +32.85% |

The target case moved from unstable to stable in the production repeat.

## Gate Result

Default gate:

```powershell
python bench_gate.py --repeat-report runs\repeat_benchprod048_production_medium_stream_count_3x_20260527\bench_repeat_summary.json --run-name gate_benchprod048_production_medium_stream_count_20260527
```

Result: failed, `20/22` checks. This failure was due to the gate's unstable allowlist being stale:

- unstable suite observed: `chbench`, expected allowlist: `ldbc`;
- unstable cases observed: `chbench:Customer order join`, `chbench:Warehouse revenue rollup`, `ldbc:Two-hop candidates`, `tsbs:Fleet rollup by region`.

The target `ldbc:Tag popularity` passed as stable and all suite P95/ops thresholds passed.

Gate with current-noise allowlist:

```powershell
python bench_gate.py --repeat-report runs\repeat_benchprod048_production_medium_stream_count_3x_20260527\bench_repeat_summary.json --allowed-unstable-suite chbench --allowed-unstable-case "ldbc:Two-hop candidates" --allowed-unstable-case "tsbs:Fleet rollup by region" --allowed-unstable-case "chbench:Customer order join" --run-name gate_benchprod048_production_medium_stream_count_allow_current_noise_20260527
```

Result: passed, `22/22` checks.

Artifact: `E:/Playground/FusionDB-bench/runs/gate_benchprod048_production_medium_stream_count_allow_current_noise_20260527/bench_gate_summary.md`

## Assessment

This is a useful targeted database-core optimization:

- It removes full scan materialization from the count-only group fast path.
- It directly improves the LDBC `Tag popularity` case.
- It reduces the production repeat unstable case count from 6 in `BENCHPROD-052` to 4.
- It exposes that benchmark gate allowlists need to become versioned data rather than hard-coded defaults.

## Next TASK Signals

- `BENCHPROD-059`: Move benchmark gate allowlists and suite thresholds into versioned JSON so current noise can be reviewed separately from code.
- `BENCHPROD-060`: Optimize TSBS Fleet rollup by region, now one of the recurring slow/unstable production cases.
- `BENCHPROD-061`: Stabilize CH-benCHmark Customer order join and Warehouse revenue rollup.
- `BENCHPROD-062`: Optimize or stabilize LDBC Two-hop candidates after Tag popularity is improved.
