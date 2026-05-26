# BENCHPROD-050 Benchmark Stability Report

Date: 2026-05-27
Scope: benchmark harness only; database code and dashboard/ui unchanged.

## Objective

Add repeat-run stability reporting to the FusionDB benchmark system so performance tasks can distinguish real regressions from single-run noise before committing database changes.

This directly follows the rejected `BENCHPROD-047` attempt: functionality passed, but one full medium run showed mixed improvements and regressions. We need tooling that reports variance across repeated matrix runs instead of relying on one trend delta.

## Implementation

Repository: `E:/Playground/FusionDB-bench`

- Added `bench_stability.py`.
- Updated `README.md` with stability report commands.
- Supports:
  - multiple `--report-glob` inputs;
  - `--scale`, `--load-mode`, `--suite`, and `--exact-suites` filtering;
  - `--latest N` selection;
  - suite-level and case-level p95/ops/load statistics;
  - sample count, mean, median, stdev, coefficient of variation, min/max spread;
  - unstable suite/case classification by CV and spread thresholds;
  - throughput-only cases without false latency `sample_count` failures;
  - `--fail-on-unstable` for future CI/commit gates.

## Verification

| Command | Result |
|---|---|
| `python bench_stability.py --help` | passed |
| `python -m py_compile bench_stability.py bench_trend.py fusiondb_matrix.py fusiondb_bench.py external_smoke.py external_bootstrap.py` | passed |
| `python bench_stability.py --scale medium --load-mode insert --suite tpcc,memtier,tsbs,ldbc,chbench --latest 5 --run-name stability_benchprod050_production_medium_latest5_20260527` | passed |
| `python bench_stability.py --scale medium --load-mode insert --suite ycsb,tpcc,tpch,search,memtier,tsbs,ldbc,ann,chbench --exact-suites --latest 3 --run-name stability_benchprod050_full_medium_exact3_final_20260527` | passed |

## Artifacts

- Production target stability: `E:/Playground/FusionDB-bench/runs/stability_benchprod050_production_medium_latest5_20260527/bench_stability_summary.md`
- Full medium exact stability: `E:/Playground/FusionDB-bench/runs/stability_benchprod050_full_medium_exact3_final_20260527/bench_stability_summary.md`

## Production Target Stability Snapshot

Input filter:

```powershell
python bench_stability.py --scale medium --load-mode insert --suite tpcc,memtier,tsbs,ldbc,chbench --latest 5 --run-name stability_benchprod050_production_medium_latest5_20260527
```

Summary:

| Metric | Value |
|---|---:|
| Reports | 5 |
| Suites | 5 |
| Cases | 20 |
| Unstable suites | 3 |
| Unstable cases | 6 |
| Reports with case errors | 0 |

Unstable production suites:

| Suite | Reason | Notes |
|---|---|---|
| chbench | CV/spread | P95 spread `39.71%` across selected samples |
| ldbc | CV/spread | P95 spread `59.50%`; tag popularity is the main driver |
| tsbs | CV/spread | P95 spread `48.89%`; includes pre/post `BENCHPROD-040` optimization samples |

Top unstable production cases:

| Suite | Case | P95 median | P95 spread |
|---|---|---:|---:|
| ldbc | Tag popularity | 9.919 ms | 78.31% |
| chbench | Warehouse revenue rollup | 13.043 ms | 65.22% |
| ldbc | One-hop friends | 0.929 ms | 62.39% |
| tsbs | Fleet rollup by region | 41.322 ms | 55.81% |
| tpcc | Stock level query | 5.881 ms | 31.80% |
| chbench | Customer order join | 31.496 ms | 31.23% |

## Full Medium Exact Snapshot

Input filter:

```powershell
python bench_stability.py --scale medium --load-mode insert --suite ycsb,tpcc,tpch,search,memtier,tsbs,ldbc,ann,chbench --exact-suites --latest 3 --run-name stability_benchprod050_full_medium_exact3_final_20260527
```

Summary:

| Metric | Value |
|---|---:|
| Reports | 3 |
| Suites | 9 |
| Cases | 39 |
| Unstable suites | 3 |
| Unstable cases | 11 |
| Reports with case errors | 0 |

## Current Assessment

`BENCHPROD-050` improves the benchmark system itself. Future performance tasks should use:

1. `bench_trend.py` to compare a candidate run against a selected baseline.
2. `bench_stability.py` over at least 3 same-profile matrix reports to confirm whether a regression/improvement is stable.
3. Exact suite matching for full-matrix gates, and production subset filters for TPC-C/memtier/TSBS/LDBC/CH-benCHmark gates.

## Next TASK Signals

- `BENCHPROD-051`: Add a matrix repeater command that runs N full matrices and automatically emits stability/trend reports.
- `BENCHPROD-048`: Revisit LDBC tag popularity optimization with stability gating.
- `BENCHPROD-049`: Configure native pgbench or BenchBase artifact under `E:/Playground` and add first external smoke evidence.
