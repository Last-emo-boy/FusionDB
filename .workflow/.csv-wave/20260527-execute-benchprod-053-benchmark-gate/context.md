# BENCHPROD-053 Benchmark Gate Evaluator

Date: 2026-05-27
Scope: benchmark harness and workflow evidence only; database code and dashboard/ui unchanged.

## Objective

Turn the `BENCHPROD-052` production medium repeat baseline into an executable gate. The gate should read existing benchmark artifacts, evaluate pass/fail conditions, and produce JSON/Markdown reports that can be used before accepting performance-sensitive database changes.

## Implementation

Repository: `E:/Playground/FusionDB-bench`

- Added `bench_gate.py`.
- Updated `README.md` with gate examples.
- Bench repo commit: `538c3bb feat: 增加benchmark门禁评估`.

`bench_gate.py` evaluates:

- repeat profile: scale, load mode, and suite set;
- matrix pass/fail count;
- case error count;
- stability report sample count and error count;
- unstable suite/case counts;
- unstable suite/case allowlists;
- suite-level P95 and ops/sec thresholds;
- optional all-suite matrix summary against the same production target thresholds.

## Default Gate Profile

The default gate is based on `BENCHPROD-052` production medium repeat:

| Field | Value |
|---|---:|
| Scale | medium |
| Suite | production |
| Suites | tpcc, memtier, tsbs, ldbc, chbench |
| Load mode | insert |
| Min repeats | 3 |
| Max matrix failures | 0 |
| Max case errors | 0 |
| Max unstable suites | 1 |
| Max unstable cases | 6 |
| Stability metric | median |

Default thresholds:

| Suite | Max P95 ms | Min ops/sec |
|---|---:|---:|
| tpcc | 5.000 | 450.0 |
| memtier | 1.000 | 1100.0 |
| tsbs | 16.000 | 400.0 |
| ldbc | 4.000 | 850.0 |
| chbench | 18.000 | 200.0 |

Known unstable allowlist:

- `ldbc` suite.
- `chbench:Warehouse revenue rollup`.
- `ldbc:Tag popularity`.
- `memtier:ADD new key`.
- `tpcc:Delivery status update`.
- `tpcc:Stock level query`.
- `tsbs:Ingest one point`.

## Verification

Syntax/help:

| Command | Result |
|---|---|
| `python -m py_compile bench_gate.py bench_repeat.py bench_stability.py bench_trend.py fusiondb_matrix.py fusiondb_bench.py` | passed |
| `python bench_gate.py --help` | passed |

Baseline gate:

```powershell
python bench_gate.py --repeat-report runs\repeat_benchprod052_medium_production_3x_20260527\bench_repeat_summary.json --matrix-report runs\matrix_benchprod052_all_medium_insert_20260527\matrix_summary.json --matrix-suite all --run-name gate_benchprod053_baseline_20260527
```

Result: passed, `37/37` checks.

Artifact: `E:/Playground/FusionDB-bench/runs/gate_benchprod053_baseline_20260527/bench_gate_summary.md`

Expected failure:

```powershell
python bench_gate.py --repeat-report runs\repeat_benchprod052_medium_production_3x_20260527\bench_repeat_summary.json --suite-max-p95-ms tsbs=1 --run-name gate_benchprod053_expected_fail_20260527
```

Result: failed as expected, `21/22` checks. The failure was `stability.tsbs.p95` because the observed median P95 was `12.580 ms` and the artificial threshold was `<= 1.000 ms`.

Artifact: `E:/Playground/FusionDB-bench/runs/gate_benchprod053_expected_fail_20260527/bench_gate_summary.md`

Tiny smoke:

```powershell
python bench_repeat.py --scale tiny --suite production --repeats 2 --threads 2 --run-name repeat_benchprod053_tiny_production_2x_20260527 --suite-timeout 900 --matrix-timeout 1800
python bench_gate.py --repeat-report runs\repeat_benchprod053_tiny_production_2x_20260527\bench_repeat_summary.json --scale tiny --min-repeats 2 --max-unstable-suites 1 --max-unstable-cases 3 --suite-max-p95-ms tpcc=20 --suite-max-p95-ms memtier=10 --suite-max-p95-ms tsbs=30 --suite-max-p95-ms ldbc=15 --suite-max-p95-ms chbench=30 --suite-min-ops-sec tpcc=50 --suite-min-ops-sec memtier=50 --suite-min-ops-sec tsbs=20 --suite-min-ops-sec ldbc=50 --suite-min-ops-sec chbench=20 --run-name gate_benchprod053_tiny_smoke_20260527
```

Result: repeat passed with `2/2` matrices and `0` case errors; gate passed, `22/22` checks.

Artifacts:

- `E:/Playground/FusionDB-bench/runs/repeat_benchprod053_tiny_production_2x_20260527/bench_repeat_summary.md`
- `E:/Playground/FusionDB-bench/runs/gate_benchprod053_tiny_smoke_20260527/bench_gate_summary.md`

## Assessment

`BENCHPROD-053` makes the benchmark system operationally safer: future optimization work can now run `bench_repeat.py` and `bench_gate.py` to fail fast on new errors, missing suites, unexpected instability, or suite-level latency/throughput regressions.

This is still a local production gate for FusionDB's benchmark-like workloads. It does not replace official TPC-C, memtier, TSBS, LDBC, or CH-benCHmark validation.

## Next TASK Signals

- `BENCHPROD-048`: Use `bench_gate.py` while optimizing LDBC Tag popularity so regressions are caught immediately.
- `BENCHPROD-054`: Add official benchmark adapter inventory and runnable smoke for BenchBase TPC-C or pgbench under `E:/Playground`.
- `BENCHPROD-058`: Add CI/local profile wrappers for `cargo build`, `bench_repeat.py`, and `bench_gate.py`.
- `BENCHPROD-059`: Split threshold configuration into versioned JSON once the gate starts tracking multiple profiles.
