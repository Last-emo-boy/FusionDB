# BENCHPROD-042 External Benchmark Readiness Matrix

## Purpose

Extend the E-drive benchmark harness so the external benchmark gap runner covers the production targets explicitly requested for FusionDB:

- pgbench
- sysbench
- native memtier
- BenchBase/TPC-C
- TSBS
- LDBC SNB
- CH-benCHmark

The runner records tool availability and local artifact readiness only. It does not claim official benchmark compliance.

## Scope

- Repository: `E:/Playground/FusionDB-bench`
- Files:
  - `external_smoke.py`
  - `README.md`
- FusionDB workflow tracking:
  - `.workflow/.csv-wave/20260527-execute-benchprod-042-external-readiness/`
  - `.workflow/.csv-wave/20260526-analyze-benchprod-production-gap/tasks.csv`

## Expected Evidence

- `python external_smoke.py --target all --run-name external_smoke_benchprod042_20260527`
- JSON and Markdown report under `E:/Playground/FusionDB-bench/runs/external_smoke_benchprod042_20260527/`

## Result

- Verification command passed.
- Report JSON: `E:/Playground/FusionDB-bench/runs/external_smoke_benchprod042_20260527/external_smoke_summary.json`
- Report Markdown: `E:/Playground/FusionDB-bench/runs/external_smoke_benchprod042_20260527/external_smoke_summary.md`
- Summary: 7 targets checked, 0 tool_available, 4 tool_missing, 3 artifact_missing, 0 tool_error.
- Current environment:
  - `pgbench`, `sysbench`, `memtier_benchmark`, and `tsbs_generate_data` are missing from `PATH`.
  - Java is present for BenchBase/LDBC/CH-benCHmark style targets, but `BENCHBASE_HOME` or `BENCHBASE_JAR`, `LDBC_SNB_HOME` or `LDBC_DRIVER_HOME`, and `CHBENCH_HOME`/`CHBENCHMARK_HOME`/`CH_BENCHMARK_HOME` are not configured.
