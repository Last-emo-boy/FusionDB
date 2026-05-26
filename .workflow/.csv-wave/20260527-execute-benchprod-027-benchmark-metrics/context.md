# BENCHPROD-027 Benchmark Credibility Metrics

Date: 2026-05-27
Scope: benchmark harness/reporting only; FusionDB database core and dashboard/ui unchanged.

## Objective

Improve benchmark credibility reporting so later database optimization work is judged from richer latency distribution evidence instead of only average and p95.

## Changes

- `E:/Playground/FusionDB-bench/fusiondb_bench.py`
  - Expanded suite Markdown result table to show `p50`, `p90`, `p95`, `p99`, `min`, `max`, `stddev`, `ops/sec`, row count, sample count, and errors.
  - Reused existing JSON metrics, so old consumers still keep `avg_ms`, `p95_ms`, and `ops_per_sec`.
- `E:/Playground/FusionDB-bench/fusiondb_matrix.py`
  - Added suite-level aggregate fields: `load_ms`, `avg_avg_ms`, `avg_p50_ms`, `avg_p90_ms`, `avg_p99_ms`.
  - Expanded matrix Markdown suite and case tables with latency distribution and sample counts.
  - Kept existing fields such as `avg_p95_ms` and `avg_ops_per_sec`.

## Verification

Commands:

```powershell
cd E:\Playground\FusionDB-bench
python -m py_compile fusiondb_bench.py fusiondb_matrix.py
python fusiondb_matrix.py --scale tiny --suite memtier,tsbs --load-mode insert --allow-failures --run-name matrix_metrics_tiny_memtier_tsbs_20260527
python fusiondb_matrix.py --scale medium --suite memtier,tsbs --load-mode insert --allow-failures --run-name matrix_metrics_medium_memtier_tsbs_20260527
```

Results:

- Tiny matrix: `2/2` suites passed, `8/8` cases passed.
- Medium matrix: `2/2` suites passed, `8/8` cases passed.
- Post-run hygiene: no `fusiondb` process remained; ports `8091` and `8092` were free.

Reports:

- `E:/Playground/FusionDB-bench/runs/matrix_metrics_tiny_memtier_tsbs_20260527/matrix_summary.md`
- `E:/Playground/FusionDB-bench/runs/matrix_metrics_medium_memtier_tsbs_20260527/matrix_summary.md`

## TSBS Optimization Note

During BENCHPROD-040 exploration, three database-core experiments were benchmarked and rejected because they did not improve the `TSBS Fleet rollup by region` case:

- Per-row decoded column cache: p95 `69.179 ms`.
- Low-cardinality linear group state: p95 `59.659 ms`, effectively baseline noise.
- Multi-column one-shot decode: p95 `68.192 ms`.

The database-code experiments were not retained. Current tree intentionally keeps database core unchanged for this task.

## Next TASK Signals

- `BENCHPROD-040`: TSBS fleet rollup still needs a larger storage/columnar/statistics design, not the rejected decode-cache micro-optimizations.
- `BENCHPROD-033`: CH-benCHmark join/rollup remains a high-value optimizer target.
- `BENCHPROD-037`: ANN recall/build/index-size metrics remain needed for production credibility.
