# BENCHPROD-051 Benchmark Matrix Repeater

Date: 2026-05-27
Scope: benchmark harness only; database code and dashboard/ui unchanged.

## Objective

Add a repeat-run command for the FusionDB benchmark system. The goal is to make performance-sensitive database work easier to validate with repeated matrices, stability analysis, and adjacent trend reports from one command.

## Implementation

Repository: `E:/Playground/FusionDB-bench`

- Added `bench_repeat.py`.
- Updated `README.md` with repeat workflow examples.
- `bench_repeat.py`:
  - runs `fusiondb_matrix.py` N times;
  - stores matrices under `runs/<repeat-run>/matrices/matrix_XX_*`;
  - runs `bench_stability.py` over the repeated matrices;
  - runs `bench_trend.py` between adjacent matrix pairs unless `--skip-trend` is set;
  - writes `bench_repeat_summary.json` and `bench_repeat_summary.md`.

## Verification

| Command | Result |
|---|---|
| `python -m py_compile bench_repeat.py bench_stability.py bench_trend.py fusiondb_matrix.py fusiondb_bench.py external_smoke.py external_bootstrap.py` | passed |
| `python bench_repeat.py --help` | passed |
| `cargo build --release --bin fusiondb` | passed |
| `python bench_repeat.py --scale tiny --suite production --repeats 2 --threads 2 --run-name repeat_benchprod051_tiny_production_2x_20260527 --suite-timeout 900 --matrix-timeout 1800` | passed |

## Artifacts

- Repeat summary: `E:/Playground/FusionDB-bench/runs/repeat_benchprod051_tiny_production_2x_20260527/bench_repeat_summary.md`
- Stability report: `E:/Playground/FusionDB-bench/runs/repeat_benchprod051_tiny_production_2x_20260527/stability/bench_stability_summary.md`
- Adjacent trend: `E:/Playground/FusionDB-bench/runs/repeat_benchprod051_tiny_production_2x_20260527/trends/trend_01_to_02/bench_trend_summary.md`

## Tiny Production Repeat Result

Input:

```powershell
python bench_repeat.py --scale tiny --suite production --repeats 2 --threads 2 --run-name repeat_benchprod051_tiny_production_2x_20260527 --suite-timeout 900 --matrix-timeout 1800
```

Summary:

| Metric | Value |
|---|---:|
| Matrix runs passed | 2 |
| Matrix runs failed | 0 |
| Case errors | 0 |
| Suites per matrix | 5/5 |
| Cases per matrix | 20 |

The automatic stability report marked all 5 production suites stable for this tiny run. It still flagged 3 unstable micro-cases, which is expected for a 2-sample tiny smoke and confirms the tool reports variance rather than hiding it.

## Current Assessment

`BENCHPROD-051` makes the benchmark system more operational. For future performance work:

1. Use `bench_repeat.py --scale medium --suite production --repeats 3` before accepting broad performance changes.
2. Use the generated stability report as the main noise filter.
3. Use adjacent trend reports to spot monotonic drift or single-run outliers.

## Next TASK Signals

- `BENCHPROD-052`: Run production medium repeat x3 and promote it as the new performance gate baseline.
- `BENCHPROD-048`: Optimize LDBC tag popularity using repeat/stability gates.
- `BENCHPROD-049`: Configure native pgbench or BenchBase under `E:/Playground`.
