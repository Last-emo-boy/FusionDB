# BENCHPROD Matrix Runner Plan

Date: 2026-05-27
Scope: `E:/Playground/FusionDB-bench`; database core/dashboard/ui excluded for this task.

## Objective

Create a production-target benchmark matrix runner for TPC-C-like, memtier-like, TSBS-like, LDBC-like, and CH-benCHmark-like suites.

The runner should isolate each suite into its own FusionDB server/data directory, continue after individual suite failures, and emit a single machine-readable and human-readable summary. This makes benchmark evidence reliable enough to drive the next database optimization TASKs.

## Findings

- `fusiondb_bench.py --suite all` stops on setup-level exceptions before later suites run.
- TPC-H medium currently fails while TPC-H tiny/small pass, so a single shared run can hide useful target-suite data.
- Target production suites can run independently today: `tpcc`, `memtier`, `tsbs`, `ldbc`, `chbench`.
- Runtime artifacts must stay under `E:/Playground/FusionDB-bench/runs/`.

## Plan

1. Add an isolated matrix runner in the bench repository.
2. Ignore generated `runs/` artifacts in git.
3. Document the production-target matrix command and output paths.
4. Verify with a tiny production matrix run.

## Execution Result

Status: completed.

Verification:

- `python -m py_compile fusiondb_matrix.py fusiondb_bench.py` passed.
- `python fusiondb_matrix.py --scale tiny --suite production --load-mode insert --allow-failures` passed 5/5 suites.
- `python fusiondb_matrix.py --scale medium --suite production --load-mode insert --allow-failures` passed 5/5 suites.

Medium matrix report:

- `E:/Playground/FusionDB-bench/runs/matrix_production_medium_insert_20260527_012553/matrix_summary.md`

Data-driven next bottlenecks:

- TSBS rollup and time-range scans are the slowest production target path.
- TPC-C OrderStatus is the slowest OLTP transaction path.
- CH-benCHmark customer-order join and revenue rollup need optimizer/index improvements.
- LDBC tag popularity is the main social graph aggregation hotspot.
