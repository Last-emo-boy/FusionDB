# BENCHPROD-033 CH-benCHmark Group Aggregate Fast Path

Date: 2026-05-27
Scope: FusionDB database core optimizer/execution only; dashboard/ui unchanged.

## Objective

Improve CH-benCHmark analytical cases, especially customer-order join and warehouse rollup, while keeping the full medium benchmark matrix green.

## Baseline

Baseline report:

- `E:/Playground/FusionDB-bench/runs/matrix_all_medium_full_observe_20260527/matrix_summary.md`

Baseline case metrics:

- `chbench / Customer order join`: p95 `36.301 ms`
- `chbench / Warehouse revenue rollup`: p95 `13.972 ms`
- Full medium matrix: `9/9` suites, `39/39` cases passed

## Changes

- `src/execution/query/mod.rs`
  - Added a projected group aggregate fast path for simple post-scan/post-join aggregate shapes:
    - single-column `GROUP BY`
    - projection starts with the group column
    - aggregate list limited to `COUNT(*)` and `SUM(column)`
    - no `HAVING`
  - The fast path aggregates directly by projected column indices, avoiding the generic `Vec<Value>` group key construction and final expression evaluator for common CH-style rollups.
- `tests/sql_join.rs`
  - Added a join/group-by shape test covering `GROUP BY city`, `COUNT(*)`, and `SUM(orders.total)`.

## Verification

Rust:

```powershell
cargo fmt --check
cargo check --lib
cargo test --test sql_join -- --nocapture
cargo test --test sql_group_aggregate -- --nocapture
cargo build --release --bin fusiondb
```

Benchmark:

```powershell
cd E:\Playground\FusionDB-bench
python fusiondb_matrix.py --scale medium --suite chbench --load-mode insert --allow-failures --run-name matrix_chbench_medium_after_benchprod033_groupfast_20260527
python fusiondb_matrix.py --scale medium --suite all --load-mode insert --allow-failures --run-name matrix_all_medium_after_benchprod033_groupfast_20260527
```

Results:

- `sql_join`: `10/10` tests passed
- `sql_group_aggregate`: `34/34` tests passed
- CH medium suite: `1/1` suite, `3/3` cases passed
- Full medium matrix: `9/9` suites, `39/39` cases passed
- Post-run hygiene: no `fusiondb` process remained; ports `8091` and `8092` were free.

## Benchmark Delta

CH suite-only after report:

- `E:/Playground/FusionDB-bench/runs/matrix_chbench_medium_after_benchprod033_groupfast_20260527/matrix_summary.md`

CH suite-only:

- `Customer order join`: p95 `25.840 ms`, from `36.301 ms`, about `28.8%` lower.
- `Warehouse revenue rollup`: p95 `8.929 ms`, from `13.972 ms`, about `36.1%` lower.

Full medium after report:

- `E:/Playground/FusionDB-bench/runs/matrix_all_medium_after_benchprod033_groupfast_20260527/matrix_summary.md`

Full medium:

- `Customer order join`: p95 `28.466 ms`, from `36.301 ms`, about `21.6%` lower.
- `Warehouse revenue rollup`: p95 `13.738 ms`, from `13.972 ms`, roughly stable.

## Next TASK Signals

- `BENCHPROD-040`: TSBS fleet rollup remains the slowest medium case; likely needs columnar/summary/statistics design, not decode-cache micro-optimizations.
- `BENCHPROD-037`: ANN HNSW still needs recall/build/index-size metrics and search-path work.
- `BENCHPROD-034`: LDBC tag popularity remains a contained group aggregate candidate.
