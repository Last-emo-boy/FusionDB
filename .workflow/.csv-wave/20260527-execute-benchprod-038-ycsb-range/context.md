# BENCHPROD-038 YCSB Range Scan Pushdown

Date: 2026-05-27
Scope: FusionDB database core optimization and workflow evidence; dashboard/ui unchanged.

## Objective

Optimize the YCSB short range scan shape:

```sql
SELECT id, field2
FROM bench_ycsb
WHERE id >= ?
ORDER BY id
LIMIT 100
```

## Implementation

- Extended primary-key `ORDER BY ... LIMIT` pushdown to allow a single primary-key range predicate.
- Kept the optimization narrow:
  - single table only
  - no join
  - no GROUP BY/HAVING/DISTINCT
  - projection uses simple pushdown-safe expressions
  - `ORDER BY` must be the primary key in ascending order
  - selection must be a single primary-key range predicate
- Reused existing primary-key range detection by widening `primary_key_range_value_expr` visibility to `pub(crate)`.
- Added regression tests for:
  - LIMIT window stopping before decoding rows after the requested range window
  - LIMIT + OFFSET correctness on primary-key range ordered scan

## Verification

Commands:

```powershell
cd E:\Playground\FusionDB
cargo fmt --check
cargo test --test sql_index_cache primary_key_range_order_limit -- --nocapture
cargo test --test sql_index_cache select_order_by_primary_key_limit_offset -- --nocapture
cargo build --release --bin fusiondb

cd E:\Playground\FusionDB-bench
python fusiondb_matrix.py --scale tiny --suite ycsb --load-mode insert --allow-failures --run-name matrix_ycsb_tiny_after_benchprod038_20260527
python fusiondb_matrix.py --scale medium --suite ycsb --load-mode insert --allow-failures --run-name matrix_ycsb_medium_after_benchprod038_20260527
```

Results:

- Targeted tests: 3/3 passed.
- Release build: passed.
- YCSB tiny matrix: passed, 6/6 cases, 0 errors.
- YCSB medium matrix: passed, 6/6 cases, 0 errors.

Artifacts:

- Tiny YCSB matrix: `E:/Playground/FusionDB-bench/runs/matrix_ycsb_tiny_after_benchprod038_20260527/matrix_summary.md`
- Medium YCSB matrix: `E:/Playground/FusionDB-bench/runs/matrix_ycsb_medium_after_benchprod038_20260527/matrix_summary.md`

## Performance

Compared against the previous full medium benchmark:

| Case | Before avg ms | Before p95 ms | Before ops/sec | After avg ms | After p95 ms | After ops/sec |
|---|---:|---:|---:|---:|---:|---:|
| YCSB Short range scan | 6.834 | 11.167 | 146.3 | 0.858 | 0.927 | 1166.0 |

This makes the range case comparable to point/index cases in the current medium suite.

## Follow-up

- Run a full `all` medium matrix after the next optimizer batch to confirm no cross-suite regression.
- Consider broader ordered composite-index LIMIT pushdown for TSBS and CH-style queries.
