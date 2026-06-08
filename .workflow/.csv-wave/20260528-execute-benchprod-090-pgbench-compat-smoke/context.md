# BENCHPROD-090 Context

## Result

`pgbench_compat_smoke.py` now passes with `status=passed` and `steps=6/6`:

- `official_pgbench_schema`
- `fusiondb_compatible_schema`
- `jdbc_copy_load`
- `jdbc_metadata`
- `jdbc_transaction_mix`

Report:

- `E:\Playground\FusionDB-bench\runs\pgbench_compat_benchprod090_add_primary_key_20260528\pgbench_compat_smoke_summary.md`

External smoke now carries this as pgbench readiness evidence while still reporting the real blocker:

- `pgbench` binary is not installed on PATH.
- Official pgbench has not been run end to end.

Report:

- `E:\Playground\FusionDB-bench\runs\external_smoke_benchprod090_pgbench_compat_evidence_20260528\external_smoke_summary.md`

## Implementation Notes

FusionDB stores rows under a key derived from the first column. Because primary-key scans treat `is_primary` as row-key addressability, `ALTER TABLE ADD PRIMARY KEY` is intentionally limited to a single first-column primary key. This matches official pgbench tables (`bid`, `tid`, `aid` are first columns) and avoids creating incorrect non-first-column primary-key semantics.

The DDL path validates existing rows before updating schema metadata:

- no existing primary key,
- exactly one simple identifier column,
- column is first,
- existing values are non-NULL,
- existing values are unique,
- existing values match current row storage keys.

## Files

- `src/execution/ddl/table.rs`
- `tests/sql_ddl.rs`
- `E:\Playground\FusionDB-bench\external_smoke.py`
- `E:\Playground\FusionDB-bench\README.md`
