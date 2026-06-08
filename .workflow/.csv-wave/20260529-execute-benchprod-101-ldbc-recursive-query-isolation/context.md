# BENCHPROD-101: LDBC recursive query isolation

## Purpose

Continue from BENCHPROD-100 without claiming a full native LDBC pass. The goal was to make `ldbc_snb_native_smoke.py` able to disable individual LDBC read queries for diagnosis, then use that to map the next recursive CTE blockers after Query 14.

## Changes

- Added repeatable `--disable-read-query` to `E:\Playground\FusionDB-bench\ldbc_snb_native_smoke.py`.
- Generated `ldbc.snb.interactive.*_enable=false` properties for disabled read queries.
- Added `config.disabled_read_queries` and `config.isolation_mode` to LDBC native smoke reports.
- Added `ldbc_isolation_mode` blocker step so a disabled-query run remains `gap` even when the LDBC command exits `0`.
- Updated `external_smoke.py` to report `isolation_mode` and to prefer non-isolation LDBC evidence when auto-discovering latest native reports.
- Updated `bench_gate.py` to fail `external_smoke.ldbc.native_isolation_mode` if disabled-query evidence is used as native pass evidence.
- Updated README/bootstrap wording to document that disabled-query LDBC runs are diagnosis-only.

## Evidence

- `python -m py_compile ldbc_snb_native_smoke.py external_smoke.py bench_gate.py external_bootstrap.py`: passed.
- `python ldbc_snb_native_smoke.py --help`: passed and shows `--disable-read-query`.
- Query 14 disabled:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod101_disable_q14_isolate_20rows_10ops_20260529\ldbc_snb_native_smoke_summary.json`
  - Result: `gap`; next blocker is `LdbcQuery13`, `WITH RECURSIVE is not supported`.
- Query 13 and 14 disabled:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod101_disable_q13_q14_isolate_20rows_10ops_20260529\ldbc_snb_native_smoke_summary.json`
  - Result: `gap`; next blocker is `LdbcQuery12`, `WITH RECURSIVE is not supported`.
- Query 12, 13, and 14 disabled:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod101_disable_q12_q13_q14_isolate_20rows_10ops_20260529\ldbc_snb_native_smoke_summary.json`
  - Result: LDBC command completed `LdbcQuery1` and `LdbcQuery11`, but report remains `gap` because `isolation_mode=true`.
- External smoke isolation marker:
  - `E:\Playground\FusionDB-bench\runs\external_smoke_benchprod101_ldbc_isolation_marker_20260529\external_smoke_summary.json`
- Default external smoke evidence discovery skips isolation reports:
  - `E:\Playground\FusionDB-bench\runs\external_smoke_benchprod101_default_ldbc_evidence_skip_isolation_20260529\external_smoke_summary.json`
  - It selected BENCHPROD-100 non-isolation LDBC evidence instead of the latest isolation run.
- Strict gate targeted check:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod101_ldbc_isolation_marker_strict_20260529\bench_gate_summary.json`
  - Expected failure includes `external_smoke.ldbc.native_isolation_mode` observed `True`, expected `false`.

## Recursive CTE Coverage Map

Static scan of `E:\Playground\ldbc-snb\impls\postgres\queries` found recursive CTE use in:

- `interactive-complex-12.sql`
- `interactive-complex-13.sql`
- `interactive-complex-14.sql`
- `interactive-short-2.sql`
- `interactive-short-6.sql`

Dynamic isolation confirmed Query 13 and Query 12 as the next blockers after Query 14. The 12/13/14-disabled 10-operation smoke did not happen to schedule Short Query 2 or Short Query 6, so those remain static coverage risks rather than dynamic failures in this run.

## Result

BENCHPROD-101 improved the LDBC diagnostic harness and production gate semantics. It did not make LDBC pass. The next implementation task should focus on bounded `WITH RECURSIVE` support or an equivalent LDBC adapter path, because recursive CTE is shared across multiple official PostgreSQL implementation queries.

## Current Blockers

- Native memtier still lacks a real `memtier_benchmark` probe.
- Native LDBC non-isolation expanded workload remains blocked by `WITH RECURSIVE`.
- After recursive CTE support, LDBC Query 14 is still expected to need arrays, `generate_subscripts`, window `row_number()`, and multi-layer aggregation coverage.
