# BENCHPROD-100: LDBC Query 14 recursive CTE gap isolation

## Purpose

Continue native LDBC SNB readiness after BENCHPROD-099 by isolating the first concrete SQL feature blocker inside `LdbcQuery14`.

## Changes

- Added support for `sqlparser::ast::SetExpr::Query` in query execution, allowing parenthesized query bodies and parenthesized set operation branches to execute instead of falling through to `Unsupported SELECT format`.
- Added an explicit `WITH RECURSIVE is not supported` error in `Executor::handle_query`.
- Added focused regression coverage:
  - Parenthesized `UNION ALL` query body.
  - `WITH RECURSIVE` reports a specific gap instead of the generic unsupported SELECT format.
- Updated LDBC README/bootstrap wording so Query 14 is documented as reaching a precise `WITH RECURSIVE` blocker.

## Evidence

- `cargo test --test sql_set_subquery`: passed, `13/13`.
- `cargo build --release --bin fusiondb`: passed.
- `python ldbc_snb_native_smoke.py --ldbc-artifact E:\Playground\ldbc-snb\driver\target\driver-standalone.jar --run-mode command --ldbc-command-preset postgres-interactive --preload-postgres-test-data --preload-max-rows-per-file 20 --operation-count 10 --tool-timeout 180 --preload-timeout 180 --run-name ldbc_snb_native_benchprod100_recursive_cte_gap_20260529 --fail-on-gap`: expected gap.
- LDBC evidence:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod100_recursive_cte_gap_20260529\ldbc_snb_native_smoke_summary.json`
  - `LdbcQuery14` now fails with `WITH RECURSIVE is not supported`.
- `python -m py_compile external_bootstrap.py external_smoke.py bench_gate.py ldbc_snb_native_smoke.py`: passed.
- External smoke:
  - `E:\Playground\FusionDB-bench\runs\external_smoke_benchprod100_recursive_cte_gap_20260529\external_smoke_summary.json`
- Strict native gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod100_recursive_cte_gap_strict_20260529\bench_gate_summary.json`
  - Status `failed`, `59/62`.
- `cargo test --release --test pg_integration`: passed, `25/25`.

## Result

BENCHPROD-100 did not make full LDBC pass. It advanced the blocker from an ambiguous `Unsupported SELECT format` to a precise recursive CTE coverage gap and added support for a prerequisite AST shape used by Query 14.

## Current Blockers

- Native memtier remains blocked by missing real `memtier_benchmark`.
- Native LDBC expanded command still fails strict native gate because `LdbcQuery14` requires `WITH RECURSIVE`.
- After recursive CTE support, Query 14 is still expected to need additional work for arrays, `generate_subscripts`, `row_number()`, and multi-layer aggregation.

## Next Task Candidate

BENCHPROD-101 should either implement a minimal bounded recursive CTE executor for `search_graph`-style paths or introduce an LDBC Query 14 adapter rewrite with equivalent bounded traversal semantics.
