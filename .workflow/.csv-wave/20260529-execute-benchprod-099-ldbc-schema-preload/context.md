# BENCHPROD-099 Context

## Result

Added bounded LDBC SNB schema/import preload coverage to the native benchmark tooling in `E:\Playground\FusionDB-bench`.

Changes:

- `ldbc_snb_native_smoke.py` supports `--preload-postgres-test-data`.
- The preload path compiles a small JDBC helper, creates the official PostgreSQL implementation SNB table shape, and loads bounded rows from `E:\Playground\ldbc-snb\impls\postgres\test-data` through PostgreSQL JDBC `CopyManager`.
- The preload verifies that `knows`, `person`, and `message` contain rows before the official LDBC driver command runs.
- `external_smoke.py`, `external_bootstrap.py`, and `README.md` now describe the current LDBC state as schema/import covered, with Query 1 blocked on PostgreSQL prepared placeholder compatibility.

## Verification

- `python -m py_compile external_bootstrap.py external_smoke.py ldbc_snb_native_smoke.py`: passed.
- `python ldbc_snb_native_smoke.py --ldbc-artifact E:\Playground\ldbc-snb\driver\target\driver-standalone.jar --run-mode command --ldbc-command-preset postgres-interactive --preload-postgres-test-data --preload-max-rows-per-file 200 --operation-count 1 --tool-timeout 120 --preload-timeout 180 --run-name ldbc_snb_native_benchprod099_schema_preload_final_20260529 --fail-on-gap`: failed as expected with `status=gap`.
- `python external_smoke.py --target benchbase-tpcc,memtier,tsbs,ldbc,chbenchmark --ldbc-native-evidence runs\ldbc_snb_native_benchprod099_schema_preload_final_20260529\ldbc_snb_native_smoke_summary.json --run-name external_smoke_benchprod099_ldbc_schema_preload_env_20260529`: report generated with `ldbc.status=tool_available`.
- `python bench_gate.py --gate-profile gate_profiles\production_medium_strict_native.json --repeat-report runs\benchprod_current_medium_production_3x_20260528_fix2\bench_repeat_summary.json --external-smoke-report runs\external_smoke_benchprod099_ldbc_schema_preload_env_20260529\external_smoke_summary.json --recovery-smoke-report runs\recovery_smoke_benchprod086_pgtest_port_fix_20260528\recovery_smoke_summary.json --run-name gate_benchprod099_ldbc_schema_preload_strict_20260529`: failed as expected, `58/62` checks passed.

Reports:

- `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod099_schema_preload_final_20260529\ldbc_snb_native_smoke_summary.json`
- `E:\Playground\FusionDB-bench\runs\external_smoke_benchprod099_ldbc_schema_preload_env_20260529\external_smoke_summary.json`
- `E:\Playground\FusionDB-bench\runs\gate_benchprod099_ldbc_schema_preload_strict_20260529\bench_gate_summary.json`

## Current Gate State

Strict native gate remains blocked, correctly:

- `external_smoke.memtier.status`: `tool_missing`, expected `tool_available`.
- `external_smoke.tsbs.status`: `tool_missing`, expected `tool_available` in the current PATH environment.
- `external_smoke.memtier.native_status`: `tool_missing`, expected `passed`.
- `external_smoke.ldbc.native_status`: `gap`, expected `passed`.

LDBC has progressed:

- Previous blocker: official Query 1 failed with `Table knows not found`.
- Current state: schema/import preload succeeds and verifies rows in `knows`, `person`, and `message`.
- Current blocker: official Query 1 reaches FusionDB with `$1`/`$2` PostgreSQL placeholders and fails with `Invalid parameter placeholder: $1`.

## Remaining Work

Next high-value tasks:

1. Add PostgreSQL `$n` prepared placeholder mapping in FusionDB SQL/PgWire execution paths.
2. Rerun LDBC preload command smoke and capture the next Query 1 blocker, likely `array_agg`, `::text` casts, subquery aggregation, `UNION ALL`, or multi-hop join planning.
3. Ensure TSBS tools are on PATH and install real `memtier_benchmark`, then rerun strict native gate.
