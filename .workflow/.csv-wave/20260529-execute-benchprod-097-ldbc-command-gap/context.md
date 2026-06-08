# BENCHPROD-097 Context

## Result

Moved LDBC native evidence beyond artifact/help readiness into first real command-mode execution.

Changes in `E:\Playground\FusionDB-bench`:

- `ldbc_snb_native_smoke.py` now supports `--ldbc-command-preset postgres-interactive`.
- The preset automatically resolves the official PostgreSQL implementation jar:
  - `E:\Playground\ldbc-snb\impls\postgres\target\postgres-1.2.0-SNAPSHOT.jar`
- The smoke generates official LDBC driver properties with:
  - `workload=org.ldbcouncil.snb.driver.workloads.interactive.LdbcSnbInteractiveWorkload`
  - `db=org.ldbcouncil.snb.impls.workloads.postgres.interactive.PostgresInteractiveDb`
  - test-data `substitution_parameters` / `update_streams`
  - all required complex and short read enable flags
  - updates disabled for the first read-path command smoke
- `README.md` now documents the command-mode preset and current FusionDB blocker.

## Verification

- `python -m py_compile ldbc_snb_native_smoke.py`: passed.
- `python ldbc_snb_native_smoke.py --ldbc-artifact E:\Playground\ldbc-snb\driver\target\driver-standalone.jar --run-mode help --run-name ldbc_snb_native_benchprod097_help_regression_20260529`: passed.
- `python ldbc_snb_native_smoke.py --ldbc-artifact E:\Playground\ldbc-snb\driver\target\driver-standalone.jar --run-mode command --ldbc-command-preset postgres-interactive --operation-count 1 --tool-timeout 120 --run-name ldbc_snb_native_benchprod097_postgres_interactive_command_flags_20260529 --fail-on-gap`: failed as expected with `status=gap`.
- `python external_smoke.py --target benchbase-tpcc,memtier,tsbs,ldbc,chbenchmark --ldbc-native-evidence runs\ldbc_snb_native_benchprod097_postgres_interactive_command_flags_20260529\ldbc_snb_native_smoke_summary.json --run-name external_smoke_benchprod097_ldbc_command_gap_20260529`: report generated.
- `python bench_gate.py --gate-profile gate_profiles\production_medium_strict_native.json --repeat-report runs\benchprod_current_medium_production_3x_20260528_fix2\bench_repeat_summary.json --external-smoke-report runs\external_smoke_benchprod097_ldbc_command_gap_20260529\external_smoke_summary.json --recovery-smoke-report runs\recovery_smoke_benchprod086_pgtest_port_fix_20260528\recovery_smoke_summary.json --run-name gate_benchprod097_ldbc_command_gap_strict_20260529`: failed as expected, `59/62` checks passed.

Reports:

- `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod097_help_regression_20260529\ldbc_snb_native_smoke_summary.json`
- `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod097_postgres_interactive_command_flags_20260529\ldbc_snb_native_smoke_summary.json`
- `E:\Playground\FusionDB-bench\runs\external_smoke_benchprod097_ldbc_command_gap_20260529\external_smoke_summary.json`
- `E:\Playground\FusionDB-bench\runs\gate_benchprod097_ldbc_command_gap_strict_20260529\bench_gate_summary.json`

## Current Gate State

Strict native gate remains blocked, correctly:

- `external_smoke.memtier.status`: `tool_missing`, expected `tool_available`.
- `external_smoke.memtier.native_status`: `tool_missing`, expected `passed`.
- `external_smoke.ldbc.native_status`: `gap`, expected `passed`.

LDBC has progressed:

- Previous blocker: `external_smoke.ldbc.native_run_mode=help`.
- Current state: `run_mode=command`, `ldbc_command_preset=postgres-interactive`, official driver reaches FusionDB PgWire.
- Current blocker: PostgreSQL JDBC/Hikari initialization calls `SHOW TRANSACTION ISOLATION LEVEL`; FusionDB logs `PG Execute Portal : SHOW TRANSACTION ISOLATION LEVEL params=[]`, but JDBC receives no result set.

## Remaining Work

Next high-value tasks:

1. `BENCHPROD-098`: implement PostgreSQL-compatible `SHOW TRANSACTION ISOLATION LEVEL` result handling in FusionDB PgWire/SQL path, then rerun the LDBC command-mode smoke.
2. After connection initialization passes, capture the next LDBC blocker, likely schema absence/load SQL compatibility or LDBC query SQL dialect gaps.
3. Continue memtier native work in parallel by installing real `memtier_benchmark` and rerunning native probe evidence.
