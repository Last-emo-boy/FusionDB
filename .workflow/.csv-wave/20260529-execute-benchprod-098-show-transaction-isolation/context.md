# BENCHPROD-098 Context

## Result

Fixed the LDBC PostgreSQL JDBC/Hikari startup blocker for transaction isolation metadata.

Changes in `E:\Playground\FusionDB`:

- `src/server/pg_server.rs` now treats `SHOW TRANSACTION ISOLATION LEVEL` as a Pg metadata query.
- `pg_metadata_show` returns a PostgreSQL-compatible result set:
  - column: `transaction_isolation`
  - value: `read committed`
- `tests/pg_integration.rs` now covers both extended `client.query(...)` and simple query paths for the SHOW statement.

Changes in `E:\Playground\FusionDB-bench`:

- `ldbc_snb_native_smoke.py` now records command timeouts as structured `gap` steps instead of losing command stdout/stderr as harness failures.
- The built-in `postgres-interactive` LDBC preset includes `--add-exports=java.base/sun.nio.ch=ALL-UNNAMED` for the current Java 17 runtime and the older LDBC driver metrics dependency.
- `README.md` documents the new LDBC state: the Hikari startup blocker is fixed, and the current native command blocker is missing SNB schema/data.

## Verification

- `cargo test test_pg_protocol_show_transaction_isolation_level_for_jdbc_pool_startup --test pg_integration`: passed.
- `cargo test --test pg_integration`: passed, `24/24`.
- `cargo build --release --bin fusiondb`: passed, refreshing `E:\Playground\FusionDB\target\release\fusiondb.exe`.
- `python -m py_compile ldbc_snb_native_smoke.py`: passed.
- `python ldbc_snb_native_smoke.py --ldbc-artifact E:\Playground\ldbc-snb\driver\target\driver-standalone.jar --run-mode command --ldbc-command-preset postgres-interactive --operation-count 1 --tool-timeout 120 --run-name ldbc_snb_native_benchprod098_show_tx_isolation_java17_20260529 --fail-on-gap`: failed as expected with `status=gap`, but progressed beyond Hikari initialization into the official LDBC Query 1 path.
- `python external_smoke.py --target benchbase-tpcc,memtier,tsbs,ldbc,chbenchmark --ldbc-native-evidence runs\ldbc_snb_native_benchprod098_show_tx_isolation_java17_20260529\ldbc_snb_native_smoke_summary.json --run-name external_smoke_benchprod098_show_tx_isolation_20260529`: report generated.
- `python bench_gate.py --gate-profile gate_profiles\production_medium_strict_native.json --repeat-report runs\benchprod_current_medium_production_3x_20260528_fix2\bench_repeat_summary.json --external-smoke-report runs\external_smoke_benchprod098_show_tx_isolation_20260529\external_smoke_summary.json --recovery-smoke-report runs\recovery_smoke_benchprod086_pgtest_port_fix_20260528\recovery_smoke_summary.json --run-name gate_benchprod098_show_tx_isolation_strict_20260529`: failed as expected, `59/62` checks passed.

Reports:

- `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod098_show_tx_isolation_java17_20260529\ldbc_snb_native_smoke_summary.json`
- `E:\Playground\FusionDB-bench\runs\external_smoke_benchprod098_show_tx_isolation_20260529\external_smoke_summary.json`
- `E:\Playground\FusionDB-bench\runs\gate_benchprod098_show_tx_isolation_strict_20260529\bench_gate_summary.json`

## Current Gate State

Strict native gate remains blocked, correctly:

- `external_smoke.memtier.status`: `tool_missing`, expected `tool_available`.
- `external_smoke.memtier.native_status`: `tool_missing`, expected `passed`.
- `external_smoke.ldbc.native_status`: `gap`, expected `passed`.

LDBC has progressed:

- Previous blocker: PostgreSQL JDBC/Hikari called `SHOW TRANSACTION ISOLATION LEVEL` and FusionDB did not return a result set.
- Current state: the official LDBC PostgreSQL implementation loads, starts workload execution, and sends Query 1 through FusionDB PgWire.
- Current blocker: Query 1 fails with `ERROR: Execution Error: Execution("Table knows not found")`, because the official SNB schema/data import path is not wired into the native smoke yet.

## Remaining Work

Next high-value tasks:

1. `BENCHPROD-099`: add LDBC SNB schema/import adapter coverage so official PostgreSQL implementation tables such as `knows`, `person`, `person_email`, and related graph tables exist before command-mode query execution.
2. After schema/data import passes, capture the next LDBC blocker, likely PostgreSQL array/aggregation syntax, nested subqueries, path-query join planning, or type coercion.
3. Continue memtier native work in parallel by installing real `memtier_benchmark` and rerunning native probe evidence.
