# BENCHPROD-080 LDBC PK Probe Projection and Memtier Update Fast Path

## Status

Completed on 2026-05-27.

## Why This Task

BENCHPROD-079 left production medium with residual instability around `ldbc:One-hop friends` and later exposed `memtier:SET existing key` instability during production repeat. Both cases were small indexed operations where row decoding and generic update bookkeeping were too expensive relative to the benchmark latency envelope.

## Implementation

- Updated `src/execution/scan/join.rs` so an indexed join probe can restore a projected right-side primary key from the probe key when the join equality guarantees it.
- Extended projection trimming so `projection=None` can still avoid decoding the guaranteed probe key from the right row payload.
- Added `try_handle_simple_primary_key_update` in `src/execution/dml/update.rs` for simple PK equality updates on plain tables.
- Kept the update fast path conservative: no `RETURNING`, no `CHECK`, no secondary index, no composite index, and no foreign key involvement.
- Exposed `composite_index_table_prefix` as `pub(crate)` so the fast path can cheaply reject tables with composite index metadata.

## Verification Evidence

- `cargo fmt --check`: passed.
- `cargo check --lib`: passed.
- `cargo test --test sql_join -- --nocapture`: passed, `14 passed`.
- `cargo test --test sql_group_aggregate -- --nocapture`: passed, `44 passed`.
- `cargo test --test sql_dml -- --nocapture`: passed, `25 passed`.
- Bench Python syntax check: passed.
- `cargo build --release --bin fusiondb`: passed.
- LDBC targeted repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod080_ldbc_medium_pk_probe_projection_5x_20260527`, `matrix_passed=5`, `case_errors=0`, `unstable_cases=0`.
- Memtier targeted repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod080_memtier_medium_pk_update_fast_5x_20260527`, `matrix_passed=5`, `case_errors=0`, `unstable_cases=0`.
- Production repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod080_production_medium_pk_probe_update_fast_5x_20260527`, `matrix_passed=5`, `matrix_failed=0`, `case_errors=0`, `unstable_cases=0`.
- Production gate: `E:/Playground/FusionDB-bench/runs/gate_benchprod080_production_medium_pk_probe_update_fast_5x_20260527/bench_gate_summary.md`, passed `22/22`.

## Next Task

BENCHPROD-081 should target `chbench:Customer order join`, which became the largest case-level p95 contributor after stability was restored.
