# BENCHPROD-082 TSBS Composite DESC Order Scan

## Status

Completed on 2026-05-27. Production medium gate passed.

## Why This Task

BENCHPROD-081 fixed the CH-benCHmark target, but production gate failed because TSBS became the unexpected unstable suite. The failing signal included `tsbs:Latest points for host`, which maps to equality-prefix composite index access with `ORDER BY ts DESC LIMIT N`.

## Implementation

- Updated `src/execution/composite_index.rs` so composite index order matching returns the next-column order direction instead of a boolean.
- Supported equality-prefix scans where the next composite index column satisfies `ORDER BY <next-column> DESC LIMIT N`.
- Reversed and truncated ordered row ids before row fetch so latest-point queries read the newest index window first.
- Added regression coverage in `tests/sql_index_cache.rs` proving `WHERE host_id = 1 ORDER BY ts DESC LIMIT 2` skips older corrupted rows.

## Verification Evidence

- `cargo fmt --check`: passed.
- `cargo check --lib`: passed.
- `cargo test --test sql_index_cache -- --nocapture`: passed.
- `cargo test --test sql_join -- --nocapture`: passed.
- Bench Python syntax check: passed.
- `cargo build --release --bin fusiondb`: passed.
- Targeted TSBS repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod082_tsbs_medium_composite_desc_5x_20260527`, `matrix_passed=5`, `case_errors=0`, `unstable_suites=0`, `unstable_cases=0`, suite p95 median `0.976 ms`.
- Production repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod082_production_medium_composite_desc_5x_20260527`, `matrix_passed=5`, `matrix_failed=0`, `case_errors=0`.
- Production stability: `E:/Playground/FusionDB-bench/runs/repeat_benchprod082_production_medium_composite_desc_5x_20260527/stability/bench_stability_summary.md`, only suite-level unstable item was allowlisted `chbench`; case-level unstable count was `0`.
- Production gate: `E:/Playground/FusionDB-bench/runs/gate_benchprod082_production_medium_composite_desc_5x_20260527/bench_gate_summary.md`, passed `22/22`.

## Next Task

BENCHPROD-083 should target the largest remaining production case latency: `tpcc:New order transaction`, especially the stock primary-key update subpath.
