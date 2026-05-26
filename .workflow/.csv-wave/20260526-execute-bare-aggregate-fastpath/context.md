# TASK-128 Execution Context

## Summary

Optimized bare aggregate evaluation in `src/execution/query.rs` by reusing compiled aggregate argument sources from the GROUP BY fast path.

The bare aggregate loop now avoids repeated generic expression evaluation for simple column arguments, literals, wildcards, and column multiplication expressions.

## Files

- `src/execution/query.rs`
- `tests/sql_integration.rs`

## Verification

- `cargo fmt --check`
- `cargo check --lib`
- `cargo test --test sql_integration aggregate -- --nocapture`
- `cargo test --test sql_integration count_distinct -- --nocapture`
- `cargo build --release`
- `BENCH_SCALE=medium python benchmark.py`

## Benchmark

Report: `C:\Users\ES&E\AppData\Local\Temp\fusiondb-task128-bareagg-medium-20260526-130038\benchmark_report_medium.json`
