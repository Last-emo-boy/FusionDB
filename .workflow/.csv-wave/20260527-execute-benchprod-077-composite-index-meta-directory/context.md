# BENCHPROD-077 Composite Index Metadata Directory

## Summary

Implemented a table-scoped composite index metadata directory so DML no longer needs to scan global `index_meta:` entries for new benchmark tables.

New tables now write an `index_meta_table:<table>:__marker` key. Composite index creation rebuilds that table's directory from existing global metadata, preserving legacy metadata values and key encoding. DML reads table-scoped metadata when the marker exists and falls back to the legacy global scan for old databases without the marker.

## Evidence

- `cargo fmt --check`: passed
- `cargo check --lib`: passed
- `cargo test --test sql_dml composite_index -- --nocapture`: 2 passed
- `cargo test --test sql_index_cache composite_index -- --nocapture`: 2 passed
- `cargo test --test sql_dml -- --nocapture`: 24 passed
- `cargo test --test sql_index_cache -- --nocapture`: 35 passed
- `cargo build --release --bin fusiondb`: passed
- Production medium repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod077_production_medium_composite_meta_3x_20260527`
- Production gate: `E:/Playground/FusionDB-bench/runs/gate_benchprod077_production_medium_composite_meta_20260527`

## Result

Production medium repeat:

- `3/3` matrices passed, `0` case errors
- `0` unstable suites, `2` unstable cases, all allowlisted by the existing production profile
- Suite P95 medians: CH-benCHmark `10.283 ms`, LDBC `2.084 ms`, memtier `0.653 ms`, TPC-C `2.730 ms`, TSBS `12.016 ms`
- Gate passed `22/22`

## Next Signals

- `memtier:SET existing key` and `tpcc:Stock level query` remain allowlisted variance points in the latest repeat.
- `tsbs:Fleet rollup by region` is still the dominant production-suite latency contributor.
- Next candidate: improve grouped aggregate / rollup paths without changing benchmark thresholds or allowlists.
