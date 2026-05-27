# BENCHPROD-076 TSBS Ingest Optimization

## Summary

Implemented database-core DML maintenance changes so plain non-indexed `TEXT` columns no longer populate the FusionStorage trigram side index on every `INSERT`.

This directly targets TSBS ingest rows, where `region` and `rack` are plain text fields but not single-column text indexes. Indexed text columns still maintain trigram postings for wildcard `LIKE`, and the lifecycle now covers `CREATE INDEX` backfill, `UPDATE`, and `DELETE`.

## Evidence

- `cargo fmt --check`: passed
- `cargo check --lib`: passed
- `cargo test --test sql_index_cache trigram -- --nocapture`: 5 passed
- `cargo test --test sql_index_cache -- --nocapture`: 35 passed
- `cargo test --test sql_expr_functions test_like_full_patterns -- --nocapture`: passed
- `cargo test --test sql_dml -- --nocapture`: 23 passed
- `cargo build --release --bin fusiondb`: passed
- Targeted TSBS medium repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod076_tsbs_medium_ingest_3x_20260527`
- Production medium repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod076_production_medium_3x_20260527`
- Production gate: `E:/Playground/FusionDB-bench/runs/gate_benchprod076_production_medium_20260527`

## Result

Targeted TSBS medium repeat:

- `3/3` matrices passed, `0` case errors
- `0` unstable suites, `1` unstable case
- `Ingest one point`: P95 median `0.835 ms`, CV `16.05%`, spread `33.25%`, ops median `1261.7`

Production medium repeat:

- `3/3` matrices passed, `0` case errors
- `0` unstable suites, `4` unstable cases, all allowlisted by the existing production profile
- Suite P95 medians: CH-benCHmark `10.547 ms`, LDBC `2.127 ms`, memtier `0.607 ms`, TPC-C `2.645 ms`, TSBS `11.875 ms`
- `TSBS Ingest one point`: stable, P95 median `0.862 ms`, CV `4.76%`, spread `9.88%`, ops median `1260.9`
- Gate passed `22/22`

## Next Signals

- Composite index metadata still scans `index_meta:` on each DML statement and is a likely next write-path optimization.
- LDBC one-hop/two-hop and CH-benCHmark warehouse rollup remain low-latency variance candidates.
- Keep the rule that production gate thresholds and allowlists are evidence artifacts, not optimization levers.
