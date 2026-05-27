# BENCHPROD-073 memtier Primary-Key GET Optimization

## Summary

Implemented a database-core optimization for SQL KV-like memtier GET queries. For primary-key equality point lookups, the executor now trusts the data-key lookup as satisfying `WHERE pk = literal`, avoids redundant selection evaluation, removes the primary-key column from partial row decode when it is only needed by the predicate, and restores the primary key from the data key when needed by row semantics.

The hot query shape is:

```sql
SELECT value FROM bench_kv WHERE key_id = ?
```

## Evidence

- `cargo test --test sql_index_cache test_primary_key_lookup_projection_skips_where_key_decode -- --nocapture`: passed
- `cargo test --test sql_index_cache`: 29 passed
- `cargo test --test sql_select`: 26 passed
- `cargo fmt --check`: passed
- `cargo build --release --bin fusiondb`: passed
- memtier medium repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod073_memtier_medium_pk_get_3x_20260527`
- Production medium repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod073_production_medium_3x_20260527`
- Production gate: `E:/Playground/FusionDB-bench/runs/gate_benchprod073_production_medium_20260527`

## Result

Targeted memtier medium repeat:

- `3/3` matrices passed, `0` case errors
- `0` unstable suites, `0` unstable cases
- `GET by key`: P95 median `0.793 ms`, CV `3.56%`, spread `7.24%`, ops median `1361.1`

Production medium repeat:

- `3/3` matrices passed, `0` case errors
- `0` unstable suites, `1` unstable case
- Suite P95 medians: CH-benCHmark `9.82 ms`, LDBC `2.18 ms`, memtier `0.62 ms`, TPC-C `3.13 ms`, TSBS `12.03 ms`
- Remaining unstable case: `tpcc:Stock level query`
- Gate passed `22/22`

## Notes

This TASK removes the previous production `memtier:GET by key` instability from the targeted memtier repeat. In full production repeat, the remaining variance moved to `tpcc:Stock level query`, so the next production TASK should focus on TPC-C stock-level read paths rather than changing gate thresholds.
