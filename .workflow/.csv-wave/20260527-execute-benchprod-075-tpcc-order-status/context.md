# BENCHPROD-075 TPC-C Order Status Optimization

## Summary

Implemented a database-core optimization for TPC-C order-status shaped queries:

```sql
SELECT o_id, status, total
FROM bench_tpcc_orders
WHERE c_id = ?
ORDER BY o_id DESC
LIMIT 1
```

Secondary BTree equality scans now detect when `ORDER BY` targets the table primary key. Because secondary index keys end with the primary row id, the scan can preserve that order, reverse it for `DESC`, truncate before fetching rows, and report that the scan satisfies `ORDER BY`.

## Evidence

- `cargo fmt --check`: passed
- `cargo test --test sql_index_cache test_index_equality_order_by_primary_desc_limit_fetches_top_row_only -- --nocapture`: passed
- `cargo test --test sql_index_cache`: 30 passed
- `cargo test --test sql_select`: 26 passed
- `cargo build --release --bin fusiondb`: passed
- Targeted TPC-C medium repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod075_tpcc_medium_order_status_3x_20260527`
- Production medium repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod075_production_medium_3x_20260527`
- Production gate: `E:/Playground/FusionDB-bench/runs/gate_benchprod075_production_medium_20260527`

## Result

Targeted TPC-C medium repeat:

- `3/3` matrices passed, `0` case errors
- `0` unstable suites, `0` unstable cases
- `Order status lookup`: P95 median `1.881 ms`, CV `3.21%`, spread `6.63%`, ops median `623.8`

Production medium repeat:

- `3/3` matrices passed, `0` case errors
- `0` unstable suites, `1` unstable case
- Suite P95 medians: CH-benCHmark `8.99 ms`, LDBC `1.94 ms`, memtier `0.56 ms`, TPC-C `2.48 ms`, TSBS `11.44 ms`
- `Order status lookup`: stable, P95 median `1.872 ms`
- Gate passed `22/22`

## Next Signals

- `tsbs:Ingest one point` was the only unstable case in the accepted production repeat and is already allowlisted.
- TSBS tag-filtered range ordering and storage-level reverse scans are candidate follow-up tasks.
- A future storage trait extension for reverse prefix/range scan would make DESC ordered probes cheaper for large equality groups.
