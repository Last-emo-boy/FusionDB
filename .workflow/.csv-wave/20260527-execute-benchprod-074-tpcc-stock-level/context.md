# BENCHPROD-074 TPC-C Stock Level Optimization

## Summary

Implemented a database-core fast path for TPC-C stock-level shaped bare aggregates:

```sql
SELECT COUNT(*) FROM bench_tpcc_stock
WHERE w_id = ? AND quantity < 20
```

The bare column aggregate scan now supports `COUNT(*)` without requiring an aggregate column decode. With a simple predicate, it decodes only the predicate columns and increments the count for each matching row.

## Evidence

- `cargo fmt --check`: passed
- `cargo test --test sql_group_aggregate test_select_count_star_with_simple_where_column_scan -- --nocapture`: passed
- `cargo test --test sql_group_aggregate`: 40 passed
- `cargo test --test sql_select`: 26 passed
- `cargo build --release --bin fusiondb`: passed
- Targeted TPC-C medium repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod074_tpcc_medium_stock_level_3x_20260527`
- Targeted TPC-C order-status noise probe: `E:/Playground/FusionDB-bench/runs/repeat_benchprod074_tpcc_medium_order_status_probe_3x_20260527`
- Production medium repeat used for gate: `E:/Playground/FusionDB-bench/runs/repeat_benchprod074_production_medium_retry2_3x_20260527`
- Production gate: `E:/Playground/FusionDB-bench/runs/gate_benchprod074_production_medium_retry2_20260527`

## Result

Targeted TPC-C medium repeat:

- `3/3` matrices passed, `0` case errors
- `0` unstable suites, `0` unstable cases
- `Stock level query`: P95 median `2.283 ms`, CV `5.54%`, spread `11.60%`, ops median `454.9`

Production medium repeat used for gate:

- `3/3` matrices passed, `0` case errors
- `0` unstable suites, `3` unstable cases
- Suite P95 medians: CH-benCHmark `8.84 ms`, LDBC `1.89 ms`, memtier `0.60 ms`, TPC-C `2.44 ms`, TSBS `11.61 ms`
- `Stock level query` is stable in production repeat
- Gate passed `22/22`

## Noise Notes

Two earlier full production repeats after the same code change did not pass gate, but failed on different low-latency stability checks:

- `repeat_benchprod074_production_medium_3x_20260527`: new `tpcc:Order status lookup` instability, not reproduced by targeted TPC-C repeat.
- `repeat_benchprod074_production_medium_retry_3x_20260527`: `memtier` suite instability.

No gate thresholds or allowlists were changed. The accepted evidence is the later production repeat and gate run that passed under the existing `production_medium` profile.

## Next Signals

- Add longer production noise profiling before treating occasional low-latency P95 instability as a code regression.
- Investigate indexed equality plus `ORDER BY DESC LIMIT 1` for TPC-C order-status.
- Continue toward external benchmark compatibility and longer soak runs.
