# BENCHPROD-070 TSBS Range Order Optimization

## Summary

Implemented database-core optimization for TSBS tag-filtered time range queries using ordered v3 composite index components and ordered row-id preservation through scan planning.

The hot query shape is:

```sql
SELECT ts, usage_user, usage_system
FROM bench_tsbs_cpu
WHERE host_id = ? AND ts >= 1000 AND ts < 50000
ORDER BY ts LIMIT 100
```

## Evidence

- `cargo fmt --check`: passed
- `cargo test --test sql_index_cache`: 28 passed
- `cargo test --test sql_select`: 26 passed
- `cargo test --test sql_join`: 12 passed
- `cargo test --test sql_group_aggregate`: 39 passed
- `cargo build --release --bin fusiondb`: passed
- TSBS medium repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod070_tsbs_medium_range_order_final_3x_20260527`
- Production medium repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod070_production_medium_range_order_3x_20260527`
- Production gate: `E:/Playground/FusionDB-bench/runs/gate_benchprod070_production_medium_range_order_20260527`

## Result

Targeted TSBS medium repeat:

- `Tag-filtered time range`: P95 median `1.14 ms`, ops median `987.5`, status `stable`
- TSBS suite: P95 median `12.28 ms`, ops median `653.1`, `0` unstable cases

Production medium repeat:

- `3/3` matrices passed, `0` case errors
- `0` unstable suites, `2` unstable cases
- Gate passed `22/22`

## Notes

Existing v2 composite indexes remain readable and maintainable. New composite indexes use v3 metadata with order-preserving components for integer/date/timestamp/interval/boolean columns. Range pushdown and ORDER BY skip are limited to those order-preserving component types.
