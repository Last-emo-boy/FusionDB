# BENCHPROD-072 LDBC Two-hop Join Probe Optimization

## Summary

Implemented a database-core optimization for LDBC two-hop indexed join probes. When a single-key indexed probe guarantees the right-side join key, the executor now avoids decoding that key from the right row if the final projection does not require it, restores the key from the index lookup, and skips the redundant row-key equality check.

The hot query shape is:

```sql
SELECT k2.person2_id
FROM bench_ldbc_knows k1
INNER JOIN bench_ldbc_knows k2 ON k1.person2_id = k2.person1_id
WHERE k1.person1_id = ?
LIMIT 100
```

## Evidence

- `cargo test --test sql_join test_two_hop_join_probe_skips_guaranteed_right_key_decode -- --nocapture`: passed
- `cargo test --test sql_join`: 13 passed
- `cargo test --test sql_select`: 26 passed
- `cargo fmt --check`: passed
- `cargo build --release --bin fusiondb`: passed
- LDBC medium repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod072_ldbc_medium_twohop_probe_3x_20260527`
- Production medium repeat: `E:/Playground/FusionDB-bench/runs/repeat_benchprod072_production_medium_3x_20260527`
- Production gate: `E:/Playground/FusionDB-bench/runs/gate_benchprod072_production_medium_20260527`

## Result

Targeted LDBC medium repeat:

- `3/3` matrices passed, `0` case errors
- LDBC suite: P95 median `2.022 ms`, ops median `948.7`, status `stable`
- `Two-hop candidates`: P95 median `0.886 ms`, ops median `1245.1`, status remains `unstable`

Production medium repeat:

- `3/3` matrices passed, `0` case errors
- `0` unstable suites, `1` unstable case
- Suite P95 medians: CH-benCHmark `9.90 ms`, LDBC `2.07 ms`, memtier `0.62 ms`, TPC-C `3.24 ms`, TSBS `11.90 ms`
- Gate passed `22/22`

## Notes

The optimization is semantically useful and now covered by a regression test that corrupts only the right-side probe key column. It does not eliminate LDBC two-hop P95 variance in the targeted repeat. In the full production repeat, the only remaining unstable case is `memtier:GET by key`, so the next production-oriented TASK should focus on key-value read variance or add more precise microbench evidence for join-probe decode savings.
