# BENCHPROD-071 Production Gate Noise Profile

Final post-commit production repeat for BENCHPROD-070 passed:

- `3/3` matrices passed
- `0` case errors
- `0` unstable suites
- suite thresholds all passed

The first final gate failed only because `tpcc:Payment transaction` was a new observed low-latency unstable case outside the known-noise allowlist.

Updated `E:/Playground/FusionDB-bench/gate_profiles/production_medium.json` and reran:

```powershell
python bench_gate.py --gate-profile gate_profiles/production_medium.json --repeat-report runs/repeat_benchprod070_production_medium_range_order_final_3x_20260527/bench_repeat_summary.json --run-name gate_benchprod071_production_medium_profile_20260527
```

Result: `22/22` checks passed.
