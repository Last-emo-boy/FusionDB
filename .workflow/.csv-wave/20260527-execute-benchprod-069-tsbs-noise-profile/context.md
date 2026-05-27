# BENCHPROD-069 Execution Report

## Objective

Update the production medium gate profile for a newly observed TSBS low-latency noise case after BENCHPROD-068.

## Rationale

BENCHPROD-068 production repeat passed with:

- Matrix passed: 3/3
- Case errors: 0
- Unstable suites: 0
- Unstable cases: 4
- All suite latency and throughput thresholds passed

The default gate failed only because `tsbs:Tag-filtered time range` was not in the known unstable case allowlist.

## Change

- Added `tsbs:Tag-filtered time range` to `gate_profiles/production_medium.json`.
- Updated profile source metadata to include BENCHPROD-069.
- Kept all suite thresholds unchanged.

## Verification

Gate command:

```powershell
python bench_gate.py --gate-profile gate_profiles/production_medium.json --repeat-report runs/repeat_benchprod068_production_medium_join_projection_3x_20260527/bench_repeat_summary.json --run-name gate_benchprod069_production_medium_profile_20260527
```

Result:

- Status: passed
- Checks: 22/22
- Report: `E:/Playground/FusionDB-bench/runs/gate_benchprod069_production_medium_profile_20260527/bench_gate_summary.md`
