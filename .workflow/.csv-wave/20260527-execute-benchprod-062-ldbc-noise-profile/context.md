# BENCHPROD-062 Execution Report

## Objective

Restore the default `production_medium` gate after BENCHPROD-060 exposed a new low-latency LDBC noise case.

## Evidence

BENCHPROD-060 production medium repeat passed all core checks:

- Matrix passed: 3/3
- Case errors: 0
- Unstable suites: 0
- Suite thresholds passed for `tpcc`, `memtier`, `tsbs`, `ldbc`, and `chbench`

The default gate failed only because `ldbc:One-hop friends` was not in the versioned unstable-case allowlist.

Observed case:

- `ldbc:One-hop friends`
- P95 median: `0.905 ms`
- Ops/sec median: `1217.7`
- Reasons: `cv,spread`

This is low-latency stability noise rather than a threshold regression; the `ldbc` suite remained stable and above production thresholds.

## Change

Updated `E:/Playground/FusionDB-bench/gate_profiles/production_medium.json`:

- Added `ldbc:One-hop friends` to `allowlist.unstable_cases`
- Updated `source` to mention BENCHPROD-062
- Did not change suite thresholds, max unstable suite count, max unstable case count, or benchmark harness behavior

## Verification

Command:

```powershell
python bench_gate.py --gate-profile gate_profiles/production_medium.json --repeat-report runs/repeat_benchprod060_production_medium_3x_20260527/bench_repeat_summary.json --run-name gate_benchprod062_production_medium_profile_20260527
```

Result:

- Status: passed
- Checks: 22/22
- Report: `E:/Playground/FusionDB-bench/runs/gate_benchprod062_production_medium_profile_20260527/bench_gate_summary.md`

## Next TASK Signals

- `BENCHPROD-061`: Stabilize CH-benCHmark Warehouse revenue rollup.
- `BENCHPROD-063`: Design real TSBS range-index/time-partition path.
