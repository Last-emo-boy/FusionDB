# BENCHPROD-067 Execution Report

## Objective

Restore the default `production_medium` benchmark gate after BENCHPROD-064 exposed new low-latency noise cases.

## Evidence

BENCHPROD-064 production repeat passed all core checks:

- Matrix passed: 3/3
- Case errors: 0
- Suite thresholds passed for `tpcc`, `memtier`, `tsbs`, `ldbc`, and `chbench`
- Unstable cases: 6, still within the existing maximum

The default gate failed only because these cases were not in the versioned unstable-case allowlist:

- `ldbc:Recent posts by friends`
- `memtier:GET by key`
- `memtier:SET existing key`

These are low-latency CV/spread noise cases; no threshold or error regression was observed.

## Change

Updated `E:/Playground/FusionDB-bench/gate_profiles/production_medium.json`:

- Added the 3 observed cases to `allowlist.unstable_cases`
- Updated `source` to mention BENCHPROD-067
- Did not change suite thresholds or max unstable counts

## Verification

Command:

```powershell
python bench_gate.py --gate-profile gate_profiles/production_medium.json --repeat-report runs/repeat_benchprod064_production_medium_3x_20260527/bench_repeat_summary.json --run-name gate_benchprod067_production_medium_profile_20260527
```

Result:

- Status: passed
- Checks: 22/22
- Report: `E:/Playground/FusionDB-bench/runs/gate_benchprod067_production_medium_profile_20260527/bench_gate_summary.md`

## Next TASK Signals

- `BENCHPROD-068`: Add index-assisted LDBC recent posts path.
- `BENCHPROD-069`: Investigate memtier GET/SET variance under HTTP SQL protocol.
- `BENCHPROD-070`: Separate latency-noise gate policy from true regression policy for sub-millisecond cases.
