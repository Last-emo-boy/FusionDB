# BENCHPROD-059 Versioned Benchmark Gate Profiles

Date: 2026-05-27
Scope: benchmark harness policy and workflow evidence only; database code and dashboard/ui unchanged.

## Objective

Move benchmark gate thresholds and unstable allowlists out of `bench_gate.py` and into versioned JSON profiles. This lets gate policy be reviewed and updated independently from benchmark code and database-core optimization work.

## Implementation

Repository: `E:/Playground/FusionDB-bench`

- Added `gate_profiles/production_medium.json`.
- Updated `bench_gate.py` to load a JSON gate profile by default.
- Kept CLI overrides for temporary experiments:
  - `--suite-max-p95-ms suite=value`
  - `--suite-min-ops-sec suite=value`
  - `--allowed-unstable-suite suite`
  - `--allowed-unstable-case suite:case`
  - `--no-default-unstable-allowlist`
  - `--no-default-thresholds`
- Gate reports now record the active gate profile path.
- Updated `README.md` with `--gate-profile` usage.

Bench repo commit: `469639e feat: 增加benchmark门禁配置`

## Profile

Profile path:

`E:/Playground/FusionDB-bench/gate_profiles/production_medium.json`

Profile summary:

| Field | Value |
|---|---|
| schema_version | 1 |
| name | production_medium |
| source | BENCHPROD-052 baseline with BENCHPROD-048 current-noise allowlist update |
| scale | medium |
| suite | production |
| suites | tpcc, memtier, tsbs, ldbc, chbench |
| load_mode | insert |
| min_repeats | 3 |
| max_matrix_failures | 0 |
| max_case_errors | 0 |
| max_unstable_suites | 1 |
| max_unstable_cases | 6 |
| stability_metric | median |

Thresholds:

| Suite | Max P95 ms | Min ops/sec |
|---|---:|---:|
| tpcc | 5.000 | 450.0 |
| memtier | 1.000 | 1100.0 |
| tsbs | 16.000 | 400.0 |
| ldbc | 4.000 | 850.0 |
| chbench | 18.000 | 200.0 |

Unstable allowlist:

- suites: `chbench`, `ldbc`
- cases:
  - `chbench:Customer order join`
  - `chbench:Warehouse revenue rollup`
  - `ldbc:Tag popularity`
  - `ldbc:Two-hop candidates`
  - `memtier:ADD new key`
  - `tpcc:Delivery status update`
  - `tpcc:Stock level query`
  - `tsbs:Fleet rollup by region`
  - `tsbs:Ingest one point`

## Verification

Syntax and JSON checks:

| Command | Result |
|---|---|
| `python -m py_compile bench_gate.py bench_repeat.py bench_stability.py bench_trend.py fusiondb_matrix.py fusiondb_bench.py external_smoke.py external_bootstrap.py` | passed |
| `Get-Content gate_profiles\production_medium.json -Raw \| ConvertFrom-Json` | passed |
| `python bench_gate.py --help` | passed |

Default profile:

```powershell
python bench_gate.py --repeat-report runs\repeat_benchprod048_production_medium_stream_count_3x_20260527\bench_repeat_summary.json --run-name gate_benchprod059_default_profile_20260527
```

Result: passed, `22/22` checks.

Artifact: `E:/Playground/FusionDB-bench/runs/gate_benchprod059_default_profile_20260527/bench_gate_summary.md`

Explicit profile:

```powershell
python bench_gate.py --gate-profile gate_profiles\production_medium.json --repeat-report runs\repeat_benchprod048_production_medium_stream_count_3x_20260527\bench_repeat_summary.json --run-name gate_benchprod059_explicit_profile_20260527
```

Result: passed, `22/22` checks.

Artifact: `E:/Playground/FusionDB-bench/runs/gate_benchprod059_explicit_profile_20260527/bench_gate_summary.md`

Matrix compatibility:

```powershell
python bench_gate.py --repeat-report runs\repeat_benchprod048_production_medium_stream_count_3x_20260527\bench_repeat_summary.json --matrix-report runs\matrix_benchprod052_all_medium_insert_20260527\matrix_summary.json --matrix-suite all --run-name gate_benchprod059_profile_with_matrix_20260527
```

Result: passed, `37/37` checks.

Artifact: `E:/Playground/FusionDB-bench/runs/gate_benchprod059_profile_with_matrix_20260527/bench_gate_summary.md`

Expected threshold failure:

```powershell
python bench_gate.py --repeat-report runs\repeat_benchprod048_production_medium_stream_count_3x_20260527\bench_repeat_summary.json --suite-max-p95-ms tsbs=1 --run-name gate_benchprod059_expected_fail_20260527
```

Result: failed as expected, `21/22` checks. The artificial TSBS P95 threshold rejected the run.

Expected allowlist failure:

```powershell
python bench_gate.py --repeat-report runs\repeat_benchprod048_production_medium_stream_count_3x_20260527\bench_repeat_summary.json --no-default-unstable-allowlist --run-name gate_benchprod059_expected_allowlist_fail_20260527
```

Result: failed as expected, `20/22` checks. Disabling the profile allowlist rejected current unstable suite/cases.

## Assessment

`BENCHPROD-059` separates benchmark gate policy from benchmark code. That matters because recent work showed instability can migrate between cases even when the target optimization succeeds. Future tasks can now update gate policy as a reviewable JSON artifact instead of editing `bench_gate.py`.

## Next TASK Signals

- `BENCHPROD-060`: Optimize TSBS Fleet rollup by region, using `production_medium` gate profile as the acceptance guard.
- `BENCHPROD-061`: Stabilize CH-benCHmark Customer order join and Warehouse revenue rollup.
- `BENCHPROD-063`: Add additional gate profiles for tiny smoke and all-suite medium coverage.
- `BENCHPROD-064`: Add a wrapper command that runs `cargo build`, `bench_repeat.py`, and `bench_gate.py` with a selected profile.
