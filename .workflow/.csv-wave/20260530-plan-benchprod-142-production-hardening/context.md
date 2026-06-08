# BENCHPROD-142+ Production Hardening Plan

## Purpose

Continue the benchmark-system and database-production objective after BENCHPROD-141. The current local medium gate is healthy, CH-benCHmark Q1-Q22 one-hot coverage is complete, and 60s mixed HTAP has passing evidence. The next phase should move from local coverage toward production confidence: recovery, longer mixed workloads, native tool completeness, and official-shape benchmark runs.

## Current Baseline

- Production medium gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod140_medium_explicit_q1_q22_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`.
- Strict native gate:
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod140_strict_explicit_q1_q22_20260529\bench_gate_summary.json`
  - status `passed`, checks `54/54`.
- CH-benCHmark Q1-Q22 matrix:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_query_class_matrix_benchprod140_q1_q2_q3_q4_q5_q6_q7_q8_q9_q10_q11_q12_q13_q14_q15_q16_q17_q18_q19_q20_q21_q22_20260529\chbenchmark_query_class_matrix_summary.json`
  - status `passed`, `22/22`.
- CH-benCHmark 60s mixed HTAP:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_native_benchprod141_mixed_htap_60s_after_limit_fix_20260530\chbenchmark_native_smoke_summary.json`
  - status `passed`, completed `38`.
- CH-benCHmark longer mixed HTAP:
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_native_benchprod143_mixed_htap_300s_t1_20260530\chbenchmark_native_smoke_summary.json`
  - status `passed`, duration `300s`, terminals `1`, completed `120`, TPC-C classes `5/5`, CH query classes `18/22`.
  - `E:\Playground\FusionDB-bench\runs\chbenchmark_native_benchprod143_mixed_htap_300s_t2_after_40001_20260530\chbenchmark_native_smoke_summary.json`
  - status `passed`, duration `300s`, terminals `2`, completed `243`, TPC-C classes `5/5`, CH query classes `20/22`.
  - `E:\Playground\FusionDB-bench\runs\gate_benchprod143_medium_chbenchmark_300s_t2_40001_recovery_20260530\bench_gate_summary.json`
  - status `passed`, checks `86/86`.
- Stability evidence:
  - `E:\Playground\FusionDB-bench\runs\benchprod_current_medium_production_3x_20260528_fix2\stability\bench_stability_summary.json`
  - `0` unstable suites, `0` unstable cases.

## Planned Tasks

### BENCHPROD-142: Recovery Gate Smoke

Build or enable a recovery smoke that proves checkpoint/snapshot, forced process kill, restart, WAL replay, and post-recovery query correctness on a mixed write/read dataset.

Acceptance evidence:

- `recovery_smoke_summary.json` with `status=passed`.
- At least one forced-kill run and one clean-shutdown/restart run.
- Gate profile can consume the report with `recovery_smoke_enabled=true`.

### BENCHPROD-143: CH-benCHmark Longer Mixed HTAP

Extend BENCHPROD-141 from `60s`, `1` terminal to a longer and larger run.

Acceptance evidence:

- `chbenchmark_native_smoke_summary.json` with `status=passed`.
- Duration at least `300s`.
- More than one terminal, or a documented reason why the current driver/harness cannot scale terminals yet.
- Coverage and throughput recorded separately from official-score claims.

Status: done. The `300s` / `1` terminal run passed. The first `300s` / `2` terminal run exposed a real PgWire SQLSTATE blocker: FusionDB returned write conflicts as `XX000`, so BenchBase counted TPC-C `Payment` and `Delivery` conflicts as unexpected SQL errors. BENCHPROD-143 fixed this by mapping FusionDB write conflicts to PostgreSQL `40001` serialization failures. The follow-up full `300s` / `2` terminal run passed and showed conflicts under BenchBase `Rejected Transactions (Server Retry)` with no unexpected SQL errors. The task has moved from "cannot scale terminals" to verified retryable concurrency behavior.

### BENCHPROD-144: Native memtier Completion

Close the native memtier gap by installing/routing a real `memtier_benchmark` binary or strengthening the Redis-compatible endpoint until a real probe runs.

Acceptance evidence:

- `memtier_native_smoke_summary.json` with `status=passed`.
- RESP command coverage for GET/SET/ADD-style paths used by the production matrix.
- Gate profile consumes native memtier passed evidence.

Status: pending. BENCHPROD-144 has current blocker evidence, not a pass. `E:\Playground\FusionDB-bench\runs\memtier_native_benchprod144_resp_preflight_20260530\memtier_native_smoke_summary.json` shows FusionDB can start the Redis-compatible endpoint and pass direct RESP preflight for `PING`, `ECHO`, `SELECT`, `SET`, `GET`, `MSET`, `MGET`, `EXISTS`, `INCR`, `INFO`, `DEL`, and `QUIT`. `E:\Playground\FusionDB-bench\runs\memtier_native_benchprod144_detect_after_wsl_local_path_20260530\memtier_native_smoke_summary.json` still reports `tool_missing`: Windows PATH has no `memtier_benchmark`, default WSL has no `memtier_benchmark` in PATH or `~/src/memtier_benchmark/memtier_benchmark`, and WSL is missing `autoconf`, `automake`, `libtool`, `libpcre3-dev`, `libevent-dev`, `pkg-config`, and `libssl-dev`. The smoke harness now detects a WSL local source-build binary under `~/src/memtier_benchmark/memtier_benchmark`, so the next operator step can build without requiring `sudo install` to `/usr/local/bin`. The evidence chain was refreshed in `E:\Playground\FusionDB-bench\runs\external_smoke_benchprod144_production_targets_memtier_resp_ldbc_command_gap_20260530\external_smoke_summary.json`, and the medium gate still passes with this explicit memtier blocker via `E:\Playground\FusionDB-bench\runs\gate_benchprod144_medium_memtier_resp_preflight_ldbc_command_gap_20260530\bench_gate_summary.json` (`86/86`).

### BENCHPROD-145: LDBC Official-Shape Gap Reduction

Continue the LDBC path beyond the current Q1-Q14 command-mode matrix by handling the remaining official implementation blockers.

Acceptance evidence:

- Focused tests for the next unsupported SQL feature, likely recursive CTE/window/array shapes around Query 12-14.
- Native LDBC command report with broader operation count or explicit next blocker.
- Keep `full_official_ldbc_pass=false` until mixed updates and official scheduling are actually covered.

### BENCHPROD-146: Larger-Scale Production Repeat

Run a larger scale or higher concurrency production repeat across TPC-C, memtier, TSBS, LDBC, and CH-benCHmark.

Acceptance evidence:

- `bench_repeat_summary.json` with at least `3` repeats.
- Stability report with suite and case-level p95/ops.
- Gate result using a separate larger-scale profile or explicit command-line thresholds.

## Guardrails

- Keep official-score claims separate from local gate evidence.
- Do not mark the long-running production objective complete until recovery, larger scale, native memtier, official-shape CH-benCHmark/LDBC, and sustained HTAP evidence all exist.
- Treat test timeouts as production hardening signal when they map to benchmark-shaped paths.
