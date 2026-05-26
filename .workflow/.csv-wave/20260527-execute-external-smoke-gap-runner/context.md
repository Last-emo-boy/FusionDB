# External Benchmark Smoke/Gap Runner

Date: 2026-05-27
Scope: benchmark harness and workflow evidence only; FusionDB database core and dashboard/ui unchanged.

## Objective

Move the production benchmark program closer to real external workloads by adding a structured smoke/gap runner for native external tools:

- pgbench
- sysbench
- memtier_benchmark

This task does not claim official benchmark readiness. It replaces informal notes with reproducible JSON/Markdown evidence about tool availability and adapter/protocol blockers.

## Implementation

- Added `E:/Playground/FusionDB-bench/external_smoke.py`.
- Added README usage for external benchmark smoke/gap detection.
- The runner detects tool availability on `PATH` and emits:
  - `external_smoke_summary.json`
  - `external_smoke_summary.md`
- The runner exits successfully for structured `tool_missing` results so CI/development runs can preserve the gap report instead of failing before evidence is written.

## Verification

Commands:

```powershell
cd E:\Playground\FusionDB-bench
python -m py_compile external_smoke.py
python external_smoke.py --target all --run-name external_smoke_benchprod020_022_20260527
```

Artifacts:

- Markdown: `E:/Playground/FusionDB-bench/runs/external_smoke_benchprod020_022_20260527/external_smoke_summary.md`
- JSON: `E:/Playground/FusionDB-bench/runs/external_smoke_benchprod020_022_20260527/external_smoke_summary.json`

## Result

| Target | Status | Meaning |
|---|---|---|
| pgbench | tool_missing | `pgbench` was not found on PATH; native pgbench execution still pending. |
| sysbench | tool_missing | `sysbench` was not found on PATH; native sysbench execution still pending. |
| memtier | tool_missing | `memtier_benchmark` was not found on PATH; native Redis/Memcached protocol benchmark still pending. |

## Production Gap

- pgbench: tool install plus PostgreSQL DDL/COPY/DATE/NUMERIC/prepared-query parity still required.
- sysbench: tool install plus PostgreSQL/MySQL dialect adapter and lifecycle scripts still required.
- memtier: tool install plus Redis/Memcached-compatible protocol decision remains required; current `memtier` suite is SQL KV-like only.

## Related TASK Status

- `BENCHPROD-020`: gap documented, still blocked on external `pgbench` availability and native adapter execution.
- `BENCHPROD-022`: gap documented, still blocked on external `sysbench` availability and native adapter execution.
- `BENCHPROD-010`: native memtier remains a protocol decision; SQL KV-like benchmark coverage already exists.
