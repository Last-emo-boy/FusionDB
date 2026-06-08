# BENCHPROD-088 External Readiness Evidence

## Status

Completed on 2026-05-27. External readiness reporting now links missing external tools/artifacts with the latest PgWire/JDBC catalog evidence.

## Why This Task

BENCHPROD-087 expanded FusionDB's PgWire catalog surface, but `external_smoke.py` still reported only missing tools or benchmark artifacts. For production benchmark iteration, every external gap report should preserve two facts at once:

- what external artifact is missing,
- what FusionDB-side prerequisite evidence already exists.

## Implementation

Updated `E:/Playground/FusionDB-bench/external_smoke.py`.

New behavior:

- accepts `--pgwire-evidence <pgwire_smoke_summary.json>`,
- defaults to the latest `runs/pgwire_smoke*/pgwire_smoke_summary.json` when omitted,
- parses passed PgWire smoke steps,
- adds FusionDB readiness evidence to PostgreSQL/PgWire/JDBC target details,
- preserves `tool_missing` and `artifact_missing` statuses.

## Verification Evidence

- `python -m py_compile external_smoke.py pgwire_smoke.py`: passed.
- `python external_smoke.py --target pgbench,benchbase-tpcc,ldbc,chbenchmark --run-name external_smoke_benchprod088_pgwire_evidence_20260527 --pgwire-evidence runs\pgwire_smoke_benchprod087_jdbc_catalog_final_20260527\pgwire_smoke_summary.json`: passed.
- Report: `E:/Playground/FusionDB-bench/runs/external_smoke_benchprod088_pgwire_evidence_20260527/external_smoke_summary.md`.
- `python -m py_compile pgwire_smoke.py fusiondb_bench.py bench_repeat.py bench_stability.py bench_gate.py fusiondb_matrix.py external_smoke.py external_bootstrap.py`: passed.

## Environment Findings

- Java is available at `C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot\bin\java.exe`.
- `pgbench` and `psql` are not on PATH.
- No matching PostgreSQL JDBC / BenchBase / LDBC / CH-benCHmark jar was found under `E:/Playground` via `rg --files E:\Playground -g '*.jar' | rg -i 'postgres|jdbc|benchbase|ldbc|chbench|chbenchmark'`.

## Output Signal

The external smoke report still marks:

- `pgbench`: `tool_missing`
- `benchbase-tpcc`: `artifact_missing`
- `ldbc`: `artifact_missing`
- `chbenchmark`: `artifact_missing`

Each PostgreSQL/PgWire/JDBC target now also references the BENCHPROD-087 PgWire smoke:

- `status=passed`, `passed_steps=18/18`
- covered steps include extended metadata, `COPY FROM STDIN`, information schema, `pg_catalog.pg_type`, `pg_catalog.pg_class`, `pg_catalog.pg_namespace`, `pg_catalog.pg_database`, `current_setting`, `SHOW server_version`, and `pg_catalog.pg_attribute`.

## Caveats

This still is not a real pgbench, JDBC, BenchBase, LDBC, or CH-benCHmark execution. It is a readiness report upgrade so the next external-tool task can distinguish missing local artifacts from FusionDB protocol/catalog blockers.

## Next Task

BENCHPROD-089 should turn one external target into a real execution path as soon as a tool is available. Best next options:

- install/configure PostgreSQL client tools and add a `pgbench --initialize` compatibility probe,
- or place PostgreSQL JDBC driver under `E:/Playground` and add a Java JDBC smoke against FusionDB.
