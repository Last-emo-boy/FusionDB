# BENCHPROD-091 Context

## Result

Added `E:\Playground\FusionDB-bench\pgbench_native_smoke.py`.

Current environment result:

- `pgbench` is not on PATH.
- Native smoke report status is `tool_missing`.
- This is intentionally not treated as a FusionDB pass or failure.

Reports:

- `E:\Playground\FusionDB-bench\runs\pgbench_native_benchprod091_tool_missing_20260528\pgbench_native_smoke_summary.md`
- `E:\Playground\FusionDB-bench\runs\external_smoke_benchprod091_pgbench_native_evidence_20260528\external_smoke_summary.md`

## Behavior

When `pgbench` is available, the runner will:

1. start release FusionDB unless `--reuse-server` is set,
2. run `pgbench --version`,
3. run `pgbench -i -s <scale>`,
4. run a short `pgbench -c <clients> -t <transactions>` transaction smoke unless `--skip-run` is set,
5. save stdout/stderr and classify command failures as `gap`.

## Remaining Work

The next real blocker is external-state dependent: install or provide official PostgreSQL client tools. Once `pgbench.exe` exists, the runner is ready to capture the next concrete FusionDB compatibility gap.
