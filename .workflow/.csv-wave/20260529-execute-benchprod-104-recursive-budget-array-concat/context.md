# BENCHPROD-104: recursive CTE budget and array concat

## Purpose

Continue from BENCHPROD-103, where the LDBC native smoke moved past the correlated `EXISTS` blocker but Q14 timed out after 180 seconds. BENCHPROD-104 turns that timeout into a bounded, diagnosable engine error and fixes the array concatenation shape used by Q14 path expansion.

## Changes

- Updated `src/execution/query/mod.rs` recursive CTE evaluation with explicit safety limits:
  - `MAX_RECURSIVE_CTE_ROWS = 4096`
  - `MAX_RECURSIVE_CTE_ITERATIONS = 128`
- Recursive CTE evaluation now returns a clear error when either limit is reached instead of silently returning a partial result or relying on an external workload timeout.
- Updated `src/execution/expr/value.rs` so `BinaryOperator::StringConcat` preserves array semantics:
  - `array || array` concatenates both arrays.
  - `array || scalar` appends the scalar.
  - `scalar || array` prepends the scalar.
  - Existing scalar string concatenation remains the fallback path.
- Added regression tests in `tests/sql_set_subquery.rs`:
  - `test_recursive_cte_row_budget_fails_fast`
  - `test_recursive_cte_preserves_array_concat_values`

## Evidence

- `cargo fmt --check`: passed.
- `cargo test --test sql_set_subquery -- --nocapture`: passed, `20/20`.
- `cargo test --test sql_expr_functions test_string_concat_operator -- --nocapture`: passed.
- `cargo build --release --bin fusiondb`: passed.
- `cargo test --release --test pg_integration -- --nocapture`: passed, `25/25`.
- LDBC non-isolation smoke:
  - `E:\Playground\FusionDB-bench\runs\ldbc_snb_native_benchprod104_recursive_budget_array_concat_20rows_10ops_20260529\ldbc_snb_native_smoke_summary.json`
  - `status=gap`, `steps=7/8`.
  - `ldbc_command` failed with exit code `1`.
  - Workload ran about 28 seconds and reported `Operations [1]`.
  - Core error: `ERROR: Execution Error: Execution("WITH RECURSIVE CTE search_graph row limit exceeded: max 4096 rows")`.

## Result

BENCHPROD-104 does not make LDBC pass. It improves the frontier by replacing a black-box 180 second Q14 timeout with a fast, explicit recursive CTE row-budget error. The Q14 path expression `path || ARRAY[[x.link, k_person2id]]` is also covered by array concat regression coverage.

## Current Blockers

- Q14 `search_graph` expansion still exceeds the current row budget. The next engine step should reduce row explosion through better recursive pruning, visited-node/path semantics, or a targeted Q14 harness that isolates the failing expansion.
- Q14 downstream SQL remains unproven beyond this frontier: multidimensional array indexing, `generate_subscripts`, `row_number() OVER ()`, and weighted path aggregation.
- The LDBC evidence is still non-isolation `gap` evidence and must not be described as a full native benchmark pass.
- Native memtier remains blocked by missing real `memtier_benchmark` tooling.

## Next Task Candidate

BENCHPROD-105 should either implement a targeted Q14 reproducer/harness around `interactive-complex-14.sql` or directly address `search_graph` recursive row explosion so the workload can progress beyond the 4096-row frontier.
