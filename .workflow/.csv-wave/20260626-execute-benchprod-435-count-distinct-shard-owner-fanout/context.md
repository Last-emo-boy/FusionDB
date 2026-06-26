# BENCHPROD-435 Execution Context

## Outcome

Completed `BENCHPROD-435` as the twenty-first `P5-3` automatic sharding iteration.

## Implementation

- Added executor-level planning for conservative single-table `COUNT(DISTINCT column)` fan-out.
- Rewrote eligible `COUNT(DISTINCT column)` queries to shard-local `SELECT DISTINCT column` queries.
- Merged returned non-NULL JSON values in the coordinator with a deterministic set before returning the global count.
- Kept eligibility conservative: one statement, one table, one projection, no joins, no GROUP BY/HAVING, no SELECT DISTINCT projection, no ORDER BY/LIMIT, and only identifier or compound-identifier distinct arguments.
- Added HTTP `/query` COUNT DISTINCT fan-out.
- Added HTTP prepared `/execute` COUNT DISTINCT fan-out using rewritten prepared SQL with original parameters.
- Added pgwire simple-query COUNT DISTINCT fan-out.
- Added pgwire extended-query COUNT DISTINCT fan-out.
- Extended shard-owner tests so duplicate values exist on local and remote owners, proving the implementation does not add shard-local distinct counts.
- Updated README and ROADMAP to document distributed `COUNT(DISTINCT column)` aggregation.

## Verification

- `cargo fmt` passed.
- `cargo check --bins` passed.
- `cargo test http_query_fanouts_simple_select_across_shard_owners -- --nocapture` passed.
- `cargo test http_execute_forwards_non_local_shard_owner_insert_to_owner -- --nocapture` passed.
- `cargo test --test pg_integration test_pg_protocol_simple_query_forwards_non_local_shard_owner_insert -- --nocapture` passed.
- `cargo test --test pg_integration test_pg_protocol_extended_query_forwards_non_local_shard_owner_insert -- --nocapture` passed.
- `cargo test shard_owner -- --nocapture` passed with 8 HTTP shard-owner tests and 7 pgwire shard-owner tests.
- `cargo test --lib` passed with 336 tests.

## Maestro Mode

The canonical `spawn_agents_on_csv` tool was unavailable in this Codex toolset. This iteration used the established local fallback and produced maestro-compatible artifacts.

## Remaining Production Gaps

- `P5-3`: Aggregate forms beyond `COUNT(*)`, `COUNT(DISTINCT column)`, `SUM(column)`, numeric `MIN(column)`, numeric `MAX(column)`, and numeric `AVG(column)`, including multi-column/expression DISTINCT aggregate forms, DECIMAL/NUMERIC AVG precision handling, `ORDER BY`/`LIMIT`, join, subquery, and set-operation distributed planning remain open.
- `P5-3`: Mixed local/non-local point writes and multi-owner point writes remain conservative.
- `P5-3`: Distributed index ownership and maintenance remain open.
- `P5-3`: INSERT/COPY routing remains conservative for generated/default primary keys, composite primary keys, `INSERT ... SELECT`, partially nondeterministic VALUES rows, and COPY column lists that omit the primary key.
- `P5-3`: HNSW/vector and trigram in-memory index shard ownership remain open.
- `P5-3`: Broad SELECT and aggregate fan-out inside explicit pgwire transactions remain conservative.
- `P3-3`: SCRAM-SHA-256 remains blocked by pgwire 0.37 limitations.
