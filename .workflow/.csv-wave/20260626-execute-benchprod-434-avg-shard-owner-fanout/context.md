# BENCHPROD-434 Execution Context

## Outcome

Completed `BENCHPROD-434` as the twentieth `P5-3` automatic sharding iteration.

## Implementation

- Added executor-level eligibility and planning for conservative single-table `AVG(column)` fan-out.
- Rewrote eligible `AVG(column)` queries to shard-local `SUM(column), COUNT(column)` queries and merged the global average as `SUM / COUNT`.
- Restricted AVG fan-out planning to integer and floating-point table columns after schema lookup; DECIMAL/NUMERIC and non-numeric columns remain local/conservative.
- Reused existing shard-owner SELECT fan-out owner discovery after verifying query shape and numeric column type.
- Added HTTP `/query` AVG fan-out that merges local and remote SUM/COUNT pairs, preserving NULL behavior for empty inputs.
- Added HTTP prepared `/execute` AVG fan-out using rewritten prepared SQL with the original parameter values.
- Added pgwire simple-query AVG fan-out using owner HTTP `/query` responses and pgwire response conversion.
- Added pgwire extended-query AVG fan-out using owner prepared forwarding and extended result encoding.
- Updated README and ROADMAP to document distributed `COUNT(*)`, `SUM(column)`, `MIN(column)`, `MAX(column)`, and `AVG(column)` aggregation.

## Verification

- `cargo fmt --check` passed.
- `git diff --check` passed with expected CRLF warnings.
- `cargo check --bins` passed.
- `cargo test http_query_fanouts_simple_select_across_shard_owners -- --nocapture` passed.
- `cargo test http_execute_forwards_non_local_shard_owner_insert_to_owner -- --nocapture` passed.
- `cargo test --test pg_integration test_pg_protocol_simple_query_forwards_non_local_shard_owner_insert -- --nocapture` passed.
- `cargo test --test pg_integration test_pg_protocol_extended_query_forwards_non_local_shard_owner_insert -- --nocapture` passed.
- `cargo test shard_owner -- --nocapture` passed with 8 HTTP shard-owner tests and 7 pgwire shard-owner tests.
- `cargo test --lib` passed with 336 tests.

## Maestro Mode

The canonical `spawn_agents_on_csv` tool was unavailable in this Codex toolset. A generic sub-agent tool was discoverable, but current tool rules do not allow unsolicited sub-agent spawning without an explicit delegation request. This iteration used the established local fallback and produced maestro-compatible artifacts.

## Remaining Production Gaps

- `P5-3`: Aggregate forms beyond `COUNT(*)`, `SUM(column)`, numeric `MIN(column)`, numeric `MAX(column)`, and numeric `AVG(column)`, including `DISTINCT` aggregate forms, aggregate expressions, DECIMAL/NUMERIC AVG precision handling, `ORDER BY`/`LIMIT`, join, subquery, and set-operation distributed planning remain open.
- `P5-3`: Mixed local/non-local point writes and multi-owner point writes remain conservative.
- `P5-3`: Distributed index ownership and maintenance remain open.
- `P5-3`: INSERT/COPY routing remains conservative for generated/default primary keys, composite primary keys, `INSERT ... SELECT`, partially nondeterministic VALUES rows, and COPY column lists that omit the primary key.
- `P5-3`: HNSW/vector and trigram in-memory index shard ownership remain open.
- `P5-3`: Broad SELECT and aggregate fan-out inside explicit pgwire transactions remain conservative.
- `P3-3`: SCRAM-SHA-256 remains blocked by pgwire 0.37 limitations.
