# BENCHPROD-431 Execution Context

## Outcome

Completed `BENCHPROD-431` as the seventeenth `P5-3` automatic sharding iteration.

## Implementation

- Added executor-level eligibility for conservative single-table `COUNT(*)` fan-out.
- Reused existing shard-owner fan-out owner discovery while preserving primary-key point-read forwarding precedence.
- Added HTTP `/query` count fan-out that executes local and remote `COUNT(*)`, validates one-row/one-column count results, and returns the summed total.
- Added HTTP prepared `/execute` count fan-out using owner prepare/execute/deallocate forwarding.
- Added pgwire simple-query count fan-out using owner HTTP `/query` responses and pgwire response conversion.
- Added pgwire extended-query count fan-out using owner prepare/execute/deallocate forwarding and extended result encoding.
- Kept the scope conservative: one table, no joins, no CTE, no DISTINCT, no GROUP BY/HAVING, no ORDER BY/LIMIT, and `COUNT(*)` only.
- Updated README and ROADMAP to document distributed `COUNT(*)` summation and clarify that other aggregate planning remains open.

## Verification

- `cargo check --bins` passed.
- `cargo fmt --check` passed.
- `git diff --check` passed with expected CRLF warnings.
- `cargo test http_query_fanouts_simple_select_across_shard_owners -- --nocapture` passed.
- `cargo test http_execute_forwards_non_local_shard_owner_insert_to_owner -- --nocapture` passed.
- `cargo test --test pg_integration test_pg_protocol_simple_query_forwards_non_local_shard_owner_insert -- --nocapture` passed.
- `cargo test --test pg_integration test_pg_protocol_extended_query_forwards_non_local_shard_owner_insert -- --nocapture` passed.
- `cargo test shard_owner -- --nocapture` passed with 8 HTTP shard-owner tests and 7 pgwire shard-owner tests.
- `cargo test --lib` passed with 336 tests.

## Maestro Mode

The canonical `spawn_agents_on_csv` tool was unavailable in this Codex toolset. A generic sub-agent tool was discoverable, but current tool rules do not allow unsolicited sub-agent spawning without an explicit delegation request. This iteration used the established local fallback and produced maestro-compatible artifacts.

## Remaining Production Gaps

- `P5-3`: Aggregates beyond `COUNT(*)`, `DISTINCT`, `ORDER BY`/`LIMIT`, join, subquery, and set-operation distributed planning remain open.
- `P5-3`: Mixed local/non-local point writes and multi-owner point writes remain conservative.
- `P5-3`: Distributed index ownership and maintenance remain open.
- `P5-3`: INSERT/COPY routing remains conservative for generated/default primary keys, composite primary keys, `INSERT ... SELECT`, partially nondeterministic VALUES rows, and COPY column lists that omit the primary key.
- `P5-3`: HNSW/vector and trigram in-memory index shard ownership remain open.
- `P5-3`: Broad SELECT and `COUNT(*)` fan-out inside explicit pgwire transactions remain conservative.
- `P3-3`: SCRAM-SHA-256 remains blocked by pgwire 0.37 limitations.
