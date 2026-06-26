# BENCHPROD-432 Execution Context

## Outcome

Completed `BENCHPROD-432` as the eighteenth `P5-3` automatic sharding iteration.

## Implementation

- Added executor-level eligibility for conservative single-table `SUM(column)` fan-out.
- Reused existing shard-owner SELECT fan-out owner discovery after verifying the query shape is local-merge safe.
- Added HTTP `/query` SUM fan-out that executes local and remote shard-owner sums, validates one-row/one-column results, handles NULL shard sums, and returns the merged total.
- Added HTTP prepared `/execute` SUM fan-out using the existing prepared owner fan-out transport.
- Added pgwire simple-query SUM fan-out using owner HTTP `/query` responses and pgwire response conversion.
- Added pgwire extended-query SUM fan-out using owner prepared forwarding and extended result encoding.
- Adjusted pgwire projection type inference so `SUM(integer_column)` describes as `INT8`, floating SUM describes as `FLOAT8`, numeric SUM describes as `NUMERIC`, and `MIN`/`MAX` preserve their argument type.
- Kept the scope conservative: one table, no joins, no CTE, no DISTINCT, no GROUP BY/HAVING, no ORDER BY/LIMIT, and `SUM(identifier)` or `SUM(qualified.identifier)` only.
- Updated README and ROADMAP to document distributed `COUNT(*)` and `SUM(column)` summation.

## Verification

- `cargo fmt --check` passed.
- `git diff --check` passed with expected CRLF warnings.
- `cargo check --bins` passed.
- `cargo test http_query_fanouts_simple_select_across_shard_owners -- --nocapture` passed.
- `cargo test http_execute_forwards_non_local_shard_owner_insert_to_owner -- --nocapture` passed.
- `cargo test --test pg_integration test_pg_protocol_simple_query_forwards_non_local_shard_owner_insert -- --nocapture` passed.
- `cargo test --test pg_integration test_pg_protocol_extended_query_forwards_non_local_shard_owner_insert -- --nocapture` passed after the pgwire SUM type inference fix.
- `cargo test shard_owner -- --nocapture` passed with 8 HTTP shard-owner tests and 7 pgwire shard-owner tests.
- `cargo test --lib` passed with 336 tests.

## Maestro Mode

The canonical `spawn_agents_on_csv` tool was unavailable in this Codex toolset. A generic sub-agent tool was discoverable, but current tool rules do not allow unsolicited sub-agent spawning without an explicit delegation request. This iteration used the established local fallback and produced maestro-compatible artifacts.

## Remaining Production Gaps

- `P5-3`: Aggregates beyond `COUNT(*)` and `SUM(column)`, `SUM(DISTINCT ...)`, aggregate expressions, `DISTINCT`, `ORDER BY`/`LIMIT`, join, subquery, and set-operation distributed planning remain open.
- `P5-3`: Mixed local/non-local point writes and multi-owner point writes remain conservative.
- `P5-3`: Distributed index ownership and maintenance remain open.
- `P5-3`: INSERT/COPY routing remains conservative for generated/default primary keys, composite primary keys, `INSERT ... SELECT`, partially nondeterministic VALUES rows, and COPY column lists that omit the primary key.
- `P5-3`: HNSW/vector and trigram in-memory index shard ownership remain open.
- `P5-3`: Broad SELECT and aggregate fan-out inside explicit pgwire transactions remain conservative.
- `P3-3`: SCRAM-SHA-256 remains blocked by pgwire 0.37 limitations.
