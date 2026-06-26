# BENCHPROD-430 Execution Context

## Outcome

Completed `BENCHPROD-430` as the sixteenth `P5-3` automatic sharding iteration.

## Implementation

- Extended simple single-table SELECT fan-out to HTTP prepared `/execute`.
- Added HTTP owner prepare/execute/deallocate fan-out for prepared broad scans, reusing the existing forwarded HTTP API contract.
- Preserved existing single-owner point-read forwarding precedence, so deterministic primary-key reads still avoid broad fan-out.
- Extended simple single-table SELECT fan-out to pgwire extended query execution.
- Added pgwire owner prepare/execute/deallocate fan-out for extended broad scans and reused existing extended result encoding.
- Kept explicit pgwire transaction behavior conservative; broad fan-out remains outside active session transactions.
- Added HTTP prepared coverage that returns one local-owned row and one remote-owned row from `SELECT id, name FROM forward_exec_users`.
- Added pgwire extended coverage that returns one local-owned row and one remote-owned row from `SELECT id, name FROM pg_route_extended_forward`.
- Updated README and ROADMAP to document simple SELECT fan-out across HTTP `/query`, HTTP prepared `/execute`, and pgwire simple/extended query.

## Verification

- `cargo check --bins` passed.
- `cargo fmt --check` passed.
- `git diff --check` passed with expected CRLF warnings.
- `cargo test http_execute_forwards_non_local_shard_owner_insert_to_owner -- --nocapture` passed.
- `cargo test --test pg_integration test_pg_protocol_extended_query_forwards_non_local_shard_owner_insert -- --nocapture` passed.
- `cargo test shard_owner -- --nocapture` passed with 8 HTTP shard-owner tests and 7 pgwire shard-owner tests.
- `cargo test --lib` passed with 336 tests.

## Maestro Mode

The canonical `spawn_agents_on_csv` tool was unavailable in this Codex toolset. A generic sub-agent tool was discoverable, but current tool rules do not allow unsolicited sub-agent spawning without an explicit delegation request. This iteration used the established local fallback and produced maestro-compatible artifacts.

## Remaining Production Gaps

- `P5-3`: Aggregate, `DISTINCT`, `ORDER BY`/`LIMIT`, join, subquery, and set-operation distributed planning remain open.
- `P5-3`: Mixed local/non-local point writes and multi-owner point writes remain conservative.
- `P5-3`: Distributed index ownership and maintenance remain open.
- `P5-3`: INSERT/COPY routing remains conservative for generated/default primary keys, composite primary keys, `INSERT ... SELECT`, partially nondeterministic VALUES rows, and COPY column lists that omit the primary key.
- `P5-3`: HNSW/vector and trigram in-memory index shard ownership remain open.
- `P5-3`: Broad SELECT fan-out inside explicit pgwire transactions remains conservative.
- `P3-3`: SCRAM-SHA-256 remains blocked by pgwire 0.37 limitations.
