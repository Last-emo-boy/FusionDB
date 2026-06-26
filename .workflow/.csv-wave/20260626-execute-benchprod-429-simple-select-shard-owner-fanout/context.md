# BENCHPROD-429 Execution Context

## Outcome

Completed `BENCHPROD-429` as the fifteenth `P5-3` automatic sharding iteration.

## Implementation

- Added executor-level planning for simple single-table SELECT fan-out across unique non-local shard owners.
- Kept fan-out conservative: only one plain table, no joins, no CTE, no DISTINCT, no GROUP BY/HAVING, no ORDER BY/LIMIT, no subquery predicates, and only simple column/wildcard projections.
- Suppressed fan-out for deterministic primary-key point reads so the existing single-owner point-read forwarding path remains preferred.
- Added HTTP `/query` fan-out that executes locally, queries each non-local owner through forwarded `/query`, verifies column consistency, and merges row results.
- Added pgwire simple-query fan-out that uses owner HTTP `/query` responses and converts the merged result back into pgwire rows.
- Left HTTP prepared `/execute` and pgwire extended-query fan-out open because parameterized broad scans need owner prepare/execute fan-out and result-format handling.
- Added HTTP and pgwire integration coverage with one local-owned row and one remote-owned row returned by a broad `SELECT id, name FROM ...`.
- Updated README and ROADMAP to document simple SELECT fan-out and its conservative boundaries.

## Verification

- `cargo check --bins` passed.
- `cargo test http_query_fanouts_simple_select_across_shard_owners -- --nocapture` passed.
- `cargo test --test pg_integration test_pg_protocol_simple_query_forwards_non_local_shard_owner_insert -- --nocapture` passed.
- `cargo test shard_owner -- --nocapture` passed with 8 HTTP shard-owner tests and 7 pgwire shard-owner tests.
- `cargo test --lib` passed with 336 tests.
- `cargo fmt --check` passed.
- `git diff --check` passed with expected CRLF warnings.
- `cargo test --test pg_integration shard_owner -- --nocapture` passed with 7 pgwire shard-owner tests.

## Commit

- `3aac6f3 feat: 支持简单 SELECT 分片 owner fanout`

## Cleanup

- `target/` was measured at approximately 14.71 GiB after verification.
- Ran `cargo clean`, which removed 9374 files and 14.7 GiB of build artifacts.

## Maestro Mode

The canonical `spawn_agents_on_csv` tool was unavailable in this Codex toolset; only generic sub-agent tools were discoverable, and current system rules do not allow unsolicited sub-agent spawning. This iteration used the established local fallback and produced maestro-compatible artifacts.

## Remaining Production Gaps

- `P5-3`: HTTP prepared `/execute` and pgwire extended-query broad SELECT fan-out remain open.
- `P5-3`: Aggregate, `DISTINCT`, `ORDER BY`/`LIMIT`, join, subquery, and set-operation distributed planning remain open.
- `P5-3`: Mixed local/non-local point writes and multi-owner point writes remain conservative.
- `P5-3`: Distributed index ownership and maintenance remain open.
- `P5-3`: INSERT/COPY routing remains conservative for generated/default primary keys, composite primary keys, `INSERT ... SELECT`, partially nondeterministic VALUES rows, and COPY column lists that omit the primary key.
- `P5-3`: HNSW/vector and trigram in-memory index shard ownership remain open.
- `P3-3`: SCRAM-SHA-256 remains blocked by pgwire 0.37 limitations.
