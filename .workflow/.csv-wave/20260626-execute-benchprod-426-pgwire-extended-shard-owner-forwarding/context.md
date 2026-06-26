# BENCHPROD-426 Execution Context

## Outcome

Completed `BENCHPROD-426` as the twelfth `P5-3` automatic sharding iteration.

## Implementation

- Extended pgwire shard-owner forwarding from simple query to extended query Parse/Bind/Execute flows.
- Added route-action evaluation for parameterized SQL outside active pgwire transactions.
- Forwarded eligible extended queries through the shard owner's HTTP `/prepare` and `/execute` endpoints while preserving the caller user context and shard-forwarding marker.
- Added best-effort cleanup of the transient owner-node prepared statement after forwarded extended execution.
- Converted owner HTTP JSON query results back into pgwire extended query `DataRow` and `CommandComplete` messages, honoring existing result-format handling.
- Preserved transaction-aware guard behavior; pgwire transaction-local schemas still reject non-local shard-owner writes, and pgwire COPY remains conservative.
- Added an end-to-end two-node pgwire extended-query test using `tokio_postgres::Client::execute`, including verification that the forwarded row lands on the owner and not the local node.
- Updated README and ROADMAP to document pgwire extended-query single-owner point-write forwarding.

## Verification

- `cargo test --test pg_integration test_pg_protocol_extended_query_forwards_non_local_shard_owner_insert -- --nocapture` passed.
- `cargo test --test pg_integration shard_owner -- --nocapture` passed with 6 pgwire shard-owner tests.
- `cargo fmt --check` passed.
- `cargo check --bins` passed.
- `cargo test --lib` passed with 335 tests.
- `cargo test shard_owner -- --nocapture` passed with 7 HTTP shard-owner tests and 6 pgwire shard-owner tests.
- `git diff --check` passed with expected CRLF warnings.

## Commit

- `fba23e6 feat: 支持 pgwire extended 分片 owner 转发`

## Cleanup

- `target/` was measured at approximately 10.94 GiB after verification.
- Ran `cargo clean`, which removed 7516 files and 10.9 GiB of build artifacts.

## Maestro Mode

The canonical `spawn_agents_on_csv` tool was unavailable in this Codex toolset; only generic sub-agent tools were discoverable, and current system rules do not allow unsolicited sub-agent spawning. This iteration used the established local fallback and produced maestro-compatible artifacts.

## Remaining Production Gaps

- `P5-3`: pgwire COPY shard-owner forwarding remains open.
- `P5-3`: Mixed local/non-local point writes and multi-owner point writes remain conservative.
- `P5-3`: Distributed index ownership and maintenance remain open.
- `P5-3`: Broad multi-shard query routing and cross-node query planning remain open.
- `P5-3`: INSERT/COPY routing remains conservative for generated/default primary keys, composite primary keys, `INSERT ... SELECT`, partially nondeterministic VALUES rows, and COPY column lists that omit the primary key.
- `P5-3`: HNSW/vector and trigram in-memory index shard ownership remain open.
- `P3-3`: SCRAM-SHA-256 remains blocked by pgwire 0.37 limitations.
