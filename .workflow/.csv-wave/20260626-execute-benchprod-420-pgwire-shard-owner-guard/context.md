# BENCHPROD-420 Execution Context

## Outcome

Completed `BENCHPROD-420` as the sixth `P5-3` automatic sharding iteration.

## Implementation

- Added pgwire error mapping for deterministic shard-owner route conflicts using SQLSTATE `0A000`.
- Reused executor `SqlShardRoutingDecision` output for pgwire simple and extended query paths.
- Checked simple-query statements immediately before normal statement execution so previous statements in the same simple-query batch can publish schemas before a following point write is routed.
- Checked extended-query portals after parameter decoding and metadata-query bypass, before executing the first statement.
- Fixed pgwire parameter inference so final placeholder count takes the maximum of AST discovery and direct SQL text scanning; this covers `INSERT ... VALUES ($1, $2)` where the AST path did not count `SetExpr::Values` placeholders.
- Updated README and ROADMAP to reflect HTTP/pgwire INSERT/UPDATE/DELETE point-write owner guarding.

## Verification

- `cargo test --test pg_integration test_pg_protocol_extended_query_rejects_non_local_shard_owner_insert -- --nocapture` passed.
- `cargo test --test pg_integration shard_owner -- --nocapture` passed with 2 tests.
- `cargo fmt --check` passed.
- `cargo check --bins` passed.
- `cargo test --lib shard_owner -- --nocapture` passed with 5 tests.
- `cargo test --lib` passed with 333 tests.
- `cargo test --test sql_index_cache` passed with 38 tests.
- `cargo test --test sql_expr_functions` passed with 22 tests.
- `git diff --check` passed with expected CRLF warnings.

## Commit

- `08ad3a3 feat: 支持 pgwire 分片 owner 校验`

## Maestro Mode

The canonical `spawn_agents_on_csv` tool was unavailable in this Codex toolset; only generic sub-agent tools were discoverable, and current system rules do not allow unsolicited sub-agent spawning. This iteration used the established local fallback and produced maestro-compatible artifacts.

## Remaining Production Gaps

- `P5-3`: Automatic cross-node SQL forwarding remains open.
- `P5-3`: Distributed index ownership and maintenance remain open.
- `P5-3`: Broad multi-shard query routing remains open.
- `P5-3`: INSERT routing remains conservative for generated/default primary keys, composite primary keys, `INSERT ... SELECT`, and partially nondeterministic VALUES rows.
- `P5-3`: pgwire COPY row owner routing remains open.
- `P5-3`: Transaction-local newly-created schema route inference remains conservative.
- `P5-3`: HNSW/vector and trigram in-memory index shard ownership remain open.
- `P3-3`: SCRAM-SHA-256 remains blocked by pgwire 0.37 limitations.
