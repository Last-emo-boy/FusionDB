# BENCHPROD-422 Execution Context

## Outcome

Completed `BENCHPROD-422` as the eighth `P5-3` automatic sharding iteration.

## Implementation

- Added a dedicated `ShardRouteConflict` error variant so COPY owner-routing failures can map to pgwire shard-route SQLSTATE handling instead of generic COPY execution errors.
- Added pgwire `COPY FROM STDIN` row owner validation before inserting parsed COPY rows when sharding is enabled.
- Reused transaction-visible table schema loading so COPY guards see tables created earlier in the same pgwire transaction.
- Kept COPY routing conservative for unroutable cases: missing table schema, no primary key, composite primary key, omitted primary key column, malformed row shape, or primary key values that cannot be converted to a routing row id.
- Mapped COPY shard-route conflicts to pgwire shard-route errors and SQLSTATE `0A000`.
- Added pgwire integration coverage for successful local-owner COPY rows and rejected remote-owner COPY rows, including a table created inside an active transaction.
- Updated README and ROADMAP to document COPY point-write owner guard coverage.

## Verification

- `cargo test --test pg_integration test_pg_protocol_copy_from_stdin_rejects_non_local_shard_owner_rows -- --nocapture` passed.
- `cargo test --test pg_integration shard_owner -- --nocapture` passed with 4 tests.
- `cargo test --test pg_integration -- --nocapture` passed with 33 tests.
- `cargo fmt --check` passed.
- `cargo check --bins` passed.
- `cargo test --lib shard_owner -- --nocapture` passed with 5 tests.
- `cargo test --lib` passed with 333 tests.
- `cargo test --test sql_index_cache` passed with 38 tests.
- `cargo test --test sql_expr_functions` passed with 22 tests.
- `git diff --check` passed with expected CRLF warnings.

## Commit

- `76a0504 feat: 支持 pgwire COPY 分片 owner 校验`

## Maestro Mode

The canonical `spawn_agents_on_csv` tool was unavailable in this Codex toolset; only generic sub-agent tools were discoverable, and current system rules do not allow unsolicited sub-agent spawning. This iteration used the established local fallback and produced maestro-compatible artifacts.

## Remaining Production Gaps

- `P5-3`: Automatic cross-node SQL forwarding remains open.
- `P5-3`: Distributed index ownership and maintenance remain open.
- `P5-3`: Broad multi-shard query routing remains open.
- `P5-3`: INSERT/COPY routing remains conservative for generated/default primary keys, composite primary keys, `INSERT ... SELECT`, partially nondeterministic VALUES rows, and COPY column lists that omit the primary key.
- `P5-3`: HNSW/vector and trigram in-memory index shard ownership remain open.
- `P3-3`: SCRAM-SHA-256 remains blocked by pgwire 0.37 limitations.
