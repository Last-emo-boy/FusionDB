# BENCHPROD-421 Execution Context

## Outcome

Completed `BENCHPROD-421` as the seventh `P5-3` automatic sharding iteration.

## Implementation

- Added an executor routing-decision entry point that accepts the caller's active transaction instead of always opening a fresh transaction.
- Updated pgwire simple-query shard-owner guard to use the current session transaction when one is active.
- Updated pgwire extended-query shard-owner guard to parse SQL once and use the current session transaction before executing a portal.
- Updated extended-query parameter inference so `Parse` reads table schemas from the current session transaction when available. This prevents `$1` type fallback to TEXT for tables created earlier in the same uncommitted transaction.
- Added pgwire integration coverage for simple and extended INSERT rejection against tables created earlier in the same session transaction.
- Updated README and ROADMAP to document pgwire transaction-visible schema routing for deterministic shard-owner point writes.

## Verification

- `cargo test --test pg_integration test_pg_protocol_extended_query_rejects_transaction_local_shard_owner_insert -- --nocapture` passed.
- `cargo test --test pg_integration shard_owner -- --nocapture` passed with 3 tests.
- `cargo test --test pg_integration -- --nocapture` passed with 32 tests.
- `cargo fmt --check` passed.
- `cargo check --bins` passed.
- `cargo test --lib shard_owner -- --nocapture` passed with 5 tests.
- `cargo test --lib` passed with 333 tests.
- `cargo test --test sql_index_cache` passed with 38 tests.
- `cargo test --test sql_expr_functions` passed with 22 tests.
- `git diff --check` passed with expected CRLF warnings.

## Commit

- `49ce852 feat: 支持 pgwire 事务内分片 owner 校验`

## Maestro Mode

The canonical `spawn_agents_on_csv` tool was unavailable in this Codex toolset; only generic sub-agent tools were discoverable, and current system rules do not allow unsolicited sub-agent spawning. This iteration used the established local fallback and produced maestro-compatible artifacts.

## Remaining Production Gaps

- `P5-3`: Automatic cross-node SQL forwarding remains open.
- `P5-3`: Distributed index ownership and maintenance remain open.
- `P5-3`: Broad multi-shard query routing remains open.
- `P5-3`: INSERT routing remains conservative for generated/default primary keys, composite primary keys, `INSERT ... SELECT`, and partially nondeterministic VALUES rows.
- `P5-3`: pgwire COPY row owner routing remains open.
- `P5-3`: HNSW/vector and trigram in-memory index shard ownership remain open.
- `P3-3`: SCRAM-SHA-256 remains blocked by pgwire 0.37 limitations.
