# BENCHPROD-417 Execution Context

## Outcome

Completed `BENCHPROD-417` as the third `P5-3` automatic sharding iteration.

## Implementation

- Added routed secondary-index helpers for local shard-prefixed BTree keys: `shard:{id}:index:{table}:{column}:{value}:{row_id}`.
- Added routed FTS helpers for local shard-prefixed FTS keys: `shard:{id}:fts:{table}:{column}:{token}:{row_id}`.
- Routed CREATE INDEX backfill, DROP INDEX cleanup, DROP/TRUNCATE cleanup, INSERT, INSERT ... SELECT, UPSERT update, UPDATE, DELETE, column-aggregate index scans, index scan plans, join index probes, composite-index scans, composite unique checks, and composite foreign-key parent checks through shard-aware index prefixes.
- Kept legacy `index:*` and `fts:*` layouts for non-sharded executors.
- Extended sharded executor coverage to assert BTree and FTS entries are stored under shard-prefixed keys, legacy keys are not written, BTree update maintenance works, FTS lookup works, and composite BTree lookup works in sharded mode.
- Updated README and ROADMAP to describe local row/index shard layouts without marking full automatic sharding complete.

## Verification

- `cargo fmt --check` passed.
- `cargo check --bins` passed.
- `cargo test --lib execution::tests::sharded_executor_uses_physical_shard_data_keys_for_crud -- --nocapture` passed.
- `cargo test --lib` passed with 328 tests.
- `cargo test --test sql_index_cache` passed with 38 tests.
- `cargo test --test sql_expr_functions` passed with 22 tests.
- `git diff --check` passed with expected CRLF warnings.

## Commit

- `eb9b0f1 feat: 支持本地分片二级索引布局`

## Maestro Mode

The canonical `spawn_agents_on_csv` tool was unavailable in this Codex toolset, so this iteration used the established local fallback and produced maestro-compatible artifacts.

## Remaining Production Gaps

- `P5-3`: Cross-node SQL execution routing remains open.
- `P5-3`: Distributed index ownership and maintenance remain open.
- `P5-3`: HNSW/vector and trigram in-memory index shard ownership remain open.
- `P3-3`: SCRAM-SHA-256 remains blocked by pgwire 0.37 limitations.
