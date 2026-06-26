# BENCHPROD-428 Execution Context

## Outcome

Completed `BENCHPROD-428` as the fourteenth `P5-3` automatic sharding iteration.

## Implementation

- Added executor-level routing decisions for simple single-table `SELECT` statements with a deterministic primary-key equality predicate.
- Added HTTP `/query` primary-key point-read forwarding to the non-local shard owner's HTTP endpoint.
- Added HTTP prepared `/execute` primary-key point-read forwarding using the existing owner prepare/execute forwarding path.
- Added pgwire simple-query primary-key point-read forwarding using the existing owner HTTP `/query` bridge.
- Added pgwire extended-query primary-key point-read forwarding for parameterized reads such as `SELECT ... WHERE id = $1`.
- Kept non-routable SELECT statements conservative; broad scans, joins, composite primary keys, and multi-statement reads continue to execute locally.
- Added transaction safety for pgwire: deterministic non-local point reads inside an active session transaction now return a shard route error instead of silently returning a local empty result.
- Updated existing shard-owner forwarding tests so API-level local SELECTs prove read forwarding works while raw local-storage probes still prove the row was not written locally.
- Updated README and ROADMAP to document primary-key point-read forwarding and the remaining broad query-planning gap.

## Verification

- `cargo test shard_owner -- --nocapture` passed with 7 HTTP shard-owner tests and 7 pgwire shard-owner tests.
- `cargo test --lib` passed with 335 tests.
- `cargo fmt --check` passed.
- `git diff --check` passed with expected CRLF warnings.
- `cargo check --bins` passed.
- `cargo test --test pg_integration shard_owner -- --nocapture` passed with 7 pgwire shard-owner tests.

## Commit

- `a5a5d86 feat: 支持分片 owner 主键点读转发`

## Cleanup

- `target/` was measured at approximately 11.06 GiB after verification.
- Ran `cargo clean`, which removed 7537 files and 11.1 GiB of build artifacts.

## Maestro Mode

The canonical `spawn_agents_on_csv` tool was unavailable in this Codex toolset; only generic sub-agent tools were discoverable, and current system rules do not allow unsolicited sub-agent spawning. This iteration used the established local fallback and produced maestro-compatible artifacts.

## Remaining Production Gaps

- `P5-3`: Mixed local/non-local point writes and multi-owner point writes remain conservative.
- `P5-3`: Distributed index ownership and maintenance remain open.
- `P5-3`: Broad multi-shard query routing and cross-node query planning beyond primary-key point reads remain open.
- `P5-3`: INSERT/COPY routing remains conservative for generated/default primary keys, composite primary keys, `INSERT ... SELECT`, partially nondeterministic VALUES rows, and COPY column lists that omit the primary key.
- `P5-3`: HNSW/vector and trigram in-memory index shard ownership remain open.
- `P3-3`: SCRAM-SHA-256 remains blocked by pgwire 0.37 limitations.
