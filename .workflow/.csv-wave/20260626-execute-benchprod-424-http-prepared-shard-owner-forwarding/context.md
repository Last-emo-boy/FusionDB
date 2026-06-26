# BENCHPROD-424 Execution Context

## Outcome

Completed `BENCHPROD-424` as the tenth `P5-3` automatic sharding iteration.

## Implementation

- Extended HTTP shard-owner forwarding from raw `/query` requests to prepared `/execute` requests.
- Reused the existing owner-node `/prepare` and `/execute` API instead of adding a new internal execution endpoint.
- Forwarded the caller user context and shard-forwarding marker to the owner node.
- Added best-effort cleanup of the transient owner-node prepared statement after forwarded execution.
- Preserved conservative behavior for forwarded requests that still route non-locally, mixed local/non-local writes, and writes spanning multiple non-local owners.
- Added an end-to-end two-node HTTP test for prepared execution forwarding, including verification that the owner-node temporary prepared statement is removed.
- Updated README and ROADMAP to document prepared `/execute` single-owner point-write forwarding.

## Verification

- `cargo test http_execute_forwards_non_local_shard_owner_insert_to_owner -- --nocapture` passed.
- `cargo fmt --check` passed.
- `cargo test shard_owner -- --nocapture` passed with 11 matching tests across lib and pgwire integration suites.
- `cargo check --bins` passed.
- `cargo test --lib` passed with 335 tests.
- `cargo test --test pg_integration shard_owner -- --nocapture` passed with 4 tests.
- `git diff --check` passed with expected CRLF warnings.

## Commit

- `7560814 feat: 支持 HTTP prepared 分片 owner 转发`

## Cleanup

- `target/` was measured at approximately 10.84 GB after verification.
- Ran `cargo clean`, which removed 7494 files and 10.8 GiB of build artifacts.

## Maestro Mode

The canonical `spawn_agents_on_csv` tool was unavailable in this Codex toolset; only generic sub-agent tools were discoverable, and current system rules do not allow unsolicited sub-agent spawning. This iteration used the established local fallback and produced maestro-compatible artifacts.

## Remaining Production Gaps

- `P5-3`: pgwire shard-owner forwarding remains open.
- `P5-3`: Mixed local/non-local point writes and multi-owner point writes remain conservative.
- `P5-3`: Distributed index ownership and maintenance remain open.
- `P5-3`: Broad multi-shard query routing and cross-node query planning remain open.
- `P5-3`: INSERT/COPY routing remains conservative for generated/default primary keys, composite primary keys, `INSERT ... SELECT`, partially nondeterministic VALUES rows, and COPY column lists that omit the primary key.
- `P5-3`: HNSW/vector and trigram in-memory index shard ownership remain open.
- `P3-3`: SCRAM-SHA-256 remains blocked by pgwire 0.37 limitations.
