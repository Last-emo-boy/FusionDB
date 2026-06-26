# BENCHPROD-423 Execution Context

## Outcome

Completed `BENCHPROD-423` as the ninth `P5-3` automatic sharding iteration.

## Implementation

- Added HTTP shard-owner forwarding for `/query` requests whose deterministic routed point writes all target one non-local owner.
- Added a forwarding header so the receiving node can avoid recursive forwarding and fall back to the existing route-conflict guard if the request still is not local.
- Preserved the existing conservative behavior for forwarded requests, disabled test mode, mixed local/non-local writes, and writes spanning multiple non-local owners.
- Forwarded the authenticated user context via `x-fusiondb-user` so owner nodes still run normal authorization checks.
- Added an end-to-end two-node HTTP test that starts an owner-node router on a dynamic port, sends a remote-owner `INSERT` through the local node, and verifies the row lands only on the owner node.
- Updated README and ROADMAP to document that automatic cross-node SQL forwarding has started for HTTP `/query` single-owner deterministic point writes.

## Verification

- `cargo test http_query_forwards_non_local_shard_owner_insert_to_owner -- --nocapture` passed.
- `cargo test shard_owner -- --nocapture` passed with 10 matching tests across lib and pgwire integration suites.
- `cargo fmt --check` passed.
- `cargo check --bins` passed.
- `cargo test --lib` passed with 334 tests.
- `cargo test --test pg_integration shard_owner -- --nocapture` passed with 4 tests.
- `git diff --check` passed with expected CRLF warnings.

## Commit

- `3c762f2 feat: 支持 HTTP 分片 owner 自动转发`

## Cleanup

- `target/` was measured at approximately 23.01 GB after verification.
- Ran `cargo clean`, which removed 17720 files and 23.0 GiB of build artifacts.

## Maestro Mode

The canonical `spawn_agents_on_csv` tool was unavailable in this Codex toolset; only generic sub-agent tools were discoverable, and current system rules do not allow unsolicited sub-agent spawning. This iteration used the established local fallback and produced maestro-compatible artifacts.

## Remaining Production Gaps

- `P5-3`: Prepared HTTP `/execute` shard-owner forwarding remains open.
- `P5-3`: pgwire shard-owner forwarding remains open.
- `P5-3`: Mixed local/non-local point writes and multi-owner point writes remain conservative.
- `P5-3`: Distributed index ownership and maintenance remain open.
- `P5-3`: Broad multi-shard query routing and cross-node query planning remain open.
- `P5-3`: INSERT/COPY routing remains conservative for generated/default primary keys, composite primary keys, `INSERT ... SELECT`, partially nondeterministic VALUES rows, and COPY column lists that omit the primary key.
- `P5-3`: HNSW/vector and trigram in-memory index shard ownership remain open.
- `P3-3`: SCRAM-SHA-256 remains blocked by pgwire 0.37 limitations.
