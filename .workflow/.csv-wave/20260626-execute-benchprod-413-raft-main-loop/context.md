# BENCHPROD-413 Execution Context

## Outcome

Completed `BENCHPROD-413` by closing the `P5-1` OpenRaft main-loop wiring roadmap item.

## Implementation

- Added `[distributed]` configuration with Raft enablement, node id, advertised address, bootstrap behavior, cluster name, and initial member list.
- Wired optional Raft node startup into `start_server`, including bootstrap membership construction and distributed capability reporting.
- Merged `/raft/*` routes into the HTTP server when distributed mode is enabled.
- Routed HTTP `/query` write statements through Raft client writes while preserving local execution for reads.
- Added leader forwarding for Raft writes using the leader node address returned by OpenRaft.
- Added `/raft/query` for read-only follower-local reads with optional linearizable reads.
- Added `Executor::sql_requires_raft_write` to classify custom commands and parser-backed SQL statements.
- Updated default config, README, and ROADMAP to document the current distributed state and limitations.

## Verification

- `cargo fmt --check` passed.
- `git diff --check` passed with expected CRLF warnings.
- `cargo check --bins` passed.
- `cargo test --lib` passed with 319 tests.

## Commit

- `e0b36c5 feat: 接入 OpenRaft 主循环`

## Maestro Mode

The canonical `spawn_agents_on_csv` tool was unavailable in this Codex toolset, so this iteration used the established local fallback and produced maestro-compatible artifacts.

## Remaining Production Gaps

- `P3-3`: SCRAM-SHA-256 remains blocked by pgwire 0.37 limitations.
- `P5-2`: Snapshot transfer for new node bootstrap.
- `P5-3`: Automatic sharding with hash/range partitioning.
