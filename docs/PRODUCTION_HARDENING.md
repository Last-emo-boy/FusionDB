# FusionDB Production Hardening Plan

Last updated: 2026-07-11.

This plan prioritizes correctness, recoverability, and operability over feature
count. A phase is complete only when its behavior is durable, observable,
upgradeable, and exercised under injected failures.

## Baseline Closed in the Current Pass

- WAL append failures roll back to the last durable segment/offset. A writer is
  permanently poisoned when rollback durability cannot be confirmed.
- Active MVCC readers are registered atomically with the compaction watermark.
- HNSW and trigram state is rebuilt from visible durable rows, published
  atomically, bypassed by stale snapshots, and streamed without materializing a
  second full copy of the visible keyspace.
- SSTable readers reject truncated, overlong, and trailing-byte structures.
- Composite UNIQUE enforcement uses transactionally maintained, index-scoped
  sentinels with `NULLS DISTINCT` semantics.
- Raft vote, log, applied boundary, and snapshot state use versioned,
  length-delimited, CRC-protected atomic files.
- Snapshot installation uses a CRC-framed, SHA-256-bound durable intent and an
  atomically published data marker. Data plus marker is the commit point;
  post-commit metadata or cleanup failures retain the intent and return success
  to OpenRaft, and startup reconciles every interrupted phase.
- Raft replicates a deterministic mutation batch evaluated once by the leader;
  unsupported write paths fail closed instead of applying raw SQL per replica.
- Redis and global side-index endpoints have explicit authentication, resource,
  and deployment boundaries; unsupported Redis TTL semantics fail closed.
- Data V2 has a bounded, versioned, typed codec with exact route/table prefixes,
  ordered binary row IDs, strict decoding, and property-style malformed-input
  tests. The first migration stage mirrors base-row writes transactionally while
  legacy keys remain the sole read authority; deletes and table cleanup remove
  both forms, and CDC suppresses duplicate shadow events.
- Composite-index directory markers use a separate structured Catalog key, so a
  real index named `__marker` remains visible and enforced. Derived HNSW indexes
  use a structured `(table, column)` identity, eliminating underscore tuple
  collisions; they rebuild under the new identity on restart.

## Delivery Order

### P0: Versioned Structured Keyspace

Replace delimiter-concatenated physical keys before expanding SQL identifier or
sharding compatibility.

Design:

1. Introduce a leading codec version and typed tuple fields for namespace,
   tenant, object ID, index ID, typed value, and MVCC timestamp.
2. Preserve byte ordering for range scans and make every field unambiguous.
3. Persist one monotonic migration phase in the catalog and fence every writer
   version before advancing. The required phases are `legacy`, `delete-only`,
   `write-delete-shadow`, `backfill`, `validated`, `v2-readable`, `v2-only`, and
   `legacy-gc`.
4. In `write-delete-shadow`, read only legacy keys and write/delete both forms.
   Never read v2 first with legacy fallback: an old writer can delete only the
   legacy key and a stale v2 value would resurrect the row.
5. Backfill from a stable commit-order fence with idempotent chunks, durable
   checkpoints, and a delta/catch-up stream. Publish `v2-readable` only after all
   writers enforce the phase and an exact verifier proves value/tombstone
   equivalence at the same logical boundary.
6. Retire legacy keys only after the rollback window, validated backup, and an
   explicit irreversible downgrade fence.
7. Property-test round trips, prefix boundaries, arbitrary bytes, all numeric
   extrema, `NULL`, NaN policy, and cross-version ordering.

Current implementation boundary:

- `structured_data_shadow_v2 = false` still performs symmetric v2 deletes but
  does not create v2 values, matching a safe delete-only capability.
- `structured_data_shadow_v2 = true` enables transactional shadow writes. It is
  deliberately not a persistent migration phase and does not enable v2 reads.
- DROP/TRUNCATE scan the reserved Data V2 namespace and match decoded table
  bytes, so shadows from historical unsharded or removed-shard routes are also
  removed without touching prefix-related table names. This correctness path is
  currently O(all Data V2 keys); a route-independent table directory or revised
  prefix layout is required before large-scale cutover.
- Backfill, delta catch-up, catalog phase persistence, equivalence verification,
  read cutover, and legacy GC remain release blockers. Mixed-version Raft
  clusters still require the documented stop-the-world protocol upgrade.
- Legacy data-key readers now strip the exact routed table prefix, preserving
  text primary keys containing `:`. Legacy secondary/composite index reads for
  text-primary-key tables fall back to base scans because the old
  `(value):(row_id)` layout has real tuple collisions that no parser can repair.

Acceptance gates:

- Crash at every migration transition and resume without lost or duplicated
  logical rows.
- A verifier proves row/index/catalog equivalence before publication.
- Existing databases open without an offline manual rewrite.

### P0: Segmented Raft Log and Resumable Snapshots

The current durable Raft files close data-loss holes but still rewrite the full
log and transfer snapshots as one payload.

Design:

1. Store append-only log segments with a versioned header, per-record CRC, a
   sparse index, and explicit start/end `LogId` bounds.
2. Rotate and publish segments using file and directory sync; recover by
   truncating only an invalid tail and reject corruption in committed records.
3. Persist purge metadata before deleting obsolete segments.
4. Stream snapshots into an unpublished staging generation using numbered,
   length-bounded chunks with per-chunk and whole-snapshot hashes.
5. Resume from the first missing chunk and atomically install only when payload,
   membership, and applied `LogId` validate as one boundary.

Acceptance gates:

- Bounded memory and network frame size independent of database size.
- Crash/restart coverage at append, rotate, purge, receive, validate, rename,
  and directory-sync boundaries.
- Snapshot install racing with apply/build cannot move state backward.

### P0: Deterministic Failure Simulation

Example tests are insufficient for a storage engine with concurrency and crash
recovery. Make schedules replayable before adding more distributed behavior.

Design:

1. Abstract time, task scheduling, RNG, filesystem durability, and Raft network
   delivery behind deterministic test adapters.
2. Generate transactions, checkpoints, compactions, elections, partitions,
   retries, disk-full events, torn tails, restarts, and snapshot installation.
3. Record one seed plus workload parameters for exact replay.
4. Continuously check committed-prefix durability, snapshot isolation,
   uniqueness, index/table equivalence, Raft state-machine equality, and monotonic
   applied boundaries.
5. Run a short seeded corpus in every PR and a larger rotating corpus in nightly
   CI; retain every failing seed as a permanent regression.

Acceptance gates:

- Thousands of virtual cluster-hours per wall-clock hour on one machine.
- A failure report contains a minimal deterministic event trace.
- All replicas converge to byte-equivalent logical state after healing.

### P1: Serializable Isolation

Snapshot Isolation permits write skew. Add Serializable Snapshot Isolation
(SSI) without replacing the MVCC read path.

Design:

1. Track point and range SIREAD dependencies, including index and full scans.
2. Detect dangerous rw-antidependency structures at commit.
3. Bound memory through transaction summarization and garbage collection tied
   to the oldest relevant snapshot.
4. Add safe snapshots for read-only/deferrable transactions.
5. Expose stable serialization errors and bounded server/client retry policy.

Acceptance gates:

- Standard write-skew, phantom, and predicate-conflict histories are rejected.
- Snapshot Isolation remains selectable and its performance does not regress
  without measurement.
- History checking validates serializable executions under concurrency.

### P1: Online Schema and Index Jobs

The existing DDL barrier protects concurrent composite-index backfill, but it is
not a resumable online schema-change system.

Design:

1. Version table descriptors and pin a descriptor epoch per transaction.
2. Move indexes through explicit states such as delete-only, write-only,
   backfilling, validating, and public.
3. Make every backfill chunk idempotent and persist checkpoints/progress.
4. Validate table/index equality before read publication and support safe
   cancellation/rollback from every state.
5. Add admission control so backfill cannot starve foreground traffic.

Acceptance gates:

- Writes before, during, and after backfill appear exactly once in the index.
- Restart, cancellation, and node failover resume from durable progress.
- Old transactions continue with their pinned schema contract.

### P1: Backup, PITR, and Restore Validation

Replication is not backup. Add recovery outside the live failure domain.

Design:

1. Export a timestamp-consistent full snapshot with a checksummed manifest.
2. Add incremental WAL/MVCC history ranges and explicit retention watermarks.
3. Encrypt and authenticate manifests/chunks; support immutable object storage.
4. Restore into staging, verify every object and index, then publish atomically.
5. Schedule restore drills and report measured RPO/RTO, not only backup success.

Acceptance gates:

- Restore to a chosen committed timestamp and reject incomplete/corrupt chains.
- A restore never depends on files from the live data directory.
- CI performs recurring backup/restore equivalence tests.

### P1: Distributed CDC

Define CDC semantics before adding sinks.

Design:

1. Derive events from replicated logical mutations and attach a stable cluster
   position, transaction boundary, schema version, and event ID.
2. Persist per-sink checkpoints transactionally with delivery progress.
3. Specify ordering scope and at-least-once behavior first; add exactly-once only
   for sinks with a transactional/idempotent contract.
4. Tie retention to the slowest active sink with quotas and eviction policy.

## Release Gates

FusionDB should not claim production distributed durability until all of these
gates pass:

- Crash matrix: process kill, power-loss model, torn tail, disk-full, stale
  directory entry, and restart at every durability boundary.
- Consensus matrix: leader loss before/after quorum, duplicate proposal,
  unknown outcome, partition healing, snapshot/log overlap, and membership
  changes.
- Compatibility matrix: rolling upgrade across adjacent codec/storage versions
  with downgrade refusal after irreversible migration.
- Protocol matrix: every mutating protocol either enters the same consensus
  proposal path or returns a documented fail-closed error.
- Operability matrix: backup restore drill, disk-pressure behavior, compaction
  debt, snapshot progress, CDC lag, and corruption diagnostics are observable.
- Benchmark claims include hardware, dataset, durability mode, concurrency,
  p50/p95/p99, variance, and correctness gates.

## Primary References

- FoundationDB tuple/data-modeling guidance: typed, order-preserving composite
  keys instead of delimiter concatenation.
  <https://apple.github.io/foundationdb/data-modeling.html>
- FoundationDB tuple encoding specification: byte-stable type codes, escaped
  byte strings, and prefix-compatible tuple ordering.
  <https://github.com/apple/foundationdb/blob/main/design/tuple.md>
- TiDB table and scalar codecs: explicit key families plus order-preserving
  integer/byte encodings.
  <https://github.com/pingcap/tidb/blob/master/pkg/tablecodec/tablecodec.go>
  <https://github.com/pingcap/tidb/blob/master/pkg/util/codec/number.go>
  <https://github.com/pingcap/tidb/blob/master/pkg/util/codec/bytes.go>
- CockroachDB incremental index backfill RFC: mutation-safe progress tracking,
  resumable backfill work, and validation before publication.
  <https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20211004_incremental_index_backfiller.md>
- Google F1: schema leases and staged online schema changes keep old and new
  binaries mutually compatible while jobs advance.
  <https://research.google.com/pubs/archive/41376.pdf>
- FoundationDB SIGMOD 2021 paper: strict transactions, unbundled architecture,
  and deterministic simulation as a release discipline.
  <https://www.foundationdb.org/files/fdb-paper.pdf>
- TigerBeetle architecture: deterministic state, seeded simulation, bounded
  work, and fault injection against the actual implementation.
  <https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/ARCHITECTURE.md>
- OpenRaft 0.9 storage API: separate durable log/state-machine contracts and
  streamed snapshot installation boundaries.
  <https://docs.rs/openraft/0.9.21/openraft/storage/index.html>
- CockroachDB SIGMOD 2020 paper: replicated ranges, distributed transactions,
  and multi-version online schema change states.
  <https://www.cockroachlabs.com/pdf/cockroachdb-the-resilient-geo-distributed-sql-database-sigmod-2020.pdf>
- CockroachDB online schema-change documentation: resumable background
  backfills with staged schema versions and validation.
  <https://www.cockroachlabs.com/docs/stable/online-schema-changes.html>
- Ports and Grittner, VLDB 2012: production Serializable Snapshot Isolation,
  bounded memory, and safe read-only snapshots.
  <https://www.vldb.org/pvldb/vol5/p1850_danrkports_vldb2012.pdf>
- CockroachDB backup documentation: full/incremental, timestamp-consistent,
  checksummed backup and point-in-time restore practices.
  <https://www.cockroachlabs.com/docs/stable/backup>
