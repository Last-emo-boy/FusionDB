use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{self, Cursor};
use std::path::Path;
use std::sync::{Arc, RwLock};

use openraft::log_id::RaftLogId;
use openraft::{
    AnyError, Entry, EntryPayload, LogId, RaftLogReader, RaftSnapshotBuilder, RaftStorage,
    Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};

use super::persistence::{
    snapshot_payload_sha256, DurableSnapshot, RaftPersistence, SnapshotInstallCheckpoint,
    SnapshotInstallIntent,
};
use super::typ::{
    KvMutation, MutationBatch, NodeId, Request, Response, SideIndexMutation, TypeConfig,
    MUTATION_BATCH_VERSION,
};
use crate::execution::Executor;
use crate::storage::data_migration::{
    migration_phase_key, DataMigrationPhase, DataMigrationPhaseRecord, MAX_SUPPORTED_PHASE,
};
use crate::storage::fusion::{FusionTransaction, SideIndexDelta};
use crate::storage::{FusionStorage, Storage};

const SNAPSHOT_PAYLOAD_VERSION: u16 = 1;
const SNAPSHOT_SCAN_START: &[u8] = b"";
const SNAPSHOT_SCAN_END: &[u8] = &[0xff];
const RAFT_APPLIED_WATERMARK_KEY: &[u8] = b"\0fusiondb/raft/applied-watermark";
const RAFT_SNAPSHOT_INSTALL_MARKER_KEY: &[u8] = b"\0fusiondb/raft/snapshot-install-marker";
const SNAPSHOT_INSTALL_MARKER_VERSION: u16 = 1;
const MAX_MUTATIONS_PER_BATCH: usize = 1_000_000;
const MAX_SIDE_INDEX_MUTATIONS_PER_BATCH: usize = 1_000_000;
// Raft RPCs use JSON and the authenticated transport caps request bodies at
// 16 MiB. A 2 MiB bincode payload leaves headroom for JSON expansion of byte
// arrays/u32 vector bits plus the surrounding OpenRaft entry envelope.
const MAX_MUTATION_BATCH_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct FusionSnapshotPayload {
    version: u16,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SnapshotInstallMarker {
    version: u16,
    meta: SnapshotMeta<NodeId, openraft::BasicNode>,
    payload_sha256: [u8; 32],
}

impl SnapshotInstallMarker {
    fn from_intent(intent: &SnapshotInstallIntent) -> Self {
        Self {
            version: SNAPSHOT_INSTALL_MARKER_VERSION,
            meta: intent.snapshot.meta.clone(),
            payload_sha256: intent.payload_sha256,
        }
    }
}

struct StateMachineMeta {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
}

/// Combined Raft storage: log + state machine + snapshots.
/// Used with `openraft::storage::Adaptor` to satisfy both
/// `RaftLogStorage` and `RaftStateMachine`.
pub struct FusionRaftStore {
    vote: Option<Vote<NodeId>>,
    log: Arc<RwLock<BTreeMap<u64, Entry<TypeConfig>>>>,
    last_purged: Option<LogId<NodeId>>,
    state_machine_meta: Arc<tokio::sync::Mutex<StateMachineMeta>>,
    snapshot: Arc<RwLock<Option<DurableSnapshot>>>,
    persistence: Option<Arc<RaftPersistence>>,
    // Application state
    executor: Arc<Executor>,
    #[allow(dead_code)]
    storage: Arc<dyn Storage>,
}

impl FusionRaftStore {
    pub fn new(executor: Arc<Executor>, storage: Arc<dyn Storage>) -> Self {
        Self {
            vote: None,
            log: Arc::new(RwLock::new(BTreeMap::new())),
            last_purged: None,
            state_machine_meta: Arc::new(tokio::sync::Mutex::new(StateMachineMeta {
                last_applied: None,
                last_membership: StoredMembership::default(),
            })),
            snapshot: Arc::new(RwLock::new(None)),
            persistence: None,
            executor,
            storage,
        }
    }

    /// Open a Raft store whose consensus metadata is rooted in the configured
    /// FusionDB data directory. Missing files are treated as the legacy empty
    /// state; malformed, truncated, or CRC-invalid files fail startup.
    pub async fn open_durable(
        executor: Arc<Executor>,
        storage: Arc<dyn Storage>,
        raft_state_dir: impl AsRef<Path>,
    ) -> io::Result<Self> {
        let persistence = Arc::new(RaftPersistence::open(raft_state_dir)?);
        let recovered = persistence.recover()?;
        let snapshot_install = recovered.snapshot_install;

        let mut store = Self {
            vote: recovered.vote,
            log: Arc::new(RwLock::new(recovered.log)),
            last_purged: recovered.last_purged,
            state_machine_meta: Arc::new(tokio::sync::Mutex::new(StateMachineMeta {
                last_applied: recovered.last_applied,
                last_membership: recovered.last_membership,
            })),
            snapshot: Arc::new(RwLock::new(recovered.snapshot)),
            persistence: Some(persistence),
            executor,
            storage,
        };
        store
            .reconcile_snapshot_install(snapshot_install)
            .await
            .map_err(snapshot_reconcile_startup_error)?;
        Ok(store)
    }

    fn persist_vote(&self, vote: Option<Vote<NodeId>>) -> Result<(), StorageError<NodeId>> {
        if let Some(persistence) = &self.persistence {
            persistence.persist_vote(vote).map_err(vote_write_error)?;
        }
        Ok(())
    }

    fn persist_log(
        &self,
        last_purged: Option<LogId<NodeId>>,
        log: &BTreeMap<u64, Entry<TypeConfig>>,
    ) -> Result<(), StorageError<NodeId>> {
        if let Some(persistence) = &self.persistence {
            persistence
                .persist_log(last_purged, log)
                .map_err(log_write_error)?;
        }
        Ok(())
    }

    fn persist_state_machine(
        &self,
        last_applied: Option<LogId<NodeId>>,
        last_membership: &StoredMembership<NodeId, openraft::BasicNode>,
    ) -> Result<(), StorageError<NodeId>> {
        if let Some(persistence) = &self.persistence {
            persistence
                .persist_state_machine(last_applied, last_membership)
                .map_err(state_machine_write_error)?;
        }
        Ok(())
    }

    fn persist_snapshot(&self, snapshot: &DurableSnapshot) -> Result<(), StorageError<NodeId>> {
        if let Some(persistence) = &self.persistence {
            persistence
                .persist_snapshot(snapshot)
                .map_err(snapshot_write_error)?;
        }
        Ok(())
    }

    fn persist_snapshot_install_intent(
        &self,
        intent: &SnapshotInstallIntent,
    ) -> Result<(), StorageError<NodeId>> {
        if let Some(persistence) = &self.persistence {
            persistence
                .persist_snapshot_install_intent(intent)
                .map_err(snapshot_write_error)?;
            persistence
                .snapshot_install_checkpoint(SnapshotInstallCheckpoint::AfterIntent)
                .map_err(snapshot_write_error)?;
        }
        Ok(())
    }

    fn snapshot_install_checkpoint(
        &self,
        checkpoint: SnapshotInstallCheckpoint,
    ) -> Result<(), StorageError<NodeId>> {
        if let Some(persistence) = &self.persistence {
            persistence
                .snapshot_install_checkpoint(checkpoint)
                .map_err(snapshot_write_error)?;
        }
        Ok(())
    }

    fn finalize_snapshot_install(
        &self,
        intent: &SnapshotInstallIntent,
    ) -> Result<(), StorageError<NodeId>> {
        let Some(persistence) = &self.persistence else {
            return Ok(());
        };

        persistence
            .persist_snapshot(&intent.snapshot)
            .map_err(snapshot_write_error)?;
        persistence
            .snapshot_install_checkpoint(SnapshotInstallCheckpoint::AfterSnapshot)
            .map_err(snapshot_write_error)?;
        persistence
            .persist_state_machine(
                intent.snapshot.meta.last_log_id,
                &intent.snapshot.meta.last_membership,
            )
            .map_err(state_machine_write_error)?;
        persistence
            .snapshot_install_checkpoint(SnapshotInstallCheckpoint::AfterStateMachine)
            .map_err(snapshot_write_error)?;
        persistence
            .clear_snapshot_install_intent()
            .map_err(snapshot_write_error)
    }

    fn finalize_snapshot_install_after_commit(&self, intent: &SnapshotInstallIntent) {
        let result = self
            .snapshot_install_checkpoint(SnapshotInstallCheckpoint::AfterData)
            .and_then(|()| self.finalize_snapshot_install(intent));
        let Err(error) = result else {
            return;
        };

        // The data and install marker are already atomically durable. Returning
        // an error here would tell OpenRaft that installation failed even though
        // readers observe the new state. Keep (or restore) the intent so startup
        // recovery can finish the metadata publication deterministically.
        let intent_status = if let Some(persistence) = &self.persistence {
            match persistence.persist_snapshot_install_intent(intent) {
                Ok(()) => "the durable install intent was retained".to_string(),
                Err(retain_error) => {
                    format!("the install intent could not be re-persisted: {retain_error}")
                }
            }
        } else {
            "the in-memory store has no durable install intent".to_string()
        };
        eprintln!(
            "[raft] snapshot '{}' reached its durable data commit point, but metadata finalization was deferred: {error}; {intent_status}. Recovery will retry finalization on restart.",
            intent.snapshot.meta.snapshot_id
        );
    }

    async fn apply_request(
        &self,
        req: &Request,
        log_id: &LogId<NodeId>,
    ) -> Result<Response, StorageError<NodeId>> {
        let Request::MutationBatch(batch) = req else {
            return Ok(Response::error(
                "legacy raw SQL Raft entries are rejected; resubmit through the leader mutation-batch endpoint",
            ));
        };

        if let Err(message) = validate_mutation_batch(batch) {
            return Ok(Response::error(message));
        }

        let mut txn = self
            .storage
            .begin_transaction()
            .await
            .map_err(state_machine_write_error)?;
        if let Some(fusion_txn) = txn.as_any().downcast_ref::<FusionTransaction>() {
            fusion_txn.disable_cdc_capture();
        }
        if let Some(encoded_watermark) = txn
            .get(RAFT_APPLIED_WATERMARK_KEY)
            .await
            .map_err(state_machine_read_error)?
        {
            let watermark = decode_applied_watermark(&encoded_watermark)?;
            if watermark >= *log_id {
                txn.rollback().await.map_err(state_machine_write_error)?;
                return Ok(batch.response.clone());
            }
        }

        // Monotonic guard for the Data V2 migration phase record. Violations
        // are deterministic across replicas (same log order, same prior
        // state), so they use the graceful rejection channel — state
        // unchanged, replay reaches the same verdict, the node keeps
        // running. Only a legitimate advance beyond this binary's support
        // halts the state machine: applying past the fence would diverge.
        for mutation in &batch.mutations {
            let (key, new_value) = match mutation {
                KvMutation::Put { key, value } => (key, Some(value)),
                KvMutation::Delete { key } => (key, None),
            };
            if key.as_slice() != migration_phase_key() {
                continue;
            }
            let Some(new_value) = new_value else {
                txn.rollback().await.map_err(state_machine_write_error)?;
                return Ok(Response::error(
                    "the Data V2 migration phase record must never be deleted",
                ));
            };
            let new_record = match DataMigrationPhaseRecord::decode(new_value) {
                Ok(record) => record,
                Err(error) => {
                    txn.rollback().await.map_err(state_machine_write_error)?;
                    return Ok(Response::error(format!(
                        "malformed Data V2 migration phase record in Raft mutation batch: {error}"
                    )));
                }
            };
            let existing = txn
                .get(migration_phase_key())
                .await
                .map_err(state_machine_read_error)?;
            let step_is_valid = match &existing {
                None => {
                    new_record.phase_seq == 1
                        && matches!(
                        new_record.phase,
                        crate::storage::data_migration::DataMigrationPhase::DeleteOnly
                            | crate::storage::data_migration::DataMigrationPhase::WriteDeleteShadow
                    )
                }
                Some(existing) => {
                    // A malformed record already in storage is local
                    // corruption, not a batch defect: halt loudly.
                    let existing = DataMigrationPhaseRecord::decode(existing)
                        .map_err(state_machine_write_error)?;
                    new_record.phase_seq == existing.phase_seq + 1
                        && Some(new_record.phase) == existing.phase.next()
                }
            };
            if !step_is_valid {
                txn.rollback().await.map_err(state_machine_write_error)?;
                return Ok(Response::error(format!(
                    "Data V2 migration phase mutation is not a valid monotonic step (proposed '{}' seq {})",
                    new_record.phase.name(),
                    new_record.phase_seq
                )));
            }
            if new_record.phase > MAX_SUPPORTED_PHASE {
                txn.rollback().await.map_err(state_machine_write_error)?;
                return Err(state_machine_write_error(format!(
                    "the cluster advanced to Data V2 migration phase '{}' (seq {}), but this binary only supports up to '{}'; halting instead of diverging — upgrade this node",
                    new_record.phase.name(),
                    new_record.phase_seq,
                    MAX_SUPPORTED_PHASE.name()
                )));
            }
        }

        // Once the store is at Backfill or beyond, every data-family write
        // must have been fenced, which means its batch carries a phase
        // precondition. A batch without one was produced by a binary that
        // does not know about the fence; applying it would punch silent holes
        // in the shadow set that no verifier could later explain. Reject
        // deterministically so the node keeps serving and the operator sees a
        // loud failure instead of quiet corruption.
        if batch.mutations.iter().any(|mutation| {
            let key = match mutation {
                KvMutation::Put { key, .. } => key,
                KvMutation::Delete { key } => key,
            };
            is_data_family_key(key)
        }) {
            let record = txn
                .get(migration_phase_key())
                .await
                .map_err(state_machine_read_error)?
                .as_deref()
                .map(DataMigrationPhaseRecord::decode)
                .transpose()
                .map_err(state_machine_write_error)?;
            let fenced = batch
                .preconditions
                .iter()
                .any(|precondition| precondition.key.as_slice() == migration_phase_key());
            if !fenced && record.is_some_and(|record| record.phase >= DataMigrationPhase::Backfill)
            {
                txn.rollback().await.map_err(state_machine_write_error)?;
                return Ok(Response::error(
                    "Raft batch writes data rows without a Data V2 migration phase precondition while the store is at 'backfill' or beyond; the proposing node must be upgraded",
                ));
            }
        }

        for precondition in &batch.preconditions {
            let actual = txn
                .get(&precondition.key)
                .await
                .map_err(state_machine_read_error)?;
            if actual != precondition.expected {
                txn.rollback().await.map_err(state_machine_write_error)?;
                return Err(state_machine_write_error(format!(
                    "Raft mutation precondition failed for key {:?}",
                    String::from_utf8_lossy(&precondition.key)
                )));
            }
        }

        for mutation in &batch.mutations {
            match mutation {
                KvMutation::Put { key, value } => txn
                    .put(key, value)
                    .await
                    .map_err(state_machine_write_error)?,
                KvMutation::Delete { key } => {
                    txn.delete(key).await.map_err(state_machine_write_error)?
                }
            }
        }

        if !batch.side_index_mutations.is_empty() {
            let Some(fusion_txn) = txn.as_any().downcast_ref::<FusionTransaction>() else {
                txn.rollback().await.map_err(state_machine_write_error)?;
                return Err(state_machine_write_error(
                    "side-index Raft mutations require FusionStorage",
                ));
            };
            for mutation in &batch.side_index_mutations {
                fusion_txn.defer_side_index_delta(side_index_delta_from_mutation(mutation));
            }
        }

        let applied_watermark = bincode::serialize(log_id).map_err(state_machine_write_error)?;
        txn.put(RAFT_APPLIED_WATERMARK_KEY, &applied_watermark)
            .await
            .map_err(state_machine_write_error)?;
        txn.commit().await.map_err(state_machine_write_error)?;
        self.executor.invalidate_storage_caches();
        Ok(batch.response.clone())
    }

    async fn build_snapshot_payload(&self) -> Result<FusionSnapshotPayload, StorageError<NodeId>> {
        let mut entries = export_visible_storage(&self.storage).await?;
        // The install marker describes the target node's publication history,
        // not replicated application state. Excluding it keeps snapshot bytes
        // deterministic across nodes at the same applied boundary.
        entries.retain(|(key, _)| key.as_slice() != RAFT_SNAPSHOT_INSTALL_MARKER_KEY);
        Ok(FusionSnapshotPayload {
            version: SNAPSHOT_PAYLOAD_VERSION,
            entries,
        })
    }

    async fn reconcile_snapshot_install(
        &mut self,
        intent: Option<SnapshotInstallIntent>,
    ) -> Result<(), StorageError<NodeId>> {
        let marker = read_snapshot_install_marker(&self.storage).await?;
        let applied_watermark = read_applied_watermark(&self.storage).await?;

        let Some(intent) = intent else {
            return self.validate_recovered_snapshot_state(marker, applied_watermark);
        };
        intent
            .validate()
            .map_err(|error| snapshot_read_error(error.to_string()))?;

        let decoded_payload = decode_snapshot_payload(&intent.snapshot.data)?;
        let payload =
            normalize_snapshot_payload(&decoded_payload, intent.snapshot.meta.last_log_id)?;
        if encode_snapshot_payload(&payload)? != intent.snapshot.data {
            return Err(snapshot_read_error(
                "Raft snapshot install intent payload is not canonical for its metadata boundary",
            ));
        }
        let expected_marker = SnapshotInstallMarker::from_intent(&intent);
        self.validate_pending_snapshot_boundaries(&intent, marker.as_ref(), applied_watermark)?;

        let data_is_published = marker.as_ref() == Some(&expected_marker);
        if !data_is_published {
            let entries = snapshot_entries_with_install_markers(&payload, &intent)?;
            replace_visible_storage(&self.storage, &entries).await?;
        }

        self.publish_snapshot_install_in_memory(&intent)?;
        self.executor.invalidate_storage_caches();
        self.finalize_snapshot_install(&intent)
    }

    fn validate_pending_snapshot_boundaries(
        &self,
        intent: &SnapshotInstallIntent,
        marker: Option<&SnapshotInstallMarker>,
        applied_watermark: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let target = intent.snapshot.meta.last_log_id;
        let state_machine_meta = self
            .state_machine_meta
            .try_lock()
            .map_err(|_| snapshot_read_error("Raft state-machine metadata lock is busy"))?;
        validate_boundary_not_newer("state-machine", state_machine_meta.last_applied, target)?;
        if state_machine_meta.last_applied == target
            && state_machine_meta.last_membership != intent.snapshot.meta.last_membership
        {
            return Err(snapshot_read_error(
                "Raft snapshot install intent conflicts with state-machine membership at the same applied boundary",
            ));
        }
        drop(state_machine_meta);

        validate_boundary_not_newer("applied watermark", applied_watermark, target)?;

        if let Some(marker) = marker {
            match marker.meta.last_log_id.cmp(&target) {
                Ordering::Greater => {
                    return Err(snapshot_read_error(
                        "Raft snapshot install marker is newer than the durable install intent",
                    ));
                }
                Ordering::Equal if marker != &SnapshotInstallMarker::from_intent(intent) => {
                    return Err(snapshot_read_error(
                        "Raft snapshot install marker hash or metadata conflicts with the durable intent",
                    ));
                }
                _ => {}
            }
        }

        if let Some(snapshot) = self.snapshot.read().map_err(snapshot_read_error)?.as_ref() {
            match snapshot.meta.last_log_id.cmp(&target) {
                Ordering::Greater => {
                    return Err(snapshot_read_error(
                        "durable Raft snapshot is newer than the pending install intent",
                    ));
                }
                Ordering::Equal
                    if snapshot.meta != intent.snapshot.meta
                        || snapshot_payload_sha256(&snapshot.data) != intent.payload_sha256 =>
                {
                    return Err(snapshot_read_error(
                        "durable Raft snapshot hash or metadata conflicts with the install intent at the same boundary",
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn validate_recovered_snapshot_state(
        &self,
        marker: Option<SnapshotInstallMarker>,
        applied_watermark: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let state_machine_meta = self
            .state_machine_meta
            .try_lock()
            .map_err(|_| snapshot_read_error("Raft state-machine metadata lock is busy"))?;
        let snapshot = self.snapshot.read().map_err(snapshot_read_error)?;

        if let Some(snapshot) = snapshot.as_ref() {
            validate_boundary_not_newer(
                "durable snapshot",
                snapshot.meta.last_log_id,
                state_machine_meta.last_applied,
            )?;
            if snapshot.meta.last_log_id == state_machine_meta.last_applied
                && snapshot.meta.last_membership != state_machine_meta.last_membership
            {
                return Err(snapshot_read_error(
                    "durable Raft snapshot membership conflicts with state-machine metadata",
                ));
            }
        }

        if let Some(marker) = marker {
            validate_boundary_not_newer(
                "snapshot install marker",
                marker.meta.last_log_id,
                state_machine_meta.last_applied,
            )?;
            validate_boundary_not_newer(
                "snapshot install marker",
                marker.meta.last_log_id,
                applied_watermark,
            )?;
            let Some(snapshot) = snapshot.as_ref() else {
                return Err(snapshot_read_error(
                    "snapshot install marker exists without durable snapshot metadata",
                ));
            };
            match snapshot.meta.last_log_id.cmp(&marker.meta.last_log_id) {
                Ordering::Less => {
                    return Err(snapshot_read_error(
                        "durable Raft snapshot is older than the installed data marker",
                    ));
                }
                Ordering::Equal
                    if snapshot.meta != marker.meta
                        || snapshot_payload_sha256(&snapshot.data) != marker.payload_sha256 =>
                {
                    return Err(snapshot_read_error(
                        "durable Raft snapshot hash or metadata conflicts with the installed data marker",
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn publish_snapshot_install_in_memory(
        &self,
        intent: &SnapshotInstallIntent,
    ) -> Result<(), StorageError<NodeId>> {
        let mut state_machine_meta = self
            .state_machine_meta
            .try_lock()
            .map_err(|_| snapshot_write_error("Raft state-machine metadata lock is busy"))?;
        state_machine_meta.last_applied = intent.snapshot.meta.last_log_id;
        state_machine_meta.last_membership = intent.snapshot.meta.last_membership.clone();
        *self.snapshot.write().map_err(snapshot_write_error)? = Some(intent.snapshot.clone());
        Ok(())
    }
}

fn snapshot_write_error(error: impl ToString) -> StorageError<NodeId> {
    StorageIOError::write_snapshot(None, AnyError::error(error.to_string())).into()
}

fn vote_write_error(error: impl ToString) -> StorageError<NodeId> {
    StorageIOError::write_vote(AnyError::error(error.to_string())).into()
}

fn log_write_error(error: impl ToString) -> StorageError<NodeId> {
    StorageIOError::write_logs(AnyError::error(error.to_string())).into()
}

fn snapshot_read_error(error: impl ToString) -> StorageError<NodeId> {
    StorageIOError::read_snapshot(None, AnyError::error(error.to_string())).into()
}

fn state_machine_read_error(error: impl ToString) -> StorageError<NodeId> {
    StorageIOError::read_state_machine(AnyError::error(error.to_string())).into()
}

fn state_machine_write_error(error: impl ToString) -> StorageError<NodeId> {
    StorageIOError::write_state_machine(AnyError::error(error.to_string())).into()
}

fn snapshot_reconcile_startup_error(error: StorageError<NodeId>) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("Raft snapshot recovery failed: {error}"),
    )
}

fn validate_boundary_not_newer(
    name: &str,
    boundary: Option<LogId<NodeId>>,
    target: Option<LogId<NodeId>>,
) -> Result<(), StorageError<NodeId>> {
    if boundary > target {
        return Err(snapshot_read_error(format!(
            "Raft {name} boundary {boundary:?} is newer than expected boundary {target:?}",
        )));
    }
    Ok(())
}

fn decode_applied_watermark(bytes: &[u8]) -> Result<LogId<NodeId>, StorageError<NodeId>> {
    let mut cursor = Cursor::new(bytes);
    let watermark: LogId<NodeId> =
        bincode::deserialize_from(&mut cursor).map_err(state_machine_read_error)?;
    if cursor.position() != bytes.len() as u64 {
        return Err(state_machine_read_error(
            "Raft applied watermark has trailing bytes",
        ));
    }
    Ok(watermark)
}

fn encode_snapshot_install_marker(
    marker: &SnapshotInstallMarker,
) -> Result<Vec<u8>, StorageError<NodeId>> {
    bincode::serialize(marker).map_err(snapshot_write_error)
}

fn decode_snapshot_install_marker(
    bytes: &[u8],
) -> Result<SnapshotInstallMarker, StorageError<NodeId>> {
    let mut cursor = Cursor::new(bytes);
    let marker: SnapshotInstallMarker =
        bincode::deserialize_from(&mut cursor).map_err(snapshot_read_error)?;
    if cursor.position() != bytes.len() as u64 {
        return Err(snapshot_read_error(
            "Raft snapshot install marker has trailing bytes",
        ));
    }
    if marker.version != SNAPSHOT_INSTALL_MARKER_VERSION {
        return Err(snapshot_read_error(format!(
            "unsupported Raft snapshot install marker version {}",
            marker.version
        )));
    }
    Ok(marker)
}

async fn read_internal_value(
    storage: &Arc<dyn Storage>,
    key: &[u8],
) -> Result<Option<Vec<u8>>, StorageError<NodeId>> {
    let txn = storage
        .begin_transaction()
        .await
        .map_err(state_machine_read_error)?;
    let value = txn.get(key).await.map_err(state_machine_read_error)?;
    txn.rollback().await.map_err(state_machine_read_error)?;
    Ok(value)
}

async fn read_snapshot_install_marker(
    storage: &Arc<dyn Storage>,
) -> Result<Option<SnapshotInstallMarker>, StorageError<NodeId>> {
    read_internal_value(storage, RAFT_SNAPSHOT_INSTALL_MARKER_KEY)
        .await?
        .map(|bytes| decode_snapshot_install_marker(&bytes))
        .transpose()
}

async fn read_applied_watermark(
    storage: &Arc<dyn Storage>,
) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
    read_internal_value(storage, RAFT_APPLIED_WATERMARK_KEY)
        .await?
        .map(|bytes| decode_applied_watermark(&bytes))
        .transpose()
}

pub(crate) fn validate_mutation_batch(batch: &MutationBatch) -> Result<(), String> {
    if batch.version != MUTATION_BATCH_VERSION {
        return Err(format!(
            "unsupported Raft mutation batch version {}",
            batch.version
        ));
    }
    if batch.mutations.len() > MAX_MUTATIONS_PER_BATCH {
        return Err(format!(
            "Raft mutation batch has {} KV mutations; maximum is {}",
            batch.mutations.len(),
            MAX_MUTATIONS_PER_BATCH
        ));
    }
    if batch.preconditions.len() > MAX_MUTATIONS_PER_BATCH {
        return Err(format!(
            "Raft mutation batch has {} preconditions; maximum is {}",
            batch.preconditions.len(),
            MAX_MUTATIONS_PER_BATCH
        ));
    }
    if batch.side_index_mutations.len() > MAX_SIDE_INDEX_MUTATIONS_PER_BATCH {
        return Err(format!(
            "Raft mutation batch has {} side-index mutations; maximum is {}",
            batch.side_index_mutations.len(),
            MAX_SIDE_INDEX_MUTATIONS_PER_BATCH
        ));
    }

    let mut bytes = batch.response.message.len();
    for precondition in &batch.preconditions {
        bytes = bytes.saturating_add(precondition.key.len()).saturating_add(
            precondition
                .expected
                .as_ref()
                .map_or(0, |value| value.len()),
        );
    }
    for mutation in &batch.mutations {
        let mutation_bytes = match mutation {
            KvMutation::Put { key, value } => key.len().saturating_add(value.len()),
            KvMutation::Delete { key } => key.len(),
        };
        bytes = bytes.saturating_add(mutation_bytes);
    }
    for mutation in &batch.side_index_mutations {
        let mutation_bytes = match mutation {
            SideIndexMutation::TrigramAdd {
                table,
                column,
                row_id,
                text,
                ..
            } => table
                .len()
                .saturating_add(column.len())
                .saturating_add(row_id.len())
                .saturating_add(text.len()),
            SideIndexMutation::TrigramRemove {
                table,
                column,
                text,
                ..
            } => table
                .len()
                .saturating_add(column.len())
                .saturating_add(text.len()),
            SideIndexMutation::VectorInsert {
                index,
                id,
                vector_bits,
            } => index
                .len()
                .saturating_add(id.len())
                .saturating_add(vector_bits.len().saturating_mul(std::mem::size_of::<u32>())),
            SideIndexMutation::VectorDelete { index, id } => index.len().saturating_add(id.len()),
        };
        bytes = bytes.saturating_add(mutation_bytes);
    }
    if bytes > MAX_MUTATION_BATCH_BYTES {
        return Err(format!(
            "Raft mutation batch payload is {} bytes; maximum is {}",
            bytes, MAX_MUTATION_BATCH_BYTES
        ));
    }
    let encoded_bytes = bincode::serialized_size(batch)
        .map_err(|error| format!("Raft mutation batch size encoding failed: {error}"))?;
    if encoded_bytes > MAX_MUTATION_BATCH_BYTES as u64 {
        return Err(format!(
            "Raft mutation batch encoded payload is {encoded_bytes} bytes; maximum is {MAX_MUTATION_BATCH_BYTES}"
        ));
    }
    Ok(())
}

fn side_index_delta_from_mutation(mutation: &SideIndexMutation) -> SideIndexDelta {
    match mutation {
        SideIndexMutation::TrigramAdd {
            table,
            column,
            numeric_id,
            row_id,
            text,
        } => SideIndexDelta::TrigramAdd {
            table: table.clone(),
            column: column.clone(),
            numeric_id: *numeric_id,
            row_id: row_id.clone(),
            text: text.clone(),
        },
        SideIndexMutation::TrigramRemove {
            table,
            column,
            numeric_id,
            text,
        } => SideIndexDelta::TrigramRemove {
            table: table.clone(),
            column: column.clone(),
            numeric_id: *numeric_id,
            text: text.clone(),
        },
        SideIndexMutation::VectorInsert {
            index,
            id,
            vector_bits,
        } => SideIndexDelta::VectorInsert {
            index: index.clone(),
            id: id.clone(),
            vector: vector_bits.iter().copied().map(f32::from_bits).collect(),
        },
        SideIndexMutation::VectorDelete { index, id } => SideIndexDelta::VectorDelete {
            index: index.clone(),
            id: id.clone(),
        },
    }
}

fn encode_snapshot_payload(
    payload: &FusionSnapshotPayload,
) -> Result<Vec<u8>, StorageError<NodeId>> {
    bincode::serialize(payload).map_err(snapshot_write_error)
}

fn decode_snapshot_payload(bytes: &[u8]) -> Result<FusionSnapshotPayload, StorageError<NodeId>> {
    let mut cursor = Cursor::new(bytes);
    let payload: FusionSnapshotPayload =
        bincode::deserialize_from(&mut cursor).map_err(snapshot_read_error)?;
    if cursor.position() != bytes.len() as u64 {
        return Err(snapshot_read_error(
            "FusionDB snapshot payload has trailing bytes",
        ));
    }
    if payload.version != SNAPSHOT_PAYLOAD_VERSION {
        return Err(snapshot_read_error(format!(
            "unsupported FusionDB snapshot payload version {}",
            payload.version
        )));
    }
    Ok(payload)
}

fn snapshot_entries_with_install_markers(
    payload: &FusionSnapshotPayload,
    intent: &SnapshotInstallIntent,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError<NodeId>> {
    let mut entries = canonical_snapshot_entries(payload, intent.snapshot.meta.last_log_id)?;
    let marker = encode_snapshot_install_marker(&SnapshotInstallMarker::from_intent(intent))?;
    entries.insert(RAFT_SNAPSHOT_INSTALL_MARKER_KEY.to_vec(), marker);
    Ok(entries.into_iter().collect())
}

fn canonical_snapshot_entries(
    payload: &FusionSnapshotPayload,
    last_log_id: Option<LogId<NodeId>>,
) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, StorageError<NodeId>> {
    let mut entries = BTreeMap::new();
    for (key, value) in &payload.entries {
        if key.as_slice() == RAFT_APPLIED_WATERMARK_KEY
            || key.as_slice() == RAFT_SNAPSHOT_INSTALL_MARKER_KEY
        {
            continue;
        }
        if entries.insert(key.clone(), value.clone()).is_some() {
            return Err(snapshot_read_error(format!(
                "FusionDB snapshot payload contains duplicate key {:?}",
                String::from_utf8_lossy(key)
            )));
        }
    }

    if let Some(last_log_id) = last_log_id {
        let watermark = bincode::serialize(&last_log_id).map_err(snapshot_write_error)?;
        entries.insert(RAFT_APPLIED_WATERMARK_KEY.to_vec(), watermark);
    }
    Ok(entries)
}

/// True for every key that holds a base row, in all three physical shapes:
/// legacy unsharded, legacy sharded, and the Data V2 namespace. Index, unique,
/// fts and catalog keys are deliberately excluded — only base rows participate
/// in the shadow-write contract the fence protects.
fn is_data_family_key(key: &[u8]) -> bool {
    if crate::storage::keyspace::parse_data_key_exact(key).is_ok() {
        return true;
    }
    let Ok(text) = std::str::from_utf8(key) else {
        return false;
    };
    if text.starts_with("data:") {
        return true;
    }
    let Some(rest) = text.strip_prefix("shard:") else {
        return false;
    };
    let Some((shard, tail)) = rest.split_once(':') else {
        return false;
    };
    !shard.is_empty()
        && shard.bytes().all(|byte| byte.is_ascii_digit())
        && tail.starts_with("data:")
}

fn normalize_snapshot_payload(
    payload: &FusionSnapshotPayload,
    last_log_id: Option<LogId<NodeId>>,
) -> Result<FusionSnapshotPayload, StorageError<NodeId>> {
    let entries: Vec<(Vec<u8>, Vec<u8>)> = canonical_snapshot_entries(payload, last_log_id)?
        .into_iter()
        .collect();
    // Refuse to install state from a migration phase this binary does not
    // implement. Installing blind would leave this node reading and writing
    // under stale phase semantics — divergence, not availability.
    for (key, value) in &entries {
        if key.as_slice() == migration_phase_key() {
            let record = DataMigrationPhaseRecord::decode(value).map_err(|error| {
                snapshot_read_error(format!(
                    "Raft snapshot payload carries a malformed Data V2 migration phase record: {error}"
                ))
            })?;
            if record.phase > MAX_SUPPORTED_PHASE {
                return Err(snapshot_read_error(format!(
                    "Raft snapshot payload is at Data V2 migration phase '{}' (seq {}), but this binary only supports up to '{}'; upgrade before installing",
                    record.phase.name(),
                    record.phase_seq,
                    MAX_SUPPORTED_PHASE.name()
                )));
            }
        }
    }
    Ok(FusionSnapshotPayload {
        version: SNAPSHOT_PAYLOAD_VERSION,
        entries,
    })
}

async fn export_visible_storage(
    storage: &Arc<dyn Storage>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError<NodeId>> {
    let txn = storage
        .begin_transaction()
        .await
        .map_err(state_machine_read_error)?;
    txn.scan_range(SNAPSHOT_SCAN_START, SNAPSHOT_SCAN_END, None)
        .await
        .map_err(state_machine_read_error)
}

async fn replace_visible_storage(
    storage: &Arc<dyn Storage>,
    entries: &[(Vec<u8>, Vec<u8>)],
) -> Result<(), StorageError<NodeId>> {
    if let Some(fusion) = storage.as_any().downcast_ref::<FusionStorage>() {
        return fusion
            .replace_visible_entries_for_snapshot(SNAPSHOT_SCAN_START, SNAPSHOT_SCAN_END, entries)
            .await
            .map_err(state_machine_write_error);
    }

    let mut txn = storage
        .begin_transaction()
        .await
        .map_err(state_machine_write_error)?;
    let existing = txn
        .scan_range(SNAPSHOT_SCAN_START, SNAPSHOT_SCAN_END, None)
        .await
        .map_err(state_machine_read_error)?;

    for (key, _) in existing {
        txn.delete(&key).await.map_err(state_machine_write_error)?;
    }
    for (key, value) in entries {
        txn.put(key, value)
            .await
            .map_err(state_machine_write_error)?;
    }
    txn.commit().await.map_err(state_machine_write_error)
}

// --- RaftLogReader ---

impl RaftLogReader<TypeConfig> for FusionRaftStore {
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + Debug + openraft::OptionalSend,
    >(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let log = self.log.read().map_err(log_write_error)?;
        let entries: Vec<_> = log.range(range).map(|(_, value)| value.clone()).collect();
        Ok(entries)
    }
}

// --- RaftSnapshotBuilder ---

impl RaftSnapshotBuilder<TypeConfig> for FusionRaftStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        // Serialize state-machine apply with the visible-storage snapshot so
        // payload bytes and SnapshotMeta describe the same applied boundary.
        let state_machine_meta = self.state_machine_meta.lock().await;
        let snapshot_id = state_machine_meta
            .last_applied
            .map(|id| format!("{}-{}", id.leader_id, id.index))
            .unwrap_or_else(|| "empty".to_string());
        let payload = self.build_snapshot_payload().await?;
        let snapshot_bytes = encode_snapshot_payload(&payload)?;

        let meta = SnapshotMeta {
            last_log_id: state_machine_meta.last_applied,
            last_membership: state_machine_meta.last_membership.clone(),
            snapshot_id,
        };

        let durable_snapshot = DurableSnapshot {
            meta: meta.clone(),
            data: snapshot_bytes.clone(),
        };
        self.persist_snapshot(&durable_snapshot)?;
        *self.snapshot.write().map_err(snapshot_write_error)? = Some(durable_snapshot);

        let snap = Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(snapshot_bytes)),
        };
        Ok(snap)
    }
}

// --- RaftStorage (combined) ---

impl RaftStorage<TypeConfig> for FusionRaftStore {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.persist_vote(Some(*vote))?;
        self.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.vote)
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<openraft::LogState<TypeConfig>, StorageError<NodeId>> {
        let last_log_id = self
            .log
            .read()
            .map_err(log_write_error)?
            .iter()
            .next_back()
            .map(|(_, entry)| *entry.get_log_id())
            .or(self.last_purged);
        Ok(openraft::LogState {
            last_purged_log_id: self.last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        // Clone the store for the reader (log reader is used concurrently)
        FusionRaftStore {
            vote: self.vote,
            log: self.log.clone(),
            last_purged: self.last_purged,
            state_machine_meta: self.state_machine_meta.clone(),
            snapshot: self.snapshot.clone(),
            persistence: self.persistence.clone(),
            executor: self.executor.clone(),
            storage: self.storage.clone(),
        }
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut next_log = self.log.read().map_err(log_write_error)?.clone();
        for entry in entries {
            let log_id = *entry.get_log_id();
            next_log.insert(log_id.index, entry);
        }
        self.persist_log(self.last_purged, &next_log)?;
        *self.log.write().map_err(log_write_error)? = next_log;
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut next_log = self.log.read().map_err(log_write_error)?.clone();
        let keys: Vec<_> = next_log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in keys {
            next_log.remove(&k);
        }
        self.persist_log(self.last_purged, &next_log)?;
        *self.log.write().map_err(log_write_error)? = next_log;
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut next_log = self.log.read().map_err(log_write_error)?.clone();
        let keys: Vec<_> = next_log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys {
            next_log.remove(&k);
        }
        self.persist_log(Some(log_id), &next_log)?;
        *self.log.write().map_err(log_write_error)? = next_log;
        self.last_purged = Some(log_id);
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, openraft::BasicNode>,
        ),
        StorageError<NodeId>,
    > {
        let state_machine_meta = self.state_machine_meta.lock().await;
        Ok((
            state_machine_meta.last_applied,
            state_machine_meta.last_membership.clone(),
        ))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<Response>, StorageError<NodeId>> {
        let mut state_machine_meta = self.state_machine_meta.lock().await;
        let mut results = Vec::new();
        for entry in entries {
            let next_last_applied = Some(*entry.get_log_id());
            let mut next_membership = state_machine_meta.last_membership.clone();

            let response = match entry.payload {
                EntryPayload::Blank => Response::success(String::new()),
                EntryPayload::Normal(ref req) => {
                    self.apply_request(req, entry.get_log_id()).await?
                }
                EntryPayload::Membership(ref mem) => {
                    next_membership = StoredMembership::new(Some(*entry.get_log_id()), mem.clone());
                    Response::success("membership changed")
                }
            };

            self.persist_state_machine(next_last_applied, &next_membership)?;
            state_machine_meta.last_applied = next_last_applied;
            state_machine_meta.last_membership = next_membership;
            results.push(response);
        }
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        FusionRaftStore {
            vote: self.vote,
            log: self.log.clone(),
            last_purged: self.last_purged,
            state_machine_meta: self.state_machine_meta.clone(),
            snapshot: self.snapshot.clone(),
            persistence: self.persistence.clone(),
            executor: self.executor.clone(),
            storage: self.storage.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut state_machine_meta = self.state_machine_meta.lock().await;
        let received_snapshot_bytes = (*snapshot).into_inner();
        let received_payload = decode_snapshot_payload(&received_snapshot_bytes)?;
        let payload = normalize_snapshot_payload(&received_payload, meta.last_log_id)?;
        let snapshot_bytes = encode_snapshot_payload(&payload)?;
        let durable_snapshot = DurableSnapshot {
            meta: meta.clone(),
            data: snapshot_bytes,
        };
        let intent = SnapshotInstallIntent::new(durable_snapshot);
        let entries = snapshot_entries_with_install_markers(&payload, &intent)?;
        let marker = read_snapshot_install_marker(&self.storage).await?;
        let applied_watermark = read_applied_watermark(&self.storage).await?;

        validate_boundary_not_newer(
            "state-machine",
            state_machine_meta.last_applied,
            meta.last_log_id,
        )?;
        if state_machine_meta.last_applied == meta.last_log_id
            && state_machine_meta.last_membership != meta.last_membership
        {
            return Err(snapshot_read_error(
                "incoming Raft snapshot membership conflicts with the current state-machine boundary",
            ));
        }
        validate_boundary_not_newer("applied watermark", applied_watermark, meta.last_log_id)?;

        let expected_marker = SnapshotInstallMarker::from_intent(&intent);
        if let Some(marker) = marker.as_ref() {
            match marker.meta.last_log_id.cmp(&meta.last_log_id) {
                Ordering::Greater => {
                    return Err(snapshot_read_error(
                        "incoming Raft snapshot is older than the installed data marker",
                    ));
                }
                Ordering::Equal if marker != &expected_marker => {
                    return Err(snapshot_read_error(
                        "incoming Raft snapshot hash or metadata conflicts with the installed data marker",
                    ));
                }
                _ => {}
            }
        }

        if let Some(current) = self.snapshot.read().map_err(snapshot_read_error)?.as_ref() {
            match current.meta.last_log_id.cmp(&meta.last_log_id) {
                Ordering::Greater => {
                    return Err(snapshot_read_error(
                        "incoming Raft snapshot is older than the cached durable snapshot",
                    ));
                }
                Ordering::Equal
                    if current.meta != *meta
                        || snapshot_payload_sha256(&current.data) != intent.payload_sha256 =>
                {
                    return Err(snapshot_read_error(
                        "incoming Raft snapshot hash or metadata conflicts with the cached snapshot",
                    ));
                }
                _ => {}
            }
        }

        self.persist_snapshot_install_intent(&intent)?;
        if marker.as_ref() != Some(&expected_marker) {
            replace_visible_storage(&self.storage, &entries).await?;
        }

        state_machine_meta.last_applied = meta.last_log_id;
        state_machine_meta.last_membership = meta.last_membership.clone();
        match self.snapshot.write() {
            Ok(mut snapshot) => *snapshot = Some(intent.snapshot.clone()),
            Err(poisoned) => {
                eprintln!(
                    "[raft] snapshot cache lock was poisoned after snapshot '{}' reached its durable data commit point; recovering the cache boundary",
                    intent.snapshot.meta.snapshot_id
                );
                *poisoned.into_inner() = Some(intent.snapshot.clone());
            }
        }
        self.executor.invalidate_storage_caches();

        // From this point the data and marker are atomically visible. Even if
        // final metadata persistence or intent cleanup fails, the in-memory
        // boundary already matches the published snapshot and startup recovery
        // can deterministically finish the protocol.
        self.finalize_snapshot_install_after_commit(&intent);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let snapshot = self.snapshot.read().map_err(snapshot_read_error)?;
        if let Some(snap) = snapshot.as_ref() {
            Ok(Some(Snapshot {
                meta: snap.meta.clone(),
                snapshot: Box::new(Cursor::new(snap.data.clone())),
            }))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::TableSchema;
    use crate::config::StorageConfig;
    use crate::distributed::api::evaluate_sql_to_request;
    use crate::storage::memory::MemoryStorage;
    use crate::storage::FusionStorage;
    use std::path::{Path, PathBuf};

    fn test_store(name: &str) -> (FusionRaftStore, Arc<dyn Storage>, String) {
        let wal_path = format!("test_raft_snapshot_{}_{}.wal", name, uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
        let executor = Arc::new(Executor::new(storage.clone()));
        (
            FusionRaftStore::new(executor, storage.clone()),
            storage,
            wal_path,
        )
    }

    async fn durable_test_store(
        name: &str,
    ) -> (FusionRaftStore, Arc<dyn Storage>, String, PathBuf) {
        let wal_path = format!("test_raft_durable_{}_{}.wal", name, uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
        let executor = Arc::new(Executor::new(storage.clone()));
        let raft_dir = std::env::temp_dir().join(format!(
            "fusiondb_raft_durable_{}_{}",
            name,
            uuid::Uuid::new_v4()
        ));
        let store = FusionRaftStore::open_durable(executor, storage.clone(), &raft_dir)
            .await
            .unwrap();
        (store, storage, wal_path, raft_dir)
    }

    fn test_log_id(term: u64, index: u64) -> LogId<NodeId> {
        LogId::new(openraft::CommittedLeaderId::new(term, 1), index)
    }

    async fn test_fusion_store(name: &str) -> (FusionRaftStore, Arc<dyn Storage>, PathBuf) {
        let data_dir = std::env::temp_dir().join(format!(
            "fusiondb_raft_snapshot_{}_{}",
            name,
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        config.wal_file = "fusion.wal".to_string();
        config.sstable_dir = "sstables".to_string();
        let wal_path = config.wal_path();
        let storage: Arc<dyn Storage> = Arc::new(
            FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
                .await
                .unwrap(),
        );
        let executor = Arc::new(Executor::new(storage.clone()));
        (
            FusionRaftStore::new(executor, storage.clone()),
            storage,
            data_dir,
        )
    }

    fn cleanup_dir(path: &Path) {
        let _ = std::fs::remove_dir_all(path);
    }

    async fn put_entry(storage: &Arc<dyn Storage>, key: &[u8], value: &[u8]) {
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(key, value).await.unwrap();
        txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn snapshot_builder_persists_visible_storage_payload() {
        let (mut store, storage, wal_path) = test_store("builder_payload");
        put_entry(&storage, b"schema:users", b"schema-bytes").await;
        put_entry(&storage, b"data:users:1", b"row-one").await;

        let built = store.build_snapshot().await.unwrap();
        let current = store
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("snapshot should be cached");

        assert_eq!(current.meta, built.meta);
        let payload = decode_snapshot_payload(current.snapshot.get_ref()).unwrap();
        assert_eq!(payload.version, SNAPSHOT_PAYLOAD_VERSION);
        assert!(payload
            .entries
            .contains(&(b"schema:users".to_vec(), b"schema-bytes".to_vec())));
        assert!(payload
            .entries
            .contains(&(b"data:users:1".to_vec(), b"row-one".to_vec())));

        let _ = std::fs::remove_file(wal_path);
    }

    #[tokio::test]
    async fn install_snapshot_replaces_visible_storage_payload() {
        let (mut source_store, source_storage, source_wal) = test_store("source");
        let (mut target_store, target_storage, target_wal) = test_store("target");
        put_entry(&source_storage, b"schema:users", b"schema-bytes").await;
        put_entry(&source_storage, b"data:users:1", b"row-one").await;
        put_entry(&target_storage, b"schema:stale", b"stale-schema").await;

        let snapshot = source_store.build_snapshot().await.unwrap();
        let mut receiving = target_store.begin_receiving_snapshot().await.unwrap();
        *receiving = Cursor::new(snapshot.snapshot.get_ref().clone());
        target_store
            .install_snapshot(&snapshot.meta, receiving)
            .await
            .unwrap();

        let txn = target_storage.begin_transaction().await.unwrap();
        assert_eq!(
            txn.get(b"schema:users").await.unwrap(),
            Some(b"schema-bytes".to_vec())
        );
        assert_eq!(
            txn.get(b"data:users:1").await.unwrap(),
            Some(b"row-one".to_vec())
        );
        assert_eq!(txn.get(b"schema:stale").await.unwrap(), None);

        let current = target_store
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("installed snapshot should be cached");
        assert_eq!(current.meta, snapshot.meta);

        let _ = std::fs::remove_file(source_wal);
        let _ = std::fs::remove_file(target_wal);
    }

    #[tokio::test]
    async fn fusion_snapshot_install_restores_exact_visible_payload_without_cdc_side_effects() {
        let (mut source_store, source_storage, source_dir) =
            test_fusion_store("fusion_source").await;
        let (mut target_store, target_storage, target_dir) =
            test_fusion_store("fusion_target").await;
        let schema = bincode::serialize(&TableSchema::new("users".to_string(), Vec::new()))
            .expect("serialize test schema");
        put_entry(&source_storage, b"schema:users", &schema).await;
        put_entry(&source_storage, b"data:users:1", b"row-one").await;

        let snapshot = source_store.build_snapshot().await.unwrap();
        let payload = decode_snapshot_payload(snapshot.snapshot.get_ref()).unwrap();
        let mut receiving = target_store.begin_receiving_snapshot().await.unwrap();
        *receiving = Cursor::new(snapshot.snapshot.get_ref().clone());

        target_store
            .install_snapshot(&snapshot.meta, receiving)
            .await
            .unwrap();

        let mut installed_entries = export_visible_storage(&target_storage).await.unwrap();
        installed_entries.retain(|(key, _)| key.as_slice() != RAFT_SNAPSHOT_INSTALL_MARKER_KEY);
        let expected = normalize_snapshot_payload(&payload, snapshot.meta.last_log_id).unwrap();
        assert_eq!(installed_entries, expected.entries);

        cleanup_dir(&source_dir);
        cleanup_dir(&target_dir);
    }

    #[tokio::test]
    async fn fusion_snapshot_install_propagates_data_publication_failure_before_commit_point() {
        let (mut store, storage, data_dir) = test_fusion_store("invalid_publication").await;
        put_entry(&storage, b"data:stale:1", b"stale-row").await;
        let meta = SnapshotMeta {
            last_log_id: Some(test_log_id(19, 7)),
            last_membership: StoredMembership::default(),
            snapshot_id: "19-1-7".to_string(),
        };
        let payload = FusionSnapshotPayload {
            version: SNAPSHOT_PAYLOAD_VERSION,
            entries: vec![
                (b"schema:invalid".to_vec(), vec![0xff]),
                (b"data:invalid:1".to_vec(), b"invalid-row".to_vec()),
            ],
        };

        let error = store
            .install_snapshot(
                &meta,
                Box::new(Cursor::new(encode_snapshot_payload(&payload).unwrap())),
            )
            .await
            .expect_err("data publication failure is before the durable commit point");
        assert!(error.to_string().contains("decode"));
        assert_eq!(store.last_applied_state().await.unwrap().0, None);
        assert!(store.get_current_snapshot().await.unwrap().is_none());
        let txn = storage.begin_transaction().await.unwrap();
        assert_eq!(
            txn.get(b"data:stale:1").await.unwrap(),
            Some(b"stale-row".to_vec())
        );
        assert_eq!(txn.get(b"data:invalid:1").await.unwrap(), None);
        assert_eq!(
            txn.get(RAFT_SNAPSHOT_INSTALL_MARKER_KEY).await.unwrap(),
            None
        );
        txn.rollback().await.unwrap();

        cleanup_dir(&data_dir);
    }

    #[tokio::test]
    async fn leader_evaluated_request_applies_byte_identically_to_independent_stores() {
        let leader_wal = format!("test_raft_eval_leader_{}.wal", uuid::Uuid::new_v4());
        let left_wal = format!("test_raft_eval_left_{}.wal", uuid::Uuid::new_v4());
        let right_wal = format!("test_raft_eval_right_{}.wal", uuid::Uuid::new_v4());

        let leader_storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&leader_wal).unwrap());
        let leader_executor = Arc::new(Executor::new(leader_storage.clone()));
        leader_executor
            .execute_sql(
                "CREATE TABLE raft_events (serial_id SERIAL, created TIMESTAMP, payload TEXT)",
            )
            .await
            .unwrap();

        let seed_entries = export_visible_storage(&leader_storage).await.unwrap();
        let left_storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&left_wal).unwrap());
        let right_storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&right_wal).unwrap());
        for storage in [&left_storage, &right_storage] {
            let mut txn = storage.begin_transaction().await.unwrap();
            for (key, value) in &seed_entries {
                txn.put(key, value).await.unwrap();
            }
            txn.commit().await.unwrap();
        }

        let request = evaluate_sql_to_request(
            &leader_executor,
            "INSERT INTO raft_events(created, payload) VALUES (CURRENT_TIMESTAMP, 'once')",
        )
        .await
        .unwrap();
        assert!(matches!(request, Request::MutationBatch(_)));

        let entry = Entry {
            log_id: test_log_id(9, 42),
            payload: EntryPayload::Normal(request),
        };
        let mut left_store = FusionRaftStore::new(
            Arc::new(Executor::new(left_storage.clone())),
            left_storage.clone(),
        );
        let mut right_store = FusionRaftStore::new(
            Arc::new(Executor::new(right_storage.clone())),
            right_storage.clone(),
        );

        let left_response = left_store
            .apply_to_state_machine(std::slice::from_ref(&entry))
            .await
            .unwrap();
        let right_response = right_store
            .apply_to_state_machine(std::slice::from_ref(&entry))
            .await
            .unwrap();
        assert_eq!(left_response, right_response);
        assert!(left_response[0].success);

        let left_entries = export_visible_storage(&left_storage).await.unwrap();
        let right_entries = export_visible_storage(&right_storage).await.unwrap();
        assert_eq!(left_entries, right_entries);
        assert_eq!(
            left_entries
                .iter()
                .filter(|(key, _)| key.starts_with(b"data:raft_events:"))
                .count(),
            1
        );

        for wal in [leader_wal, left_wal, right_wal] {
            let _ = std::fs::remove_file(wal);
        }
    }

    #[tokio::test]
    async fn fusion_state_machine_apply_is_identical_despite_different_local_mvcc_clocks() {
        let (mut left_store, left_storage, left_dir) = test_fusion_store("clock_left").await;
        let (mut right_store, right_storage, right_dir) = test_fusion_store("clock_right").await;
        let ddl = "CREATE TABLE raft_clock (id INTEGER PRIMARY KEY, payload TEXT)";
        left_store.executor.execute_sql(ddl).await.unwrap();
        right_store.executor.execute_sql(ddl).await.unwrap();

        // Advance only the left MVCC clock without changing its visible key set.
        for value in [Some(b"temporary".as_slice()), None] {
            let mut txn = left_storage.begin_transaction().await.unwrap();
            txn.as_any()
                .downcast_ref::<FusionTransaction>()
                .unwrap()
                .disable_cdc_capture();
            match value {
                Some(value) => txn.put(b"\0clock-padding", value).await.unwrap(),
                None => txn.delete(b"\0clock-padding").await.unwrap(),
            }
            txn.commit().await.unwrap();
        }

        let request = evaluate_sql_to_request(
            &right_store.executor,
            "INSERT INTO raft_clock(id, payload) VALUES (1, 'same') RETURNING id, payload",
        )
        .await
        .unwrap();
        let entry = Entry {
            log_id: test_log_id(11, 19),
            payload: EntryPayload::Normal(request),
        };
        let left_response = left_store
            .apply_to_state_machine(std::slice::from_ref(&entry))
            .await
            .unwrap();
        let right_response = right_store
            .apply_to_state_machine(std::slice::from_ref(&entry))
            .await
            .unwrap();

        assert_eq!(left_response, right_response);
        assert!(!left_response[0].results.is_empty());
        assert_eq!(
            export_visible_storage(&left_storage).await.unwrap(),
            export_visible_storage(&right_storage).await.unwrap()
        );

        cleanup_dir(&left_dir);
        cleanup_dir(&right_dir);
    }

    #[tokio::test]
    async fn state_machine_rejects_legacy_raw_sql_without_executing_it() {
        let (mut store, storage, wal_path) = test_store("legacy_sql_rejected");
        let entry = Entry {
            log_id: test_log_id(2, 3),
            payload: EntryPayload::Normal(Request::LegacySql {
                sql: "CREATE TABLE must_not_exist (id INTEGER)".to_string(),
            }),
        };

        let responses = store.apply_to_state_machine(&[entry]).await.unwrap();
        assert!(!responses[0].success);
        let txn = storage.begin_transaction().await.unwrap();
        assert_eq!(txn.get(b"schema:must_not_exist").await.unwrap(), None);

        let _ = std::fs::remove_file(wal_path);
    }

    #[tokio::test]
    async fn state_machine_fails_closed_when_a_mutation_precondition_changed() {
        let (mut store, storage, wal_path) = test_store("precondition_changed");
        put_entry(&storage, b"data:orders:1", b"newer-value").await;
        let entry = Entry {
            log_id: test_log_id(4, 5),
            payload: EntryPayload::Normal(Request::MutationBatch(MutationBatch {
                version: MUTATION_BATCH_VERSION,
                preconditions: vec![crate::distributed::typ::KvPrecondition {
                    key: b"data:orders:1".to_vec(),
                    expected: Some(b"evaluated-value".to_vec()),
                }],
                mutations: vec![KvMutation::Put {
                    key: b"data:orders:1".to_vec(),
                    value: b"must-not-commit".to_vec(),
                }],
                side_index_mutations: Vec::new(),
                response: Response::success("Updated 1 rows"),
            })),
        };

        let error = store
            .apply_to_state_machine(&[entry])
            .await
            .expect_err("changed leader precondition must stop state-machine apply");
        assert!(error.to_string().contains("precondition failed"));
        let txn = storage.begin_transaction().await.unwrap();
        assert_eq!(
            txn.get(b"data:orders:1").await.unwrap(),
            Some(b"newer-value".to_vec())
        );

        let _ = std::fs::remove_file(wal_path);
    }

    #[tokio::test]
    async fn log_reader_observes_entries_appended_after_reader_creation() {
        let (mut store, _storage, wal_path) = test_store("shared_log_reader");
        let mut reader = store.get_log_reader().await;
        let entry = Entry {
            log_id: test_log_id(5, 7),
            payload: EntryPayload::Blank,
        };

        store.append_to_log([entry.clone()]).await.unwrap();

        assert_eq!(reader.try_get_log_entries(0..).await.unwrap(), vec![entry]);
        let _ = std::fs::remove_file(wal_path);
    }

    #[tokio::test]
    async fn log_state_uses_purged_boundary_when_no_entries_remain() {
        let (mut store, _storage, wal_path) = test_store("purged_log_state");
        let entry = Entry {
            log_id: test_log_id(6, 9),
            payload: EntryPayload::Blank,
        };
        store.append_to_log([entry.clone()]).await.unwrap();
        store.purge_logs_upto(entry.log_id).await.unwrap();

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(entry.log_id));
        assert_eq!(state.last_log_id, Some(entry.log_id));
        let _ = std::fs::remove_file(wal_path);
    }

    #[tokio::test]
    async fn durable_log_reopens_with_bincode_safe_returning_values() {
        use crate::distributed::typ::{ReplicatedQueryResult, ReplicatedValue};

        let (mut store, storage, wal_path, raft_dir) = durable_test_store("returning_values").await;
        let response = Response::success_with_results(
            "INSERT 1",
            vec![ReplicatedQueryResult::Select {
                columns: vec!["id".to_string(), "payload".to_string()],
                rows: vec![vec![
                    ReplicatedValue::Integer(7),
                    ReplicatedValue::Object(vec![(
                        "nested".to_string(),
                        ReplicatedValue::Array(vec![
                            ReplicatedValue::Boolean(true),
                            ReplicatedValue::FloatBits(1.5f64.to_bits()),
                        ]),
                    )]),
                ]],
            }],
        );
        let entry = Entry {
            log_id: test_log_id(8, 11),
            payload: EntryPayload::Normal(Request::MutationBatch(MutationBatch {
                version: MUTATION_BATCH_VERSION,
                preconditions: Vec::new(),
                mutations: Vec::new(),
                side_index_mutations: Vec::new(),
                response,
            })),
        };
        store.append_to_log([entry.clone()]).await.unwrap();
        drop(store);

        let executor = Arc::new(Executor::new(storage.clone()));
        let mut reopened = FusionRaftStore::open_durable(executor, storage, &raft_dir)
            .await
            .unwrap();
        assert_eq!(
            reopened.try_get_log_entries(0..).await.unwrap(),
            vec![entry]
        );

        let _ = std::fs::remove_file(wal_path);
        cleanup_dir(&raft_dir);
    }

    #[tokio::test]
    async fn applied_idempotency_watermark_uses_constant_storage() {
        let (mut store, storage, wal_path) = test_store("applied_watermark");
        let entries: Vec<_> = (1..=3)
            .map(|index| Entry {
                log_id: test_log_id(10, index),
                payload: EntryPayload::Normal(Request::MutationBatch(MutationBatch {
                    version: MUTATION_BATCH_VERSION,
                    preconditions: Vec::new(),
                    mutations: vec![KvMutation::Put {
                        key: format!("data:watermark:{index}").into_bytes(),
                        value: vec![index as u8],
                    }],
                    side_index_mutations: Vec::new(),
                    response: Response::success(format!("entry {index}")),
                })),
            })
            .collect();

        store.apply_to_state_machine(&entries).await.unwrap();
        let txn = storage.begin_transaction().await.unwrap();
        let markers = txn.scan_prefix(b"\0fusiondb/raft/", None).await.unwrap();
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].0, RAFT_APPLIED_WATERMARK_KEY);
        assert_eq!(
            decode_applied_watermark(&markers[0].1).unwrap(),
            entries[2].log_id
        );

        let _ = std::fs::remove_file(wal_path);
    }

    fn durable_install_fixture() -> (
        SnapshotMeta<NodeId, openraft::BasicNode>,
        Vec<u8>,
        LogId<NodeId>,
    ) {
        let log_id = test_log_id(12, 34);
        let meta = SnapshotMeta {
            last_log_id: Some(log_id),
            last_membership: StoredMembership::default(),
            snapshot_id: "12-1-34".to_string(),
        };
        let payload = FusionSnapshotPayload {
            version: SNAPSHOT_PAYLOAD_VERSION,
            entries: vec![
                (b"data:installed:1".to_vec(), b"row-one".to_vec()),
                (
                    b"schema:installed".to_vec(),
                    bincode::serialize(&TableSchema::new("installed".to_string(), Vec::new()))
                        .unwrap(),
                ),
            ],
        };
        (meta, encode_snapshot_payload(&payload).unwrap(), log_id)
    }

    #[tokio::test]
    async fn durable_snapshot_install_recovers_every_publication_boundary() {
        let checkpoints = [
            SnapshotInstallCheckpoint::AfterIntent,
            SnapshotInstallCheckpoint::AfterData,
            SnapshotInstallCheckpoint::AfterSnapshot,
            SnapshotInstallCheckpoint::AfterStateMachine,
        ];

        for checkpoint in checkpoints {
            let (mut store, storage, wal_path, raft_dir) =
                durable_test_store(&format!("install_{checkpoint:?}")).await;
            put_entry(&storage, b"schema:stale", b"stale-value").await;
            let (meta, snapshot_bytes, log_id) = durable_install_fixture();
            let persistence = store.persistence.as_ref().unwrap().clone();
            let intent_path = persistence.snapshot_install_path();
            persistence.set_snapshot_install_checkpoint(checkpoint);

            let result = store
                .install_snapshot(&meta, Box::new(Cursor::new(snapshot_bytes.clone())))
                .await;
            assert!(intent_path.exists());

            let (last_applied, _) = store.last_applied_state().await.unwrap();
            let txn = storage.begin_transaction().await.unwrap();
            if checkpoint == SnapshotInstallCheckpoint::AfterIntent {
                let error = result.expect_err("pre-commit checkpoint must fail installation");
                assert!(error.to_string().contains("injected Raft snapshot"));
                assert_eq!(last_applied, None);
                assert_eq!(
                    txn.get(b"schema:stale").await.unwrap(),
                    Some(b"stale-value".to_vec())
                );
                assert_eq!(txn.get(b"data:installed:1").await.unwrap(), None);
            } else {
                result.expect("post-commit finalization failure must not fail installation");
                assert_eq!(last_applied, Some(log_id));
                assert_eq!(txn.get(b"schema:stale").await.unwrap(), None);
                assert_eq!(
                    txn.get(b"data:installed:1").await.unwrap(),
                    Some(b"row-one".to_vec())
                );
                let current = store
                    .get_current_snapshot()
                    .await
                    .unwrap()
                    .expect("published data must not expose the old in-memory boundary");
                assert_eq!(current.meta, meta);
            }
            txn.rollback().await.unwrap();
            drop(store);

            let executor = Arc::new(Executor::new(storage.clone()));
            let mut reopened = FusionRaftStore::open_durable(executor, storage.clone(), &raft_dir)
                .await
                .unwrap();
            assert!(!intent_path.exists());
            assert_eq!(reopened.last_applied_state().await.unwrap().0, Some(log_id));
            assert_eq!(
                reopened
                    .get_current_snapshot()
                    .await
                    .unwrap()
                    .expect("reconciled snapshot")
                    .meta,
                meta
            );
            let txn = storage.begin_transaction().await.unwrap();
            assert_eq!(txn.get(b"schema:stale").await.unwrap(), None);
            assert_eq!(
                txn.get(b"data:installed:1").await.unwrap(),
                Some(b"row-one".to_vec())
            );
            txn.rollback().await.unwrap();

            let _ = std::fs::remove_file(wal_path);
            cleanup_dir(&raft_dir);
        }
    }

    #[tokio::test]
    async fn durable_snapshot_install_intent_is_independently_crc_framed() {
        let (mut store, storage, wal_path, raft_dir) = durable_test_store("install_crc").await;
        let (meta, snapshot_bytes, _) = durable_install_fixture();
        let persistence = store.persistence.as_ref().unwrap().clone();
        persistence.set_snapshot_install_checkpoint(SnapshotInstallCheckpoint::AfterIntent);
        store
            .install_snapshot(&meta, Box::new(Cursor::new(snapshot_bytes)))
            .await
            .expect_err("intent checkpoint must fail");
        let intent_path = persistence.snapshot_install_path();
        let mut bytes = std::fs::read(&intent_path).unwrap();
        assert_eq!(&bytes[..4], b"FRSI");
        let payload_byte = bytes.len() - 5;
        bytes[payload_byte] ^= 0x40;
        std::fs::write(&intent_path, bytes).unwrap();
        drop(store);

        let executor = Arc::new(Executor::new(storage.clone()));
        let error = FusionRaftStore::open_durable(executor, storage, &raft_dir)
            .await
            .err()
            .expect("corrupt install intent must fail startup");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        let _ = std::fs::remove_file(wal_path);
        cleanup_dir(&raft_dir);
    }

    #[tokio::test]
    async fn durable_snapshot_install_rejects_marker_hash_conflict_on_reopen() {
        let (mut store, storage, wal_path, raft_dir) =
            durable_test_store("install_marker_conflict").await;
        let (meta, snapshot_bytes, _) = durable_install_fixture();
        let persistence = store.persistence.as_ref().unwrap().clone();
        persistence.set_snapshot_install_checkpoint(SnapshotInstallCheckpoint::AfterData);
        store
            .install_snapshot(&meta, Box::new(Cursor::new(snapshot_bytes)))
            .await
            .expect("post-commit checkpoint must preserve successful install semantics");

        let marker_bytes = read_internal_value(&storage, RAFT_SNAPSHOT_INSTALL_MARKER_KEY)
            .await
            .unwrap()
            .expect("published marker");
        let mut marker = decode_snapshot_install_marker(&marker_bytes).unwrap();
        marker.payload_sha256[0] ^= 0x80;
        let corrupt_marker = encode_snapshot_install_marker(&marker).unwrap();
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(RAFT_SNAPSHOT_INSTALL_MARKER_KEY, &corrupt_marker)
            .await
            .unwrap();
        txn.commit().await.unwrap();
        drop(store);

        let executor = Arc::new(Executor::new(storage.clone()));
        let error = FusionRaftStore::open_durable(executor, storage, &raft_dir)
            .await
            .err()
            .expect("marker hash conflict must fail startup");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("hash or metadata conflicts"));

        let _ = std::fs::remove_file(wal_path);
        cleanup_dir(&raft_dir);
    }

    #[tokio::test]
    async fn fusion_snapshot_install_reconciles_after_physical_storage_restart() {
        let data_dir = std::env::temp_dir().join(format!(
            "fusiondb_raft_install_restart_{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        config.wal_file = "fusion.wal".to_string();
        config.sstable_dir = "sstables".to_string();
        let wal_path = config.wal_path();
        let raft_dir = data_dir.join("raft");

        let storage: Arc<dyn Storage> = Arc::new(
            FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
                .await
                .unwrap(),
        );
        let executor = Arc::new(Executor::new(storage.clone()));
        let mut store = FusionRaftStore::open_durable(executor, storage.clone(), &raft_dir)
            .await
            .unwrap();
        put_entry(&storage, b"schema:stale", b"stale-value").await;
        let (meta, snapshot_bytes, log_id) = durable_install_fixture();
        let persistence = store.persistence.as_ref().unwrap().clone();
        let intent_path = persistence.snapshot_install_path();
        persistence.set_snapshot_install_checkpoint(SnapshotInstallCheckpoint::AfterData);
        store
            .install_snapshot(&meta, Box::new(Cursor::new(snapshot_bytes)))
            .await
            .expect("post-commit checkpoint must preserve successful install semantics");
        assert!(intent_path.exists());
        drop(store);
        drop(storage);

        let reopened_storage: Arc<dyn Storage> = Arc::new(
            FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
                .await
                .unwrap(),
        );
        let executor = Arc::new(Executor::new(reopened_storage.clone()));
        let mut reopened =
            FusionRaftStore::open_durable(executor, reopened_storage.clone(), &raft_dir)
                .await
                .unwrap();
        assert!(!intent_path.exists());
        assert_eq!(reopened.last_applied_state().await.unwrap().0, Some(log_id));
        let txn = reopened_storage.begin_transaction().await.unwrap();
        assert_eq!(txn.get(b"schema:stale").await.unwrap(), None);
        assert_eq!(
            txn.get(b"data:installed:1").await.unwrap(),
            Some(b"row-one".to_vec())
        );
        txn.rollback().await.unwrap();
        drop(reopened);
        drop(reopened_storage);
        cleanup_dir(&data_dir);
    }

    #[tokio::test]
    async fn durable_store_recovers_vote_log_membership_applied_state_and_snapshot() {
        let (mut store, storage, wal_path, raft_dir) = durable_test_store("reopen").await;
        let vote = Vote::new_committed(7, 1);
        store.save_vote(&vote).await.unwrap();

        let membership = openraft::Membership::from(BTreeMap::from([(
            1,
            openraft::BasicNode {
                addr: "127.0.0.1:28080".to_string(),
            },
        )]));
        let entries = vec![
            Entry {
                log_id: test_log_id(7, 1),
                payload: EntryPayload::Blank,
            },
            Entry {
                log_id: test_log_id(7, 2),
                payload: EntryPayload::Membership(membership.clone()),
            },
        ];
        store.append_to_log(entries.clone()).await.unwrap();
        store.purge_logs_upto(entries[0].log_id).await.unwrap();
        store.apply_to_state_machine(&entries[1..]).await.unwrap();
        put_entry(&storage, b"schema:durable", b"schema-value").await;
        let built_snapshot = store.build_snapshot().await.unwrap();

        drop(store);
        let executor = Arc::new(Executor::new(storage.clone()));
        let mut reopened = FusionRaftStore::open_durable(executor, storage, &raft_dir)
            .await
            .unwrap();

        assert_eq!(reopened.read_vote().await.unwrap(), Some(vote));
        let log_state = reopened.get_log_state().await.unwrap();
        assert_eq!(log_state.last_purged_log_id, Some(entries[0].log_id));
        assert_eq!(log_state.last_log_id, Some(entries[1].log_id));
        assert_eq!(
            reopened.try_get_log_entries(0..).await.unwrap(),
            vec![entries[1].clone()]
        );
        let (last_applied, stored_membership) = reopened.last_applied_state().await.unwrap();
        assert_eq!(last_applied, Some(entries[1].log_id));
        assert_eq!(stored_membership.membership(), &membership);
        let recovered_snapshot = reopened
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("durable snapshot should survive reopen");
        assert_eq!(recovered_snapshot.meta, built_snapshot.meta);
        assert_eq!(
            recovered_snapshot.snapshot.get_ref(),
            built_snapshot.snapshot.get_ref()
        );

        let _ = std::fs::remove_file(wal_path);
        cleanup_dir(&raft_dir);
    }

    #[tokio::test]
    async fn durable_store_rejects_crc_corruption_on_reopen() {
        let (mut store, storage, wal_path, raft_dir) = durable_test_store("crc").await;
        store.save_vote(&Vote::new(3, 1)).await.unwrap();
        let vote_path = store.persistence.as_ref().unwrap().vote_path();
        drop(store);

        let mut bytes = std::fs::read(&vote_path).unwrap();
        let payload_byte = bytes.len() - 5;
        bytes[payload_byte] ^= 0x80;
        std::fs::write(&vote_path, bytes).unwrap();

        let executor = Arc::new(Executor::new(storage.clone()));
        let error = FusionRaftStore::open_durable(executor, storage, &raft_dir)
            .await
            .err()
            .expect("corrupt Raft metadata must fail startup");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);

        let _ = std::fs::remove_file(wal_path);
        cleanup_dir(&raft_dir);
    }

    // ---- Data V2 migration phase: apply guard + install gate (P10-2.1) ----

    use crate::storage::data_migration::DataMigrationPhase;

    fn phase_record_bytes(phase: DataMigrationPhase, phase_seq: u64) -> Vec<u8> {
        DataMigrationPhaseRecord {
            phase,
            phase_seq,
            updated_at_unix_ms: 42,
        }
        .encode()
        .to_vec()
    }

    fn phase_mutation_entry(term: u64, index: u64, mutation: KvMutation) -> Entry<TypeConfig> {
        Entry {
            log_id: test_log_id(term, index),
            payload: EntryPayload::Normal(Request::MutationBatch(MutationBatch {
                version: MUTATION_BATCH_VERSION,
                preconditions: Vec::new(),
                mutations: vec![mutation],
                side_index_mutations: Vec::new(),
                response: Response::success("phase step"),
            })),
        }
    }

    async fn stored_phase_record(storage: &Arc<dyn Storage>) -> Option<Vec<u8>> {
        let txn = storage.begin_transaction().await.unwrap();
        txn.get(migration_phase_key()).await.unwrap()
    }

    #[tokio::test]
    async fn apply_guard_rejects_invalid_phase_steps_gracefully_and_node_continues() {
        let (mut store, storage, wal_path) = test_store("phase_guard_matrix");

        // (a) Deleting the phase record is always rejected.
        let response = store
            .apply_to_state_machine(&[phase_mutation_entry(
                2,
                1,
                KvMutation::Delete {
                    key: migration_phase_key().to_vec(),
                },
            )])
            .await
            .unwrap();
        assert!(!response[0].success);
        assert!(response[0].message.contains("never be deleted"));

        // (b) A malformed record value is rejected deterministically.
        let response = store
            .apply_to_state_machine(&[phase_mutation_entry(
                2,
                2,
                KvMutation::Put {
                    key: migration_phase_key().to_vec(),
                    value: b"junk".to_vec(),
                },
            )])
            .await
            .unwrap();
        assert!(!response[0].success);
        assert!(response[0].message.contains("malformed"));

        // (c) First build must start at seq 1.
        let response = store
            .apply_to_state_machine(&[phase_mutation_entry(
                2,
                3,
                KvMutation::Put {
                    key: migration_phase_key().to_vec(),
                    value: phase_record_bytes(DataMigrationPhase::DeleteOnly, 5),
                },
            )])
            .await
            .unwrap();
        assert!(!response[0].success);
        assert!(response[0].message.contains("monotonic"));
        assert_eq!(stored_phase_record(&storage).await, None);

        // Valid INIT applies.
        let response = store
            .apply_to_state_machine(&[phase_mutation_entry(
                2,
                4,
                KvMutation::Put {
                    key: migration_phase_key().to_vec(),
                    value: phase_record_bytes(DataMigrationPhase::DeleteOnly, 1),
                },
            )])
            .await
            .unwrap();
        assert!(response[0].success);

        // (d) Skipping a rung is rejected.
        let response = store
            .apply_to_state_machine(&[phase_mutation_entry(
                2,
                5,
                KvMutation::Put {
                    key: migration_phase_key().to_vec(),
                    value: phase_record_bytes(DataMigrationPhase::Backfill, 2),
                },
            )])
            .await
            .unwrap();
        assert!(!response[0].success);
        assert!(response[0].message.contains("monotonic"));

        // Valid advance applies.
        let response = store
            .apply_to_state_machine(&[phase_mutation_entry(
                2,
                6,
                KvMutation::Put {
                    key: migration_phase_key().to_vec(),
                    value: phase_record_bytes(DataMigrationPhase::WriteDeleteShadow, 2),
                },
            )])
            .await
            .unwrap();
        assert!(response[0].success);

        // (e) Downgrade is rejected.
        let response = store
            .apply_to_state_machine(&[phase_mutation_entry(
                2,
                7,
                KvMutation::Put {
                    key: migration_phase_key().to_vec(),
                    value: phase_record_bytes(DataMigrationPhase::DeleteOnly, 3),
                },
            )])
            .await
            .unwrap();
        assert!(!response[0].success);

        // The node keeps applying ordinary writes after every rejection, and
        // the record is exactly the last valid step.
        let response = store
            .apply_to_state_machine(&[phase_mutation_entry(
                2,
                8,
                KvMutation::Put {
                    key: b"data:orders:1".to_vec(),
                    value: b"alive".to_vec(),
                },
            )])
            .await
            .unwrap();
        assert!(response[0].success);
        assert_eq!(
            stored_phase_record(&storage).await,
            Some(phase_record_bytes(DataMigrationPhase::WriteDeleteShadow, 2))
        );

        let _ = std::fs::remove_file(wal_path);
    }

    #[tokio::test]
    async fn apply_halts_when_cluster_advances_beyond_binary_support() {
        let (mut store, storage, wal_path) = test_store("phase_guard_halt");
        // Walk the ladder legitimately up to this binary's ceiling.
        for (index, phase) in [
            DataMigrationPhase::WriteDeleteShadow,
            DataMigrationPhase::Backfill,
        ]
        .into_iter()
        .enumerate()
        {
            let seq = index as u64 + 1;
            let response = store
                .apply_to_state_machine(&[phase_mutation_entry(
                    3,
                    seq,
                    KvMutation::Put {
                        key: migration_phase_key().to_vec(),
                        value: phase_record_bytes(phase, seq),
                    },
                )])
                .await
                .unwrap();
            assert!(response[0].success, "ladder step {phase:?} was rejected");
        }

        // A legitimate next step past this binary's support must halt the
        // state machine, not apply blind.
        let error = store
            .apply_to_state_machine(&[phase_mutation_entry(
                3,
                3,
                KvMutation::Put {
                    key: migration_phase_key().to_vec(),
                    value: phase_record_bytes(DataMigrationPhase::Validated, 3),
                },
            )])
            .await
            .expect_err("advance beyond MAX_SUPPORTED_PHASE must halt");
        assert!(error.to_string().contains("halting"));
        assert_eq!(
            stored_phase_record(&storage).await,
            Some(phase_record_bytes(DataMigrationPhase::Backfill, 2))
        );

        let _ = std::fs::remove_file(wal_path);
    }

    #[tokio::test]
    async fn evaluated_migration_calls_carry_phase_preconditions_and_replicate() {
        let (mut store, storage, wal_path) = test_store("phase_eval_replicate");

        let request =
            evaluate_sql_to_request(&store.executor, "CALL fusiondb_data_migration_init()")
                .await
                .unwrap();
        let Request::MutationBatch(batch) = &request else {
            panic!("migration CALL must evaluate to a mutation batch");
        };
        assert!(
            batch
                .preconditions
                .iter()
                .any(|p| p.key == migration_phase_key() && p.expected.is_none()),
            "INIT must pin the absent phase record as a precondition"
        );
        assert!(batch
            .mutations
            .iter()
            .any(|m| matches!(m, KvMutation::Put { key, .. } if key == migration_phase_key())));

        let init_entry = Entry {
            log_id: test_log_id(4, 1),
            payload: EntryPayload::Normal(request),
        };
        let response = store
            .apply_to_state_machine(std::slice::from_ref(&init_entry))
            .await
            .unwrap();
        assert!(response[0].success);
        let init_record = stored_phase_record(&storage).await.expect("record exists");
        assert_eq!(
            DataMigrationPhaseRecord::decode(&init_record)
                .unwrap()
                .phase,
            DataMigrationPhase::DeleteOnly
        );

        // Idempotent replay of the same log entry is skipped by the applied
        // watermark and must not bump the sequence.
        let replay = store
            .apply_to_state_machine(std::slice::from_ref(&init_entry))
            .await
            .unwrap();
        assert!(replay[0].success);
        assert_eq!(
            stored_phase_record(&storage).await,
            Some(init_record.clone())
        );

        // The advance pins the current record bytes and replicates.
        let request = evaluate_sql_to_request(
            &store.executor,
            "CALL fusiondb_data_migration_advance('write-delete-shadow')",
        )
        .await
        .unwrap();
        let Request::MutationBatch(batch) = &request else {
            panic!("advance must evaluate to a mutation batch");
        };
        assert!(batch
            .preconditions
            .iter()
            .any(|p| p.key == migration_phase_key()
                && p.expected.as_deref() == Some(&init_record[..])));

        let response = store
            .apply_to_state_machine(&[Entry {
                log_id: test_log_id(4, 2),
                payload: EntryPayload::Normal(request),
            }])
            .await
            .unwrap();
        assert!(response[0].success);
        let advanced = stored_phase_record(&storage).await.expect("record exists");
        let advanced = DataMigrationPhaseRecord::decode(&advanced).unwrap();
        assert_eq!(advanced.phase, DataMigrationPhase::WriteDeleteShadow);
        assert_eq!(advanced.phase_seq, 2);

        let _ = std::fs::remove_file(wal_path);
    }

    #[tokio::test]
    async fn evaluated_dml_batches_carry_the_phase_precondition() {
        let (_store, storage, wal_path) = test_store("phase_dml_precondition");
        let executor = Executor::new(storage.clone());
        executor
            .execute_sql("CREATE TABLE phase_orders (id INT PRIMARY KEY, note TEXT)")
            .await
            .unwrap();

        let request =
            evaluate_sql_to_request(&executor, "INSERT INTO phase_orders VALUES (1, 'fenced')")
                .await
                .unwrap();
        let Request::MutationBatch(batch) = &request else {
            panic!("INSERT must evaluate to a mutation batch");
        };
        assert!(
            batch
                .preconditions
                .iter()
                .any(|p| p.key == migration_phase_key()),
            "every data-family batch must pin the phase record"
        );

        let _ = std::fs::remove_file(wal_path);
    }

    #[tokio::test]
    async fn phase_precondition_mismatch_fails_closed() {
        let (mut store, storage, wal_path) = test_store("phase_precondition_mismatch");
        put_entry(
            &storage,
            migration_phase_key(),
            &phase_record_bytes(DataMigrationPhase::WriteDeleteShadow, 2),
        )
        .await;

        // A stale cross-leader proposal pinned the older record.
        let entry = Entry {
            log_id: test_log_id(5, 1),
            payload: EntryPayload::Normal(Request::MutationBatch(MutationBatch {
                version: MUTATION_BATCH_VERSION,
                preconditions: vec![crate::distributed::typ::KvPrecondition {
                    key: migration_phase_key().to_vec(),
                    expected: Some(phase_record_bytes(DataMigrationPhase::DeleteOnly, 1)),
                }],
                mutations: vec![KvMutation::Put {
                    key: b"data:orders:1".to_vec(),
                    value: b"stale-phase-write".to_vec(),
                }],
                side_index_mutations: Vec::new(),
                response: Response::success("Updated 1 rows"),
            })),
        };

        let error = store
            .apply_to_state_machine(&[entry])
            .await
            .expect_err("stale phase precondition must fail closed");
        assert!(error.to_string().contains("precondition failed"));
        let txn = storage.begin_transaction().await.unwrap();
        assert_eq!(txn.get(b"data:orders:1").await.unwrap(), None);

        let _ = std::fs::remove_file(wal_path);
    }

    /// The leader-built snapshot must actually carry the phase record: if a
    /// range narrowing or a `retain` ever excluded the `\0FDBK` Catalog key,
    /// installs would silently revert a node to config-flag behavior.
    #[tokio::test]
    async fn built_snapshot_payload_round_trips_the_phase_record() {
        let (mut source, source_storage, source_wal) = test_store("phase_snapshot_build");
        put_entry(
            &source_storage,
            migration_phase_key(),
            &phase_record_bytes(DataMigrationPhase::WriteDeleteShadow, 2),
        )
        .await;

        let payload = source.build_snapshot_payload().await.unwrap();
        assert!(
            payload
                .entries
                .iter()
                .any(|(key, value)| key.as_slice() == migration_phase_key()
                    && value.as_slice()
                        == phase_record_bytes(DataMigrationPhase::WriteDeleteShadow, 2)),
            "the built snapshot payload must carry the phase record"
        );

        let (mut target, target_storage, target_wal) = test_store("phase_snapshot_install");
        target
            .install_snapshot(
                &SnapshotMeta {
                    last_log_id: Some(test_log_id(7, 4)),
                    last_membership: StoredMembership::default(),
                    snapshot_id: "7-1-4".to_string(),
                },
                Box::new(Cursor::new(encode_snapshot_payload(&payload).unwrap())),
            )
            .await
            .unwrap();
        assert_eq!(
            stored_phase_record(&target_storage).await,
            Some(phase_record_bytes(DataMigrationPhase::WriteDeleteShadow, 2)),
            "the installed node must land on the source's phase"
        );

        let _ = std::fs::remove_file(source_wal);
        let _ = std::fs::remove_file(target_wal);
    }

    /// The backfill step is single-node only this ticket: driving chunks
    /// through Raft would record a precondition per copied key, so any
    /// concurrent DML between evaluation and apply would halt the state
    /// machine. It must therefore never reach a proposal.
    #[tokio::test]
    async fn raft_evaluation_refuses_the_backfill_step() {
        let (_store, storage, wal_path) = test_store("phase_backfill_closed");
        let executor = Executor::new(storage.clone());
        executor
            .execute_sql("CALL fusiondb_data_migration_init()")
            .await
            .unwrap();

        let error = evaluate_sql_to_request(&executor, "CALL fusiondb_data_backfill_step()")
            .await
            .expect_err("the backfill step must fail closed on the Raft path");
        assert!(
            error.contains("backfill step"),
            "the refusal must name the backfill step: {error}"
        );

        // The read-only status procedure is not a write at all, so it is
        // rejected as "not a mutating statement" rather than proposed.
        let error = evaluate_sql_to_request(&executor, "CALL fusiondb_data_backfill_status()")
            .await
            .expect_err("status is not a mutating statement");
        assert!(error.contains("mutating"), "unexpected: {error}");

        let _ = std::fs::remove_file(wal_path);
    }

    /// Once the store is at Backfill, a batch that writes base rows without a
    /// phase precondition came from a binary that does not know about the
    /// fence. It must be rejected deterministically — not applied, and not
    /// halting.
    #[tokio::test]
    async fn apply_rejects_unfenced_data_writes_at_backfill_phase() {
        let (mut store, storage, wal_path) = test_store("phase_unfenced_guard");
        for (seq, phase) in [
            DataMigrationPhase::WriteDeleteShadow,
            DataMigrationPhase::Backfill,
        ]
        .into_iter()
        .enumerate()
        {
            let seq = seq as u64 + 1;
            store
                .apply_to_state_machine(&[phase_mutation_entry(
                    8,
                    seq,
                    KvMutation::Put {
                        key: migration_phase_key().to_vec(),
                        value: phase_record_bytes(phase, seq),
                    },
                )])
                .await
                .unwrap();
        }

        // All three physical base-row shapes must be caught.
        for (index, key) in [
            b"data:orders:1".to_vec(),
            b"shard:3:data:orders:1".to_vec(),
            crate::storage::keyspace::encode_data_key(
                crate::storage::keyspace::DataRoute::Unsharded,
                b"orders",
                b"1",
            )
            .unwrap(),
        ]
        .into_iter()
        .enumerate()
        {
            let response = store
                .apply_to_state_machine(&[phase_mutation_entry(
                    8,
                    10 + index as u64,
                    KvMutation::Put {
                        key: key.clone(),
                        value: b"unfenced".to_vec(),
                    },
                )])
                .await
                .unwrap();
            assert!(
                !response[0].success,
                "an unfenced data write was accepted at phase backfill: {:?}",
                String::from_utf8_lossy(&key)
            );
            assert!(response[0].message.contains("phase precondition"));
            let txn = storage.begin_transaction().await.unwrap();
            assert_eq!(
                txn.get(&key).await.unwrap(),
                None,
                "the write must not land"
            );
        }

        // A non-data key is unaffected, and a properly fenced data write applies.
        let response = store
            .apply_to_state_machine(&[phase_mutation_entry(
                8,
                20,
                KvMutation::Put {
                    key: b"schema:orders".to_vec(),
                    value: b"not a base row".to_vec(),
                },
            )])
            .await
            .unwrap();
        assert!(response[0].success);

        let fenced = Entry {
            log_id: test_log_id(8, 21),
            payload: EntryPayload::Normal(Request::MutationBatch(MutationBatch {
                version: MUTATION_BATCH_VERSION,
                preconditions: vec![crate::distributed::typ::KvPrecondition {
                    key: migration_phase_key().to_vec(),
                    expected: Some(phase_record_bytes(DataMigrationPhase::Backfill, 2)),
                }],
                mutations: vec![KvMutation::Put {
                    key: b"data:orders:2".to_vec(),
                    value: b"fenced".to_vec(),
                }],
                side_index_mutations: Vec::new(),
                response: Response::success("Inserted 1 rows"),
            })),
        };
        let response = store.apply_to_state_machine(&[fenced]).await.unwrap();
        assert!(response[0].success, "a fenced data write must still apply");

        let _ = std::fs::remove_file(wal_path);
    }

    /// CTE materialization is query-local and must never enter a replicated
    /// mutation batch. A zero-row outer INSERT therefore carries no CTE
    /// schema/data mutations even after the cluster reaches Backfill.
    #[tokio::test]
    async fn cte_rows_stay_out_of_raft_batches_at_backfill() {
        let (mut store, storage, wal_path) = test_store("phase_cte_fenced");
        let executor = Executor::new(storage.clone());
        executor
            .execute_sql("CREATE TABLE cte_src (id INT PRIMARY KEY)")
            .await
            .unwrap();
        executor
            .execute_sql("CREATE TABLE cte_dst (id INT PRIMARY KEY)")
            .await
            .unwrap();
        for sql in [
            "CALL fusiondb_data_migration_init()",
            "CALL fusiondb_data_migration_advance('write-delete-shadow')",
            "CALL fusiondb_data_migration_advance('backfill')",
        ] {
            executor.execute_sql(sql).await.unwrap();
        }
        // Mirror the phase onto the state machine's own storage.
        for (seq, phase) in [
            DataMigrationPhase::WriteDeleteShadow,
            DataMigrationPhase::Backfill,
        ]
        .into_iter()
        .enumerate()
        {
            let seq = seq as u64 + 1;
            store
                .apply_to_state_machine(&[phase_mutation_entry(
                    9,
                    seq,
                    KvMutation::Put {
                        key: migration_phase_key().to_vec(),
                        value: phase_record_bytes(phase, seq),
                    },
                )])
                .await
                .unwrap();
        }

        // The outer INSERT selects nothing, so any schema/data mutation would
        // necessarily be an accidental persistence of the CTE itself.
        let request = evaluate_sql_to_request(
            &executor,
            "INSERT INTO cte_dst WITH batch AS (SELECT id FROM cte_src) SELECT id FROM batch WHERE id > 999999",
        )
        .await
        .expect("a CTE insert must evaluate");
        let Request::MutationBatch(batch) = &request else {
            panic!("expected a mutation batch");
        };
        assert!(
            batch.mutations.is_empty(),
            "CTE rows must not be replicated"
        );
        assert!(
            batch.side_index_mutations.is_empty(),
            "CTE rows must not produce replicated side-index mutations"
        );

        let response = store
            .apply_to_state_machine(&[Entry {
                log_id: test_log_id(9, 30),
                payload: EntryPayload::Normal(request),
            }])
            .await
            .unwrap();
        assert!(
            response[0].success,
            "an ordinary CTE insert must not be rejected at phase backfill: {}",
            response[0].message
        );

        let _ = std::fs::remove_file(wal_path);
    }

    /// The standalone rule must hold on the Raft evaluation path too, not
    /// only in `execute_sql`.
    #[tokio::test]
    async fn raft_evaluation_rejects_migration_call_batched_with_dml() {
        let (_store, storage, wal_path) = test_store("phase_raft_standalone");
        let executor = Executor::new(storage.clone());
        executor
            .execute_sql("CREATE TABLE batched (id INT PRIMARY KEY, note TEXT)")
            .await
            .unwrap();
        executor
            .execute_sql("CALL fusiondb_data_migration_init()")
            .await
            .unwrap();

        let error = evaluate_sql_to_request(
            &executor,
            "INSERT INTO batched VALUES (1, 'x'); CALL fusiondb_data_migration_advance('write-delete-shadow')",
        )
        .await
        .expect_err("an advance batched with DML must be rejected before proposal");
        assert!(error.contains("standalone"), "unexpected: {error}");

        let _ = std::fs::remove_file(wal_path);
    }

    #[tokio::test]
    async fn snapshot_install_refuses_payload_beyond_binary_support() {
        let (mut store, storage, wal_path) = test_store("phase_snapshot_gate");
        let meta = SnapshotMeta {
            last_log_id: Some(test_log_id(6, 3)),
            last_membership: StoredMembership::default(),
            snapshot_id: "6-1-3".to_string(),
        };

        let unsupported = FusionSnapshotPayload {
            version: SNAPSHOT_PAYLOAD_VERSION,
            entries: vec![(
                migration_phase_key().to_vec(),
                phase_record_bytes(DataMigrationPhase::Validated, 4),
            )],
        };
        let error = store
            .install_snapshot(
                &meta,
                Box::new(Cursor::new(encode_snapshot_payload(&unsupported).unwrap())),
            )
            .await
            .expect_err("snapshot beyond MAX_SUPPORTED_PHASE must refuse install");
        assert!(error.to_string().contains("upgrade before installing"));
        assert_eq!(stored_phase_record(&storage).await, None);

        // A supported-phase payload installs and the record becomes visible.
        let supported = FusionSnapshotPayload {
            version: SNAPSHOT_PAYLOAD_VERSION,
            entries: vec![(
                migration_phase_key().to_vec(),
                phase_record_bytes(DataMigrationPhase::WriteDeleteShadow, 2),
            )],
        };
        store
            .install_snapshot(
                &meta,
                Box::new(Cursor::new(encode_snapshot_payload(&supported).unwrap())),
            )
            .await
            .expect("supported-phase snapshot installs");
        assert_eq!(
            stored_phase_record(&storage).await,
            Some(phase_record_bytes(DataMigrationPhase::WriteDeleteShadow, 2))
        );

        let _ = std::fs::remove_file(wal_path);
    }
}
