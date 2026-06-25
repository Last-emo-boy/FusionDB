use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::sync::Arc;

use openraft::log_id::RaftLogId;
use openraft::{
    AnyError, Entry, EntryPayload, LogId, RaftLogReader, RaftSnapshotBuilder, RaftStorage,
    Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};

use super::typ::{NodeId, Request, Response, TypeConfig};
use crate::execution::{Executor, QueryResult};
use crate::storage::{FusionStorage, Storage};

const SNAPSHOT_PAYLOAD_VERSION: u16 = 1;
const SNAPSHOT_SCAN_START: &[u8] = b"";
const SNAPSHOT_SCAN_END: &[u8] = &[0xff];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct FusionSnapshotPayload {
    version: u16,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Combined Raft storage: log + state machine + snapshots.
/// Used with `openraft::storage::Adaptor` to satisfy both
/// `RaftLogStorage` and `RaftStateMachine`.
pub struct FusionRaftStore {
    vote: Option<Vote<NodeId>>,
    log: BTreeMap<u64, Entry<TypeConfig>>,
    last_purged: Option<LogId<NodeId>>,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
    snapshot: Option<Snapshot<TypeConfig>>,
    // Application state
    executor: Arc<Executor>,
    #[allow(dead_code)]
    storage: Arc<dyn Storage>,
}

impl FusionRaftStore {
    pub fn new(executor: Arc<Executor>, storage: Arc<dyn Storage>) -> Self {
        Self {
            vote: None,
            log: BTreeMap::new(),
            last_purged: None,
            last_applied: None,
            last_membership: StoredMembership::default(),
            snapshot: None,
            executor,
            storage,
        }
    }

    pub async fn apply_sql(&self, req: &Request) -> Response {
        let stmts = match self.executor.prepare(&req.sql) {
            Ok(s) => s,
            Err(e) => {
                return Response {
                    message: format!("Parse error: {}", e),
                }
            }
        };

        let mut results = Vec::new();
        for stmt in &stmts {
            match self.executor.execute(stmt).await {
                Ok(QueryResult::Success { message }) => results.push(message),
                Ok(QueryResult::Select { columns, rows }) => {
                    results.push(format!("{} columns, {} rows", columns.len(), rows.len()));
                }
                Err(e) => results.push(format!("Error: {}", e)),
            }
        }

        Response {
            message: results.join("; "),
        }
    }

    async fn build_snapshot_payload(&self) -> Result<FusionSnapshotPayload, StorageError<NodeId>> {
        let entries = export_visible_storage(&self.storage).await?;
        Ok(FusionSnapshotPayload {
            version: SNAPSHOT_PAYLOAD_VERSION,
            entries,
        })
    }
}

fn snapshot_write_error(error: impl ToString) -> StorageError<NodeId> {
    StorageIOError::write_snapshot(None, AnyError::error(error.to_string())).into()
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

fn encode_snapshot_payload(
    payload: &FusionSnapshotPayload,
) -> Result<Vec<u8>, StorageError<NodeId>> {
    bincode::serialize(payload).map_err(snapshot_write_error)
}

fn decode_snapshot_payload(bytes: &[u8]) -> Result<FusionSnapshotPayload, StorageError<NodeId>> {
    let payload: FusionSnapshotPayload =
        bincode::deserialize(bytes).map_err(snapshot_read_error)?;
    if payload.version != SNAPSHOT_PAYLOAD_VERSION {
        return Err(snapshot_read_error(format!(
            "unsupported FusionDB snapshot payload version {}",
            payload.version
        )));
    }
    Ok(payload)
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
        let entries: Vec<_> = self.log.range(range).map(|(_, v)| v.clone()).collect();
        Ok(entries)
    }
}

// --- RaftSnapshotBuilder ---

impl RaftSnapshotBuilder<TypeConfig> for FusionRaftStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let snapshot_id = self
            .last_applied
            .map(|id| format!("{}-{}", id.leader_id, id.index))
            .unwrap_or_else(|| "empty".to_string());
        let payload = self.build_snapshot_payload().await?;
        let snapshot_bytes = encode_snapshot_payload(&payload)?;

        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };

        self.snapshot = Some(Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(snapshot_bytes.clone())),
        });

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
        self.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.vote)
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<openraft::LogState<TypeConfig>, StorageError<NodeId>> {
        let last_log_id = self.log.iter().next_back().map(|(_, e)| *e.get_log_id());
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
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot: None,
            executor: self.executor.clone(),
            storage: self.storage.clone(),
        }
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        for entry in entries {
            let log_id = *entry.get_log_id();
            self.log.insert(log_id.index, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let keys: Vec<_> = self.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in keys {
            self.log.remove(&k);
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let keys: Vec<_> = self.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys {
            self.log.remove(&k);
        }
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
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<Response>, StorageError<NodeId>> {
        let mut results = Vec::new();
        for entry in entries {
            self.last_applied = Some(*entry.get_log_id());

            match entry.payload {
                EntryPayload::Blank => {
                    results.push(Response {
                        message: String::new(),
                    });
                }
                EntryPayload::Normal(ref req) => {
                    let resp = self.apply_sql(req).await;
                    results.push(resp);
                }
                EntryPayload::Membership(ref mem) => {
                    self.last_membership =
                        StoredMembership::new(Some(*entry.get_log_id()), mem.clone());
                    results.push(Response {
                        message: "membership changed".to_string(),
                    });
                }
            }
        }
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        FusionRaftStore {
            vote: self.vote,
            log: self.log.clone(),
            last_purged: self.last_purged,
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot: None,
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
        let snapshot_bytes = (*snapshot).into_inner();
        let payload = decode_snapshot_payload(&snapshot_bytes)?;
        replace_visible_storage(&self.storage, &payload.entries).await?;
        self.executor.invalidate_storage_caches();

        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        self.snapshot = Some(Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(snapshot_bytes)),
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        if let Some(snap) = &self.snapshot {
            let data = snap.snapshot.get_ref().clone();
            Ok(Some(Snapshot {
                meta: snap.meta.clone(),
                snapshot: Box::new(Cursor::new(data)),
            }))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageConfig;
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
        put_entry(&source_storage, b"schema:users", b"schema-bytes").await;
        put_entry(&source_storage, b"data:users:1", b"row-one").await;

        let snapshot = source_store.build_snapshot().await.unwrap();
        let payload = decode_snapshot_payload(snapshot.snapshot.get_ref()).unwrap();
        let mut receiving = target_store.begin_receiving_snapshot().await.unwrap();
        *receiving = Cursor::new(snapshot.snapshot.get_ref().clone());

        target_store
            .install_snapshot(&snapshot.meta, receiving)
            .await
            .unwrap();

        let installed_entries = export_visible_storage(&target_storage).await.unwrap();
        assert_eq!(installed_entries, payload.entries);

        cleanup_dir(&source_dir);
        cleanup_dir(&target_dir);
    }
}
