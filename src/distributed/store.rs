use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::sync::Arc;

use openraft::log_id::RaftLogId;
use openraft::{
    Entry, EntryPayload, LogId, RaftLogReader, RaftSnapshotBuilder, RaftStorage,
    Snapshot, SnapshotMeta, StorageError, StoredMembership, Vote,
};

use super::typ::{NodeId, Request, Response, TypeConfig};
use crate::execution::{Executor, QueryResult};
use crate::storage::Storage;

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
}

// --- RaftLogReader ---

impl RaftLogReader<TypeConfig> for FusionRaftStore {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + Debug + openraft::OptionalSend>(
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

        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };

        let snap = Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(Vec::new())),
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

    async fn get_log_state(&mut self) -> Result<openraft::LogState<TypeConfig>, StorageError<NodeId>> {
        let last_log_id = self
            .log
            .iter()
            .next_back()
            .map(|(_, e)| *e.get_log_id());
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

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
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
        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        self.snapshot = Some(Snapshot {
            meta: meta.clone(),
            snapshot,
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        // Return a clone of the snapshot if available
        if let Some(snap) = &self.snapshot {
            let data = Vec::new(); // simplified
            Ok(Some(Snapshot {
                meta: snap.meta.clone(),
                snapshot: Box::new(Cursor::new(data)),
            }))
        } else {
            Ok(None)
        }
    }
}
