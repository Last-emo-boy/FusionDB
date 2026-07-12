use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use openraft::{Entry, LogId, SnapshotMeta, StoredMembership, Vote};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::typ::{NodeId, TypeConfig};

const FRAME_VERSION: u16 = 1;
const FRAME_HEADER_LEN: usize = 4 + 2 + 8;
const FRAME_TRAILER_LEN: usize = 4;
const VOTE_MAGIC: [u8; 4] = *b"FRVT";
const LOG_MAGIC: [u8; 4] = *b"FRLG";
const STATE_MACHINE_MAGIC: [u8; 4] = *b"FRSM";
const SNAPSHOT_MAGIC: [u8; 4] = *b"FRSN";
const SNAPSHOT_INSTALL_MAGIC: [u8; 4] = *b"FRSI";

const VOTE_FILE: &str = "vote.bin";
const LOG_FILE: &str = "log.bin";
const STATE_MACHINE_FILE: &str = "state-machine.bin";
const SNAPSHOT_FILE: &str = "snapshot.bin";
const SNAPSHOT_INSTALL_FILE: &str = "snapshot-install.bin";

#[derive(Debug, Default, Serialize, Deserialize)]
struct DurableVote {
    vote: Option<Vote<NodeId>>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct DurableLog {
    last_purged: Option<LogId<NodeId>>,
    entries: BTreeMap<u64, Entry<TypeConfig>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct DurableStateMachine {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
}

impl Default for DurableStateMachine {
    fn default() -> Self {
        Self {
            last_applied: None,
            last_membership: StoredMembership::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DurableSnapshot {
    pub(crate) meta: SnapshotMeta<NodeId, openraft::BasicNode>,
    pub(crate) data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SnapshotInstallIntent {
    pub(crate) snapshot: DurableSnapshot,
    pub(crate) payload_sha256: [u8; 32],
}

impl SnapshotInstallIntent {
    pub(crate) fn new(snapshot: DurableSnapshot) -> Self {
        let payload_sha256 = snapshot_payload_sha256(&snapshot.data);
        Self {
            snapshot,
            payload_sha256,
        }
    }

    pub(crate) fn validate(&self) -> io::Result<()> {
        if snapshot_payload_sha256(&self.snapshot.data) != self.payload_sha256 {
            return Err(invalid_data(
                "Raft snapshot install intent payload hash does not match snapshot data",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotInstallCheckpoint {
    AfterIntent,
    AfterData,
    AfterSnapshot,
    AfterStateMachine,
}

pub(crate) struct RecoveredRaftState {
    pub(crate) vote: Option<Vote<NodeId>>,
    pub(crate) log: BTreeMap<u64, Entry<TypeConfig>>,
    pub(crate) last_purged: Option<LogId<NodeId>>,
    pub(crate) last_applied: Option<LogId<NodeId>>,
    pub(crate) last_membership: StoredMembership<NodeId, openraft::BasicNode>,
    pub(crate) snapshot: Option<DurableSnapshot>,
    pub(crate) snapshot_install: Option<SnapshotInstallIntent>,
}

/// Versioned, CRC-protected Raft metadata rooted in the configured data directory.
///
/// Components use separate atomic files so a snapshot builder, which OpenRaft may
/// run from a cloned store, cannot overwrite a newer vote or log with stale state.
pub(crate) struct RaftPersistence {
    root: PathBuf,
    write_lock: Mutex<()>,
    #[cfg(test)]
    snapshot_install_checkpoint: Mutex<Option<SnapshotInstallCheckpoint>>,
}

impl RaftPersistence {
    pub(crate) fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        create_dir_all_durable(&root)?;

        Ok(Self {
            root,
            write_lock: Mutex::new(()),
            #[cfg(test)]
            snapshot_install_checkpoint: Mutex::new(None),
        })
    }

    pub(crate) fn recover(&self) -> io::Result<RecoveredRaftState> {
        let vote =
            read_component::<DurableVote>(&self.path(VOTE_FILE), VOTE_MAGIC)?.unwrap_or_default();
        let log =
            read_component::<DurableLog>(&self.path(LOG_FILE), LOG_MAGIC)?.unwrap_or_default();
        for (index, entry) in &log.entries {
            if *index != entry.log_id.index {
                return Err(invalid_data(format!(
                    "Raft log map key {index} does not match entry index {}",
                    entry.log_id.index
                )));
            }
            if log
                .last_purged
                .is_some_and(|last_purged| *index <= last_purged.index)
            {
                return Err(invalid_data(format!(
                    "Raft log entry {index} is not newer than purged boundary {}",
                    log.last_purged.unwrap().index
                )));
            }
        }
        let state_machine = read_component::<DurableStateMachine>(
            &self.path(STATE_MACHINE_FILE),
            STATE_MACHINE_MAGIC,
        )?
        .unwrap_or_default();
        let snapshot =
            read_component::<DurableSnapshot>(&self.path(SNAPSHOT_FILE), SNAPSHOT_MAGIC)?;
        let snapshot_install = read_component::<SnapshotInstallIntent>(
            &self.path(SNAPSHOT_INSTALL_FILE),
            SNAPSHOT_INSTALL_MAGIC,
        )?;
        if let Some(intent) = &snapshot_install {
            intent.validate()?;
        }

        Ok(RecoveredRaftState {
            vote: vote.vote,
            log: log.entries,
            last_purged: log.last_purged,
            last_applied: state_machine.last_applied,
            last_membership: state_machine.last_membership,
            snapshot,
            snapshot_install,
        })
    }

    pub(crate) fn persist_vote(&self, vote: Option<Vote<NodeId>>) -> io::Result<()> {
        self.write_component(VOTE_FILE, VOTE_MAGIC, &DurableVote { vote })
    }

    pub(crate) fn persist_log(
        &self,
        last_purged: Option<LogId<NodeId>>,
        entries: &BTreeMap<u64, Entry<TypeConfig>>,
    ) -> io::Result<()> {
        self.write_component(
            LOG_FILE,
            LOG_MAGIC,
            &DurableLog {
                last_purged,
                entries: entries.clone(),
            },
        )
    }

    pub(crate) fn persist_state_machine(
        &self,
        last_applied: Option<LogId<NodeId>>,
        last_membership: &StoredMembership<NodeId, openraft::BasicNode>,
    ) -> io::Result<()> {
        self.write_component(
            STATE_MACHINE_FILE,
            STATE_MACHINE_MAGIC,
            &DurableStateMachine {
                last_applied,
                last_membership: last_membership.clone(),
            },
        )
    }

    pub(crate) fn persist_snapshot(&self, snapshot: &DurableSnapshot) -> io::Result<()> {
        self.write_component(SNAPSHOT_FILE, SNAPSHOT_MAGIC, snapshot)
    }

    pub(crate) fn persist_snapshot_install_intent(
        &self,
        intent: &SnapshotInstallIntent,
    ) -> io::Result<()> {
        intent.validate()?;
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| io::Error::other("Raft persistence lock poisoned"))?;
        if let Some(existing) = read_component::<SnapshotInstallIntent>(
            &self.path(SNAPSHOT_INSTALL_FILE),
            SNAPSHOT_INSTALL_MAGIC,
        )? {
            existing.validate()?;
            if existing.snapshot.meta != intent.snapshot.meta
                || existing.payload_sha256 != intent.payload_sha256
                || existing.snapshot.data != intent.snapshot.data
            {
                return Err(invalid_data(
                    "a different Raft snapshot install intent is already pending",
                ));
            }
        }
        write_component_atomic(
            &self.path(SNAPSHOT_INSTALL_FILE),
            SNAPSHOT_INSTALL_MAGIC,
            intent,
        )
    }

    pub(crate) fn clear_snapshot_install_intent(&self) -> io::Result<()> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| io::Error::other("Raft persistence lock poisoned"))?;
        match fs::remove_file(self.path(SNAPSHOT_INSTALL_FILE)) {
            Ok(()) => sync_directory(&self.root),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn snapshot_install_checkpoint(
        &self,
        checkpoint: SnapshotInstallCheckpoint,
    ) -> io::Result<()> {
        #[cfg(test)]
        {
            let mut armed = self
                .snapshot_install_checkpoint
                .lock()
                .map_err(|_| io::Error::other("Raft snapshot checkpoint lock poisoned"))?;
            if armed.as_ref() == Some(&checkpoint) {
                *armed = None;
                return Err(io::Error::other(format!(
                    "injected Raft snapshot install failure after {checkpoint:?}"
                )));
            }
        }
        #[cfg(not(test))]
        let _ = checkpoint;
        Ok(())
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    fn write_component<T: Serialize>(
        &self,
        name: &str,
        magic: [u8; 4],
        value: &T,
    ) -> io::Result<()> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| io::Error::other("Raft persistence lock poisoned"))?;
        write_component_atomic(&self.path(name), magic, value)
    }

    #[cfg(test)]
    pub(crate) fn vote_path(&self) -> PathBuf {
        self.path(VOTE_FILE)
    }

    #[cfg(test)]
    pub(crate) fn snapshot_install_path(&self) -> PathBuf {
        self.path(SNAPSHOT_INSTALL_FILE)
    }

    #[cfg(test)]
    pub(crate) fn set_snapshot_install_checkpoint(&self, checkpoint: SnapshotInstallCheckpoint) {
        *self.snapshot_install_checkpoint.lock().unwrap() = Some(checkpoint);
    }
}

pub(crate) fn snapshot_payload_sha256(payload: &[u8]) -> [u8; 32] {
    Sha256::digest(payload).into()
}

fn write_component_atomic<T: Serialize>(path: &Path, magic: [u8; 4], value: &T) -> io::Result<()> {
    let payload = bincode::serialize(value).map_err(invalid_data)?;
    let payload_len = u64::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Raft payload too large"))?;
    let checksum = crc32fast::hash(&payload);

    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len() + FRAME_TRAILER_LEN);
    frame.extend_from_slice(&magic);
    frame.extend_from_slice(&FRAME_VERSION.to_le_bytes());
    frame.extend_from_slice(&payload_len.to_le_bytes());
    frame.extend_from_slice(&payload);
    frame.extend_from_slice(&checksum.to_le_bytes());

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid Raft state path"))?;
    let staging_path = path.with_file_name(format!("{file_name}.building"));
    let _ = fs::remove_file(&staging_path);

    let result = (|| {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&staging_path)?;
        file.write_all(&frame)?;
        file.flush()?;
        file.sync_all()?;
        fs::rename(&staging_path, path)?;
        let parent = path.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "Raft state path has no parent")
        })?;
        sync_directory(parent)
    })();

    if result.is_err() {
        let _ = fs::remove_file(staging_path);
    }
    result
}

fn read_component<T: DeserializeOwned>(path: &Path, magic: [u8; 4]) -> io::Result<Option<T>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };

    if bytes.len() < FRAME_HEADER_LEN + FRAME_TRAILER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Raft state file '{}' is truncated", path.display()),
        ));
    }
    if bytes[..4] != magic {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Raft state file '{}' has invalid magic", path.display()),
        ));
    }

    let version = u16::from_le_bytes(bytes[4..6].try_into().expect("fixed version slice"));
    if version != FRAME_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Raft state file '{}' uses unsupported version {version}",
                path.display()
            ),
        ));
    }

    let payload_len = u64::from_le_bytes(bytes[6..14].try_into().expect("fixed length slice"));
    let payload_len = usize::try_from(payload_len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "Raft payload length overflows usize",
        )
    })?;
    let expected_len = FRAME_HEADER_LEN
        .checked_add(payload_len)
        .and_then(|len| len.checked_add(FRAME_TRAILER_LEN))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Raft frame length overflow"))?;
    if bytes.len() != expected_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Raft state file '{}' has invalid length", path.display()),
        ));
    }

    let payload_end = FRAME_HEADER_LEN + payload_len;
    let payload = &bytes[FRAME_HEADER_LEN..payload_end];
    let stored_checksum = u32::from_le_bytes(
        bytes[payload_end..expected_len]
            .try_into()
            .expect("fixed checksum slice"),
    );
    let actual_checksum = crc32fast::hash(payload);
    if stored_checksum != actual_checksum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Raft state file '{}' failed CRC validation", path.display()),
        ));
    }

    let mut cursor = std::io::Cursor::new(payload);
    let value = bincode::deserialize_from(&mut cursor).map_err(invalid_data)?;
    if cursor.position() as usize != payload.len() {
        return Err(invalid_data(format!(
            "Raft state file '{}' has trailing payload bytes",
            path.display()
        )));
    }
    Ok(Some(value))
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn create_dir_all_durable(path: &Path) -> io::Result<()> {
    create_dir_all_durable_with(path, sync_directory)
}

fn create_dir_all_durable_with(
    path: &Path,
    mut sync: impl FnMut(&Path) -> io::Result<()>,
) -> io::Result<()> {
    let mut missing = Vec::new();
    let mut current = path;
    loop {
        match fs::metadata(current) {
            Ok(_) => break,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                missing.push(current.to_path_buf());
                current = directory_parent(current).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Raft state directory has no parent",
                    )
                })?;
            }
            Err(error) => return Err(error),
        }
    }

    fs::create_dir_all(path)?;
    let root_was_missing = missing.first().is_some_and(|directory| directory == path);
    for directory in missing.iter().rev() {
        let parent = directory_parent(directory).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Raft state directory has no parent",
            )
        })?;
        sync(parent)?;
    }
    if !root_was_missing {
        let parent = directory_parent(path).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Raft state directory has no parent",
            )
        })?;
        sync(parent)?;
    }
    sync(path)
}

fn directory_parent(path: &Path) -> Option<&Path> {
    path.parent().map(|parent| {
        if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        }
    })
}

fn invalid_data(error: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);

    fn test_directory(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "fusiondb-raft-persistence-{name}-{}-{}",
            std::process::id(),
            NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn durable_create_syncs_every_new_directory_entry_in_order() {
        let base = test_directory("directory-sync");
        fs::create_dir_all(&base).unwrap();
        let data_dir = base.join("data");
        let raft_dir = data_dir.join("raft");
        let mut synced = Vec::new();

        create_dir_all_durable_with(&raft_dir, |path| {
            synced.push(path.to_path_buf());
            Ok(())
        })
        .unwrap();

        assert!(raft_dir.is_dir());
        assert_eq!(synced, vec![base.clone(), data_dir, raft_dir]);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn durable_create_retries_parent_sync_after_failure() {
        let base = test_directory("directory-sync-failure");
        fs::create_dir_all(&base).unwrap();
        let raft_dir = base.join("raft");

        let error = create_dir_all_durable_with(&raft_dir, |path| {
            if path == base {
                Err(io::Error::other("injected parent sync failure"))
            } else {
                Ok(())
            }
        })
        .expect_err("parent directory sync failure must fail closed");

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(error.to_string(), "injected parent sync failure");
        assert!(raft_dir.is_dir());

        let mut retry_synced = Vec::new();
        create_dir_all_durable_with(&raft_dir, |path| {
            retry_synced.push(path.to_path_buf());
            Ok(())
        })
        .unwrap();
        assert_eq!(retry_synced, vec![base.clone(), raft_dir]);

        fs::remove_dir_all(base).unwrap();
    }
}
