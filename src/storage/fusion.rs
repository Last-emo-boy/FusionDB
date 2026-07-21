use super::columnar::ColumnarVectorStore;
use super::data_migration::{
    migration_phase_key, DataMigrationFence, DataMigrationPhase, DataMigrationPhaseRecord,
    FenceSnapshot, MAX_SUPPORTED_PHASE,
};
use super::manifest_edit::{
    ManifestEdit, ManifestSstableEntry as ManifestV2SstableEntry,
    ManifestSstableFingerprint as ManifestV2SstableFingerprint,
};
use super::manifest_log;
use super::wal::{WalEntry, WalManager};
use super::{
    hnsw_index_name_for_column, ScanVisitor, SqlBlockZoneMapFailOpenReason,
    SqlBlockZoneMapPruningDecision, SqlBlockZoneMapPruningPlan, Storage, StorageScanOptions,
    Transaction,
};
use crate::catalog::TableSchema;
use crate::common::{FusionError, Result};
use crate::config::StorageConfig;
use async_trait::async_trait;
use base64::Engine as _;
use crossbeam_skiplist::SkipMap;
use serde::{Deserialize, Serialize};
use std::fmt::Write as FmtWrite;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock};
use tokio::sync::{mpsc, Mutex as AsyncMutex, Notify, OwnedRwLockReadGuard, RwLock as AsyncRwLock};

// Fusion Storage Engine
// Combines:
// 1. MVCC (Lock-free reads, Snapshot Isolation)
// 2. LSM-Tree Structure (MemTable -> Flush -> SST)
// 3. Columnar Vector Store (Integrated for Vector Search)

const TS_SIZE: usize = 8;
const COMPACTION_FANIN: usize = 4;
const SSTABLE_BLOCK_BUFFER_CAPACITY: usize = 4096;
const CDC_KEY_PREFIX: &str = "__fusiondb_cdc:";
const CDC_KEY_END: &str = "__fusiondb_cdc;";
const CDC_SEQUENCE_EVENT_BITS: u32 = 20;
const CDC_MAX_EVENT_INDEX: usize = (1usize << CDC_SEQUENCE_EVENT_BITS) - 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SqlBlockZoneMapMvccFailOpenReason {
    BoundarySplit,
    WriteBufferOverlap,
    MemtableOverlap,
    SstableOverlap,
}

fn sstable_read_options(options: &StorageScanOptions) -> SsTableReadOptions {
    if options.fill_cache {
        SsTableReadOptions::fill_cache()
    } else {
        SsTableReadOptions::no_fill_cache()
    }
}

#[cfg(test)]
mod reverse_activation_test_hooks {
    use std::cell::Cell;

    thread_local! {
        static REVERSE_SSTABLE_ACTIVATION_COUNT: Cell<u64> = Cell::new(0);
        static REVERSE_SOURCE_OPEN_COUNT: Cell<u64> = Cell::new(0);
    }

    pub fn reset() {
        REVERSE_SSTABLE_ACTIVATION_COUNT.with(|count| count.set(0));
        REVERSE_SOURCE_OPEN_COUNT.with(|count| count.set(0));
    }

    pub fn inc() {
        REVERSE_SSTABLE_ACTIVATION_COUNT.with(|count| count.set(count.get().saturating_add(1)));
    }

    pub fn inc_source_open() {
        REVERSE_SOURCE_OPEN_COUNT.with(|count| count.set(count.get().saturating_add(1)));
    }

    pub fn get() -> u64 {
        REVERSE_SSTABLE_ACTIVATION_COUNT.with(Cell::get)
    }

    pub fn get_source_open() -> u64 {
        REVERSE_SOURCE_OPEN_COUNT.with(Cell::get)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcBytes {
    pub encoding: String,
    pub data: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CdcOperation {
    Put,
    Delete,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdcEvent {
    pub sequence: u64,
    pub commit_ts: u64,
    pub operation: CdcOperation,
    pub key: CdcBytes,
    pub value: Option<CdcBytes>,
}

impl CdcBytes {
    fn from_raw(bytes: &[u8]) -> Self {
        match std::str::from_utf8(bytes) {
            Ok(text) => Self {
                encoding: "utf8".to_string(),
                data: text.to_string(),
            },
            Err(_) => Self {
                encoding: "base64".to_string(),
                data: base64::engine::general_purpose::STANDARD.encode(bytes),
            },
        }
    }
}

impl CdcEvent {
    fn from_write(sequence: u64, commit_ts: u64, key: &[u8], value: Option<&[u8]>) -> Self {
        Self {
            sequence,
            commit_ts,
            operation: if value.is_some() {
                CdcOperation::Put
            } else {
                CdcOperation::Delete
            },
            key: CdcBytes::from_raw(key),
            value: value.map(CdcBytes::from_raw),
        }
    }
}

fn obsolete_sstable_path_buffer(capacity: usize) -> Vec<PathBuf> {
    Vec::with_capacity(capacity)
}

fn sstable_live_file_buffer() -> Vec<SstableLiveFile> {
    Vec::with_capacity(1)
}

fn sstable_handle_buffer() -> Vec<Arc<SsTable>> {
    Vec::with_capacity(1)
}

fn sstable_file_name_for_id(id: u64) -> String {
    let mut name = String::with_capacity(u64_decimal_len(id) + ".sst".len());
    write!(&mut name, "{id}").expect("writing to String cannot fail");
    name.push_str(".sst");
    name
}

fn u64_decimal_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 10 {
        value /= 10;
        len += 1;
    }
    len
}

fn obsolete_sstable_buffer() -> Vec<Arc<SsTable>> {
    Vec::with_capacity(1)
}

fn block_cache_weight(_key: &BlockCacheKey, value: &BlockCacheValue) -> u32 {
    u32::try_from(value.len()).unwrap_or(u32::MAX)
}

fn build_block_cache(config: &StorageConfig) -> BlockCache {
    BlockCache::builder()
        .max_capacity(config.block_cache_capacity_bytes())
        .weigher(block_cache_weight)
        .eviction_listener(|_key, value, _cause| {
            crate::monitor::inc_block_cache_eviction(value.len() as u64);
        })
        .build()
}

// --- Data Structures ---

use super::fbtree::FBTree;

#[derive(Clone)]
struct MemTable {
    // Key -> (Value, Timestamp)
    // We encode Key+TS in the SkipMap key for MVCC
    map: Arc<SkipMap<Vec<u8>, Vec<u8>>>,
    size: Arc<AtomicU64>,
    id: u64,
    // Optional FB+-Tree index (built when immutable)
    // We use Arc<FBTree> to allow cheap cloning for iterators without holding RwLock
    fbtree: Arc<RwLock<Option<Arc<FBTree>>>>,
}

impl MemTable {
    fn new(id: u64) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            size: Arc::new(AtomicU64::new(0)),
            id,
            fbtree: Arc::new(RwLock::new(None)),
        }
    }

    fn insert(&self, key: Vec<u8>, value: Vec<u8>) {
        let len = key.len() + value.len();
        self.map.insert(key, value);
        self.size.fetch_add(len as u64, Ordering::Relaxed);
    }

    fn build_fbtree(&self) {
        // Build FBTree from SkipMap
        // This is called when MemTable becomes immutable
        // Use bulk_load if possible, but we need sorted iterator. SkipMap is sorted!

        let iter = self
            .map
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()));
        let tree = FBTree::bulk_load(iter);

        let mut guard = self.fbtree.write().unwrap();
        *guard = Some(Arc::new(tree));
        // println!("Built FBTree for MemTable {}", self.id);
    }
}

fn immutable_memtable_buffer() -> Vec<MemTable> {
    Vec::with_capacity(1)
}

fn transaction_write_buffer() -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    Vec::with_capacity(1)
}

fn cdc_key_for_sequence(sequence: u64) -> String {
    let mut key = String::with_capacity(CDC_KEY_PREFIX.len() + 20);
    key.push_str(CDC_KEY_PREFIX);
    write!(&mut key, "{sequence:020}").expect("writing to String cannot fail");
    key
}

fn cdc_sequence_for(commit_ts: u64, event_index: usize) -> Result<u64> {
    if event_index > CDC_MAX_EVENT_INDEX {
        return Err(FusionError::Storage(format!(
            "CDC transaction event limit exceeded: {} > {}",
            event_index, CDC_MAX_EVENT_INDEX
        )));
    }
    if commit_ts > (u64::MAX >> CDC_SEQUENCE_EVENT_BITS) {
        return Err(FusionError::Storage(format!(
            "CDC sequence overflow for commit timestamp {}",
            commit_ts
        )));
    }
    Ok((commit_ts << CDC_SEQUENCE_EVENT_BITS) | event_index as u64)
}

fn cdc_should_capture_key(key: &[u8]) -> bool {
    !key.starts_with(CDC_KEY_PREFIX.as_bytes())
        && crate::storage::keyspace::parse_data_key_exact(key).is_err()
}

fn encode_cdc_event(event: &CdcEvent) -> Result<Vec<u8>> {
    bincode::serialize(event)
        .map_err(|error| FusionError::Storage(format!("CDC event encode error: {}", error)))
}

fn decode_cdc_event(bytes: &[u8]) -> Result<CdcEvent> {
    bincode::deserialize(bytes)
        .map_err(|error| FusionError::Storage(format!("CDC event decode error: {}", error)))
}

#[cfg(test)]
fn vector_rebuild_data_prefix_for_table(table_name: &str) -> String {
    let mut prefix = String::with_capacity("data:".len() + table_name.len() + 1);
    prefix.push_str("data:");
    prefix.push_str(table_name);
    prefix.push(':');
    prefix
}

fn side_index_data_key_payload(key: &[u8]) -> Result<Option<&[u8]>> {
    if let Some(payload) = key.strip_prefix(b"data:") {
        return Ok(Some(payload));
    }
    let Some(rest) = key.strip_prefix(b"shard:") else {
        return Ok(None);
    };
    let shard_end = rest.iter().position(|byte| *byte == b':').ok_or_else(|| {
        FusionError::Storage(format!(
            "malformed sharded storage key '{}'",
            String::from_utf8_lossy(key)
        ))
    })?;
    let shard_id = std::str::from_utf8(&rest[..shard_end]).map_err(|error| {
        FusionError::Storage(format!(
            "sharded storage key has non-UTF-8 shard id: {error}"
        ))
    })?;
    shard_id.parse::<u64>().map_err(|error| {
        FusionError::Storage(format!("invalid shard id in storage key: {error}"))
    })?;
    Ok(rest[shard_end + 1..].strip_prefix(b"data:"))
}

struct SideIndexRebuildPlan {
    table_name: String,
    trigram_columns: Vec<(usize, String)>,
    hnsw_columns: Vec<(usize, String)>,
}

use crate::storage::inverted_index::InvertedIndex;
use crate::storage::sstable::{
    BlockCache, BlockCacheKey, BlockCacheValue, SsTable, SsTableBlockProperties, SsTableBuilder,
    SsTableOpenDescriptor, SsTablePrefixFilterProbe, SsTableReadOptions,
    SsTableReverseFrontierKind, SsTableReverseIterator,
};
use crate::storage::vector_index::VectorIndex;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap};
use std::io::{Error as IoError, ErrorKind};
use std::path::{Path, PathBuf};
use std::time::{Instant, UNIX_EPOCH};

const SSTABLE_TIMESTAMP_CACHE_VERSION: u32 = 1;
const SSTABLE_TIMESTAMP_CACHE_FILE: &str = "_fusiondb_sstable_ts_cache.json";
const SSTABLE_DESCRIPTOR_CACHE_VERSION: u32 = 1;
const SSTABLE_DESCRIPTOR_CACHE_FILE: &str = "_fusiondb_sstable_descriptor_cache.json";
const SSTABLE_MANIFEST_VERSION: u32 = 1;
const SSTABLE_MANIFEST_CURRENT_FILE: &str = "CURRENT";
const SSTABLE_MANIFEST_FILE: &str = "MANIFEST-000001";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct SstableTimestampFingerprint {
    file_len: u64,
    modified_unix_secs: u64,
    modified_subsec_nanos: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SstableTimestampCacheEntry {
    fingerprint: SstableTimestampFingerprint,
    max_ts: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct SstableTimestampCache {
    version: u32,
    entries: BTreeMap<u64, SstableTimestampCacheEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SstableDescriptorCacheEntry {
    fingerprint: SstableTimestampFingerprint,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    format_version: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct SstableDescriptorCache {
    version: u32,
    entries: BTreeMap<u64, SstableDescriptorCacheEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct SstableManifestEntry {
    id: u64,
    file_name: String,
    fingerprint: SstableTimestampFingerprint,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct SstableManifest {
    version: u32,
    files: Vec<SstableManifestEntry>,
}

struct SstableLiveFile {
    id: u64,
    path: PathBuf,
    descriptor: Option<SsTableOpenDescriptor>,
}

impl Default for SstableTimestampCache {
    fn default() -> Self {
        Self {
            version: SSTABLE_TIMESTAMP_CACHE_VERSION,
            entries: BTreeMap::new(),
        }
    }
}

impl SstableTimestampCache {
    fn load(path: &Path) -> Self {
        let Ok(bytes) = std::fs::read(path) else {
            return Self::default();
        };
        let Ok(cache) = serde_json::from_slice::<Self>(&bytes) else {
            return Self::default();
        };
        if cache.version == SSTABLE_TIMESTAMP_CACHE_VERSION {
            cache
        } else {
            Self::default()
        }
    }

    fn max_ts_for(
        &self,
        sstable_id: u64,
        fingerprint: &SstableTimestampFingerprint,
    ) -> Option<u64> {
        self.entries.get(&sstable_id).and_then(|entry| {
            if &entry.fingerprint == fingerprint {
                Some(entry.max_ts)
            } else {
                None
            }
        })
    }

    fn set(&mut self, sstable_id: u64, fingerprint: SstableTimestampFingerprint, max_ts: u64) {
        self.entries.insert(
            sstable_id,
            SstableTimestampCacheEntry {
                fingerprint,
                max_ts,
            },
        );
    }

    fn retain_live_sstables(&mut self, sstables: &[Arc<SsTable>]) -> bool {
        let before = self.entries.len();
        self.entries
            .retain(|id, _| sstables.iter().any(|sstable| sstable.id == *id));
        self.entries.len() != before
    }

    fn persist(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(SSTABLE_TIMESTAMP_CACHE_FILE);
        let tmp_path = path.with_file_name(format!("{file_name}.tmp"));
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        std::fs::write(&tmp_path, bytes)?;
        std::fs::rename(tmp_path, path)
    }
}

impl Default for SstableDescriptorCache {
    fn default() -> Self {
        Self {
            version: SSTABLE_DESCRIPTOR_CACHE_VERSION,
            entries: BTreeMap::new(),
        }
    }
}

impl SstableDescriptorCache {
    fn load(path: &Path) -> Self {
        let Ok(bytes) = std::fs::read(path) else {
            return Self::default();
        };
        let Ok(cache) = serde_json::from_slice::<Self>(&bytes) else {
            return Self::default();
        };
        if cache.version == SSTABLE_DESCRIPTOR_CACHE_VERSION {
            cache
        } else {
            Self::default()
        }
    }

    fn descriptor_for(
        &self,
        sstable_id: u64,
        fingerprint: &SstableTimestampFingerprint,
    ) -> Option<SsTableOpenDescriptor> {
        self.entries.get(&sstable_id).and_then(|entry| {
            if &entry.fingerprint == fingerprint {
                Some(SsTableOpenDescriptor {
                    first_key: entry.first_key.clone(),
                    last_key: entry.last_key.clone(),
                    format_version: entry.format_version,
                })
            } else {
                None
            }
        })
    }

    fn set_from_sstable(
        &mut self,
        sstable_id: u64,
        fingerprint: SstableTimestampFingerprint,
        sstable: &SsTable,
    ) {
        self.entries.insert(
            sstable_id,
            SstableDescriptorCacheEntry {
                fingerprint,
                first_key: sstable.meta.first_key.clone(),
                last_key: sstable.meta.last_key.clone(),
                format_version: sstable.meta.format_version,
            },
        );
    }

    fn retain_live_sstables(&mut self, sstables: &[Arc<SsTable>]) -> bool {
        let before = self.entries.len();
        self.entries
            .retain(|id, _| sstables.iter().any(|sstable| sstable.id == *id));
        self.entries.len() != before
    }

    fn persist(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(SSTABLE_DESCRIPTOR_CACHE_FILE);
        let tmp_path = path.with_file_name(format!("{file_name}.tmp"));
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        std::fs::write(&tmp_path, bytes)?;
        std::fs::rename(tmp_path, path)
    }
}

impl SstableManifest {
    #[cfg(test)]
    fn from_sstables(sstables: &[Arc<SsTable>]) -> std::io::Result<Self> {
        let mut files = Vec::with_capacity(sstables.len());
        for sstable in sstables {
            let fingerprint = sstable_timestamp_fingerprint(&sstable.path).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!(
                        "live SSTable {} is missing or has invalid metadata",
                        sstable.path.display()
                    ),
                )
            })?;
            files.push(SstableManifestEntry {
                id: sstable.id,
                file_name: sstable_file_name_for_id(sstable.id),
                fingerprint,
            });
        }
        files.sort_by_key(|entry| entry.id);
        Ok(Self {
            version: SSTABLE_MANIFEST_VERSION,
            files,
        })
    }

    fn load_live_files(sstable_dir: &Path) -> Option<Vec<SstableLiveFile>> {
        Self::load_live_files_v2(sstable_dir)
            .or_else(|| Self::load_live_files_legacy_json(sstable_dir))
    }

    fn load_live_files_v2(sstable_dir: &Path) -> Option<Vec<SstableLiveFile>> {
        let replay = manifest_log::recover_current_manifest_with_rollover(sstable_dir).ok()?;
        let mut files = Vec::with_capacity(replay.edit_replay.state.files.len());
        for entry in replay.edit_replay.state.files.values() {
            let path = sstable_dir.join(&entry.file_name);
            let fingerprint = sstable_timestamp_fingerprint(&path)?;
            if fingerprint.file_len != entry.fingerprint.file_len
                || fingerprint.modified_unix_secs != entry.fingerprint.modified_unix_secs
                || fingerprint.modified_subsec_nanos != entry.fingerprint.modified_subsec_nanos
            {
                return None;
            }
            files.push(SstableLiveFile {
                id: entry.id,
                path,
                descriptor: Some(SsTableOpenDescriptor {
                    first_key: entry.first_key.clone(),
                    last_key: entry.last_key.clone(),
                    format_version: entry.format_version,
                }),
            });
        }
        Some(files)
    }

    fn load_live_files_legacy_json(sstable_dir: &Path) -> Option<Vec<SstableLiveFile>> {
        let current_path = sstable_manifest_current_path(sstable_dir);
        let manifest_name = std::fs::read_to_string(current_path).ok()?;
        let manifest_name = manifest_name.trim();
        if manifest_name != SSTABLE_MANIFEST_FILE {
            return None;
        }

        let manifest_path = sstable_dir.join(manifest_name);
        let bytes = std::fs::read(manifest_path).ok()?;
        let manifest = serde_json::from_slice::<Self>(&bytes).ok()?;
        if manifest.version != SSTABLE_MANIFEST_VERSION {
            return None;
        }

        let mut files = Vec::with_capacity(manifest.files.len());
        let mut previous_id = None;
        for entry in manifest.files {
            if entry.file_name != sstable_file_name_for_id(entry.id) {
                return None;
            }
            if previous_id.is_some_and(|id| id >= entry.id) {
                return None;
            }
            previous_id = Some(entry.id);

            let path = sstable_dir.join(&entry.file_name);
            let fingerprint = sstable_timestamp_fingerprint(&path)?;
            if fingerprint != entry.fingerprint {
                return None;
            }
            files.push(SstableLiveFile {
                id: entry.id,
                path,
                descriptor: None,
            });
        }
        Some(files)
    }

    fn persist_sstables(sstable_dir: &Path, sstables: &[Arc<SsTable>]) -> std::io::Result<()> {
        let files = Self::v2_entries_from_sstables(sstable_dir, sstables)?;
        if !sstable_manifest_current_path(sstable_dir).exists() {
            return Self::write_v2_snapshot(sstable_dir, files);
        }

        match manifest_log::recover_current_manifest_with_rollover(sstable_dir) {
            Ok(replay) => Self::append_v2_version_edit(sstable_dir, replay, files),
            Err(_) if Self::load_live_files_legacy_json(sstable_dir).is_some() => {
                Self::write_v2_snapshot(sstable_dir, files)
            }
            Err(error) => Err(fusion_error_to_io(error)),
        }
    }

    fn write_v2_snapshot(
        sstable_dir: &Path,
        files: Vec<ManifestV2SstableEntry>,
    ) -> std::io::Result<()> {
        let snapshot = Self::v2_snapshot_from_entries(files);
        let file_number = next_sstable_manifest_file_number(sstable_dir)?;
        manifest_log::write_manifest_file(sstable_dir, file_number, &snapshot)
            .map_err(fusion_error_to_io)?;
        manifest_log::install_current_file(
            sstable_dir,
            &manifest_log::manifest_file_name(file_number),
        )
        .map_err(fusion_error_to_io)?;
        Ok(())
    }

    fn append_v2_version_edit(
        _sstable_dir: &Path,
        replay: manifest_log::ManifestLogReplay,
        files: Vec<ManifestV2SstableEntry>,
    ) -> std::io::Result<()> {
        let current = replay.current.ok_or_else(|| {
            IoError::new(ErrorKind::InvalidData, "manifest replay missing CURRENT")
        })?;
        let mut next_files = BTreeMap::new();
        for mut entry in files {
            if let Some(existing) = replay.edit_replay.state.files.get(&entry.id) {
                if manifest_entries_share_stable_descriptor(existing, &entry)
                    && entry.max_ts == 0
                    && existing.max_ts > 0
                {
                    entry.max_ts = existing.max_ts;
                    entry.content_fingerprint = existing.content_fingerprint;
                }
            }
            next_files.insert(entry.id, entry);
        }

        let mut delete_ids = Vec::new();
        let mut add_files = Vec::new();
        for (id, current_entry) in &replay.edit_replay.state.files {
            match next_files.get(id) {
                Some(next_entry) if next_entry == current_entry => {}
                Some(next_entry) => {
                    delete_ids.push(*id);
                    add_files.push(next_entry.clone());
                }
                None => delete_ids.push(*id),
            }
        }
        for (id, next_entry) in &next_files {
            if !replay.edit_replay.state.files.contains_key(id) {
                add_files.push(next_entry.clone());
            }
        }

        let next_file_number = replay
            .edit_replay
            .state
            .next_file_number
            .max(min_next_file_number_for_entries(next_files.values()));
        let high_watermark = replay
            .edit_replay
            .state
            .high_watermark
            .max(max_ts_for_entries(next_files.values()));
        if delete_ids.is_empty()
            && add_files.is_empty()
            && next_file_number == replay.edit_replay.state.next_file_number
            && high_watermark == replay.edit_replay.state.high_watermark
        {
            return Ok(());
        }

        let edit = ManifestEdit::VersionEdit {
            delete_ids,
            add_files,
            next_file_number: Some(next_file_number),
            high_watermark: Some(high_watermark),
            wal_replay_floor: None,
        };
        manifest_log::append_manifest_edit_file(&current.path, &edit)
            .map_err(fusion_error_to_io)?;
        Ok(())
    }

    fn v2_entries_from_sstables(
        sstable_dir: &Path,
        sstables: &[Arc<SsTable>],
    ) -> std::io::Result<Vec<ManifestV2SstableEntry>> {
        let timestamp_cache =
            SstableTimestampCache::load(&sstable_timestamp_cache_path(sstable_dir));
        let mut files = Vec::with_capacity(sstables.len());
        for sstable in sstables {
            let fingerprint = sstable_timestamp_fingerprint(&sstable.path).ok_or_else(|| {
                IoError::new(
                    ErrorKind::NotFound,
                    format!(
                        "live SSTable {} is missing or has invalid metadata",
                        sstable.path.display()
                    ),
                )
            })?;
            let max_ts = timestamp_cache
                .max_ts_for(sstable.id, &fingerprint)
                .unwrap_or(0);
            files.push(ManifestV2SstableEntry {
                id: sstable.id,
                file_name: sstable_file_name_for_id(sstable.id),
                fingerprint: ManifestV2SstableFingerprint {
                    file_len: fingerprint.file_len,
                    modified_unix_secs: fingerprint.modified_unix_secs,
                    modified_subsec_nanos: fingerprint.modified_subsec_nanos,
                },
                first_key: sstable.meta.first_key.clone(),
                last_key: sstable.meta.last_key.clone(),
                format_version: sstable.meta.format_version,
                max_ts,
                content_fingerprint: sstable_manifest_content_fingerprint(&fingerprint, max_ts),
            });
        }
        files.sort_by_key(|entry| entry.id);
        Ok(files)
    }

    fn v2_snapshot_from_entries(files: Vec<ManifestV2SstableEntry>) -> ManifestEdit {
        let next_file_number = min_next_file_number_for_entries(files.iter());
        let high_watermark = max_ts_for_entries(files.iter());
        ManifestEdit::Snapshot {
            files,
            next_file_number,
            high_watermark,
            wal_replay_floor: None,
        }
    }
}

fn sstable_timestamp_cache_path(sstable_dir: &Path) -> PathBuf {
    sstable_dir.join(SSTABLE_TIMESTAMP_CACHE_FILE)
}

fn sstable_descriptor_cache_path(sstable_dir: &Path) -> PathBuf {
    sstable_dir.join(SSTABLE_DESCRIPTOR_CACHE_FILE)
}

fn sstable_manifest_current_path(sstable_dir: &Path) -> PathBuf {
    sstable_dir.join(SSTABLE_MANIFEST_CURRENT_FILE)
}

fn sstable_manifest_path(sstable_dir: &Path) -> PathBuf {
    sstable_dir.join(SSTABLE_MANIFEST_FILE)
}

fn next_sstable_manifest_file_number(sstable_dir: &Path) -> std::io::Result<u64> {
    let mut next = manifest_log::read_current_file(sstable_dir)
        .ok()
        .and_then(|current| current.file_number.checked_add(1))
        .unwrap_or(1);

    if let Ok(entries) = std::fs::read_dir(sstable_dir) {
        for entry in entries.flatten() {
            let Some(file_name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            let Some(file_number) = manifest_log::parse_manifest_file_name(&file_name) else {
                continue;
            };
            let candidate = file_number.checked_add(1).ok_or_else(|| {
                IoError::new(ErrorKind::InvalidData, "manifest file number overflow")
            })?;
            next = next.max(candidate);
        }
    }

    loop {
        let file_name = manifest_log::manifest_file_name(next);
        if !sstable_dir.join(file_name).exists() {
            return Ok(next);
        }
        next = next
            .checked_add(1)
            .ok_or_else(|| IoError::new(ErrorKind::InvalidData, "manifest file number overflow"))?;
    }
}

fn sstable_timestamp_fingerprint(path: &Path) -> Option<SstableTimestampFingerprint> {
    let metadata = std::fs::metadata(path).ok()?;
    let modified = metadata.modified().ok()?.duration_since(UNIX_EPOCH).ok()?;
    Some(SstableTimestampFingerprint {
        file_len: metadata.len(),
        modified_unix_secs: modified.as_secs(),
        modified_subsec_nanos: modified.subsec_nanos(),
    })
}

fn sstable_manifest_content_fingerprint(
    fingerprint: &SstableTimestampFingerprint,
    max_ts: u64,
) -> u64 {
    fingerprint.file_len
        ^ fingerprint.modified_unix_secs.rotate_left(13)
        ^ u64::from(fingerprint.modified_subsec_nanos).rotate_left(29)
        ^ max_ts.rotate_left(41)
}

fn min_next_file_number_for_entries<'a>(
    entries: impl Iterator<Item = &'a ManifestV2SstableEntry>,
) -> u64 {
    entries
        .map(|entry| entry.id.saturating_add(1))
        .max()
        .unwrap_or(1)
}

fn max_ts_for_entries<'a>(entries: impl Iterator<Item = &'a ManifestV2SstableEntry>) -> u64 {
    entries.map(|entry| entry.max_ts).max().unwrap_or(0)
}

fn manifest_entries_share_stable_descriptor(
    left: &ManifestV2SstableEntry,
    right: &ManifestV2SstableEntry,
) -> bool {
    left.file_name == right.file_name
        && left.fingerprint.file_len == right.fingerprint.file_len
        && left.fingerprint.modified_unix_secs == right.fingerprint.modified_unix_secs
        && left.fingerprint.modified_subsec_nanos == right.fingerprint.modified_subsec_nanos
        && left.first_key == right.first_key
        && left.last_key == right.last_key
        && left.format_version == right.format_version
}

fn fusion_error_to_io(error: FusionError) -> IoError {
    IoError::new(ErrorKind::InvalidData, error.to_string())
}

fn sync_directory(path: &Path) -> std::io::Result<()> {
    std::fs::File::open(path)?.sync_all()
}

fn scan_sstable_files(sst_dir: &Path) -> Vec<SstableLiveFile> {
    let mut files = sstable_live_file_buffer();
    let Ok(mut entries) = std::fs::read_dir(sst_dir) else {
        return files;
    };
    while let Some(Ok(entry)) = entries.next() {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "sst") {
            if let Some(stem) = path.file_stem() {
                if let Ok(id) = stem.to_string_lossy().parse::<u64>() {
                    files.push(SstableLiveFile {
                        id,
                        path,
                        descriptor: None,
                    });
                }
            }
        }
    }
    files.sort_by_key(|entry| entry.id);
    files
}

/// Heap item for the compaction merge. All compaction sources are SSTables,
/// so entries stay as borrowed views into their block (one `Arc` bump per
/// entry instead of two heap copies); bytes are copied only when the builder
/// writes its own block buffer. Ordering is the canonical merge order:
/// reverse key compare for a min-heap, no source tie-break.
///
/// Memory boundedness: the heap holds at most one view per source and each
/// iterator holds its current block, so at most 2 block `Arc`s are alive per
/// input SSTable (<= 2 x COMPACTION_FANIN in total); advancing a source
/// drops its previous view's block reference.
struct MergeItem {
    entry: crate::storage::sstable::SsTableEntryView,
    iter_idx: usize,
}

impl PartialEq for MergeItem {
    fn eq(&self, other: &Self) -> bool {
        self.entry.key() == other.entry.key()
    }
}

impl Eq for MergeItem {}

impl PartialOrd for MergeItem {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeItem {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // Reverse order for Min-Heap: we want smallest key to pop first
        other.entry.key().cmp(self.entry.key())
    }
}

/// Heap item for the forward visible-range merge. SSTable entries stay as
/// borrowed views into their cached blocks (one Arc bump per entry) and are
/// materialized only at the visitor boundary; write-buffer and memtable
/// entries are already owned. Ordering replicates `MergeItem` exactly:
/// reverse key compare for a min-heap, no source tie-break.
enum VisibleEntry {
    Owned(Vec<u8>, Vec<u8>),
    Sst(crate::storage::sstable::SsTableEntryView),
}

impl VisibleEntry {
    fn key(&self) -> &[u8] {
        match self {
            Self::Owned(key, _) => key,
            Self::Sst(view) => view.key(),
        }
    }

    fn val(&self) -> &[u8] {
        match self {
            Self::Owned(_, val) => val,
            Self::Sst(view) => view.value(),
        }
    }
}

struct VisibleMergeItem {
    entry: VisibleEntry,
    iter_idx: usize,
}

impl PartialEq for VisibleMergeItem {
    fn eq(&self, other: &Self) -> bool {
        self.entry.key() == other.entry.key()
    }
}

impl Eq for VisibleMergeItem {}

impl PartialOrd for VisibleMergeItem {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for VisibleMergeItem {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // Reverse order for Min-Heap: we want smallest key to pop first
        other.entry.key().cmp(self.entry.key())
    }
}

fn merge_heap(capacity: usize) -> BinaryHeap<MergeItem> {
    BinaryHeap::with_capacity(capacity)
}

struct ReverseMergeItem {
    user_key: Vec<u8>,
    source_idx: usize,
}

impl PartialEq for ReverseMergeItem {
    fn eq(&self, other: &Self) -> bool {
        self.user_key == other.user_key && self.source_idx == other.source_idx
    }
}

impl Eq for ReverseMergeItem {}

impl PartialOrd for ReverseMergeItem {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReverseMergeItem {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.source_idx.cmp(&self.source_idx))
    }
}

/// Raw entry flowing out of a reverse merge source. SSTable entries stay
/// as borrowed views into their cached blocks (one Arc bump per entry);
/// buffered (write-buffer / memtable) entries are already owned. Accessors
/// return the encoded internal key and encoded value either way.
enum ReverseRawEntry {
    Owned(Vec<u8>, Vec<u8>),
    Sst(crate::storage::sstable::SsTableEntryView),
}

impl ReverseRawEntry {
    fn key(&self) -> &[u8] {
        match self {
            Self::Owned(key, _) => key,
            Self::Sst(view) => view.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self {
            Self::Owned(_, value) => value,
            Self::Sst(view) => view.value(),
        }
    }
}

/// One visible version for a source's current user-key group. Holds the
/// raw entry (SSTable versions stay as block views) and decodes user_key /
/// value on demand; `wins_over` needs only the metadata, and the visitor
/// receives borrows, so winner bytes are never copied inside the merge.
/// Memory boundedness: the merge keeps at most one candidate (`current`)
/// plus one stashed raw entry per source, so with the iterator's own
/// current block each SSTable source pins at most three block Arcs.
struct ReverseCandidate {
    raw: ReverseRawEntry,
    ts: u64,
    is_put: bool,
    is_write_buffer: bool,
    source_order: usize,
}

impl ReverseCandidate {
    fn user_key(&self) -> &[u8] {
        FusionStorage::decode_key(self.raw.key()).0
    }

    fn value(&self) -> &[u8] {
        FusionStorage::decode_value(self.raw.value()).1
    }

    fn wins_over(&self, other: &Self) -> bool {
        if self.is_write_buffer != other.is_write_buffer {
            return self.is_write_buffer;
        }
        self.ts > other.ts || (self.ts == other.ts && self.source_order < other.source_order)
    }
}

enum ReverseSource<'a> {
    Buffered {
        entries: Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send + 'a>,
        pending: Option<ReverseRawEntry>,
        is_write_buffer: bool,
        source_order: usize,
    },
    SsTable {
        iter: SsTableReverseIterator,
        pending: Option<ReverseRawEntry>,
        source_order: usize,
    },
}

impl<'a> ReverseSource<'a> {
    async fn next_raw(&mut self) -> Result<Option<ReverseRawEntry>> {
        match self {
            ReverseSource::Buffered {
                entries, pending, ..
            } => {
                if pending.is_some() {
                    Ok(pending.take())
                } else {
                    let next = entries.next();
                    if next.is_some() {
                        crate::monitor::inc_fusion_reverse_raw_entry_read();
                    }
                    Ok(next.map(|(key, value)| ReverseRawEntry::Owned(key, value)))
                }
            }
            ReverseSource::SsTable { iter, pending, .. } => {
                if pending.is_some() {
                    Ok(pending.take())
                } else {
                    let next = iter.next_entry().await?;
                    if next.is_some() {
                        crate::monitor::inc_fusion_reverse_raw_entry_read();
                    }
                    Ok(next.map(ReverseRawEntry::Sst))
                }
            }
        }
    }

    fn stash_raw(&mut self, raw: ReverseRawEntry) {
        match self {
            ReverseSource::Buffered { pending, .. } | ReverseSource::SsTable { pending, .. } => {
                debug_assert!(pending.is_none());
                *pending = Some(raw);
            }
        }
    }

    fn is_write_buffer(&self) -> bool {
        matches!(
            self,
            ReverseSource::Buffered {
                is_write_buffer: true,
                ..
            }
        )
    }

    fn source_order(&self) -> usize {
        match self {
            ReverseSource::Buffered { source_order, .. }
            | ReverseSource::SsTable { source_order, .. } => *source_order,
        }
    }

    async fn next_candidate(&mut self, read_ts: u64) -> Result<Option<ReverseCandidate>> {
        loop {
            let Some(first_raw) = self.next_raw().await? else {
                return Ok(None);
            };
            let (first_user_key, first_ts) = FusionStorage::decode_key(first_raw.key());
            // One owned copy per user-key group anchors the group boundary
            // while the raw entries themselves move into candidates.
            let group_user_key = first_user_key.to_vec();
            let is_write_buffer = self.is_write_buffer();
            let source_order = self.source_order();
            let mut best = visible_reverse_candidate(
                first_raw,
                first_ts,
                is_write_buffer,
                source_order,
                read_ts,
            );

            loop {
                let Some(next_raw) = self.next_raw().await? else {
                    break;
                };
                let (next_user_key, next_ts) = FusionStorage::decode_key(next_raw.key());
                if next_user_key != group_user_key.as_slice() {
                    self.stash_raw(next_raw);
                    break;
                }

                if let Some(candidate) = visible_reverse_candidate(
                    next_raw,
                    next_ts,
                    is_write_buffer,
                    source_order,
                    read_ts,
                ) {
                    if best
                        .as_ref()
                        .map_or(true, |current| candidate.wins_over(current))
                    {
                        best = Some(candidate);
                    }
                }
            }

            if best.is_some() {
                crate::monitor::inc_fusion_reverse_visible_candidate();
                return Ok(best);
            }
        }
    }
}

fn visible_reverse_candidate(
    raw: ReverseRawEntry,
    ts: u64,
    is_write_buffer: bool,
    source_order: usize,
    read_ts: u64,
) -> Option<ReverseCandidate> {
    if !is_write_buffer && ts > read_ts {
        return None;
    }
    let (is_put, _) = FusionStorage::decode_value(raw.value());
    Some(ReverseCandidate {
        raw,
        ts,
        is_put,
        is_write_buffer,
        source_order,
    })
}

async fn add_reverse_source<'a>(
    sources: &mut Vec<ReverseSource<'a>>,
    current: &mut Vec<Option<ReverseCandidate>>,
    heap: &mut BinaryHeap<ReverseMergeItem>,
    read_ts: u64,
    mut source: ReverseSource<'a>,
) -> Result<()> {
    let source_idx = sources.len();
    crate::monitor::inc_fusion_reverse_source_open();
    #[cfg(test)]
    reverse_activation_test_hooks::inc_source_open();
    let candidate = source.next_candidate(read_ts).await?;
    sources.push(source);
    if let Some(candidate) = candidate {
        heap.push(ReverseMergeItem {
            user_key: candidate.user_key().to_vec(),
            source_idx,
        });
        current.push(Some(candidate));
    } else {
        current.push(None);
    }
    Ok(())
}

struct PendingReverseSstable {
    frontier_user_key: Vec<u8>,
    source_order: usize,
    sst: Arc<SsTable>,
}

impl PartialEq for PendingReverseSstable {
    fn eq(&self, other: &Self) -> bool {
        self.frontier_user_key == other.frontier_user_key && self.source_order == other.source_order
    }
}

impl Eq for PendingReverseSstable {}

impl PartialOrd for PendingReverseSstable {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingReverseSstable {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.frontier_user_key
            .cmp(&other.frontier_user_key)
            .then_with(|| other.source_order.cmp(&self.source_order))
    }
}

async fn activate_pending_reverse_sstables<'a>(
    pending_sstables: &mut BinaryHeap<PendingReverseSstable>,
    sources: &mut Vec<ReverseSource<'a>>,
    current: &mut Vec<Option<ReverseCandidate>>,
    heap: &mut BinaryHeap<ReverseMergeItem>,
    read_ts: u64,
    start: &[u8],
    end: &[u8],
    read_options: SsTableReadOptions,
) -> Result<()> {
    loop {
        let activation_reason_equal_frontier = match (heap.peek(), pending_sstables.peek()) {
            (Some(active_top), Some(pending_top)) => {
                pending_top.frontier_user_key.as_slice() == active_top.user_key.as_slice()
            }
            _ => false,
        };
        let should_activate = match (heap.peek(), pending_sstables.peek()) {
            (_, None) => false,
            (None, Some(_)) => true,
            (Some(active_top), Some(pending_top)) => {
                pending_top.frontier_user_key.as_slice() >= active_top.user_key.as_slice()
            }
        };
        if !should_activate {
            break;
        }

        let pending = pending_sstables
            .pop()
            .expect("peeked pending SSTable exists");
        let iter = pending
            .sst
            .new_user_key_range_reverse_iterator_with_options(
                Some(start),
                Some(end),
                TS_SIZE,
                read_options,
            )
            .await?;
        crate::monitor::inc_sstable_iterator_open();
        crate::monitor::inc_sstable_reverse_iterator_open();
        crate::monitor::inc_fusion_reverse_sstable_activation();
        if activation_reason_equal_frontier {
            crate::monitor::inc_fusion_reverse_sstable_activation_equal_frontier();
        }
        #[cfg(test)]
        reverse_activation_test_hooks::inc();
        add_reverse_source(
            sources,
            current,
            heap,
            read_ts,
            ReverseSource::SsTable {
                iter,
                pending: None,
                source_order: pending.source_order,
            },
        )
        .await?;
    }
    Ok(())
}

#[derive(Clone)]
struct FusionStoragePaths {
    sstable_dir: PathBuf,
    inverted_index_path: PathBuf,
    trigram_index_path: PathBuf,
}

#[derive(Clone)]
pub struct FusionStorage {
    active_memtable: Arc<RwLock<MemTable>>,
    immutable_memtables: Arc<RwLock<Vec<MemTable>>>,
    sstables: Arc<RwLock<Vec<Arc<SsTable>>>>,
    obsolete_sstables: Arc<RwLock<Vec<Arc<SsTable>>>>,
    wal: Arc<WalManager>,

    // Global Clock for MVCC
    current_ts: Arc<AtomicU64>,
    active_read_timestamps: Arc<StdMutex<BTreeMap<u64, usize>>>,
    side_index_visibility: Arc<AsyncRwLock<()>>,

    // ID Generator for MemTables
    next_memtable_id: Arc<AtomicU64>,
    flush_notify: Arc<Notify>,

    // Columnar Store (In-Memory for now)
    // We wrap it in RwLock because we update it in batches.
    // In a real LSM, we would merge this into SSTables.
    columnar_store: Arc<RwLock<Option<ColumnarVectorStore>>>,

    // Inverted Index (In-Memory for now)
    inverted_index: Arc<RwLock<InvertedIndex>>,

    // HNSW Index (Restored)
    pub vector_index: Arc<VectorIndex>,

    // Trigram Index (In-Memory Prototype)
    pub trigram_index: Arc<RwLock<crate::storage::trigram::TrigramIndex>>,

    // Block Cache for SSTables (SST ID, Offset) -> shared block data
    block_cache: Arc<BlockCache>,
    memtable_threshold: usize,
    commit_lock: Arc<AsyncMutex<()>>,
    flush_lock: Arc<AsyncMutex<()>>,
    compaction_lock: Arc<AsyncMutex<()>>,
    data_migration_fence: Arc<DataMigrationFence>,
    paths: Arc<FusionStoragePaths>,
}

impl FusionStorage {
    pub async fn new(path: &str) -> Result<Self> {
        let wal_path = Path::new(path);
        let data_dir = wal_path.parent().unwrap_or_else(|| Path::new("."));
        let storage_config = StorageConfig {
            data_dir: data_dir.to_string_lossy().to_string(),
            wal_file: wal_path
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                .unwrap_or_else(|| "fusion.wal".to_string()),
            ..StorageConfig::default()
        };
        Self::with_config(path, &storage_config).await
    }

    pub async fn with_config(path: &str, config: &StorageConfig) -> Result<Self> {
        let wal = WalManager::new(path)?;
        let sstable_dir = config.sstable_path();
        let inverted_index_path = config.inverted_index_path();
        let trigram_index_path = config.trigram_index_path();
        std::fs::create_dir_all(&config.data_dir).ok();
        std::fs::create_dir_all(&sstable_dir).ok();

        let block_cache = Arc::new(build_block_cache(config));

        // Load existing SSTables
        let mut sstables_vec = sstable_handle_buffer();
        let sst_dir = sstable_dir.as_path();
        if sst_dir.exists() {
            let manifest_current_path = sstable_manifest_current_path(sst_dir);
            let manifest_present = manifest_current_path.exists();
            let files = if manifest_present {
                let manifest_load_start = Instant::now();
                match SstableManifest::load_live_files(sst_dir) {
                    Some(files) => {
                        let manifest_load_us =
                            u64::try_from(manifest_load_start.elapsed().as_micros())
                                .unwrap_or(u64::MAX);
                        crate::monitor::record_sstable_manifest_load(
                            manifest_load_us,
                            files.len() as u64,
                        );
                        files
                    }
                    None => {
                        crate::monitor::inc_sstable_manifest_load_error();
                        return Err(FusionError::Storage(format!(
                            "SSTable manifest is corrupt or references stale files: {}",
                            manifest_current_path.display()
                        )));
                    }
                }
            } else {
                let files = scan_sstable_files(sst_dir);
                crate::monitor::record_sstable_manifest_legacy_scan(files.len() as u64);
                files
            };

            if !files.is_empty() {
                sstables_vec.reserve(files.len());
                let descriptor_cache_path = sstable_descriptor_cache_path(sst_dir);
                let mut descriptor_cache = SstableDescriptorCache::load(&descriptor_cache_path);
                let mut descriptor_cache_dirty = false;
                let mut open_failures = 0usize;
                let mut open_tasks = Vec::with_capacity(files.len());
                for live_file in files {
                    let block_cache = block_cache.clone();
                    let id = live_file.id;
                    let path = live_file.path;
                    let fingerprint = sstable_timestamp_fingerprint(&path);
                    let descriptor = live_file.descriptor.or_else(|| {
                        fingerprint.as_ref().and_then(|fingerprint| {
                            descriptor_cache.descriptor_for(id, fingerprint)
                        })
                    });
                    let used_descriptor = descriptor.is_some();
                    open_tasks.push(tokio::spawn(async move {
                        (
                            id,
                            fingerprint,
                            used_descriptor,
                            SsTable::open_with_descriptor(path, id, block_cache, descriptor).await,
                        )
                    }));
                }
                for task in open_tasks {
                    match task.await {
                        Ok((id, fingerprint, used_descriptor, Ok(sst))) => {
                            if !used_descriptor {
                                if let Some(fingerprint) = fingerprint {
                                    descriptor_cache.set_from_sstable(id, fingerprint, &sst);
                                    descriptor_cache_dirty = true;
                                }
                            }
                            sstables_vec.push(Arc::new(sst));
                        }
                        Ok((_id, _fingerprint, _used_descriptor, Err(_))) => {
                            crate::monitor::inc_sstable_manifest_open_error();
                            if manifest_present {
                                return Err(FusionError::Storage(
                                    "manifest-referenced SSTable failed to open".to_string(),
                                ));
                            }
                            open_failures += 1;
                        }
                        Err(_) => {
                            crate::monitor::inc_sstable_manifest_open_error();
                            if manifest_present {
                                return Err(FusionError::Storage(
                                    "manifest-referenced SSTable open task failed".to_string(),
                                ));
                            }
                            open_failures += 1;
                        }
                    }
                }
                sstables_vec.sort_by_key(|sst| sst.id);
                descriptor_cache_dirty |= descriptor_cache.retain_live_sstables(&sstables_vec);
                if descriptor_cache_dirty {
                    if let Err(error) = descriptor_cache.persist(&descriptor_cache_path) {
                        eprintln!(
                            "Warning: failed to persist SSTable descriptor cache {}: {}",
                            descriptor_cache_path.display(),
                            error
                        );
                    }
                }
                if open_failures == 0 {
                    if let Err(error) = SstableManifest::persist_sstables(sst_dir, &sstables_vec) {
                        eprintln!(
                            "Warning: failed to persist SSTable manifest {}: {}",
                            sstable_manifest_path(sst_dir).display(),
                            error
                        );
                    }
                }
            }
        }

        let active_memtable_id = sstables_vec
            .last()
            .map(|sst| sst.id.saturating_add(1))
            .unwrap_or(1);
        let next_id = active_memtable_id.saturating_add(1);
        let active = MemTable::new(active_memtable_id);
        let max_sstable_ts = Self::restore_max_sstable_timestamp(&sstables_vec, sst_dir).await?;

        // Replay WAL
        // We need to replay committed transactions into the active memtable.
        let replay_entries = wal.replay()?;
        let memtable_threshold = config.memtable_flush_threshold_bytes().max(1);
        let paths = Arc::new(FusionStoragePaths {
            sstable_dir,
            inverted_index_path,
            trigram_index_path,
        });

        let storage = Self {
            active_memtable: Arc::new(RwLock::new(active)),
            immutable_memtables: Arc::new(RwLock::new(immutable_memtable_buffer())),
            sstables: Arc::new(RwLock::new(sstables_vec)),
            obsolete_sstables: Arc::new(RwLock::new(obsolete_sstable_buffer())),
            wal: Arc::new(wal),
            current_ts: Arc::new(AtomicU64::new(0)), // Will be updated by replay
            active_read_timestamps: Arc::new(StdMutex::new(BTreeMap::new())),
            side_index_visibility: Arc::new(AsyncRwLock::new(())),
            next_memtable_id: Arc::new(AtomicU64::new(next_id)),
            flush_notify: Arc::new(Notify::new()),
            columnar_store: Arc::new(RwLock::new(None)),
            inverted_index: Arc::new(RwLock::new(
                InvertedIndex::load(&paths.inverted_index_path)
                    .unwrap_or_else(|_| InvertedIndex::new()),
            )),
            vector_index: {
                let vi = Arc::new(VectorIndex::new());
                vi.create_index("default");
                vi
            },
            // Trigram/HNSW are derived indexes. Startup rebuilds them from the
            // durable row state before serving requests, so a stale checkpoint
            // file can never become the recovery source of truth.
            trigram_index: Arc::new(RwLock::new(crate::storage::trigram::TrigramIndex::new())),
            block_cache,
            memtable_threshold,
            commit_lock: Arc::new(AsyncMutex::new(())),
            flush_lock: Arc::new(AsyncMutex::new(())),
            compaction_lock: Arc::new(AsyncMutex::new(())),
            data_migration_fence: Arc::new(DataMigrationFence::new(
                config.structured_data_shadow_v2,
            )),
            paths,
        };

        // Publish startup SSTable shape before WAL replay may add immutable memtables.
        crate::monitor::set_live_sstable_count(storage.sstables.read().unwrap().len() as u64);

        // Apply Replay
        let replay_apply_start = Instant::now();
        let mut max_replay_ts = 0;
        if !replay_entries.is_empty() {
            println!("Replaying {} WAL entries...", replay_entries.len());
            // Use active_memtable logic but with manual rotation

            for entry in replay_entries {
                match entry {
                    WalEntry::Put(k, v) => {
                        // Strict validation: Skip keys smaller than TS_SIZE
                        if k.len() < TS_SIZE {
                            continue;
                        }

                        if k.len() > TS_SIZE {
                            let (_, ts) = Self::decode_key(&k);
                            if ts > max_replay_ts {
                                max_replay_ts = ts;
                            }
                        }

                        let needs_rotate = {
                            let active = storage.active_memtable.read().unwrap();
                            active.insert(k, v);
                            active.size.load(Ordering::Relaxed) > storage.memtable_threshold as u64
                        };

                        if needs_rotate {
                            storage.rotate_memtable().await;
                        }
                    }
                    WalEntry::Delete(k) => {
                        if k.len() > TS_SIZE {
                            let (_, ts) = Self::decode_key(&k);
                            if ts > max_replay_ts {
                                max_replay_ts = ts;
                            }
                        }
                        let needs_rotate = {
                            let active = storage.active_memtable.read().unwrap();
                            active.insert(k, Vec::new());
                            active.size.load(Ordering::Relaxed) > storage.memtable_threshold as u64
                        };

                        if needs_rotate {
                            storage.rotate_memtable().await;
                        }
                    }
                }
            }
            println!("WAL Replay complete. Restored TS: {}", max_replay_ts);
        }
        crate::monitor::record_wal_replay_apply(
            u64::try_from(replay_apply_start.elapsed().as_micros()).unwrap_or(u64::MAX),
            max_replay_ts,
        );
        let restored_ts = max_sstable_ts.max(max_replay_ts);
        storage.current_ts.store(restored_ts, Ordering::SeqCst);

        storage
            .load_and_gate_data_migration_phase(config.structured_data_shadow_v2)
            .await?;

        storage.rebuild_side_indexes().await?;

        // Start flush thread
        let s = storage.clone();
        tokio::spawn(async move {
            s.flush_loop().await;
        });

        // Start compaction thread
        let s2 = storage.clone();
        tokio::spawn(async move {
            s2.compaction_loop().await;
        });

        let s4 = storage.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            let sstables = s4.sstables.read().unwrap().clone();
            for sstable in sstables {
                sstable.preload_block_properties().await;
            }
        });

        Ok(storage)
    }

    fn register_current_read_ts(&self) -> u64 {
        self.register_current_read_ts_with(|_| {})
    }

    fn register_current_read_ts_with<F>(&self, after_read_ts: F) -> u64
    where
        F: FnOnce(u64),
    {
        // This mutex is also the compaction watermark barrier. Loading the public
        // timestamp and registering it must be one critical section; otherwise
        // compaction can sample an empty registry after the load and discard the
        // floor version before the transaction becomes visible to the registry.
        let mut active = self.active_read_timestamps.lock().unwrap();
        let read_ts = self.current_ts.load(Ordering::SeqCst);
        after_read_ts(read_ts);
        *active.entry(read_ts).or_insert(0) += 1;
        read_ts
    }

    fn unregister_active_read_ts(&self, read_ts: u64) {
        let mut active = self.active_read_timestamps.lock().unwrap();
        if let Some(count) = active.get_mut(&read_ts) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                active.remove(&read_ts);
            }
        }
    }

    fn oldest_active_read_ts(&self) -> Option<u64> {
        self.active_read_timestamps
            .lock()
            .unwrap()
            .keys()
            .next()
            .copied()
    }

    pub async fn cdc_events_since(&self, since: u64, limit: usize) -> Result<Vec<CdcEvent>> {
        if limit == 0 || since == u64::MAX {
            return Ok(Vec::new());
        }

        let start_key = if since == 0 {
            CDC_KEY_PREFIX.as_bytes().to_vec()
        } else {
            cdc_key_for_sequence(since + 1).into_bytes()
        };
        let txn = self.begin_transaction().await?;
        let pairs = txn
            .scan_range(&start_key, CDC_KEY_END.as_bytes(), Some(limit))
            .await?;

        let mut events = Vec::with_capacity(pairs.len());
        for (_, value) in pairs {
            events.push(decode_cdc_event(&value)?);
        }
        Ok(events)
    }

    pub async fn cdc_latest_sequence(&self) -> Result<u64> {
        let txn = self.begin_transaction().await?;
        match txn
            .last(CDC_KEY_PREFIX.as_bytes(), CDC_KEY_END.as_bytes())
            .await?
        {
            Some((_, value)) => Ok(decode_cdc_event(&value)?.sequence),
            None => Ok(0),
        }
    }

    pub async fn update_columnar_store(&self, ids: Vec<String>, vectors: Vec<Vec<f32>>) {
        let _visibility_guard = self.side_index_visibility.clone().write_owned().await;
        // New: HNSW Index
        for (id, vec) in ids.iter().zip(vectors.iter()) {
            let _ = self.vector_index.insert("default", id.clone(), vec.clone());
        }

        // Legacy: Columnar Store
        let store = ColumnarVectorStore::new(ids, vectors, 3);
        let mut guard = self.columnar_store.write().unwrap();
        *guard = Some(store);
    }

    // Update Inverted Index (Batch)
    pub fn update_inverted_index(&self, doc_id: String, text: &str) {
        let mut guard = self.inverted_index.write().unwrap();
        guard.add_document(doc_id, text);
    }

    fn vector_search_unlocked(&self, query: &[f32], limit: usize) -> Vec<(String, f32)> {
        // Use HNSW Index
        self.vector_index
            .search("default", query, limit)
            .unwrap_or_default()
    }

    pub async fn vector_search(&self, query: &[f32], limit: usize) -> Vec<(String, f32)> {
        let _visibility_guard = self.side_index_visibility.clone().read_owned().await;
        self.vector_search_unlocked(query, limit)
    }

    pub fn bm25_search(&self, query: &str, limit: usize) -> Vec<(String, f32)> {
        let guard = self.inverted_index.read().unwrap();
        // k1=1.2, b=0.75 are standard defaults
        guard.search_bm25_limited(query, 1.2, 0.75, limit)
    }

    // Hybrid Search: RRF (Reciprocal Rank Fusion)
    pub async fn hybrid_search(
        &self,
        text_query: &str,
        vector_query: &[f32],
        limit: usize,
    ) -> Vec<(String, f32)> {
        if limit == 0 {
            return Vec::new();
        }

        let _visibility_guard = self.side_index_visibility.clone().read_owned().await;
        // 1. Get results from both sources
        let candidate_limit = limit.saturating_mul(2);
        let text_results = self.bm25_search(text_query, candidate_limit); // Get more candidates
        let vector_results = self.vector_search_unlocked(vector_query, candidate_limit);

        // 2. RRF Fusion
        // Score = 1 / (k + rank)
        let k = 60.0;
        let mut rrf_scores =
            HashMap::with_capacity(text_results.len().saturating_add(vector_results.len()));

        for (rank, (id, _score)) in text_results.iter().enumerate() {
            let s = 1.0 / (k + rank as f32 + 1.0);
            *rrf_scores.entry(id.clone()).or_insert(0.0) += s;
        }

        for (rank, (id, _score)) in vector_results.iter().enumerate() {
            let s = 1.0 / (k + rank as f32 + 1.0);
            *rrf_scores.entry(id.clone()).or_insert(0.0) += s;
        }

        let mut final_results = Vec::with_capacity(rrf_scores.len());
        for score in rrf_scores {
            final_results.push(score);
        }
        if final_results.len() > limit {
            let _ = final_results.select_nth_unstable_by(limit, rrf_score_order);
            final_results.truncate(limit);
        }
        final_results.sort_by(rrf_score_order);

        final_results
    }

    // MVCC Key Encoding: Key + (MAX - TS)
    fn encode_key(user_key: &[u8], ts: u64) -> Vec<u8> {
        let mut k = Vec::with_capacity(user_key.len() + TS_SIZE);
        k.extend_from_slice(user_key);
        k.extend_from_slice(&(u64::MAX - ts).to_be_bytes());
        k
    }

    fn decode_key(internal_key: &[u8]) -> (&[u8], u64) {
        let len = internal_key.len();
        if len < TS_SIZE {
            return (internal_key, 0);
        }
        let (k, ts_bytes) = internal_key.split_at(len - TS_SIZE);
        let inverted_ts = u64::from_be_bytes(ts_bytes.try_into().unwrap());
        (k, u64::MAX - inverted_ts)
    }

    // Value Encoding: [Flag] + [Data]
    fn encode_value(is_put: bool, data: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(1 + data.len());
        v.push(if is_put { 1 } else { 0 });
        v.extend_from_slice(data);
        v
    }

    fn decode_value(data: &[u8]) -> (bool, &[u8]) {
        if data.is_empty() {
            return (false, &[]);
        }
        (data[0] == 1, &data[1..])
    }

    fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
        let mut end = prefix.to_vec();
        while let Some(last) = end.last_mut() {
            if *last < 255 {
                *last += 1;
                return Some(end);
            }
            end.pop();
        }
        None
    }

    fn sstable_path_for(&self, id: u64) -> PathBuf {
        self.paths.sstable_dir.join(sstable_file_name_for_id(id))
    }

    fn sstable_staging_path_for(&self, id: u64) -> PathBuf {
        self.paths
            .sstable_dir
            .join(format!("{}.building", sstable_file_name_for_id(id)))
    }

    async fn publish_staged_sstable(&self, staging_path: &Path, final_path: &Path) -> Result<()> {
        tokio::fs::rename(staging_path, final_path).await?;
        sync_directory(&self.paths.sstable_dir)?;
        SsTable::remove_reverse_seek_file_for_path(staging_path).await;
        SsTable::remove_index_cache_file_for_path(staging_path).await;
        Ok(())
    }

    async fn restore_max_sstable_timestamp(
        sstables: &[Arc<SsTable>],
        sstable_dir: &Path,
    ) -> Result<u64> {
        if sstables.is_empty() {
            return Ok(0);
        }

        let cache_path = sstable_timestamp_cache_path(sstable_dir);
        let mut cache = SstableTimestampCache::load(&cache_path);
        let mut cache_dirty = cache.retain_live_sstables(sstables);
        let mut max_sstable_ts = 0;
        let mut cache_hits = 0usize;
        let mut scanned = 0usize;

        for sst in sstables {
            let fingerprint = sstable_timestamp_fingerprint(&sst.path);
            if let Some(fingerprint) = fingerprint.as_ref() {
                if let Some(max_ts) = cache.max_ts_for(sst.id, fingerprint) {
                    cache_hits += 1;
                    max_sstable_ts = max_sstable_ts.max(max_ts);
                    continue;
                }
            }

            let max_ts = Self::scan_sstable_max_timestamp(sst)
                .await
                .map_err(|error| {
                    FusionError::Storage(format!(
                        "failed to restore MVCC timestamp from live SSTable {}: {error}",
                        sst.id
                    ))
                })?;
            scanned += 1;
            max_sstable_ts = max_sstable_ts.max(max_ts);
            if let Some(fingerprint) = fingerprint {
                cache.set(sst.id, fingerprint, max_ts);
                cache_dirty = true;
            }
        }

        if cache_dirty {
            if let Err(error) = cache.persist(&cache_path) {
                eprintln!(
                    "Warning: failed to persist SSTable timestamp cache {}: {}",
                    cache_path.display(),
                    error
                );
            }
        }

        println!(
            "Restored SSTable max timestamp {} ({} cached, {} scanned).",
            max_sstable_ts, cache_hits, scanned
        );
        Ok(max_sstable_ts)
    }

    async fn scan_sstable_max_timestamp(sst: &SsTable) -> Result<u64> {
        let mut max_ts = 0;
        let mut iter = sst
            .new_iterator_with_options(None, SsTableReadOptions::no_fill_cache())
            .await?;
        while let Some((key, _)) = iter.next().await? {
            if key.len() >= TS_SIZE {
                let (_, ts) = Self::decode_key(&key);
                max_ts = max_ts.max(ts);
            }
        }
        Ok(max_ts)
    }

    fn persist_sstable_timestamp_cache_entry(
        sstable_dir: &Path,
        sstable_path: &Path,
        sstable_id: u64,
        max_ts: u64,
    ) {
        let Some(fingerprint) = sstable_timestamp_fingerprint(sstable_path) else {
            return;
        };
        let cache_path = sstable_timestamp_cache_path(sstable_dir);
        let mut cache = SstableTimestampCache::load(&cache_path);
        cache.set(sstable_id, fingerprint, max_ts);
        if let Err(error) = cache.persist(&cache_path) {
            eprintln!(
                "Warning: failed to persist SSTable timestamp cache {}: {}",
                cache_path.display(),
                error
            );
        }
    }

    fn persist_sstable_descriptor_cache_entry(sstable_dir: &Path, sstable: &SsTable) {
        let Some(fingerprint) = sstable_timestamp_fingerprint(&sstable.path) else {
            return;
        };
        let cache_path = sstable_descriptor_cache_path(sstable_dir);
        let mut cache = SstableDescriptorCache::load(&cache_path);
        cache.set_from_sstable(sstable.id, fingerprint, sstable);
        if let Err(error) = cache.persist(&cache_path) {
            eprintln!(
                "Warning: failed to persist SSTable descriptor cache {}: {}",
                cache_path.display(),
                error
            );
        }
    }

    fn latest_memtable_timestamp(
        memtable: &MemTable,
        search_key: &[u8],
        user_key: &[u8],
    ) -> Option<u64> {
        memtable
            .map
            .range(search_key.to_vec()..)
            .next()
            .and_then(|entry| {
                let (entry_user_key, timestamp) = Self::decode_key(entry.key());
                (entry_user_key == user_key).then_some(timestamp)
            })
    }

    async fn latest_committed_timestamp(&self, user_key: &[u8]) -> Result<Option<u64>> {
        let search_key = Self::encode_key(user_key, u64::MAX);
        let mut latest = {
            let active = self.active_memtable.read().unwrap();
            Self::latest_memtable_timestamp(&active, &search_key, user_key)
        };

        {
            let immutable = self.immutable_memtables.read().unwrap();
            for memtable in immutable.iter().rev() {
                if let Some(timestamp) =
                    Self::latest_memtable_timestamp(memtable, &search_key, user_key)
                {
                    latest = Some(latest.map_or(timestamp, |current| current.max(timestamp)));
                }
            }
        }

        // Flush publishes the replacement SSTable before removing its immutable MemTable, and
        // compaction publishes its output before retiring inputs. Taking the SSTable snapshot
        // after the MemTable probes therefore cannot miss a version while a source is moving.
        let sstables = self.sstables.read().unwrap().clone();
        for sstable in sstables {
            if sstable.meta.first_key.len() < TS_SIZE || sstable.meta.last_key.len() < TS_SIZE {
                return Err(FusionError::Storage(format!(
                    "SSTable {} has malformed MVCC key bounds",
                    sstable.id
                )));
            }
            let (first_user_key, _) = Self::decode_key(&sstable.meta.first_key);
            let (last_user_key, _) = Self::decode_key(&sstable.meta.last_key);
            if user_key < first_user_key || user_key > last_user_key {
                continue;
            }
            if matches!(
                sstable.probe_user_key_filter(user_key, TS_SIZE),
                SsTablePrefixFilterProbe::NoMatch
            ) {
                continue;
            }

            if let Some((internal_key, _)) = sstable.find_ge(&search_key).await? {
                let (found_user_key, timestamp) = Self::decode_key(&internal_key);
                if found_user_key == user_key {
                    latest = Some(latest.map_or(timestamp, |current| current.max(timestamp)));
                }
            }
        }

        Ok(latest)
    }

    async fn sstable_matches_memtable(sstable: &SsTable, memtable: &MemTable) -> Result<bool> {
        let expected = memtable
            .map
            .iter()
            .filter(|entry| entry.key().len() >= TS_SIZE)
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect::<Vec<_>>();
        let mut iterator = sstable
            .new_iterator_with_options(None, SsTableReadOptions::no_fill_cache())
            .await?;
        let mut expected = expected.into_iter();

        loop {
            match (expected.next(), iterator.next().await?) {
                (Some(expected_entry), Some(actual_entry)) if expected_entry == actual_entry => {}
                (None, None) => return Ok(true),
                _ => return Ok(false),
            }
        }
    }

    fn register_live_sstable(&self, sstable: SsTable) -> std::io::Result<()> {
        let new_sstable = Arc::new(sstable);
        let mut sstables = self.sstables.write().unwrap();
        if sstables
            .iter()
            .any(|existing| existing.id == new_sstable.id)
        {
            crate::monitor::set_live_sstable_count(sstables.len() as u64);
            return Ok(());
        }
        let mut next_sstables = Vec::with_capacity(sstables.len().saturating_add(1));
        next_sstables.extend(sstables.iter().cloned());
        next_sstables.push(new_sstable);
        next_sstables.sort_by_key(|sstable| sstable.id);
        SstableManifest::persist_sstables(&self.paths.sstable_dir, &next_sstables)?;
        *sstables = next_sstables;
        crate::monitor::set_live_sstable_count(sstables.len() as u64);
        Ok(())
    }

    fn install_compacted_sstable(
        &self,
        new_sstable: SsTable,
        candidate_ids: &[u64; COMPACTION_FANIN],
    ) -> std::io::Result<()> {
        let new_sstable = Arc::new(new_sstable);
        let mut sstables = self.sstables.write().unwrap();
        let mut next_sstables = Vec::with_capacity(sstables.len().saturating_add(1));
        next_sstables.extend(
            sstables
                .iter()
                .filter(|sstable| !candidate_ids.contains(&sstable.id))
                .cloned(),
        );
        next_sstables.push(new_sstable);
        next_sstables.sort_by_key(|sstable| sstable.id);
        SstableManifest::persist_sstables(&self.paths.sstable_dir, &next_sstables)?;
        *sstables = next_sstables;
        crate::monitor::set_live_sstable_count(sstables.len() as u64);
        Ok(())
    }

    fn persist_secondary_indexes(&self, log_prefix: &str) {
        if let Ok(guard) = self.inverted_index.read() {
            if let Err(e) = guard.save(&self.paths.inverted_index_path) {
                eprintln!("{} Failed to save inverted index: {:?}", log_prefix, e);
            }
        }
        if let Ok(guard) = self.trigram_index.read() {
            if let Err(e) = guard.save(&self.paths.trigram_index_path) {
                eprintln!("{} Failed to save trigram index: {:?}", log_prefix, e);
            }
        }
    }

    async fn flush_all_immutable_memtables(&self) -> Result<()> {
        let _guard = self.flush_lock.lock().await;
        self.flush_all_immutable_memtables_locked().await
    }

    async fn flush_all_immutable_memtables_locked(&self) -> Result<()> {
        while let Some(memtable) = self.next_memtable_to_flush() {
            self.flush_memtable_sync(&memtable).await?;
            self.mark_memtable_flushed(memtable.id);
        }
        Ok(())
    }

    pub async fn create_snapshot_now(&self) -> Result<()> {
        let _commit_guard = self.commit_lock.lock().await;
        self.rotate_memtable().await;
        self.flush_all_immutable_memtables().await?;
        self.persist_secondary_indexes("[snapshot]");
        // Commits are blocked from the rotation through WAL reset. Every entry accepted before
        // the barrier is now in a manifest-listed SSTable, and no later active entry can be lost.
        self.wal.truncate()?;
        Ok(())
    }

    pub async fn replace_visible_entries_for_snapshot(
        &self,
        start: &[u8],
        end: &[u8],
        entries: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<()> {
        let _visibility_guard = self.side_index_visibility.clone().write_owned().await;
        let _commit_guard = self.commit_lock.lock().await;
        let read_ts = self.current_ts.load(Ordering::SeqCst);
        let snapshot_txn = FusionTransaction {
            storage: self.clone(),
            write_buffer: transaction_write_buffer(),
            read_ts,
            read_ts_registered: false,
            capture_cdc: AtomicBool::new(true),
            side_index_deltas: std::sync::Mutex::new(Vec::new()),
            fenced_migration_phase: None,
        };
        let existing = snapshot_txn.scan_range(start, end, None).await?;
        let mut future_entries = if start.is_empty() && end == [0xff] {
            BTreeMap::new()
        } else {
            snapshot_txn
                .scan_range(b"", &[0xff], None)
                .await?
                .into_iter()
                .filter(|(key, _)| key.as_slice() < start || key.as_slice() >= end)
                .collect()
        };
        for (key, value) in entries {
            future_entries.insert(key.clone(), value.clone());
        }
        // Validate and fully build derived state before making the replacement
        // durable. A malformed snapshot therefore cannot leave an unpublished
        // WAL/MemTable commit that becomes visible only after restart.
        let future_entries: Vec<_> = future_entries.into_iter().collect();
        let (rebuilt_vector, rebuilt_trigram) =
            Self::build_side_indexes_from_visible_entries(&future_entries)?;
        let total_entries = existing.len().saturating_add(entries.len());
        let mut publish_ts = read_ts;
        let mut needs_rotate = false;
        if total_entries > 0 {
            let commit_ts = read_ts
                .checked_add(1)
                .ok_or_else(|| FusionError::Storage("MVCC timestamp exhausted".to_string()))?;
            let mut wal_entries = Vec::with_capacity(total_entries);
            let mut mem_entries = Vec::with_capacity(total_entries);

            for (key, _) in existing {
                let encoded_key = FusionStorage::encode_key(&key, commit_ts);
                let encoded_value = FusionStorage::encode_value(false, &[]);
                wal_entries.push(WalEntry::Put(encoded_key.clone(), encoded_value.clone()));
                mem_entries.push((encoded_key, encoded_value));
            }
            for (key, value) in entries {
                let encoded_key = FusionStorage::encode_key(key, commit_ts);
                let encoded_value = FusionStorage::encode_value(true, value);
                wal_entries.push(WalEntry::Put(encoded_key.clone(), encoded_value.clone()));
                mem_entries.push((encoded_key, encoded_value));
            }

            self.wal.append_batch_async(wal_entries).await?;
            needs_rotate = {
                let active = self.active_memtable.write().unwrap();
                for (key, value) in mem_entries {
                    active.insert(key, value);
                }
                active.size.load(Ordering::Relaxed) > self.memtable_threshold as u64
            };
            publish_ts = commit_ts;
        }

        self.vector_index.replace_with(rebuilt_vector);
        *self.trigram_index.write().unwrap() = rebuilt_trigram;
        self.current_ts.store(publish_ts, Ordering::SeqCst);

        // A snapshot install rewrites the whole visible keyspace, including
        // the migration phase record. Drop the cached fence inside this same
        // critical section: publishing the new keyspace and invalidating the
        // fence must be one step, or a commit landing in between would
        // revalidate its pin against a pre-install phase and pass.
        self.data_migration_fence.invalidate();

        if needs_rotate {
            self.rotate_memtable().await;
        }
        Ok(())
    }

    async fn build_side_indexes(
        &self,
        txn: &dyn Transaction,
    ) -> Result<(VectorIndex, crate::storage::trigram::TrigramIndex)> {
        let schema_entries = txn.scan_prefix(b"schema:", None).await?;
        let (vector, mut trigram, plans) = Self::prepare_side_index_rebuild(
            schema_entries
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice())),
        )?;
        let mut rebuild_error = None;
        let mut visitor = |key: &[u8], row: &[u8]| {
            if rebuild_error.is_some() {
                return false;
            }
            match Self::apply_visible_side_index_row(key, row, &plans, &vector, &mut trigram) {
                Ok(()) => true,
                Err(error) => {
                    rebuild_error = Some(error);
                    false
                }
            }
        };
        txn.scan_range_for_each(b"", &[0xff], None, &mut visitor)
            .await?;
        drop(visitor);
        if let Some(error) = rebuild_error {
            return Err(error);
        }
        vector.build_all()?;
        Ok((vector, trigram))
    }

    fn build_side_indexes_from_visible_entries(
        entries: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(VectorIndex, crate::storage::trigram::TrigramIndex)> {
        let (vector, mut trigram, plans) = Self::prepare_side_index_rebuild(
            entries
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice())),
        )?;
        for (key, row) in entries {
            Self::apply_visible_side_index_row(key, row, &plans, &vector, &mut trigram)?;
        }
        vector.build_all()?;
        Ok((vector, trigram))
    }

    fn prepare_side_index_rebuild<'a>(
        entries: impl IntoIterator<Item = (&'a [u8], &'a [u8])>,
    ) -> Result<(
        VectorIndex,
        crate::storage::trigram::TrigramIndex,
        Vec<SideIndexRebuildPlan>,
    )> {
        let vector = VectorIndex::new();
        vector.create_index("default");
        let trigram = crate::storage::trigram::TrigramIndex::new();
        let mut plans = Vec::new();

        for (key, value) in entries {
            let Some(table_bytes) = key.strip_prefix(b"schema:") else {
                continue;
            };
            let table_name = std::str::from_utf8(table_bytes).map_err(|error| {
                FusionError::Storage(format!("side-index schema key is not UTF-8: {error}"))
            })?;
            let schema =
                bincode::deserialize::<crate::catalog::TableSchema>(value).map_err(|error| {
                    FusionError::Storage(format!(
                        "side-index rebuild failed to decode schema '{table_name}': {error}"
                    ))
                })?;
            let trigram_columns: Vec<(usize, String)> = schema
                .columns
                .iter()
                .enumerate()
                .filter(|(_, column)| column.is_trigram_text_column())
                .map(|(index, column)| (index, column.name.clone()))
                .collect();
            let mut hnsw_columns = Vec::new();
            for (index, column) in schema.columns.iter().enumerate() {
                if column.is_indexed && column.index_type == crate::catalog::IndexType::HNSW {
                    let index_name = hnsw_index_name_for_column(&table_name, &column.name)?;
                    vector.create_index(&index_name);
                    hnsw_columns.push((index, index_name));
                }
            }
            if !trigram_columns.is_empty() || !hnsw_columns.is_empty() {
                plans.push(SideIndexRebuildPlan {
                    table_name: table_name.to_string(),
                    trigram_columns,
                    hnsw_columns,
                });
            }
        }
        // Longest-name-first prevents `data:tenant:archive:*` from also being
        // interpreted as a row of table `tenant`.
        plans.sort_by(|left, right| {
            right
                .table_name
                .len()
                .cmp(&left.table_name.len())
                .then(left.table_name.cmp(&right.table_name))
        });
        Ok((vector, trigram, plans))
    }

    fn apply_visible_side_index_row(
        key: &[u8],
        row: &[u8],
        plans: &[SideIndexRebuildPlan],
        vector: &VectorIndex,
        trigram: &mut crate::storage::trigram::TrigramIndex,
    ) -> Result<()> {
        let Some(payload) = side_index_data_key_payload(key)? else {
            return Ok(());
        };
        let Some(plan) = plans.iter().find(|plan| {
            let table = plan.table_name.as_bytes();
            payload.get(..table.len()) == Some(table) && payload.get(table.len()) == Some(&b':')
        }) else {
            return Ok(());
        };
        let row_id =
            std::str::from_utf8(&payload[plan.table_name.len() + 1..]).map_err(|error| {
                FusionError::Storage(format!(
                    "side-index row id for '{}' is not UTF-8: {error}",
                    plan.table_name
                ))
            })?;
        let numeric_id = crate::storage::trigram::numeric_row_id_for_str(row_id);
        for (column_index, column_name) in &plan.trigram_columns {
            match crate::common::encoding::RowDecoder::decode_column(row, *column_index).map_err(
                |error| {
                    FusionError::Storage(format!(
                        "trigram rebuild failed to decode {}.{} for row '{}': {}",
                        plan.table_name, column_name, row_id, error
                    ))
                },
            )? {
                Some(crate::common::Value::String(text)) => trigram.add_with_id_str(
                    &plan.table_name,
                    column_name,
                    numeric_id,
                    row_id,
                    &text,
                ),
                Some(crate::common::Value::Null) | None => {}
                Some(value) => {
                    return Err(FusionError::Storage(format!(
                        "trigram rebuild found non-text value {value:?} in {}.{}",
                        plan.table_name, column_name
                    )));
                }
            }
        }
        for (column_index, index_name) in &plan.hnsw_columns {
            match crate::common::encoding::RowDecoder::decode_column(row, *column_index).map_err(
                |error| {
                    FusionError::Storage(format!(
                        "vector rebuild failed to decode {} column {} for row '{}': {}",
                        plan.table_name, column_index, row_id, error
                    ))
                },
            )? {
                Some(crate::common::Value::Vector(value)) => {
                    vector.insert(index_name, row_id.to_string(), value)?;
                }
                Some(crate::common::Value::Null) | None => {}
                Some(value) => {
                    return Err(FusionError::Storage(format!(
                        "vector rebuild found non-vector value {value:?} in index '{index_name}'"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Read the durable Data V2 migration phase record and prime the fence.
    /// Called once during `with_config`, after WAL replay restored the MVCC
    /// clock, so the read sees crash-consistent state. A malformed record or
    /// a phase above what this binary implements refuses the open outright —
    /// running blind past the fence would corrupt the migration invariants.
    async fn load_and_gate_data_migration_phase(
        &self,
        structured_data_shadow_v2: bool,
    ) -> Result<()> {
        let txn = self.begin_transaction().await?;
        let raw = txn.get(migration_phase_key()).await?;
        drop(txn);

        let Some(raw) = raw else {
            self.data_migration_fence.resolve_with(None);
            return Ok(());
        };
        let record = DataMigrationPhaseRecord::decode(&raw).map_err(|error| {
            FusionError::Storage(format!(
                "Refusing to open: the Data V2 migration phase record is malformed ({error})"
            ))
        })?;
        if record.phase > MAX_SUPPORTED_PHASE {
            return Err(FusionError::Storage(format!(
                "Refusing to open: the store is at Data V2 migration phase '{}' (seq {}), but this binary only supports up to '{}'; upgrade the binary",
                record.phase.name(),
                record.phase_seq,
                MAX_SUPPORTED_PHASE.name()
            )));
        }
        if record.phase.shadow_writes_enabled() != structured_data_shadow_v2 {
            eprintln!(
                "[data-v2] the durable migration phase record ('{}', seq {}) overrides the structured_data_shadow_v2 config flag ({}); the record is authoritative",
                record.phase.name(),
                record.phase_seq,
                structured_data_shadow_v2
            );
        }
        self.data_migration_fence.resolve_with(Some(&record));
        Ok(())
    }

    /// Re-read the durable phase record and refresh the fence cache. Used when
    /// the fence was invalidated (Raft apply / snapshot install) and a commit
    /// or observer needs the current value.
    pub(crate) async fn reload_data_migration_fence(&self) -> Result<FenceSnapshot> {
        let txn = self.begin_transaction().await?;
        let raw = txn.get(migration_phase_key()).await?;
        drop(txn);
        let record = raw
            .as_deref()
            .map(DataMigrationPhaseRecord::decode)
            .transpose()?;
        Ok(self.data_migration_fence.resolve_with(record.as_ref()))
    }

    pub(crate) fn data_migration_fence(&self) -> Arc<DataMigrationFence> {
        self.data_migration_fence.clone()
    }

    async fn rebuild_side_indexes(&self) -> Result<()> {
        println!("Rebuilding derived side indexes from storage...");
        let txn = self.begin_transaction().await?;
        let (vector, trigram) = self.build_side_indexes(txn.as_ref()).await?;
        self.vector_index.replace_with(vector);
        *self.trigram_index.write().unwrap() = trigram;
        println!("Derived side-index rebuild complete.");
        Ok(())
    }

    #[cfg(test)]
    async fn rebuild_vector_index(&self) -> Result<()> {
        let txn = self.begin_transaction().await?;
        let (vector, _) = self.build_side_indexes(txn.as_ref()).await?;
        self.vector_index.replace_with(vector);
        Ok(())
    }

    fn next_memtable_to_flush(&self) -> Option<MemTable> {
        let imm = self.immutable_memtables.read().unwrap();
        imm.first().cloned()
    }

    fn mark_memtable_flushed(&self, memtable_id: u64) {
        let mut imm = self.immutable_memtables.write().unwrap();
        if let Some(pos) = imm.iter().position(|candidate| candidate.id == memtable_id) {
            imm.remove(pos);
        }
    }

    async fn sql_zone_map_schema_snapshot(&self) -> Arc<BTreeMap<String, TableSchema>> {
        let txn = match self.begin_transaction().await {
            Ok(txn) => txn,
            Err(_) => return Arc::new(BTreeMap::new()),
        };
        let entries = match txn.scan_prefix(b"schema:", None).await {
            Ok(entries) => entries,
            Err(_) => return Arc::new(BTreeMap::new()),
        };

        let mut schemas = BTreeMap::new();
        for (key, value) in entries {
            let Some(table_name) = std::str::from_utf8(&key)
                .ok()
                .and_then(|key| key.strip_prefix("schema:"))
            else {
                continue;
            };
            if let Ok(schema) = bincode::deserialize::<TableSchema>(&value) {
                schemas.insert(table_name.to_string(), schema);
            }
        }
        Arc::new(schemas)
    }

    async fn sstable_builder_with_zone_maps(
        &self,
        path: PathBuf,
        expected_items: usize,
    ) -> SsTableBuilder {
        let mut builder = SsTableBuilder::new(path);
        builder.set_expected_filter_items(expected_items);
        builder.enable_user_key_prefix_filter(TS_SIZE);
        builder.enable_sql_zone_map_collection(self.sql_zone_map_schema_snapshot().await);
        builder
    }

    async fn flush_loop(&self) {
        let _ = tokio::fs::create_dir_all(&self.paths.sstable_dir).await;

        loop {
            self.flush_notify.notified().await;
            let flush_result = {
                let _guard = self.flush_lock.lock().await;
                self.flush_all_immutable_memtables_locked().await
            };
            if let Err(error) = flush_result {
                eprintln!("Background MemTable flush failed; WAL and source retained: {error}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                self.flush_notify.notify_one();
                continue;
            }
            self.persist_secondary_indexes("[flush]");
        }
    }

    fn compaction_candidates(&self) -> Option<[Arc<SsTable>; COMPACTION_FANIN]> {
        let sstables = self.sstables.read().unwrap();
        if sstables.len() < COMPACTION_FANIN {
            return None;
        }

        Some(std::array::from_fn(|index| Arc::clone(&sstables[index])))
    }

    async fn compact_once(&self) -> Result<bool> {
        let _guard = self.compaction_lock.lock().await;
        self.compact_once_inner().await
    }

    async fn collect_obsolete_sstables(&self) {
        let ready_to_delete = {
            let mut obsolete = self.obsolete_sstables.write().unwrap();
            let mut ready_to_delete = obsolete_sstable_path_buffer(obsolete.len());
            let mut index = 0;
            while index < obsolete.len() {
                if Arc::strong_count(&obsolete[index]) == 1 {
                    let sst = obsolete.remove(index);
                    ready_to_delete.push(sst.path.clone());
                } else {
                    index += 1;
                }
            }
            ready_to_delete
        };

        for path in ready_to_delete {
            let _ = tokio::fs::remove_file(&path).await;
            SsTable::remove_index_cache_file_for_path(&path).await;
            SsTable::remove_reverse_seek_file_for_path(&path).await;
        }
    }

    async fn compact_once_inner(&self) -> Result<bool> {
        self.collect_obsolete_sstables().await;

        let Some(candidates) = self.compaction_candidates() else {
            return Ok(false);
        };
        let compaction_input_bytes = candidates.iter().map(|sst| sst.file_len).sum::<u64>();
        let compaction_input_entries = candidates
            .iter()
            .map(|sst| sst.estimated_entry_count())
            .sum::<usize>();

        let mut iterators = Vec::with_capacity(COMPACTION_FANIN);
        for sst in &candidates {
            iterators.push(
                sst.new_iterator_with_options(None, SsTableReadOptions::no_fill_cache())
                    .await?,
            );
        }

        if iterators.is_empty() {
            return Ok(false);
        }

        // Output builder
        let new_id = self.next_memtable_id.fetch_add(1, Ordering::Relaxed);
        let out_path = self.sstable_path_for(new_id);
        let staging_path = self.sstable_staging_path_for(new_id);
        let _ = tokio::fs::remove_file(&staging_path).await;
        SsTable::remove_reverse_seek_file_for_path(&staging_path).await;
        SsTable::remove_index_cache_file_for_path(&staging_path).await;
        let mut builder = self
            .sstable_builder_with_zone_maps(staging_path.clone(), compaction_input_entries)
            .await;

        // Merge Logic
        let mut heap = merge_heap(iterators.len());

        // Init heap
        for (idx, it) in iterators.iter_mut().enumerate() {
            if let Some(entry) = it.next_entry().await? {
                heap.push(MergeItem {
                    entry,
                    iter_idx: idx,
                });
            }
        }

        let mut block_buffer = Vec::with_capacity(SSTABLE_BLOCK_BUFFER_CAPACITY);
        let mut block_count = 0;
        let mut first_key = None;
        let oldest_active_read_ts = self.oldest_active_read_ts();
        let mut last_base_key: Option<Vec<u8>> = None;
        let mut kept_floor_version_for_base = false;
        let mut dropped_version_count: u64 = 0;
        let mut max_output_ts = 0;

        while let Some(item) = heap.pop() {
            let k = item.entry.key();
            let v = item.entry.value();
            let idx = item.iter_idx;

            if k.len() < TS_SIZE {
                if let Some(entry) = iterators[idx].next_entry().await? {
                    heap.push(MergeItem {
                        entry,
                        iter_idx: idx,
                    });
                }
                continue;
            }

            // Keep exactly the versions needed by current and future snapshots. Without active
            // readers, only the latest version is needed. With active readers, keep all versions
            // newer than the oldest reader plus the first floor version visible to that reader.
            let base_key = &k[..k.len() - TS_SIZE];
            let is_new_base_key = last_base_key.as_deref() != Some(base_key);
            if is_new_base_key {
                last_base_key = Some(base_key.to_vec());
                kept_floor_version_for_base = false;
            }
            let (_, ts) = FusionStorage::decode_key(k);
            let keep_version = match oldest_active_read_ts {
                None => is_new_base_key,
                Some(oldest_read_ts) => {
                    if is_new_base_key || ts > oldest_read_ts {
                        true
                    } else if !kept_floor_version_for_base {
                        kept_floor_version_for_base = true;
                        true
                    } else {
                        false
                    }
                }
            };
            if !keep_version {
                dropped_version_count += 1;
                if let Some(entry) = iterators[idx].next_entry().await? {
                    heap.push(MergeItem {
                        entry,
                        iter_idx: idx,
                    });
                }
                continue;
            }
            if oldest_active_read_ts.is_some() && ts <= oldest_active_read_ts.unwrap_or(0) {
                kept_floor_version_for_base = true;
            }
            max_output_ts = max_output_ts.max(ts);

            if first_key.is_none() {
                first_key = Some(k.to_vec());
            }

            builder.add_key(k);
            block_buffer.extend_from_slice(&(k.len() as u32).to_le_bytes());
            block_buffer.extend_from_slice(k);
            block_buffer.extend_from_slice(&(v.len() as u32).to_le_bytes());
            block_buffer.extend_from_slice(v);
            block_count += 1;

            if block_buffer.len() >= SSTABLE_BLOCK_BUFFER_CAPACITY {
                if let Err(e) = builder
                    .flush_block(first_key.take().unwrap(), block_count, &block_buffer)
                    .await
                {
                    return Err(crate::common::FusionError::Storage(format!(
                        "Compaction failed to flush block: {:?}",
                        e
                    )));
                }
                block_buffer.clear();
                block_count = 0;
            }

            // Advance iterator
            if let Some(entry) = iterators[idx].next_entry().await? {
                heap.push(MergeItem {
                    entry,
                    iter_idx: idx,
                });
            }
        }

        if !block_buffer.is_empty() {
            if let Err(e) = builder
                .flush_block(first_key.take().unwrap(), block_count, &block_buffer)
                .await
            {
                return Err(crate::common::FusionError::Storage(format!(
                    "Compaction failed to flush last block: {:?}",
                    e
                )));
            }
        }

        if let Err(e) = builder.finish().await {
            return Err(crate::common::FusionError::Storage(format!(
                "Compaction failed to finish: {:?}",
                e
            )));
        }
        self.publish_staged_sstable(&staging_path, &out_path)
            .await?;
        let compaction_output_bytes = tokio::fs::metadata(&out_path)
            .await
            .map(|metadata| metadata.len())
            .unwrap_or(0);

        // Open new SST
        match SsTable::open(out_path.clone(), new_id, self.block_cache.clone()).await {
            Ok(new_sst) => {
                Self::persist_sstable_timestamp_cache_entry(
                    &self.paths.sstable_dir,
                    &new_sst.path,
                    new_sst.id,
                    max_output_ts,
                );
                Self::persist_sstable_descriptor_cache_entry(&self.paths.sstable_dir, &new_sst);
                let new_sst_path = new_sst.path.clone();
                let candidate_ids = candidates.each_ref().map(|candidate| candidate.id);
                if let Err(error) = self.install_compacted_sstable(new_sst, &candidate_ids) {
                    let _ = tokio::fs::remove_file(&new_sst_path).await;
                    SsTable::remove_index_cache_file_for_path(&new_sst_path).await;
                    SsTable::remove_reverse_seek_file_for_path(&new_sst_path).await;
                    return Err(crate::common::FusionError::Storage(format!(
                        "Compaction failed to persist SSTable manifest: {}",
                        error
                    )));
                }

                {
                    let mut obsolete = self.obsolete_sstables.write().unwrap();
                    for sst in candidates {
                        obsolete.push(sst);
                    }
                }
                self.collect_obsolete_sstables().await;
                crate::monitor::inc_compaction_run();
                crate::monitor::add_compaction_input_bytes(compaction_input_bytes);
                crate::monitor::add_compaction_output_bytes(compaction_output_bytes);
                crate::monitor::add_compaction_dropped_versions(dropped_version_count);

                Ok(true)
            }
            Err(e) => {
                let _ = tokio::fs::remove_file(&out_path).await;
                SsTable::remove_index_cache_file_for_path(&out_path).await;
                SsTable::remove_reverse_seek_file_for_path(&out_path).await;
                Err(crate::common::FusionError::Storage(format!(
                    "Failed to open compacted SST: {:?}",
                    e
                )))
            }
        }
    }

    pub async fn compact_now(&self) -> Result<bool> {
        self.create_snapshot_now().await?;
        self.compact_once().await
    }

    async fn compaction_loop(&self) {
        loop {
            // Check every 1 second
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            if let Err(e) = self.compact_once().await {
                eprintln!("Compaction error: {:?}", e);
            }
        }
    }

    /// Graceful shutdown: flush active MemTable to SSTable, save indexes, sync WAL.
    pub async fn shutdown(&self) {
        println!("[shutdown] Flushing active MemTable...");
        let _commit_guard = self.commit_lock.lock().await;

        self.rotate_memtable().await;
        match self.flush_all_immutable_memtables().await {
            Ok(()) => {
                self.persist_secondary_indexes("[shutdown]");
                if let Err(error) = self.wal.truncate() {
                    eprintln!("[shutdown] Failed to truncate WAL: {error}");
                }
            }
            Err(error) => {
                eprintln!("[shutdown] Flush failed; WAL and unflushed MemTable retained: {error}");
            }
        }

        println!("[shutdown] FusionDB shut down cleanly.");
    }

    /// Flush a single MemTable to SSTable (used during shutdown).
    async fn flush_memtable_sync(&self, mem: &MemTable) -> Result<()> {
        if mem.map.is_empty() {
            return Ok(());
        }

        let sst_path = self.sstable_path_for(mem.id);
        if self
            .sstables
            .read()
            .unwrap()
            .iter()
            .any(|sstable| sstable.id == mem.id)
        {
            return Ok(());
        }

        let max_ts = mem
            .map
            .iter()
            .filter(|entry| entry.key().len() >= TS_SIZE)
            .map(|entry| Self::decode_key(entry.key()).1)
            .max()
            .ok_or_else(|| {
                FusionError::Storage(format!(
                    "MemTable {} contains no valid MVCC entries",
                    mem.id
                ))
            })?;

        // A prior manifest update may have failed after the complete SSTable was renamed. Reuse
        // that durable file instead of replacing a file that recovery might already reference.
        if sst_path.exists() {
            let reusable =
                match SsTable::open(sst_path.clone(), mem.id, self.block_cache.clone()).await {
                    Ok(sstable) => match Self::sstable_matches_memtable(&sstable, mem).await {
                        Ok(true) => Some(sstable),
                        Ok(false) | Err(_) => None,
                    },
                    Err(_) => None,
                };
            if let Some(sstable) = reusable {
                Self::persist_sstable_timestamp_cache_entry(
                    &self.paths.sstable_dir,
                    &sstable.path,
                    sstable.id,
                    max_ts,
                );
                Self::persist_sstable_descriptor_cache_entry(&self.paths.sstable_dir, &sstable);
                self.register_live_sstable(sstable).map_err(|error| {
                    FusionError::Storage(format!(
                        "failed to persist SSTable {} in manifest: {}",
                        mem.id, error
                    ))
                })?;
                return Ok(());
            }
            tokio::fs::remove_file(&sst_path).await?;
            SsTable::remove_index_cache_file_for_path(&sst_path).await;
            SsTable::remove_reverse_seek_file_for_path(&sst_path).await;
        }

        let staging_path = self.sstable_staging_path_for(mem.id);
        let _ = tokio::fs::remove_file(&staging_path).await;
        SsTable::remove_index_cache_file_for_path(&staging_path).await;
        SsTable::remove_reverse_seek_file_for_path(&staging_path).await;
        let mut builder = self
            .sstable_builder_with_zone_maps(staging_path.clone(), mem.map.len())
            .await;

        let mut block_count = 0;
        let mut block_buffer = Vec::with_capacity(SSTABLE_BLOCK_BUFFER_CAPACITY);
        let mut first_key = None;

        for entry in mem.map.iter() {
            let key = entry.key();
            let val = entry.value();
            if key.len() < TS_SIZE {
                continue;
            }
            if first_key.is_none() {
                first_key = Some(key.clone());
            }
            builder.add_key(key);
            block_buffer.extend_from_slice(&(key.len() as u32).to_le_bytes());
            block_buffer.extend_from_slice(key);
            block_buffer.extend_from_slice(&(val.len() as u32).to_le_bytes());
            block_buffer.extend_from_slice(val);
            block_count += 1;

            if block_buffer.len() >= SSTABLE_BLOCK_BUFFER_CAPACITY {
                builder
                    .flush_block(first_key.take().unwrap(), block_count, &block_buffer)
                    .await?;
                block_buffer.clear();
                block_count = 0;
            }
        }

        if !block_buffer.is_empty() {
            if let Some(fk) = first_key.take() {
                builder.flush_block(fk, block_count, &block_buffer).await?;
            }
        }

        builder.finish().await?;
        self.publish_staged_sstable(&staging_path, &sst_path)
            .await?;

        let sstable = match SsTable::open(sst_path.clone(), mem.id, self.block_cache.clone()).await
        {
            Ok(sstable) => sstable,
            Err(error) => {
                let _ = tokio::fs::remove_file(&sst_path).await;
                SsTable::remove_index_cache_file_for_path(&sst_path).await;
                SsTable::remove_reverse_seek_file_for_path(&sst_path).await;
                return Err(error);
            }
        };
        Self::persist_sstable_timestamp_cache_entry(
            &self.paths.sstable_dir,
            &sstable.path,
            sstable.id,
            max_ts,
        );
        Self::persist_sstable_descriptor_cache_entry(&self.paths.sstable_dir, &sstable);
        self.register_live_sstable(sstable).map_err(|error| {
            FusionError::Storage(format!(
                "failed to persist SSTable {} in manifest: {}",
                mem.id, error
            ))
        })?;
        Ok(())
    }

    async fn rotate_memtable(&self) {
        let mut active = self.active_memtable.write().unwrap();
        let mut imm = self.immutable_memtables.write().unwrap();

        let new_id = self.next_memtable_id.fetch_add(1, Ordering::Relaxed);
        let new_mem = MemTable::new(new_id);

        let old = std::mem::replace(&mut *active, new_mem);

        // Build FBTree for the immutable memtable (Read Optimization)
        old.build_fbtree();

        imm.push(old);
        self.flush_notify.notify_one();
    }
}

/// A deferred side-index mutation (trigram / HNSW). These structures are not
/// part of the OCC write set; buffering them on the transaction and applying
/// only after commit-time validation succeeds keeps aborted transactions from
/// leaving phantom entries in shared in-memory indexes (BENCHPROD-465,
/// InnoDB-FTS-style deferral). Rollback simply drops the buffer.
#[derive(Debug)]
pub enum SideIndexDelta {
    TrigramAdd {
        table: String,
        column: String,
        numeric_id: u64,
        row_id: String,
        text: String,
    },
    TrigramRemove {
        table: String,
        column: String,
        numeric_id: u64,
        text: String,
    },
    VectorInsert {
        index: String,
        id: String,
        vector: Vec<f32>,
    },
    VectorDelete {
        index: String,
        id: String,
    },
}

pub struct FusionTransaction {
    pub storage: FusionStorage,
    pub write_buffer: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    pub read_ts: u64,
    read_ts_registered: bool,
    capture_cdc: AtomicBool,
    side_index_deltas: std::sync::Mutex<Vec<SideIndexDelta>>,
    fenced_migration_phase: Option<FenceSnapshot>,
}

impl FusionTransaction {
    /// Buffer a side-index mutation to be applied only if this transaction
    /// commits (after OCC validation and WAL durability). See SideIndexDelta.
    pub fn defer_side_index_delta(&self, delta: SideIndexDelta) {
        self.side_index_deltas.lock().unwrap().push(delta);
    }

    /// Revalidate the migration-phase pin. Called inside the commit critical
    /// section, where every phase advance also publishes its fence, so a
    /// commit serialized after an advance either carries the new pin or
    /// aborts here — write skew across a phase change is impossible.
    async fn revalidate_migration_fence(&self) -> Result<()> {
        let Some(pin) = self.fenced_migration_phase else {
            return Ok(());
        };
        let current = match self.storage.data_migration_fence.cached() {
            Some(current) => current,
            None => self.storage.reload_data_migration_fence().await?,
        };
        if current != pin {
            return Err(FusionError::Storage(format!(
                "Data V2 migration phase advanced during transaction (fenced '{}' seq {}, now '{}' seq {}); retry",
                pin.phase.name(),
                pin.phase_seq,
                current.phase.name(),
                current.phase_seq
            )));
        }
        Ok(())
    }

    /// Validate any staged phase-record write and return it for publication.
    ///
    /// Three rules, all enforced before the write can become durable: the
    /// record must decode, it must never be deleted (a missing record silently
    /// reverts the cluster to per-process config-flag behavior), and it must
    /// not share a transaction with fenced data writes. The last one is the
    /// atomicity rule: those writes acted on the *old* phase, so committing
    /// them together with the advance would publish rows that silently violate
    /// the new phase's contract (for example unshadowed rows under
    /// `write-delete-shadow`).
    fn staged_migration_phase_record(&self) -> Result<Option<DataMigrationPhaseRecord>> {
        let mut staged = None;
        for (user_key, value) in &self.write_buffer {
            if user_key.as_slice() != migration_phase_key() {
                continue;
            }
            let Some(bytes) = value else {
                return Err(FusionError::Storage(
                    "the Data V2 migration phase record must never be deleted".to_string(),
                ));
            };
            staged = Some(DataMigrationPhaseRecord::decode(bytes)?);
        }
        if staged.is_some() && self.fenced_migration_phase.is_some() {
            return Err(FusionError::Storage(
                "a Data V2 migration phase advance must not share a transaction with data writes; run it as its own transaction"
                    .to_string(),
            ));
        }
        Ok(staged)
    }

    /// Raft replicates the exact logical mutation batch. Per-node CDC records
    /// contain local MVCC timestamps, so state-machine apply must not derive
    /// them independently on every replica.
    pub(crate) fn disable_cdc_capture(&self) {
        self.capture_cdc.store(false, Ordering::Release);
    }

    /// Side indexes represent only the latest visibility epoch. An older
    /// snapshot must fall back to its MVCC row scan instead of consulting an
    /// index that has already been updated in place.
    pub(crate) async fn current_side_index_read_guard(&self) -> Option<OwnedRwLockReadGuard<()>> {
        let guard = self
            .storage
            .side_index_visibility
            .clone()
            .read_owned()
            .await;
        (self.read_ts == self.storage.current_ts.load(Ordering::SeqCst)).then_some(guard)
    }

    /// Transfer deferred side-index work to a deterministic Raft mutation
    /// batch before the leader rolls its evaluation transaction back.
    pub(crate) fn take_side_index_deltas(&self) -> Vec<SideIndexDelta> {
        std::mem::take(&mut *self.side_index_deltas.lock().unwrap())
    }

    fn validate_side_index_deltas(&self) -> Result<()> {
        let deltas = self.side_index_deltas.lock().unwrap();
        let mut pending_dimensions = HashMap::new();
        for delta in deltas.iter() {
            let SideIndexDelta::VectorInsert { index, vector, .. } = delta else {
                continue;
            };
            self.storage
                .vector_index
                .validate_insert_dimensions(index, vector.len())?;
            if let Some(expected) = pending_dimensions.insert(index.as_str(), vector.len()) {
                if expected != vector.len() {
                    return Err(FusionError::Execution(format!(
                        "Vector dimension mismatch within transaction for index '{index}': expected {expected}, got {}",
                        vector.len()
                    )));
                }
            }
        }
        Ok(())
    }

    /// Apply the buffered side-index deltas. Called from commit after the
    /// write set is durable and published to the memtable, immediately
    /// before the visibility watermark moves: a concurrent search may see an
    /// index entry for a not-yet-visible row (harmless — index hits are
    /// re-verified against base rows), and within this process a visible row
    /// is never missing its index entries. (Across a crash, trigram postings
    /// remain checkpoint-granular as before: postings committed after the
    /// last checkpoint are not replayed; the vector index self-heals via the
    /// startup rebuild from rows.)
    fn apply_side_index_deltas(&self) -> Result<()> {
        let deltas = std::mem::take(&mut *self.side_index_deltas.lock().unwrap());
        if deltas.is_empty() {
            return Ok(());
        }

        let mut trigram_lock = None;
        for delta in &deltas {
            match delta {
                SideIndexDelta::TrigramAdd {
                    table,
                    column,
                    numeric_id,
                    row_id,
                    text,
                } => {
                    let lock = trigram_lock
                        .get_or_insert_with(|| self.storage.trigram_index.write().unwrap());
                    lock.add_with_id_str(table, column, *numeric_id, row_id, text);
                }
                SideIndexDelta::TrigramRemove {
                    table,
                    column,
                    numeric_id,
                    text,
                } => {
                    let lock = trigram_lock
                        .get_or_insert_with(|| self.storage.trigram_index.write().unwrap());
                    lock.remove_with_id(table, column, *numeric_id, text);
                }
                SideIndexDelta::VectorInsert { .. } | SideIndexDelta::VectorDelete { .. } => {}
            }
        }
        drop(trigram_lock);

        for delta in deltas {
            match delta {
                SideIndexDelta::VectorInsert { index, id, vector } => {
                    self.storage.vector_index.insert(&index, id, vector)?;
                }
                SideIndexDelta::VectorDelete { index, id } => {
                    self.storage.vector_index.delete(&index, &id)?;
                }
                SideIndexDelta::TrigramAdd { .. } | SideIndexDelta::TrigramRemove { .. } => {}
            }
        }
        Ok(())
    }

    /// Snapshot the visible memtables (active + immutable, newest-first) as a cheap Arc clone.
    fn snapshot_memtables(&self) -> Vec<MemTable> {
        let mut mem_tables =
            Vec::with_capacity(self.storage.immutable_memtables.read().unwrap().len() + 1);
        {
            let active = self.storage.active_memtable.read().unwrap();
            mem_tables.push(active.clone());
        }
        {
            let imm = self.storage.immutable_memtables.read().unwrap();
            for mem in imm.iter().rev() {
                mem_tables.push(mem.clone());
            }
        }
        mem_tables
    }

    async fn for_each_visible_range<F>(&self, start: &[u8], end: &[u8], mut visit: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send,
    {
        self.for_each_visible_range_with_options(
            start,
            end,
            StorageScanOptions::fill_cache(),
            &mut visit,
        )
        .await
    }

    async fn for_each_visible_range_with_options<F>(
        &self,
        start: &[u8],
        end: &[u8],
        scan_options: StorageScanOptions,
        mut visit: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send,
    {
        // Snapshot once (cheap Arc clones), then run the shared merge over the full range.
        let mem_tables = self.snapshot_memtables();
        let sstables = self.storage.sstables.read().unwrap().clone();
        Self::merge_visible_range(
            &mem_tables,
            &sstables,
            &self.write_buffer,
            self.read_ts,
            start,
            end,
            scan_options,
            &mut visit,
        )
        .await
    }

    async fn for_each_visible_range_reverse<F>(
        &self,
        start: &[u8],
        end: &[u8],
        mut visit: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send,
    {
        self.for_each_visible_range_reverse_with_options(
            start,
            end,
            StorageScanOptions::fill_cache(),
            &mut visit,
        )
        .await
    }

    async fn for_each_visible_range_reverse_with_options<F>(
        &self,
        start: &[u8],
        end: &[u8],
        scan_options: StorageScanOptions,
        mut visit: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send,
    {
        let mem_tables = self.snapshot_memtables();
        let sstables = self.storage.sstables.read().unwrap().clone();
        Self::merge_visible_range_reverse(
            &mem_tables,
            &sstables,
            &self.write_buffer,
            self.read_ts,
            start,
            end,
            scan_options,
            &mut visit,
        )
        .await
    }

    fn sql_zone_map_table_prefix_for_range(
        plan: &SqlBlockZoneMapPruningPlan,
        start: &[u8],
        end: &[u8],
    ) -> Option<Vec<u8>> {
        if start >= end {
            return None;
        }

        let legacy_prefix = format!("data:{}:", plan.table_name);
        if Self::range_within_prefix(start, end, legacy_prefix.as_bytes()) {
            return Some(legacy_prefix.into_bytes());
        }

        let start_text = std::str::from_utf8(start).ok()?;
        let shard_rest = start_text.strip_prefix("shard:")?;
        let shard_id_end = shard_rest.find(':')?;
        let shard_id = &shard_rest[..shard_id_end];
        let shard_prefix = format!("shard:{shard_id}:data:{}:", plan.table_name);
        Self::range_within_prefix(start, end, shard_prefix.as_bytes())
            .then(|| shard_prefix.into_bytes())
    }

    fn range_within_prefix(start: &[u8], end: &[u8], prefix: &[u8]) -> bool {
        let Some(prefix_end) = FusionStorage::prefix_end(prefix) else {
            return false;
        };
        start >= prefix && end <= prefix_end.as_slice()
    }

    fn write_buffer_overlaps_user_key_interval(
        write_buffer: &[(Vec<u8>, Option<Vec<u8>>)],
        first_user_key: &[u8],
        last_user_key: &[u8],
    ) -> bool {
        write_buffer
            .iter()
            .any(|(key, _)| key.as_slice() >= first_user_key && key.as_slice() <= last_user_key)
    }

    fn memtables_overlap_user_key_interval(
        mem_tables: &[MemTable],
        first_user_key: &[u8],
        last_user_key: &[u8],
    ) -> bool {
        let lower_bound = FusionStorage::encode_key(first_user_key, u64::MAX);
        for mem in mem_tables {
            for entry in mem.map.range(lower_bound.clone()..) {
                let (user_key, _) = FusionStorage::decode_key(entry.key());
                if user_key > last_user_key {
                    break;
                }
                if user_key >= first_user_key {
                    return true;
                }
            }
        }
        false
    }

    fn sstable_overlaps_user_key_interval(
        sstable: &SsTable,
        first_user_key: &[u8],
        last_user_key: &[u8],
    ) -> bool {
        let (sst_first_user_key, _) = FusionStorage::decode_key(&sstable.meta.first_key);
        let (sst_last_user_key, _) = FusionStorage::decode_key(&sstable.meta.last_key);
        !(sst_last_user_key < first_user_key || sst_first_user_key > last_user_key)
    }

    fn sstable_table_blocks_overlap_user_key_interval(
        sstable: &SsTable,
        table_prefix: &[u8],
        first_user_key: &[u8],
        last_user_key: &[u8],
    ) -> Option<bool> {
        let block_properties = sstable.validated_block_properties_for_zone_maps()?;

        for property in block_properties.iter() {
            let Some(interval) =
                SsTable::block_property_table_prefix_interval(property, table_prefix)
            else {
                return None;
            };
            let Some((block_first_user_key, block_last_user_key)) = interval else {
                continue;
            };
            if !(block_last_user_key.as_slice() < first_user_key
                || block_first_user_key.as_slice() > last_user_key)
            {
                return Some(true);
            }
        }

        Some(false)
    }

    fn other_sstable_overlaps_user_key_interval(
        current_sstable: &Arc<SsTable>,
        sstables: &[Arc<SsTable>],
        table_prefix: &[u8],
        first_user_key: &[u8],
        last_user_key: &[u8],
    ) -> bool {
        sstables.iter().any(|sstable| {
            if Arc::ptr_eq(sstable, current_sstable)
                || !Self::sstable_overlaps_user_key_interval(sstable, first_user_key, last_user_key)
            {
                return false;
            }

            if let Some(overlaps) = Self::sstable_table_blocks_overlap_user_key_interval(
                sstable,
                table_prefix,
                first_user_key,
                last_user_key,
            ) {
                return overlaps;
            }

            !matches!(
                sstable.probe_user_key_prefix_filter(table_prefix),
                SsTablePrefixFilterProbe::NoMatch
            )
        })
    }

    fn block_has_split_user_key_boundary(
        block_properties: &[SsTableBlockProperties],
        block_idx: usize,
        table_prefix: &[u8],
        first_user_key: &[u8],
        last_user_key: &[u8],
    ) -> bool {
        if block_idx > 0 {
            match SsTable::block_property_table_prefix_interval(
                &block_properties[block_idx - 1],
                table_prefix,
            ) {
                Some(Some((_prev_first, prev_last))) => {
                    if prev_last == first_user_key {
                        return true;
                    }
                }
                Some(None) => {}
                None => return true,
            }
        }

        if let Some(next_property) = block_properties.get(block_idx + 1) {
            match SsTable::block_property_table_prefix_interval(next_property, table_prefix) {
                Some(Some((next_first, _next_last))) => {
                    if next_first == last_user_key {
                        return true;
                    }
                }
                Some(None) => {}
                None => return true,
            }
        }

        false
    }

    fn record_sql_zone_map_fail_open(reason: SqlBlockZoneMapFailOpenReason) {
        crate::monitor::inc_sstable_block_zone_map_filter_fail_open();
        if matches!(
            reason,
            SqlBlockZoneMapFailOpenReason::MissingColumn
                | SqlBlockZoneMapFailOpenReason::SchemaMismatch
                | SqlBlockZoneMapFailOpenReason::ColumnMismatch
                | SqlBlockZoneMapFailOpenReason::TypeMismatch
                | SqlBlockZoneMapFailOpenReason::UnsupportedValueEncoding
        ) {
            crate::monitor::inc_sstable_block_zone_map_schema_fail_open();
        }
    }

    fn record_sql_zone_map_mvcc_fail_open(reason: SqlBlockZoneMapMvccFailOpenReason) {
        crate::monitor::inc_sstable_block_zone_map_filter_fail_open();
        crate::monitor::inc_sstable_block_zone_map_mvcc_overlap_fail_open();
        match reason {
            SqlBlockZoneMapMvccFailOpenReason::BoundarySplit => {
                crate::monitor::inc_sstable_block_zone_map_mvcc_boundary_split_fail_open();
            }
            SqlBlockZoneMapMvccFailOpenReason::WriteBufferOverlap => {
                crate::monitor::inc_sstable_block_zone_map_mvcc_write_buffer_overlap_fail_open();
            }
            SqlBlockZoneMapMvccFailOpenReason::MemtableOverlap => {
                crate::monitor::inc_sstable_block_zone_map_mvcc_memtable_overlap_fail_open();
            }
            SqlBlockZoneMapMvccFailOpenReason::SstableOverlap => {
                crate::monitor::inc_sstable_block_zone_map_mvcc_sstable_overlap_fail_open();
            }
        }
    }

    fn sql_zone_map_skip_offsets_for_sstable(
        plan: &SqlBlockZoneMapPruningPlan,
        table_prefix: &[u8],
        sstable: &Arc<SsTable>,
        sstables: &[Arc<SsTable>],
        mem_tables: &[MemTable],
        write_buffer: &[(Vec<u8>, Option<Vec<u8>>)],
        start: &[u8],
        end: &[u8],
    ) -> Option<Arc<BTreeSet<u64>>> {
        let block_properties = sstable.validated_block_properties_for_zone_maps()?;
        let mut skip_offsets = BTreeSet::new();

        for (block_idx, property) in block_properties.iter().enumerate() {
            let (first_user_key, last_user_key) =
                match SsTable::block_property_table_prefix_interval(property, table_prefix) {
                    Some(Some(interval)) => interval,
                    Some(None) => continue,
                    None => {
                        let Some((first_user_key, last_user_key)) =
                            SsTable::block_property_user_key_interval(property, TS_SIZE)
                        else {
                            crate::monitor::inc_sstable_block_zone_map_filter_check();
                            Self::record_sql_zone_map_fail_open(
                                SqlBlockZoneMapFailOpenReason::IncompleteMetadata,
                            );
                            continue;
                        };
                        if last_user_key.as_slice() < start || first_user_key.as_slice() >= end {
                            continue;
                        }
                        crate::monitor::inc_sstable_block_zone_map_filter_check();
                        Self::record_sql_zone_map_fail_open(
                            SqlBlockZoneMapFailOpenReason::IncompleteMetadata,
                        );
                        continue;
                    }
                };

            // Blocks entirely outside [start, end) are simply irrelevant to
            // this (sub-)scan — not a metadata failure. Parallel full scans
            // split the keyspace into K slices; without this test every
            // slice paid an interval clone + a bogus IncompleteMetadata
            // fail-open for the other slices' blocks (exactly (K-1)/K of
            // all evaluations, BENCHPROD-470).
            if last_user_key.as_slice() < start || first_user_key.as_slice() >= end {
                continue;
            }

            crate::monitor::inc_sstable_block_zone_map_filter_check();

            if first_user_key.as_slice() < start || last_user_key.as_slice() >= end {
                // Partially overlapping the range boundary: pruning must not
                // trust a decision that spans beyond the validated range.
                Self::record_sql_zone_map_fail_open(
                    SqlBlockZoneMapFailOpenReason::IncompleteMetadata,
                );
                continue;
            }

            let mvcc_fail_open_reason = if Self::block_has_split_user_key_boundary(
                block_properties.as_ref(),
                block_idx,
                table_prefix,
                &first_user_key,
                &last_user_key,
            ) {
                Some(SqlBlockZoneMapMvccFailOpenReason::BoundarySplit)
            } else if Self::write_buffer_overlaps_user_key_interval(
                write_buffer,
                &first_user_key,
                &last_user_key,
            ) {
                Some(SqlBlockZoneMapMvccFailOpenReason::WriteBufferOverlap)
            } else if Self::memtables_overlap_user_key_interval(
                mem_tables,
                &first_user_key,
                &last_user_key,
            ) {
                Some(SqlBlockZoneMapMvccFailOpenReason::MemtableOverlap)
            } else if Self::other_sstable_overlaps_user_key_interval(
                sstable,
                sstables,
                table_prefix,
                &first_user_key,
                &last_user_key,
            ) {
                Some(SqlBlockZoneMapMvccFailOpenReason::SstableOverlap)
            } else {
                None
            };

            if let Some(reason) = mvcc_fail_open_reason {
                Self::record_sql_zone_map_mvcc_fail_open(reason);
                continue;
            }

            match plan.evaluate_block_zone_maps(
                table_prefix,
                property.sql_zone_maps_complete,
                &property.sql_zone_maps,
            ) {
                SqlBlockZoneMapPruningDecision::SkipBlock => {
                    crate::monitor::inc_sstable_block_zone_map_filter_skip();
                    skip_offsets.insert(property.offset);
                }
                SqlBlockZoneMapPruningDecision::ReadBlock => {
                    crate::monitor::inc_sstable_block_zone_map_filter_positive();
                }
                SqlBlockZoneMapPruningDecision::FailOpen(reason) => {
                    Self::record_sql_zone_map_fail_open(reason);
                }
            }
        }

        (!skip_offsets.is_empty()).then(|| Arc::new(skip_offsets))
    }

    /// Core N-way MVCC merge over an owned snapshot (memtables + sstables + write buffer) for the
    /// range `[start, end)`. It takes borrowed snapshot pieces (not `&self`) so a single consistent
    /// snapshot can be shared across several sub-range merges running on spawned tasks (see
    /// `scan_range_parallel`). Invokes `visit(user_key, value)` for each latest visible PUT in key
    /// order and stops early when `visit` returns false.
    async fn merge_visible_range(
        mem_tables: &[MemTable],
        sstables: &[Arc<SsTable>],
        write_buffer: &[(Vec<u8>, Option<Vec<u8>>)],
        read_ts: u64,
        start: &[u8],
        end: &[u8],
        scan_options: StorageScanOptions,
        visit: &mut (dyn FnMut(&[u8], &[u8]) -> bool + Send),
    ) -> Result<()> {
        let start_ik = FusionStorage::encode_key(start, u64::MAX);
        let read_options = sstable_read_options(&scan_options);
        let sql_block_zone_map_pruning_plan = scan_options
            .sql_block_zone_map_pruning_enabled()
            .then(|| scan_options.sql_block_zone_map_pruning_plan.as_deref())
            .flatten();
        let sql_zone_map_table_prefix = sql_block_zone_map_pruning_plan
            .and_then(|plan| Self::sql_zone_map_table_prefix_for_range(plan, start, end));
        if sql_block_zone_map_pruning_plan.is_some() && sql_zone_map_table_prefix.is_some() {
            for sstable in sstables {
                sstable.preload_block_properties().await;
            }
        }

        // WriteBuffer
        let mut wb_latest = BTreeMap::new();
        for (k, v) in write_buffer {
            if k.as_slice() >= start && k.as_slice() < end {
                wb_latest.insert(k.clone(), v.clone());
            }
        }
        let mut wb_iter = wb_latest.into_iter().map(|(k, v)| {
            let ik = FusionStorage::encode_key(&k, u64::MAX);
            let iv = match v {
                Some(val) => FusionStorage::encode_value(true, &val),
                None => FusionStorage::encode_value(false, &[]),
            };
            (ik, iv)
        });

        // 2. Initialize Heap
        let mut heap: BinaryHeap<VisibleMergeItem> =
            BinaryHeap::with_capacity(1 + mem_tables.len() + sstables.len());

        if let Some((k, v)) = wb_iter.next() {
            heap.push(VisibleMergeItem {
                entry: VisibleEntry::Owned(k, v),
                iter_idx: 0,
            });
        }

        // Helper Type for Iterators
        type BoxedIter<'a> = Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send + 'a>;

        let mut mem_iters: Vec<BoxedIter> = Vec::with_capacity(mem_tables.len());

        // 2. Create Iterators
        for (i, mem) in mem_tables.iter().enumerate() {
            // Use the SkipMap as the source of truth for range scans. The optional FBTree is
            // a read optimization, but its approximate child descent can skip keys under long
            // mixed soak runs; correctness for MVCC visibility depends on complete iteration.
            let mut iter = Box::new(
                mem.map
                    .range(start_ik.clone()..)
                    .map(|e| (e.key().clone(), e.value().clone())),
            ) as BoxedIter;
            if let Some((k, v)) = iter.next() {
                let (user_k, _) = FusionStorage::decode_key(&k);
                if user_k < end {
                    heap.push(VisibleMergeItem {
                        entry: VisibleEntry::Owned(k, v),
                        iter_idx: 1 + i,
                    });
                }
                mem_iters.push(iter);
            } else {
                mem_iters.push(iter);
            }
        }

        let mut sst_iters: Vec<Option<crate::storage::sstable::SsTableIterator>> =
            Vec::with_capacity(sstables.len());
        let prefix_filter_probe = FusionStorage::prefix_end(start)
            .filter(|prefix_end| prefix_end.as_slice() == end)
            .map(|_| start);
        let sql_index_prefix_filter_probe =
            crate::storage::sstable::SsTable::sql_index_prefix_for_range(start, end);
        for (i, sst) in sstables.iter().rev().enumerate() {
            let idx = 1 + mem_tables.len() + i;
            crate::monitor::inc_sstable_range_probe();

            // Check Overlap
            let sst_min = &sst.meta.first_key;
            let sst_max = &sst.meta.last_key;
            let (sst_min_user_key, _) = FusionStorage::decode_key(sst_min);
            let (sst_max_user_key, _) = FusionStorage::decode_key(sst_max);
            if sst_max_user_key < start || sst_min_user_key >= end {
                crate::monitor::inc_sstable_range_overlap_skip();
                sst_iters.push(None);
                continue;
            }
            if let Some(prefix) = prefix_filter_probe {
                crate::monitor::inc_sstable_prefix_filter_check();
                match sst.probe_user_key_prefix_filter(prefix) {
                    SsTablePrefixFilterProbe::MayMatch => {
                        crate::monitor::inc_sstable_prefix_filter_positive();
                    }
                    SsTablePrefixFilterProbe::NoMatch => {
                        crate::monitor::inc_sstable_prefix_filter_skip();
                        sst_iters.push(None);
                        continue;
                    }
                    SsTablePrefixFilterProbe::FailOpen => {
                        crate::monitor::inc_sstable_prefix_filter_fail_open();
                    }
                }
            }
            if let Some(prefix) = sql_index_prefix_filter_probe.as_deref() {
                crate::monitor::inc_sstable_index_prefix_filter_check();
                match sst.probe_sql_index_prefix_filter(prefix) {
                    SsTablePrefixFilterProbe::MayMatch => {
                        crate::monitor::inc_sstable_index_prefix_filter_positive();
                    }
                    SsTablePrefixFilterProbe::NoMatch => {
                        crate::monitor::inc_sstable_index_prefix_filter_skip();
                        sst_iters.push(None);
                        continue;
                    }
                    SsTablePrefixFilterProbe::FailOpen => {
                        crate::monitor::inc_sstable_index_prefix_filter_fail_open();
                    }
                }
            }

            // Use seek/range optimization to jump to start key and avoid reading blocks that
            // start at or beyond the exclusive range end.
            let approved_block_skip_offsets = sql_block_zone_map_pruning_plan
                .zip(sql_zone_map_table_prefix.as_deref())
                .and_then(|(plan, table_prefix)| {
                    Self::sql_zone_map_skip_offsets_for_sstable(
                        plan,
                        table_prefix,
                        sst,
                        sstables,
                        mem_tables,
                        write_buffer,
                        start,
                        end,
                    )
                });
            let mut it = sst
                .new_user_key_range_iterator_with_options_and_block_skips(
                    Some(&start_ik),
                    Some(end),
                    TS_SIZE,
                    read_options,
                    approved_block_skip_offsets,
                )
                .await?;
            crate::monitor::inc_sstable_iterator_open();
            if let Some(view) = it.next_entry().await? {
                // Check if the first key we found is already past end
                // (This can happen if start_ik is not in SSTable and we landed on a key > end)
                if view.key() >= start_ik.as_slice() {
                    let (uk, _) = FusionStorage::decode_key(view.key());
                    if uk < end {
                        heap.push(VisibleMergeItem {
                            entry: VisibleEntry::Sst(view),
                            iter_idx: idx,
                        });
                    }
                }
            }
            sst_iters.push(Some(it));
        }

        // 3. Merge Loop
        let mut last_user_key: Option<Vec<u8>> = None;

        while let Some(item) = heap.pop() {
            let entry = item.entry;
            let idx = item.iter_idx;

            let (user_k, ts) = FusionStorage::decode_key(entry.key());
            if user_k >= end {
                break;
            }
            let current_visible = idx == 0 || ts <= read_ts;

            // Advance Iterator (must happen before dedup skip)
            let mut next_item = None;

            if idx == 0 {
                while let Some((nk, nv)) = wb_iter.next() {
                    let (nuk, _nts) = FusionStorage::decode_key(&nk);
                    if nuk == user_k && current_visible {
                        continue;
                    }
                    next_item = Some((nk, nv));
                    break;
                }
                if let Some((nk, nv)) = next_item {
                    heap.push(VisibleMergeItem {
                        entry: VisibleEntry::Owned(nk, nv),
                        iter_idx: 0,
                    });
                }
            } else if idx <= mem_tables.len() {
                let mem_idx = idx - 1;
                while let Some((nk, nv)) = mem_iters[mem_idx].next() {
                    let (nuk, _nts) = FusionStorage::decode_key(&nk);
                    if nuk >= end {
                        next_item = None;
                        break;
                    }
                    if nuk == user_k && current_visible {
                        continue;
                    }
                    next_item = Some((nk, nv));
                    break;
                }
                if let Some((nk, nv)) = next_item {
                    heap.push(VisibleMergeItem {
                        entry: VisibleEntry::Owned(nk, nv),
                        iter_idx: idx,
                    });
                }
            } else {
                let sst_idx = idx - 1 - mem_tables.len();
                if let Some(it) = &mut sst_iters[sst_idx] {
                    let mut next_view = None;
                    while let Some(view) = it.next_entry().await? {
                        let (nuk, _nts) = FusionStorage::decode_key(view.key());
                        if nuk >= end {
                            break;
                        }
                        if nuk == user_k && current_visible {
                            continue;
                        }
                        next_view = Some(view);
                        break;
                    }
                    if let Some(view) = next_view {
                        heap.push(VisibleMergeItem {
                            entry: VisibleEntry::Sst(view),
                            iter_idx: idx,
                        });
                    }
                }
            }

            // Dedup: skip if same user key as last processed
            if let Some(ref last_uk) = last_user_key {
                if last_uk.as_slice() == user_k {
                    continue;
                }
            }

            if current_visible {
                last_user_key = Some(user_k.to_vec());
                let (is_put, val) = FusionStorage::decode_value(entry.val());
                if is_put && !visit(user_k, val) {
                    break;
                }
            }
        }

        Ok(())
    }

    /// Reverse counterpart of `merge_visible_range` for `[start, end)`.
    /// Each source yields at most one latest-visible candidate per user key; the global merge then
    /// resolves write-buffer priority, newest MVCC timestamp, tombstones, and output limit order.
    async fn merge_visible_range_reverse(
        mem_tables: &[MemTable],
        sstables: &[Arc<SsTable>],
        write_buffer: &[(Vec<u8>, Option<Vec<u8>>)],
        read_ts: u64,
        start: &[u8],
        end: &[u8],
        scan_options: StorageScanOptions,
        visit: &mut (dyn FnMut(&[u8], &[u8]) -> bool + Send),
    ) -> Result<()> {
        let read_options = sstable_read_options(&scan_options);

        if start >= end {
            return Ok(());
        }
        crate::monitor::inc_fusion_reverse_scan();

        let start_ik = FusionStorage::encode_key(start, u64::MAX);
        let mut sources = Vec::with_capacity(1 + mem_tables.len() + sstables.len());
        let mut current: Vec<Option<ReverseCandidate>> =
            Vec::with_capacity(1 + mem_tables.len() + sstables.len());
        let mut heap = BinaryHeap::with_capacity(1 + mem_tables.len() + sstables.len());
        let mut pending_sstables = BinaryHeap::with_capacity(sstables.len());
        let mut next_source_order = 0usize;

        let mut wb_latest = BTreeMap::new();
        for (k, v) in write_buffer {
            if k.as_slice() >= start && k.as_slice() < end {
                wb_latest.insert(k.clone(), v.clone());
            }
        }
        if !wb_latest.is_empty() {
            let entries = wb_latest.into_iter().rev().map(|(k, v)| {
                let ik = FusionStorage::encode_key(&k, u64::MAX);
                let iv = match v {
                    Some(val) => FusionStorage::encode_value(true, &val),
                    None => FusionStorage::encode_value(false, &[]),
                };
                (ik, iv)
            });
            add_reverse_source(
                &mut sources,
                &mut current,
                &mut heap,
                read_ts,
                ReverseSource::Buffered {
                    entries: Box::new(entries),
                    pending: None,
                    is_write_buffer: true,
                    source_order: next_source_order,
                },
            )
            .await?;
            next_source_order += 1;
        }

        for mem in mem_tables {
            if mem.map.is_empty() {
                continue;
            }
            let entries = mem.map.range(start_ik.clone()..).rev().filter_map(|entry| {
                let (user_key, _) = FusionStorage::decode_key(entry.key());
                if user_key < end {
                    Some((entry.key().clone(), entry.value().clone()))
                } else {
                    None
                }
            });
            add_reverse_source(
                &mut sources,
                &mut current,
                &mut heap,
                read_ts,
                ReverseSource::Buffered {
                    entries: Box::new(entries),
                    pending: None,
                    is_write_buffer: false,
                    source_order: next_source_order,
                },
            )
            .await?;
            next_source_order += 1;
        }

        let prefix_filter_probe = FusionStorage::prefix_end(start)
            .filter(|prefix_end| prefix_end.as_slice() == end)
            .map(|_| start);
        let sql_index_prefix_filter_probe =
            crate::storage::sstable::SsTable::sql_index_prefix_for_range(start, end);
        for sst in sstables.iter().rev() {
            crate::monitor::inc_sstable_range_probe();

            let (sst_min_user_key, _) = FusionStorage::decode_key(&sst.meta.first_key);
            let (sst_max_user_key, _) = FusionStorage::decode_key(&sst.meta.last_key);
            if sst_max_user_key < start || sst_min_user_key >= end {
                crate::monitor::inc_sstable_range_overlap_skip();
                continue;
            }
            if let Some(prefix) = prefix_filter_probe {
                crate::monitor::inc_sstable_prefix_filter_check();
                match sst.probe_user_key_prefix_filter(prefix) {
                    SsTablePrefixFilterProbe::MayMatch => {
                        crate::monitor::inc_sstable_prefix_filter_positive();
                    }
                    SsTablePrefixFilterProbe::NoMatch => {
                        crate::monitor::inc_sstable_prefix_filter_skip();
                        continue;
                    }
                    SsTablePrefixFilterProbe::FailOpen => {
                        crate::monitor::inc_sstable_prefix_filter_fail_open();
                    }
                }
            }
            if let Some(prefix) = sql_index_prefix_filter_probe.as_deref() {
                crate::monitor::inc_sstable_index_prefix_filter_check();
                match sst.probe_sql_index_prefix_filter(prefix) {
                    SsTablePrefixFilterProbe::MayMatch => {
                        crate::monitor::inc_sstable_index_prefix_filter_positive();
                    }
                    SsTablePrefixFilterProbe::NoMatch => {
                        crate::monitor::inc_sstable_index_prefix_filter_skip();
                        continue;
                    }
                    SsTablePrefixFilterProbe::FailOpen => {
                        crate::monitor::inc_sstable_index_prefix_filter_fail_open();
                    }
                }
            }

            crate::monitor::inc_fusion_reverse_sstable_frontier_probe();
            let Some(frontier) = sst.reverse_frontier_for_range(start, end, TS_SIZE) else {
                crate::monitor::inc_fusion_reverse_sstable_frontier_empty_skip();
                crate::monitor::inc_sstable_range_overlap_skip();
                continue;
            };
            match frontier.kind {
                SsTableReverseFrontierKind::BlockProperty => {
                    crate::monitor::inc_fusion_reverse_sstable_frontier_in_range();
                }
                SsTableReverseFrontierKind::FileFallback => {
                    crate::monitor::inc_fusion_reverse_sstable_frontier_file();
                    crate::monitor::inc_fusion_reverse_sstable_frontier_fail_open();
                }
            }
            if frontier.user_key.as_slice() < sst_max_user_key {
                crate::monitor::inc_fusion_reverse_sstable_frontier_tighten();
            }

            pending_sstables.push(PendingReverseSstable {
                frontier_user_key: frontier.user_key,
                source_order: next_source_order,
                sst: Arc::clone(sst),
            });
            crate::monitor::inc_fusion_reverse_sstable_pending();
            next_source_order += 1;
        }

        activate_pending_reverse_sstables(
            &mut pending_sstables,
            &mut sources,
            &mut current,
            &mut heap,
            read_ts,
            start,
            end,
            read_options,
        )
        .await?;

        while !heap.is_empty() || !pending_sstables.is_empty() {
            activate_pending_reverse_sstables(
                &mut pending_sstables,
                &mut sources,
                &mut current,
                &mut heap,
                read_ts,
                start,
                end,
                read_options,
            )
            .await?;

            let Some(item) = heap.pop() else {
                break;
            };
            let user_key = item.user_key;
            let mut source_indices = vec![item.source_idx];
            while heap
                .peek()
                .is_some_and(|next| next.user_key.as_slice() == user_key.as_slice())
            {
                source_indices.push(heap.pop().expect("peeked item exists").source_idx);
            }

            let mut winner: Option<ReverseCandidate> = None;
            for source_idx in &source_indices {
                let Some(candidate) = current[*source_idx].take() else {
                    continue;
                };
                if winner
                    .as_ref()
                    .map_or(true, |current| candidate.wins_over(current))
                {
                    winner = Some(candidate);
                }
            }

            for source_idx in source_indices {
                if let Some(candidate) = sources[source_idx].next_candidate(read_ts).await? {
                    heap.push(ReverseMergeItem {
                        user_key: candidate.user_key().to_vec(),
                        source_idx,
                    });
                    current[source_idx] = Some(candidate);
                }
            }

            if let Some(winner) = winner {
                if winner.is_put {
                    crate::monitor::inc_fusion_reverse_visible_put();
                    // Winner bytes are borrowed straight from the candidate
                    // (for SSTable sources: from the cached block); the
                    // visitor copies at its own boundary if it retains.
                    if !visit(winner.user_key(), winner.value()) {
                        break;
                    }
                }
            }
        }

        crate::monitor::add_fusion_reverse_sstable_deferred_unopened(pending_sstables.len() as u64);

        Ok(())
    }

    /// Parallel equivalent of an unbounded `scan_range` over `[start, end)`: splits the range into
    /// disjoint integer-primary-key sub-ranges and merges them on spawned tasks over one shared
    /// snapshot, then concatenates in key order. Disjoint sub-ranges + a single shared snapshot +
    /// one `read_ts` make the result identical to the serial scan (no cross-boundary dedup needed).
    /// Falls back to the serial scan when the key space is not integer-PK or is below the threshold.
    async fn scan_range_parallel(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_range_parallel_with_options(start, end, StorageScanOptions::fill_cache())
            .await
    }

    async fn scan_range_parallel_with_options(
        &self,
        start: &[u8],
        end: &[u8],
        scan_options: StorageScanOptions,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let Some(splits) = self.integer_pk_range_splits(start, end).await? else {
            let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            self.for_each_visible_range_with_options(start, end, scan_options, |user_k, val| {
                rows.push((user_k.to_vec(), val.to_vec()));
                true
            })
            .await?;
            return Ok(rows);
        };

        // One consistent snapshot shared (Arc) across all sub-range merges.
        let mem_tables = Arc::new(self.snapshot_memtables());
        let sstables = Arc::new(self.storage.sstables.read().unwrap().clone());
        let write_buffer = Arc::new(self.write_buffer.clone());
        let read_ts = self.read_ts;

        // Bounds: [start, splits[0]), [splits[0], splits[1]), ..., [splits[last], end).
        let mut bounds: Vec<Vec<u8>> = Vec::with_capacity(splits.len() + 2);
        bounds.push(start.to_vec());
        bounds.extend(splits);
        bounds.push(end.to_vec());

        let mut handles = Vec::with_capacity(bounds.len() - 1);
        for pair in bounds.windows(2) {
            let sub_start = pair[0].clone();
            let sub_end = pair[1].clone();
            let mem_tables = mem_tables.clone();
            let sstables = sstables.clone();
            let write_buffer = write_buffer.clone();
            let scan_options = scan_options.clone();
            handles.push(tokio::spawn(async move {
                let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                let mut visit = |user_k: &[u8], val: &[u8]| {
                    rows.push((user_k.to_vec(), val.to_vec()));
                    true
                };
                FusionTransaction::merge_visible_range(
                    mem_tables.as_slice(),
                    sstables.as_slice(),
                    write_buffer.as_slice(),
                    read_ts,
                    &sub_start,
                    &sub_end,
                    scan_options,
                    &mut visit,
                )
                .await?;
                Ok::<Vec<(Vec<u8>, Vec<u8>)>, FusionError>(rows)
            }));
        }

        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for handle in handles {
            let part = handle.await.map_err(|e| {
                FusionError::Storage(format!("parallel scan task panicked: {}", e))
            })??;
            out.extend(part);
        }
        Ok(out)
    }

    async fn scan_range_parallel_for_each(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
    ) -> Result<Option<usize>> {
        self.scan_range_parallel_for_each_with_options(
            start,
            end,
            limit,
            visitor,
            StorageScanOptions::fill_cache(),
        )
        .await
    }

    async fn scan_range_parallel_for_each_with_options(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
        scan_options: StorageScanOptions,
    ) -> Result<Option<usize>> {
        let safe_limit = limit.unwrap_or(usize::MAX);
        if safe_limit == 0 {
            return Ok(Some(0));
        }

        let Some(splits) = self.integer_pk_range_splits(start, end).await? else {
            return Ok(None);
        };

        let mem_tables = Arc::new(self.snapshot_memtables());
        let sstables = Arc::new(self.storage.sstables.read().unwrap().clone());
        let write_buffer = Arc::new(self.write_buffer.clone());
        let read_ts = self.read_ts;
        let stop = Arc::new(AtomicBool::new(false));

        let mut bounds: Vec<Vec<u8>> = Vec::with_capacity(splits.len() + 2);
        bounds.push(start.to_vec());
        bounds.extend(splits);
        bounds.push(end.to_vec());

        let mut partitions = Vec::with_capacity(bounds.len() - 1);
        for pair in bounds.windows(2) {
            let sub_start = pair[0].clone();
            let sub_end = pair[1].clone();
            let mem_tables = mem_tables.clone();
            let sstables = sstables.clone();
            let write_buffer = write_buffer.clone();
            let stop = stop.clone();
            let scan_options = scan_options.clone();
            let (sender, receiver) = mpsc::unbounded_channel();

            let handle = tokio::spawn(async move {
                let mut visit = |user_k: &[u8], val: &[u8]| {
                    if stop.load(Ordering::Relaxed) {
                        return false;
                    }
                    sender.send((user_k.to_vec(), val.to_vec())).is_ok()
                };
                FusionTransaction::merge_visible_range(
                    mem_tables.as_slice(),
                    sstables.as_slice(),
                    write_buffer.as_slice(),
                    read_ts,
                    &sub_start,
                    &sub_end,
                    scan_options,
                    &mut visit,
                )
                .await
            });
            partitions.push((receiver, handle));
        }

        let mut visited = 0usize;
        let mut stopped_early = false;
        let mut partitions = partitions.into_iter();
        while let Some((mut receiver, handle)) = partitions.next() {
            while let Some((key, value)) = receiver.recv().await {
                visited += 1;
                if !visitor.visit(&key, &value) || visited >= safe_limit {
                    stop.store(true, Ordering::Relaxed);
                    stopped_early = true;
                    break;
                }
            }

            if stopped_early {
                handle.abort();
                match handle.await {
                    Ok(result) => result?,
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => {
                        return Err(FusionError::Storage(format!(
                            "parallel streaming scan task panicked: {}",
                            error
                        )))
                    }
                }
                for (_receiver, handle) in partitions {
                    handle.abort();
                    match handle.await {
                        Ok(result) => result?,
                        Err(error) if error.is_cancelled() => {}
                        Err(error) => {
                            return Err(FusionError::Storage(format!(
                                "parallel streaming scan task panicked: {}",
                                error
                            )))
                        }
                    }
                }
                break;
            }

            match handle.await {
                Ok(result) => result?,
                Err(error) => {
                    return Err(FusionError::Storage(format!(
                        "parallel streaming scan task panicked: {}",
                        error
                    )))
                }
            }
        }

        Ok(Some(visited))
    }

    /// Derive up to K-1 split keys dividing `[start, end)` into roughly even integer-primary-key
    /// sub-ranges (keys shaped `<table prefix> ++ <16 ASCII hex>`, the `encode_i64_comparable`
    /// encoding). Returns `None` — so the caller stays serial — when the range is empty, not that
    /// encoding, or estimated to hold fewer than `PARALLEL_SCAN_MIN_ROWS` rows.
    /// Cheap, in-memory upper bound on how many entries `[start, end)` can
    /// hold, counting at most `cap`. Memtable entries come from a bounded
    /// skip-map range walk; SSTable entries from the preloaded per-block
    /// properties (blocks whose key span overlaps the range).
    ///
    /// This is a performance heuristic only: the parallel-split boundaries
    /// are still computed from real keys, so an estimate that is off in
    /// either direction changes whether the probe runs, never the results.
    /// It deliberately over-counts (MVCC versions, partial block overlap,
    /// unloaded properties count as `cap`) so a table worth parallelizing
    /// never loses its probe.
    fn range_entry_upper_bound_capped(&self, start: &[u8], end: &[u8], cap: u64) -> u64 {
        let start_ik = FusionStorage::encode_key(start, u64::MAX);
        let end_ik = FusionStorage::encode_key(end, u64::MAX);
        let mut total = 0u64;

        // SSTable block metadata first: for a table already worth
        // parallelizing, a handful of block entry counts reaches `cap`
        // without touching a single entry. The bounded memtable walk (up to
        // `cap` skip-map hops) only runs when blocks alone were not enough.
        let sstables = self.storage.sstables.read().unwrap().clone();
        for sst in &sstables {
            let properties = sst.current_block_properties();
            if properties.is_empty() {
                // Properties not preloaded yet: assume the worst so the
                // probe still runs — same behavior as before this gate.
                return cap;
            }
            for property in properties.iter() {
                let (block_first, _) = FusionStorage::decode_key(&property.first_key);
                let (block_last, _) = FusionStorage::decode_key(&property.last_key);
                if block_last < start || block_first >= end {
                    continue;
                }
                total = total.saturating_add(u64::from(property.entry_count));
                if total >= cap {
                    return total;
                }
            }
        }

        let mem_tables = {
            let mut tables = vec![self.storage.active_memtable.read().unwrap().clone()];
            tables.extend(
                self.storage
                    .immutable_memtables
                    .read()
                    .unwrap()
                    .iter()
                    .cloned(),
            );
            tables
        };
        for mem in &mem_tables {
            for _ in mem.map.range(start_ik.clone()..end_ik.clone()) {
                total += 1;
                if total >= cap {
                    return total;
                }
            }
        }
        total
    }

    async fn integer_pk_range_splits(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<Vec<Vec<u8>>>> {
        const PARALLEL_SCAN_SHARDS_CAP: usize = 8;
        const PARALLEL_SCAN_MIN_ROWS: u64 = 8192;

        let shards = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(PARALLEL_SCAN_SHARDS_CAP);
        if shards <= 1 {
            return Ok(None);
        }
        // Ranges that cannot possibly hold enough rows skip the key-span
        // probe entirely. The probe's `last()` opens a reverse iterator over
        // every overlapping SSTable, which on a small table costs more than
        // the query itself — and its result would be discarded against
        // PARALLEL_SCAN_MIN_ROWS anyway.
        if self.range_entry_upper_bound_capped(start, end, PARALLEL_SCAN_MIN_ROWS)
            < PARALLEL_SCAN_MIN_ROWS
        {
            return Ok(None);
        }
        let (Some((min_key, _)), Some((max_key, _))) =
            (self.first(start, end).await?, self.last(start, end).await?)
        else {
            return Ok(None);
        };
        // Both keys must be `<table prefix> ++ <16 ASCII hex>` sharing the same prefix.
        if min_key.len() != max_key.len() || min_key.len() < 16 {
            return Ok(None);
        }
        let prefix_len = min_key.len() - 16;
        if min_key[..prefix_len] != max_key[..prefix_len] {
            return Ok(None);
        }
        let parse_hex = |k: &[u8]| -> Option<u64> {
            let suffix = &k[prefix_len..];
            if !suffix.iter().all(u8::is_ascii_hexdigit) {
                return None;
            }
            u64::from_str_radix(std::str::from_utf8(suffix).ok()?, 16).ok()
        };
        let (Some(min_u), Some(max_u)) = (parse_hex(&min_key), parse_hex(&max_key)) else {
            return Ok(None);
        };
        if max_u <= min_u || max_u - min_u < PARALLEL_SCAN_MIN_ROWS {
            return Ok(None);
        }
        let span = max_u - min_u;
        let mut splits: Vec<Vec<u8>> = Vec::with_capacity(shards - 1);
        let mut last_boundary: Option<u64> = None;
        for i in 1..shards {
            // Interpolate in u128 — `span` can approach u64::MAX (ids spanning the full i64 range),
            // so `span * i` would overflow u64. The result is always <= max_u, so the cast is lossless.
            let boundary = min_u + ((span as u128 * i as u128) / shards as u128) as u64;
            if boundary <= min_u || boundary >= max_u || last_boundary == Some(boundary) {
                continue;
            }
            last_boundary = Some(boundary);
            let mut key = min_key[..prefix_len].to_vec();
            key.extend_from_slice(format!("{:016x}", boundary).as_bytes());
            splits.push(key);
        }
        if splits.is_empty() {
            return Ok(None);
        }
        Ok(Some(splits))
    }
}

impl Drop for FusionTransaction {
    fn drop(&mut self) {
        if self.read_ts_registered {
            self.storage.unregister_active_read_ts(self.read_ts);
            self.read_ts_registered = false;
        }
    }
}

#[async_trait]
impl Transaction for FusionTransaction {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 1. Read-Your-Own-Writes
        for (k, v) in self.write_buffer.iter().rev() {
            if k == key {
                return Ok(v.clone());
            }
        }

        // 2. Scan Storage (Active + Immutable) for latest version <= read_ts
        let search_key = FusionStorage::encode_key(key, self.read_ts);
        let mut best: Option<(u64, bool, Vec<u8>)> = None;

        let mut consider = |ts: u64, encoded_value: &[u8]| {
            if ts > self.read_ts {
                return;
            }
            if best.as_ref().map_or(true, |(best_ts, _, _)| ts > *best_ts) {
                let (is_put, val) = FusionStorage::decode_value(encoded_value);
                best = Some((ts, is_put, val.to_vec()));
            }
        };

        // Helper to check a memtable
        let check_mem = |mem: &MemTable| -> Option<(u64, Vec<u8>)> {
            // Range scan starting from (Key, MAX-read_ts)
            // The first entry >= search_key
            let entry = mem.map.range(search_key.clone()..).next();
            if let Some(ent) = entry {
                let (k, ts) = FusionStorage::decode_key(ent.key());
                if k == key && ts <= self.read_ts {
                    return Some((ts, ent.value().clone()));
                }
            }
            None
        };

        // Check Active
        {
            let active = self.storage.active_memtable.read().unwrap();
            if let Some((ts, val)) = check_mem(&active) {
                consider(ts, &val);
            }
        }

        // Check Immutable
        {
            let imm = self.storage.immutable_memtables.read().unwrap();
            for mem in imm.iter().rev() {
                if let Some((ts, val)) = check_mem(mem) {
                    consider(ts, &val);
                }
            }
        }

        // Check SSTables
        let sstables: Vec<Arc<SsTable>> = {
            let guard = self.storage.sstables.read().unwrap();
            guard.clone()
        };

        for sst in &sstables {
            let (sst_min_user_key, _) = FusionStorage::decode_key(&sst.meta.first_key);
            let (sst_max_user_key, _) = FusionStorage::decode_key(&sst.meta.last_key);
            if key < sst_min_user_key || key > sst_max_user_key {
                crate::monitor::inc_sstable_point_overlap_skip();
                continue;
            }
            crate::monitor::inc_sstable_point_probe();
            crate::monitor::inc_sstable_user_key_filter_check();
            match sst.probe_user_key_filter(key, TS_SIZE) {
                crate::storage::sstable::SsTablePrefixFilterProbe::MayMatch => {
                    crate::monitor::inc_sstable_user_key_filter_positive();
                }
                crate::storage::sstable::SsTablePrefixFilterProbe::NoMatch => {
                    crate::monitor::inc_sstable_user_key_filter_skip();
                    continue;
                }
                crate::storage::sstable::SsTablePrefixFilterProbe::FailOpen => {
                    crate::monitor::inc_sstable_user_key_filter_fail_open();
                }
            }
            if let Some((k_bytes, v_bytes)) = sst.find_ge(&search_key).await? {
                let (k, ts) = FusionStorage::decode_key(&k_bytes);
                if k == key && ts <= self.read_ts {
                    consider(ts, &v_bytes);
                }
            }
        }

        match best {
            Some((_ts, true, val)) => Ok(Some(val)),
            Some((_ts, false, _)) => Ok(None),
            None => Ok(None),
        }
    }

    async fn fence_data_migration_phase(&mut self, phase: u8, phase_seq: u64) -> Result<()> {
        let phase = DataMigrationPhase::from_byte(phase).ok_or_else(|| {
            FusionError::Storage(format!(
                "cannot fence on invalid Data V2 migration phase ordinal {phase}"
            ))
        })?;
        let pin = FenceSnapshot { phase, phase_seq };
        match self.fenced_migration_phase {
            None => {
                self.fenced_migration_phase = Some(pin);
                Ok(())
            }
            Some(existing) if existing == pin => Ok(()),
            Some(existing) => Err(FusionError::Storage(format!(
                "Data V2 migration phase changed within transaction (fenced '{}' seq {}, now '{}' seq {}); abort and retry",
                existing.phase.name(),
                existing.phase_seq,
                pin.phase.name(),
                pin.phase_seq
            ))),
        }
    }

    fn data_migration_phase_pin(&self) -> Option<(u8, u64)> {
        self.fenced_migration_phase
            .map(|pin| (pin.phase.as_byte(), pin.phase_seq))
    }

    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_buffer.push((key.to_vec(), Some(value.to_vec())));
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write_buffer.push((key.to_vec(), None));
        Ok(())
    }

    async fn scan_prefix(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if let Some(end) = FusionStorage::prefix_end(prefix) {
            return self.scan_range(prefix, &end, limit).await;
        }
        Ok(Vec::new())
    }

    async fn scan_prefix_with_options(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
        options: StorageScanOptions,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if let Some(end) = FusionStorage::prefix_end(prefix) {
            return self
                .scan_range_with_options(prefix, &end, limit, options)
                .await;
        }
        Ok(Vec::new())
    }

    async fn scan_prefix_parallel(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // A pushed limit keeps the serial early-break; only unbounded scans split in parallel.
        if limit.is_some() {
            return self.scan_prefix(prefix, limit).await;
        }
        let Some(end) = FusionStorage::prefix_end(prefix) else {
            return Ok(Vec::new());
        };
        self.scan_range_parallel(prefix, &end).await
    }

    async fn scan_prefix_parallel_with_options(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
        options: StorageScanOptions,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit.is_some() {
            return self.scan_prefix_with_options(prefix, limit, options).await;
        }
        let Some(end) = FusionStorage::prefix_end(prefix) else {
            return Ok(Vec::new());
        };
        self.scan_range_parallel_with_options(prefix, &end, options)
            .await
    }

    async fn scan_prefix_for_each(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
    ) -> Result<usize> {
        let Some(end) = FusionStorage::prefix_end(prefix) else {
            return Ok(0);
        };
        let safe_limit = limit.unwrap_or(usize::MAX);
        if safe_limit == 0 {
            return Ok(0);
        }

        let mut visited = 0;
        self.for_each_visible_range(prefix, &end, |user_k, val| {
            visited += 1;
            visitor.visit(user_k, val) && visited < safe_limit
        })
        .await?;
        Ok(visited)
    }

    async fn scan_prefix_for_each_with_options(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
        options: StorageScanOptions,
    ) -> Result<usize> {
        let Some(end) = FusionStorage::prefix_end(prefix) else {
            return Ok(0);
        };
        self.scan_range_for_each_with_options(prefix, &end, limit, visitor, options)
            .await
    }

    async fn scan_prefix_parallel_for_each(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
    ) -> Result<Option<usize>> {
        let Some(end) = FusionStorage::prefix_end(prefix) else {
            return Ok(Some(0));
        };
        self.scan_range_parallel_for_each(prefix, &end, limit, visitor)
            .await
    }

    async fn scan_prefix_parallel_for_each_with_options(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
        options: StorageScanOptions,
    ) -> Result<Option<usize>> {
        let Some(end) = FusionStorage::prefix_end(prefix) else {
            return Ok(Some(0));
        };
        self.scan_range_parallel_for_each_with_options(prefix, &end, limit, visitor, options)
            .await
    }

    async fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let safe_limit = limit.unwrap_or(usize::MAX);
        if safe_limit == 0 {
            return Ok(Vec::new());
        }

        let mut res = Vec::with_capacity(safe_limit.min(4096));
        self.for_each_visible_range(start, end, |user_k, val| {
            res.push((user_k.to_vec(), val.to_vec()));
            res.len() < safe_limit
        })
        .await?;

        Ok(res)
    }

    async fn scan_range_with_options(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        options: StorageScanOptions,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let safe_limit = limit.unwrap_or(usize::MAX);
        if safe_limit == 0 {
            return Ok(Vec::new());
        }

        let mut res = Vec::with_capacity(safe_limit.min(4096));
        self.for_each_visible_range_with_options(start, end, options, |user_k, val| {
            res.push((user_k.to_vec(), val.to_vec()));
            res.len() < safe_limit
        })
        .await?;

        Ok(res)
    }

    async fn scan_range_for_each(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
    ) -> Result<usize> {
        let safe_limit = limit.unwrap_or(usize::MAX);
        if safe_limit == 0 || start >= end {
            return Ok(0);
        }

        let mut visited = 0;
        self.for_each_visible_range(start, end, |user_k, val| {
            visited += 1;
            visitor.visit(user_k, val) && visited < safe_limit
        })
        .await?;
        Ok(visited)
    }

    async fn scan_range_for_each_with_options(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
        options: StorageScanOptions,
    ) -> Result<usize> {
        let safe_limit = limit.unwrap_or(usize::MAX);
        if safe_limit == 0 || start >= end {
            return Ok(0);
        }

        let mut visited = 0;
        self.for_each_visible_range_with_options(start, end, options, |user_k, val| {
            visited += 1;
            visitor.visit(user_k, val) && visited < safe_limit
        })
        .await?;
        Ok(visited)
    }

    async fn scan_range_reverse(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let safe_limit = limit.unwrap_or(usize::MAX);
        if safe_limit == 0 || start >= end {
            return Ok(Vec::new());
        }

        let mut res = Vec::with_capacity(safe_limit.min(4096));
        self.for_each_visible_range_reverse(start, end, |user_k, val| {
            res.push((user_k.to_vec(), val.to_vec()));
            res.len() < safe_limit
        })
        .await?;

        Ok(res)
    }

    async fn scan_range_reverse_with_options(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        options: StorageScanOptions,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let safe_limit = limit.unwrap_or(usize::MAX);
        if safe_limit == 0 || start >= end {
            return Ok(Vec::new());
        }

        let mut res = Vec::with_capacity(safe_limit.min(4096));
        self.for_each_visible_range_reverse_with_options(start, end, options, |user_k, val| {
            res.push((user_k.to_vec(), val.to_vec()));
            res.len() < safe_limit
        })
        .await?;

        Ok(res)
    }

    async fn scan_range_reverse_for_each(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
    ) -> Result<usize> {
        let safe_limit = limit.unwrap_or(usize::MAX);
        if safe_limit == 0 || start >= end {
            return Ok(0);
        }

        let mut visited = 0;
        self.for_each_visible_range_reverse(start, end, |user_k, val| {
            visited += 1;
            visitor.visit(user_k, val) && visited < safe_limit
        })
        .await?;
        Ok(visited)
    }

    async fn scan_range_reverse_for_each_with_options(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
        options: StorageScanOptions,
    ) -> Result<usize> {
        let safe_limit = limit.unwrap_or(usize::MAX);
        if safe_limit == 0 || start >= end {
            return Ok(0);
        }

        let mut visited = 0;
        self.for_each_visible_range_reverse_with_options(start, end, options, |user_k, val| {
            visited += 1;
            visitor.visit(user_k, val) && visited < safe_limit
        })
        .await?;
        Ok(visited)
    }

    fn supports_bounded_scan_range_reverse(&self) -> bool {
        true
    }

    async fn count_prefix(&self, prefix: &[u8]) -> Result<usize> {
        let Some(end) = FusionStorage::prefix_end(prefix) else {
            return Ok(0);
        };

        let mut count = 0;
        self.for_each_visible_range(prefix, &end, |_user_k, _val| {
            count += 1;
            true
        })
        .await?;
        Ok(count)
    }

    async fn first(&self, start: &[u8], end: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut first = None;
        self.for_each_visible_range(start, end, |user_k, val| {
            first = Some((user_k.to_vec(), val.to_vec()));
            false
        })
        .await?;
        Ok(first)
    }

    async fn last(&self, start: &[u8], end: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        Ok(self
            .scan_range_reverse(start, end, Some(1))
            .await?
            .into_iter()
            .next())
    }

    async fn commit(mut self: Box<Self>) -> Result<()> {
        if self.write_buffer.is_empty() {
            // No KV writes means nothing to validate or log, but buffered
            // side-index deltas must still apply rather than vanish.
            if self.side_index_deltas.lock().unwrap().is_empty() {
                return Ok(());
            }
            let _visibility_guard = self
                .storage
                .side_index_visibility
                .clone()
                .write_owned()
                .await;
            let _commit_guard = self.storage.commit_lock.lock().await;
            self.revalidate_migration_fence().await?;
            self.validate_side_index_deltas()?;
            self.apply_side_index_deltas()?;
            return Ok(());
        }

        // The trigram and HNSW structures are not MVCC-versioned. Drain old
        // snapshots, then block new ones until the KV versions, side indexes,
        // and public timestamp have been published as one visibility epoch.
        let _visibility_guard = self
            .storage
            .side_index_visibility
            .clone()
            .write_owned()
            .await;
        let _commit_guard = self.storage.commit_lock.lock().await;
        for (user_key, _) in &self.write_buffer {
            if let Some(timestamp) = self.storage.latest_committed_timestamp(user_key).await? {
                if timestamp > self.read_ts {
                    return Err(crate::common::FusionError::Storage(format!(
                    "Write conflict: key modified by another transaction (read_ts={}, conflict_ts={})",
                        self.read_ts, timestamp
                    )));
                }
            }
        }
        self.validate_side_index_deltas()?;

        self.revalidate_migration_fence().await?;
        let pending_fence_publish = self.staged_migration_phase_record()?;

        let commit_ts = self
            .storage
            .current_ts
            .load(Ordering::SeqCst)
            .checked_add(1)
            .ok_or_else(|| FusionError::Storage("MVCC timestamp exhausted".to_string()))?;
        let write_buffer = std::mem::take(&mut self.write_buffer);

        // Prepare encoded keys/values for both WAL and MemTable
        // We use Put for both Put and Delete (Delete is Put with Tombstone Flag)
        let capture_cdc = self.capture_cdc.load(Ordering::Acquire);
        let cdc_event_count = if capture_cdc {
            write_buffer
                .iter()
                .filter(|(key, _)| cdc_should_capture_key(key))
                .count()
        } else {
            0
        };
        let total_entries = write_buffer.len().saturating_add(cdc_event_count);
        let mut wal_entries = Vec::with_capacity(total_entries);
        let mut mem_entries = Vec::with_capacity(total_entries);
        let mut cdc_event_index = 0usize;

        for (k, v) in write_buffer {
            if capture_cdc && cdc_should_capture_key(&k) {
                let sequence = cdc_sequence_for(commit_ts, cdc_event_index)?;
                cdc_event_index += 1;
                let event = CdcEvent::from_write(sequence, commit_ts, &k, v.as_deref());
                let cdc_key = cdc_key_for_sequence(sequence);
                let cdc_value = encode_cdc_event(&event)?;
                let encoded_cdc_key = FusionStorage::encode_key(cdc_key.as_bytes(), commit_ts);
                let encoded_cdc_value = FusionStorage::encode_value(true, &cdc_value);
                wal_entries.push(WalEntry::Put(
                    encoded_cdc_key.clone(),
                    encoded_cdc_value.clone(),
                ));
                mem_entries.push((encoded_cdc_key, encoded_cdc_value));
            }

            let key = FusionStorage::encode_key(&k, commit_ts);
            let val = match v {
                Some(d) => FusionStorage::encode_value(true, &d),
                None => FusionStorage::encode_value(false, &[]),
            };

            // We use Put in WAL for everything, as MemTable handles tombstones
            wal_entries.push(WalEntry::Put(key.clone(), val.clone()));
            mem_entries.push((key, val));
        }

        // 1. WAL Write (Encoded)
        self.storage.wal.append_batch_async(wal_entries).await?;

        // 2. Publish every MemTable entry while readers of the active source are excluded.
        let needs_rotate = {
            let active = self.storage.active_memtable.write().unwrap();
            for (key, val) in mem_entries {
                active.insert(key, val);
            }
            active.size.load(Ordering::Relaxed) > self.storage.memtable_threshold as u64
        };

        // Side-index deltas apply only now — after OCC validation and WAL
        // durability, so an aborted transaction never touches the shared
        // trigram/vector indexes — and before the visibility watermark, so a
        // visible row is never missing its index entries.
        self.apply_side_index_deltas()?;

        // current_ts is the public visibility watermark. It must move only after WAL durability
        // and complete source publication, otherwise a new reader can observe a partial commit.
        self.storage.current_ts.store(commit_ts, Ordering::SeqCst);

        // Publish the new fence while still inside the commit critical
        // section, so the advance and its fence visibility are one atomic
        // step in the commit total order. This also covers Raft followers:
        // their apply path commits the replicated record through this same
        // code.
        if let Some(record) = &pending_fence_publish {
            self.storage.data_migration_fence.publish_committed(record);
        }

        // Rotation comes after the visibility watermark. Otherwise a newly
        // committed version can reach an SSTable (and therefore compaction)
        // while begin_transaction still selects the previous read_ts.
        if needs_rotate {
            self.storage.rotate_memtable().await;
        }

        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[async_trait]
impl Storage for FusionStorage {
    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        let read_ts = self.register_current_read_ts();
        Ok(Box::new(FusionTransaction {
            storage: self.clone(),
            write_buffer: transaction_write_buffer(),
            read_ts,
            read_ts_registered: true,
            capture_cdc: AtomicBool::new(true),
            side_index_deltas: std::sync::Mutex::new(Vec::new()),
            fenced_migration_phase: None,
        }))
    }

    async fn create_snapshot(&self) -> Result<()> {
        self.create_snapshot_now().await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn rrf_score_order(a: &(String, f32), b: &(String, f32)) -> CmpOrdering {
    b.1.partial_cmp(&a.1).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_storage_dir(test_name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("fusiondb_{}_{}", test_name, uuid::Uuid::new_v4()))
    }

    fn cleanup_storage_dir(path: &Path) {
        let _ = std::fs::remove_dir_all(path);
    }

    async fn test_fusion_storage(test_name: &str) -> (FusionStorage, PathBuf) {
        let data_dir = unique_storage_dir(test_name);
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();
        (storage, data_dir)
    }

    fn append_test_sstable_block_entry(block: &mut Vec<u8>, key: &[u8], value: &[u8]) {
        block.extend_from_slice(&(key.len() as u32).to_le_bytes());
        block.extend_from_slice(key);
        block.extend_from_slice(&(value.len() as u32).to_le_bytes());
        block.extend_from_slice(value);
    }

    fn zone_map_test_schema() -> crate::catalog::TableSchema {
        crate::catalog::TableSchema::new(
            "metrics".to_string(),
            vec![
                crate::catalog::Column {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    is_primary: true,
                    is_indexed: true,
                    index_type: crate::catalog::IndexType::BTree,
                    default_value: None,
                    is_nullable: false,
                    is_unique: true,
                    check_expr: None,
                },
                crate::catalog::Column {
                    name: "bucket".to_string(),
                    data_type: "INTEGER".to_string(),
                    is_primary: false,
                    is_indexed: false,
                    index_type: crate::catalog::IndexType::None,
                    default_value: None,
                    is_nullable: true,
                    is_unique: false,
                    check_expr: None,
                },
            ],
        )
    }

    fn bucket_eq_zone_map_plan(
        schema: &crate::catalog::TableSchema,
        scalar: i64,
    ) -> crate::storage::SqlBlockZoneMapPruningPlan {
        let bucket_column = &schema.columns[1];
        crate::storage::SqlBlockZoneMapPruningPlan {
            table_name: schema.name.clone(),
            schema_fingerprint: crate::storage::sql_block_zone_map_schema_fingerprint(schema),
            terms: vec![crate::storage::SqlBlockZoneMapPredicateTerm {
                column_index: 1,
                column_name: bucket_column.name.clone(),
                type_tag: crate::storage::sql_block_zone_map_type_tag(&bucket_column.data_type)
                    .expect("bucket column should support zone maps"),
                value_encoding_version: crate::storage::SQL_BLOCK_ZONE_MAP_VALUE_ENCODING_VERSION,
                kind: crate::storage::SqlBlockZoneMapPredicateKind::Compare {
                    op: crate::storage::SqlBlockZoneMapComparisonOp::Eq,
                    scalar,
                },
            }],
        }
    }

    fn metric_row(id: i64, bucket: i64) -> Vec<u8> {
        crate::common::encoding::RowEncoder::encode(&[
            crate::common::Value::Integer(id),
            crate::common::Value::Integer(bucket),
        ])
    }

    async fn put_metric_rows(storage: &FusionStorage, row_count: usize, bucket: i64) {
        let mut txn = storage.begin_transaction().await.unwrap();
        for id in 0..row_count {
            let key = format!("data:metrics:{id:04}");
            txn.put(key.as_bytes(), &metric_row(id as i64, bucket))
                .await
                .unwrap();
        }
        txn.commit().await.unwrap();
    }

    fn first_fully_data_zone_map_block_start(storage: &FusionStorage) -> Vec<u8> {
        let table_prefix = b"data:metrics:";
        let sstables = storage.sstables.read().unwrap();
        sstables
            .iter()
            .rev()
            .filter_map(|sstable| sstable.validated_block_properties_for_zone_maps())
            .flat_map(|properties| {
                properties
                    .iter()
                    .filter_map(|property| {
                        let has_zone_map = property
                            .sql_zone_maps
                            .iter()
                            .any(|zone_map| zone_map.table_prefix.as_slice() == table_prefix);
                        if !has_zone_map {
                            return None;
                        }
                        let (first, last) =
                            SsTable::block_property_user_key_interval(property, TS_SIZE)?;
                        (first.starts_with(table_prefix) && last.starts_with(table_prefix))
                            .then_some(first)
                    })
                    .collect::<Vec<_>>()
            })
            .next()
            .expect("test setup should create a fully data-prefixed zone-map block")
    }

    async fn build_test_sstable(
        storage: &FusionStorage,
        file_id: u64,
        blocks: &[Vec<(&[u8], &[u8])>],
    ) -> Arc<SsTable> {
        let path = storage.sstable_path_for(file_id);
        let mut builder = SsTableBuilder::new(path.clone());
        builder.enable_user_key_prefix_filter(TS_SIZE);
        for block_entries in blocks {
            let mut block = Vec::new();
            let mut first_key = None;
            for (user_key, value) in block_entries {
                let encoded_key = FusionStorage::encode_key(user_key, 1);
                let encoded_value = FusionStorage::encode_value(true, value);
                if first_key.is_none() {
                    first_key = Some(encoded_key.clone());
                }
                builder.add_key(&encoded_key);
                append_test_sstable_block_entry(&mut block, &encoded_key, &encoded_value);
            }
            builder
                .flush_block(
                    first_key.expect("test block must not be empty"),
                    block_entries.len() as u32,
                    &block,
                )
                .await
                .unwrap();
        }
        builder.finish().await.unwrap();
        Arc::new(
            SsTable::open(path, file_id, storage.block_cache.clone())
                .await
                .unwrap(),
        )
    }

    #[test]
    fn fusion_transaction_write_buffer_preallocates_first_write() {
        assert!(transaction_write_buffer().capacity() >= 1);
    }

    #[test]
    fn cdc_key_for_sequence_preserves_lexical_order() {
        assert!(cdc_key_for_sequence(9) < cdc_key_for_sequence(10));
        assert!(cdc_key_for_sequence(10) < cdc_key_for_sequence(100));
    }

    #[tokio::test]
    async fn fusion_flush_selects_oldest_immutable_memtable_first() {
        let (storage, data_dir) = test_fusion_storage("flush_oldest_first").await;
        storage
            .immutable_memtables
            .write()
            .unwrap()
            .extend([MemTable::new(10), MemTable::new(11)]);

        assert_eq!(storage.next_memtable_to_flush().unwrap().id, 10);

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fusion_concurrent_same_key_commits_allow_exactly_one_writer() {
        let (storage, data_dir) = test_fusion_storage("concurrent_same_key_commit").await;
        let mut first = storage.begin_transaction().await.unwrap();
        let mut second = storage.begin_transaction().await.unwrap();
        first.put(b"data:occ:shared", b"first").await.unwrap();
        second.put(b"data:occ:shared", b"second").await.unwrap();

        let (first_result, second_result) = tokio::join!(first.commit(), second.commit());
        assert_eq!(
            usize::from(first_result.is_ok()) + usize::from(second_result.is_ok()),
            1,
            "serialized OCC validation must allow exactly one stale writer"
        );
        let conflict = if let Err(error) = first_result {
            error
        } else {
            second_result.unwrap_err()
        };
        assert!(conflict.to_string().contains("Write conflict:"));

        let reader = storage.begin_transaction().await.unwrap();
        let value = reader.get(b"data:occ:shared").await.unwrap().unwrap();
        assert!(value == b"first" || value == b"second");

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_revalidates_deferred_vector_dimensions_before_wal() {
        let (storage, data_dir) = test_fusion_storage("vector_commit_validation").await;
        let mut first = storage.begin_transaction().await.unwrap();
        let mut second = storage.begin_transaction().await.unwrap();
        first.put(b"data:vector:one", b"one").await.unwrap();
        second.put(b"data:vector:two", b"two").await.unwrap();

        first
            .as_any()
            .downcast_ref::<FusionTransaction>()
            .unwrap()
            .defer_side_index_delta(SideIndexDelta::VectorInsert {
                index: "hnsw_commit_validation".to_string(),
                id: "one".to_string(),
                vector: vec![1.0, 2.0],
            });
        second
            .as_any()
            .downcast_ref::<FusionTransaction>()
            .unwrap()
            .defer_side_index_delta(SideIndexDelta::VectorInsert {
                index: "hnsw_commit_validation".to_string(),
                id: "two".to_string(),
                vector: vec![1.0, 2.0, 3.0],
            });

        first.commit().await.unwrap();
        let error = second.commit().await.unwrap_err();
        assert!(error.to_string().contains("Vector dimension mismatch"));

        let reader = storage.begin_transaction().await.unwrap();
        assert_eq!(
            reader.get(b"data:vector:one").await.unwrap(),
            Some(b"one".to_vec())
        );
        assert_eq!(reader.get(b"data:vector:two").await.unwrap(), None);
        let hits = storage
            .vector_index
            .search("hnsw_commit_validation", &[1.0, 2.0], 5)
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, "one");

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn stale_snapshot_falls_back_from_latest_only_side_indexes_without_blocking_commit() {
        let (storage, data_dir) = test_fusion_storage("side_index_epoch_guard").await;
        let old_snapshot = storage.begin_transaction().await.unwrap();

        let mut writer = storage.begin_transaction().await.unwrap();
        writer
            .put(b"data:side_epoch:new", b"new-row")
            .await
            .unwrap();
        writer
            .as_any()
            .downcast_ref::<FusionTransaction>()
            .unwrap()
            .defer_side_index_delta(SideIndexDelta::VectorInsert {
                index: "hnsw_side_epoch".to_string(),
                id: "new".to_string(),
                vector: vec![1.0, 2.0],
            });
        tokio::time::timeout(std::time::Duration::from_secs(2), writer.commit())
            .await
            .expect("a long-lived snapshot must not deadlock side-index publication")
            .unwrap();

        let old_fusion = old_snapshot
            .as_any()
            .downcast_ref::<FusionTransaction>()
            .unwrap();
        assert!(old_fusion.current_side_index_read_guard().await.is_none());

        let current_snapshot = storage.begin_transaction().await.unwrap();
        let current_fusion = current_snapshot
            .as_any()
            .downcast_ref::<FusionTransaction>()
            .unwrap();
        assert!(current_fusion
            .current_side_index_read_guard()
            .await
            .is_some());

        drop(current_snapshot);
        drop(old_snapshot);
        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_occ_detects_conflict_after_newer_version_is_flushed() {
        let (storage, data_dir) = test_fusion_storage("occ_conflict_in_sstable").await;
        let mut stale_writer = storage.begin_transaction().await.unwrap();

        {
            let mut winner = storage.begin_transaction().await.unwrap();
            winner.put(b"data:occ:flushed", b"winner").await.unwrap();
            winner.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();
        assert!(storage.immutable_memtables.read().unwrap().is_empty());
        assert!(!storage.sstables.read().unwrap().is_empty());

        stale_writer
            .put(b"data:occ:flushed", b"stale")
            .await
            .unwrap();
        let error = stale_writer.commit().await.unwrap_err();
        assert!(error.to_string().contains("Write conflict:"));

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fusion_current_ts_is_published_after_complete_memtable_commit() {
        let (storage, data_dir) = test_fusion_storage("commit_publication").await;
        let before = storage.current_ts.load(Ordering::SeqCst);
        let mut writer = storage.begin_transaction().await.unwrap();
        let entry_count = 256usize;
        for id in 0..entry_count {
            let key = format!("data:publication:{id:04}");
            let value = format!("value-{id:04}");
            writer.put(key.as_bytes(), value.as_bytes()).await.unwrap();
        }

        let commit = tokio::spawn(async move { writer.commit().await });
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while storage.current_ts.load(Ordering::SeqCst) == before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("commit should publish its timestamp");

        let reader = storage.begin_transaction().await.unwrap();
        for id in 0..entry_count {
            let key = format!("data:publication:{id:04}");
            let expected = format!("value-{id:04}").into_bytes();
            assert_eq!(reader.get(key.as_bytes()).await.unwrap(), Some(expected));
        }
        commit.await.unwrap().unwrap();

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fusion_background_flush_keeps_wal_for_active_memtable() {
        let (storage, data_dir) = test_fusion_storage("background_flush_wal_floor").await;
        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:wal_floor:immutable", b"old").await.unwrap();
            txn.commit().await.unwrap();
        }

        let flush_guard = storage.flush_lock.lock().await;
        storage.rotate_memtable().await;
        tokio::task::yield_now().await;
        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:wal_floor:active", b"must-replay")
                .await
                .unwrap();
            txn.commit().await.unwrap();
        }
        drop(flush_guard);
        storage.flush_notify.notify_one();

        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !storage.immutable_memtables.read().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background flush should drain the immutable MemTable");
        // Reacquiring the lock waits until the worker has passed its former truncate point.
        let completed_flush = storage.flush_lock.lock().await;
        drop(completed_flush);
        assert!(storage.immutable_memtables.read().unwrap().is_empty());

        let replay_entries = storage.wal.replay().unwrap();
        assert!(replay_entries.iter().any(|entry| match entry {
            WalEntry::Put(internal_key, _) | WalEntry::Delete(internal_key) => {
                FusionStorage::decode_key(internal_key).0 == b"data:wal_floor:active"
            }
        }));

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_point_and_range_reads_propagate_sstable_io_errors() {
        let (storage, data_dir) = test_fusion_storage("sstable_read_error").await;
        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:read_error:001", b"value").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();
        let mut writer = storage.begin_transaction().await.unwrap();
        let sstable_path = storage.sstables.read().unwrap()[0].path.clone();
        storage.block_cache.invalidate_all();
        storage.block_cache.run_pending_tasks();
        tokio::fs::remove_file(&sstable_path).await.unwrap();

        let reader = storage.begin_transaction().await.unwrap();
        assert!(reader.get(b"data:read_error:001").await.is_err());
        assert!(reader
            .scan_range(b"data:read_error:", b"data:read_error;", None)
            .await
            .is_err());
        writer
            .put(b"data:read_error:001", b"replacement")
            .await
            .unwrap();
        assert!(writer.commit().await.is_err());

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_commit_records_cdc_put_and_delete_events() {
        let (storage, data_dir) = test_fusion_storage("cdc_put_delete").await;

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:cdc:001", b"one").await.unwrap();
            txn.delete(b"data:cdc:002").await.unwrap();
            txn.commit().await.unwrap();
        }

        let events = storage.cdc_events_since(0, 10).await.unwrap();
        assert_eq!(events.len(), 2);
        assert!(events[0].sequence < events[1].sequence);
        assert_eq!(events[0].commit_ts, events[1].commit_ts);
        assert_eq!(events[0].operation, CdcOperation::Put);
        assert_eq!(events[0].key.data, "data:cdc:001");
        assert_eq!(events[0].value.as_ref().unwrap().data, "one");
        assert_eq!(events[1].operation, CdcOperation::Delete);
        assert_eq!(events[1].key.data, "data:cdc:002");
        assert!(events[1].value.is_none());
        assert_eq!(
            storage.cdc_latest_sequence().await.unwrap(),
            events[1].sequence
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_cdc_suppresses_structured_data_shadow_events() {
        let (storage, data_dir) = test_fusion_storage("cdc_structured_data_shadow").await;
        let shadow_key = crate::storage::keyspace::encode_data_key(
            crate::storage::keyspace::DataRoute::Unsharded,
            b"cdc_shadow",
            b"row:001",
        )
        .unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:cdc_shadow:row:001", b"one").await.unwrap();
            txn.put(&shadow_key, b"one").await.unwrap();
            txn.commit().await.unwrap();
        }
        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.delete(b"data:cdc_shadow:row:001").await.unwrap();
            txn.delete(&shadow_key).await.unwrap();
            txn.commit().await.unwrap();
        }

        let events = storage.cdc_events_since(0, 10).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].operation, CdcOperation::Put);
        assert_eq!(events[0].key.data, "data:cdc_shadow:row:001");
        assert_eq!(events[1].operation, CdcOperation::Delete);
        assert_eq!(events[1].key.data, "data:cdc_shadow:row:001");

        cleanup_storage_dir(&data_dir);
    }

    #[test]
    fn cdc_only_suppresses_exact_structured_data_keys() {
        let mut malformed = crate::storage::keyspace::encode_data_key(
            crate::storage::keyspace::DataRoute::Unsharded,
            b"cdc_shadow",
            b"row",
        )
        .unwrap();
        malformed.push(0x7f);

        assert!(cdc_should_capture_key(&malformed));
        assert!(!cdc_should_capture_key(
            &crate::storage::keyspace::encode_data_key(
                crate::storage::keyspace::DataRoute::Unsharded,
                b"cdc_shadow",
                b"row",
            )
            .unwrap()
        ));
    }

    #[tokio::test]
    async fn fusion_cdc_since_and_limit_resume_from_sequence() {
        let (storage, data_dir) = test_fusion_storage("cdc_since_limit").await;

        for id in 0..3 {
            let mut txn = storage.begin_transaction().await.unwrap();
            let key = format!("data:cdc_resume:{id}");
            txn.put(key.as_bytes(), b"value").await.unwrap();
            txn.commit().await.unwrap();
        }

        let first = storage.cdc_events_since(0, 2).await.unwrap();
        assert_eq!(first.len(), 2);
        let resumed = storage
            .cdc_events_since(first.last().unwrap().sequence, 10)
            .await
            .unwrap();
        assert_eq!(resumed.len(), 1);
        assert_eq!(resumed[0].key.data, "data:cdc_resume:2");

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_cdc_events_survive_snapshot_and_reopen() {
        let data_dir = unique_storage_dir("cdc_reopen");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();

        {
            let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
                .await
                .unwrap();
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:cdc_persist:001", b"persisted")
                .await
                .unwrap();
            txn.commit().await.unwrap();
            storage.create_snapshot_now().await.unwrap();
        }

        let reopened = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();
        let events = reopened.cdc_events_since(0, 10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].key.data, "data:cdc_persist:001");
        assert_eq!(events[0].value.as_ref().unwrap().data, "persisted");

        cleanup_storage_dir(&data_dir);
    }

    #[test]
    fn vector_rebuild_data_prefix_for_table_preallocates_exact_prefix() {
        let prefix = vector_rebuild_data_prefix_for_table("embeddings");

        assert_eq!(prefix, "data:embeddings:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn side_index_rebuild_assigns_prefix_overlapping_tables_once() {
        let text_column = || crate::catalog::Column {
            name: "body".to_string(),
            data_type: "TEXT".to_string(),
            is_primary: false,
            is_indexed: true,
            index_type: crate::catalog::IndexType::BTree,
            default_value: None,
            is_nullable: true,
            is_unique: false,
            check_expr: None,
        };
        let base_schema =
            crate::catalog::TableSchema::new("tenant".to_string(), vec![text_column()]);
        let archive_schema =
            crate::catalog::TableSchema::new("tenant:archive".to_string(), vec![text_column()]);
        let entries = vec![
            (
                b"schema:tenant".to_vec(),
                bincode::serialize(&base_schema).unwrap(),
            ),
            (
                b"schema:tenant:archive".to_vec(),
                bincode::serialize(&archive_schema).unwrap(),
            ),
            (
                b"data:tenant:base-row".to_vec(),
                crate::common::encoding::RowEncoder::encode(&[crate::common::Value::String(
                    "base needle".to_string(),
                )]),
            ),
            (
                b"data:tenant:archive:archive-row".to_vec(),
                crate::common::encoding::RowEncoder::encode(&[crate::common::Value::String(
                    "archive needle".to_string(),
                )]),
            ),
        ];

        let (_, trigram) =
            FusionStorage::build_side_indexes_from_visible_entries(&entries).unwrap();
        let base_ids = trigram.search("tenant", "body", "%needle%").unwrap();
        let archive_ids = trigram
            .search("tenant:archive", "body", "%needle%")
            .unwrap();
        assert_eq!(
            trigram.map_ids_to_row_keys("tenant", &base_ids),
            vec!["base-row".to_string()]
        );
        assert_eq!(
            trigram.map_ids_to_row_keys("tenant:archive", &archive_ids),
            vec!["archive-row".to_string()]
        );
    }

    #[test]
    fn vector_rebuild_uses_structured_hnsw_identity() {
        let name = hnsw_index_name_for_column("docs", "embedding").unwrap();

        assert_eq!(name, "hnsw_v2_AEZEQksCBwAAAARkb2NzAAAACWVtYmVkZGluZw");
    }

    #[test]
    fn merge_heap_reserves_candidate_iterators() {
        let capacity = 1 + COMPACTION_FANIN;
        assert!(merge_heap(capacity).capacity() >= capacity);
    }

    #[test]
    fn block_cache_capacity_uses_byte_weighting() {
        let mut config = StorageConfig::default();
        config.block_cache_capacity = 1;
        let cache = build_block_cache(&config);
        let first_len = 3 * 1024;
        let first: BlockCacheValue = vec![0; first_len].into();

        cache.insert((1, 0), first);
        cache.run_pending_tasks();
        assert_eq!(cache.weighted_size(), first_len as u64);

        let second: BlockCacheValue = vec![1; first_len].into();
        cache.insert((1, 4096), second);
        cache.run_pending_tasks();
        assert!(
            cache.weighted_size() <= config.block_cache_capacity_bytes(),
            "block cache should evict or reject entries by byte weight"
        );
    }

    #[tokio::test]
    async fn scan_range_user_key_prefix_boundary_includes_shorter_key_from_sstable() {
        let data_dir = unique_storage_dir("range_prefix_boundary");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        let mem = MemTable::new(1);
        mem.insert(
            FusionStorage::encode_key(b"a", 1),
            FusionStorage::encode_value(true, b"short-key"),
        );
        storage.immutable_memtables.write().unwrap().push(mem);
        storage.current_ts.store(1, Ordering::SeqCst);

        {
            let txn = storage.begin_transaction().await.unwrap();
            let rows = txn.scan_range(b"a", b"a\0", None).await.unwrap();
            assert_eq!(rows, vec![(b"a".to_vec(), b"short-key".to_vec())]);
            let reverse = txn.scan_range_reverse(b"a", b"a\0", None).await.unwrap();
            assert_eq!(reverse, vec![(b"a".to_vec(), b"short-key".to_vec())]);
        }

        storage.flush_all_immutable_memtables().await.unwrap();

        {
            let txn = storage.begin_transaction().await.unwrap();
            let rows = txn.scan_range(b"a", b"a\0", None).await.unwrap();
            assert_eq!(rows, vec![(b"a".to_vec(), b"short-key".to_vec())]);
            let reverse = txn.scan_range_reverse(b"a", b"a\0", None).await.unwrap();
            assert_eq!(reverse, vec![(b"a".to_vec(), b"short-key".to_vec())]);
        }

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_scan_range_reverse_matches_forward_after_mvcc_merge_and_limit() {
        let (storage, data_dir) = test_fusion_storage("reverse_mvcc_merge").await;
        let start = b"data:rev:";
        let end = b"data:rev;";

        let base = MemTable::new(1);
        for id in 1..=9 {
            let key = format!("data:rev:{id:03}");
            let value = format!("old{id}");
            base.insert(
                FusionStorage::encode_key(key.as_bytes(), 1),
                FusionStorage::encode_value(true, value.as_bytes()),
            );
        }
        storage.immutable_memtables.write().unwrap().push(base);
        storage.current_ts.store(1, Ordering::SeqCst);
        storage.flush_all_immutable_memtables().await.unwrap();

        let immutable = MemTable::new(2);
        immutable.insert(
            FusionStorage::encode_key(b"data:rev:002", 2),
            FusionStorage::encode_value(true, b"imm2"),
        );
        immutable.insert(
            FusionStorage::encode_key(b"data:rev:004", 2),
            FusionStorage::encode_value(false, b""),
        );
        immutable.insert(
            FusionStorage::encode_key(b"data:rev:005", 2),
            FusionStorage::encode_value(true, b"imm5"),
        );
        immutable.insert(
            FusionStorage::encode_key(b"data:rev:011", 2),
            FusionStorage::encode_value(false, b""),
        );
        storage.immutable_memtables.write().unwrap().push(immutable);

        {
            let active = storage.active_memtable.read().unwrap();
            active.insert(
                FusionStorage::encode_key(b"data:rev:003", 3),
                FusionStorage::encode_value(false, b""),
            );
            active.insert(
                FusionStorage::encode_key(b"data:rev:005", 3),
                FusionStorage::encode_value(true, b"active5"),
            );
            active.insert(
                FusionStorage::encode_key(b"data:rev:007", 3),
                FusionStorage::encode_value(true, b"active7"),
            );
            active.insert(
                FusionStorage::encode_key(b"data:rev:008", 5),
                FusionStorage::encode_value(true, b"future8"),
            );
            active.insert(
                FusionStorage::encode_key(b"data:rev:012", 5),
                FusionStorage::encode_value(true, b"future12"),
            );
        }
        storage.current_ts.store(3, Ordering::SeqCst);

        let mut txn = FusionTransaction {
            storage: storage.clone(),
            write_buffer: transaction_write_buffer(),
            read_ts: 3,
            read_ts_registered: false,
            capture_cdc: AtomicBool::new(true),
            side_index_deltas: std::sync::Mutex::new(Vec::new()),
            fenced_migration_phase: None,
        };
        txn.put(b"data:rev:009", b"wb9").await.unwrap();
        txn.delete(b"data:rev:007").await.unwrap();
        txn.put(b"data:rev:002", b"wb2").await.unwrap();
        txn.delete(b"data:rev:010").await.unwrap();

        let expected = vec![
            (b"data:rev:001".to_vec(), b"old1".to_vec()),
            (b"data:rev:002".to_vec(), b"wb2".to_vec()),
            (b"data:rev:005".to_vec(), b"active5".to_vec()),
            (b"data:rev:006".to_vec(), b"old6".to_vec()),
            (b"data:rev:008".to_vec(), b"old8".to_vec()),
            (b"data:rev:009".to_vec(), b"wb9".to_vec()),
        ];
        let mut expected_reverse = expected.clone();
        expected_reverse.reverse();

        assert_eq!(txn.scan_range(start, end, None).await.unwrap(), expected);
        assert_eq!(
            txn.scan_range_reverse(start, end, None).await.unwrap(),
            expected_reverse
        );
        assert_eq!(
            txn.scan_range_reverse(start, end, Some(2)).await.unwrap(),
            expected_reverse.iter().take(2).cloned().collect::<Vec<_>>()
        );
        assert_eq!(
            txn.scan_range_reverse(start, end, Some(0)).await.unwrap(),
            Vec::<(Vec<u8>, Vec<u8>)>::new()
        );
        assert_eq!(
            txn.last(start, end).await.unwrap(),
            expected_reverse.first().cloned()
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_scan_range_reverse_respects_snapshot_read_ts_for_future_sstable_tombstone() {
        let (storage, data_dir) = test_fusion_storage("reverse_snapshot_sstable").await;

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:rev_ts:001", b"old").await.unwrap();
            txn.put(b"data:rev_ts:002", b"keep").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        let stale_txn = FusionTransaction {
            storage: storage.clone(),
            write_buffer: transaction_write_buffer(),
            read_ts: 1,
            read_ts_registered: false,
            capture_cdc: AtomicBool::new(true),
            side_index_deltas: std::sync::Mutex::new(Vec::new()),
            fenced_migration_phase: None,
        };

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.delete(b"data:rev_ts:001").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        assert_eq!(
            stale_txn
                .scan_range_reverse(b"data:rev_ts:", b"data:rev_ts;", None)
                .await
                .unwrap(),
            vec![
                (b"data:rev_ts:002".to_vec(), b"keep".to_vec()),
                (b"data:rev_ts:001".to_vec(), b"old".to_vec()),
            ]
        );

        let fresh_txn = storage.begin_transaction().await.unwrap();
        assert_eq!(
            fresh_txn
                .scan_range_reverse(b"data:rev_ts:", b"data:rev_ts;", None)
                .await
                .unwrap(),
            vec![(b"data:rev_ts:002".to_vec(), b"keep".to_vec())]
        );
        assert_eq!(
            fresh_txn
                .last(b"data:rev_ts:", b"data:rev_ts;")
                .await
                .unwrap(),
            Some((b"data:rev_ts:002".to_vec(), b"keep".to_vec()))
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_scan_range_reverse_records_raw_sstable_work_counters() {
        let (storage, data_dir) = test_fusion_storage("reverse_raw_metrics").await;

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            for id in 1..=5 {
                let key = format!("data:raw:{id:03}");
                let value = format!("value{id}");
                txn.put(key.as_bytes(), value.as_bytes()).await.unwrap();
            }
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        let metrics = &crate::monitor::GLOBAL_METRICS;
        let scans_before = metrics.fusion_reverse_scan_count.load(Ordering::Relaxed);
        let sources_before = metrics
            .fusion_reverse_source_open_count
            .load(Ordering::Relaxed);
        let raw_before = metrics
            .fusion_reverse_raw_entry_read_count
            .load(Ordering::Relaxed);
        let candidates_before = metrics
            .fusion_reverse_visible_candidate_count
            .load(Ordering::Relaxed);
        let puts_before = metrics
            .fusion_reverse_visible_put_count
            .load(Ordering::Relaxed);
        let sstable_blocks_before = metrics
            .sstable_reverse_block_read_count
            .load(Ordering::Relaxed);
        let sstable_reverse_iterators_before = metrics
            .sstable_reverse_iterator_open_count
            .load(Ordering::Relaxed);
        let sstable_decodes_before = metrics
            .sstable_reverse_block_entry_decode_count
            .load(Ordering::Relaxed);
        let sstable_yields_before = metrics
            .sstable_reverse_block_entry_yield_count
            .load(Ordering::Relaxed);

        let txn = storage.begin_transaction().await.unwrap();
        let rows = txn
            .scan_range_reverse(b"data:raw:", b"data:raw;", Some(2))
            .await
            .unwrap();

        assert_eq!(
            rows,
            vec![
                (b"data:raw:005".to_vec(), b"value5".to_vec()),
                (b"data:raw:004".to_vec(), b"value4".to_vec()),
            ]
        );

        let scan_delta = metrics
            .fusion_reverse_scan_count
            .load(Ordering::Relaxed)
            .saturating_sub(scans_before);
        let source_delta = metrics
            .fusion_reverse_source_open_count
            .load(Ordering::Relaxed)
            .saturating_sub(sources_before);
        let raw_delta = metrics
            .fusion_reverse_raw_entry_read_count
            .load(Ordering::Relaxed)
            .saturating_sub(raw_before);
        let candidate_delta = metrics
            .fusion_reverse_visible_candidate_count
            .load(Ordering::Relaxed)
            .saturating_sub(candidates_before);
        let put_delta = metrics
            .fusion_reverse_visible_put_count
            .load(Ordering::Relaxed)
            .saturating_sub(puts_before);
        let sstable_block_delta = metrics
            .sstable_reverse_block_read_count
            .load(Ordering::Relaxed)
            .saturating_sub(sstable_blocks_before);
        let sstable_reverse_iterator_delta = metrics
            .sstable_reverse_iterator_open_count
            .load(Ordering::Relaxed)
            .saturating_sub(sstable_reverse_iterators_before);
        let sstable_decode_delta = metrics
            .sstable_reverse_block_entry_decode_count
            .load(Ordering::Relaxed)
            .saturating_sub(sstable_decodes_before);
        let sstable_yield_delta = metrics
            .sstable_reverse_block_entry_yield_count
            .load(Ordering::Relaxed)
            .saturating_sub(sstable_yields_before);

        assert!(
            scan_delta >= 1,
            "reverse scan counter should increment, got {scan_delta}"
        );
        assert!(
            source_delta >= 1,
            "reverse source counter should increment, got {source_delta}"
        );
        assert!(
            raw_delta >= 2,
            "reverse raw-entry counter should observe SSTable pulls, got {raw_delta}"
        );
        assert!(
            candidate_delta >= 2,
            "visible candidate counter should observe yielded source candidates, got {candidate_delta}"
        );
        assert!(
            put_delta >= 2,
            "visible PUT counter should observe emitted rows, got {put_delta}"
        );
        assert!(
            sstable_block_delta >= 1,
            "SSTable reverse block counter should increment, got {sstable_block_delta}"
        );
        assert!(
            sstable_reverse_iterator_delta >= 1,
            "SSTable reverse iterator counter should increment, got {sstable_reverse_iterator_delta}"
        );
        assert!(
            sstable_decode_delta >= 2,
            "SSTable reverse decode counter should increment, got {sstable_decode_delta}"
        );
        assert!(
            sstable_yield_delta >= 2,
            "SSTable reverse yield counter should increment, got {sstable_yield_delta}"
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fusion_scan_range_reverse_lazily_activates_sstable_sources_by_frontier() {
        let (storage, data_dir) = test_fusion_storage("reverse_lazy_sstable_activation").await;

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            for id in 1..=3 {
                let key = format!("data:lazy:{id:03}");
                let value = format!("low{id}");
                txn.put(key.as_bytes(), value.as_bytes()).await.unwrap();
            }
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            for id in 900..=902 {
                let key = format!("data:lazy:{id:03}");
                let value = format!("high{id}");
                txn.put(key.as_bytes(), value.as_bytes()).await.unwrap();
            }
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        assert_eq!(
            storage.sstables.read().unwrap().len(),
            2,
            "test setup should create two non-compacted SSTables"
        );

        reverse_activation_test_hooks::reset();
        let txn = storage.begin_transaction().await.unwrap();
        let rows = txn
            .scan_range_reverse(b"data:lazy:", b"data:lazy;", Some(1))
            .await
            .unwrap();

        assert_eq!(rows, vec![(b"data:lazy:902".to_vec(), b"high902".to_vec())]);
        assert_eq!(
            reverse_activation_test_hooks::get(),
            1,
            "LIMIT 1 should not activate the lower-frontier SSTable"
        );

        reverse_activation_test_hooks::reset();
        let full_txn = storage.begin_transaction().await.unwrap();
        let full_rows = full_txn
            .scan_range_reverse(b"data:lazy:", b"data:lazy;", None)
            .await
            .unwrap();

        assert_eq!(
            full_rows,
            vec![
                (b"data:lazy:902".to_vec(), b"high902".to_vec()),
                (b"data:lazy:901".to_vec(), b"high901".to_vec()),
                (b"data:lazy:900".to_vec(), b"high900".to_vec()),
                (b"data:lazy:003".to_vec(), b"low3".to_vec()),
                (b"data:lazy:002".to_vec(), b"low2".to_vec()),
                (b"data:lazy:001".to_vec(), b"low1".to_vec()),
            ]
        );
        assert_eq!(
            reverse_activation_test_hooks::get(),
            2,
            "unbounded reverse scan should activate the lower-frontier SSTable after high keys drain"
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fusion_scan_range_reverse_uses_in_range_block_frontier() {
        let (storage, data_dir) = test_fusion_storage("reverse_block_frontier_activation").await;
        storage.current_ts.store(2, Ordering::SeqCst);

        let gap_sstable = build_test_sstable(
            &storage,
            10_001,
            &[
                vec![(b"data:block_frontier:050".as_slice(), b"gap-low".as_slice())],
                vec![(
                    b"data:block_frontier:900".as_slice(),
                    b"gap-high".as_slice(),
                )],
            ],
        )
        .await;
        let matching_sstable = build_test_sstable(
            &storage,
            10_002,
            &[vec![
                (
                    b"data:block_frontier:500".as_slice(),
                    b"match-500".as_slice(),
                ),
                (
                    b"data:block_frontier:501".as_slice(),
                    b"match-501".as_slice(),
                ),
            ]],
        )
        .await;
        let deferred_sstable = build_test_sstable(
            &storage,
            10_003,
            &[
                vec![(
                    b"data:block_frontier:150".as_slice(),
                    b"deferred-low".as_slice(),
                )],
                vec![(
                    b"data:block_frontier:900".as_slice(),
                    b"deferred-high".as_slice(),
                )],
            ],
        )
        .await;

        {
            let mut sstables = storage.sstables.write().unwrap();
            sstables.push(gap_sstable);
            sstables.push(deferred_sstable);
            sstables.push(matching_sstable);
        }

        let metrics = &crate::monitor::GLOBAL_METRICS;
        let probe_before = metrics
            .fusion_reverse_sstable_frontier_probe_count
            .load(Ordering::Relaxed);
        let in_range_before = metrics
            .fusion_reverse_sstable_frontier_in_range_count
            .load(Ordering::Relaxed);
        let tighten_before = metrics
            .fusion_reverse_sstable_frontier_tighten_count
            .load(Ordering::Relaxed);
        let empty_skip_before = metrics
            .fusion_reverse_sstable_frontier_empty_skip_count
            .load(Ordering::Relaxed);
        let pending_before = metrics
            .fusion_reverse_sstable_pending_count
            .load(Ordering::Relaxed);
        let activation_before = metrics
            .fusion_reverse_sstable_activation_count
            .load(Ordering::Relaxed);
        let deferred_before = metrics
            .fusion_reverse_sstable_deferred_unopened_count
            .load(Ordering::Relaxed);

        reverse_activation_test_hooks::reset();
        let txn = storage.begin_transaction().await.unwrap();
        let rows = txn
            .scan_range_reverse(
                b"data:block_frontier:100",
                b"data:block_frontier:600",
                Some(1),
            )
            .await
            .unwrap();

        assert_eq!(
            rows,
            vec![(b"data:block_frontier:501".to_vec(), b"match-501".to_vec())]
        );
        assert_eq!(
            reverse_activation_test_hooks::get(),
            1,
            "block/index frontier should avoid activating lower-frontier SSTables under LIMIT"
        );
        assert_eq!(
            metrics
                .fusion_reverse_sstable_frontier_probe_count
                .load(Ordering::Relaxed)
                .saturating_sub(probe_before),
            3
        );
        assert_eq!(
            metrics
                .fusion_reverse_sstable_frontier_in_range_count
                .load(Ordering::Relaxed)
                .saturating_sub(in_range_before),
            2
        );
        assert_eq!(
            metrics
                .fusion_reverse_sstable_frontier_tighten_count
                .load(Ordering::Relaxed)
                .saturating_sub(tighten_before),
            1
        );
        assert_eq!(
            metrics
                .fusion_reverse_sstable_frontier_empty_skip_count
                .load(Ordering::Relaxed)
                .saturating_sub(empty_skip_before),
            1
        );
        assert_eq!(
            metrics
                .fusion_reverse_sstable_pending_count
                .load(Ordering::Relaxed)
                .saturating_sub(pending_before),
            2
        );
        assert_eq!(
            metrics
                .fusion_reverse_sstable_activation_count
                .load(Ordering::Relaxed)
                .saturating_sub(activation_before),
            1
        );
        assert_eq!(
            metrics
                .fusion_reverse_sstable_deferred_unopened_count
                .load(Ordering::Relaxed)
                .saturating_sub(deferred_before),
            1
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fusion_scan_range_reverse_skips_empty_memtable_sources() {
        let (storage, data_dir) = test_fusion_storage("reverse_skip_empty_memtable_sources").await;

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:empty_mem:001", b"one").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        assert!(
            storage.active_memtable.read().unwrap().map.is_empty(),
            "snapshot should leave an empty active memtable"
        );

        reverse_activation_test_hooks::reset();
        let txn = storage.begin_transaction().await.unwrap();
        let rows = txn
            .scan_range_reverse(b"data:empty_mem:", b"data:empty_mem;", Some(1))
            .await
            .unwrap();

        assert_eq!(
            rows,
            vec![(b"data:empty_mem:001".to_vec(), b"one".to_vec())]
        );
        assert_eq!(
            reverse_activation_test_hooks::get_source_open(),
            1,
            "reverse scan should not open a source for the empty active memtable"
        );
        assert_eq!(
            reverse_activation_test_hooks::get(),
            1,
            "the only opened source should be the matching SSTable"
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_scan_range_reverse_activates_equal_frontier_sstables_before_emit() {
        let (storage, data_dir) = test_fusion_storage("reverse_equal_frontier_tombstone").await;

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:eq:001", b"old1").await.unwrap();
            txn.put(b"data:eq:002", b"old2").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.delete(b"data:eq:002").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        assert_eq!(
            storage.sstables.read().unwrap().len(),
            2,
            "test setup should create two SSTables with the same frontier user key"
        );

        let equal_frontier_before = crate::monitor::GLOBAL_METRICS
            .fusion_reverse_sstable_activation_equal_frontier_count
            .load(Ordering::Relaxed);

        let txn = storage.begin_transaction().await.unwrap();
        let rows = txn
            .scan_range_reverse(b"data:eq:", b"data:eq;", None)
            .await
            .unwrap();

        assert_eq!(rows, vec![(b"data:eq:001".to_vec(), b"old1".to_vec())]);
        assert!(
            crate::monitor::GLOBAL_METRICS
                .fusion_reverse_sstable_activation_equal_frontier_count
                .load(Ordering::Relaxed)
                .saturating_sub(equal_frontier_before)
                >= 1,
            "equal-frontier SSTable activation should be visible in metrics"
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fusion_scan_range_reverse_skips_sstable_by_sql_index_prefix_filter() {
        let (storage, data_dir) = test_fusion_storage("reverse_sql_index_prefix_filter").await;

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"index:metrics:host_id,ts:i0|i001:row0", b"low")
                .await
                .unwrap();
            txn.put(b"index:metrics:host_id,ts:i9|i001:row9", b"high")
                .await
                .unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"index:metrics:host_id,ts:i5|i001:row1", b"one")
                .await
                .unwrap();
            txn.put(b"index:metrics:host_id,ts:i5|i002:row2", b"two")
                .await
                .unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        assert_eq!(
            storage.sstables.read().unwrap().len(),
            2,
            "test setup should create one matching SSTable and one overlapping no-match SSTable"
        );

        let checks_before = crate::monitor::GLOBAL_METRICS
            .sstable_index_prefix_filter_check_count
            .load(Ordering::Relaxed);
        let skips_before = crate::monitor::GLOBAL_METRICS
            .sstable_index_prefix_filter_skip_count
            .load(Ordering::Relaxed);
        let positives_before = crate::monitor::GLOBAL_METRICS
            .sstable_index_prefix_filter_positive_count
            .load(Ordering::Relaxed);

        reverse_activation_test_hooks::reset();
        let mut end = b"index:metrics:host_id,ts:i5|".to_vec();
        end.push(0xff);
        let txn = storage.begin_transaction().await.unwrap();
        let rows = txn
            .scan_range_reverse(b"index:metrics:host_id,ts:i5|", &end, None)
            .await
            .unwrap();

        assert_eq!(
            rows,
            vec![
                (
                    b"index:metrics:host_id,ts:i5|i002:row2".to_vec(),
                    b"two".to_vec()
                ),
                (
                    b"index:metrics:host_id,ts:i5|i001:row1".to_vec(),
                    b"one".to_vec()
                ),
            ]
        );
        assert_eq!(
            reverse_activation_test_hooks::get(),
            1,
            "SQL index-prefix Bloom should skip the overlapping no-match SSTable before activation"
        );

        let check_delta = crate::monitor::GLOBAL_METRICS
            .sstable_index_prefix_filter_check_count
            .load(Ordering::Relaxed)
            .saturating_sub(checks_before);
        let skip_delta = crate::monitor::GLOBAL_METRICS
            .sstable_index_prefix_filter_skip_count
            .load(Ordering::Relaxed)
            .saturating_sub(skips_before);
        let positive_delta = crate::monitor::GLOBAL_METRICS
            .sstable_index_prefix_filter_positive_count
            .load(Ordering::Relaxed)
            .saturating_sub(positives_before);
        assert!(
            check_delta >= 2,
            "reverse range should probe both overlapping SSTables, got {check_delta}"
        );
        assert!(
            skip_delta >= 1,
            "reverse range should skip the no-match SSTable by SQL index-prefix Bloom"
        );
        assert!(
            positive_delta >= 1,
            "reverse range should count the matching SSTable as a positive SQL index-prefix probe"
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn hybrid_search_limited_results_are_sorted_by_rrf_score() {
        let data_dir = unique_storage_dir("hybrid_topk");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        storage.update_inverted_index("doc1".to_string(), "apple apple apple apple");
        storage.update_inverted_index("doc2".to_string(), "apple apple");
        storage.update_inverted_index("doc3".to_string(), "banana");
        storage
            .update_columnar_store(
                vec!["doc1".to_string(), "doc2".to_string(), "doc3".to_string()],
                vec![
                    vec![0.0, 0.0, 0.0],
                    vec![1.0, 0.0, 0.0],
                    vec![9.0, 0.0, 0.0],
                ],
            )
            .await;

        let results = storage.hybrid_search("apple", &[0.0, 0.0, 0.0], 2).await;

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "doc1");
        assert_eq!(results[1].0, "doc2");
        assert!(results[0].1 >= results[1].1);

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn hybrid_search_zero_limit_skips_work() {
        let data_dir = unique_storage_dir("hybrid_zero_limit");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        assert!(storage
            .hybrid_search("apple", &[0.0, 0.0, 0.0], 0)
            .await
            .is_empty());

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_startup_rebuilds_trigram_from_sharded_rows_before_returning() {
        let data_dir = unique_storage_dir("startup_trigram_rebuild");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        let schema = crate::catalog::TableSchema::new(
            "rebuild_docs".to_string(),
            vec![
                crate::catalog::Column {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    is_primary: true,
                    is_indexed: true,
                    index_type: crate::catalog::IndexType::BTree,
                    default_value: None,
                    is_nullable: false,
                    is_unique: true,
                    check_expr: None,
                },
                crate::catalog::Column {
                    name: "body".to_string(),
                    data_type: "TEXT".to_string(),
                    is_primary: false,
                    is_indexed: true,
                    index_type: crate::catalog::IndexType::BTree,
                    default_value: None,
                    is_nullable: true,
                    is_unique: false,
                    check_expr: None,
                },
            ],
        );
        let schema_bytes = bincode::serialize(&schema).unwrap();
        let row_id = crate::common::encoding::encode_i64_comparable(1);
        let row = crate::common::encoding::RowEncoder::encode(&[
            crate::common::Value::Integer(1),
            crate::common::Value::String("durable recovery needle".to_string()),
        ]);
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(b"schema:rebuild_docs", &schema_bytes)
            .await
            .unwrap();
        txn.put(
            format!("shard:3:data:rebuild_docs:{row_id}").as_bytes(),
            &row,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
        storage.shutdown().await;
        drop(storage);

        // The derived checkpoint is intentionally unusable. Startup must use
        // durable rows as truth rather than accepting an empty/stale index.
        std::fs::write(config.trigram_index_path(), b"corrupt derived index").unwrap();
        let reopened = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();
        let row_keys = {
            let index = reopened.trigram_index.read().unwrap();
            let ids = index
                .search("rebuild_docs", "body", "%needle%")
                .expect("startup rebuild must publish postings before open returns");
            index.map_ids_to_row_keys("rebuild_docs", &ids)
        };
        assert_eq!(row_keys, vec![row_id]);

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_snapshot_rejects_invalid_derived_state_before_wal_publication() {
        let (storage, data_dir) = test_fusion_storage("snapshot_prebuild_failure").await;
        let schema = crate::catalog::TableSchema::new(
            "invalid_snapshot".to_string(),
            vec![crate::catalog::Column {
                name: "embedding".to_string(),
                data_type: "VECTOR".to_string(),
                is_primary: false,
                is_indexed: true,
                index_type: crate::catalog::IndexType::HNSW,
                default_value: None,
                is_nullable: true,
                is_unique: false,
                check_expr: None,
            }],
        );
        let entries = vec![
            (
                b"schema:invalid_snapshot".to_vec(),
                bincode::serialize(&schema).unwrap(),
            ),
            (b"data:invalid_snapshot:1".to_vec(), vec![0xff]),
        ];
        let timestamp_before = storage.current_ts.load(Ordering::SeqCst);
        let wal_path = data_dir.join(StorageConfig::default().wal_file);
        let wal_len_before = std::fs::metadata(&wal_path).unwrap().len();

        let error = storage
            .replace_visible_entries_for_snapshot(b"", &[0xff], &entries)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("decode"));
        assert_eq!(storage.current_ts.load(Ordering::SeqCst), timestamp_before);
        assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), wal_len_before);
        let reader = storage.begin_transaction().await.unwrap();
        assert_eq!(reader.get(b"schema:invalid_snapshot").await.unwrap(), None);

        drop(reader);
        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_rebuild_vector_index_decodes_only_hnsw_columns() {
        let data_dir = unique_storage_dir("rebuild_hnsw_single_column");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        let schema = crate::catalog::TableSchema::new(
            "vec_rebuild".to_string(),
            vec![
                crate::catalog::Column {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    is_primary: true,
                    is_indexed: true,
                    index_type: crate::catalog::IndexType::BTree,
                    default_value: None,
                    is_nullable: false,
                    is_unique: true,
                    check_expr: None,
                },
                crate::catalog::Column {
                    name: "embedding".to_string(),
                    data_type: "VECTOR".to_string(),
                    is_primary: false,
                    is_indexed: true,
                    index_type: crate::catalog::IndexType::HNSW,
                    default_value: None,
                    is_nullable: true,
                    is_unique: false,
                    check_expr: None,
                },
                crate::catalog::Column {
                    name: "payload".to_string(),
                    data_type: "TEXT".to_string(),
                    is_primary: false,
                    is_indexed: false,
                    index_type: crate::catalog::IndexType::None,
                    default_value: None,
                    is_nullable: true,
                    is_unique: false,
                    check_expr: None,
                },
            ],
        );

        let mut row = crate::common::encoding::RowEncoder::encode(&[
            crate::common::Value::Integer(1),
            crate::common::Value::Vector(vec![1.0, 0.0]),
            crate::common::Value::String("payload".to_string()),
        ]);
        let corrupt_col_idx = 2usize;
        let off_pos = 2 + corrupt_col_idx * 4;
        let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
        for byte in &mut row[start..] {
            *byte = 0xff;
        }

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            let schema_bytes = bincode::serialize(&schema).unwrap();
            txn.put(b"schema:vec_rebuild", &schema_bytes).await.unwrap();
            txn.put(b"data:vec_rebuild:0000000000000001", &row)
                .await
                .unwrap();
            txn.commit().await.unwrap();
        }

        storage.rebuild_vector_index().await.unwrap();

        let results = storage
            .vector_index
            .search(
                "hnsw_v2_AEZEQksCBwAAAAt2ZWNfcmVidWlsZAAAAAllbWJlZGRpbmc",
                &[1.0, 0.0],
                1,
            )
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "0000000000000001");

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_count_prefix_matches_scan_prefix_after_overwrite_delete_and_write_buffer() {
        let data_dir = unique_storage_dir("count_prefix");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:x:001", b"one").await.unwrap();
            txn.put(b"data:x:002", b"two").await.unwrap();
            txn.put(b"data:y:001", b"other").await.unwrap();
            txn.commit().await.unwrap();
        }

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:x:002", b"two-new").await.unwrap();
            txn.delete(b"data:x:001").await.unwrap();
            txn.put(b"data:x:003", b"three").await.unwrap();
            txn.commit().await.unwrap();
        }

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:x:004", b"four").await.unwrap();
            txn.delete(b"data:x:002").await.unwrap();

            let rows = txn.scan_prefix(b"data:x:", None).await.unwrap();
            let count = txn.count_prefix(b"data:x:").await.unwrap();

            assert_eq!(count, rows.len());
            assert_eq!(count, 2);
            assert_eq!(
                rows.iter()
                    .map(|(key, _)| key.as_slice())
                    .collect::<Vec<_>>(),
                vec![b"data:x:003".as_slice(), b"data:x:004".as_slice()]
            );
        }

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn scan_prefix_uses_sstable_prefix_bloom_to_skip_overlapping_absent_prefix() {
        let data_dir = unique_storage_dir("prefix_bloom_skip");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:a:001", b"low").await.unwrap();
            txn.put(b"data:z:001", b"high").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:m:001", b"middle").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        let negative_sst = {
            let sstables = storage.sstables.read().unwrap();
            sstables
                .iter()
                .find(|sst| {
                    !sst.prefix_may_match(b"data:m:")
                        && sst.prefix_may_match(b"data:a:")
                        && sst.prefix_may_match(b"data:z:")
                })
                .cloned()
                .expect("expected an SSTable whose key range overlaps data:m: without that prefix")
        };
        let negative_offsets = negative_sst.index_offsets.as_ref().clone();
        let checks_before = crate::monitor::GLOBAL_METRICS
            .sstable_prefix_filter_check_count
            .load(Ordering::Relaxed);
        let positives_before = crate::monitor::GLOBAL_METRICS
            .sstable_prefix_filter_positive_count
            .load(Ordering::Relaxed);
        let skips_before = crate::monitor::GLOBAL_METRICS
            .sstable_prefix_filter_skip_count
            .load(Ordering::Relaxed);

        let rows = {
            let txn = storage.begin_transaction().await.unwrap();
            txn.scan_prefix(b"data:m:", None).await.unwrap()
        };

        assert_eq!(rows, vec![(b"data:m:001".to_vec(), b"middle".to_vec())]);
        let check_delta = crate::monitor::GLOBAL_METRICS
            .sstable_prefix_filter_check_count
            .load(Ordering::Relaxed)
            .saturating_sub(checks_before);
        let positive_delta = crate::monitor::GLOBAL_METRICS
            .sstable_prefix_filter_positive_count
            .load(Ordering::Relaxed)
            .saturating_sub(positives_before);
        let skip_delta = crate::monitor::GLOBAL_METRICS
            .sstable_prefix_filter_skip_count
            .load(Ordering::Relaxed)
            .saturating_sub(skips_before);
        assert!(
            check_delta >= 2,
            "prefix scan should probe both overlapping SSTables, got {check_delta}"
        );
        assert!(
            positive_delta >= 1,
            "prefix scan should count the matching SSTable as a positive probe"
        );
        assert!(
            skip_delta >= 1,
            "prefix scan should count the absent-prefix SSTable as a negative skip"
        );
        for offset in negative_offsets {
            assert!(
                storage
                    .block_cache
                    .get(&(negative_sst.id, offset))
                    .is_none(),
                "prefix bloom negative should skip the overlapping SSTable without reading its blocks"
            );
        }

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn scan_range_not_prefix_safe_does_not_use_prefix_bloom_negative_skip() {
        let data_dir = unique_storage_dir("prefix_bloom_range_control");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:m:001", b"middle").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        let sst = {
            let sstables = storage.sstables.read().unwrap();
            sstables
                .iter()
                .find(|sst| sst.prefix_may_match(b"data:m:"))
                .cloned()
                .expect("expected data:m SSTable")
        };
        let offsets = sst.index_offsets.as_ref().clone();

        let rows = {
            let txn = storage.begin_transaction().await.unwrap();
            txn.scan_range(b"data:b:", b"data:n:", None).await.unwrap()
        };

        assert_eq!(rows, vec![(b"data:m:001".to_vec(), b"middle".to_vec())]);
        assert!(
            offsets
                .into_iter()
                .any(|offset| storage.block_cache.get(&(sst.id, offset)).is_some()),
            "ordinary non-prefix-safe ranges must read candidate SSTable blocks instead of using prefix Bloom negative skip"
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_scan_range_no_fill_cache_reads_without_populating_block_cache() {
        let (storage, data_dir) = test_fusion_storage("scan_range_no_fill_cache").await;

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            for id in 0..128 {
                let key = format!("data:nofill:{id:04}");
                let value = vec![b'x'; 512];
                txn.put(key.as_bytes(), &value).await.unwrap();
            }
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        let sstable_offsets = {
            let sstables = storage.sstables.read().unwrap();
            assert!(!sstables.is_empty(), "snapshot should create an SSTable");
            sstables
                .iter()
                .map(|sst| (sst.id, sst.index_offsets.as_ref().clone()))
                .collect::<Vec<_>>()
        };

        let fill_skips_before = crate::monitor::GLOBAL_METRICS
            .block_cache_fill_skip_count
            .load(Ordering::Relaxed);
        let no_fill_rows = {
            let txn = storage.begin_transaction().await.unwrap();
            txn.scan_range_with_options(
                b"data:nofill:",
                b"data:nofill;",
                None,
                StorageScanOptions::no_fill_cache(),
            )
            .await
            .unwrap()
        };
        assert_eq!(no_fill_rows.len(), 128);

        let fill_skip_delta = crate::monitor::GLOBAL_METRICS
            .block_cache_fill_skip_count
            .load(Ordering::Relaxed)
            .saturating_sub(fill_skips_before);
        assert!(
            fill_skip_delta > 0,
            "no-fill range scan should skip block cache fills"
        );
        for (sst_id, offsets) in &sstable_offsets {
            for offset in offsets {
                assert!(
                    storage.block_cache.get(&(*sst_id, *offset)).is_none(),
                    "no-fill scan should not cache SSTable {sst_id} block {offset}"
                );
            }
        }

        let fill_rows = {
            let txn = storage.begin_transaction().await.unwrap();
            txn.scan_range(b"data:nofill:", b"data:nofill;", None)
                .await
                .unwrap()
        };
        assert_eq!(fill_rows.len(), 128);
        assert!(
            sstable_offsets.iter().any(|(sst_id, offsets)| offsets
                .iter()
                .any(|offset| storage.block_cache.get(&(*sst_id, *offset)).is_some())),
            "default range scan should still populate the block cache"
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn point_get_uses_sstable_user_key_bloom_to_skip_absent_key() {
        let data_dir = unique_storage_dir("point_user_key_bloom_skip");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:point_bloom_a:001", b"low").await.unwrap();
            txn.put(b"data:point_bloom_z:001", b"high").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        let (absent_key, sstable_offsets) = {
            let sstables = storage.sstables.read().unwrap();
            assert!(
                !sstables.is_empty(),
                "test setup should create at least one SSTable"
            );

            let mut candidate = None;
            for id in 0..10_000 {
                let key = format!("data:point_bloom_m:{id:04}").into_bytes();
                if sstables.iter().all(|sst| {
                    matches!(
                        sst.probe_user_key_filter(&key, TS_SIZE),
                        SsTablePrefixFilterProbe::NoMatch
                    )
                }) {
                    candidate = Some(key);
                    break;
                }
            }
            let offsets = sstables
                .iter()
                .map(|sst| (sst.id, sst.index_offsets.as_ref().clone()))
                .collect::<Vec<_>>();
            (
                candidate.expect("should find an absent key with deterministic Bloom no-match"),
                offsets,
            )
        };

        let checks_before = crate::monitor::GLOBAL_METRICS
            .sstable_user_key_filter_check_count
            .load(Ordering::Relaxed);
        let skips_before = crate::monitor::GLOBAL_METRICS
            .sstable_user_key_filter_skip_count
            .load(Ordering::Relaxed);
        let overlap_skips_before = crate::monitor::GLOBAL_METRICS
            .sstable_point_overlap_skip_count
            .load(Ordering::Relaxed);

        let value = {
            let txn = storage.begin_transaction().await.unwrap();
            txn.get(&absent_key).await.unwrap()
        };

        assert_eq!(value, None);
        let check_delta = crate::monitor::GLOBAL_METRICS
            .sstable_user_key_filter_check_count
            .load(Ordering::Relaxed)
            .saturating_sub(checks_before);
        let skip_delta = crate::monitor::GLOBAL_METRICS
            .sstable_user_key_filter_skip_count
            .load(Ordering::Relaxed)
            .saturating_sub(skips_before);
        let overlap_skip_delta = crate::monitor::GLOBAL_METRICS
            .sstable_point_overlap_skip_count
            .load(Ordering::Relaxed)
            .saturating_sub(overlap_skips_before);
        assert_eq!(
            overlap_skip_delta, 0,
            "absent key is inside the SSTable min/max range, so overlap skip should not bypass Bloom"
        );
        assert!(
            check_delta >= sstable_offsets.len() as u64,
            "point get should probe every SSTable user-key Bloom, got {check_delta}"
        );
        assert!(
            skip_delta >= sstable_offsets.len() as u64,
            "point get should skip absent-key SSTables via user-key Bloom, got {skip_delta}"
        );
        for (sst_id, offsets) in sstable_offsets {
            for offset in offsets {
                assert!(
                    storage.block_cache.get(&(sst_id, offset)).is_none(),
                    "user-key Bloom negative should skip SSTable {sst_id} block {offset} without reading it"
                );
            }
        }

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn point_get_skips_sstable_before_bloom_when_user_key_outside_file_range() {
        let data_dir = unique_storage_dir("point_overlap_skip");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:point_overlap_a:001", b"low").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:point_overlap_z:001", b"high").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        let (sstable_offsets, expected_overlap_skips) = {
            let sstables = storage.sstables.read().unwrap();
            assert!(
                sstables.len() >= 2,
                "test setup should create multiple disjoint SSTables"
            );
            let target_key = b"data:point_overlap_m:001";
            let expected_skips = sstables
                .iter()
                .filter(|sst| {
                    let (sst_min_user_key, _) = FusionStorage::decode_key(&sst.meta.first_key);
                    let (sst_max_user_key, _) = FusionStorage::decode_key(&sst.meta.last_key);
                    target_key.as_slice() < sst_min_user_key
                        || target_key.as_slice() > sst_max_user_key
                })
                .count();
            let offsets = sstables
                .iter()
                .map(|sst| (sst.id, sst.index_offsets.as_ref().clone()))
                .collect::<Vec<_>>();
            (offsets, expected_skips)
        };
        assert!(
            expected_overlap_skips > 0,
            "test setup should include at least one SSTable outside the point key range"
        );

        let probes_before = crate::monitor::GLOBAL_METRICS
            .sstable_point_probe_count
            .load(Ordering::Relaxed);
        let checks_before = crate::monitor::GLOBAL_METRICS
            .sstable_user_key_filter_check_count
            .load(Ordering::Relaxed);
        let overlap_skips_before = crate::monitor::GLOBAL_METRICS
            .sstable_point_overlap_skip_count
            .load(Ordering::Relaxed);

        let value = {
            let txn = storage.begin_transaction().await.unwrap();
            txn.get(b"data:point_overlap_m:001").await.unwrap()
        };

        assert_eq!(value, None);
        let probe_delta = crate::monitor::GLOBAL_METRICS
            .sstable_point_probe_count
            .load(Ordering::Relaxed)
            .saturating_sub(probes_before);
        let check_delta = crate::monitor::GLOBAL_METRICS
            .sstable_user_key_filter_check_count
            .load(Ordering::Relaxed)
            .saturating_sub(checks_before);
        let overlap_skip_delta = crate::monitor::GLOBAL_METRICS
            .sstable_point_overlap_skip_count
            .load(Ordering::Relaxed)
            .saturating_sub(overlap_skips_before);

        assert_eq!(
            overlap_skip_delta, expected_overlap_skips as u64,
            "point get should skip all disjoint SSTables by user-key min/max"
        );
        assert_eq!(
            probe_delta,
            (sstable_offsets.len() - expected_overlap_skips) as u64,
            "only SSTables whose min/max overlaps the key should count as point probes"
        );
        assert_eq!(
            check_delta,
            (sstable_offsets.len() - expected_overlap_skips) as u64,
            "only SSTables whose min/max overlaps the key should reach user-key Bloom"
        );
        for (sst_id, offsets) in sstable_offsets {
            for offset in offsets {
                assert!(
                    storage.block_cache.get(&(sst_id, offset)).is_none(),
                    "point overlap skip should avoid reading SSTable {sst_id} block {offset}"
                );
            }
        }

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_scan_prefix_for_each_matches_scan_prefix_after_overwrite_delete_and_write_buffer(
    ) {
        let data_dir = unique_storage_dir("scan_for_each");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:x:001", b"one").await.unwrap();
            txn.put(b"data:x:002", b"two").await.unwrap();
            txn.put(b"data:y:001", b"other").await.unwrap();
            txn.commit().await.unwrap();
        }

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:x:002", b"two-new").await.unwrap();
            txn.delete(b"data:x:001").await.unwrap();
            txn.put(b"data:x:003", b"three").await.unwrap();
            txn.commit().await.unwrap();
        }

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:x:004", b"four").await.unwrap();
            txn.delete(b"data:x:002").await.unwrap();

            let rows = txn.scan_prefix(b"data:x:", None).await.unwrap();
            struct Collector {
                rows: Vec<(Vec<u8>, Vec<u8>)>,
            }

            impl crate::storage::ScanVisitor for Collector {
                fn visit(&mut self, key: &[u8], value: &[u8]) -> bool {
                    self.rows.push((key.to_vec(), value.to_vec()));
                    true
                }
            }

            let mut collector = Collector { rows: Vec::new() };
            let visited = txn
                .scan_prefix_for_each(b"data:x:", None, &mut collector)
                .await
                .unwrap();

            assert_eq!(visited, rows.len());
            assert_eq!(collector.rows, rows);
        }

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_scan_range_for_each_matches_forward_and_reverse_range() {
        let data_dir = unique_storage_dir("scan_range_for_each");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:x:001", b"one").await.unwrap();
            txn.put(b"data:x:002", b"two").await.unwrap();
            txn.put(b"data:x:003", b"three").await.unwrap();
            txn.put(b"data:y:001", b"other").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:x:002", b"two-new").await.unwrap();
            txn.commit().await.unwrap();
        }

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.delete(b"data:x:003").await.unwrap();
            txn.put(b"data:x:004", b"four").await.unwrap();

            struct Collector {
                rows: Vec<(Vec<u8>, Vec<u8>)>,
            }

            impl crate::storage::ScanVisitor for Collector {
                fn visit(&mut self, key: &[u8], value: &[u8]) -> bool {
                    self.rows.push((key.to_vec(), value.to_vec()));
                    true
                }
            }

            let forward_rows = txn.scan_range(b"data:x:", b"data:x;", None).await.unwrap();
            let mut forward = Collector { rows: Vec::new() };
            let forward_visited = txn
                .scan_range_for_each(b"data:x:", b"data:x;", None, &mut forward)
                .await
                .unwrap();
            assert_eq!(forward_visited, forward_rows.len());
            assert_eq!(forward.rows, forward_rows);

            let reverse_rows = txn
                .scan_range_reverse(b"data:x:", b"data:x;", Some(2))
                .await
                .unwrap();
            let mut reverse = Collector { rows: Vec::new() };
            let reverse_visited = txn
                .scan_range_reverse_for_each(b"data:x:", b"data:x;", Some(2), &mut reverse)
                .await
                .unwrap();
            assert_eq!(reverse_visited, reverse_rows.len());
            assert_eq!(reverse.rows, reverse_rows);
        }

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_snapshot_flush_writes_sql_zone_map_metadata() {
        let (storage, data_dir) = test_fusion_storage("zone_map_flush_metadata").await;
        let schema = zone_map_test_schema();
        let schema_bytes = bincode::serialize(&schema).unwrap();
        let row_one = crate::common::encoding::RowEncoder::encode(&[
            crate::common::Value::Integer(1),
            crate::common::Value::Integer(7),
        ]);
        let row_two = crate::common::encoding::RowEncoder::encode(&[
            crate::common::Value::Integer(2),
            crate::common::Value::Integer(9),
        ]);

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"schema:metrics", &schema_bytes).await.unwrap();
            txn.put(b"data:metrics:001", &row_one).await.unwrap();
            txn.put(b"data:metrics:002", &row_two).await.unwrap();
            txn.commit().await.unwrap();
        }

        storage.create_snapshot_now().await.unwrap();

        let sstable = storage
            .sstables
            .read()
            .unwrap()
            .last()
            .expect("snapshot should register an SSTable")
            .clone();
        assert_eq!(sstable.meta.format_version, 6);
        let block_properties = sstable.current_block_properties();
        let maps = block_properties
            .iter()
            .flat_map(|property| property.sql_zone_maps.iter())
            .collect::<Vec<_>>();
        let bucket = maps
            .iter()
            .find(|map| map.column_name == "bucket")
            .expect("bucket zone map should be produced");
        assert_eq!(bucket.table_prefix, b"data:metrics:".to_vec());
        assert_eq!(bucket.min_scalar, 7);
        assert_eq!(bucket.max_scalar, 9);
        assert_eq!(bucket.put_count, 2);
        assert_eq!(bucket.tombstone_count, 0);
        assert!(bucket.bounds_valid);

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_forward_scan_skips_approved_sql_zone_map_blocks() {
        let (storage, data_dir) = test_fusion_storage("zone_map_approved_skip").await;
        let schema = zone_map_test_schema();
        let schema_bytes = bincode::serialize(&schema).unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"schema:metrics", &schema_bytes).await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        put_metric_rows(&storage, 512, 7).await;
        storage.create_snapshot_now().await.unwrap();

        let plan = bucket_eq_zone_map_plan(&schema, 999);
        let scan_start = first_fully_data_zone_map_block_start(&storage);
        assert_eq!(
            FusionTransaction::sql_zone_map_table_prefix_for_range(
                &plan,
                &scan_start,
                b"data:metrics;",
            ),
            Some(b"data:metrics:".to_vec())
        );

        let check_before = crate::monitor::GLOBAL_METRICS
            .sstable_block_zone_map_filter_check_count
            .load(Ordering::Relaxed);
        let positive_before = crate::monitor::GLOBAL_METRICS
            .sstable_block_zone_map_filter_positive_count
            .load(Ordering::Relaxed);
        let skip_before = crate::monitor::GLOBAL_METRICS
            .sstable_block_zone_map_filter_skip_count
            .load(Ordering::Relaxed);
        let fail_open_before = crate::monitor::GLOBAL_METRICS
            .sstable_block_zone_map_filter_fail_open_count
            .load(Ordering::Relaxed);

        let rows = {
            let txn = storage.begin_transaction().await.unwrap();
            txn.scan_range_with_options(
                &scan_start,
                b"data:metrics;",
                None,
                StorageScanOptions::no_fill_cache()
                    .with_sql_block_zone_map_pruning_plan(Arc::new(plan)),
            )
            .await
            .unwrap()
        };

        let check_delta = crate::monitor::GLOBAL_METRICS
            .sstable_block_zone_map_filter_check_count
            .load(Ordering::Relaxed)
            .saturating_sub(check_before);
        let positive_delta = crate::monitor::GLOBAL_METRICS
            .sstable_block_zone_map_filter_positive_count
            .load(Ordering::Relaxed)
            .saturating_sub(positive_before);
        let skip_delta = crate::monitor::GLOBAL_METRICS
            .sstable_block_zone_map_filter_skip_count
            .load(Ordering::Relaxed)
            .saturating_sub(skip_before);
        let fail_open_delta = crate::monitor::GLOBAL_METRICS
            .sstable_block_zone_map_filter_fail_open_count
            .load(Ordering::Relaxed)
            .saturating_sub(fail_open_before);
        assert!(
            rows.is_empty(),
            "no row can match bucket = 999; rows={}, check_delta={}, skip_delta={}, positive_delta={}, fail_open_delta={}",
            rows.len(),
            check_delta,
            skip_delta,
            positive_delta,
            fail_open_delta
        );
        assert!(
            skip_delta > 0,
            "zone map should approve at least one block skip"
        );
        assert_eq!(
            check_delta,
            positive_delta + skip_delta + fail_open_delta,
            "each checked zone-map block should have exactly one outcome"
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_sql_zone_map_skip_fails_open_on_overlapping_newer_sstable() {
        let (storage, data_dir) = test_fusion_storage("zone_map_mvcc_overlap_fail_open").await;
        let schema = zone_map_test_schema();
        let schema_bytes = bincode::serialize(&schema).unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"schema:metrics", &schema_bytes).await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        put_metric_rows(&storage, 512, 7).await;
        storage.create_snapshot_now().await.unwrap();

        put_metric_rows(&storage, 512, 9).await;
        storage.create_snapshot_now().await.unwrap();

        let scan_start = first_fully_data_zone_map_block_start(&storage);
        let mvcc_fail_open_before = crate::monitor::GLOBAL_METRICS
            .sstable_block_zone_map_mvcc_overlap_fail_open_count
            .load(Ordering::Relaxed);
        let mvcc_boundary_before = crate::monitor::GLOBAL_METRICS
            .sstable_block_zone_map_mvcc_boundary_split_fail_open_count
            .load(Ordering::Relaxed);
        let mvcc_write_buffer_before = crate::monitor::GLOBAL_METRICS
            .sstable_block_zone_map_mvcc_write_buffer_overlap_fail_open_count
            .load(Ordering::Relaxed);
        let mvcc_memtable_before = crate::monitor::GLOBAL_METRICS
            .sstable_block_zone_map_mvcc_memtable_overlap_fail_open_count
            .load(Ordering::Relaxed);
        let mvcc_sstable_before = crate::monitor::GLOBAL_METRICS
            .sstable_block_zone_map_mvcc_sstable_overlap_fail_open_count
            .load(Ordering::Relaxed);
        let rows = {
            let txn = storage.begin_transaction().await.unwrap();
            txn.scan_range_with_options(
                &scan_start,
                b"data:metrics;",
                None,
                StorageScanOptions::no_fill_cache().with_sql_block_zone_map_pruning_plan(Arc::new(
                    bucket_eq_zone_map_plan(&schema, 7),
                )),
            )
            .await
            .unwrap()
        };

        assert!(!rows.is_empty(), "scan range should include newer rows");
        for (key, value) in &rows {
            let id = std::str::from_utf8(
                key.strip_prefix(b"data:metrics:")
                    .expect("test key should use metrics prefix"),
            )
            .unwrap()
            .parse::<i64>()
            .unwrap();
            assert_eq!(
                value,
                &metric_row(id, 9),
                "overlapping newer SSTable must be read, not skipped by a no-match zone map"
            );
        }
        let mvcc_fail_open_delta = crate::monitor::GLOBAL_METRICS
            .sstable_block_zone_map_mvcc_overlap_fail_open_count
            .load(Ordering::Relaxed)
            .saturating_sub(mvcc_fail_open_before);
        let mvcc_boundary_delta = crate::monitor::GLOBAL_METRICS
            .sstable_block_zone_map_mvcc_boundary_split_fail_open_count
            .load(Ordering::Relaxed)
            .saturating_sub(mvcc_boundary_before);
        let mvcc_write_buffer_delta = crate::monitor::GLOBAL_METRICS
            .sstable_block_zone_map_mvcc_write_buffer_overlap_fail_open_count
            .load(Ordering::Relaxed)
            .saturating_sub(mvcc_write_buffer_before);
        let mvcc_memtable_delta = crate::monitor::GLOBAL_METRICS
            .sstable_block_zone_map_mvcc_memtable_overlap_fail_open_count
            .load(Ordering::Relaxed)
            .saturating_sub(mvcc_memtable_before);
        let mvcc_sstable_delta = crate::monitor::GLOBAL_METRICS
            .sstable_block_zone_map_mvcc_sstable_overlap_fail_open_count
            .load(Ordering::Relaxed)
            .saturating_sub(mvcc_sstable_before);
        assert!(
            mvcc_fail_open_delta > 0,
            "overlapping SSTables should force zone-map fail-open"
        );
        assert!(
            mvcc_sstable_delta > 0,
            "overlapping SSTables should be attributed to the SSTable-overlap reason"
        );
        assert_eq!(
            mvcc_fail_open_delta,
            mvcc_boundary_delta
                + mvcc_write_buffer_delta
                + mvcc_memtable_delta
                + mvcc_sstable_delta,
            "MVCC reason counters should account for every MVCC fail-open"
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_compaction_recomputes_sql_zone_maps_with_tombstones() {
        let (storage, data_dir) = test_fusion_storage("zone_map_compaction_metadata").await;
        let schema = zone_map_test_schema();
        let schema_bytes = bincode::serialize(&schema).unwrap();
        let row_old = crate::common::encoding::RowEncoder::encode(&[
            crate::common::Value::Integer(1),
            crate::common::Value::Integer(7),
        ]);
        let row_new = crate::common::encoding::RowEncoder::encode(&[
            crate::common::Value::Integer(1),
            crate::common::Value::Integer(9),
        ]);
        let row_filler = crate::common::encoding::RowEncoder::encode(&[
            crate::common::Value::Integer(3),
            crate::common::Value::Integer(5),
        ]);

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"schema:metrics", &schema_bytes).await.unwrap();
            txn.put(b"data:metrics:001", &row_old).await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:metrics:001", &row_new).await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.delete(b"data:metrics:002").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:metrics:003", &row_filler).await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        assert_eq!(storage.sstables.read().unwrap().len(), COMPACTION_FANIN);
        assert!(storage.compact_once().await.unwrap());

        let output = storage
            .sstables
            .read()
            .unwrap()
            .iter()
            .max_by_key(|sstable| sstable.id)
            .expect("compaction should install an output SSTable")
            .clone();
        assert_eq!(output.meta.format_version, 6);
        let block_properties = output.current_block_properties();
        let maps = block_properties
            .iter()
            .flat_map(|property| property.sql_zone_maps.iter())
            .collect::<Vec<_>>();
        let bucket = maps
            .iter()
            .find(|map| map.column_name == "bucket")
            .expect("bucket zone map should be produced after compaction");
        assert_eq!(bucket.table_prefix, b"data:metrics:".to_vec());
        assert_eq!(bucket.min_scalar, 5);
        assert_eq!(bucket.max_scalar, 9);
        assert_eq!(bucket.row_count, 3);
        assert_eq!(bucket.put_count, 2);
        assert_eq!(bucket.tombstone_count, 1);
        assert_eq!(bucket.non_null_count, 2);
        assert_eq!(bucket.null_count, 0);
        assert!(bucket.bounds_valid);

        let txn = storage.begin_transaction().await.unwrap();
        assert_eq!(txn.get(b"data:metrics:001").await.unwrap(), Some(row_new));
        assert_eq!(txn.get(b"data:metrics:002").await.unwrap(), None);
        assert_eq!(
            txn.get(b"data:metrics:003").await.unwrap(),
            Some(row_filler)
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_flush_candidate_remains_visible_until_sstable_registration() {
        let data_dir = unique_storage_dir("flush_visibility");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        let candidate = MemTable::new(42);
        candidate.insert(
            FusionStorage::encode_key(b"schema:flush_visible", 1),
            FusionStorage::encode_value(true, b"schema-bytes"),
        );
        candidate.insert(
            FusionStorage::encode_key(b"data:flush_visible:001", 1),
            FusionStorage::encode_value(true, b"row-bytes"),
        );
        storage.current_ts.store(1, Ordering::SeqCst);
        storage.immutable_memtables.write().unwrap().push(candidate);

        let candidate_id = storage
            .next_memtable_to_flush()
            .expect("queued memtable should be selected for flush")
            .id;

        {
            let txn = storage.begin_transaction().await.unwrap();
            assert_eq!(
                txn.get(b"schema:flush_visible").await.unwrap(),
                Some(b"schema-bytes".to_vec())
            );
            assert_eq!(
                txn.get(b"data:flush_visible:001").await.unwrap(),
                Some(b"row-bytes".to_vec())
            );
        }

        storage.mark_memtable_flushed(candidate_id);

        {
            let txn = storage.begin_transaction().await.unwrap();
            assert_eq!(txn.get(b"schema:flush_visible").await.unwrap(), None);
        }

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_first_uses_visible_range_with_write_buffer_shadowing() {
        let data_dir = unique_storage_dir("first_visible");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:z:001", b"one").await.unwrap();
            txn.put(b"data:z:002", b"two").await.unwrap();
            txn.commit().await.unwrap();
        }

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.delete(b"data:z:001").await.unwrap();
            txn.put(b"data:z:000", b"zero").await.unwrap();

            let first = txn.first(b"data:z:", b"data:z;").await.unwrap();
            assert_eq!(first, Some((b"data:z:000".to_vec(), b"zero".to_vec())));
        }

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_last_reads_visible_key_from_sstable() {
        let data_dir = unique_storage_dir("last_sstable");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            for id in 0..80 {
                let key = format!("data:last:{id:03}");
                let value = format!("value-{id:03}");
                txn.put(key.as_bytes(), value.as_bytes()).await.unwrap();
            }
            txn.commit().await.unwrap();
        }

        storage.create_snapshot_now().await.unwrap();
        assert!(!storage.sstables.read().unwrap().is_empty());

        {
            let txn = storage.begin_transaction().await.unwrap();
            let last = txn.last(b"data:last:", b"data:last;").await.unwrap();
            assert_eq!(
                last,
                Some((b"data:last:079".to_vec(), b"value-079".to_vec()))
            );
        }

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_reopen_uses_fresh_memtable_id_after_existing_sstables() {
        let data_dir = unique_storage_dir("reopen_memtable_id");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();

        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();
        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:reopen:001", b"before").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();
        let max_existing_id = storage
            .sstables
            .read()
            .unwrap()
            .iter()
            .map(|sst| sst.id)
            .max()
            .expect("snapshot should create an SSTable");

        let reopened = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();
        let active_id = reopened.active_memtable.read().unwrap().id;
        let next_id = reopened.next_memtable_id.load(Ordering::SeqCst);

        assert!(
            active_id > max_existing_id,
            "reopened active memtable id {active_id} must not reuse existing SSTable id {max_existing_id}"
        );
        assert!(
            next_id > active_id,
            "next memtable id {next_id} must remain ahead of active id {active_id}"
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_reopen_restores_current_ts_from_all_sstable_keys() {
        let data_dir = unique_storage_dir("reopen_current_ts");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();

        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();
        for (key, value) in [
            (b"data:restore_ts:a".as_slice(), b"one".as_slice()),
            (b"data:restore_ts:z".as_slice(), b"two".as_slice()),
            (b"data:restore_ts:m".as_slice(), b"three".as_slice()),
        ] {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(key, value).await.unwrap();
            txn.commit().await.unwrap();
        }
        let persisted_ts = storage.current_ts.load(Ordering::SeqCst);
        storage.create_snapshot_now().await.unwrap();

        let reopened = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        assert_eq!(reopened.current_ts.load(Ordering::SeqCst), persisted_ts);

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_reopen_persists_sstable_timestamp_cache() {
        let data_dir = unique_storage_dir("reopen_ts_cache");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let cache_path = sstable_timestamp_cache_path(&config.sstable_path());
        let descriptor_cache_path = sstable_descriptor_cache_path(&config.sstable_path());

        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();
        for (key, value) in [
            (b"data:restore_cache:z".as_slice(), b"one".as_slice()),
            (b"data:restore_cache:a".as_slice(), b"two".as_slice()),
            (b"data:restore_cache:m".as_slice(), b"three".as_slice()),
        ] {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(key, value).await.unwrap();
            txn.commit().await.unwrap();
        }
        let persisted_ts = storage.current_ts.load(Ordering::SeqCst);
        storage.create_snapshot_now().await.unwrap();
        assert!(
            cache_path.exists(),
            "flush should persist timestamp cache before the next reopen"
        );
        assert!(
            descriptor_cache_path.exists(),
            "flush should persist descriptor cache before the next reopen"
        );
        let cache = SstableTimestampCache::load(&cache_path);
        let cached_max_ts = cache.entries.values().map(|entry| entry.max_ts).max();
        assert_eq!(cached_max_ts, Some(persisted_ts));
        let descriptor_cache = SstableDescriptorCache::load(&descriptor_cache_path);
        let sstable = storage.sstables.read().unwrap()[0].clone();
        let fingerprint = sstable_timestamp_fingerprint(&sstable.path).unwrap();
        let descriptor = descriptor_cache
            .descriptor_for(sstable.id, &fingerprint)
            .expect("descriptor cache should contain flushed SSTable");
        assert_eq!(descriptor.first_key, sstable.meta.first_key);
        assert_eq!(descriptor.last_key, sstable.meta.last_key);
        assert_eq!(descriptor.format_version, sstable.meta.format_version);

        let reopened = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();
        assert_eq!(reopened.current_ts.load(Ordering::SeqCst), persisted_ts);
        assert!(cache_path.exists(), "reopen should persist timestamp cache");
        assert!(
            descriptor_cache_path.exists(),
            "reopen should persist descriptor cache"
        );

        let reopened_again = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();
        assert_eq!(
            reopened_again.current_ts.load(Ordering::SeqCst),
            persisted_ts
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_reopen_uses_manifest_live_sstable_list() {
        let data_dir = unique_storage_dir("reopen_manifest_live");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let sstable_dir = config.sstable_path();

        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();
        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:manifest:live", b"value").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();
        assert!(sstable_manifest_current_path(&sstable_dir).exists());
        assert!(sstable_manifest_path(&sstable_dir).exists());
        let manifest_replay = manifest_log::replay_current_manifest(&sstable_dir).unwrap();

        let live_ids = storage
            .sstables
            .read()
            .unwrap()
            .iter()
            .map(|sstable| sstable.id)
            .collect::<Vec<_>>();
        assert_eq!(live_ids.len(), 1);
        assert_eq!(
            manifest_replay
                .edit_replay
                .state
                .files
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            live_ids,
            "new SSTable manifests should be written through manifest v2 replayable records"
        );
        let orphan_id = 9_999;
        let source_path = sstable_dir.join(sstable_file_name_for_id(live_ids[0]));
        let orphan_path = sstable_dir.join(sstable_file_name_for_id(orphan_id));
        std::fs::copy(&source_path, &orphan_path).unwrap();
        assert!(orphan_path.exists());
        let descriptor_cache_path = sstable_descriptor_cache_path(&sstable_dir);
        assert!(descriptor_cache_path.exists());
        drop(storage);
        std::fs::remove_file(&descriptor_cache_path).unwrap();
        let manifest_live_files = SstableManifest::load_live_files(&sstable_dir).unwrap();
        assert_eq!(
            manifest_live_files
                .iter()
                .map(|file| file.id)
                .collect::<Vec<_>>(),
            live_ids
        );
        assert!(
            manifest_live_files
                .iter()
                .all(|file| file.descriptor.is_some()),
            "v2 manifest should carry startup descriptors without the derived descriptor cache"
        );

        let reopened = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();
        let reopened_ids = reopened
            .sstables
            .read()
            .unwrap()
            .iter()
            .map(|sstable| sstable.id)
            .collect::<Vec<_>>();
        assert_eq!(
            reopened_ids, live_ids,
            "valid manifest should ignore orphan SSTables that are not listed"
        );

        let txn = reopened.begin_transaction().await.unwrap();
        assert_eq!(
            txn.get(b"data:manifest:live").await.unwrap(),
            Some(b"value".to_vec())
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_flush_appends_manifest_version_edits_to_existing_manifest() {
        let data_dir = unique_storage_dir("append_manifest_flush");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let sstable_dir = config.sstable_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        for batch in 0..3 {
            let mut txn = storage.begin_transaction().await.unwrap();
            for id in 0..16 {
                let key = format!("data:manifest_append:{batch}:{id:02}");
                txn.put(key.as_bytes(), b"value").await.unwrap();
            }
            txn.commit().await.unwrap();
            storage.create_snapshot_now().await.unwrap();
        }

        let current = manifest_log::read_current_file(&sstable_dir).unwrap();
        assert_eq!(current.file_number, 1);
        assert!(!manifest_log::manifest_path(&sstable_dir, 2).exists());

        let replay = manifest_log::replay_current_manifest(&sstable_dir).unwrap();
        assert!(matches!(
            replay.edit_replay.edits.first(),
            Some(ManifestEdit::Snapshot { .. })
        ));
        let version_edit_count = replay
            .edit_replay
            .edits
            .iter()
            .filter(|edit| matches!(edit, ManifestEdit::VersionEdit { .. }))
            .count();
        assert_eq!(version_edit_count, 2);
        let live_ids = storage
            .sstables
            .read()
            .unwrap()
            .iter()
            .map(|sstable| sstable.id)
            .collect::<Vec<_>>();
        assert_eq!(
            replay
                .edit_replay
                .state
                .files
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            live_ids
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_reopen_accepts_legacy_json_sstable_manifest() {
        let data_dir = unique_storage_dir("legacy_json_manifest");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let sstable_dir = config.sstable_path();

        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();
        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:manifest:legacy", b"value").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        let live_sstables = storage.sstables.read().unwrap().clone();
        let legacy_manifest = SstableManifest::from_sstables(&live_sstables).unwrap();
        std::fs::write(
            sstable_manifest_path(&sstable_dir),
            serde_json::to_vec_pretty(&legacy_manifest).unwrap(),
        )
        .unwrap();
        std::fs::write(
            sstable_manifest_current_path(&sstable_dir),
            format!("{SSTABLE_MANIFEST_FILE}\n"),
        )
        .unwrap();

        let reopened = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();
        let txn = reopened.begin_transaction().await.unwrap();
        assert_eq!(
            txn.get(b"data:manifest:legacy").await.unwrap(),
            Some(b"value".to_vec())
        );
        drop(txn);

        {
            let mut txn = reopened.begin_transaction().await.unwrap();
            txn.put(b"data:manifest:legacy:next", b"value")
                .await
                .unwrap();
            txn.commit().await.unwrap();
        }
        reopened.create_snapshot_now().await.unwrap();
        let current = manifest_log::read_current_file(&sstable_dir).unwrap();
        assert_eq!(current.file_number, 2);
        let replay = manifest_log::replay_current_manifest(&sstable_dir).unwrap();
        assert!(matches!(
            replay.edit_replay.edits.first(),
            Some(ManifestEdit::Snapshot { .. })
        ));
        assert_eq!(
            replay
                .edit_replay
                .state
                .files
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            reopened
                .sstables
                .read()
                .unwrap()
                .iter()
                .map(|sstable| sstable.id)
                .collect::<Vec<_>>()
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_compaction_updates_manifest_live_sstable_list() {
        let data_dir = unique_storage_dir("compaction_manifest_live");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let sstable_dir = config.sstable_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        for batch in 0..4 {
            let mut txn = storage.begin_transaction().await.unwrap();
            for id in 0..16 {
                let key = format!("data:manifest_compact:{batch}:{id:02}");
                txn.put(key.as_bytes(), b"value").await.unwrap();
            }
            txn.commit().await.unwrap();
            storage.create_snapshot_now().await.unwrap();
        }

        let old_ids = storage
            .sstables
            .read()
            .unwrap()
            .iter()
            .map(|sstable| sstable.id)
            .collect::<Vec<_>>();
        assert_eq!(old_ids.len(), COMPACTION_FANIN);
        let version_edits_before_compaction = manifest_log::replay_current_manifest(&sstable_dir)
            .unwrap()
            .edit_replay
            .edits
            .iter()
            .filter(|edit| matches!(edit, ManifestEdit::VersionEdit { .. }))
            .count();
        assert!(storage.compact_once().await.unwrap());

        let live_ids = storage
            .sstables
            .read()
            .unwrap()
            .iter()
            .map(|sstable| sstable.id)
            .collect::<Vec<_>>();
        let manifest_ids = SstableManifest::load_live_files(&sstable_dir)
            .expect("manifest should remain valid after compaction")
            .into_iter()
            .map(|file| file.id)
            .collect::<Vec<_>>();
        assert_eq!(manifest_ids, live_ids);
        let current_manifest = manifest_log::read_current_file(&sstable_dir).unwrap();
        assert_eq!(
            current_manifest.file_number, 1,
            "normal flush and compaction should append VersionEdit records to the current MANIFEST"
        );
        let replay = manifest_log::replay_current_manifest(&sstable_dir).unwrap();
        let version_edit_count = replay
            .edit_replay
            .edits
            .iter()
            .filter(|edit| matches!(edit, ManifestEdit::VersionEdit { .. }))
            .count();
        assert_eq!(version_edit_count, version_edits_before_compaction + 1);
        let replay_ids = replay
            .edit_replay
            .state
            .files
            .keys()
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(replay_ids, live_ids);
        for old_id in old_ids {
            assert!(
                !manifest_ids.contains(&old_id),
                "compaction manifest should not list obsolete SSTable {old_id}"
            );
        }

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_reopen_fails_when_manifest_references_missing_sstable() {
        let data_dir = unique_storage_dir("manifest_missing_sstable");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();

        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();
        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:manifest:missing", b"value").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();
        let sstable_path = storage.sstables.read().unwrap()[0].path.clone();
        std::fs::remove_file(&sstable_path).unwrap();

        let reopened = FusionStorage::with_config(&wal_path.to_string_lossy(), &config).await;
        assert!(
            reopened.is_err(),
            "manifest-referenced SSTable loss must not silently fall back to directory scan"
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_reopen_returns_error_for_corrupt_wal() {
        let data_dir = unique_storage_dir("corrupt_wal_startup");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        std::fs::write(&wal_path, [255u8]).unwrap();

        let reopened = FusionStorage::with_config(&wal_path.to_string_lossy(), &config).await;
        assert!(
            reopened.is_err(),
            "corrupt WAL must return a startup error instead of panicking or recovering silently"
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_shutdown_does_not_create_empty_sstable() {
        let data_dir = unique_storage_dir("shutdown_empty_sstable");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        storage.shutdown().await;

        let sstable_count = std::fs::read_dir(config.sstable_path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "sst"))
            .count();
        assert_eq!(sstable_count, 0);

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_get_uses_latest_mvcc_timestamp_after_compaction() {
        let data_dir = unique_storage_dir("get_after_compaction");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:compact_get:001", b"old").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        for id in 0..3 {
            let mut txn = storage.begin_transaction().await.unwrap();
            let key = format!("data:compact_get:filler:{id}");
            txn.put(key.as_bytes(), b"filler").await.unwrap();
            txn.commit().await.unwrap();
            storage.create_snapshot_now().await.unwrap();
        }

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:compact_get:001", b"new").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        assert!(storage.compact_once().await.unwrap());

        let txn = storage.begin_transaction().await.unwrap();
        assert_eq!(
            txn.get(b"data:compact_get:001").await.unwrap(),
            Some(b"new".to_vec())
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_read_ts_registration_closes_compaction_watermark_race() {
        let (storage, data_dir) = test_fusion_storage("read_ts_registration_barrier").await;
        const SNAPSHOT_TS: u64 = 41;
        const NEXT_COMMIT_TS: u64 = 42;
        storage.current_ts.store(SNAPSHOT_TS, Ordering::SeqCst);

        let read_ts = storage.register_current_read_ts_with(|loaded_read_ts| {
            assert_eq!(loaded_read_ts, SNAPSHOT_TS);

            // Reproduce the old race window: a commit publishes a newer timestamp
            // after begin_transaction has selected its snapshot but before that
            // snapshot is entered in the active-reader registry.
            storage.current_ts.store(NEXT_COMMIT_TS, Ordering::SeqCst);

            // Compaction samples the registry through this same mutex. It must be
            // unable to observe the pre-registration window while the selected
            // snapshot is stale relative to current_ts.
            match storage.active_read_timestamps.try_lock() {
                Err(std::sync::TryLockError::WouldBlock) => {}
                Ok(_) => panic!("compaction watermark barrier was not held during registration"),
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    panic!("active read timestamp registry was poisoned")
                }
            }
        });

        assert_eq!(read_ts, SNAPSHOT_TS);
        assert_eq!(
            storage.oldest_active_read_ts(),
            Some(SNAPSHOT_TS),
            "the first compaction sample after the barrier must retain the stale snapshot floor"
        );
        storage.unregister_active_read_ts(read_ts);
        assert_eq!(storage.oldest_active_read_ts(), None);

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_compaction_preserves_versions_visible_to_existing_snapshot() {
        let data_dir = unique_storage_dir("snapshot_reads_after_compaction");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:compact_snapshot:001", b"old").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        let long_txn = storage.begin_transaction().await.unwrap();
        assert_eq!(
            long_txn.get(b"data:compact_snapshot:001").await.unwrap(),
            Some(b"old".to_vec())
        );

        {
            let mut txn = storage.begin_transaction().await.unwrap();
            txn.put(b"data:compact_snapshot:001", b"new").await.unwrap();
            txn.commit().await.unwrap();
        }
        storage.create_snapshot_now().await.unwrap();

        for id in 0..2 {
            let mut txn = storage.begin_transaction().await.unwrap();
            let key = format!("data:compact_snapshot:filler:{id}");
            txn.put(key.as_bytes(), b"filler").await.unwrap();
            txn.commit().await.unwrap();
            storage.create_snapshot_now().await.unwrap();
        }

        assert!(
            storage.sstables.read().unwrap().len() >= COMPACTION_FANIN,
            "test setup should create enough SSTables to trigger compaction"
        );
        assert!(storage.compact_once().await.unwrap());

        let fresh_txn = storage.begin_transaction().await.unwrap();
        assert_eq!(
            fresh_txn.get(b"data:compact_snapshot:001").await.unwrap(),
            Some(b"new".to_vec())
        );
        assert_eq!(
            long_txn.get(b"data:compact_snapshot:001").await.unwrap(),
            Some(b"old".to_vec()),
            "compaction must not drop versions that remain visible to an existing transaction"
        );

        drop(fresh_txn);
        drop(long_txn);

        let dropped_before = crate::monitor::GLOBAL_METRICS
            .compaction_dropped_version_count
            .load(Ordering::Relaxed);
        for id in 2..5 {
            let mut txn = storage.begin_transaction().await.unwrap();
            let key = format!("data:compact_snapshot:filler:{id}");
            txn.put(key.as_bytes(), b"filler").await.unwrap();
            txn.commit().await.unwrap();
            storage.create_snapshot_now().await.unwrap();
        }
        assert!(storage.compact_once().await.unwrap());
        let dropped_delta = crate::monitor::GLOBAL_METRICS
            .compaction_dropped_version_count
            .load(Ordering::Relaxed)
            .saturating_sub(dropped_before);
        assert!(
            dropped_delta >= 1,
            "after active readers release, compaction should safely drop obsolete versions"
        );

        let stale_txn = FusionTransaction {
            storage: storage.clone(),
            write_buffer: transaction_write_buffer(),
            read_ts: 1,
            read_ts_registered: false,
            capture_cdc: AtomicBool::new(true),
            side_index_deltas: std::sync::Mutex::new(Vec::new()),
            fenced_migration_phase: None,
        };
        assert_eq!(
            stale_txn.get(b"data:compact_snapshot:001").await.unwrap(),
            None,
            "old versions should be eligible for GC after no transaction can read them"
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_immutable_memtable_prefix_scan_covers_all_large_fbtree_keys() {
        let data_dir = unique_storage_dir("large_immutable_prefix_scan");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        let mem = MemTable::new(42);
        let prefix = b"data:fbtree_scan:";
        for id in 0..10_000 {
            let key = format!("data:fbtree_scan:{id:08}");
            mem.insert(
                FusionStorage::encode_key(key.as_bytes(), 1),
                FusionStorage::encode_value(true, b"value"),
            );
        }
        mem.build_fbtree();

        storage.current_ts.store(1, Ordering::SeqCst);
        storage.immutable_memtables.write().unwrap().push(mem);

        let txn = storage.begin_transaction().await.unwrap();
        assert_eq!(txn.count_prefix(prefix).await.unwrap(), 10_000);

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn scan_prefix_parallel_matches_serial_across_split_boundaries() {
        use crate::common::encoding::encode_i64_comparable;

        let data_dir = unique_storage_dir("parallel_scan_equiv");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        let prefix = b"data:pscan:";
        let n: i64 = 20_000; // > the 8192 parallel threshold

        // Base versions at ts=1 in an immutable memtable, then flushed to an SSTable so the parallel
        // sub-ranges exercise the SSTable iterator path (the real full-scan scenario).
        let base = MemTable::new(1);
        for id in 0..n {
            let key = format!("data:pscan:{}", encode_i64_comparable(id));
            base.insert(
                FusionStorage::encode_key(key.as_bytes(), 1),
                FusionStorage::encode_value(true, format!("v{id}").as_bytes()),
            );
        }
        storage.immutable_memtables.write().unwrap().push(base);
        storage.flush_all_immutable_memtables().await.unwrap();

        // Overwrites (ts=2) and one tombstone in the active memtable — exercises MVCC dedup that must
        // resolve identically regardless of which sub-range a key falls into.
        {
            let active = storage.active_memtable.read().unwrap();
            for id in (0..n).step_by(997) {
                let key = format!("data:pscan:{}", encode_i64_comparable(id));
                active.insert(
                    FusionStorage::encode_key(key.as_bytes(), 2),
                    FusionStorage::encode_value(true, b"updated"),
                );
            }
            let dk = format!("data:pscan:{}", encode_i64_comparable(12_345));
            active.insert(
                FusionStorage::encode_key(dk.as_bytes(), 2),
                FusionStorage::encode_value(false, b""),
            );
        }
        storage.current_ts.store(2, Ordering::SeqCst);

        let txn = storage.begin_transaction().await.unwrap();
        let serial = txn.scan_prefix(prefix, None).await.unwrap();
        let parallel = txn.scan_prefix_parallel(prefix, None).await.unwrap();

        // Parallel scan must be byte-for-byte identical (same rows, same key order) to the serial scan.
        assert_eq!(
            serial, parallel,
            "parallel range-merge must equal the serial scan"
        );
        assert_eq!(
            serial.len(),
            (n as usize) - 1,
            "one id was tombstoned at ts=2"
        );

        struct Collector {
            rows: Vec<(Vec<u8>, Vec<u8>)>,
            stop_after: Option<usize>,
        }

        impl ScanVisitor for Collector {
            fn visit(&mut self, key: &[u8], value: &[u8]) -> bool {
                self.rows.push((key.to_vec(), value.to_vec()));
                self.stop_after
                    .map_or(true, |limit| self.rows.len() < limit)
            }
        }

        let mut collector = Collector {
            rows: Vec::new(),
            stop_after: None,
        };
        let visited = txn
            .scan_prefix_parallel_for_each(prefix, None, &mut collector)
            .await
            .unwrap();
        assert_eq!(visited, Some(serial.len()));
        assert_eq!(collector.rows, serial);

        let mut early_stop = Collector {
            rows: Vec::new(),
            stop_after: Some(5),
        };
        let visited = txn
            .scan_prefix_parallel_for_each(prefix, None, &mut early_stop)
            .await
            .unwrap();
        assert_eq!(visited, Some(5));
        assert_eq!(
            early_stop.rows,
            serial.iter().take(5).cloned().collect::<Vec<_>>()
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn scan_prefix_parallel_handles_full_i64_id_span_without_overflow() {
        use crate::common::encoding::encode_i64_comparable;

        let data_dir = unique_storage_dir("parallel_scan_wide_span");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        let prefix = b"data:wspan:";
        // Ids spanning the full i64 range so `max_u - min_u` approaches u64::MAX. The interpolation
        // must use u128 internally; multiplying `span * shard` in u64 would panic (debug) or wrap
        // into out-of-order boundaries (release) -> duplicated/missing rows.
        let ids: [i64; 8] = [
            i64::MIN,
            i64::MIN + 1,
            -3_000_000_000,
            -1,
            0,
            1,
            6_637_030_065_269_067_181,
            i64::MAX,
        ];
        let mem = MemTable::new(1);
        for id in ids {
            let key = format!("data:wspan:{}", encode_i64_comparable(id));
            mem.insert(
                FusionStorage::encode_key(key.as_bytes(), 1),
                FusionStorage::encode_value(true, b"v"),
            );
        }
        storage.immutable_memtables.write().unwrap().push(mem);
        storage.current_ts.store(1, Ordering::SeqCst);

        let txn = storage.begin_transaction().await.unwrap();
        let serial = txn.scan_prefix(prefix, None).await.unwrap();
        let parallel = txn.scan_prefix_parallel(prefix, None).await.unwrap();
        assert_eq!(
            serial, parallel,
            "wide-span parallel scan must equal serial (no overflow-induced gaps/dupes)"
        );
        assert_eq!(serial.len(), ids.len(), "all rows returned exactly once");

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fusion_compaction_defers_obsolete_sstable_delete_until_readers_drop() {
        let data_dir = unique_storage_dir("obsolete_sstable_readers");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .unwrap();

        for batch in 0..4 {
            let mut txn = storage.begin_transaction().await.unwrap();
            for id in 0..32 {
                let key = format!("data:obsolete:{batch}:{id:02}");
                txn.put(key.as_bytes(), b"value").await.unwrap();
            }
            txn.commit().await.unwrap();
            storage.create_snapshot_now().await.unwrap();
        }

        let held_sstable = storage.sstables.read().unwrap()[0].clone();
        let held_id = held_sstable.id;
        let held_path = held_sstable.path.clone();
        assert!(held_path.exists());

        assert!(storage.compact_once().await.unwrap());
        assert!(!storage
            .sstables
            .read()
            .unwrap()
            .iter()
            .any(|sst| sst.id == held_id));
        assert!(
            held_path.exists(),
            "obsolete SSTable file must stay readable while a reader holds its Arc"
        );

        drop(held_sstable);
        storage.collect_obsolete_sstables().await;
        assert!(
            !held_path.exists(),
            "obsolete SSTable file should be deleted after readers release it"
        );

        cleanup_storage_dir(&data_dir);
    }

    #[test]
    fn obsolete_sstable_path_buffer_reserves_current_obsolete_len() {
        let paths = obsolete_sstable_path_buffer(4);
        assert!(paths.capacity() >= 4);
    }

    #[test]
    fn sstable_live_file_buffer_preallocates_first_file() {
        let files = sstable_live_file_buffer();
        assert!(files.capacity() >= 1);
    }

    #[test]
    fn sstable_handle_buffer_preallocates_first_sstable() {
        let sstables = sstable_handle_buffer();
        assert!(sstables.capacity() >= 1);
    }

    #[test]
    fn sstable_file_name_for_id_preallocates_exact_name() {
        let name = sstable_file_name_for_id(42);

        assert_eq!(name, "42.sst");
        assert!(name.capacity() >= name.len());
    }

    #[test]
    fn fusion_u64_decimal_len_counts_digits() {
        assert_eq!(u64_decimal_len(0), 1);
        assert_eq!(u64_decimal_len(9), 1);
        assert_eq!(u64_decimal_len(10), 2);
        assert_eq!(u64_decimal_len(u64::MAX), 20);
    }

    #[test]
    fn immutable_memtable_buffer_preallocates_first_flush() {
        let memtables = immutable_memtable_buffer();
        assert!(memtables.capacity() >= 1);
    }

    #[test]
    fn obsolete_sstable_buffer_preallocates_first_compaction_output() {
        let sstables = obsolete_sstable_buffer();
        assert!(sstables.capacity() >= 1);
    }

    // ---- Data V2 migration phase: crash matrix + commit fence (P10-2.1) ----

    fn migration_record(phase: DataMigrationPhase, phase_seq: u64) -> DataMigrationPhaseRecord {
        DataMigrationPhaseRecord {
            phase,
            phase_seq,
            updated_at_unix_ms: 42,
        }
    }

    async fn commit_migration_record(
        storage: &FusionStorage,
        record: &DataMigrationPhaseRecord,
    ) -> Result<()> {
        let mut txn = storage.begin_transaction().await?;
        txn.put(migration_phase_key(), &record.encode()).await?;
        txn.commit().await
    }

    async fn read_migration_record(storage: &FusionStorage) -> Option<DataMigrationPhaseRecord> {
        let txn = storage.begin_transaction().await.unwrap();
        let raw = txn.get(migration_phase_key()).await.unwrap();
        raw.map(|raw| DataMigrationPhaseRecord::decode(&raw).unwrap())
    }

    #[tokio::test]
    async fn migration_phase_commit_rolls_back_on_wal_failure_and_reopens_clean() {
        let data_dir = unique_storage_dir("migration_phase_wal_fault");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();

        {
            let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
                .await
                .unwrap();
            storage
                .wal
                .inject_faults(&[crate::storage::wal::WalFaultPoint::AfterWrite]);
            let error = commit_migration_record(
                &storage,
                &migration_record(DataMigrationPhase::DeleteOnly, 1),
            )
            .await
            .expect_err("WAL append failure must abort the phase-record commit");
            assert!(error.to_string().contains("WAL"), "unexpected: {error}");
            assert_eq!(read_migration_record(&storage).await, None);
            // The fence must not observe an aborted advance: it stays at the
            // startup-primed no-record default (seq 0), never seq 1.
            assert_eq!(
                storage.data_migration_fence().cached(),
                Some(FenceSnapshot {
                    phase: DataMigrationPhase::DeleteOnly,
                    phase_seq: 0
                })
            );
        }

        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .expect("reopen after aborted phase commit");
        assert_eq!(read_migration_record(&storage).await, None);
        commit_migration_record(
            &storage,
            &migration_record(DataMigrationPhase::DeleteOnly, 1),
        )
        .await
        .expect("clean retry after rollback");
        assert_eq!(
            read_migration_record(&storage).await,
            Some(migration_record(DataMigrationPhase::DeleteOnly, 1))
        );
        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn migration_phase_torn_tail_replay_recovers_prior_phase() {
        let data_dir = unique_storage_dir("migration_phase_torn_tail");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();

        let size_after_first;
        let size_after_second;
        {
            let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
                .await
                .unwrap();
            commit_migration_record(
                &storage,
                &migration_record(DataMigrationPhase::DeleteOnly, 1),
            )
            .await
            .unwrap();
            size_after_first = std::fs::metadata(&wal_path).unwrap().len();
            commit_migration_record(
                &storage,
                &migration_record(DataMigrationPhase::WriteDeleteShadow, 2),
            )
            .await
            .unwrap();
            size_after_second = std::fs::metadata(&wal_path).unwrap().len();
        }
        assert!(size_after_second > size_after_first);

        // Cut into the middle of the second (advance) batch record: a torn
        // tail, exactly what a crash mid-append leaves behind.
        let torn_len = size_after_first + (size_after_second - size_after_first) / 2;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .unwrap();
        file.set_len(torn_len).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .expect("reopen with torn advance tail");
        assert_eq!(
            read_migration_record(&storage).await,
            Some(migration_record(DataMigrationPhase::DeleteOnly, 1)),
            "the torn advance must be dropped; the prior durable phase survives"
        );
        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn reopen_refuses_phase_record_beyond_binary_support() {
        let data_dir = unique_storage_dir("migration_phase_unsupported");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();

        {
            let storage = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
                .await
                .unwrap();
            // Plant a decode-valid record above this binary's support (the
            // SQL surface cannot reach it; this simulates a store touched by
            // a newer binary).
            commit_migration_record(
                &storage,
                &migration_record(DataMigrationPhase::Validated, 4),
            )
            .await
            .unwrap();
        }

        let error = match FusionStorage::with_config(&wal_path.to_string_lossy(), &config).await {
            Ok(_) => panic!("open must refuse a phase beyond this binary's support"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("only supports"),
            "unexpected: {error}"
        );
        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn commit_rejects_malformed_phase_record_and_deletion() {
        let (storage, data_dir) = test_fusion_storage("migration_phase_malformed").await;

        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(migration_phase_key(), b"junk").await.unwrap();
        let error = txn
            .commit()
            .await
            .expect_err("malformed phase record must not become durable");
        assert!(
            error.to_string().contains("invalid length"),
            "unexpected: {error}"
        );

        let mut txn = storage.begin_transaction().await.unwrap();
        txn.delete(migration_phase_key()).await.unwrap();
        let error = txn
            .commit()
            .await
            .expect_err("phase record deletion must be rejected");
        assert!(
            error.to_string().contains("never be deleted"),
            "unexpected: {error}"
        );

        assert_eq!(read_migration_record(&storage).await, None);
        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn commit_fence_aborts_stale_pin_after_advance() {
        let (storage, data_dir) = test_fusion_storage("migration_phase_fence_race").await;

        // In-flight transaction pins the no-record fence (seq 0).
        let mut stale = storage.begin_transaction().await.unwrap();
        stale
            .fence_data_migration_phase(DataMigrationPhase::DeleteOnly.as_byte(), 0)
            .await
            .unwrap();
        stale.put(b"data:fence_race:1", b"stale").await.unwrap();

        // A concurrent INIT commits and publishes the new fence.
        commit_migration_record(
            &storage,
            &migration_record(DataMigrationPhase::DeleteOnly, 1),
        )
        .await
        .unwrap();

        let error = stale
            .commit()
            .await
            .expect_err("a commit serialized after an advance must abort on its stale pin");
        assert!(
            error.to_string().contains("migration phase advanced"),
            "unexpected: {error}"
        );

        // The retry (new fence) succeeds.
        let mut retry = storage.begin_transaction().await.unwrap();
        retry
            .fence_data_migration_phase(DataMigrationPhase::DeleteOnly.as_byte(), 1)
            .await
            .unwrap();
        retry.put(b"data:fence_race:1", b"fresh").await.unwrap();
        retry.commit().await.unwrap();

        // A second fence with a different value inside one transaction is an
        // immediate loud error (multi-statement transaction crossing an
        // advance).
        let mut crossing = storage.begin_transaction().await.unwrap();
        crossing
            .fence_data_migration_phase(DataMigrationPhase::DeleteOnly.as_byte(), 1)
            .await
            .unwrap();
        let error = crossing
            .fence_data_migration_phase(DataMigrationPhase::WriteDeleteShadow.as_byte(), 2)
            .await
            .expect_err("a changed fence within one transaction must error immediately");
        assert!(
            error.to_string().contains("changed within transaction"),
            "unexpected: {error}"
        );

        cleanup_storage_dir(&data_dir);
    }

    #[tokio::test]
    async fn fence_reload_after_invalidate_reads_durable_record() {
        let (storage, data_dir) = test_fusion_storage("migration_phase_reload").await;
        commit_migration_record(
            &storage,
            &migration_record(DataMigrationPhase::WriteDeleteShadow, 1),
        )
        .await
        .unwrap();
        assert_eq!(
            storage.data_migration_fence().cached(),
            Some(FenceSnapshot {
                phase: DataMigrationPhase::WriteDeleteShadow,
                phase_seq: 1
            })
        );

        storage.data_migration_fence().invalidate();
        assert_eq!(storage.data_migration_fence().cached(), None);
        assert_eq!(
            storage.reload_data_migration_fence().await.unwrap(),
            FenceSnapshot {
                phase: DataMigrationPhase::WriteDeleteShadow,
                phase_seq: 1
            }
        );
        cleanup_storage_dir(&data_dir);
    }
}
