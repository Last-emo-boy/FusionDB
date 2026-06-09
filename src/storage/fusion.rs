use super::columnar::ColumnarVectorStore;
use super::wal::{WalEntry, WalManager};
use super::{ScanVisitor, Storage, Transaction};
use crate::common::Result;
use crate::config::StorageConfig;
use async_trait::async_trait;
use crossbeam_skiplist::SkipMap;
use moka::sync::Cache;
use std::fmt::Write as FmtWrite;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::{Mutex as AsyncMutex, Notify};

// Fusion Storage Engine
// Combines:
// 1. MVCC (Lock-free reads, Snapshot Isolation)
// 2. LSM-Tree Structure (MemTable -> Flush -> SST)
// 3. Columnar Vector Store (Integrated for Vector Search)

const TS_SIZE: usize = 8;
const COMPACTION_FANIN: usize = 4;
const SSTABLE_BLOCK_BUFFER_CAPACITY: usize = 4096;

fn obsolete_sstable_path_buffer(capacity: usize) -> Vec<PathBuf> {
    Vec::with_capacity(capacity)
}

fn sstable_file_candidate_buffer() -> Vec<(u64, PathBuf)> {
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

fn vector_rebuild_data_prefix_for_table(table_name: &str) -> String {
    let mut prefix = String::with_capacity("data:".len() + table_name.len() + 1);
    prefix.push_str("data:");
    prefix.push_str(table_name);
    prefix.push(':');
    prefix
}

fn vector_rebuild_hnsw_index_name_for_column(table_name: &str, column_name: &str) -> String {
    let mut name = String::with_capacity("hnsw_".len() + table_name.len() + 1 + column_name.len());
    name.push_str("hnsw_");
    name.push_str(table_name);
    name.push('_');
    name.push_str(column_name);
    name
}

use crate::storage::inverted_index::InvertedIndex;
use crate::storage::sstable::{SsTable, SsTableBuilder};
use crate::storage::vector_index::VectorIndex;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::path::{Path, PathBuf};

struct MergeItem {
    key: Vec<u8>,
    val: Vec<u8>,
    iter_idx: usize,
}

impl PartialEq for MergeItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
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
        other.key.cmp(&self.key)
    }
}

fn merge_heap(capacity: usize) -> BinaryHeap<MergeItem> {
    BinaryHeap::with_capacity(capacity)
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

    // Block Cache for SSTables (SST ID, Offset) -> Block Data
    block_cache: Arc<Cache<(u64, u64), Vec<u8>>>,
    memtable_threshold: usize,
    compaction_lock: Arc<AsyncMutex<()>>,
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

        // Block Cache (e.g. 100MB capacity)
        // Note: moka Cache size is number of entries by default.
        // To limit by bytes, we need weigher.
        // Let's assume average block 4KB. 100MB = 25,000 blocks.
        let block_cache = Arc::new(Cache::new(config.block_cache_capacity));

        // Load existing SSTables
        let mut sstables_vec = sstable_handle_buffer();
        let sst_dir = sstable_dir.as_path();
        if sst_dir.exists() {
            if let Ok(mut entries) = std::fs::read_dir(sst_dir) {
                let mut files = sstable_file_candidate_buffer();
                while let Some(Ok(entry)) = entries.next() {
                    let path = entry.path();
                    if let Some(ext) = path.extension() {
                        if ext == "sst" {
                            if let Some(stem) = path.file_stem() {
                                if let Ok(id) = stem.to_string_lossy().parse::<u64>() {
                                    files.push((id, path));
                                }
                            }
                        }
                    }
                }
                files.sort_by_key(|k| k.0);

                sstables_vec.reserve(files.len());
                for (id, path) in files {
                    if let Ok(sst) = SsTable::open(path, id, block_cache.clone()).await {
                        sstables_vec.push(Arc::new(sst));
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
        let mut max_sstable_ts = 0;
        for sst in &sstables_vec {
            match sst.new_iterator(None).await {
                Ok(mut iter) => {
                    while let Ok(Some((key, _))) = iter.next().await {
                        if key.len() >= TS_SIZE {
                            let (_, ts) = Self::decode_key(&key);
                            max_sstable_ts = max_sstable_ts.max(ts);
                        }
                    }
                }
                Err(e) => {
                    eprintln!(
                        "Warning: failed to scan SSTable {} for timestamp restore: {:?}",
                        sst.id, e
                    );
                }
            }
        }

        // Replay WAL
        // We need to replay committed transactions into the active memtable.
        let replay_entries = wal.replay().expect("Critical: Failed to replay WAL");
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
            trigram_index: Arc::new(RwLock::new(
                crate::storage::trigram::TrigramIndex::load(&paths.trigram_index_path)
                    .unwrap_or_else(|_| crate::storage::trigram::TrigramIndex::new()),
            )),
            block_cache,
            memtable_threshold,
            compaction_lock: Arc::new(AsyncMutex::new(())),
            paths,
        };

        // Apply Replay
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
        let restored_ts = max_sstable_ts.max(max_replay_ts);
        storage.current_ts.store(restored_ts, Ordering::SeqCst);

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

        // Rebuild Vector Index in background
        let s3 = storage.clone();
        tokio::spawn(async move {
            s3.rebuild_vector_index().await;
        });

        Ok(storage)
    }

    pub fn update_columnar_store(&self, ids: Vec<String>, vectors: Vec<Vec<f32>>) {
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

    pub fn vector_search(&self, query: &[f32], limit: usize) -> Vec<(String, f32)> {
        // Use HNSW Index
        self.vector_index
            .search("default", query, limit)
            .unwrap_or_default()
    }

    pub fn bm25_search(&self, query: &str, limit: usize) -> Vec<(String, f32)> {
        let guard = self.inverted_index.read().unwrap();
        // k1=1.2, b=0.75 are standard defaults
        guard.search_bm25_limited(query, 1.2, 0.75, limit)
    }

    // Hybrid Search: RRF (Reciprocal Rank Fusion)
    pub fn hybrid_search(
        &self,
        text_query: &str,
        vector_query: &[f32],
        limit: usize,
    ) -> Vec<(String, f32)> {
        if limit == 0 {
            return Vec::new();
        }

        // 1. Get results from both sources
        let text_results = self.bm25_search(text_query, limit * 2); // Get more candidates
        let vector_results = self.vector_search(vector_query, limit * 2);

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

    async fn flush_all_immutable_memtables(&self) {
        loop {
            let memtable_to_flush = {
                let mut imm = self.immutable_memtables.write().unwrap();
                imm.pop()
            };
            match memtable_to_flush {
                Some(mem) => {
                    self.flush_memtable_sync(&mem).await;
                }
                None => break,
            }
        }
    }

    pub async fn create_snapshot_now(&self) -> Result<()> {
        self.rotate_memtable().await;
        self.flush_all_immutable_memtables().await;
        self.persist_secondary_indexes("[snapshot]");
        self.wal.truncate()?;
        Ok(())
    }

    async fn rebuild_vector_index(&self) {
        println!("Rebuilding Vector Index from Storage...");
        let txn = match self.begin_transaction().await {
            Ok(t) => t,
            Err(e) => {
                eprintln!("Failed to begin transaction for rebuild: {:?}", e);
                return;
            }
        };

        let prefix = "schema:";
        let kv_pairs = match txn.scan_prefix(prefix.as_bytes(), None).await {
            Ok(kv) => kv,
            Err(e) => {
                eprintln!("Failed to scan schemas: {:?}", e);
                return;
            }
        };

        for (k, v) in kv_pairs {
            if let Ok(key_str) = std::str::from_utf8(&k) {
                if let Some(table_name) = key_str.strip_prefix(prefix) {
                    if let Ok(schema) = bincode::deserialize::<crate::catalog::TableSchema>(&v) {
                        let mut hnsw_cols = Vec::with_capacity(schema.columns.len());
                        for (idx, col) in schema.columns.iter().enumerate() {
                            if col.is_indexed && col.index_type == crate::catalog::IndexType::HNSW {
                                let idx_name = vector_rebuild_hnsw_index_name_for_column(
                                    table_name, &col.name,
                                );
                                self.vector_index.create_index(&idx_name);
                                hnsw_cols.push((idx, idx_name));
                            }
                        }

                        if hnsw_cols.is_empty() {
                            continue;
                        }

                        let data_prefix = vector_rebuild_data_prefix_for_table(table_name);
                        if let Ok(data_pairs) = txn.scan_prefix(data_prefix.as_bytes(), None).await
                        {
                            let mut batches: HashMap<String, Vec<(String, Vec<f32>)>> =
                                HashMap::with_capacity(hnsw_cols.len());

                            for (dk, dv) in data_pairs {
                                let Some(row_id) = std::str::from_utf8(&dk)
                                    .ok()
                                    .and_then(|key| key.rsplit(':').next())
                                    .map(|value| value.to_string())
                                else {
                                    continue;
                                };

                                for (col_idx, idx_name) in &hnsw_cols {
                                    if let Ok(Some(crate::common::Value::Vector(vec))) =
                                        crate::common::encoding::RowDecoder::decode_column(
                                            &dv, *col_idx,
                                        )
                                    {
                                        batches
                                            .entry(idx_name.clone())
                                            .or_default()
                                            .push((row_id.clone(), vec));
                                    }
                                }
                            }

                            for (idx_name, items) in batches {
                                let _ = self.vector_index.batch_insert(&idx_name, items);
                            }
                        }
                    }
                }
            }
        }
        println!("Vector Index Rebuild Complete.");
    }

    fn next_memtable_to_flush(&self) -> Option<MemTable> {
        let imm = self.immutable_memtables.read().unwrap();
        imm.last().cloned()
    }

    fn mark_memtable_flushed(&self, memtable_id: u64) {
        let mut imm = self.immutable_memtables.write().unwrap();
        if let Some(pos) = imm.iter().position(|candidate| candidate.id == memtable_id) {
            imm.remove(pos);
        }
    }

    async fn flush_loop(&self) {
        let _ = tokio::fs::create_dir_all(&self.paths.sstable_dir).await;

        loop {
            self.flush_notify.notified().await;

            let memtable_to_flush = self.next_memtable_to_flush();

            if let Some(mem) = memtable_to_flush {
                let sst_path = self.sstable_path_for(mem.id);
                let mut builder = SsTableBuilder::new(sst_path.clone());

                // Write memtable to builder
                // We reuse the logic from lsm.rs but applied to Fusion's MemTable
                // Fusion's MemTable stores Key+TS -> Value
                // SSTable doesn't care about encoding, just bytes.

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

                    builder.add_key(key); // Add to Bloom Filter

                    block_buffer.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    block_buffer.extend_from_slice(key);
                    block_buffer.extend_from_slice(&(val.len() as u32).to_le_bytes());
                    block_buffer.extend_from_slice(val);
                    block_count += 1;

                    if block_buffer.len() >= SSTABLE_BLOCK_BUFFER_CAPACITY {
                        if let Err(e) = builder
                            .flush_block(first_key.take().unwrap(), block_count, &block_buffer)
                            .await
                        {
                            eprintln!("Failed to flush block: {:?}", e);
                            break;
                        }
                        block_buffer.clear();
                        block_count = 0;
                    }
                }

                if !block_buffer.is_empty() {
                    if let Err(e) = builder
                        .flush_block(first_key.take().unwrap(), block_count, &block_buffer)
                        .await
                    {
                        eprintln!("Failed to flush last block: {:?}", e);
                    }
                }

                if let Err(e) = builder.finish().await {
                    eprintln!("Failed to flush sstable {}: {:?}", mem.id, e);
                    continue;
                }

                // Add a small delay and retry loop to allow file system to settle
                let mut attempts = 0;
                loop {
                    match SsTable::open(sst_path.clone(), mem.id, self.block_cache.clone()).await {
                        Ok(sst) => {
                            {
                                let mut sstables = self.sstables.write().unwrap();
                                sstables.push(Arc::new(sst));
                            }
                            self.mark_memtable_flushed(mem.id);
                            break;
                        }
                        Err(e) => {
                            attempts += 1;
                            if attempts >= 10 {
                                panic!("Critical: Failed to open flushed sstable {} after {} attempts: {:?}", mem.id, attempts, e);
                            }
                            eprintln!("Warning: Failed to open sstable {} (attempt {}): {:?}. Retrying...", mem.id, attempts, e);
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                    }
                }

                self.persist_secondary_indexes("[flush]");

                // WAL Truncation: if no more immutable memtables remain,
                // all data is persisted to SSTables, so we can truncate WAL.
                let remaining = {
                    let imm = self.immutable_memtables.read().unwrap();
                    imm.len()
                };
                if remaining == 0 {
                    if let Err(e) = self.wal.truncate() {
                        eprintln!("Failed to truncate WAL after flush: {:?}", e);
                    }
                }
            }
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
            let _ = tokio::fs::remove_file(path).await;
        }
    }

    async fn compact_once_inner(&self) -> Result<bool> {
        self.collect_obsolete_sstables().await;

        let Some(candidates) = self.compaction_candidates() else {
            return Ok(false);
        };

        let mut iterators = Vec::with_capacity(COMPACTION_FANIN);
        for sst in &candidates {
            iterators.push(sst.new_iterator(None).await?);
        }

        if iterators.is_empty() {
            return Ok(false);
        }

        // Output builder
        let new_id = self.next_memtable_id.fetch_add(1, Ordering::Relaxed);
        let out_path = self.sstable_path_for(new_id);
        let mut builder = SsTableBuilder::new(out_path.clone());

        // Merge Logic
        let mut heap = merge_heap(iterators.len());

        // Init heap
        for (idx, it) in iterators.iter_mut().enumerate() {
            if let Some((k, v)) = it.next().await? {
                heap.push(MergeItem {
                    key: k,
                    val: v,
                    iter_idx: idx,
                });
            }
        }

        let mut block_buffer = Vec::with_capacity(SSTABLE_BLOCK_BUFFER_CAPACITY);
        let mut block_count = 0;
        let mut first_key = None;
        let mut last_base_key: Option<Vec<u8>> = None;
        let mut dedup_count: u64 = 0;

        while let Some(item) = heap.pop() {
            let k = item.key;
            let v = item.val;
            let idx = item.iter_idx;

            if k.len() < TS_SIZE {
                if let Some((next_k, next_v)) = iterators[idx].next().await? {
                    heap.push(MergeItem {
                        key: next_k,
                        val: next_v,
                        iter_idx: idx,
                    });
                }
                continue;
            }

            // Dedup: for the same base key (without timestamp), keep only the latest version.
            // Keys come out of the min-heap sorted by [base_key][timestamp].
            // The first occurrence for a base key is the latest version (highest ts).
            let base_key = k[..k.len() - TS_SIZE].to_vec();
            let is_dup = last_base_key.as_ref() == Some(&base_key);
            if is_dup {
                dedup_count += 1;
                // Skip this older version, but still advance the iterator
                if let Some((next_k, next_v)) = iterators[idx].next().await? {
                    heap.push(MergeItem {
                        key: next_k,
                        val: next_v,
                        iter_idx: idx,
                    });
                }
                continue;
            }
            last_base_key = Some(base_key);

            // Add to Builder
            if first_key.is_none() {
                first_key = Some(k.clone());
            }

            builder.add_key(&k);
            block_buffer.extend_from_slice(&(k.len() as u32).to_le_bytes());
            block_buffer.extend_from_slice(&k);
            block_buffer.extend_from_slice(&(v.len() as u32).to_le_bytes());
            block_buffer.extend_from_slice(&v);
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
            if let Some((next_k, next_v)) = iterators[idx].next().await? {
                heap.push(MergeItem {
                    key: next_k,
                    val: next_v,
                    iter_idx: idx,
                });
            }
        }

        if dedup_count > 0 {
            println!(
                "[compaction] Deduplicated {} stale key versions",
                dedup_count
            );
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

        // Open new SST
        match SsTable::open(out_path, new_id, self.block_cache.clone()).await {
            Ok(new_sst) => {
                {
                    let mut sstables = self.sstables.write().unwrap();
                    // Remove old candidates (by ID)
                    let candidate_ids = candidates.each_ref().map(|candidate| candidate.id);
                    sstables.retain(|s| !candidate_ids.contains(&s.id));

                    // Insert new SST (sorted by ID)
                    sstables.push(Arc::new(new_sst));
                    sstables.sort_by_key(|s| s.id);
                } // Drop lock

                {
                    let mut obsolete = self.obsolete_sstables.write().unwrap();
                    for sst in candidates {
                        obsolete.push(sst);
                    }
                }
                self.collect_obsolete_sstables().await;

                Ok(true)
            }
            Err(e) => Err(crate::common::FusionError::Storage(format!(
                "Failed to open compacted SST: {:?}",
                e
            ))),
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

        // 1. Rotate active memtable to immutable
        self.rotate_memtable().await;

        // 2. Flush all immutable memtables
        self.flush_all_immutable_memtables().await;

        // 3. Save secondary indexes
        self.persist_secondary_indexes("[shutdown]");

        // 4. Truncate WAL (all data is now in SSTables)
        if let Err(e) = self.wal.truncate() {
            eprintln!("[shutdown] Failed to truncate WAL: {:?}", e);
        }

        println!("[shutdown] FusionDB shut down cleanly.");
    }

    /// Flush a single MemTable to SSTable (used during shutdown).
    async fn flush_memtable_sync(&self, mem: &MemTable) {
        let sst_path = self.sstable_path_for(mem.id);
        let mut builder = SsTableBuilder::new(sst_path.clone());

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
                if let Err(e) = builder
                    .flush_block(first_key.take().unwrap(), block_count, &block_buffer)
                    .await
                {
                    eprintln!("[shutdown] flush block error: {:?}", e);
                    return;
                }
                block_buffer.clear();
                block_count = 0;
            }
        }

        if !block_buffer.is_empty() {
            if let Some(fk) = first_key.take() {
                let _ = builder.flush_block(fk, block_count, &block_buffer).await;
            }
        }

        if let Err(e) = builder.finish().await {
            eprintln!("[shutdown] SSTable finish error: {:?}", e);
            return;
        }

        // Open and register
        match SsTable::open(sst_path, mem.id, self.block_cache.clone()).await {
            Ok(sst) => {
                let mut sstables = self.sstables.write().unwrap();
                sstables.push(Arc::new(sst));
            }
            Err(e) => eprintln!("[shutdown] Failed to open flushed SST: {:?}", e),
        }
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

pub struct FusionTransaction {
    pub storage: FusionStorage,
    pub write_buffer: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    pub read_ts: u64,
}

impl FusionTransaction {
    async fn for_each_visible_range<F>(&self, start: &[u8], end: &[u8], mut visit: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send,
    {
        let read_ts = self.read_ts;
        let start_ik = FusionStorage::encode_key(start, u64::MAX);

        // 1. Snapshot MemTables (Cheap Clone)
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

        // WriteBuffer
        let mut wb_latest = BTreeMap::new();
        for (k, v) in &self.write_buffer {
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

        let sstables = self.storage.sstables.read().unwrap().clone();

        // 2. Initialize Heap
        let mut heap = merge_heap(1 + mem_tables.len() + sstables.len());

        if let Some((k, v)) = wb_iter.next() {
            heap.push(MergeItem {
                key: k,
                val: v,
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
                    heap.push(MergeItem {
                        key: k.clone(),
                        val: v,
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
        let end_ik = FusionStorage::encode_key(end, u64::MAX);

        for (i, sst) in sstables.iter().rev().enumerate() {
            let idx = 1 + mem_tables.len() + i;

            // Check Overlap
            let sst_min = &sst.meta.first_key;
            let sst_max = &sst.meta.last_key;
            if sst_max.as_slice() < start_ik.as_slice() || sst_min.as_slice() >= end_ik.as_slice() {
                sst_iters.push(None);
                continue;
            }

            // Use seek optimization to jump to start key
            let mut it = sst.new_iterator(Some(&start_ik)).await?;
            if let Ok(Some((k, v))) = it.next().await {
                let current_k = k;
                let current_v = v;
                // Check if the first key we found is already past end
                // (This can happen if start_ik is not in SSTable and we landed on a key > end)
                if current_k >= start_ik {
                    let (uk, _) = FusionStorage::decode_key(&current_k);
                    if uk < end {
                        heap.push(MergeItem {
                            key: current_k,
                            val: current_v,
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
            let k = item.key;
            let v = item.val;
            let idx = item.iter_idx;

            let (user_k, ts) = FusionStorage::decode_key(&k);
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
                    heap.push(MergeItem {
                        key: nk,
                        val: nv,
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
                    heap.push(MergeItem {
                        key: nk,
                        val: nv,
                        iter_idx: idx,
                    });
                }
            } else {
                let sst_idx = idx - 1 - mem_tables.len();
                if let Some(it) = &mut sst_iters[sst_idx] {
                    while let Some((nk, nv)) = it.next().await? {
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
                        heap.push(MergeItem {
                            key: nk,
                            val: nv,
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
                let (is_put, val) = FusionStorage::decode_value(&v);
                if is_put && !visit(user_k, val) {
                    break;
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Transaction for FusionTransaction {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 1. Read-Your-Own-Writes
        for (k, v) in self.write_buffer.iter().rev() {
            if k == key {
                if let Ok(_s) = std::str::from_utf8(key) {
                    // if s.contains("9999999") {
                    //    eprintln!("DEBUG: FusionTransaction::get HIT WriteBuffer. Key: {}, ValLen: {}, Val: {:?}", s, v.as_ref().map(|x| x.len()).unwrap_or(0), v);
                    // }
                }
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
            if let Ok(Some((k_bytes, v_bytes))) = sst.find_ge(&search_key).await {
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
        // Optimized last() using metadata
        let read_ts = self.read_ts;

        // 1. Candidate from MemTables
        let mut max_key: Option<(Vec<u8>, Vec<u8>, u64)> = None;
        let update_max =
            |k: &[u8], v: &[u8], ts: u64, current_max: &mut Option<(Vec<u8>, Vec<u8>, u64)>| {
                if ts <= read_ts {
                    if let Some((mk, _, _)) = current_max {
                        if k > mk.as_slice() {
                            *current_max = Some((k.to_vec(), v.to_vec(), ts));
                        }
                    } else {
                        *current_max = Some((k.to_vec(), v.to_vec(), ts));
                    }
                }
            };

        // Scan MemTables (Active + Immutable)
        // Since MemTables are small, we can scan them.
        // Optimization: Iterate range? SkipMap range is forward only.
        // We use scan_range logic but scoped to MemTable.
        let end_ik = FusionStorage::encode_key(end, u64::MAX);
        let start_ik = FusionStorage::encode_key(start, u64::MAX);

        let check_mem = |mem: &MemTable, current_max: &mut Option<(Vec<u8>, Vec<u8>, u64)>| {
            for entry in mem.map.range(start_ik.clone()..end_ik.clone()) {
                let (k, ts) = FusionStorage::decode_key(entry.key());
                update_max(k, entry.value(), ts, current_max);
            }
        };

        {
            let active = self.storage.active_memtable.read().unwrap();
            check_mem(&active, &mut max_key);
        }
        {
            let imm = self.storage.immutable_memtables.read().unwrap();
            for mem in imm.iter() {
                check_mem(mem, &mut max_key);
            }
        }

        // 2. Candidate from SSTables (using Metadata)
        // We look for the SSTable that *could* contain the largest key.
        // Since SSTables might overlap, we need to check any SSTable whose range overlaps with (current_max_key..end).
        // If current_max is None, we check all SSTables in range (start..end).

        let sstables = self.storage.sstables.read().unwrap().clone();

        for sst in sstables.iter() {
            // Check if SSTable overlaps with [start, end)
            // SST range: [first_key, last_key] (Internal Keys)
            // We need to decode them to check User Keys?
            // Or just compare bytes? Internal Keys are UserKey + TS.
            // User Key comparison is prefix of Internal Key.
            // But TS is inverted.
            // Let's assume metadata stores INTERNAL keys.
            // We can check if sst.last_key >= start_ik AND sst.first_key < end_ik

            // Wait, SsTableMeta stores whatever we passed to `add_key`.
            // In flush_loop, we pass `key` which is Internal Key.
            // So metadata has Internal Keys.

            // Overlap check:
            // SST: [Min, Max]
            // Query: [Start, End)
            // Overlap if Max >= Start AND Min < End

            // Actually, we want the LARGEST key.
            // We should process SSTables that have the largest `last_key` first?
            // Not necessarily, `last_key` is just the bound.

            // We process overlapping candidates directly.
            let sst_min = &sst.meta.first_key;
            let sst_max = &sst.meta.last_key;

            if sst_max.as_slice() < start_ik.as_slice() || sst_min.as_slice() >= end_ik.as_slice() {
                continue;
            }

            // For each relevant SSTable, we want to find the largest key < end.
            // Optimization: If `sst.last_key` < end, then `sst.last_key` is a candidate!
            // But `sst.last_key` might be a tombstone or older version.
            // We still need to check validity.
            // BUT, we can iterate *that specific block* where `last_key` resides.

            // To be safe and correct without full reverse iterator:
            // We iterate relevant SSTables.
            // But we can optimize:
            // If we find a key `K` in MemTable, we only care about SSTables where `last_key > K`.

            // Read the block containing the largest key <= end_ik
            // We use `index` to find the offset.
            // `index` maps StartKey -> Offset.
            // We want the block that starts <= end_ik.

            // `sst.index` is BTreeMap. `range(..=end_ik).next_back()` gives the block starting before end_ik.
            // This block *might* contain keys < end_ik.
            // The *next* block starts > end_ik (or doesn't exist).

            // So we just need to read this ONE block and scan it.
            // Wait, what if the block contains only keys >= end_ik? (Possible if block start == end_ik)
            // But we used `..=`.

            // Let's get the block.
            // If the block contains keys < end, the largest one is our candidate.
            // If not, we might need the *previous* block.
            // But `range(..=)` gives the block where `start_key <= end_ik`.
            // So the keys in that block are `>= start_key`.
            // They *could* exceed `end_ik`.

            // So we read this block, and iterate it.
            // Since block is small (4KB), scanning it is cheap.
            // We find the largest key in this block < end_ik.

            // If this block yields nothing < end_ik, we check the *previous* block?
            // Yes.

            // Heuristic: Check the last 2 blocks that might overlap.
            // 1. Block A: `range(..end_ik).next_back()` -> Starts < end_ik.
            // This is the primary candidate.

            let candidate_idx = match sst
                .index_keys
                .binary_search_by(|key| key.as_slice().cmp(end_ik.as_slice()))
            {
                Ok(idx) | Err(idx) => idx.checked_sub(1),
            };

            let Some(candidate_idx) = candidate_idx else {
                continue;
            };

            let current_offset = sst.index_offsets[candidate_idx];
            let previous_offset = candidate_idx
                .checked_sub(1)
                .map(|idx| sst.index_offsets[idx]);

            for offset in [Some(current_offset), previous_offset]
                .into_iter()
                .flatten()
            {
                if let Ok(block_data) = sst.read_block(offset).await {
                    // Iterate block
                    let mut cursor = std::io::Cursor::new(block_data);
                    let mut count_buf = [0u8; 4];
                    if std::io::Read::read_exact(&mut cursor, &mut count_buf).is_ok() {
                        let count = u32::from_le_bytes(count_buf);
                        for _ in 0..count {
                            // Read KV
                            // (Simplified manual read to avoid dependency issues)
                            let mut len_buf = [0u8; 4];
                            if std::io::Read::read_exact(&mut cursor, &mut len_buf).is_err() {
                                break;
                            }
                            let k_len = u32::from_le_bytes(len_buf) as usize;
                            let mut k = vec![0u8; k_len];
                            if std::io::Read::read_exact(&mut cursor, &mut k).is_err() {
                                break;
                            }

                            if std::io::Read::read_exact(&mut cursor, &mut len_buf).is_err() {
                                break;
                            }
                            let v_len = u32::from_le_bytes(len_buf) as usize;
                            let mut v = vec![0u8; v_len];
                            if std::io::Read::read_exact(&mut cursor, &mut v).is_err() {
                                break;
                            }

                            // Check Range
                            let (uk, ts) = FusionStorage::decode_key(&k);
                            // Decode Internal Key to check against end
                            // `end` is user key bound.
                            // `k` is Internal.
                            // `uk` is User Key.

                            if uk < end && uk >= start {
                                update_max(&uk, &v, ts, &mut max_key);
                            }
                        }
                    }
                }
            }
        }

        if let Some((k, v, _)) = max_key {
            // Check if it's a delete (Tombstone)
            let (is_put, val) = FusionStorage::decode_value(&v);
            if is_put {
                Ok(Some((k, val.to_vec())))
            } else {
                Ok(None) // Last value is a delete
            }
        } else {
            Ok(None)
        }
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }

        // Write Conflict Detection (OCC):
        // For each key in write_buffer, check if a newer version (ts > read_ts)
        // was committed by another transaction since we started.
        let check_mem_conflict =
            |mem: &MemTable, search_key: &[u8], user_key: &[u8], read_ts: u64| -> Option<u64> {
                mem.map
                    .range(search_key.to_vec()..)
                    .next()
                    .and_then(|entry| {
                        let (k, ts) = FusionStorage::decode_key(entry.key());
                        if k == user_key && ts > read_ts {
                            Some(ts)
                        } else {
                            None
                        }
                    })
            };

        for (user_key, _) in &self.write_buffer {
            let search_key = FusionStorage::encode_key(user_key, u64::MAX);

            // Check active memtable
            let conflict_ts = {
                let active = self.storage.active_memtable.read().unwrap();
                check_mem_conflict(&active, &search_key, user_key, self.read_ts)
            };
            if let Some(ts) = conflict_ts {
                return Err(crate::common::FusionError::Storage(format!(
                    "Write conflict: key modified by another transaction (read_ts={}, conflict_ts={})",
                    self.read_ts, ts
                )));
            }

            // Check immutable memtables
            let conflict_ts = {
                let imm = self.storage.immutable_memtables.read().unwrap();
                let mut found = None;
                for mem in imm.iter().rev() {
                    if let Some(ts) = check_mem_conflict(mem, &search_key, user_key, self.read_ts) {
                        found = Some(ts);
                        break;
                    }
                }
                found
            };
            if let Some(ts) = conflict_ts {
                return Err(crate::common::FusionError::Storage(format!(
                    "Write conflict: key modified by another transaction (read_ts={}, conflict_ts={})",
                    self.read_ts, ts
                )));
            }
        }

        let commit_ts = self.storage.current_ts.fetch_add(1, Ordering::SeqCst) + 1;

        // Prepare encoded keys/values for both WAL and MemTable
        // We use Put for both Put and Delete (Delete is Put with Tombstone Flag)
        let mut wal_entries = Vec::with_capacity(self.write_buffer.len());
        let mut mem_entries = Vec::with_capacity(self.write_buffer.len());

        for (k, v) in self.write_buffer {
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

        // 2. MemTable Insert
        {
            let active = self.storage.active_memtable.read().unwrap();
            for (key, val) in mem_entries {
                active.insert(key, val);
            }
        }

        // Check rotation after insert
        let needs_rotate = {
            let active = self.storage.active_memtable.read().unwrap();
            active.size.load(Ordering::Relaxed) > self.storage.memtable_threshold as u64
        };

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
        let read_ts = self.current_ts.load(Ordering::SeqCst);
        Ok(Box::new(FusionTransaction {
            storage: self.clone(),
            write_buffer: transaction_write_buffer(),
            read_ts,
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

    #[test]
    fn fusion_transaction_write_buffer_preallocates_first_write() {
        assert!(transaction_write_buffer().capacity() >= 1);
    }

    #[test]
    fn vector_rebuild_data_prefix_for_table_preallocates_exact_prefix() {
        let prefix = vector_rebuild_data_prefix_for_table("embeddings");

        assert_eq!(prefix, "data:embeddings:");
        assert!(prefix.capacity() >= prefix.len());
    }

    #[test]
    fn vector_rebuild_hnsw_index_name_for_column_preallocates_exact_name() {
        let name = vector_rebuild_hnsw_index_name_for_column("docs", "embedding");

        assert_eq!(name, "hnsw_docs_embedding");
        assert!(name.capacity() >= name.len());
    }

    #[test]
    fn merge_heap_reserves_candidate_iterators() {
        let capacity = 1 + COMPACTION_FANIN;
        assert!(merge_heap(capacity).capacity() >= capacity);
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
        storage.update_columnar_store(
            vec!["doc1".to_string(), "doc2".to_string(), "doc3".to_string()],
            vec![
                vec![0.0, 0.0, 0.0],
                vec![1.0, 0.0, 0.0],
                vec![9.0, 0.0, 0.0],
            ],
        );

        let results = storage.hybrid_search("apple", &[0.0, 0.0, 0.0], 2);

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
            .is_empty());

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

        storage.rebuild_vector_index().await;

        let results = storage
            .vector_index
            .search("hnsw_vec_rebuild_embedding", &[1.0, 0.0], 1)
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
    fn sstable_file_candidate_buffer_preallocates_first_file() {
        let files = sstable_file_candidate_buffer();
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
}
