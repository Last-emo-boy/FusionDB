use super::columnar::ColumnarVectorStore;
use super::wal::{WalEntry, WalManager};
use super::{Storage, Transaction};
use crate::common::Result;
use crate::config::StorageConfig;
use async_trait::async_trait;
use crossbeam_skiplist::SkipMap;
use moka::sync::Cache;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::Notify;

// Fusion Storage Engine
// Combines:
// 1. MVCC (Lock-free reads, Snapshot Isolation)
// 2. LSM-Tree Structure (MemTable -> Flush -> SST)
// 3. Columnar Vector Store (Integrated for Vector Search)

const TS_SIZE: usize = 8;
const COMPACTION_FANIN: usize = 4;

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
        let active = MemTable::new(1);
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
        let mut sstables_vec = Vec::new();
        let sst_dir = sstable_dir.as_path();
        if sst_dir.exists() {
            if let Ok(mut entries) = std::fs::read_dir(sst_dir) {
                let mut files = Vec::new();
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

                for (id, path) in files {
                    if let Ok(sst) = SsTable::open(path, id, block_cache.clone()).await {
                        sstables_vec.push(Arc::new(sst));
                    }
                }
            }
        }

        let next_id = sstables_vec.last().map(|s| s.id + 1).unwrap_or(2);

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
            immutable_memtables: Arc::new(RwLock::new(Vec::new())),
            sstables: Arc::new(RwLock::new(sstables_vec)),
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
            paths,
        };

        // Apply Replay
        if !replay_entries.is_empty() {
            println!("Replaying {} WAL entries...", replay_entries.len());
            // Use active_memtable logic but with manual rotation
            let mut max_ts = 0;

            for entry in replay_entries {
                match entry {
                    WalEntry::Put(k, v) => {
                        // Strict validation: Skip keys smaller than TS_SIZE
                        if k.len() < TS_SIZE {
                            continue;
                        }

                        if k.len() > TS_SIZE {
                            let (_, ts) = Self::decode_key(&k);
                            if ts > max_ts {
                                max_ts = ts;
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
            storage.current_ts.store(max_ts, Ordering::SeqCst);
            println!("WAL Replay complete. Restored TS: {}", max_ts);
        }

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
        // Legacy: Columnar Store
        let store = ColumnarVectorStore::new(ids.clone(), vectors.clone(), 3);
        let mut guard = self.columnar_store.write().unwrap();
        *guard = Some(store);

        // New: HNSW Index
        for (id, vec) in ids.iter().zip(vectors.iter()) {
            let _ = self.vector_index.insert("default", id.clone(), vec.clone());
        }
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
        let results = guard.search_bm25(query, 1.2, 0.75);
        results.into_iter().take(limit).collect()
    }

    // Hybrid Search: RRF (Reciprocal Rank Fusion)
    pub fn hybrid_search(
        &self,
        text_query: &str,
        vector_query: &[f32],
        limit: usize,
    ) -> Vec<(String, f32)> {
        // 1. Get results from both sources
        let text_results = self.bm25_search(text_query, limit * 2); // Get more candidates
        let vector_results = self.vector_search(vector_query, limit * 2);

        // 2. RRF Fusion
        // Score = 1 / (k + rank)
        let k = 60.0;
        let mut rrf_scores: HashMap<String, f32> = HashMap::new();

        for (rank, (id, _score)) in text_results.iter().enumerate() {
            let s = 1.0 / (k + rank as f32 + 1.0);
            *rrf_scores.entry(id.clone()).or_insert(0.0) += s;
        }

        for (rank, (id, _score)) in vector_results.iter().enumerate() {
            let s = 1.0 / (k + rank as f32 + 1.0);
            *rrf_scores.entry(id.clone()).or_insert(0.0) += s;
        }

        let mut final_results: Vec<_> = rrf_scores.into_iter().collect();
        final_results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        final_results.into_iter().take(limit).collect()
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
        self.paths.sstable_dir.join(format!("{}.sst", id))
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
                        let mut hnsw_cols = Vec::new();
                        for (idx, col) in schema.columns.iter().enumerate() {
                            if col.is_indexed && col.index_type == crate::catalog::IndexType::HNSW {
                                let idx_name = format!("hnsw_{}_{}", table_name, col.name);
                                self.vector_index.create_index(&idx_name);
                                hnsw_cols.push((idx, idx_name));
                            }
                        }

                        if hnsw_cols.is_empty() {
                            continue;
                        }

                        let data_prefix = format!("data:{}:", table_name);
                        if let Ok(data_pairs) = txn.scan_prefix(data_prefix.as_bytes(), None).await
                        {
                            let mut batches: HashMap<String, Vec<(String, Vec<f32>)>> =
                                HashMap::new();

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

    async fn flush_loop(&self) {
        let _ = tokio::fs::create_dir_all(&self.paths.sstable_dir).await;

        loop {
            self.flush_notify.notified().await;

            let memtable_to_flush = {
                let mut imm = self.immutable_memtables.write().unwrap();
                imm.pop()
            };

            if let Some(mem) = memtable_to_flush {
                let sst_path = self.sstable_path_for(mem.id);
                let mut builder = SsTableBuilder::new(sst_path.clone());

                // Write memtable to builder
                // We reuse the logic from lsm.rs but applied to Fusion's MemTable
                // Fusion's MemTable stores Key+TS -> Value
                // SSTable doesn't care about encoding, just bytes.

                let mut block_count = 0;
                let mut block_buffer = Vec::new();
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

                    if block_buffer.len() >= 4096 {
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
                            let mut sstables = self.sstables.write().unwrap();
                            sstables.push(Arc::new(sst));
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
        let Some(candidates) = self.compaction_candidates() else {
            return Ok(false);
        };

        // Open iterators
        let mut iterators = Vec::with_capacity(COMPACTION_FANIN);
        for sst in &candidates {
            match sst.new_iterator(None).await {
                Ok(it) => iterators.push(it),
                Err(e) => eprintln!("Failed to open SST iterator: {:?}", e),
            }
        }

        if iterators.is_empty() {
            return Ok(false);
        }

        // Output builder
        let new_id = self.next_memtable_id.fetch_add(1, Ordering::Relaxed);
        let out_path = self.sstable_path_for(new_id);
        let mut builder = SsTableBuilder::new(out_path.clone());

        // Merge Logic
        let mut heap = BinaryHeap::new();

        // Init heap
        for (idx, it) in iterators.iter_mut().enumerate() {
            if let Ok(Some((k, v))) = it.next().await {
                heap.push(MergeItem {
                    key: k,
                    val: v,
                    iter_idx: idx,
                });
            }
        }

        let mut block_buffer = Vec::new();
        let mut block_count = 0;
        let mut first_key = None;
        let mut last_base_key: Option<Vec<u8>> = None;
        let mut dedup_count: u64 = 0;

        while let Some(item) = heap.pop() {
            let k = item.key;
            let v = item.val;
            let idx = item.iter_idx;

            if k.len() < TS_SIZE {
                if let Ok(Some((next_k, next_v))) = iterators[idx].next().await {
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
                if let Ok(Some((next_k, next_v))) = iterators[idx].next().await {
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

            if block_buffer.len() >= 4096 {
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
            if let Ok(Some((next_k, next_v))) = iterators[idx].next().await {
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

                // Delete old files
                for sst in candidates {
                    let _ = tokio::fs::remove_file(&sst.path).await;
                }

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
        let mut block_buffer = Vec::new();
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

            if block_buffer.len() >= 4096 {
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
        let mut mem_tables = Vec::new();
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

        // 2. Initialize Heap
        let mut heap = BinaryHeap::new();

        if let Some((k, v)) = wb_iter.next() {
            heap.push(MergeItem {
                key: k,
                val: v,
                iter_idx: 0,
            });
        }

        // Helper Type for Iterators
        type BoxedIter<'a> = Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send + 'a>;

        // 1. Collect FBTree Arcs first to ensure stability
        // Defined BEFORE mem_iters so it outlives mem_iters (dropped last)
        let mut fbtree_holders: Vec<Option<Arc<FBTree>>> = Vec::with_capacity(mem_tables.len());

        for mem in &mem_tables {
            let guard = mem.fbtree.read().unwrap();
            fbtree_holders.push(guard.clone());
        }

        let mut mem_iters: Vec<BoxedIter> = Vec::new();

        // 2. Create Iterators
        for (i, mem) in mem_tables.iter().enumerate() {
            if let Some(tree) = &fbtree_holders[i] {
                // FBTree Iterator
                // We cast the iterator to BoxedIter.
                // Note: tree.scan returns FBTreeIterator which holds reference to tree.
                // tree is inside fbtree_holders.
                let iter = Box::new(tree.scan(&start_ik)) as BoxedIter;

                // We need to peek/advance
                // But we can't easily peek BoxedIter without consuming.
                // We wrap it in Peekable? No, Peekable is not Iterator of Item=(...).
                // We just consume one.
                // But we need to put it into mem_iters.
                // We can't put `iter` into `mem_iters` AND use it.
                // We have to consume it, store the item in Heap, and store the REST of iterator.
                // `BoxedIter` is a Box. `next()` takes `&mut self`.
                // So we can use it.

                // Rust ownership: `iter` is owned by local var.
                // We verify first item.
                let mut iter = iter; // mutable
                if let Some((k, v)) = iter.next() {
                    let (user_k, _) = FusionStorage::decode_key(&k);
                    if user_k < end {
                        heap.push(MergeItem {
                            key: k,
                            val: v,
                            iter_idx: 1 + i,
                        });
                        mem_iters.push(iter);
                    } else {
                        mem_iters.push(iter);
                    }
                } else {
                    mem_iters.push(iter);
                }
            } else {
                // SkipMap Iterator
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
        }

        let sstables = self.storage.sstables.read().unwrap().clone();
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
                    while let Ok(Some((nk, nv))) = it.next().await {
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

        // Helper to check a memtable
        let check_mem = |mem: &MemTable| -> Option<Vec<u8>> {
            // Range scan starting from (Key, MAX-read_ts)
            // The first entry >= search_key
            let entry = mem.map.range(search_key.clone()..).next();
            if let Some(ent) = entry {
                let (k, _ts) = FusionStorage::decode_key(ent.key());
                if k == key {
                    // Found valid version
                    let (is_put, val) = FusionStorage::decode_value(ent.value());
                    if is_put {
                        return Some(val.to_vec());
                    } else {
                        return Some(Vec::new());
                    } // Tombstone found, stop searching
                }
            }
            None
        };

        // Check Active
        {
            let active = self.storage.active_memtable.read().unwrap();
            if let Some(val) = check_mem(&active) {
                if val.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(val));
            }
        }

        // Check Immutable
        {
            let imm = self.storage.immutable_memtables.read().unwrap();
            for mem in imm.iter().rev() {
                if let Some(val) = check_mem(mem) {
                    if val.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(val));
                }
            }
        }

        // Check SSTables
        let sstables: Vec<Arc<SsTable>> = {
            let guard = self.storage.sstables.read().unwrap();
            guard.clone()
        };

        for sst in sstables.iter().rev() {
            if let Ok(Some((k_bytes, v_bytes))) = sst.find_ge(&search_key).await {
                let (k, _ts) = FusionStorage::decode_key(&k_bytes);
                if k == key {
                    let (is_put, val) = FusionStorage::decode_value(&v_bytes);
                    if is_put {
                        return Ok(Some(val.to_vec()));
                    } else {
                        return Ok(None);
                    } // Tombstone found
                }
            }
        }

        Ok(None)
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

        // Filter SSTables that overlap with range
        let mut relevant_ssts = Vec::new();
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

            // We just collect all candidates.
            let sst_min = &sst.meta.first_key;
            let sst_max = &sst.meta.last_key;

            if sst_max.as_slice() >= start_ik.as_slice() && sst_min.as_slice() < end_ik.as_slice() {
                relevant_ssts.push(sst.clone());
            }
        }

        // For each relevant SSTable, we want to find the largest key < end.
        // Optimization: If `sst.last_key` < end, then `sst.last_key` is a candidate!
        // But `sst.last_key` might be a tombstone or older version.
        // We still need to check validity.
        // BUT, we can iterate *that specific block* where `last_key` resides.

        // To be safe and correct without full reverse iterator:
        // We iterate RELEVANT SSTables.
        // But we can optimize:
        // If we find a key `K` in MemTable, we only care about SSTables where `last_key > K`.

        for sst in relevant_ssts {
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
            write_buffer: Vec::new(),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_storage_dir(test_name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("fusiondb_{}_{}", test_name, uuid::Uuid::new_v4()))
    }

    fn cleanup_storage_dir(path: &Path) {
        let _ = std::fs::remove_dir_all(path);
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
}
