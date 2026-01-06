use super::columnar::ColumnarVectorStore;
use super::wal::{WalEntry, WalManager};
use super::{Storage, Transaction};
use crate::common::Result;
use async_trait::async_trait;
use crossbeam_skiplist::SkipMap;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::Notify;

// Fusion Storage Engine
// Combines:
// 1. MVCC (Lock-free reads, Snapshot Isolation)
// 2. LSM-Tree Structure (MemTable -> Flush -> SST)
// 3. Columnar Vector Store (Integrated for Vector Search)

const MEMTABLE_THRESHOLD: usize = 32 * 1024 * 1024; // 32MB
const TS_SIZE: usize = 8;

// --- Data Structures ---

#[derive(Clone)]
struct MemTable {
    // Key -> (Value, Timestamp)
    // We encode Key+TS in the SkipMap key for MVCC
    map: Arc<SkipMap<Vec<u8>, Vec<u8>>>,
    size: Arc<AtomicU64>,
    id: u64,
}

impl MemTable {
    fn new(id: u64) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            size: Arc::new(AtomicU64::new(0)),
            id,
        }
    }

    fn insert(&self, key: Vec<u8>, value: Vec<u8>) {
        let len = key.len() + value.len();
        self.map.insert(key, value);
        self.size.fetch_add(len as u64, Ordering::Relaxed);
    }
}

use crate::storage::inverted_index::InvertedIndex;
use crate::storage::sstable::{SsTable, SsTableBuilder};
use crate::storage::vector_index::VectorIndex;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BinaryHeap, HashMap};
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

    // HNSW Index (Replaces brute-force ColumnarStore)
    vector_index: Arc<VectorIndex>,
}

impl FusionStorage {
    pub fn new(path: &str) -> Result<Self> {
        let wal = WalManager::new(path)?;
        let active = MemTable::new(1);

        // Load existing SSTables
        let mut sstables_vec = Vec::new();
        let sst_dir = Path::new("sstables");
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

                let rt = tokio::runtime::Handle::current();
                for (id, path) in files {
                    if let Ok(sst) = rt.block_on(SsTable::open(path, id)) {
                        sstables_vec.push(Arc::new(sst));
                    }
                }
            }
        }

        let next_id = sstables_vec.last().map(|s| s.id + 1).unwrap_or(2);

        // Replay WAL
        // We need to replay committed transactions into the active memtable.
        // Since we are single-threaded here (constructor), we can just insert directly.
        // Note: SSTables contain flushed data. WAL contains *all* writes including unflushed.
        // We should clear WAL after flush, but currently we just append.
        // To properly recover:
        // 1. If we have SSTables, we assume they are durable.
        // 2. We need to know where to start replaying WAL.
        //    Ideally WAL should be truncated on flush.
        //    For now, let's replay EVERYTHING and assume MemTable handles duplicates/overwrites?
        //    Wait, if we replay old data that is already in SST, it will go into MemTable.
        //    Then MemTable will flush it again to a NEW SST.
        //    This causes duplication.
        //    We need a mechanism to truncate WAL.
        //    Task 17 implemented flush, but didn't truncate WAL.
        //    Let's assume for now we replay everything, but we need to implement WAL truncation in flush loop.

        let replay_entries = wal.replay().unwrap_or_default();
        // Since we don't have TS in WAL (we strip it), we need to assign a new TS?
        // Or did we store TS in WAL?
        // WalEntry is Put(Key, Val). Fusion encodes TS in Key.
        // If we store encoded key in WAL, we are good.
        // Let's check commit():
        // entries = write_buffer.iter().map(|(k, v)| ... Put(k, v)) -> These are USER keys!
        // Then loop insert(encode_key(k, commit_ts), encode_value(v)).
        // So WAL stores USER keys and raw values (no TS, no Flag).
        // This is a problem. Replay needs to restore exact MVCC state.
        // If we assign NEW TS, we change history.
        // We MUST store encoded keys and values in WAL for FusionStorage!

        // FIX: Update commit() to store internal keys/values in WAL?
        // But WalEntry is generic.
        // Option A: Change WalEntry to just be bytes (key, val).
        // Option B: In commit(), we pass encoded keys/values to WAL.

        // Let's check commit() again.
        // WAL write happens BEFORE MemTable insert.
        // WAL entries are created from `write_buffer` which has user keys.
        // We need to fix this. WAL must log the *exact* mutation that is about to happen.
        // But commit_ts is generated inside commit().

        // Plan:
        // 1. Generate commit_ts.
        // 2. Prepare encoded keys/values.
        // 3. Write encoded keys/values to WAL.
        // 4. Insert encoded keys/values to MemTable.

        // But first, let's finish `new()` assuming we fix commit() later.

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
                InvertedIndex::load("inverted_index.bin").unwrap_or_else(|_| InvertedIndex::new()),
            )),
            vector_index: {
                let vi = Arc::new(VectorIndex::new());
                vi.create_index("default");
                vi
            },
        };

        // Apply Replay
        if !replay_entries.is_empty() {
            println!("Replaying {} WAL entries...", replay_entries.len());
            let active = storage.active_memtable.write().unwrap();
            // We need to track max TS to restore current_ts
            let mut max_ts = 0;

            for entry in replay_entries {
                match entry {
                    WalEntry::Put(k, v) => {
                        // Assume k is encoded key (UserKey + TS)
                        // Check if it's encoded.
                        if k.len() > TS_SIZE {
                            let (_, ts) = Self::decode_key(&k);
                            if ts > max_ts {
                                max_ts = ts;
                            }
                        }
                        active.insert(k, v);
                    }
                    WalEntry::Delete(k) => {
                        // Delete in WAL for Fusion should be a Put with Tombstone value?
                        // Or we use WalEntry::Delete?
                        // If we change commit() to use Put for everything (including tombstones),
                        // then WalEntry::Delete might be unused for Fusion.
                        // Let's see commit() fix.
                        active.insert(k, Vec::new()); // Treat as empty val (tombstone)?
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
        let kv_pairs = match txn.scan_prefix(prefix.as_bytes()).await {
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
                        // Find HNSW columns
                        let mut hnsw_cols = Vec::new();
                        for (idx, col) in schema.columns.iter().enumerate() {
                            if col.is_indexed && col.index_type == crate::catalog::IndexType::HNSW {
                                hnsw_cols.push((idx, col.name.clone()));
                                // Ensure index exists
                                let idx_name = format!("hnsw_{}_{}", table_name, col.name);
                                self.vector_index.create_index(&idx_name);
                            }
                        }

                        if hnsw_cols.is_empty() {
                            continue;
                        }

                        // Scan data for this table
                        let data_prefix = format!("data:{}:", table_name);
                        if let Ok(data_pairs) = txn.scan_prefix(data_prefix.as_bytes()).await {
                            for (dk, dv) in data_pairs {
                                if let Ok(row) =
                                    bincode::deserialize::<Vec<crate::common::Value>>(&dv)
                                {
                                    let parts: Vec<&str> =
                                        std::str::from_utf8(&dk).unwrap().split(':').collect();
                                    let row_id = parts.last().unwrap().to_string();

                                    for (col_idx, col_name) in &hnsw_cols {
                                        if let Some(crate::common::Value::Vector(vec)) =
                                            row.get(*col_idx)
                                        {
                                            let idx_name =
                                                format!("hnsw_{}_{}", table_name, col_name);
                                            let _ = self.vector_index.insert(
                                                &idx_name,
                                                row_id.clone(),
                                                vec.clone(),
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        println!("Vector Index Rebuild Complete.");
    }

    async fn flush_loop(&self) {
        let _ = tokio::fs::create_dir_all("sstables").await;

        loop {
            self.flush_notify.notified().await;

            let memtable_to_flush = {
                let mut imm = self.immutable_memtables.write().unwrap();
                imm.pop()
            };

            if let Some(mem) = memtable_to_flush {
                let sst_path = PathBuf::from(format!("sstables/{}.sst", mem.id));
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
                        builder.flush_block(first_key.take().unwrap(), block_count, &block_buffer);
                        block_buffer.clear();
                        block_count = 0;
                    }
                }

                if !block_buffer.is_empty() {
                    builder.flush_block(first_key.take().unwrap(), block_count, &block_buffer);
                }

                if let Err(e) = builder.finish().await {
                    eprintln!("Failed to flush sstable {}: {:?}", mem.id, e);
                    continue;
                }

                match SsTable::open(sst_path, mem.id).await {
                    Ok(sst) => {
                        let mut sstables = self.sstables.write().unwrap();
                        sstables.push(Arc::new(sst));
                    }
                    Err(e) => eprintln!("Failed to open flushed sstable: {:?}", e),
                }

                // Save Indices
                if let Ok(guard) = self.inverted_index.read() {
                    if let Err(e) = guard.save("inverted_index.bin") {
                        eprintln!("Failed to save inverted index: {:?}", e);
                    }
                }
            }
        }
    }

    async fn compaction_loop(&self) {
        loop {
            // Check every 1 second
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            let candidates = {
                let sstables = self.sstables.read().unwrap();
                if sstables.len() >= 4 {
                    // Pick oldest 4
                    sstables.iter().take(4).cloned().collect::<Vec<_>>()
                } else {
                    Vec::new()
                }
            };

            if candidates.is_empty() {
                continue;
            }

            // println!("Compacting {} SSTables...", candidates.len());

            // Open iterators
            let mut iterators = Vec::new();
            for sst in &candidates {
                match sst.new_iterator().await {
                    Ok(it) => iterators.push(it),
                    Err(e) => eprintln!("Failed to open SST iterator: {:?}", e),
                }
            }

            if iterators.is_empty() {
                continue;
            }

            // Output builder
            let new_id = self.next_memtable_id.fetch_add(1, Ordering::Relaxed);
            let out_path = PathBuf::from(format!("sstables/{}.sst", new_id));
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

            while let Some(item) = heap.pop() {
                let k = item.key;
                let v = item.val;
                let idx = item.iter_idx;

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
                    builder.flush_block(first_key.take().unwrap(), block_count, &block_buffer);
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

            if !block_buffer.is_empty() {
                builder.flush_block(first_key.take().unwrap(), block_count, &block_buffer);
            }

            if let Err(e) = builder.finish().await {
                eprintln!("Compaction failed to finish: {:?}", e);
                continue;
            }

            // Open new SST
            match SsTable::open(out_path, new_id).await {
                Ok(new_sst) => {
                    {
                        let mut sstables = self.sstables.write().unwrap();
                        // Remove old candidates (by ID)
                        let old_ids: Vec<u64> = candidates.iter().map(|s| s.id).collect();
                        sstables.retain(|s| !old_ids.contains(&s.id));

                        // Insert new SST (sorted by ID)
                        sstables.push(Arc::new(new_sst));
                        sstables.sort_by_key(|s| s.id);
                    } // Drop lock

                    // Delete old files
                    for sst in candidates {
                        let _ = tokio::fs::remove_file(&sst.path).await;
                    }
                    // println!("Compacted {} SSTables into {}", candidates.len(), new_id);
                }
                Err(e) => eprintln!("Failed to open compacted SST: {:?}", e),
            }
        }
    }

    fn rotate_memtable(&self) {
        let mut active = self.active_memtable.write().unwrap();
        let mut imm = self.immutable_memtables.write().unwrap();

        let new_id = self.next_memtable_id.fetch_add(1, Ordering::Relaxed);
        let new_mem = MemTable::new(new_id);

        let old = std::mem::replace(&mut *active, new_mem);
        imm.push(old);
        self.flush_notify.notify_one();
    }
}

pub struct FusionTransaction {
    storage: FusionStorage,
    write_buffer: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    read_ts: u64,
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

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Merge-Sort Scan over (Active + Immutable + WriteBuffer)
        // Simplified: Collect all, deduplicate

        let mut result_map = BTreeMap::new();
        let iter_key = FusionStorage::encode_key(prefix, u64::MAX); // Start of prefix range

        // Helper
        let scan_mem = |mem: &MemTable, map: &mut BTreeMap<Vec<u8>, Vec<u8>>| {
            let mut last_key: Option<Vec<u8>> = None;
            for entry in mem.map.range(iter_key.clone()..) {
                let (k, ts) = FusionStorage::decode_key(entry.key());
                if !k.starts_with(prefix) {
                    break;
                }

                // Skip if we already found a newer version in this memtable
                if let Some(last) = &last_key {
                    if last == k {
                        continue;
                    }
                }

                if ts <= self.read_ts {
                    if !map.contains_key(k) {
                        let (is_put, val) = FusionStorage::decode_value(entry.value());
                        if is_put {
                            map.insert(k.to_vec(), val.to_vec());
                        } else {
                            // Tombstone: Insert empty to shadow older versions, but filter later
                            map.insert(k.to_vec(), Vec::new());
                        }
                    }
                    last_key = Some(k.to_vec());
                }
            }
        };

        // Active
        {
            let active = self.storage.active_memtable.read().unwrap();
            scan_mem(&active, &mut result_map);
        }

        // Immutable
        {
            let imm = self.storage.immutable_memtables.read().unwrap();
            for mem in imm.iter().rev() {
                scan_mem(mem, &mut result_map);
            }
        }

        // Write Buffer
        for (k, v) in &self.write_buffer {
            if k.starts_with(prefix) {
                match v {
                    Some(val) => {
                        result_map.insert(k.clone(), val.clone());
                    }
                    None => {
                        result_map.remove(k);
                    } // If deleted in txn
                }
            }
        }

        // Filter tombstones
        let res = result_map
            .into_iter()
            .filter(|(_, v)| !v.is_empty())
            .collect();

        Ok(res)
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        if self.write_buffer.is_empty() {
            return Ok(());
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
        let needs_rotate = {
            let active = self.storage.active_memtable.read().unwrap();
            active.size.load(Ordering::Relaxed) > MEMTABLE_THRESHOLD as u64
        };

        if needs_rotate {
            self.storage.rotate_memtable();
        }

        {
            let active = self.storage.active_memtable.read().unwrap();
            for (key, val) in mem_entries {
                active.insert(key, val);
            }
        }

        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        Ok(())
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
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
