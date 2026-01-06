use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use crossbeam_skiplist::SkipMap;
use tokio::sync::Notify;
use tokio::io::AsyncWriteExt;
use crate::common::Result;
use super::{Storage, Transaction};
use super::wal::{WalManager, WalEntry};
use async_trait::async_trait;

const MEMTABLE_THRESHOLD: usize = 32 * 1024 * 1024; // 32MB

// --- Core Data Structures ---

#[derive(Clone)]
struct MemTable {
    map: Arc<SkipMap<Vec<u8>, Vec<u8>>>,
    size: Arc<AtomicU64>, // Approx size in bytes
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
        let added_size = key.len() as u64 + value.len() as u64;
        self.map.insert(key, value);
        self.size.fetch_add(added_size, Ordering::Relaxed);
    }
    
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.map.get(key).map(|e| e.value().clone())
    }
}

use super::sstable::{SsTable, SsTableBuilder};

// --- LSM Engine ---

#[derive(Clone)]
pub struct LsmStorage {
    // Current mutable memtable
    active_memtable: Arc<RwLock<MemTable>>,
    // Immutable memtables (frozen, waiting to flush)
    immutable_memtables: Arc<RwLock<Vec<MemTable>>>,
    // On-disk SSTables
    sstables: Arc<RwLock<Vec<Arc<SsTable>>>>,
    
    wal: Arc<WalManager>,
    next_memtable_id: Arc<AtomicU64>,
    flush_notify: Arc<Notify>,
}

impl LsmStorage {
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
                 
                 // We need async runtime to open SSTables (since SsTable::open is async)
                 // But new() is synchronous. We can block on it since it's startup.
                 let rt = tokio::runtime::Handle::current();
                 for (id, path) in files {
                     if let Ok(sst) = rt.block_on(SsTable::open(path, id)) {
                         sstables_vec.push(Arc::new(sst));
                     }
                 }
             }
        }

        // Determine next memtable ID
        let next_id = sstables_vec.last().map(|s| s.id + 1).unwrap_or(2); // 1 is active

        let storage = Self {
            active_memtable: Arc::new(RwLock::new(active)),
            immutable_memtables: Arc::new(RwLock::new(Vec::new())),
            sstables: Arc::new(RwLock::new(sstables_vec)),
            wal: Arc::new(wal),
            next_memtable_id: Arc::new(AtomicU64::new(next_id)),
            flush_notify: Arc::new(Notify::new()),
        };

        // Start background flush thread
        let storage_clone = storage.clone();
        tokio::spawn(async move {
            storage_clone.flush_loop().await;
        });

        Ok(storage)
    }

    async fn flush_loop(&self) {
        // Ensure sstables directory exists
        let _ = tokio::fs::create_dir_all("sstables").await;

        loop {
            self.flush_notify.notified().await;
            
            // 1. Pick a memtable to flush
            let memtable_to_flush = {
                let mut imm = self.immutable_memtables.write().unwrap();
                imm.pop()
            };

            if let Some(mem) = memtable_to_flush {
                
                // We reuse SsTableBuilder from sstable module
                let sst_path = PathBuf::from(format!("sstables/{}.sst", mem.id));
                let mut builder = SsTableBuilder::new(sst_path.clone());
                
                // Write MemTable content to Builder
                // We need to iterate MemTable here because Builder doesn't know about LSM MemTable structure
                let mut block_count = 0;
                let mut block_buffer = Vec::new();
                let mut first_key = None;
                
                for entry in mem.map.iter() {
                    let key = entry.key();
                    let val = entry.value();
                    
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
                
                // Open and Register SSTable
                match SsTable::open(sst_path, mem.id).await {
                    Ok(sst) => {
                        let mut sstables = self.sstables.write().unwrap();
                        sstables.push(Arc::new(sst));
                        // println!("LSM: Flushed MemTable {} to disk", mem.id);
                    },
                    Err(e) => eprintln!("Failed to open flushed sstable: {:?}", e),
                }
            }
        }
    }

    fn rotate_memtable(&self) {
        let mut active = self.active_memtable.write().unwrap();
        let mut immutable = self.immutable_memtables.write().unwrap();
        
        let new_id = self.next_memtable_id.fetch_add(1, Ordering::Relaxed);
        let new_mem = MemTable::new(new_id);
        
        let old_mem = std::mem::replace(&mut *active, new_mem);
        immutable.push(old_mem);
        
        self.flush_notify.notify_one();
    }
}

pub struct LsmTransaction {
    storage: LsmStorage,
    write_buffer: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

#[async_trait]
impl Transaction for LsmTransaction {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 1. Check txn write buffer
        for (k, v) in self.write_buffer.iter().rev() {
            if k == key {
                return Ok(v.clone());
            }
        }

        // 2. Check Active MemTable
        {
            let active = self.storage.active_memtable.read().unwrap();
            if let Some(val) = active.get(key) {
                // Check tombstone? For now assume val is raw data.
                // We need to encode tombstones.
                // Simplified: empty vec = deleted?
                if val.is_empty() { return Ok(None); }
                return Ok(Some(val));
            }
        }

        // 3. Check Immutable MemTables
        {
            let imm = self.storage.immutable_memtables.read().unwrap();
            for mem in imm.iter().rev() {
                if let Some(val) = mem.get(key) {
                    if val.is_empty() { return Ok(None); }
                    return Ok(Some(val));
                }
            }
        }

        // 4. Check SSTables
        let sstables: Vec<Arc<SsTable>> = {
            let guard = self.storage.sstables.read().unwrap();
            guard.clone()
        };

        for sst in sstables.iter().rev() {
            if let Some(val) = sst.get(key).await? {
                 if val.is_empty() { return Ok(None); }
                 return Ok(Some(val));
            }
        }
        
        Ok(None)
    }

    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_buffer.push((key.to_vec(), Some(value.to_vec())));
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write_buffer.push((key.to_vec(), None)); // None means delete
        Ok(())
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Simple scan on active memtable for benchmark
        // Merging multiple sources is complex (LSM Merge Iterator).
        // For this step, we just scan active + immutable.
        
        let mut result_map = BTreeMap::new();
        
        // Helper to scan a memtable
        let scan_mem = |mem: &MemTable, map: &mut BTreeMap<Vec<u8>, Vec<u8>>| {
             // Range scan in SkipList
             // We don't have a direct range method that takes reference in `MemTable` wrapper,
             // so we access internal map.
             for entry in mem.map.range(prefix.to_vec()..) {
                 let k = entry.key();
                 if !k.starts_with(prefix) { break; }
                 
                 // If not already in map (newer versions come first in logic, but here we scan old->new?)
                 // Wait, we should apply strictly newer -> older order.
                 // If we scan Immutable 1, then Immutable 2, then Active.
                 // Active overrides Immutable.
                 
                 // So we should scan Active FIRST, then Immutable.
                 // And if key exists, don't overwrite.
                 if !map.contains_key(k) {
                     map.insert(k.clone(), entry.value().clone());
                 }
             }
        };

        // 1. Active
        {
            let active = self.storage.active_memtable.read().unwrap();
            scan_mem(&active, &mut result_map);
        }
        
        // 2. Immutable (Newest to Oldest)
        {
            let imm = self.storage.immutable_memtables.read().unwrap();
            for mem in imm.iter().rev() {
                scan_mem(mem, &mut result_map);
            }
        }
        
        // Optimization: if we have too many immutable memtables, scan becomes slow.
        // We need compaction or limiting memtables.
        
        // 3. Txn Buffer
        for (k, v) in &self.write_buffer {
            if k.starts_with(prefix) {
                match v {
                    Some(val) => { result_map.insert(k.clone(), val.clone()); },
                    None => { result_map.remove(k); },
                }
            }
        }
        
        // Filter deletions (empty vals)
        let res = result_map.into_iter()
            .filter(|(_, v)| !v.is_empty())
            .collect();
            
        Ok(res)
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }

        // 1. WAL Write
        // Convert to WalEntry
        let entries: Vec<WalEntry> = self.write_buffer.iter().map(|(k, v)| {
            match v {
                Some(val) => WalEntry::Put(k.clone(), val.clone()),
                None => WalEntry::Delete(k.clone()),
            }
        }).collect();
        
        self.storage.wal.append_batch_async(entries).await?;

        // 2. Apply to MemTable
        // We need a read lock first to check size, then maybe write lock?
        // Actually we need write lock to insert.
        // If size > threshold, rotate.
        
        let needs_rotate = {
            let active = self.storage.active_memtable.read().unwrap();
            active.size.load(Ordering::Relaxed) > MEMTABLE_THRESHOLD as u64
        };
        
        if needs_rotate {
            self.storage.rotate_memtable();
        }

        {
            let active = self.storage.active_memtable.read().unwrap();
            for (k, v) in self.write_buffer {
                match v {
                    Some(val) => active.insert(k, val),
                    None => active.insert(k, Vec::new()), // Tombstone = empty vec for now
                }
            }
        }

        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Storage for LsmStorage {
    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        Ok(Box::new(LsmTransaction {
            storage: self.clone(),
            write_buffer: Vec::new(),
        }))
    }

    async fn create_snapshot(&self) -> Result<()> {
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
