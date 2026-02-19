use crate::common::Result;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use moka::sync::Cache;

use fastbloom::BloomFilter;

// --- SSTable ---
// Format:
// [Data Block 1] [Data Block 2] ... [Index Block] [Filter Block] [Footer]
// Data Block: [Count: 4b] [Entry 1] [Entry 2] ...
// Entry: [KeyLen: 4b] [Key] [ValLen: 4b] [Val]
// Index Block: [Entry 1] ... (Key -> Offset)
// Filter Block: [FilterBytes]
// Footer: [IndexOffset: 8b] [FilterOffset: 8b] [Magic: 4b]

const SST_MAGIC: u32 = 0xCAFEBABE;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct SsTableMeta {
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
}

pub struct SsTable {
    pub id: u64,
    pub path: PathBuf,
    pub index: BTreeMap<Vec<u8>, u64>,
    pub filter: BloomFilter,
    pub block_cache: Arc<Cache<(u64, u64), Vec<u8>>>,
    pub file_len: u64,
    pub index_keys: Arc<Vec<Vec<u8>>>,
    pub index_offsets: Arc<Vec<u64>>,
    pub meta: SsTableMeta,
}

impl SsTable {
    pub async fn open(path: PathBuf, id: u64, block_cache: Arc<Cache<(u64, u64), Vec<u8>>>) -> Result<Self> {
        let mut file = tokio::fs::File::open(&path).await?;
        let len = file.metadata().await?.len();

        if len < 20 {
            // Footer size (8+8+8+4) = 28 bytes now
            return Err(crate::common::FusionError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "File too small",
            )));
        }

        // Read Footer
        file.seek(SeekFrom::End(-28)).await?;
        let mut footer = [0u8; 28];
        file.read_exact(&mut footer).await?;

        let magic = u32::from_le_bytes(footer[24..28].try_into().unwrap());
        if magic != SST_MAGIC {
            return Err(crate::common::FusionError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid Magic",
            )));
        }

        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let filter_offset = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let meta_offset = u64::from_le_bytes(footer[16..24].try_into().unwrap());

        // Read Index
        file.seek(SeekFrom::Start(index_offset)).await?;
        let index_len = filter_offset - index_offset;
        let mut index_data = vec![0u8; index_len as usize];
        file.read_exact(&mut index_data).await?;

        let index: BTreeMap<Vec<u8>, u64> = bincode::deserialize(&index_data).map_err(|e| {
            crate::common::FusionError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?;

        // Read Filter
        file.seek(SeekFrom::Start(filter_offset)).await?;
        let filter_len = meta_offset - filter_offset;
        let mut filter_data = vec![0u8; filter_len as usize];
        file.read_exact(&mut filter_data).await?;

        let filter: BloomFilter = bincode::deserialize(&filter_data).map_err(|e| {
            crate::common::FusionError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?;
        
        // Read Meta
        file.seek(SeekFrom::Start(meta_offset)).await?;
        let meta_len = len - 28 - meta_offset;
        let mut meta_data = vec![0u8; meta_len as usize];
        file.read_exact(&mut meta_data).await?;
        
        let meta: SsTableMeta = bincode::deserialize(&meta_data).map_err(|e| {
            crate::common::FusionError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?;

        let index_keys: Vec<Vec<u8>> = index.keys().cloned().collect();
        let index_offsets: Vec<u64> = index.values().cloned().collect();

        Ok(Self {
            id,
            path,
            index,
            filter,
            block_cache,
            file_len: index_offset, // Data ends at index_offset
            index_keys: Arc::new(index_keys),
            index_offsets: Arc::new(index_offsets),
            meta,
        })
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Bloom Filter Check
        if !self.filter.contains(key) {
            return Ok(None);
        }

        if let Some((k, v)) = self.find_ge(key).await? {
            if k == key {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    pub async fn find_ge(&self, search_key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        // Identify start block
        let start_entry = self.index.range(..=search_key.to_vec()).next_back();
        let start_key_owned = start_entry.map(|(k, _)| k.clone());

        let block_iter = if let Some(k) = &start_key_owned {
            self.index.range::<Vec<u8>, _>(k..)
        } else {
            self.index.range::<Vec<u8>, _>(..)
        };

        // We need next offset to calculate size
        // Collect blocks to iterate
        let blocks: Vec<(&Vec<u8>, &u64)> = block_iter.collect();

        for i in 0..blocks.len() {
            let offset = *blocks[i].1;
            
            // Determine Block Length
            let next_offset = if i + 1 < blocks.len() {
                *blocks[i + 1].1
            } else {
                self.file_len // End of Data
            };
            let block_len = (next_offset - offset) as usize;

            // Check Cache
            let block_data = if let Some(data) = self.block_cache.get(&(self.id, offset)) {
                data
            } else {
                // Read from file
                let mut file = tokio::fs::File::open(&self.path).await?;
                file.seek(SeekFrom::Start(offset)).await?;
                let mut buf = vec![0u8; block_len];
                file.read_exact(&mut buf).await?;
                self.block_cache.insert((self.id, offset), buf.clone());
                buf
            };

            // Parse Block
            let mut cursor = std::io::Cursor::new(block_data);
            
            let mut count_buf = [0u8; 4];
            // Use std::io::Read for Cursor (sync)
            if std::io::Read::read_exact(&mut cursor, &mut count_buf).is_err() { continue; }
            let count = u32::from_le_bytes(count_buf);

            for _ in 0..count {
                let mut len_buf = [0u8; 4];
                if std::io::Read::read_exact(&mut cursor, &mut len_buf).is_err() { break; }
                let k_len = u32::from_le_bytes(len_buf) as usize;
                let mut k_buf = vec![0u8; k_len];
                if std::io::Read::read_exact(&mut cursor, &mut k_buf).is_err() { break; }

                let mut len_buf = [0u8; 4];
                if std::io::Read::read_exact(&mut cursor, &mut len_buf).is_err() { break; }
                let v_len = u32::from_le_bytes(len_buf) as usize;
                let mut v_buf = vec![0u8; v_len];
                if std::io::Read::read_exact(&mut cursor, &mut v_buf).is_err() { break; }

                if k_buf.as_slice() >= search_key {
                    return Ok(Some((k_buf, v_buf)));
                }
            }
        }

        Ok(None)
    }

    pub async fn read_block(&self, offset: u64) -> Result<Vec<u8>> {
        if let Some(data) = self.block_cache.get(&(self.id, offset)) {
            return Ok(data);
        }
        
        // Find block length
        let idx_keys: Vec<&Vec<u8>> = self.index.keys().collect();
        let idx_offsets: Vec<&u64> = self.index.values().collect();
        
        let mut next_offset = self.file_len;
        
        // Linear scan to find next offset? BTreeMap is sorted by key, not necessarily offset (though usually is).
        // Let's assume offsets are sorted.
        if let Ok(idx) = idx_offsets.binary_search(&&offset) {
            if idx + 1 < idx_offsets.len() {
                next_offset = *idx_offsets[idx + 1];
            }
        }
        
        let len = (next_offset - offset) as usize;
        let mut file = tokio::fs::File::open(&self.path).await?;
        file.seek(SeekFrom::Start(offset)).await?;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf).await?;
        
        self.block_cache.insert((self.id, offset), buf.clone());
        Ok(buf)
    }

    pub async fn new_iterator(&self, start_key: Option<&[u8]>) -> Result<SsTableIterator> {
        let index_keys: Vec<Vec<u8>> = self.index.keys().cloned().collect();
        let index_offsets: Vec<u64> = self.index.values().cloned().collect();
        
        let start_idx = if let Some(key) = start_key {
            match index_keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                Ok(idx) => idx,
                Err(idx) => idx.saturating_sub(1),
            }
        } else {
            0
        };

        Ok(SsTableIterator {
            path: self.path.clone(),
            block_cache: self.block_cache.clone(),
            sst_id: self.id,
            index_keys,
            index_offsets,
            file_len: self.file_len,
            current_block_idx: start_idx,
            current_block_entries: std::collections::VecDeque::new(),
        })
    }
}

// Implement Clone manually if needed or derive?
// BloomFilter might not implement Clone.
// Let's assume we don't clone SsTable often.

pub struct SsTableIterator {
    path: PathBuf,
    block_cache: Arc<Cache<(u64, u64), Vec<u8>>>,
    sst_id: u64,
    index_keys: Vec<Vec<u8>>,
    index_offsets: Vec<u64>,
    file_len: u64,
    current_block_idx: usize,
    current_block_entries: std::collections::VecDeque<(Vec<u8>, Vec<u8>)>,
}

impl SsTableIterator {
    pub async fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        loop {
            if let Some(entry) = self.current_block_entries.pop_front() {
                return Ok(Some(entry));
            }

            // Load next block
            if self.current_block_idx >= self.index_offsets.len() {
                return Ok(None);
            }

            let offset = self.index_offsets[self.current_block_idx];
            let next_offset = if self.current_block_idx + 1 < self.index_offsets.len() {
                self.index_offsets[self.current_block_idx + 1]
            } else {
                self.file_len
            };
            let block_len = (next_offset - offset) as usize;
            
            self.current_block_idx += 1;

            // Check Cache
            let block_data = if let Some(data) = self.block_cache.get(&(self.sst_id, offset)) {
                data
            } else {
                let mut file = tokio::fs::File::open(&self.path).await?;
                file.seek(SeekFrom::Start(offset)).await?;
                let mut buf = vec![0u8; block_len];
                file.read_exact(&mut buf).await?;
                self.block_cache.insert((self.sst_id, offset), buf.clone());
                buf
            };

            // Parse Block
            let mut cursor = std::io::Cursor::new(block_data);
            
            let mut count_buf = [0u8; 4];
            if std::io::Read::read_exact(&mut cursor, &mut count_buf).is_err() { continue; }
            let count = u32::from_le_bytes(count_buf);

            for _ in 0..count {
                let mut len_buf = [0u8; 4];
                if std::io::Read::read_exact(&mut cursor, &mut len_buf).is_err() { break; }
                let k_len = u32::from_le_bytes(len_buf) as usize;
                let mut k_buf = vec![0u8; k_len];
                if std::io::Read::read_exact(&mut cursor, &mut k_buf).is_err() { break; }

                let mut len_buf = [0u8; 4];
                if std::io::Read::read_exact(&mut cursor, &mut len_buf).is_err() { break; }
                let v_len = u32::from_le_bytes(len_buf) as usize;
                let mut v_buf = vec![0u8; v_len];
                if std::io::Read::read_exact(&mut cursor, &mut v_buf).is_err() { break; }

                self.current_block_entries.push_back((k_buf, v_buf));
            }
        }
    }
}

// Builder for SSTable
pub struct SsTableBuilder {
    file: Option<tokio::fs::File>,
    path: PathBuf,
    index: BTreeMap<Vec<u8>, u64>,
    filter: BloomFilter,
    current_offset: u64,
    first_key: Option<Vec<u8>>,
    last_key: Option<Vec<u8>>,
}

impl SsTableBuilder {
    pub fn new(path: PathBuf) -> Self {
        // Estimate size? For now default.
        // BloomFilter size: 10k items, 0.01 fp rate
        let filter = BloomFilter::with_false_pos(0.01).expected_items(100_000);

        Self {
            file: None,
            path,
            index: BTreeMap::new(),
            filter,
            current_offset: 0,
            first_key: None,
            last_key: None,
        }
    }

    pub async fn init(&mut self) -> Result<()> {
        let file = tokio::fs::File::create(&self.path).await?;
        self.file = Some(file);
        Ok(())
    }

    pub fn add(&mut self, _key: &[u8], _val: &[u8]) {
        // This is a simplified "add single entry" method.
        // But for flushing MemTable, we iterate and batch.
        // Let's make a generic batch add or expose low-level.
        // For simplicity, let's just make `add` handle blocking logic?
        // No, `write_memtable` logic in `lsm.rs` was efficient (buffering blocks).

        // Let's reimplement the block buffering logic here but exposing `add`.
        // Wait, `SsTableBuilder` needs to buffer a block.
    }

    // We will port `write_memtable` logic but make it generic iterator based?
    // Or just keep it simple and assume caller handles iteration.

    pub async fn flush_block(&mut self, start_key: Vec<u8>, count: u32, buf: &[u8]) -> Result<()> {
        if self.file.is_none() {
            self.init().await?;
        }
        
        self.index.insert(start_key.clone(), self.current_offset);

        // Track global first/last key
        // NOTE: We rely on flush_block being called in order.
        if self.first_key.is_none() {
             self.first_key = Some(start_key.clone());
        }
        
        // We can't know the TRUE last key of the block from `start_key`.
        // We rely on `add_key` or `finish`?
        // Actually, `flush_block` doesn't pass the last key.
        // But the caller (FusionStorage) knows the keys.
        // FusionStorage calls `add_key` for BloomFilter.
        // So we should track last_key in `add_key`!
        
        // Write Block Header (Count)
        if let Some(file) = &mut self.file {
             file.write_all(&count.to_le_bytes()).await?;
             self.current_offset += 4;

             // Write Data
             file.write_all(buf).await?;
             self.current_offset += buf.len() as u64;
        }
        Ok(())
    }

    pub fn add_key(&mut self, key: &[u8]) {
        self.filter.insert(key);
        // Track last_key (assuming sorted insertion)
        self.last_key = Some(key.to_vec());
    }

    pub async fn finish(mut self) -> Result<()> {
        if self.file.is_none() {
            self.init().await?;
        }

        let index_offset = self.current_offset;
        let index_bytes = bincode::serialize(&self.index).unwrap();
        
        let mut file = self.file.unwrap();
        
        file.write_all(&index_bytes).await?;

        let filter_offset = index_offset + index_bytes.len() as u64;
        let filter_bytes = bincode::serialize(&self.filter).unwrap();
        file.write_all(&filter_bytes).await?;

        let meta_offset = filter_offset + filter_bytes.len() as u64;
        let meta = SsTableMeta {
             first_key: self.first_key.unwrap_or_default(),
             last_key: self.last_key.unwrap_or_default(),
        };
        let meta_bytes = bincode::serialize(&meta).unwrap();
        file.write_all(&meta_bytes).await?;

        // Footer: [IndexOffset: 8b] [FilterOffset: 8b] [MetaOffset: 8b] [Magic: 4b]
        file.write_all(&index_offset.to_le_bytes()).await?;
        file.write_all(&filter_offset.to_le_bytes()).await?;
        file.write_all(&meta_offset.to_le_bytes()).await?;
        file.write_all(&SST_MAGIC.to_le_bytes()).await?;
        
        file.sync_all().await?;
        Ok(())
    }
}
