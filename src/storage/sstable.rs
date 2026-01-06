use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use crate::common::Result;

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

pub struct SsTable {
    pub id: u64,
    pub path: PathBuf,
    pub index: BTreeMap<Vec<u8>, u64>,
    pub filter: BloomFilter,
}

impl SsTable {
    pub async fn open(path: PathBuf, id: u64) -> Result<Self> {
        let mut file = tokio::fs::File::open(&path).await?;
        let len = file.metadata().await?.len();
        
        if len < 20 { // Footer size (8+8+4)
             return Err(crate::common::FusionError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "File too small")));
        }

        // Read Footer
        file.seek(SeekFrom::End(-20)).await?;
        let mut footer = [0u8; 20];
        file.read_exact(&mut footer).await?;
        
        let magic = u32::from_le_bytes(footer[16..20].try_into().unwrap());
        if magic != SST_MAGIC {
             return Err(crate::common::FusionError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid Magic")));
        }
        
        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let filter_offset = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        
        // Read Index
        file.seek(SeekFrom::Start(index_offset)).await?;
        let index_len = filter_offset - index_offset;
        let mut index_data = vec![0u8; index_len as usize];
        file.read_exact(&mut index_data).await?;
        
        let index: BTreeMap<Vec<u8>, u64> = bincode::deserialize(&index_data)
            .map_err(|e| crate::common::FusionError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?;

        // Read Filter
        file.seek(SeekFrom::Start(filter_offset)).await?;
        let filter_len = len - 20 - filter_offset;
        let mut filter_data = vec![0u8; filter_len as usize];
        file.read_exact(&mut filter_data).await?;
        
        let filter: BloomFilter = bincode::deserialize(&filter_data)
            .map_err(|e| crate::common::FusionError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?;
            
        Ok(Self {
            id,
            path,
            index,
            filter,
        })
    }
    
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Bloom Filter Check
        if !self.filter.contains(key) {
            return Ok(None);
        }

        // ... (existing get implementation)
        // Actually, we can implement get using find_ge if we want, but get is exact match.
        // Let's keep get as is for now or use find_ge?
        // find_ge returns >= key.
        if let Some((k, v)) = self.find_ge(key).await? {
            if k == key { return Ok(Some(v)); }
        }
        Ok(None)
    }

    pub async fn find_ge(&self, search_key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        // Identify start block
        let start_entry = self.index.range(..=search_key.to_vec()).next_back();
        let start_key_owned = start_entry.map(|(k, _)| k.clone());
        
        let mut block_iter = if let Some(k) = start_key_owned {
            self.index.range(k..)
        } else {
            self.index.range::<Vec<u8>, _>(..)
        };

        // Iterate blocks
        for (_, offset) in block_iter {
            let mut file = tokio::fs::File::open(&self.path).await?;
            file.seek(SeekFrom::Start(*offset)).await?;
            
            let mut count_buf = [0u8; 4];
            file.read_exact(&mut count_buf).await?;
            let count = u32::from_le_bytes(count_buf);
            
            for _ in 0..count {
                let mut len_buf = [0u8; 4];
                file.read_exact(&mut len_buf).await?;
                let k_len = u32::from_le_bytes(len_buf) as usize;
                let mut k_buf = vec![0u8; k_len];
                file.read_exact(&mut k_buf).await?;
                
                let mut len_buf = [0u8; 4];
                file.read_exact(&mut len_buf).await?;
                let v_len = u32::from_le_bytes(len_buf) as usize;
                let mut v_buf = vec![0u8; v_len];
                file.read_exact(&mut v_buf).await?;
                
                if k_buf.as_slice() >= search_key {
                    return Ok(Some((k_buf, v_buf)));
                }
            }
        }
        
        Ok(None)
    }
    pub async fn new_iterator(&self) -> Result<SsTableIterator> {
        let file = tokio::fs::File::open(&self.path).await?;
        Ok(SsTableIterator {
            file,
            index_keys: self.index.keys().cloned().collect(),
            index_offsets: self.index.values().cloned().collect(),
            current_block_idx: 0,
            current_block_entries: std::collections::VecDeque::new(),
        })
    }
}

pub struct SsTableIterator {
    file: tokio::fs::File,
    index_keys: Vec<Vec<u8>>,
    index_offsets: Vec<u64>,
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
            self.current_block_idx += 1;

            self.file.seek(SeekFrom::Start(offset)).await?;
            
            let mut count_buf = [0u8; 4];
            self.file.read_exact(&mut count_buf).await?;
            let count = u32::from_le_bytes(count_buf);

            for _ in 0..count {
                let mut len_buf = [0u8; 4];
                self.file.read_exact(&mut len_buf).await?;
                let k_len = u32::from_le_bytes(len_buf) as usize;
                let mut k_buf = vec![0u8; k_len];
                self.file.read_exact(&mut k_buf).await?;
                
                let mut len_buf = [0u8; 4];
                self.file.read_exact(&mut len_buf).await?;
                let v_len = u32::from_le_bytes(len_buf) as usize;
                let mut v_buf = vec![0u8; v_len];
                self.file.read_exact(&mut v_buf).await?;
                
                self.current_block_entries.push_back((k_buf, v_buf));
            }
        }
    }
}

// Builder for SSTable
pub struct SsTableBuilder {
    path: PathBuf,
    data: Vec<u8>,
    index: BTreeMap<Vec<u8>, u64>,
    filter: BloomFilter,
    current_offset: u64,
}

impl SsTableBuilder {
    pub fn new(path: PathBuf) -> Self {
        // Estimate size? For now default.
        // BloomFilter size: 10k items, 0.01 fp rate
        let filter = BloomFilter::with_false_pos(0.01).expected_items(100_000); 
        
        Self {
            path,
            data: Vec::new(),
            index: BTreeMap::new(),
            filter,
            current_offset: 0,
        }
    }
    
    pub fn add(&mut self, key: &[u8], val: &[u8]) {
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
    
    pub fn flush_block(&mut self, start_key: Vec<u8>, count: u32, buf: &[u8]) {
        self.index.insert(start_key, self.current_offset);
        
        // Add keys to Bloom Filter
        // We need to parse the buffer again? Or caller should add?
        // Caller iterates anyway. But `flush_block` takes a raw buffer.
        // Parsing raw buffer is annoying here.
        // Let's change `write_memtable` in `fusion.rs` (or `lsm.rs`) to add to filter.
        // Or better: `flush_block` is low level. We should add a method `add_key_to_filter`.
        
        // Write Block Header (Count)
        self.data.extend_from_slice(&count.to_le_bytes());
        self.current_offset += 4;
        
        // Write Data
        self.data.extend_from_slice(buf);
        self.current_offset += buf.len() as u64;
    }
    
    pub fn add_key(&mut self, key: &[u8]) {
        self.filter.insert(key);
    }
    
    pub async fn finish(mut self) -> Result<()> {
        let index_offset = self.current_offset;
        let index_bytes = bincode::serialize(&self.index).unwrap();
        self.data.extend_from_slice(&index_bytes);
        
        let filter_offset = self.current_offset + index_bytes.len() as u64;
        let filter_bytes = bincode::serialize(&self.filter).unwrap();
        self.data.extend_from_slice(&filter_bytes);
        
        // Footer: [IndexOffset: 8b] [FilterOffset: 8b] [Magic: 4b]
        self.data.extend_from_slice(&index_offset.to_le_bytes());
        self.data.extend_from_slice(&filter_offset.to_le_bytes());
        self.data.extend_from_slice(&SST_MAGIC.to_le_bytes());
        
        let mut file = tokio::fs::File::create(&self.path).await?;
        file.write_all(&self.data).await?;
        file.sync_all().await?;
        Ok(())
    }
}
