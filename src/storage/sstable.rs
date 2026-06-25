use crate::common::Result;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use moka::sync::Cache;
use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

use crc32fast::Hasher as Crc32Hasher;
use fastbloom::BloomFilter;

// --- SSTable ---
// Format:
// [Data Block 1] [Data Block 2] ... [Index Block] [Filter Block] [Meta Block] [Footer]
// Data Block: legacy [Count: 4b] [Entry 1] ... [CRC32: 4b],
// or compressed [Magic: 4b] [Count: 4b] [LZ4 payload] [CRC32: 4b]
// Entry: [KeyLen: 4b] [Key] [ValLen: 4b] [Val]
// Index Block: [Entry 1] ... (Key -> Offset)
// Filter Block: [FilterBytes]
// Footer: [IndexOffset: 8b] [FilterOffset: 8b] [MetaOffset: 8b] [Magic: 4b]

const SST_MAGIC: u32 = 0xCAFEBABE;
const COMPRESSED_BLOCK_MAGIC: &[u8; 4] = b"FDBL";
const COMPRESSED_BLOCK_HEADER_LEN: usize = 8;

fn block_entry_buffer() -> VecDeque<(Vec<u8>, Vec<u8>)> {
    VecDeque::with_capacity(1)
}

fn block_entry_reserve_count(count: u32, block_len: usize) -> usize {
    (count as usize).min(block_len / 8)
}

fn legacy_block_len(entry_bytes_len: usize) -> usize {
    4 + entry_bytes_len
}

fn compressed_block_payload_capacity(compressed_len: usize) -> usize {
    COMPRESSED_BLOCK_HEADER_LEN + compressed_len
}

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
    pub async fn open(
        path: PathBuf,
        id: u64,
        block_cache: Arc<Cache<(u64, u64), Vec<u8>>>,
    ) -> Result<Self> {
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

        let mut index_keys = Vec::with_capacity(index.len());
        let mut index_offsets = Vec::with_capacity(index.len());
        for (key, offset) in &index {
            index_keys.push(key.clone());
            index_offsets.push(*offset);
        }

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
        if self.index_offsets.is_empty() {
            return Ok(None);
        }

        let start_idx = match self
            .index_keys
            .binary_search_by(|key| key.as_slice().cmp(search_key))
        {
            Ok(idx) => idx,
            Err(idx) => idx.saturating_sub(1),
        };

        for i in start_idx..self.index_offsets.len() {
            let offset = self.index_offsets[i];
            let block_data = self.read_block(offset).await?;

            let mut cursor = std::io::Cursor::new(block_data);

            let mut count_buf = [0u8; 4];
            if std::io::Read::read_exact(&mut cursor, &mut count_buf).is_err() {
                continue;
            }
            let count = u32::from_le_bytes(count_buf);

            for _ in 0..count {
                let mut len_buf = [0u8; 4];
                if std::io::Read::read_exact(&mut cursor, &mut len_buf).is_err() {
                    break;
                }
                let k_len = u32::from_le_bytes(len_buf) as usize;
                let mut k_buf = vec![0u8; k_len];
                if std::io::Read::read_exact(&mut cursor, &mut k_buf).is_err() {
                    break;
                }

                let mut len_buf = [0u8; 4];
                if std::io::Read::read_exact(&mut cursor, &mut len_buf).is_err() {
                    break;
                }
                let v_len = u32::from_le_bytes(len_buf) as usize;
                let mut v_buf = vec![0u8; v_len];
                if std::io::Read::read_exact(&mut cursor, &mut v_buf).is_err() {
                    break;
                }

                if k_buf.as_slice() >= search_key {
                    return Ok(Some((k_buf, v_buf)));
                }
            }
        }

        Ok(None)
    }

    /// Verify CRC32 of a block. Returns the data portion (without trailing CRC).
    fn verify_block_crc(block_data: &[u8]) -> Result<&[u8]> {
        if block_data.len() < 4 {
            return Ok(block_data); // Too small for CRC, legacy block
        }
        let (data, crc_bytes) = block_data.split_at(block_data.len() - 4);
        let stored_crc = u32::from_le_bytes(crc_bytes.try_into().unwrap_or([0; 4]));
        // If stored_crc is 0, treat as legacy block without checksum
        if stored_crc == 0 {
            return Ok(block_data);
        }
        let mut hasher = Crc32Hasher::new();
        hasher.update(data);
        let computed = hasher.finalize();
        if computed != stored_crc {
            return Err(crate::common::FusionError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "SSTable block CRC mismatch: expected {:08x}, got {:08x}",
                    stored_crc, computed
                ),
            )));
        }
        Ok(data)
    }

    fn encode_block_payload(count: u32, entries: &[u8]) -> Vec<u8> {
        let compressed = compress_prepend_size(entries);
        let compressed_len = compressed_block_payload_capacity(compressed.len());
        let legacy_len = legacy_block_len(entries.len());

        if compressed_len < legacy_len {
            let mut encoded = Vec::with_capacity(compressed_len);
            encoded.extend_from_slice(COMPRESSED_BLOCK_MAGIC);
            encoded.extend_from_slice(&count.to_le_bytes());
            encoded.extend_from_slice(&compressed);
            encoded
        } else {
            let mut encoded = Vec::with_capacity(legacy_len);
            encoded.extend_from_slice(&count.to_le_bytes());
            encoded.extend_from_slice(entries);
            encoded
        }
    }

    fn decode_block_payload(block_data: &[u8]) -> Result<Vec<u8>> {
        let data = Self::verify_block_crc(block_data)?;
        if data.len() < COMPRESSED_BLOCK_HEADER_LEN
            || &data[..COMPRESSED_BLOCK_MAGIC.len()] != COMPRESSED_BLOCK_MAGIC
        {
            return Ok(data.to_vec());
        }

        let count = u32::from_le_bytes(data[4..8].try_into().unwrap());
        let entries =
            decompress_size_prepended(&data[COMPRESSED_BLOCK_HEADER_LEN..]).map_err(|error| {
                crate::common::FusionError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("SSTable block decompression failed: {}", error),
                ))
            })?;

        let mut decoded = Vec::with_capacity(legacy_block_len(entries.len()));
        decoded.extend_from_slice(&count.to_le_bytes());
        decoded.extend_from_slice(&entries);
        Ok(decoded)
    }

    async fn read_block_at(
        path: &PathBuf,
        block_cache: &Arc<Cache<(u64, u64), Vec<u8>>>,
        sst_id: u64,
        index_offsets: &[u64],
        file_len: u64,
        offset: u64,
    ) -> Result<Vec<u8>> {
        if let Some(data) = block_cache.get(&(sst_id, offset)) {
            return Ok(data);
        }

        let mut next_offset = file_len;

        if let Ok(idx) = index_offsets.binary_search(&offset) {
            if idx + 1 < index_offsets.len() {
                next_offset = index_offsets[idx + 1];
            }
        }

        let len = (next_offset - offset) as usize;
        let mut file = tokio::fs::File::open(path).await?;
        file.seek(SeekFrom::Start(offset)).await?;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf).await?;

        let decoded = Self::decode_block_payload(&buf)?;
        block_cache.insert((sst_id, offset), decoded.clone());
        Ok(decoded)
    }

    pub async fn read_block(&self, offset: u64) -> Result<Vec<u8>> {
        Self::read_block_at(
            &self.path,
            &self.block_cache,
            self.id,
            &self.index_offsets,
            self.file_len,
            offset,
        )
        .await
    }

    pub async fn new_iterator(&self, start_key: Option<&[u8]>) -> Result<SsTableIterator> {
        let start_idx = if let Some(key) = start_key {
            match self.index_keys.binary_search_by(|k| k.as_slice().cmp(key)) {
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
            index_offsets: self.index_offsets.clone(),
            file_len: self.file_len,
            current_block_idx: start_idx,
            current_block_entries: block_entry_buffer(),
            lower_bound: start_key.map(|key| key.to_vec()),
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
    index_offsets: Arc<Vec<u64>>,
    file_len: u64,
    current_block_idx: usize,
    current_block_entries: VecDeque<(Vec<u8>, Vec<u8>)>,
    lower_bound: Option<Vec<u8>>,
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

            let block_data = SsTable::read_block_at(
                &self.path,
                &self.block_cache,
                self.sst_id,
                &self.index_offsets,
                self.file_len,
                offset,
            )
            .await?;

            // Parse Block
            let mut cursor = std::io::Cursor::new(block_data);

            let mut count_buf = [0u8; 4];
            if std::io::Read::read_exact(&mut cursor, &mut count_buf).is_err() {
                continue;
            }
            let count = u32::from_le_bytes(count_buf);
            self.current_block_entries
                .reserve(block_entry_reserve_count(count, cursor.get_ref().len()));

            for _ in 0..count {
                let mut len_buf = [0u8; 4];
                if std::io::Read::read_exact(&mut cursor, &mut len_buf).is_err() {
                    break;
                }
                let k_len = u32::from_le_bytes(len_buf) as usize;
                let mut k_buf = vec![0u8; k_len];
                if std::io::Read::read_exact(&mut cursor, &mut k_buf).is_err() {
                    break;
                }

                let mut len_buf = [0u8; 4];
                if std::io::Read::read_exact(&mut cursor, &mut len_buf).is_err() {
                    break;
                }
                let v_len = u32::from_le_bytes(len_buf) as usize;
                let mut v_buf = vec![0u8; v_len];
                if std::io::Read::read_exact(&mut cursor, &mut v_buf).is_err() {
                    break;
                }

                if self.lower_bound.as_ref().map_or(true, |lower_bound| {
                    k_buf.as_slice() >= lower_bound.as_slice()
                }) {
                    self.current_block_entries.push_back((k_buf, v_buf));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        block_entry_buffer, block_entry_reserve_count, Crc32Hasher, SsTable, SsTableBuilder,
        COMPRESSED_BLOCK_MAGIC,
    };
    use moka::sync::Cache;
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    #[test]
    fn block_entry_buffer_preallocates_first_entry() {
        assert!(block_entry_buffer().capacity() >= 1);
    }

    #[test]
    fn block_entry_reserve_count_is_bounded_by_block_length() {
        assert_eq!(block_entry_reserve_count(3, 24), 3);
        assert_eq!(block_entry_reserve_count(100, 16), 2);
    }

    fn append_crc(mut payload: Vec<u8>) -> Vec<u8> {
        let mut hasher = Crc32Hasher::new();
        hasher.update(&payload);
        let crc = hasher.finalize();
        payload.extend_from_slice(&crc.to_le_bytes());
        payload
    }

    #[test]
    fn compressed_block_payload_round_trips_to_legacy_layout() {
        let entries = vec![b'x'; 4096];
        let encoded = SsTable::encode_block_payload(3, &entries);

        assert_eq!(
            &encoded[..COMPRESSED_BLOCK_MAGIC.len()],
            COMPRESSED_BLOCK_MAGIC
        );

        let decoded = SsTable::decode_block_payload(&append_crc(encoded)).unwrap();
        assert_eq!(u32::from_le_bytes(decoded[..4].try_into().unwrap()), 3);
        assert_eq!(&decoded[4..], entries.as_slice());
    }

    #[test]
    fn legacy_block_payload_stays_readable() {
        let entries = b"not worth compressing".to_vec();
        let mut legacy = Vec::new();
        legacy.extend_from_slice(&1u32.to_le_bytes());
        legacy.extend_from_slice(&entries);

        let decoded = SsTable::decode_block_payload(&append_crc(legacy.clone())).unwrap();

        assert_eq!(decoded, legacy);
    }

    #[tokio::test]
    async fn sstable_compresses_repetitive_blocks_and_reads_entries() {
        let path = std::env::temp_dir().join(format!(
            "fusiondb_sstable_compress_{}.sst",
            uuid::Uuid::new_v4()
        ));
        let mut builder = SsTableBuilder::new(path.clone());
        let mut block_buffer = Vec::new();
        let mut first_key = None;
        let mut expected_value = Vec::new();
        expected_value.extend(std::iter::repeat(b'a').take(512));

        for id in 0..64 {
            let key = format!("k{id:03}").into_bytes();
            if first_key.is_none() {
                first_key = Some(key.clone());
            }
            builder.add_key(&key);
            block_buffer.extend_from_slice(&(key.len() as u32).to_le_bytes());
            block_buffer.extend_from_slice(&key);
            block_buffer.extend_from_slice(&(expected_value.len() as u32).to_le_bytes());
            block_buffer.extend_from_slice(&expected_value);
        }

        builder
            .flush_block(first_key.unwrap(), 64, &block_buffer)
            .await
            .unwrap();
        builder.finish().await.unwrap();

        let mut file = tokio::fs::File::open(&path).await.unwrap();
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic).await.unwrap();
        assert_eq!(&magic, COMPRESSED_BLOCK_MAGIC);

        let table = SsTable::open(path.clone(), 1, Arc::new(Cache::new(16)))
            .await
            .unwrap();
        let found = table.find_ge(b"k050").await.unwrap().unwrap();
        assert_eq!(found.0, b"k050");
        assert_eq!(found.1, expected_value);

        let mut iter = table.new_iterator(None).await.unwrap();
        let mut count = 0;
        while let Some((_key, value)) = iter.next().await.unwrap() {
            assert_eq!(value.len(), 512);
            count += 1;
        }
        assert_eq!(count, 64);

        let _ = std::fs::remove_file(&path);
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

        if self.first_key.is_none() {
            self.first_key = Some(start_key.clone());
        }

        if let Some(file) = &mut self.file {
            let encoded = SsTable::encode_block_payload(count, buf);

            // CRC covers the encoded block payload, whether compressed or legacy.
            let mut hasher = Crc32Hasher::new();
            hasher.update(&encoded);
            let crc = hasher.finalize();

            file.write_all(&encoded).await?;
            self.current_offset += encoded.len() as u64;
            file.write_all(&crc.to_le_bytes()).await?;
            self.current_offset += 4;
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
