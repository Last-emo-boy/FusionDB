use crate::common::{FusionError, Result};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::path::Path;

/// Split text into trigrams (3-byte sequences)
pub fn trigrams_bytes(s: &str) -> Vec<u32> {
    let b = s.as_bytes();
    if b.len() < 3 {
        return vec![];
    }
    let mut out = Vec::with_capacity(b.len() - 2);
    for i in 0..=b.len() - 3 {
        // pack 3 bytes -> u32 key (Big Endian-ish)
        let val = ((b[i] as u32) << 16) | ((b[i + 1] as u32) << 8) | (b[i + 2] as u32);
        out.push(val);
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// Stable numeric id for a row key: comparable-encoded integers decode
/// directly, plain integers parse, anything else hashes deterministically.
/// (Moved from the executor so the storage-level rebuild can share it.)
pub fn numeric_row_id_for_str(row_id: &str) -> u64 {
    if let Some(n) = crate::common::encoding::decode_i64_comparable(row_id) {
        n as u64
    } else if let Ok(n) = row_id.parse::<u64>() {
        n
    } else {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        row_id.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Serialize, Deserialize)]
pub struct TrigramIndex {
    // table_name -> column_name -> trigram -> bitmap(row_id)
    postings: HashMap<String, HashMap<String, HashMap<u32, RoaringTreemap>>>,
    // table_name -> rid(u64) -> original row_id string
    id_map: HashMap<String, HashMap<u64, String>>,
}

fn postings_map() -> HashMap<String, HashMap<String, HashMap<u32, RoaringTreemap>>> {
    HashMap::with_capacity(1)
}

fn id_map() -> HashMap<String, HashMap<u64, String>> {
    HashMap::with_capacity(1)
}

impl Default for TrigramIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl TrigramIndex {
    pub fn new() -> Self {
        Self {
            postings: postings_map(),
            id_map: id_map(),
        }
    }

    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let file = std::fs::File::create(path).map_err(FusionError::Io)?;
        bincode::serialize_into(file, self)
            .map_err(|e| FusionError::Execution(format!("Trigram serialization error: {:?}", e)))?;
        Ok(())
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let file = std::fs::File::open(path).map_err(FusionError::Io)?;
        let index = bincode::deserialize_from(file).map_err(|e| {
            FusionError::Execution(format!("Trigram deserialization error: {:?}", e))
        })?;
        Ok(index)
    }

    pub fn add_with_id_str(
        &mut self,
        table: &str,
        col: &str,
        row_id: u64,
        row_id_str: &str,
        text: &str,
    ) {
        let grams = trigrams_bytes(text);
        let table_map = self
            .postings
            .entry(table.to_string())
            .or_insert_with(|| HashMap::with_capacity(1));
        let col_map = table_map
            .entry(col.to_string())
            .or_insert_with(|| HashMap::with_capacity(grams.len()));
        let map = self
            .id_map
            .entry(table.to_string())
            .or_insert_with(|| HashMap::with_capacity(1));
        map.insert(row_id, row_id_str.to_string());

        for tg in grams {
            col_map.entry(tg).or_default().insert(row_id);
        }
    }

    pub fn remove_with_id(&mut self, table: &str, col: &str, row_id: u64, text: &str) {
        let grams = trigrams_bytes(text);
        let Some(table_map) = self.postings.get_mut(table) else {
            return;
        };
        let Some(col_map) = table_map.get_mut(col) else {
            return;
        };

        for tg in grams {
            let should_remove = if let Some(bitmap) = col_map.get_mut(&tg) {
                bitmap.remove(row_id);
                bitmap.is_empty()
            } else {
                false
            };
            if should_remove {
                col_map.remove(&tg);
            }
        }
    }

    pub fn search(&self, table: &str, col: &str, pattern: &str) -> Option<RoaringTreemap> {
        // Pattern processing: remove % and _
        let clean_pattern = if pattern.as_bytes().iter().any(|b| matches!(b, b'%' | b'_')) {
            let mut clean = String::with_capacity(pattern.len());
            for c in pattern.chars() {
                if c != '%' && c != '_' {
                    clean.push(c);
                }
            }
            Cow::Owned(clean)
        } else {
            Cow::Borrowed(pattern)
        };
        let grams = trigrams_bytes(clean_pattern.as_ref());

        if grams.is_empty() {
            return None; // Pattern too short or empty
        }

        let table_map = self.postings.get(table)?;
        let col_map = table_map.get(col)?;

        let mut bitmaps = Vec::with_capacity(grams.len());
        for tg in &grams {
            let Some(bitmap) = col_map.get(tg) else {
                return Some(RoaringTreemap::new());
            };
            if bitmap.is_empty() {
                return Some(RoaringTreemap::new());
            }
            bitmaps.push(bitmap);
        }

        bitmaps.sort_unstable_by_key(|bitmap| bitmap.len());
        let mut bitmap_iter = bitmaps.into_iter();
        let mut result = bitmap_iter.next()?.clone();
        for bitmap in bitmap_iter {
            result &= bitmap;
            if result.is_empty() {
                break;
            }
        }

        Some(result)
    }

    pub fn map_ids_to_row_keys(&self, table: &str, ids: &RoaringTreemap) -> Vec<String> {
        if let Some(map) = self.id_map.get(table) {
            let capacity = ids.len().min(map.len() as u64) as usize;
            let mut out = Vec::with_capacity(capacity);
            for id in ids.iter() {
                if let Some(s) = map.get(&id) {
                    out.push(s.clone());
                }
            }
            return out;
        }
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{id_map, postings_map, trigrams_bytes, TrigramIndex};

    #[test]
    fn trigrams_bytes_deduplicates_sorted_keys() {
        let grams = trigrams_bytes("aaaa");

        assert_eq!(grams.len(), 1);
    }

    #[test]
    fn new_preallocates_first_table_maps() {
        assert!(postings_map().capacity() >= 1);
        assert!(id_map().capacity() >= 1);
    }

    #[test]
    fn search_intersects_trigram_postings_and_maps_row_keys() {
        let mut index = TrigramIndex::new();
        index.add_with_id_str("docs", "body", 1, "row-1", "xxabcdefyy");
        index.add_with_id_str("docs", "body", 2, "row-2", "zzabczz");
        index.add_with_id_str("docs", "body", 3, "row-3", "qqabcdefrr");

        let ids = index.search("docs", "body", "%abcdef%").unwrap();
        let mut row_keys = index.map_ids_to_row_keys("docs", &ids);
        row_keys.sort();

        assert_eq!(row_keys, vec!["row-1".to_string(), "row-3".to_string()]);
        assert!(index
            .search("docs", "body", "%missing%")
            .unwrap()
            .is_empty());
    }
}
