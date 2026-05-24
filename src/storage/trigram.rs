use crate::common::{FusionError, Result};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
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

#[derive(Default, Serialize, Deserialize)]
pub struct TrigramIndex {
    // table_name -> column_name -> trigram -> bitmap(row_id)
    postings: HashMap<String, HashMap<String, HashMap<u32, RoaringTreemap>>>,
    // table_name -> rid(u64) -> original row_id string
    id_map: HashMap<String, HashMap<u64, String>>,
}

impl TrigramIndex {
    pub fn new() -> Self {
        Self {
            postings: HashMap::new(),
            id_map: HashMap::new(),
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
        let table_map = self.postings.entry(table.to_string()).or_default();
        let col_map = table_map.entry(col.to_string()).or_default();
        let map = self.id_map.entry(table.to_string()).or_default();
        map.insert(row_id, row_id_str.to_string());

        for tg in grams {
            col_map.entry(tg).or_default().insert(row_id);
        }
    }

    pub fn search(&self, table: &str, col: &str, pattern: &str) -> Option<RoaringTreemap> {
        // Pattern processing: remove % and _
        let clean_pattern: String = pattern.chars().filter(|c| *c != '%' && *c != '_').collect();
        let grams = trigrams_bytes(&clean_pattern);

        if grams.is_empty() {
            return None; // Pattern too short or empty
        }

        let table_map = self.postings.get(table)?;
        let col_map = table_map.get(col)?;

        // Find intersection of all trigrams
        let mut result: Option<RoaringTreemap> = None;

        for tg in grams {
            if let Some(bm) = col_map.get(&tg) {
                if let Some(res) = &mut result {
                    *res &= bm;
                } else {
                    result = Some(bm.clone());
                }
            } else {
                // If any trigram is missing, the result is empty (AND logic)
                return Some(RoaringTreemap::new());
            }
        }

        result
    }

    pub fn map_ids_to_row_keys(&self, table: &str, ids: &RoaringTreemap) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(map) = self.id_map.get(table) {
            for id in ids.iter() {
                if let Some(s) = map.get(&id) {
                    out.push(s.clone());
                }
            }
        }
        out
    }
}
