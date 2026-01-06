use crate::common::{FusionError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
// use std::sync::Arc;

// Simple Inverted Index for BM25
// Term -> [(DocID, Frequency)]
#[derive(Serialize, Deserialize)]
pub struct InvertedIndex {
    postings: HashMap<String, Vec<(String, u32)>>,
    doc_lengths: HashMap<String, u32>,
    avg_doc_length: f32,
    total_docs: usize,
}

impl Default for InvertedIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl InvertedIndex {
    pub fn new() -> Self {
        Self {
            postings: HashMap::new(),
            doc_lengths: HashMap::new(),
            avg_doc_length: 0.0,
            total_docs: 0,
        }
    }

    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let file = std::fs::File::create(path).map_err(FusionError::Io)?;
        bincode::serialize_into(file, self)
            .map_err(|e| FusionError::Execution(format!("Serialization error: {:?}", e)))?;
        Ok(())
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let file = std::fs::File::open(path).map_err(FusionError::Io)?;
        let index = bincode::deserialize_from(file)
            .map_err(|e| FusionError::Execution(format!("Deserialization error: {:?}", e)))?;
        Ok(index)
    }

    pub fn add_document(&mut self, doc_id: String, text: &str) {
        let tokens = self.tokenize(text);
        let doc_len = tokens.len() as u32;

        let mut term_freqs = HashMap::new();
        for token in tokens {
            *term_freqs.entry(token).or_insert(0) += 1;
        }

        for (term, freq) in term_freqs {
            self.postings
                .entry(term)
                .or_default()
                .push((doc_id.clone(), freq));
        }

        self.doc_lengths.insert(doc_id, doc_len);
        self.total_docs += 1;

        // Update avg length (simplified)
        let total_len: u32 = self.doc_lengths.values().sum();
        self.avg_doc_length = total_len as f32 / self.total_docs as f32;
    }

    fn tokenize(&self, text: &str) -> Vec<String> {
        text.to_lowercase()
            .split_whitespace()
            .map(|s| s.replace(|c: char| !c.is_alphanumeric(), ""))
            .filter(|s| !s.is_empty())
            .collect()
    }

    pub fn search_bm25(&self, query: &str, k1: f32, b: f32) -> Vec<(String, f32)> {
        let query_tokens = self.tokenize(query);
        let mut scores: HashMap<String, f32> = HashMap::new();

        for term in query_tokens {
            if let Some(posting_list) = self.postings.get(&term) {
                // Calculate IDF
                let doc_freq = posting_list.len();
                let idf = ((self.total_docs as f32 - doc_freq as f32 + 0.5)
                    / (doc_freq as f32 + 0.5)
                    + 1.0)
                    .ln();

                for (doc_id, freq) in posting_list {
                    let doc_len = *self.doc_lengths.get(doc_id).unwrap_or(&0);
                    let tf = *freq as f32;

                    let numerator = tf * (k1 + 1.0);
                    let denominator =
                        tf + k1 * (1.0 - b + b * (doc_len as f32 / self.avg_doc_length));

                    let score = idf * (numerator / denominator);
                    *scores.entry(doc_id.clone()).or_insert(0.0) += score;
                }
            }
        }

        let mut result: Vec<_> = scores.into_iter().collect();
        result.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        result
    }
}
