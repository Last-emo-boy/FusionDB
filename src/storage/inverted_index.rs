use crate::common::{FusionError, Result};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
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

fn postings_map() -> HashMap<String, Vec<(String, u32)>> {
    HashMap::with_capacity(1)
}

fn doc_lengths_map() -> HashMap<String, u32> {
    HashMap::with_capacity(1)
}

impl Default for InvertedIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl InvertedIndex {
    pub fn new() -> Self {
        Self {
            postings: postings_map(),
            doc_lengths: doc_lengths_map(),
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

        let mut term_freqs = HashMap::with_capacity(tokens.len());
        for token in tokens {
            *term_freqs.entry(token).or_insert(0) += 1;
        }

        for (term, freq) in term_freqs {
            self.postings
                .entry(term)
                .or_insert_with(|| Vec::with_capacity(1))
                .push((doc_id.clone(), freq));
        }

        let replaced_doc_len = self.doc_lengths.insert(doc_id, doc_len).unwrap_or(0);
        let total_len = self.avg_doc_length as f64 * self.total_docs as f64
            - replaced_doc_len as f64
            + doc_len as f64;
        self.total_docs += 1;
        self.avg_doc_length = (total_len / self.total_docs as f64) as f32;
    }

    fn tokenize(&self, text: &str) -> Vec<String> {
        text.split_whitespace()
            .filter_map(|raw_token| {
                let mut token = String::with_capacity(raw_token.len());
                for c in raw_token.chars() {
                    for lower in c.to_lowercase() {
                        if lower.is_alphanumeric() {
                            token.push(lower);
                        }
                    }
                }

                if token.is_empty() {
                    None
                } else {
                    Some(token)
                }
            })
            .collect()
    }

    pub fn search_bm25(&self, query: &str, k1: f32, b: f32) -> Vec<(String, f32)> {
        self.search_bm25_limited(query, k1, b, usize::MAX)
    }

    pub fn search_bm25_limited(
        &self,
        query: &str,
        k1: f32,
        b: f32,
        limit: usize,
    ) -> Vec<(String, f32)> {
        if limit == 0 {
            return Vec::new();
        }

        let query_tokens = self.tokenize(query);
        let mut posting_lists = Vec::with_capacity(query_tokens.len());
        let mut score_capacity = 0usize;
        for term in &query_tokens {
            if let Some(posting_list) = self.postings.get(term) {
                score_capacity = score_capacity.saturating_add(posting_list.len());
                posting_lists.push(posting_list);
            }
        }

        if posting_lists.is_empty() {
            return Vec::new();
        }

        let mut scores: HashMap<String, f32> =
            HashMap::with_capacity(score_capacity.min(self.doc_lengths.len()));

        for posting_list in posting_lists {
            // Calculate IDF
            let doc_freq = posting_list.len();
            let idf = ((self.total_docs as f32 - doc_freq as f32 + 0.5) / (doc_freq as f32 + 0.5)
                + 1.0)
                .ln();

            for (doc_id, freq) in posting_list {
                let doc_len = *self.doc_lengths.get(doc_id).unwrap_or(&0);
                let tf = *freq as f32;

                let numerator = tf * (k1 + 1.0);
                let denominator = tf + k1 * (1.0 - b + b * (doc_len as f32 / self.avg_doc_length));

                let score = idf * (numerator / denominator);
                *scores.entry(doc_id.clone()).or_insert(0.0) += score;
            }
        }

        let mut result = Vec::with_capacity(scores.len());
        for score in scores {
            result.push(score);
        }
        if result.len() > limit {
            let _ = result.select_nth_unstable_by(limit, bm25_score_order);
            result.truncate(limit);
        }
        result.sort_by(bm25_score_order);
        result
    }
}

fn bm25_score_order(a: &(String, f32), b: &(String, f32)) -> Ordering {
    b.1.partial_cmp(&a.1).unwrap()
}

#[cfg(test)]
mod tests {
    use super::{doc_lengths_map, postings_map, InvertedIndex};

    #[test]
    fn add_document_updates_average_length_incrementally() {
        let mut index = InvertedIndex::new();

        index.add_document("doc1".to_string(), "quick brown fox");
        index.add_document("doc2".to_string(), "quick fox");

        assert_eq!(index.total_docs, 2);
        assert_eq!(index.doc_lengths.get("doc1"), Some(&3));
        assert_eq!(index.doc_lengths.get("doc2"), Some(&2));
        assert!((index.avg_doc_length - 2.5).abs() < f32::EPSILON);
    }

    #[test]
    fn new_preallocates_first_document_maps() {
        assert!(postings_map().capacity() >= 1);
        assert!(doc_lengths_map().capacity() >= 1);
    }

    #[test]
    fn add_document_preallocates_new_posting_lists() {
        let mut index = InvertedIndex::new();

        index.add_document("doc1".to_string(), "quick");

        let postings = index.postings.get("quick").unwrap();
        assert_eq!(postings.len(), 1);
        assert!(postings.capacity() >= 1);
    }

    #[test]
    fn add_document_preserves_existing_duplicate_doc_id_average_semantics() {
        let mut index = InvertedIndex::new();

        index.add_document("doc1".to_string(), "one two three");
        index.add_document("doc1".to_string(), "one two");

        assert_eq!(index.total_docs, 2);
        assert_eq!(index.doc_lengths.get("doc1"), Some(&2));
        assert!((index.avg_doc_length - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn tokenize_lowercases_and_removes_punctuation_within_whitespace_tokens() {
        let index = InvertedIndex::new();

        let tokens = index.tokenize("Quick, QUICK! co-op ...");

        assert_eq!(tokens, vec!["quick", "quick", "coop"]);
    }

    #[test]
    fn search_bm25_limited_matches_full_result_prefix() {
        let mut index = InvertedIndex::new();

        index.add_document("doc1".to_string(), "apple apple apple apple");
        index.add_document("doc2".to_string(), "apple apple apple");
        index.add_document("doc3".to_string(), "apple apple");
        index.add_document("doc4".to_string(), "banana");

        let full = index.search_bm25("apple", 1.2, 0.75);
        let limited = index.search_bm25_limited("apple", 1.2, 0.75, 2);

        assert_eq!(limited, full[..2]);
    }

    #[test]
    fn search_bm25_limited_zero_skips_work() {
        let mut index = InvertedIndex::new();
        index.add_document("doc1".to_string(), "apple");

        assert!(index.search_bm25_limited("apple", 1.2, 0.75, 0).is_empty());
    }
}
