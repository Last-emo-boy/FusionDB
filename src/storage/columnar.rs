// use crate::common::Result;
use arrow::array::{Array, Float32Array, StringArray};
use std::sync::Arc; // Unused but kept for consistency if needed later

pub struct ColumnarVectorStore {
    // Column-oriented data
    ids: Arc<StringArray>,
    // Flattened embeddings: [v1_d1, v1_d2, v1_d3, v2_d1...]
    // We assume fixed dimension = 3 for the benchmark
    embeddings: Arc<Float32Array>,
    dimension: usize,
}

impl ColumnarVectorStore {
    pub fn new(ids: Vec<String>, vectors: Vec<Vec<f32>>, dimension: usize) -> Self {
        let ids_array = Arc::new(StringArray::from(ids));

        let mut flat_vec = Vec::with_capacity(vectors.len() * dimension);
        for v in vectors {
            flat_vec.extend_from_slice(&v);
        }
        let embeddings_array = Arc::new(Float32Array::from(flat_vec));

        Self {
            ids: ids_array,
            embeddings: embeddings_array,
            dimension,
        }
    }

    // Manual SIMD-friendly loop
    // In Rust, the compiler auto-vectorizes this loop if we write it correctly.
    // Explicit AVX intrinsic usage would be faster but unsafe/platform-specific.
    pub fn search(&self, query: &[f32], limit: usize) -> Vec<(String, f32)> {
        let num_rows = self.ids.len();
        let mut scores = Vec::with_capacity(num_rows);

        // Access raw slice for speed (skip bounds check)
        let embedding_values = self.embeddings.values();

        for i in 0..num_rows {
            let start = i * self.dimension;
            let end = start + self.dimension;
            let vec_slice = &embedding_values[start..end];

            // L2 Distance Squared
            let mut dist_sq = 0.0;
            for (j, &q_val) in query.iter().enumerate() {
                let diff = q_val - vec_slice[j];
                dist_sq += diff * diff;
            }

            scores.push((dist_sq, i));
        }

        // Sort and top-K
        // Partial sort is better O(N + K log K) vs O(N log N)
        // For simplicity:
        scores.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        scores
            .iter()
            .take(limit)
            .map(|(score, idx)| (self.ids.value(*idx).to_string(), score.sqrt()))
            .collect()
    }
}
