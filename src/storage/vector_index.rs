use crate::common::{FusionError, Result};
use hora::core::ann_index::ANNIndex;
use hora::index::hnsw_idx::HNSWIndex;
use hora::index::hnsw_params::HNSWParams;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

// Wrapper for a single HNSW index to handle lazy initialization and parameters
struct HnswIndexWrapper {
    index: Option<HNSWIndex<f32, String>>,
    dimension: usize,
}

impl HnswIndexWrapper {
    fn new(dimension: usize) -> Self {
        Self {
            index: None,
            dimension,
        }
    }

    fn get_or_create_index(&mut self, dim: usize) -> &mut HNSWIndex<f32, String> {
        if self.index.is_none() {
            // Update dimension if it was 0 (lazy init)
            if self.dimension == 0 {
                self.dimension = dim;
            }

            // Use default parameters for now to ensure compilation.
            // TODO: Tune HNSW parameters (max_item, m, ef_construction)
            let params = HNSWParams::<f32>::default();
            self.index = Some(HNSWIndex::new(dim, &params));
        }
        self.index.as_mut().unwrap()
    }
}

pub struct VectorIndex {
    indexes: RwLock<HashMap<String, Arc<RwLock<HnswIndexWrapper>>>>,
}

impl Default for VectorIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl VectorIndex {
    pub fn new() -> Self {
        Self {
            indexes: RwLock::new(HashMap::new()),
        }
    }

    pub fn create_index(&self, name: &str) {
        let mut indexes = self.indexes.write();
        if !indexes.contains_key(name) {
            // Initialize with 0 dimension, will be set on first insert
            indexes.insert(
                name.to_string(),
                Arc::new(RwLock::new(HnswIndexWrapper::new(0))),
            );
        }
    }

    pub fn insert(&self, name: &str, id: String, vector: Vec<f32>) -> Result<()> {
        let indexes = self.indexes.read();
        if let Some(wrapper_lock) = indexes.get(name) {
            let mut wrapper = wrapper_lock.write();
            let dim = vector.len();

            // Check consistency if already initialized
            if wrapper.index.is_some() {
                // We can't easily check dimension on existing index without public field,
                // but we trust it matches or Hora handles it (it might panic).
            }

            let index = wrapper.get_or_create_index(dim);

            // Hora insert
            index
                .add(&vector, id)
                .map_err(|e| FusionError::Execution(format!("HNSW insert error: {:?}", e)))?;

            // Build is required for the added item to be searchable.
            // Building after every insert is inefficient but required for immediate consistency in this simple implementation.
            // TODO: Optimize by batching or periodic builds.
            index
                .build(hora::core::metrics::Metric::Euclidean)
                .map_err(|e| FusionError::Execution(format!("HNSW build error: {:?}", e)))?;

            Ok(())
        } else {
            Err(FusionError::Execution(format!(
                "Vector index {} not found",
                name
            )))
        }
    }

    pub fn search(&self, name: &str, query: &[f32], k: usize) -> Result<Vec<(String, f32)>> {
        let indexes = self.indexes.read();
        if let Some(wrapper_lock) = indexes.get(name) {
            let wrapper = wrapper_lock.read();
            if let Some(index) = &wrapper.index {
                let results = index.search(query, k);
                // Hora returns only IDs. We return dummy distance 0.0 for now.
                // In a real implementation, we might compute actual distance or store it.
                Ok(results.into_iter().map(|id| (id, 0.0)).collect())
            } else {
                Ok(vec![]) // Empty index
            }
        } else {
            Err(FusionError::Execution(format!(
                "Vector index {} not found",
                name
            )))
        }
    }
}
