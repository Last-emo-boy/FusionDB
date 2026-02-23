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
    vectors: HashMap<String, Vec<f32>>,
    #[allow(dead_code)]
    dirty: bool, // reserved for future deferred-build optimization
}

impl HnswIndexWrapper {
    fn new(dimension: usize) -> Self {
        Self {
            index: None,
            dimension,
            vectors: HashMap::new(),
            dirty: false,
        }
    }

    fn ensure_index(&mut self, dim: usize) {
        if self.index.is_none() {
            if self.dimension == 0 {
                self.dimension = dim;
            }
            let params = HNSWParams::<f32>::default();
            self.index = Some(HNSWIndex::new(dim, &params));
        }
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
            wrapper.ensure_index(dim);

            {
                let index = wrapper.index.as_mut().unwrap();
                index
                    .add(&vector, id.clone())
                    .map_err(|e| FusionError::Execution(format!("HNSW insert error: {:?}", e)))?;
                index
                    .build(hora::core::metrics::Metric::Euclidean)
                    .map_err(|e| FusionError::Execution(format!("HNSW build error: {:?}", e)))?;
            }

            wrapper.vectors.insert(id, vector);
            Ok(())
        } else {
            Err(FusionError::Execution(format!(
                "Vector index {} not found",
                name
            )))
        }
    }

    pub fn batch_insert(&self, name: &str, items: Vec<(String, Vec<f32>)>) -> Result<()> {
        let indexes = self.indexes.read();
        if let Some(wrapper_lock) = indexes.get(name) {
            let mut wrapper = wrapper_lock.write();
            if items.is_empty() {
                return Ok(());
            }
            let dim = items[0].1.len();
            wrapper.ensure_index(dim);

            let vecs: Vec<(String, Vec<f32>)> = items;

            {
                let index = wrapper.index.as_mut().unwrap();
                for (id, vector) in &vecs {
                    index
                        .add(vector, id.clone())
                        .map_err(|e| FusionError::Execution(format!("HNSW insert error: {:?}", e)))?;
                }
                // Build once after all inserts
                index
                    .build(hora::core::metrics::Metric::Euclidean)
                    .map_err(|e| FusionError::Execution(format!("HNSW build error: {:?}", e)))?;
            }

            for (id, vector) in vecs {
                wrapper.vectors.insert(id, vector);
            }
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
                // Compute real Euclidean distances from stored vectors
                let mut scored: Vec<(String, f32)> = results
                    .into_iter()
                    .map(|id| {
                        let dist = if let Some(vec) = wrapper.vectors.get(&id) {
                            euclidean_distance(query, vec)
                        } else {
                            f32::MAX
                        };
                        (id, dist)
                    })
                    .collect();
                // Sort by distance ascending (closest first)
                scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
                Ok(scored)
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

fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y) * (x - y))
        .sum::<f32>()
        .sqrt()
}
