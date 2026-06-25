use crate::common::{FusionError, Result};
use hora::core::ann_index::ANNIndex;
use hora::index::hnsw_idx::HNSWIndex;
use hora::index::hnsw_params::HNSWParams;
use parking_lot::RwLock;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

const ANN_OVERSAMPLING_FACTOR: usize = 4;

// Wrapper for a single HNSW index to handle lazy initialization and parameters
struct HnswIndexWrapper {
    index: Option<HNSWIndex<f32, String>>,
    dimension: usize,
    vectors: HashMap<String, Vec<f32>>,
    dirty: bool,
}

impl HnswIndexWrapper {
    fn with_vector_capacity(dimension: usize, vector_capacity: usize) -> Self {
        Self {
            index: None,
            dimension,
            vectors: HashMap::with_capacity(vector_capacity),
            dirty: false,
        }
    }

    fn set_dimension_if_needed(&mut self, dim: usize) -> Result<()> {
        if self.dimension == 0 {
            self.dimension = dim;
            return Ok(());
        }

        if self.dimension != dim {
            return Err(FusionError::Execution(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.dimension, dim
            )));
        }

        Ok(())
    }

    fn upsert_vector(&mut self, id: String, vector: Vec<f32>) -> Result<()> {
        self.set_dimension_if_needed(vector.len())?;
        self.vectors.insert(id, vector);
        self.dirty = true;
        Ok(())
    }

    fn remove_vector(&mut self, id: &str) -> bool {
        let removed = self.vectors.remove(id).is_some();
        if removed {
            self.index = None;
            self.dirty = !self.vectors.is_empty();
            if self.vectors.is_empty() {
                self.dimension = 0;
            }
        }
        removed
    }

    fn ensure_built(&mut self) -> Result<()> {
        if !self.dirty && self.index.is_some() {
            return Ok(());
        }

        if self.vectors.is_empty() {
            self.index = None;
            self.dirty = false;
            return Ok(());
        }

        if self.dimension == 0 {
            return Err(FusionError::Execution(
                "Cannot build HNSW index without a vector dimension".to_string(),
            ));
        }

        let params = HNSWParams::<f32>::default();
        let mut rebuilt = HNSWIndex::new(self.dimension, &params);
        for (id, vector) in &self.vectors {
            rebuilt
                .add(vector, id.clone())
                .map_err(|e| FusionError::Execution(format!("HNSW insert error: {:?}", e)))?;
        }
        rebuilt
            .build(hora::core::metrics::Metric::Euclidean)
            .map_err(|e| FusionError::Execution(format!("HNSW build error: {:?}", e)))?;

        self.index = Some(rebuilt);
        self.dirty = false;
        Ok(())
    }
}

pub struct VectorIndex {
    indexes: RwLock<HashMap<String, Arc<RwLock<HnswIndexWrapper>>>>,
}

fn vector_index_map() -> HashMap<String, Arc<RwLock<HnswIndexWrapper>>> {
    HashMap::with_capacity(1)
}

impl Default for VectorIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl VectorIndex {
    pub fn new() -> Self {
        Self {
            indexes: RwLock::new(vector_index_map()),
        }
    }

    fn get_or_create_wrapper(&self, name: &str) -> Arc<RwLock<HnswIndexWrapper>> {
        self.get_or_create_wrapper_with_capacity(name, 0)
    }

    fn get_or_create_wrapper_with_capacity(
        &self,
        name: &str,
        vector_capacity: usize,
    ) -> Arc<RwLock<HnswIndexWrapper>> {
        if let Some(existing) = self.indexes.read().get(name).cloned() {
            return existing;
        }

        let mut indexes = self.indexes.write();
        indexes
            .entry(name.to_string())
            .or_insert_with(|| {
                Arc::new(RwLock::new(HnswIndexWrapper::with_vector_capacity(
                    0,
                    vector_capacity,
                )))
            })
            .clone()
    }

    pub fn create_index(&self, name: &str) {
        let _ = self.get_or_create_wrapper(name);
    }

    pub fn clear(&self) {
        self.indexes.write().clear();
    }

    pub fn insert(&self, name: &str, id: String, vector: Vec<f32>) -> Result<()> {
        let wrapper_lock = self.get_or_create_wrapper(name);
        let mut wrapper = wrapper_lock.write();
        wrapper.upsert_vector(id, vector)
    }

    pub fn batch_insert(&self, name: &str, items: Vec<(String, Vec<f32>)>) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        let wrapper_lock = self.get_or_create_wrapper_with_capacity(name, items.len());
        let mut wrapper = wrapper_lock.write();
        wrapper.vectors.reserve(items.len());

        for (id, vector) in items {
            wrapper.upsert_vector(id, vector)?;
        }

        wrapper.ensure_built()
    }

    pub fn delete(&self, name: &str, id: &str) -> Result<bool> {
        let wrapper_lock = {
            let indexes = self.indexes.read();
            indexes.get(name).cloned()
        };

        if let Some(wrapper_lock) = wrapper_lock {
            let mut wrapper = wrapper_lock.write();
            Ok(wrapper.remove_vector(id))
        } else {
            Ok(false)
        }
    }

    pub fn search(&self, name: &str, query: &[f32], k: usize) -> Result<Vec<(String, f32)>> {
        let wrapper_lock = {
            let indexes = self.indexes.read();
            indexes.get(name).cloned()
        };

        if let Some(wrapper_lock) = wrapper_lock {
            let mut wrapper = wrapper_lock.write();

            if wrapper.dimension != 0 && query.len() != wrapper.dimension {
                return Err(FusionError::Execution(format!(
                    "Query vector dimension mismatch: expected {}, got {}",
                    wrapper.dimension,
                    query.len()
                )));
            }

            if k == 0 {
                return Ok(vec![]);
            }

            wrapper.ensure_built()?;

            if let Some(index) = &wrapper.index {
                let candidate_count = std::cmp::min(
                    wrapper.vectors.len(),
                    std::cmp::max(k, k.saturating_mul(ANN_OVERSAMPLING_FACTOR)),
                );
                let results = index.search(query, candidate_count);
                // Compute real Euclidean distances from stored vectors
                let mut scored = Vec::with_capacity(results.len());
                for id in results {
                    if let Some(vector) = wrapper.vectors.get(&id) {
                        scored.push((id, euclidean_distance(query, vector)));
                    }
                }
                if scored.len() > k {
                    let _ = scored.select_nth_unstable_by(k, vector_distance_order);
                    scored.truncate(k);
                }
                // Sort retained candidates by distance ascending (closest first)
                scored.sort_by(vector_distance_order);
                Ok(scored)
            } else {
                Ok(vec![])
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

fn vector_distance_order(a: &(String, f32), b: &(String, f32)) -> Ordering {
    a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal)
}

#[cfg(test)]
mod tests {
    use super::{vector_index_map, HnswIndexWrapper, VectorIndex};

    #[test]
    fn wrapper_with_vector_capacity_reserves_vectors() {
        let wrapper = HnswIndexWrapper::with_vector_capacity(0, 4);
        assert!(wrapper.vectors.capacity() >= 4);
    }

    #[test]
    fn vector_index_map_preallocates_default_index_slot() {
        let indexes = vector_index_map();
        assert!(indexes.capacity() >= 1);
    }

    #[test]
    fn search_builds_index_lazily() {
        let index = VectorIndex::new();
        index.create_index("default");
        index
            .insert("default", "a".to_string(), vec![1.0, 0.0])
            .unwrap();
        index
            .insert("default", "b".to_string(), vec![0.0, 1.0])
            .unwrap();

        let results = index.search("default", &[1.0, 0.0], 2).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "a");
        assert!(results[0].1 <= results[1].1);
    }

    #[test]
    fn delete_removes_vector_from_future_searches() {
        let index = VectorIndex::new();
        index.create_index("default");
        index
            .batch_insert(
                "default",
                vec![
                    ("a".to_string(), vec![1.0, 0.0]),
                    ("b".to_string(), vec![0.0, 1.0]),
                ],
            )
            .unwrap();

        assert!(index.delete("default", "a").unwrap());

        let results = index.search("default", &[1.0, 0.0], 2).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "b");
    }

    #[test]
    fn upsert_replaces_existing_vector() {
        let index = VectorIndex::new();
        index.create_index("default");
        index
            .insert("default", "a".to_string(), vec![10.0, 10.0])
            .unwrap();
        index
            .insert("default", "a".to_string(), vec![1.0, 1.0])
            .unwrap();

        let results = index.search("default", &[1.0, 1.0], 1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "a");
        assert!(results[0].1 < 0.001);
    }

    #[test]
    fn search_limited_results_remain_sorted_by_distance() {
        let index = VectorIndex::new();
        index
            .batch_insert(
                "default",
                vec![
                    ("far".to_string(), vec![10.0, 0.0]),
                    ("near".to_string(), vec![0.1, 0.0]),
                    ("mid".to_string(), vec![2.0, 0.0]),
                    ("edge".to_string(), vec![5.0, 0.0]),
                ],
            )
            .unwrap();

        let results = index.search("default", &[0.0, 0.0], 2).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "near");
        assert_eq!(results[1].0, "mid");
        assert!(results[0].1 <= results[1].1);
    }

    #[test]
    fn search_zero_limit_skips_lazy_build_but_validates_dimension() {
        let index = VectorIndex::new();
        index.create_index("default");
        index
            .insert("default", "a".to_string(), vec![1.0, 0.0])
            .unwrap();

        let results = index.search("default", &[1.0, 0.0], 0).unwrap();
        assert!(results.is_empty());

        let wrapper_lock = index.indexes.read().get("default").cloned().unwrap();
        let wrapper = wrapper_lock.read();
        assert!(wrapper.dirty);
        assert!(wrapper.index.is_none());
        drop(wrapper);

        assert!(index.search("default", &[1.0], 0).is_err());
    }
}
