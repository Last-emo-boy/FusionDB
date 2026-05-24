use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Trait for embedding providers. Implementations convert text to vectors.
pub trait EmbeddingProvider: Send + Sync {
    fn embed(&self, text: &str) -> Vec<f32>;
    fn dimension(&self) -> usize;
    fn name(&self) -> &str;
}

/// Built-in bag-of-words embedding provider.
/// Uses a learned vocabulary to produce fixed-dimension sparse vectors.
/// Suitable for simple similarity search without external dependencies.
pub struct BuiltinEmbeddingProvider {
    vocab: RwLock<HashMap<String, usize>>,
    dim: usize,
}

impl BuiltinEmbeddingProvider {
    pub fn new(dim: usize) -> Self {
        Self {
            vocab: RwLock::new(HashMap::new()),
            dim,
        }
    }

    fn tokenize(text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty() && s.len() > 1)
            .map(|s| s.to_string())
            .collect()
    }

    fn get_or_assign_index(&self, token: &str) -> usize {
        {
            let vocab = self.vocab.read();
            if let Some(&idx) = vocab.get(token) {
                return idx;
            }
        }
        let mut vocab = self.vocab.write();
        let next = vocab.len();
        *vocab.entry(token.to_string()).or_insert(next)
    }
}

impl EmbeddingProvider for BuiltinEmbeddingProvider {
    fn embed(&self, text: &str) -> Vec<f32> {
        let tokens = Self::tokenize(text);
        let mut vec = vec![0.0f32; self.dim];

        if tokens.is_empty() {
            return vec;
        }

        // Hash each token to a dimension and accumulate
        for token in &tokens {
            let idx = self.get_or_assign_index(token) % self.dim;
            vec[idx] += 1.0;
        }

        // L2 normalize
        let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for v in &mut vec {
                *v /= norm;
            }
        }

        vec
    }

    fn dimension(&self) -> usize {
        self.dim
    }

    fn name(&self) -> &str {
        "builtin-bow"
    }
}

/// Global embedding registry. Allows selecting providers by name.
pub struct EmbeddingRegistry {
    providers: RwLock<HashMap<String, Arc<dyn EmbeddingProvider>>>,
    default_name: RwLock<String>,
}

impl EmbeddingRegistry {
    pub fn new() -> Self {
        let builtin = Arc::new(BuiltinEmbeddingProvider::new(128)) as Arc<dyn EmbeddingProvider>;
        let mut providers = HashMap::new();
        providers.insert("builtin-bow".to_string(), builtin);
        Self {
            providers: RwLock::new(providers),
            default_name: RwLock::new("builtin-bow".to_string()),
        }
    }

    pub fn register(&self, provider: Arc<dyn EmbeddingProvider>) {
        let name = provider.name().to_string();
        self.providers.write().insert(name, provider);
    }

    pub fn get_default(&self) -> Option<Arc<dyn EmbeddingProvider>> {
        let name = self.default_name.read().clone();
        self.providers.read().get(&name).cloned()
    }

    #[allow(dead_code)]
    pub fn set_default(&self, name: &str) {
        *self.default_name.write() = name.to_string();
    }

    pub fn embed(&self, text: &str) -> Option<Vec<f32>> {
        self.get_default().map(|p| p.embed(text))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builtin_embedding_produces_correct_dim() {
        let provider = BuiltinEmbeddingProvider::new(64);
        let vec = provider.embed("hello world test");
        assert_eq!(vec.len(), 64);
    }

    #[test]
    fn test_builtin_embedding_normalized() {
        let provider = BuiltinEmbeddingProvider::new(64);
        let vec = provider.embed("hello world");
        let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (norm - 1.0).abs() < 0.01,
            "Vector should be L2-normalized, got norm={}",
            norm
        );
    }

    #[test]
    fn test_similar_texts_closer() {
        let provider = BuiltinEmbeddingProvider::new(128);
        let v1 = provider.embed("rust programming language");
        let v2 = provider.embed("rust language programming");
        let v3 = provider.embed("cooking recipes food kitchen");

        let sim_12: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
        let sim_13: f32 = v1.iter().zip(v3.iter()).map(|(a, b)| a * b).sum();
        assert!(
            sim_12 > sim_13,
            "Similar texts should have higher cosine sim: {} vs {}",
            sim_12,
            sim_13
        );
    }

    #[test]
    fn test_registry_default() {
        let reg = EmbeddingRegistry::new();
        let vec = reg.embed("test text").unwrap();
        assert_eq!(vec.len(), 128);
    }

    #[test]
    fn test_empty_text() {
        let provider = BuiltinEmbeddingProvider::new(64);
        let vec = provider.embed("");
        assert_eq!(vec.len(), 64);
        assert!(vec.iter().all(|v| *v == 0.0));
    }
}
