use super::memory::MemoryStorage;
use super::{FusionStorage, Storage};
use crate::common::{FusionError, Result};
use std::sync::Arc;

/// Supported storage backend types.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum BackendConfig {
    /// FusionStorage: MVCC + LSM-Tree + SSTable (default, production)
    Fusion { wal_path: String },
    /// In-memory storage (testing, development)
    Memory { wal_path: String },
    // Future backends:
    // RocksDB { path: String, options: RocksDbOptions },
    // MDBX { path: String, map_size: usize },
}

impl Default for BackendConfig {
    fn default() -> Self {
        BackendConfig::Fusion {
            wal_path: "fusion.wal".to_string(),
        }
    }
}

/// Create a storage backend from configuration.
pub async fn create_backend(config: &BackendConfig) -> Result<Arc<dyn Storage>> {
    match config {
        BackendConfig::Fusion { wal_path } => {
            let storage = FusionStorage::new(wal_path).await?;
            Ok(Arc::new(storage))
        }
        BackendConfig::Memory { wal_path } => {
            let storage = MemoryStorage::new(wal_path)?;
            Ok(Arc::new(storage))
        }
    }
}

/// Parse backend config from a TOML-like string or environment variable.
/// Format: "fusion:path/to/wal" or "memory:path/to/wal"
pub fn parse_backend_config(s: &str) -> Result<BackendConfig> {
    if let Some(path) = s.strip_prefix("fusion:") {
        Ok(BackendConfig::Fusion {
            wal_path: path.to_string(),
        })
    } else if let Some(path) = s.strip_prefix("memory:") {
        Ok(BackendConfig::Memory {
            wal_path: path.to_string(),
        })
    } else if s == "fusion" {
        Ok(BackendConfig::default())
    } else if s == "memory" {
        Ok(BackendConfig::Memory {
            wal_path: "memory.wal".to_string(),
        })
    } else {
        Err(FusionError::Storage(format!(
            "Unknown backend: '{}'. Supported: fusion, memory",
            s
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_fusion_default() {
        let config = parse_backend_config("fusion").unwrap();
        match config {
            BackendConfig::Fusion { wal_path } => assert_eq!(wal_path, "fusion.wal"),
            _ => panic!("Expected Fusion"),
        }
    }

    #[test]
    fn test_parse_fusion_custom_path() {
        let config = parse_backend_config("fusion:/data/db.wal").unwrap();
        match config {
            BackendConfig::Fusion { wal_path } => assert_eq!(wal_path, "/data/db.wal"),
            _ => panic!("Expected Fusion"),
        }
    }

    #[test]
    fn test_parse_memory() {
        let config = parse_backend_config("memory").unwrap();
        assert!(matches!(config, BackendConfig::Memory { .. }));
    }

    #[test]
    fn test_parse_unknown() {
        let result = parse_backend_config("redis");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_memory_backend() {
        let config = BackendConfig::Memory {
            wal_path: format!("test_backend_{}.wal", std::process::id()),
        };
        let storage = create_backend(&config).await.unwrap();
        let txn = storage.begin_transaction().await.unwrap();
        txn.commit().await.unwrap();
        if let BackendConfig::Memory { wal_path } = &config {
            let _ = std::fs::remove_file(wal_path);
        }
    }

    #[test]
    fn test_default_config() {
        let config = BackendConfig::default();
        assert!(matches!(config, BackendConfig::Fusion { .. }));
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = BackendConfig::Fusion {
            wal_path: "test.wal".to_string(),
        };
        let json = serde_json::to_string(&config).unwrap();
        let parsed: BackendConfig = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, BackendConfig::Fusion { .. }));
    }
}
