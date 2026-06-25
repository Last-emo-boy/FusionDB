use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// FusionDB server configuration, loaded from `fusiondb.toml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub auth: AuthConfig,
    pub tls: TlsConfig,
    pub distributed: DistributedConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// HTTP API port
    pub http_port: u16,
    /// PostgreSQL wire protocol port (http_port + 1 if not set)
    pub pg_port: u16,
    /// Enable the Redis-compatible RESP endpoint for native memtier probes
    pub redis_enabled: bool,
    /// Redis-compatible RESP endpoint port
    pub redis_port: u16,
    /// Bind address
    pub bind: String,
    /// Maximum concurrent PostgreSQL wire protocol connections
    pub max_connections: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// Base data directory for all persistent files
    pub data_dir: String,
    /// WAL file name (relative to data_dir)
    pub wal_file: String,
    /// SSTable directory name (relative to data_dir)
    pub sstable_dir: String,
    /// MemTable flush threshold in MB
    pub memtable_flush_mb: usize,
    /// Row cache capacity (number of entries)
    pub row_cache_capacity: u64,
    /// Statement cache capacity
    pub statement_cache_capacity: u64,
    /// Block cache capacity (number of 4KB blocks)
    pub block_cache_capacity: u64,
    /// Slow query threshold in milliseconds
    pub slow_query_threshold_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    /// Password for PostgreSQL cleartext auth
    pub password: String,
    /// Use SCRAM-SHA-256 instead of cleartext (default: false for backward compat)
    pub scram_sha256: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TlsConfig {
    /// Enable TLS for both pgwire and HTTP
    pub enabled: bool,
    /// Path to PEM certificate file
    pub cert_path: String,
    /// Path to PEM private key file
    pub key_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DistributedConfig {
    /// Enable OpenRaft-backed distributed mode
    pub enabled: bool,
    /// Local Raft node id
    pub node_id: u64,
    /// Address advertised to peer nodes; defaults to server bind/http_port when empty
    pub advertise_addr: String,
    /// Initialize the configured membership on startup
    pub bootstrap: bool,
    /// Application-specific OpenRaft cluster name
    pub cluster_name: String,
    /// Initial voting members used during bootstrap
    pub initial_members: Vec<DistributedPeerConfig>,
    /// Optional automatic sharding control-plane configuration
    pub sharding: ShardingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DistributedPeerConfig {
    pub node_id: u64,
    pub addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ShardingConfig {
    /// Enable automatic shard map and routing metadata
    pub enabled: bool,
    /// Partition strategy: hash or range
    pub strategy: ShardingStrategy,
    /// Number of shards for hash routing; range routing uses boundaries + 1 when boundaries exist
    pub shard_count: u64,
    /// Lexicographic upper bounds for range shards
    pub range_boundaries: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ShardingStrategy {
    Hash,
    Range,
}

// --- Defaults ---

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_path: "certs/server.crt".to_string(),
            key_path: "certs/server.key".to_string(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
            distributed: DistributedConfig::default(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_port: 8091,
            pg_port: 8092,
            redis_enabled: false,
            redis_port: 6379,
            bind: "127.0.0.1".to_string(),
            max_connections: 100,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "data".to_string(),
            wal_file: "fusion.wal".to_string(),
            sstable_dir: "sstables".to_string(),
            memtable_flush_mb: 32,
            row_cache_capacity: 10_000,
            statement_cache_capacity: 1_000,
            block_cache_capacity: 25_000,
            slow_query_threshold_ms: 100,
        }
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            password: "fusiondb".to_string(),
            scram_sha256: false,
        }
    }
}

impl StorageConfig {
    /// Full path to WAL file
    pub fn wal_path(&self) -> PathBuf {
        Path::new(&self.data_dir).join(&self.wal_file)
    }

    /// Full path to SSTable directory
    pub fn sstable_path(&self) -> PathBuf {
        Path::new(&self.data_dir).join(&self.sstable_dir)
    }

    /// Full path to inverted index snapshot file
    pub fn inverted_index_path(&self) -> PathBuf {
        Path::new(&self.data_dir).join("inverted_index.bin")
    }

    /// Full path to trigram index snapshot file
    pub fn trigram_index_path(&self) -> PathBuf {
        Path::new(&self.data_dir).join("trigram_index.bin")
    }

    /// MemTable flush threshold in bytes
    pub fn memtable_flush_threshold_bytes(&self) -> usize {
        self.memtable_flush_mb.saturating_mul(1024 * 1024)
    }
}

impl ServerConfig {
    /// Build a socket address string for a specific port.
    pub fn socket_addr(&self, port: u16) -> String {
        format!("{}:{}", self.bind, port)
    }
}

impl DistributedConfig {
    /// Address this node should advertise to Raft peers.
    pub fn effective_advertise_addr(&self, server: &ServerConfig) -> String {
        if self.advertise_addr.trim().is_empty() {
            server.socket_addr(server.http_port)
        } else {
            self.advertise_addr.clone()
        }
    }
}

impl Config {
    /// Load config from file path. Falls back to defaults if file doesn't exist.
    pub fn load(path: &str) -> Self {
        match std::fs::read_to_string(path) {
            Ok(content) => match toml::from_str::<Config>(&content) {
                Ok(config) => {
                    println!("Loaded config from {}", path);
                    config
                }
                Err(e) => {
                    eprintln!("Warning: Failed to parse {}: {}. Using defaults.", path, e);
                    Config::default()
                }
            },
            Err(_) => {
                println!("No config file found at {}. Using defaults.", path);
                Config::default()
            }
        }
    }

    /// Write default config to a file (for `fusiondb --init`).
    pub fn write_default(path: &str) -> std::io::Result<()> {
        let config = Config::default();
        let content = toml::to_string_pretty(&config).expect("Failed to serialize default config");
        std::fs::write(path, content)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.server.http_port, 8091);
        assert_eq!(config.server.pg_port, 8092);
        assert!(!config.server.redis_enabled);
        assert_eq!(config.server.redis_port, 6379);
        assert_eq!(config.server.max_connections, 100);
        assert!(!config.distributed.enabled);
        assert_eq!(config.distributed.node_id, 1);
        assert_eq!(config.distributed.cluster_name, "fusiondb");
        assert!(!config.distributed.sharding.enabled);
        assert_eq!(config.distributed.sharding.strategy, ShardingStrategy::Hash);
        assert_eq!(config.distributed.sharding.shard_count, 16);
        assert_eq!(config.storage.data_dir, "data");
        assert_eq!(config.auth.password, "fusiondb");
    }

    #[test]
    fn test_parse_toml() {
        let toml_str = r#"
[server]
http_port = 9091
pg_port = 9092
redis_enabled = true
redis_port = 6380
bind = "0.0.0.0"
max_connections = 250

[storage]
data_dir = "/var/fusiondb"
memtable_flush_mb = 64

[auth]
password = "secret123"

[distributed]
enabled = true
node_id = 2
advertise_addr = "127.0.0.1:19091"
bootstrap = false
cluster_name = "fusiondb-test"

[distributed.sharding]
enabled = true
strategy = "range"
shard_count = 4
range_boundaries = ["m", "t"]

[[distributed.initial_members]]
node_id = 1
addr = "127.0.0.1:8091"
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.server.http_port, 9091);
        assert!(config.server.redis_enabled);
        assert_eq!(config.server.redis_port, 6380);
        assert_eq!(config.server.max_connections, 250);
        assert!(config.distributed.enabled);
        assert_eq!(config.distributed.node_id, 2);
        assert_eq!(config.distributed.advertise_addr, "127.0.0.1:19091");
        assert!(!config.distributed.bootstrap);
        assert_eq!(config.distributed.cluster_name, "fusiondb-test");
        assert_eq!(config.distributed.initial_members.len(), 1);
        assert_eq!(config.distributed.initial_members[0].node_id, 1);
        assert!(config.distributed.sharding.enabled);
        assert_eq!(
            config.distributed.sharding.strategy,
            ShardingStrategy::Range
        );
        assert_eq!(config.distributed.sharding.shard_count, 4);
        assert_eq!(config.distributed.sharding.range_boundaries, vec!["m", "t"]);
        assert_eq!(config.storage.data_dir, "/var/fusiondb");
        assert_eq!(config.storage.memtable_flush_mb, 64);
        assert_eq!(config.auth.password, "secret123");
        // Defaults for unset fields
        assert_eq!(config.storage.wal_file, "fusion.wal");
    }

    #[test]
    fn test_wal_path() {
        let config = StorageConfig {
            data_dir: "/var/db".to_string(),
            wal_file: "my.wal".to_string(),
            ..Default::default()
        };
        assert_eq!(config.wal_path(), PathBuf::from("/var/db/my.wal"));
        assert_eq!(config.sstable_path(), PathBuf::from("/var/db/sstables"));
    }

    #[test]
    fn test_load_missing_file() {
        let config = Config::load("nonexistent_fusiondb.toml");
        assert_eq!(config.server.http_port, 8091);
    }

    #[test]
    fn test_serialize_default() {
        let config = Config::default();
        let serialized = toml::to_string_pretty(&config).unwrap();
        assert!(serialized.contains("http_port = 8091"));
        assert!(serialized.contains("max_connections = 100"));
        assert!(serialized.contains("[distributed]"));
        assert!(serialized.contains("enabled = false"));
        assert!(serialized.contains("[distributed.sharding]"));
        assert!(serialized.contains("strategy = \"hash\""));
        assert!(serialized.contains("data_dir = \"data\""));
    }

    #[test]
    fn distributed_advertise_addr_defaults_to_http_socket() {
        let server = ServerConfig {
            bind: "0.0.0.0".to_string(),
            http_port: 19091,
            ..Default::default()
        };
        let distributed = DistributedConfig::default();

        assert_eq!(
            distributed.effective_advertise_addr(&server),
            "0.0.0.0:19091"
        );
    }
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: 1,
            advertise_addr: String::new(),
            bootstrap: true,
            cluster_name: "fusiondb".to_string(),
            initial_members: Vec::new(),
            sharding: ShardingConfig::default(),
        }
    }
}

impl Default for DistributedPeerConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            addr: "127.0.0.1:8091".to_string(),
        }
    }
}

impl Default for ShardingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            strategy: ShardingStrategy::Hash,
            shard_count: 16,
            range_boundaries: Vec::new(),
        }
    }
}

impl Default for ShardingStrategy {
    fn default() -> Self {
        Self::Hash
    }
}
