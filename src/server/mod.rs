use crate::config::Config;
use crate::distributed::{
    self, network::FusionNetworkFactory, sharding::ShardRouter, store::FusionRaftStore, FusionRaft,
};
use crate::execution::Executor;
use crate::storage::Storage;
use openraft::BasicNode;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::broadcast;

pub mod http_server;
pub mod pg_server;
pub mod redis_server;
pub(crate) mod security;
pub mod tcp_server;
pub mod tls;

/// Start all servers and return a shutdown sender.
/// Call `tx.send(())` to initiate graceful shutdown.
pub async fn start_server(
    executor: Arc<Executor>,
    storage: Arc<dyn Storage>,
    config: &Config,
) -> crate::Result<broadcast::Sender<()>> {
    config.validate_security()?;
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    let tls_config = if config.tls.enabled {
        let tls_config = tls::build_tls_config(&config.tls)?;
        println!(
            "  TLS:     enabled (cert: {}, key: {})",
            config.tls.cert_path, config.tls.key_path
        );
        Some(tls_config)
    } else {
        None
    };
    let peer_scheme = if config.tls.enabled { "https" } else { "http" };

    let raft = if config.distributed.enabled {
        let raft = start_raft_node(executor.clone(), storage.clone(), config)
            .await
            .map_err(|e| {
                crate::common::FusionError::Execution(format!(
                    "failed to start configured distributed Raft node: {}",
                    e
                ))
            })?;
        println!(
            "  Distributed: raft node {} enabled ({})",
            config.distributed.node_id,
            config.distributed.effective_advertise_addr(&config.server)
        );
        Some(raft)
    } else {
        println!("  Distributed: isolated (set [distributed].enabled = true to enable OpenRaft)");
        None
    };
    let distributed_mode = raft
        .as_ref()
        .map(|_| format!("raft(node_id={})", config.distributed.node_id))
        .unwrap_or_else(|| "isolated".to_string());
    let shard_router = if config.distributed.enabled {
        ShardRouter::from_config(config)
    } else {
        None
    };
    if let Some(router) = &shard_router {
        println!(
            "  Sharding: {} strategy, {} shards",
            router.strategy_name(),
            router.shard_count()
        );
    }

    let http_executor = executor.clone();
    let http_storage = storage.clone();
    let mut http_rx = shutdown_tx.subscribe();
    let http_port = config.server.http_port;
    let http_bind = config.server.bind.clone();
    let http_tls = tls_config.clone();
    let http_raft = raft.clone();
    let http_distributed_mode = distributed_mode.clone();
    let http_shard_router = shard_router.clone();
    let http_security = http_server::HttpServerSecurity {
        postgres_password: config.auth.password.clone(),
        http_legacy_unsafe: config.auth.http_legacy_unsafe,
        forwarding_secret: config.distributed.forwarding_secret.clone(),
        peer_scheme: peer_scheme.to_string(),
    };

    // Start HTTP Server
    #[allow(deprecated)]
    tokio::spawn(async move {
        tokio::select! {
            result = http_server::start_http_server_with_security(http_executor, http_storage, &http_bind, http_port, http_tls, http_raft, http_distributed_mode, http_shard_router, http_security) => {
                if let Err(e) = result {
                    eprintln!("FusionDB HTTP server stopped: {}", e);
                }
            },
            _ = http_rx.recv() => {
                println!("[shutdown] HTTP server stopping...");
            },
        }
    });

    // Start Postgres Server
    let mut pg_rx = shutdown_tx.subscribe();
    let pg_executor = executor.clone();
    let pg_storage = storage.clone();
    let pg_port = config.server.pg_port;
    let pg_bind = config.server.bind.clone();
    let pg_password = config.auth.password.clone();
    let pg_tls = tls_config.map(tokio_rustls::TlsAcceptor::from);
    let pg_max_connections = config.server.max_connections;
    let pg_forwarding_secret = config.distributed.forwarding_secret.clone();
    let pg_peer_scheme = peer_scheme.to_string();
    let pg_require_tls = config.tls.enabled;

    tokio::spawn(async move {
        tokio::select! {
            _ = pg_server::start_pg_server_with_connection_limit_and_security(pg_executor, pg_storage, &pg_bind, pg_port, &pg_password, pg_tls, pg_max_connections, &pg_forwarding_secret, &pg_peer_scheme, pg_require_tls) => {},
            _ = pg_rx.recv() => {
                println!("[shutdown] Postgres server stopping...");
            },
        }
    });

    if config.server.redis_enabled {
        let mut redis_rx = shutdown_tx.subscribe();
        let redis_storage = storage.clone();
        let redis_port = config.server.redis_port;
        let redis_bind = config.server.bind.clone();

        tokio::spawn(async move {
            tokio::select! {
                _ = redis_server::start_redis_server(redis_storage, &redis_bind, redis_port) => {},
                _ = redis_rx.recv() => {
                    println!("[shutdown] Redis-compatible server stopping...");
                },
            }
        });
    }

    Ok(shutdown_tx)
}

async fn start_raft_node(
    executor: Arc<Executor>,
    storage: Arc<dyn Storage>,
    config: &Config,
) -> Result<FusionRaft, Box<dyn std::error::Error>> {
    let mut raft_config = openraft::Config::default();
    raft_config.cluster_name = config.distributed.cluster_name.clone();
    let raft_config = raft_config.validate()?;

    let raft_store = FusionRaftStore::new(executor, storage);
    let peer_scheme = if config.tls.enabled { "https" } else { "http" };
    let network =
        FusionNetworkFactory::with_security(&config.distributed.forwarding_secret, peer_scheme);
    let raft =
        distributed::new_raft_node(config.distributed.node_id, raft_config, raft_store, network)
            .await?;

    if config.distributed.bootstrap {
        let members = initial_raft_members(config);
        match raft.initialize(members).await {
            Ok(()) => {}
            Err(openraft::error::RaftError::APIError(
                openraft::error::InitializeError::NotAllowed(_),
            )) => {
                println!("  Distributed: existing Raft state detected; bootstrap skipped");
            }
            Err(e) => return Err(Box::new(e)),
        }
    }

    Ok(raft)
}

fn initial_raft_members(config: &Config) -> BTreeMap<u64, BasicNode> {
    let mut members = BTreeMap::new();
    if config.distributed.initial_members.is_empty() {
        members.insert(
            config.distributed.node_id,
            BasicNode {
                addr: config.distributed.effective_advertise_addr(&config.server),
            },
        );
    } else {
        for peer in &config.distributed.initial_members {
            members.insert(
                peer.node_id,
                BasicNode {
                    addr: peer.addr.clone(),
                },
            );
        }
    }
    members
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DistributedPeerConfig, ServerConfig};

    #[test]
    fn initial_raft_members_defaults_to_local_advertise_addr() {
        let mut config = Config::default();
        config.server = ServerConfig {
            bind: "0.0.0.0".to_string(),
            http_port: 19091,
            ..Default::default()
        };
        config.distributed.enabled = true;
        config.distributed.node_id = 7;

        let members = initial_raft_members(&config);

        assert_eq!(members.len(), 1);
        assert_eq!(members.get(&7).unwrap().addr, "0.0.0.0:19091");
    }

    #[test]
    fn initial_raft_members_uses_configured_members() {
        let mut config = Config::default();
        config.distributed.initial_members = vec![
            DistributedPeerConfig {
                node_id: 1,
                addr: "127.0.0.1:8091".to_string(),
            },
            DistributedPeerConfig {
                node_id: 2,
                addr: "127.0.0.1:8093".to_string(),
            },
        ];

        let members = initial_raft_members(&config);

        assert_eq!(members.len(), 2);
        assert_eq!(members.get(&1).unwrap().addr, "127.0.0.1:8091");
        assert_eq!(members.get(&2).unwrap().addr, "127.0.0.1:8093");
    }
}
