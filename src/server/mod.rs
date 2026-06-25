use crate::config::Config;
use crate::distributed::{self, network::FusionNetworkFactory, store::FusionRaftStore, FusionRaft};
use crate::execution::Executor;
use crate::storage::Storage;
use openraft::BasicNode;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::broadcast;

pub mod http_server;
pub mod pg_server;
pub mod redis_server;
pub mod tcp_server;
pub mod tls;

/// Start all servers and return a shutdown sender.
/// Call `tx.send(())` to initiate graceful shutdown.
pub async fn start_server(
    executor: Arc<Executor>,
    storage: Arc<dyn Storage>,
    config: &Config,
) -> broadcast::Sender<()> {
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // Build TLS acceptor if enabled
    let tls_acceptor = if config.tls.enabled {
        match tls::build_tls_acceptor(&config.tls) {
            Ok(acceptor) => {
                println!(
                    "  TLS:     enabled (cert: {}, key: {})",
                    config.tls.cert_path, config.tls.key_path
                );
                Some(acceptor)
            }
            Err(e) => {
                eprintln!(
                    "Warning: TLS config error: {}. Falling back to plaintext.",
                    e
                );
                None
            }
        }
    } else {
        None
    };

    let raft = if config.distributed.enabled {
        match start_raft_node(executor.clone(), storage.clone(), config).await {
            Ok(raft) => {
                println!(
                    "  Distributed: raft node {} enabled ({})",
                    config.distributed.node_id,
                    config.distributed.effective_advertise_addr(&config.server)
                );
                Some(raft)
            }
            Err(e) => {
                eprintln!("Warning: failed to start distributed Raft node: {}", e);
                println!("  Distributed: disabled (Raft startup failed)");
                None
            }
        }
    } else {
        println!("  Distributed: isolated (set [distributed].enabled = true to enable OpenRaft)");
        None
    };
    let distributed_mode = raft
        .as_ref()
        .map(|_| format!("raft(node_id={})", config.distributed.node_id))
        .unwrap_or_else(|| "isolated".to_string());

    let http_executor = executor.clone();
    let http_storage = storage.clone();
    let mut http_rx = shutdown_tx.subscribe();
    let http_port = config.server.http_port;
    let http_bind = config.server.bind.clone();
    let http_tls = tls_acceptor.clone();
    let http_raft = raft.clone();
    let http_distributed_mode = distributed_mode.clone();

    // Start HTTP Server
    #[allow(deprecated)]
    tokio::spawn(async move {
        tokio::select! {
            _ = http_server::start_http_server(http_executor, http_storage, &http_bind, http_port, http_tls, http_raft, http_distributed_mode) => {},
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
    let pg_tls = tls_acceptor;
    let pg_max_connections = config.server.max_connections;

    tokio::spawn(async move {
        tokio::select! {
            _ = pg_server::start_pg_server_with_connection_limit(pg_executor, pg_storage, &pg_bind, pg_port, &pg_password, pg_tls, pg_max_connections) => {},
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

    shutdown_tx
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
    let network = FusionNetworkFactory::new();
    let raft =
        distributed::new_raft_node(config.distributed.node_id, raft_config, raft_store, network)
            .await?;

    if config.distributed.bootstrap {
        let members = initial_raft_members(config);
        if let Err(e) = raft.initialize(members).await {
            eprintln!("Warning: Raft bootstrap skipped or failed: {}", e);
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
