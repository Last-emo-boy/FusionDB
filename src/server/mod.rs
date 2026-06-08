use crate::config::Config;
use crate::execution::Executor;
use crate::storage::Storage;
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

    let http_executor = executor.clone();
    let http_storage = storage.clone();
    let mut http_rx = shutdown_tx.subscribe();
    let http_port = config.server.http_port;
    let http_bind = config.server.bind.clone();
    let http_tls = tls_acceptor.clone();

    // Start HTTP Server
    #[allow(deprecated)]
    tokio::spawn(async move {
        tokio::select! {
            _ = http_server::start_http_server(http_executor, http_storage, &http_bind, http_port, http_tls) => {},
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

    tokio::spawn(async move {
        tokio::select! {
            _ = pg_server::start_pg_server(pg_executor, pg_storage, &pg_bind, pg_port, &pg_password, pg_tls) => {},
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

    println!(
        "  Distributed: isolated (OpenRaft module is available but not wired into start_server)"
    );

    shutdown_tx
}
