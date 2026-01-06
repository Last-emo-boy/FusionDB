use std::sync::Arc;
use crate::execution::Executor;
use crate::storage::Storage;

pub mod http_server;
pub mod pg_server;
pub mod tcp_server;

pub async fn start_server(executor: Arc<Executor>, storage: Arc<dyn Storage>, port: u16) {
    let http_executor = executor.clone();
    let http_storage = storage.clone();
    
    // Start HTTP Server
    tokio::spawn(async move {
        http_server::start_http_server(http_executor, http_storage, port).await;
    });

    // Start Postgres Server
    let pg_port = port + 1; // e.g. 8092
    pg_server::start_pg_server(executor, storage, pg_port).await;
}
