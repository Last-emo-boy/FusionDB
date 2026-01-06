use fusiondb::execution::Executor;
use fusiondb::server;
use fusiondb::storage::memory::MemoryStorage;
use fusiondb::Result;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    println!("FusionDB starting...");

    // 1. Initialize Storage
    // Use MemoryStorage with WAL
    let storage = Arc::new(MemoryStorage::new("fusion.wal")?);

    // 2. Initialize Executor
    let executor = Arc::new(Executor::new(storage.clone()));

    // 3. Start Server
    server::start_server(executor, storage, 8091).await;

    Ok(())
}
