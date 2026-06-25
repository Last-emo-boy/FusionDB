use fusiondb::config::Config;
use fusiondb::execution::Executor;
use fusiondb::server;
use fusiondb::storage::{FusionStorage, Storage};
use fusiondb::Result;
use std::sync::Arc;

const CONFIG_PATH: &str = "fusiondb.toml";

#[tokio::main]
async fn main() -> Result<()> {
    // Handle --init flag: write default config and exit
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--init") {
        Config::write_default(CONFIG_PATH).expect("Failed to write config");
        println!("Default config written to {}", CONFIG_PATH);
        return Ok(());
    }

    // 1. Load Config
    let config = Config::load(CONFIG_PATH);
    println!("FusionDB v{} starting...", env!("CARGO_PKG_VERSION"));
    println!(
        "  HTTP:    {}:{}",
        config.server.bind, config.server.http_port
    );
    println!(
        "  PgWire:  {}:{}",
        config.server.bind, config.server.pg_port
    );
    println!("  MaxConn: {}", config.server.max_connections);
    if config.server.redis_enabled {
        println!(
            "  Redis:   {}:{}",
            config.server.bind, config.server.redis_port
        );
    } else {
        println!("  Redis:   disabled");
    }
    if config.distributed.enabled {
        println!(
            "  Raft:    node {} advertised at {}",
            config.distributed.node_id,
            config.distributed.effective_advertise_addr(&config.server)
        );
        if config.distributed.sharding.enabled {
            println!(
                "  Shards:  {:?} strategy, {} configured shards",
                config.distributed.sharding.strategy, config.distributed.sharding.shard_count
            );
        } else {
            println!("  Shards:  disabled");
        }
    } else {
        println!("  Raft:    disabled");
    }
    println!("  Data:    {}", config.storage.data_dir);

    // 2. Apply config to monitor
    fusiondb::monitor::SLOW_QUERY_LOG.set_threshold_ms(config.storage.slow_query_threshold_ms);

    // 3. Ensure data directory exists
    std::fs::create_dir_all(&config.storage.data_dir).ok();
    std::fs::create_dir_all(config.storage.sstable_path()).ok();

    // 3. Initialize Storage
    let wal_path = config.storage.wal_path();
    let fusion =
        Arc::new(FusionStorage::with_config(wal_path.to_str().unwrap(), &config.storage).await?);
    let storage: Arc<dyn Storage> = fusion.clone();

    // 4. Initialize Executor
    let executor = Arc::new(Executor::with_config(storage.clone(), &config.storage));

    // 5. Start Servers (returns shutdown handle)
    let shutdown_tx = server::start_server(executor, storage, &config).await;

    // 6. Wait for Ctrl+C
    println!("Press Ctrl+C to shut down...");
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");

    // 7. Graceful Shutdown
    println!("\nReceived shutdown signal.");
    let _ = shutdown_tx.send(());
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    fusion.shutdown().await;

    Ok(())
}
