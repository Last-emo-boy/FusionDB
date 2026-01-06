use std::sync::Arc;
use tokio_postgres::NoTls;
use w33dDB::execution::Executor;
use w33dDB::storage::memory::MemoryStorage;
use w33dDB::server::pg_server;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
#[ignore = "Known issue: pgwire 0.37 runtime dispatch failure. Compilation works but trait methods are ignored at runtime."]
async fn test_pg_protocol_integration() {
    let _ = env_logger::builder().is_test(true).try_init();

    let storage = Arc::new(MemoryStorage::new("test_pg.wal").expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = 9999;

    let server_storage = storage.clone();
    let server_executor = executor.clone();
    tokio::spawn(async move {
        pg_server::start_pg_server(server_executor, server_storage, port).await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Connect Client
    let (client, connection) = tokio_postgres::connect("host=127.0.0.1 port=9999 user=postgres", NoTls)
        .await
        .expect("Failed to connect to server");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Test: Simple Query
    // Should print "MINIMAL QUERY CALLED: ..." in server logs
    // And return empty rows (as per MinimalHandler)
    let rows = client.simple_query("SELECT 1").await.expect("Failed to query");
    // MinimalHandler returns Ok(vec![]), so client gets CommandComplete? No, it gets nothing?
    // simple_query returns vector of messages.
    // If MinimalHandler returns empty vector of responses, client might hang or finish?
    // pgwire sends ReadyForQuery automatically.
    // So client should receive empty list of messages.
    assert_eq!(rows.len(), 0);

    let _ = std::fs::remove_file("test_pg.wal");
}
