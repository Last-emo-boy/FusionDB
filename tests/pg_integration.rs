use bytes::Bytes;
use fusiondb::auth::UserRecord;
use fusiondb::execution::Executor;
use fusiondb::server::pg_server;
use fusiondb::storage::memory::MemoryStorage;
use fusiondb::storage::Storage;
use futures::SinkExt;
use std::sync::Arc;
use tokio_postgres::types::Type;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_pg_protocol_simple_query() {
    let wal_path = format!("test_pg_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = 19999; // Use high port to avoid conflicts

    tokio::spawn(async move {
        pg_server::start_pg_server(executor, storage, "127.0.0.1", port, "fusiondb", None).await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=postgres password=fusiondb",
            port
        ),
        NoTls,
    )
    .await
    .expect("Failed to connect to server");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Test: CREATE TABLE via simple query
    client
        .simple_query("CREATE TABLE test_users (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .expect("Failed to create table");

    // Test: INSERT
    client
        .simple_query("INSERT INTO test_users VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .expect("Failed to insert");

    // Test: SELECT
    let results = client
        .simple_query("SELECT * FROM test_users")
        .await
        .expect("Failed to select");

    // simple_query returns SimpleQueryMessage variants
    // We expect at least a Row or CommandComplete
    assert!(
        !results.is_empty(),
        "Expected non-empty response from SELECT"
    );

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_extended_query() {
    let wal_path = format!("test_pg_ext_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = 19998;

    tokio::spawn(async move {
        pg_server::start_pg_server(executor, storage, "127.0.0.1", port, "fusiondb", None).await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=postgres password=fusiondb",
            port
        ),
        NoTls,
    )
    .await
    .expect("Failed to connect to server");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Setup table
    client
        .simple_query("CREATE TABLE ext_test (id INTEGER PRIMARY KEY, val TEXT)")
        .await
        .expect("Failed to create table");

    client
        .simple_query("INSERT INTO ext_test VALUES (1, 'hello'), (2, 'world')")
        .await
        .expect("Failed to insert");

    // Test: Extended query with parameters using execute()
    let rows = client
        .query("SELECT * FROM ext_test WHERE id = $1", &[&1i64])
        .await
        .expect("extended parameterized query should succeed");
    assert_eq!(rows.len(), 1, "Expected one row for parameterized query");
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[0].get::<_, String>("val"), "hello");

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_prepare_reports_columns_and_params() {
    let wal_path = format!("test_pg_prepare_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = 19993;

    tokio::spawn(async move {
        pg_server::start_pg_server(executor, storage, "127.0.0.1", port, "fusiondb", None).await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=postgres password=fusiondb",
            port
        ),
        NoTls,
    )
    .await
    .expect("Failed to connect to server");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
        .simple_query("CREATE TABLE meta_test (id INTEGER PRIMARY KEY, val TEXT, score FLOAT)")
        .await
        .expect("Failed to create table");
    client
        .simple_query("INSERT INTO meta_test VALUES (1, 'hello', 1.5), (2, 'world', 2.5)")
        .await
        .expect("Failed to insert");

    let statement = client
        .prepare("SELECT id, val, score FROM meta_test WHERE id = $1")
        .await
        .expect("prepare should return metadata");

    assert_eq!(statement.params(), &[Type::INT8]);
    assert_eq!(statement.columns().len(), 3);
    assert_eq!(statement.columns()[0].name(), "id");
    assert_eq!(statement.columns()[0].type_(), &Type::INT4);
    assert_eq!(statement.columns()[1].name(), "val");
    assert_eq!(statement.columns()[1].type_(), &Type::TEXT);
    assert_eq!(statement.columns()[2].name(), "score");
    assert_eq!(statement.columns()[2].type_(), &Type::FLOAT8);

    let typed_statement = client
        .prepare_typed("SELECT id, val FROM meta_test WHERE id = $1", &[Type::INT8])
        .await
        .expect("prepare_typed should preserve client parameter type");
    assert_eq!(typed_statement.params(), &[Type::INT8]);

    let rows = client
        .query(&typed_statement, &[&2i64])
        .await
        .expect("typed prepared query should execute");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 2);
    assert_eq!(rows[0].get::<_, String>("val"), "world");

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_copy_from_stdin_text_and_csv() {
    let wal_path = format!("test_pg_copy_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = 19992;

    tokio::spawn(async move {
        pg_server::start_pg_server(executor, storage, "127.0.0.1", port, "fusiondb", None).await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=postgres password=fusiondb",
            port
        ),
        NoTls,
    )
    .await
    .expect("Failed to connect to server");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
        .simple_query(
            "CREATE TABLE copy_text_test (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
        )
        .await
        .expect("Failed to create text copy table");

    let mut text_sink = std::pin::pin!(client
        .copy_in("COPY copy_text_test FROM STDIN")
        .await
        .expect("COPY text should enter copy-in mode"));
    text_sink
        .send(Bytes::from_static(b"1\tAlice\t30\n2\tBob\t\\N\n"))
        .await
        .expect("COPY text payload should send");
    let copied = text_sink.finish().await.expect("COPY text should finish");
    assert_eq!(copied, 2);

    client
        .simple_query("CREATE TABLE copy_csv_test (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .await
        .expect("Failed to create csv copy table");

    let mut csv_sink = std::pin::pin!(client
        .copy_in("COPY copy_csv_test (id, name, age) FROM STDIN WITH (FORMAT CSV, HEADER true, NULL 'NULL')")
        .await
        .expect("COPY CSV should enter copy-in mode"));
    csv_sink
        .send(Bytes::from_static(
            b"id,name,age\n10,Carol,41\n11,Dave,NULL\n",
        ))
        .await
        .expect("COPY CSV payload should send");
    let copied = csv_sink.finish().await.expect("COPY CSV should finish");
    assert_eq!(copied, 2);

    let rows = client
        .query(
            "SELECT id, name, age FROM copy_text_test WHERE id = $1",
            &[&1i64],
        )
        .await
        .expect("copied text row should be queryable");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
    assert_eq!(rows[0].get::<_, i32>("age"), 30);

    let rows = client
        .query(
            "SELECT id, name FROM copy_csv_test WHERE id = $1",
            &[&10i64],
        )
        .await
        .expect("copied csv row should be queryable");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 10);
    assert_eq!(rows[0].get::<_, String>("name"), "Carol");

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_transaction_commit() {
    let wal_path = format!("test_pg_txn_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = 19997;

    tokio::spawn(async move {
        pg_server::start_pg_server(executor, storage, "127.0.0.1", port, "fusiondb", None).await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=postgres password=fusiondb",
            port
        ),
        NoTls,
    )
    .await
    .expect("Failed to connect");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
        .simple_query("CREATE TABLE txn_test (id INTEGER PRIMARY KEY, val TEXT)")
        .await
        .expect("CREATE TABLE failed");

    // BEGIN + INSERT + COMMIT should persist data
    client.simple_query("BEGIN").await.expect("BEGIN failed");
    client
        .simple_query("INSERT INTO txn_test VALUES (1, 'committed')")
        .await
        .expect("INSERT failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    let results = client
        .simple_query("SELECT * FROM txn_test WHERE id = 1")
        .await
        .expect("SELECT failed");
    assert!(!results.is_empty(), "Committed data should be visible");
    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_rbac_denies_unregistered_non_legacy_user() {
    let wal_path = format!("test_pg_rbac_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = 19995;

    tokio::spawn(async move {
        pg_server::start_pg_server(executor, storage, "127.0.0.1", port, "fusiondb", None).await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    let connect_result = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=alice password=fusiondb", port),
        NoTls,
    )
    .await;
    assert!(
        connect_result.is_err(),
        "unregistered non-legacy user should be rejected during authentication"
    );

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_rbac_allows_registered_user_permissions() {
    let wal_path = format!("test_pg_rbac_allow_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));

    {
        let mut txn = storage.begin_transaction().await.expect("begin txn");
        let mut alice = UserRecord::new("fusiondb", false);
        alice.grant("allowed_test", "SELECT");
        alice.grant("allowed_test", "INSERT");
        fusiondb::auth::save_user(&mut *txn, "alice", &alice)
            .await
            .expect("save user");
        txn.commit().await.expect("commit user txn");
    }

    let executor = Arc::new(Executor::new(storage.clone()));
    let port = 19994;

    tokio::spawn(async move {
        pg_server::start_pg_server(executor, storage, "127.0.0.1", port, "fusiondb", None).await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    let (admin_client, admin_connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=postgres password=fusiondb",
            port
        ),
        NoTls,
    )
    .await
    .expect("Failed to connect as admin");
    tokio::spawn(async move {
        if let Err(e) = admin_connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    admin_client
        .simple_query("CREATE TABLE allowed_test (id INTEGER PRIMARY KEY, val TEXT)")
        .await
        .expect("admin create table failed");

    let (alice_client, alice_connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=alice password=fusiondb", port),
        NoTls,
    )
    .await
    .expect("Failed to connect as alice");
    tokio::spawn(async move {
        if let Err(e) = alice_connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    alice_client
        .simple_query("INSERT INTO allowed_test VALUES (1, 'ok')")
        .await
        .expect("alice insert should succeed");
    let rows = alice_client
        .simple_query("SELECT * FROM allowed_test")
        .await
        .expect("alice select should succeed");
    assert!(!rows.is_empty(), "authorized user should read rows");

    let denied = alice_client
        .simple_query("CREATE TABLE forbidden_test (id INTEGER PRIMARY KEY)")
        .await;
    assert!(
        denied.is_err(),
        "user without ALL permission should be denied"
    );

    let _ = std::fs::remove_file(&wal_path);
}
#[tokio::test]
async fn test_pg_protocol_transaction_rollback() {
    let wal_path = format!("test_pg_rb_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = 19996;

    tokio::spawn(async move {
        pg_server::start_pg_server(executor, storage, "127.0.0.1", port, "fusiondb", None).await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=postgres password=fusiondb",
            port
        ),
        NoTls,
    )
    .await
    .expect("Failed to connect");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
        .simple_query("CREATE TABLE rb_test (id INTEGER PRIMARY KEY, val TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("INSERT INTO rb_test VALUES (1, 'original')")
        .await
        .expect("INSERT failed");

    // BEGIN + INSERT + ROLLBACK should discard new data
    client.simple_query("BEGIN").await.expect("BEGIN failed");
    client
        .simple_query("INSERT INTO rb_test VALUES (2, 'rolled_back')")
        .await
        .expect("INSERT in txn failed");
    client
        .simple_query("ROLLBACK")
        .await
        .expect("ROLLBACK failed");

    let results = client
        .simple_query("SELECT * FROM rb_test")
        .await
        .expect("SELECT failed");
    let row_count = results
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(row_count, 1, "Rolled back row should not be visible");
    let _ = std::fs::remove_file(&wal_path);
}
