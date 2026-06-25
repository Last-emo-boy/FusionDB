use bytes::Bytes;
use chrono::{NaiveDate, NaiveDateTime};
use fusiondb::auth::UserRecord;
use fusiondb::config::{
    Config, DistributedPeerConfig, ShardingConfig, ShardingStrategy, StorageConfig,
};
use fusiondb::distributed::sharding::ShardRouter;
use fusiondb::execution::Executor;
use fusiondb::server::pg_server;
use fusiondb::storage::memory::MemoryStorage;
use fusiondb::storage::FusionStorage;
use fusiondb::storage::Storage;
use futures::SinkExt;
use std::net::TcpListener;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use tokio_postgres::types::Type;
use tokio_postgres::NoTls;

static NEXT_PG_TEST_PORT: AtomicU16 = AtomicU16::new(20000);

fn next_pg_test_port() -> u16 {
    for _ in 0..10_000 {
        let port = NEXT_PG_TEST_PORT.fetch_add(1, Ordering::Relaxed);
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!("failed to allocate a free PgWire test port");
}

fn unique_pg_storage_dir(test_name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("fusiondb_{}_{}", test_name, uuid::Uuid::new_v4()))
}

fn cleanup_storage_dir(path: &std::path::Path) {
    let _ = std::fs::remove_dir_all(path);
}

fn sharded_pg_test_config(shard_count: u64) -> Config {
    let mut config = Config::default();
    config.distributed.enabled = true;
    config.distributed.node_id = 1;
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
    config.distributed.sharding = ShardingConfig {
        enabled: true,
        strategy: ShardingStrategy::Hash,
        shard_count,
        range_boundaries: Vec::new(),
    };
    config
}

fn integer_primary_key_for_owner(
    router: &ShardRouter,
    table_name: &str,
    owner_node_id: u64,
) -> i32 {
    for value in 1_i32..10_000 {
        let row_id = fusiondb::common::encoding::encode_i64_comparable(value as i64);
        if router.route_key(table_name, &row_id).owner_node_id == owner_node_id {
            return value;
        }
    }
    panic!("no integer key routed to owner node {}", owner_node_id);
}

fn assert_pg_shard_route_conflict(error: &tokio_postgres::Error, table_name: &str) {
    assert_pg_shard_route_conflict_with_operation(error, table_name, None);
}

fn assert_pg_shard_route_conflict_with_operation(
    error: &tokio_postgres::Error,
    table_name: &str,
    operation: Option<&str>,
) {
    let db_error = error
        .as_db_error()
        .unwrap_or_else(|| panic!("database error, got: {:?}", error));
    assert_eq!(db_error.code().code(), "0A000");
    assert!(db_error.message().contains("Shard route conflict"));
    assert!(db_error.message().contains(table_name));
    if let Some(operation) = operation {
        assert!(db_error.message().contains(operation));
    }
    assert!(db_error.message().contains("owned by node 2"));
    assert!(db_error.message().contains("local node 1"));
}

#[tokio::test]
async fn test_pg_protocol_simple_query() {
    let wal_path = format!("test_pg_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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
    let port = next_pg_test_port();

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
        .query("SELECT * FROM ext_test WHERE id = $1", &[&1i32])
        .await
        .expect("extended parameterized query should succeed");
    assert_eq!(rows.len(), 1, "Expected one row for parameterized query");
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[0].get::<_, String>("val"), "hello");

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_extended_params_inside_derived_union() {
    let wal_path = format!("test_pg_derived_params_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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
        .simple_query("CREATE TABLE derived_param_test (id TEXT PRIMARY KEY, val TEXT)")
        .await
        .expect("Failed to create table");
    client
        .simple_query(
            "INSERT INTO derived_param_test VALUES ('1', 'alpha'), ('2', 'skip'), ('3', 'gamma')",
        )
        .await
        .expect("Failed to insert");

    let rows = client
        .query(
            "SELECT id, val FROM (
                SELECT id, val FROM derived_param_test WHERE id = $1
                UNION ALL
                SELECT id, val FROM derived_param_test WHERE id = $2
             ) tmp
             WHERE val <> $3
             ORDER BY id",
            &[&"1", &"3", &"skip"],
        )
        .await
        .expect("derived UNION should receive extended query params");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("id"), "1");
    assert_eq!(rows[0].get::<_, String>("val"), "alpha");
    assert_eq!(rows[1].get::<_, String>("id"), "3");
    assert_eq!(rows[1].get::<_, String>("val"), "gamma");

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_prepare_reports_columns_and_params() {
    let wal_path = format!("test_pg_prepare_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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

    assert_eq!(statement.params(), &[Type::INT4]);
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
async fn test_pg_protocol_begin_read_write_transaction_status() {
    let wal_path = format!("test_pg_begin_read_write_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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
        .simple_query("CREATE TABLE begin_rw_test (id INTEGER PRIMARY KEY, val TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("BEGIN READ WRITE")
        .await
        .expect("BEGIN READ WRITE failed");
    client
        .simple_query("INSERT INTO begin_rw_test VALUES (1, 'ok')")
        .await
        .expect("INSERT in BEGIN READ WRITE failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    let rows = client
        .query("SELECT val FROM begin_rw_test WHERE id = $1", &[&1i32])
        .await
        .expect("SELECT after COMMIT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("val"), "ok");

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_copy_from_stdin_preserves_transaction_status() {
    let wal_path = format!("test_pg_copy_tx_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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
        .simple_query("CREATE TABLE copy_tx_test (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("BEGIN READ WRITE")
        .await
        .expect("BEGIN READ WRITE failed");

    let mut sink = std::pin::pin!(client
        .copy_in(
            "COPY \"copy_tx_test\" (\"id\", \"name\") FROM STDIN WITH (FORMAT CSV, HEADER true)"
        )
        .await
        .expect("transaction COPY should enter copy-in mode"));
    sink.send(Bytes::from_static(b"id,name\n1,Alice\n"))
        .await
        .expect("transaction COPY payload should send");
    let copied = sink
        .finish()
        .await
        .expect("transaction COPY should finish without idle status");
    assert_eq!(copied, 1);

    client
        .simple_query("INSERT INTO copy_tx_test VALUES (2, 'Bob')")
        .await
        .expect("connection should still be in the explicit transaction");
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    let rows = client
        .query("SELECT id, name FROM copy_tx_test ORDER BY id", &[])
        .await
        .expect("copied rows should be visible after commit");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
    assert_eq!(rows[1].get::<_, i32>("id"), 2);
    assert_eq!(rows[1].get::<_, String>("name"), "Bob");

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_repeated_copy_transactions_preserve_status() {
    let wal_path = format!("test_pg_copy_tx_repeat_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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
        .simple_query("CREATE TABLE copy_tx_repeat (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .expect("CREATE TABLE failed");

    for (id, name) in [(1, "Alice"), (2, "Bob"), (3, "Carol")] {
        client
            .simple_query("BEGIN READ WRITE")
            .await
            .expect("BEGIN READ WRITE failed");
        let payload = format!("id,name\n{},{}\n", id, name);
        let mut sink = std::pin::pin!(client
            .copy_in(
                "COPY \"copy_tx_repeat\" (\"id\", \"name\") FROM STDIN WITH (FORMAT CSV, HEADER true)"
            )
            .await
            .expect("repeated transaction COPY should enter copy-in mode"));
        sink.send(Bytes::from(payload))
            .await
            .expect("repeated transaction COPY payload should send");
        let copied = sink
            .finish()
            .await
            .expect("repeated transaction COPY should finish without idle status");
        assert_eq!(copied, 1);
        client.simple_query("COMMIT").await.expect("COMMIT failed");
    }

    let rows = client
        .query("SELECT COUNT(*) FROM copy_tx_repeat", &[])
        .await
        .expect("copied rows should be visible after repeated commits");
    assert_eq!(rows[0].get::<_, i64>(0), 3);

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_unqualified_catalog_probes_for_tsbs_loader() {
    let wal_path = format!("test_pg_tsbs_catalog_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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

    let rows = client
        .query(
            "SELECT 1 from pg_database WHERE datname = $1",
            &[&"fusiondb"],
        )
        .await
        .expect("unqualified pg_database probe should execute");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 1);

    client
        .simple_query("CREATE TABLE tsbs_catalog_probe (id INTEGER PRIMARY KEY)")
        .await
        .expect("Failed to create catalog probe table");
    let rows = client
        .query(
            "SELECT * FROM pg_tables WHERE tablename = 'tsbs_catalog_probe'",
            &[],
        )
        .await
        .expect("unqualified pg_tables probe should execute");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("tablename"), "tsbs_catalog_probe");

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_jdbc_get_tables_pg_catalog_join_variant() {
    let wal_path = format!("test_pg_jdbc_tables_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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
        .simple_query("CREATE TABLE jdbc_tables_probe (id INTEGER PRIMARY KEY)")
        .await
        .expect("Failed to create table");

    let rows = client
        .query(
            "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME, \
             CASE c.relkind WHEN 'r' THEN 'TABLE' ELSE NULL END AS TABLE_TYPE, \
             d.description AS REMARKS, '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME, \
             '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION \
             FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c \
             LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0 \
             and d.classoid = 'pg_class'::regclass) \
             WHERE c.relnamespace = n.oid AND n.nspname LIKE 'public' \
             ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME",
            &[],
        )
        .await
        .expect("JDBC getTables pg_catalog join variant should be intercepted");

    assert!(
        rows.iter()
            .any(|row| row.get::<_, String>("TABLE_NAME") == "jdbc_tables_probe"),
        "metadata rows should include the created table"
    );

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_jdbc_get_columns_derived_pg_catalog_variant() {
    let wal_path = format!("test_pg_jdbc_columns_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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
        .simple_query("CREATE TABLE jdbc_columns_probe (id INTEGER PRIMARY KEY, amount DECIMAL(6, 2), note TEXT)")
        .await
        .expect("Failed to create table");

    let rows = client
        .query(
            "SELECT * FROM (SELECT n.nspname,c.relname,a.attname,a.atttypid,\
             a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,\
             a.atttypmod,a.attlen,t.typtypmod,\
             row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum,\
             nullif(a.attidentity, '') as attidentity,nullif(a.attgenerated, '') as attgenerated,\
             pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,dsc.description,\
             t.typbasetype,t.typtype \
             FROM pg_catalog.pg_namespace n \
             JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid) \
             JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid) \
             JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) \
             LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum) \
             LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid) \
             WHERE c.relkind in ('r','p','v','f','m') and a.attnum > 0 AND NOT a.attisdropped \
             AND n.nspname LIKE 'public' AND c.relname LIKE 'jdbc_columns_probe') c \
             WHERE true ORDER BY nspname,c.relname,attnum",
            &[],
        )
        .await
        .expect("JDBC getColumns derived pg_catalog variant should be intercepted");

    let names: Vec<String> = rows.iter().map(|row| row.get("attname")).collect();
    assert_eq!(names, vec!["id", "amount", "note"]);

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_jdbc_get_index_info_derived_pg_catalog_variant() {
    let wal_path = format!("test_pg_jdbc_indexes_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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
            "CREATE TABLE customer (
                c_w_id INTEGER,
                c_d_id INTEGER,
                c_id INTEGER,
                c_last TEXT,
                c_first TEXT,
                PRIMARY KEY (c_w_id, c_d_id, c_id)
            )",
        )
        .await
        .expect("Failed to create customer table");
    client
        .simple_query(
            "CREATE INDEX idx_customer_name ON customer (c_w_id, c_d_id, c_last, c_first)",
        )
        .await
        .expect("Failed to create customer name index");

    let rows = client
        .query(
            "SELECT tmp.TABLE_CAT, tmp.TABLE_SCHEM, tmp.TABLE_NAME, tmp.NON_UNIQUE, \
             tmp.INDEX_QUALIFIER, tmp.INDEX_NAME, tmp.TYPE, tmp.ORDINAL_POSITION, \
             trim(both '\"' from pg_catalog.pg_get_indexdef(tmp.CI_OID, tmp.ORDINAL_POSITION, false)) AS COLUMN_NAME, \
             CASE tmp.AM_NAME WHEN 'btree' THEN CASE tmp.I_INDOPTION[tmp.ORDINAL_POSITION - 1] & 1::smallint \
             WHEN 1 THEN 'D' ELSE 'A' END ELSE NULL END AS ASC_OR_DESC, \
             tmp.CARDINALITY, tmp.PAGES, tmp.FILTER_CONDITION \
             FROM ( \
               SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, \
                      ct.relname AS TABLE_NAME, NOT i.indisunique AS NON_UNIQUE, \
                      NULL AS INDEX_QUALIFIER, ci.relname AS INDEX_NAME, \
                      CASE i.indisclustered WHEN true THEN 1 ELSE CASE am.amname WHEN 'hash' THEN 2 ELSE 3 END END AS TYPE, \
                      (information_schema._pg_expandarray(i.indkey)).n AS ORDINAL_POSITION, \
                      ci.reltuples AS CARDINALITY, ci.relpages AS PAGES, \
                      pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS FILTER_CONDITION, \
                      ci.oid AS CI_OID, i.indoption AS I_INDOPTION, am.amname AS AM_NAME \
               FROM pg_catalog.pg_class ct \
               JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) \
               JOIN pg_catalog.pg_index i ON (ct.oid = i.indrelid) \
               JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) \
               JOIN pg_catalog.pg_am am ON (ci.relam = am.oid) \
               WHERE true AND n.nspname = 'public' AND ct.relname = 'customer' \
             ) AS tmp ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION",
            &[],
        )
        .await
        .expect("JDBC getIndexInfo derived pg_catalog variant should be intercepted");

    let index_columns: Vec<(String, i64, String, bool)> = rows
        .iter()
        .map(|row| {
            (
                row.get("INDEX_NAME"),
                row.get("ORDINAL_POSITION"),
                row.get("COLUMN_NAME"),
                row.get("NON_UNIQUE"),
            )
        })
        .collect();

    assert!(
        index_columns.contains(&("customer_pkey".to_string(), 1, "c_w_id".to_string(), false)),
        "metadata rows should include composite primary key first column"
    );
    assert!(
        index_columns.contains(&("customer_pkey".to_string(), 3, "c_id".to_string(), false)),
        "metadata rows should include composite primary key final column"
    );
    assert!(
        index_columns.contains(&(
            "idx_customer_name".to_string(),
            1,
            "c_w_id".to_string(),
            true
        )),
        "metadata rows should include non-unique customer name index"
    );
    assert!(
        index_columns.contains(&(
            "idx_customer_name".to_string(),
            4,
            "c_first".to_string(),
            true
        )),
        "metadata rows should include customer name index final column"
    );

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_pg_settings_max_index_keys_for_jdbc_metadata() {
    let wal_path = format!("test_pg_settings_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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

    let rows = client
        .query(
            "SELECT setting FROM pg_catalog.pg_settings WHERE name='max_index_keys'",
            &[],
        )
        .await
        .expect("pg_settings max_index_keys should be available for JDBC metadata");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("setting"), "32");

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_show_all_returns_parameter_rows_for_benchbase_collector() {
    let wal_path = format!("test_pg_show_all_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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

    let rows = client
        .query("SHOW ALL", &[])
        .await
        .expect("SHOW ALL should return a result set for DB parameter collectors");

    assert!(rows.iter().any(|row| {
        row.get::<_, String>("name") == "server_version"
            && row.get::<_, String>("setting") == "15.0"
    }));
    assert!(rows
        .iter()
        .any(|row| row.get::<_, String>("name") == "max_index_keys"));

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_show_transaction_isolation_level_for_jdbc_pool_startup() {
    let wal_path = format!("test_pg_show_tx_isolation_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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

    let rows = client
        .query("SHOW TRANSACTION ISOLATION LEVEL", &[])
        .await
        .expect("JDBC/Hikari startup isolation probe should return a result set");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get::<_, String>("transaction_isolation"),
        "read committed"
    );

    let simple_rows = client
        .simple_query("SHOW TRANSACTION ISOLATION LEVEL")
        .await
        .expect("simple SHOW TRANSACTION ISOLATION LEVEL should return a row");
    let isolation = simple_rows.iter().find_map(|message| {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            row.get("transaction_isolation").map(str::to_string)
        } else {
            None
        }
    });
    assert_eq!(isolation.as_deref(), Some("read committed"));

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_pg_stat_views_for_benchbase_collector() {
    let wal_path = format!("test_pg_stat_views_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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
        .simple_query("CREATE TABLE pg_stat_probe (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .expect("Failed to create stats probe table");
    client
        .simple_query("CREATE INDEX idx_pg_stat_probe_name ON pg_stat_probe (name)")
        .await
        .expect("Failed to create stats probe index");

    for view in [
        "pg_stat_archiver",
        "pg_stat_bgwriter",
        "pg_stat_database",
        "pg_stat_database_conflicts",
        "pg_stat_user_tables",
        "pg_statio_user_tables",
        "pg_stat_user_indexes",
        "pg_statio_user_indexes",
    ] {
        client
            .query(&format!("SELECT * FROM {}", view), &[])
            .await
            .unwrap_or_else(|err| panic!("{} should be queryable: {}", view, err));
    }

    let rows = client
        .query(
            "SELECT relname, seq_scan, n_tup_ins FROM pg_stat_user_tables WHERE relname='pg_stat_probe'",
            &[],
        )
        .await
        .expect("pg_stat_user_tables should expose user table rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("relname"), "pg_stat_probe");
    assert_eq!(rows[0].get::<_, i64>("seq_scan"), 0);
    assert_eq!(rows[0].get::<_, i64>("n_tup_ins"), 0);

    let rows = client
        .query(
            "SELECT relname, indexrelname, idx_scan FROM pg_stat_user_indexes WHERE relname='pg_stat_probe'",
            &[],
        )
        .await
        .expect("pg_stat_user_indexes should expose user index rows");
    let index_names: Vec<String> = rows.iter().map(|row| row.get("indexrelname")).collect();
    assert!(index_names.contains(&"pg_stat_probe_pkey".to_string()));
    assert!(index_names.contains(&"idx_pg_stat_probe_name".to_string()));
    assert!(rows.iter().all(|row| {
        row.get::<_, String>("relname") == "pg_stat_probe" && row.get::<_, i64>("idx_scan") == 0
    }));

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_jdbc_imported_keys_pg_catalog_variant() {
    let wal_path = format!("test_pg_jdbc_imported_keys_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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
        .simple_query("CREATE TABLE district (d_w_id INTEGER, d_id INTEGER, name TEXT, PRIMARY KEY (d_w_id, d_id))")
        .await
        .expect("Failed to create parent table");
    client
        .simple_query(
            "CREATE TABLE customer (
                c_w_id INTEGER,
                c_d_id INTEGER,
                c_id INTEGER,
                CONSTRAINT fk_customer_district FOREIGN KEY (c_w_id, c_d_id) REFERENCES district(d_w_id, d_id),
                PRIMARY KEY (c_w_id, c_d_id, c_id)
            )",
        )
        .await
        .expect("Failed to create child table");

    let item_imported_keys = jdbc_imported_keys_query("item");
    let no_imported_rows = client
        .query(&item_imported_keys, &[])
        .await
        .expect("JDBC imported keys query should return empty metadata for unrelated table");
    assert!(no_imported_rows.is_empty());

    let customer_imported_keys = jdbc_imported_keys_query("customer");
    let rows = client
        .query(&customer_imported_keys, &[])
        .await
        .expect("JDBC imported keys pg_catalog variant should be intercepted");

    let key_columns: Vec<(String, String, i64, String)> = rows
        .iter()
        .map(|row| {
            (
                row.get("PKCOLUMN_NAME"),
                row.get("FKCOLUMN_NAME"),
                row.get("KEY_SEQ"),
                row.get("FK_NAME"),
            )
        })
        .collect();

    assert_eq!(
        key_columns,
        vec![
            (
                "d_w_id".to_string(),
                "c_w_id".to_string(),
                1,
                "fk_customer_district".to_string()
            ),
            (
                "d_id".to_string(),
                "c_d_id".to_string(),
                2,
                "fk_customer_district".to_string()
            ),
        ]
    );

    let _ = std::fs::remove_file(&wal_path);
}

fn jdbc_imported_keys_query(table_name: &str) -> String {
    format!(
        "SELECT NULL::text AS PKTABLE_CAT, pkn.nspname AS PKTABLE_SCHEM, \
         pkc.relname AS PKTABLE_NAME, pka.attname AS PKCOLUMN_NAME, \
         NULL::text AS FKTABLE_CAT, fkn.nspname AS FKTABLE_SCHEM, \
         fkc.relname AS FKTABLE_NAME, fka.attname AS FKCOLUMN_NAME, \
         pos.n AS KEY_SEQ, \
         CASE con.confupdtype WHEN 'c' THEN 0 WHEN 'n' THEN 2 WHEN 'd' THEN 4 WHEN 'r' THEN 1 WHEN 'p' THEN 1 WHEN 'a' THEN 3 ELSE NULL END AS UPDATE_RULE, \
         CASE con.confdeltype WHEN 'c' THEN 0 WHEN 'n' THEN 2 WHEN 'd' THEN 4 WHEN 'r' THEN 1 WHEN 'p' THEN 1 WHEN 'a' THEN 3 ELSE NULL END AS DELETE_RULE, \
         con.conname AS FK_NAME, pkic.relname AS PK_NAME, \
         CASE WHEN con.condeferrable AND con.condeferred THEN 5 WHEN con.condeferrable THEN 6 ELSE 7 END AS DEFERRABILITY \
         FROM pg_catalog.pg_namespace pkn, pg_catalog.pg_class pkc, pg_catalog.pg_attribute pka, \
         pg_catalog.pg_namespace fkn, pg_catalog.pg_class fkc, pg_catalog.pg_attribute fka, \
         pg_catalog.pg_constraint con, pg_catalog.generate_series(1, 32) pos(n), pg_catalog.pg_class pkic \
         WHERE pkn.oid = pkc.relnamespace AND pkc.oid = pka.attrelid AND pka.attnum = con.confkey[pos.n] AND con.confrelid = pkc.oid \
         AND fkn.oid = fkc.relnamespace AND fkc.oid = fka.attrelid AND fka.attnum = con.conkey[pos.n] AND con.conrelid = fkc.oid \
         AND con.contype = 'f' AND (pkic.relkind = 'i' OR pkic.relkind = 'I') AND pkic.oid = con.conindid \
         AND fkn.nspname = 'public' AND fkc.relname = '{}' \
         ORDER BY pkn.nspname,pkc.relname, con.conname,pos.n",
        table_name
    )
}

#[tokio::test]
async fn test_pg_protocol_reports_production_scalar_types() {
    let wal_path = format!("test_pg_scalar_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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
            "CREATE TABLE pg_scalar_test (
                id INTEGER PRIMARY KEY,
                d DATE,
                ts TIMESTAMP,
                amount NUMERIC,
                span INTERVAL
            )",
        )
        .await
        .expect("Failed to create scalar table");
    client
        .simple_query(
            "INSERT INTO pg_scalar_test VALUES
                (1, DATE '2024-02-01', TIMESTAMP '2024-02-01 12:30:00', CAST('12.50' AS NUMERIC), INTERVAL '1 hour')",
        )
        .await
        .expect("Failed to insert scalar row");

    let statement = client
        .prepare("SELECT d, ts, amount, span FROM pg_scalar_test WHERE id = $1")
        .await
        .expect("prepare should return scalar metadata");
    assert_eq!(statement.columns()[0].type_(), &Type::DATE);
    assert_eq!(statement.columns()[1].type_(), &Type::TIMESTAMP);
    assert_eq!(statement.columns()[2].type_(), &Type::NUMERIC);
    assert_eq!(statement.columns()[3].type_(), &Type::INTERVAL);

    let rows = client
        .query(&statement, &[&1i32])
        .await
        .expect("scalar typed query should execute");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get::<_, NaiveDate>("d"),
        NaiveDate::from_ymd_opt(2024, 2, 1).unwrap()
    );
    assert_eq!(
        rows[0].get::<_, NaiveDateTime>("ts"),
        NaiveDate::from_ymd_opt(2024, 2, 1)
            .unwrap()
            .and_hms_opt(12, 30, 0)
            .unwrap()
    );
    let simple_rows = client
        .simple_query("SELECT amount FROM pg_scalar_test WHERE id = 1")
        .await
        .expect("simple numeric query should execute");
    let amount = simple_rows.iter().find_map(|message| {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            row.get("amount").map(str::to_string)
        } else {
            None
        }
    });
    assert_eq!(amount.as_deref(), Some("12.5"));

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_reports_nested_array_type_and_text_literal() {
    let wal_path = format!("test_pg_array_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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

    let statement = client
        .prepare(
            "WITH paths(path) AS (SELECT ARRAY[[10, 11], [20, 21]]::bigint[][])
             SELECT path FROM paths",
        )
        .await
        .expect("prepare should return nested array metadata");
    assert_eq!(statement.columns()[0].type_(), &Type::INT8_ARRAY);

    let rows = client
        .simple_query(
            "WITH paths(path) AS (SELECT ARRAY[[10, 11], [20, 21]]::bigint[][])
             SELECT path FROM paths",
        )
        .await
        .expect("simple nested array query should execute");
    let path = rows.iter().find_map(|message| {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            row.get("path").map(str::to_string)
        } else {
            None
        }
    });
    assert_eq!(path.as_deref(), Some("{{10,11},{20,21}}"));

    let element_rows = client
        .query(
            "SELECT e.oid, n.nspname = ANY(current_schemas(true)), n.nspname, e.typname \
             FROM pg_catalog.pg_type t \
             JOIN pg_catalog.pg_type e ON t.typelem = e.oid \
             JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid \
             WHERE t.oid = $1",
            &[&1016i32],
        )
        .await
        .expect("JDBC array element type metadata query should execute");
    assert_eq!(element_rows.len(), 1);
    assert_eq!(element_rows[0].get::<_, String>("typname"), "int8");

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_reports_scalar_subquery_array_agg_type() {
    let wal_path = format!("test_pg_scalar_subquery_array_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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
        .simple_query("CREATE TABLE q1_person_pg (id BIGINT PRIMARY KEY, last_name TEXT)")
        .await
        .expect("person table should be created");
    client
        .simple_query("CREATE TABLE q1_email_pg (person_id BIGINT, email TEXT)")
        .await
        .expect("email table should be created");
    client
        .simple_query(
            "INSERT INTO q1_person_pg VALUES (1, 'Smith');
             INSERT INTO q1_email_pg VALUES (1, 'a@example.test'), (1, 'b@example.test')",
        )
        .await
        .expect("test data should be inserted");

    let statement = client
        .prepare(
            "SELECT
                 id,
                 (SELECT array_agg(email) FROM q1_email_pg WHERE person_id = id GROUP BY person_id) AS emails
             FROM q1_person_pg
             GROUP BY id, last_name",
        )
        .await
        .expect("prepare should infer scalar subquery array type");

    assert_eq!(statement.columns()[0].type_(), &Type::INT8);
    assert_eq!(statement.columns()[1].type_(), &Type::TEXT_ARRAY);

    let simple_rows = client
        .simple_query(
            "SELECT
                 id,
                 (SELECT array_agg(email) FROM q1_email_pg WHERE person_id = id GROUP BY person_id) AS emails
             FROM q1_person_pg
             GROUP BY id, last_name",
        )
        .await
        .expect("scalar subquery array query should execute");
    let emails = simple_rows.iter().find_map(|message| {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            row.get("emails").map(str::to_string)
        } else {
            None
        }
    });
    let emails = emails.expect("emails array text should be returned");
    assert!(emails.starts_with('{') && emails.ends_with('}'));
    assert!(emails.contains("\"a@example.test\""));
    assert!(emails.contains("\"b@example.test\""));

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_reports_nested_text_array_agg_text() {
    let wal_path = format!("test_pg_nested_array_agg_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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

    let sql = "WITH orgs(name, since_year, place) AS (
                   SELECT 'University A', 2001, 'City A'
                   UNION ALL
                   SELECT 'University B', 2002, 'City B'
               )
               SELECT array_agg(ARRAY[name, since_year::text, place]) AS university
               FROM orgs";

    let statement = client
        .prepare(sql)
        .await
        .expect("prepare should infer nested text array type");
    assert_eq!(statement.columns()[0].type_(), &Type::TEXT_ARRAY);

    let simple_rows = client
        .simple_query(sql)
        .await
        .expect("nested array_agg query should execute");
    let university = simple_rows.iter().find_map(|message| {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            row.get("university").map(str::to_string)
        } else {
            None
        }
    });
    assert_eq!(
        university.as_deref(),
        Some(r#"{{"University A","2001","City A"},{"University B","2002","City B"}}"#)
    );

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_copy_from_stdin_text_and_csv() {
    let wal_path = format!("test_pg_copy_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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

    client
        .simple_query("CREATE TABLE copy_quoted_test (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .expect("Failed to create quoted copy table");

    let mut quoted_sink = std::pin::pin!(client
        .copy_in("COPY \"copy_quoted_test\" (\"id\", \"name\") FROM STDIN WITH (FORMAT CSV, HEADER true)")
        .await
        .expect("quoted COPY CSV should enter copy-in mode"));
    quoted_sink
        .send(Bytes::from_static(b"id,name\n20,Eve\n21,Frank\n"))
        .await
        .expect("quoted COPY CSV payload should send");
    let copied = quoted_sink
        .finish()
        .await
        .expect("quoted COPY CSV should finish");
    assert_eq!(copied, 2);

    let rows = client
        .query(
            "SELECT id, name, age FROM copy_text_test WHERE id = $1",
            &[&1i32],
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
            &[&10i32],
        )
        .await
        .expect("copied csv row should be queryable");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 10);
    assert_eq!(rows[0].get::<_, String>("name"), "Carol");

    let rows = client
        .query(
            "SELECT id, name FROM copy_quoted_test WHERE id = $1",
            &[&20i32],
        )
        .await
        .expect("quoted copied row should be queryable");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 20);
    assert_eq!(rows[0].get::<_, String>("name"), "Eve");

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_copy_from_stdin_rejects_non_local_shard_owner_rows() {
    let wal_path = format!("test_pg_shard_owner_copy_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let config = sharded_pg_test_config(4);
    let shard_router = ShardRouter::from_config(&config).expect("shard router");
    let local_key =
        integer_primary_key_for_owner(&shard_router, "pg_route_copy", shard_router.local_node_id());
    let remote_key = integer_primary_key_for_owner(&shard_router, "pg_route_copy", 2);
    let txn_remote_key = integer_primary_key_for_owner(&shard_router, "pg_route_copy_txn", 2);
    let executor = Arc::new(Executor::with_config_and_shard_router(
        storage.clone(),
        &StorageConfig::default(),
        Some(shard_router),
    ));
    let port = next_pg_test_port();

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
        .simple_query("CREATE TABLE pg_route_copy (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .expect("CREATE TABLE failed");

    let mut local_sink = std::pin::pin!(client
        .copy_in("COPY pg_route_copy (id, name) FROM STDIN")
        .await
        .expect("local owner COPY should enter copy-in mode"));
    local_sink
        .send(Bytes::from(format!("{}\tlocal\n", local_key)))
        .await
        .expect("local owner COPY payload should send");
    assert_eq!(
        local_sink
            .finish()
            .await
            .expect("local owner COPY should finish"),
        1
    );

    let mut remote_sink = std::pin::pin!(client
        .copy_in("COPY pg_route_copy (id, name) FROM STDIN")
        .await
        .expect("remote owner COPY should enter copy-in mode"));
    remote_sink
        .send(Bytes::from(format!("{}\tremote\n", remote_key)))
        .await
        .expect("remote owner COPY payload should send");
    let remote_copy = remote_sink.finish().await;
    assert_pg_shard_route_conflict_with_operation(
        &remote_copy.expect_err("remote owner COPY should fail"),
        "pg_route_copy",
        Some("COPY"),
    );

    client.simple_query("BEGIN").await.expect("BEGIN failed");
    client
        .simple_query("CREATE TABLE pg_route_copy_txn (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .expect("CREATE TABLE failed");
    let mut remote_txn_sink = std::pin::pin!(client
        .copy_in("COPY pg_route_copy_txn (id, name) FROM STDIN")
        .await
        .expect("transaction-local remote owner COPY should enter copy-in mode"));
    remote_txn_sink
        .send(Bytes::from(format!("{}\tremote\n", txn_remote_key)))
        .await
        .expect("transaction-local remote owner COPY payload should send");
    let remote_txn_copy = remote_txn_sink.finish().await;
    assert_pg_shard_route_conflict_with_operation(
        &remote_txn_copy.expect_err("transaction-local remote owner COPY should fail"),
        "pg_route_copy_txn",
        Some("COPY"),
    );
    let _ = client.simple_query("ROLLBACK").await;

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_transaction_commit() {
    let wal_path = format!("test_pg_txn_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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
async fn test_pg_protocol_write_conflict_uses_serialization_failure_sqlstate() {
    let data_dir = unique_pg_storage_dir("pg_write_conflict");
    std::fs::create_dir_all(&data_dir).unwrap();
    let mut config = StorageConfig::default();
    config.data_dir = data_dir.to_string_lossy().to_string();
    let wal_path = config.wal_path();
    let fusion = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
        .await
        .expect("Failed to create FusionStorage");
    let storage: Arc<dyn Storage> = Arc::new(fusion);
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

    tokio::spawn(async move {
        pg_server::start_pg_server(executor, storage, "127.0.0.1", port, "fusiondb", None).await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    let conninfo = format!(
        "host=127.0.0.1 port={} user=postgres password=fusiondb",
        port
    );
    let (client_a, connection_a) = tokio_postgres::connect(&conninfo, NoTls)
        .await
        .expect("Failed to connect client A");
    tokio::spawn(async move {
        if let Err(e) = connection_a.await {
            eprintln!("connection error: {}", e);
        }
    });
    let (client_b, connection_b) = tokio_postgres::connect(&conninfo, NoTls)
        .await
        .expect("Failed to connect client B");
    tokio::spawn(async move {
        if let Err(e) = connection_b.await {
            eprintln!("connection error: {}", e);
        }
    });

    client_a
        .simple_query("CREATE TABLE conflict_test (id INTEGER PRIMARY KEY, val INTEGER)")
        .await
        .expect("CREATE TABLE failed");
    client_a
        .simple_query("INSERT INTO conflict_test VALUES (1, 10)")
        .await
        .expect("INSERT failed");

    client_a
        .simple_query("BEGIN")
        .await
        .expect("BEGIN A failed");
    client_b
        .simple_query("BEGIN")
        .await
        .expect("BEGIN B failed");
    client_a
        .simple_query("UPDATE conflict_test SET val = 11 WHERE id = 1")
        .await
        .expect("UPDATE A failed");
    client_b
        .simple_query("UPDATE conflict_test SET val = 12 WHERE id = 1")
        .await
        .expect("UPDATE B failed");
    client_a
        .simple_query("COMMIT")
        .await
        .expect("COMMIT A failed");

    let error = client_b
        .simple_query("COMMIT")
        .await
        .expect_err("conflicting COMMIT should return an error");
    assert_eq!(
        error.code().map(|code| code.code()),
        Some("40001"),
        "write conflict should be exposed as PostgreSQL serialization_failure"
    );

    cleanup_storage_dir(&data_dir);
}

#[tokio::test]
async fn test_pg_protocol_rbac_denies_unregistered_non_legacy_user() {
    let wal_path = format!("test_pg_rbac_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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
    let port = next_pg_test_port();

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
    let port = next_pg_test_port();

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

#[tokio::test]
async fn test_pg_protocol_extended_update_count_with_uppercase_unquoted_identifiers() {
    let wal_path = format!("test_pg_tpcc_update_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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
        .simple_query(
            "CREATE TABLE warehouse (w_id INTEGER PRIMARY KEY, w_ytd DECIMAL(12, 2) NOT NULL)",
        )
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("INSERT INTO warehouse VALUES (1, CAST('300000.00' AS DECIMAL))")
        .await
        .expect("INSERT failed");

    let updated = client
        .execute(
            "UPDATE warehouse SET W_YTD = W_YTD + $1 WHERE W_ID = $2",
            &[&"10.25", &1i32],
        )
        .await
        .expect("UPDATE should succeed");
    assert_eq!(updated, 1, "JDBC/BenchBase executeUpdate expects row count");

    let rows = client
        .query(
            "SELECT CAST(W_YTD AS TEXT) AS w_ytd_text FROM warehouse WHERE W_ID = $1",
            &[&1i32],
        )
        .await
        .expect("SELECT should succeed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("w_ytd_text"), "300010.25");

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_extended_tpcc_district_lookup_with_for_update() {
    let wal_path = format!("test_pg_tpcc_district_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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
        .simple_query(
            "CREATE TABLE warehouse (w_id INTEGER PRIMARY KEY, w_name VARCHAR(10) NOT NULL)",
        )
        .await
        .expect("CREATE warehouse failed");
    client
        .simple_query("INSERT INTO warehouse VALUES (1, 'w1')")
        .await
        .expect("INSERT warehouse failed");
    client
        .simple_query(
            "CREATE TABLE district (
                d_w_id INTEGER NOT NULL,
                d_id INTEGER NOT NULL,
                d_ytd DECIMAL(12, 2) NOT NULL,
                d_tax DECIMAL(4, 4) NOT NULL,
                d_next_o_id INTEGER NOT NULL,
                d_name VARCHAR(10) NOT NULL,
                FOREIGN KEY (d_w_id) REFERENCES warehouse (w_id) ON DELETE CASCADE,
                PRIMARY KEY (d_w_id, d_id)
            )",
        )
        .await
        .expect("CREATE district failed");
    client
        .simple_query(
            "INSERT INTO district VALUES (
                1,
                8,
                CAST('30000.00' AS DECIMAL),
                CAST('0.0527' AS DECIMAL),
                3001,
                'district8'
            )",
        )
        .await
        .expect("INSERT district failed");

    let rows = client
        .query(
            "SELECT D_NEXT_O_ID, CAST(D_TAX AS TEXT) AS d_tax_text
             FROM district
             WHERE D_W_ID = $1 AND D_ID = $2
             FOR UPDATE",
            &[&1i32, &8i32],
        )
        .await
        .expect("district lookup should succeed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("d_next_o_id"), 3001);
    assert_eq!(rows[0].get::<_, String>("d_tax_text"), "0.0527");

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_extended_tpcc_payment_district_update_count() {
    let wal_path = format!("test_pg_tpcc_payment_district_{}.wal", std::process::id());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let executor = Arc::new(Executor::new(storage.clone()));
    let port = next_pg_test_port();

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
        .simple_query(
            "CREATE TABLE warehouse (w_id INTEGER PRIMARY KEY, w_name VARCHAR(10) NOT NULL)",
        )
        .await
        .expect("CREATE warehouse failed");
    client
        .simple_query("INSERT INTO warehouse VALUES (1, 'w1')")
        .await
        .expect("INSERT warehouse failed");
    client
        .simple_query(
            "CREATE TABLE district (
                d_w_id INTEGER NOT NULL,
                d_id INTEGER NOT NULL,
                d_ytd DECIMAL(12, 2) NOT NULL,
                d_tax DECIMAL(4, 4) NOT NULL,
                d_next_o_id INTEGER NOT NULL,
                d_name VARCHAR(10) NOT NULL,
                FOREIGN KEY (d_w_id) REFERENCES warehouse (w_id) ON DELETE CASCADE,
                PRIMARY KEY (d_w_id, d_id)
            )",
        )
        .await
        .expect("CREATE district failed");
    client
        .simple_query(
            "INSERT INTO district VALUES (
                1,
                7,
                CAST('30000.00' AS DECIMAL),
                CAST('0.0579' AS DECIMAL),
                3001,
                'district7'
            )",
        )
        .await
        .expect("INSERT district failed");

    let updated = client
        .execute(
            "UPDATE district
                SET D_YTD = D_YTD + $1
              WHERE D_W_ID = $2
                AND D_ID = $3",
            &[&"2849.77001953125", &1i32, &7i32],
        )
        .await
        .expect("district payment update should succeed");
    assert_eq!(updated, 1, "BenchBase Payment expects one updated district");

    let rows = client
        .query(
            "SELECT CAST(D_YTD AS TEXT) AS d_ytd_text
             FROM district
             WHERE D_W_ID = $1 AND D_ID = $2",
            &[&1i32, &7i32],
        )
        .await
        .expect("updated district should be queryable");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("d_ytd_text"), "32849.77001953125");

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_simple_query_rejects_non_local_shard_owner_insert() {
    let wal_path = format!("test_pg_shard_owner_simple_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let config = sharded_pg_test_config(4);
    let shard_router = ShardRouter::from_config(&config).expect("shard router");
    let local_key = integer_primary_key_for_owner(
        &shard_router,
        "pg_route_simple",
        shard_router.local_node_id(),
    );
    let remote_key = integer_primary_key_for_owner(&shard_router, "pg_route_simple", 2);
    let batch_remote_key = integer_primary_key_for_owner(&shard_router, "pg_route_simple_batch", 2);
    let txn_remote_key = integer_primary_key_for_owner(&shard_router, "pg_route_simple_txn", 2);
    let executor = Arc::new(Executor::with_config_and_shard_router(
        storage.clone(),
        &StorageConfig::default(),
        Some(shard_router),
    ));
    let port = next_pg_test_port();

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
        .simple_query("CREATE TABLE pg_route_simple (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query(&format!(
            "INSERT INTO pg_route_simple VALUES ({}, 'local')",
            local_key
        ))
        .await
        .expect("local owner insert should succeed");

    let remote_insert = client
        .simple_query(&format!(
            "INSERT INTO pg_route_simple VALUES ({}, 'remote')",
            remote_key
        ))
        .await;
    assert_pg_shard_route_conflict(
        &remote_insert.expect_err("remote owner insert should fail"),
        "pg_route_simple",
    );

    let remote_batch_insert = client
        .simple_query(&format!(
            "CREATE TABLE pg_route_simple_batch (id INTEGER PRIMARY KEY, name TEXT); \
             INSERT INTO pg_route_simple_batch VALUES ({}, 'remote')",
            batch_remote_key
        ))
        .await;
    assert_pg_shard_route_conflict_with_operation(
        &remote_batch_insert.expect_err("remote owner batch insert should fail"),
        "pg_route_simple_batch",
        Some("INSERT"),
    );

    client
        .simple_query(&format!(
            "UPDATE pg_route_simple SET name = 'local-updated' WHERE id = {}",
            local_key
        ))
        .await
        .expect("local owner update should succeed");

    let remote_update = client
        .simple_query(&format!(
            "UPDATE pg_route_simple SET name = 'remote-updated' WHERE id = {}",
            remote_key
        ))
        .await;
    assert_pg_shard_route_conflict_with_operation(
        &remote_update.expect_err("remote owner update should fail"),
        "pg_route_simple",
        Some("UPDATE"),
    );

    let remote_delete = client
        .simple_query(&format!(
            "DELETE FROM pg_route_simple WHERE id = {}",
            remote_key
        ))
        .await;
    assert_pg_shard_route_conflict_with_operation(
        &remote_delete.expect_err("remote owner delete should fail"),
        "pg_route_simple",
        Some("DELETE"),
    );

    client.simple_query("BEGIN").await.expect("BEGIN failed");
    client
        .simple_query("CREATE TABLE pg_route_simple_txn (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .expect("CREATE TABLE failed");
    let remote_txn_insert = client
        .simple_query(&format!(
            "INSERT INTO pg_route_simple_txn VALUES ({}, 'remote')",
            txn_remote_key
        ))
        .await;
    assert_pg_shard_route_conflict_with_operation(
        &remote_txn_insert.expect_err("remote owner transaction-local insert should fail"),
        "pg_route_simple_txn",
        Some("INSERT"),
    );
    let _ = client.simple_query("ROLLBACK").await;

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_extended_query_rejects_non_local_shard_owner_insert() {
    let wal_path = format!("test_pg_shard_owner_extended_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let config = sharded_pg_test_config(4);
    let shard_router = ShardRouter::from_config(&config).expect("shard router");
    let remote_key = i64::from(integer_primary_key_for_owner(
        &shard_router,
        "pg_route_extended",
        2,
    ));
    let executor = Arc::new(Executor::with_config_and_shard_router(
        storage.clone(),
        &StorageConfig::default(),
        Some(shard_router),
    ));
    let port = next_pg_test_port();

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
        .simple_query("CREATE TABLE pg_route_extended (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .expect("CREATE TABLE failed");

    let remote_insert = client
        .execute(
            "INSERT INTO pg_route_extended (name, id) VALUES ($1, $2)",
            &[&"remote", &remote_key],
        )
        .await;
    assert_pg_shard_route_conflict(
        &remote_insert.expect_err("remote owner insert should fail"),
        "pg_route_extended",
    );

    let remote_update = client
        .execute(
            "UPDATE pg_route_extended SET name = $1 WHERE id = $2",
            &[&"remote-updated", &remote_key],
        )
        .await;
    assert_pg_shard_route_conflict_with_operation(
        &remote_update.expect_err("remote owner update should fail"),
        "pg_route_extended",
        Some("UPDATE"),
    );

    let remote_delete = client
        .execute(
            "DELETE FROM pg_route_extended WHERE id = $1",
            &[&remote_key],
        )
        .await;
    assert_pg_shard_route_conflict_with_operation(
        &remote_delete.expect_err("remote owner delete should fail"),
        "pg_route_extended",
        Some("DELETE"),
    );

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_pg_protocol_extended_query_rejects_transaction_local_shard_owner_insert() {
    let wal_path = format!(
        "test_pg_shard_owner_extended_txn_{}.wal",
        uuid::Uuid::new_v4()
    );
    let storage: Arc<dyn Storage> =
        Arc::new(MemoryStorage::new(&wal_path).expect("Failed to create storage"));
    let config = sharded_pg_test_config(4);
    let shard_router = ShardRouter::from_config(&config).expect("shard router");
    let remote_key = i64::from(integer_primary_key_for_owner(
        &shard_router,
        "pg_route_txn",
        2,
    ));
    let executor = Arc::new(Executor::with_config_and_shard_router(
        storage.clone(),
        &StorageConfig::default(),
        Some(shard_router),
    ));
    let port = next_pg_test_port();

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

    client.simple_query("BEGIN").await.expect("BEGIN failed");
    client
        .simple_query("CREATE TABLE pg_route_txn (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .expect("CREATE TABLE failed");

    let remote_insert = client
        .execute(
            "INSERT INTO pg_route_txn (id, name) VALUES ($1, $2)",
            &[&remote_key, &"remote"],
        )
        .await;
    assert_pg_shard_route_conflict_with_operation(
        &remote_insert.expect_err("remote owner transaction-local insert should fail"),
        "pg_route_txn",
        Some("INSERT"),
    );

    let _ = client.simple_query("ROLLBACK").await;
    let _ = std::fs::remove_file(&wal_path);
}
