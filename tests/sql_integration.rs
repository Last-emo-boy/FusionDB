use fusiondb::common::Value;
use fusiondb::execution::{Executor, QueryResult};
use fusiondb::storage::memory::MemoryStorage;
use fusiondb::storage::Storage;
use std::sync::Arc;

/// Helper: create an executor with a fresh MemoryStorage (temp WAL file)
async fn setup() -> (Arc<Executor>, String) {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage));
    (executor, wal_path)
}

/// Helper: execute a single SQL statement
async fn exec(executor: &Executor, sql: &str) -> QueryResult {
    let stmts = executor.prepare(sql).unwrap();
    executor.execute(&stmts[0]).await.unwrap()
}

/// Helper: execute and expect a Select result, return (columns, rows)
async fn query(executor: &Executor, sql: &str) -> (Vec<String>, Vec<Vec<Value>>) {
    match exec(executor, sql).await {
        QueryResult::Select { columns, rows } => (columns, rows),
        other => panic!("Expected Select, got {:?}", other),
    }
}

/// Helper: execute and expect a Success result, return message
async fn exec_ok(executor: &Executor, sql: &str) -> String {
    match exec(executor, sql).await {
        QueryResult::Success { message } => message,
        other => panic!("Expected Success, got {:?}", other),
    }
}

/// Cleanup WAL file after test
fn cleanup(wal_path: &str) {
    let _ = std::fs::remove_file(wal_path);
}

// ==================== DDL Tests ====================

#[tokio::test]
async fn test_create_table() {
    let (executor, wal) = setup().await;
    let msg = exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    )
    .await;
    assert!(msg.contains("created"));
    cleanup(&wal);
}

#[tokio::test]
async fn test_create_table_and_show_tables() {
    let (executor, wal) = setup().await;
    exec_ok(&executor, "CREATE TABLE t1 (id INTEGER PRIMARY KEY)").await;
    exec_ok(&executor, "CREATE TABLE t2 (id INTEGER PRIMARY KEY)").await;
    let (cols, rows) = query(&executor, "SHOW TABLES").await;
    assert_eq!(cols, vec!["Table"]);
    assert_eq!(rows.len(), 2);
    cleanup(&wal);
}

#[tokio::test]
async fn test_describe_table() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price FLOAT)",
    )
    .await;
    let (cols, rows) = query(&executor, "EXPLAIN items").await;
    assert_eq!(cols, vec!["Field", "Type", "Key", "Index"]);
    assert_eq!(rows.len(), 3);
    // First column should be primary
    if let Value::String(key) = &rows[0][2] {
        assert_eq!(key, "PRI");
    }
    cleanup(&wal);
}

#[tokio::test]
async fn test_drop_table() {
    let (executor, wal) = setup().await;
    exec_ok(&executor, "CREATE TABLE temp (id INTEGER PRIMARY KEY)").await;
    exec_ok(&executor, "DROP TABLE temp").await;
    let (_, rows) = query(&executor, "SHOW TABLES").await;
    assert_eq!(rows.len(), 0);
    cleanup(&wal);
}

#[tokio::test]
async fn test_drop_table_invalidates_row_cache() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE drop_cache_stale (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO drop_cache_stale VALUES (1, 'Alice')",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT * FROM drop_cache_stale WHERE id = 1").await;
    assert_eq!(
        rows,
        vec![vec![Value::Integer(1), Value::String("Alice".to_string())]]
    );

    exec_ok(&executor, "DROP TABLE drop_cache_stale").await;
    exec_ok(
        &executor,
        "CREATE TABLE drop_cache_stale (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT * FROM drop_cache_stale WHERE id = 1").await;
    assert_eq!(rows.len(), 0);
    cleanup(&wal);
}

#[tokio::test]
async fn test_drop_table_if_exists() {
    let (executor, wal) = setup().await;
    let msg = exec_ok(&executor, "DROP TABLE IF EXISTS nonexistent").await;
    assert!(msg.contains("Dropped 0"));
    cleanup(&wal);
}

// ==================== INSERT Tests ====================

#[tokio::test]
async fn test_insert_single_row() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    let msg = exec_ok(&executor, "INSERT INTO users VALUES (1, 'Alice')").await;
    assert!(msg.contains("Inserted 1"));
    cleanup(&wal);
}

#[tokio::test]
async fn test_insert_multiple_rows() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    let msg = exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
    )
    .await;
    assert!(msg.contains("Inserted 3"));
    cleanup(&wal);
}

// ==================== SELECT Tests ====================

#[tokio::test]
async fn test_select_all() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;
    let (cols, rows) = query(&executor, "SELECT * FROM users").await;
    assert_eq!(cols, vec!["id", "name"]);
    assert_eq!(rows.len(), 2);
    cleanup(&wal);
}

#[tokio::test]
async fn test_full_table_scan_reuses_row_cache() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE full_scan_cache (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO full_scan_cache VALUES (1, 'Alice')").await;

    let (_, rows) = query(&executor, "SELECT * FROM full_scan_cache").await;
    assert_eq!(
        rows,
        vec![vec![Value::Integer(1), Value::String("Alice".to_string())]]
    );

    let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(1),
        Value::String("Alice".to_string()),
    ]);
    let corrupt_col_idx = 1usize;
    let off_pos = 2 + corrupt_col_idx * 4;
    let start = u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
    for byte in &mut corrupt_row[start..] {
        *byte = 0xff;
    }

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let key = format!(
            "data:full_scan_cache:{}",
            fusiondb::common::encoding::encode_i64_comparable(1)
        );
        txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        txn.commit().await.unwrap();
    }

    let (_, rows) = query(&executor, "SELECT * FROM full_scan_cache").await;
    assert_eq!(
        rows,
        vec![vec![Value::Integer(1), Value::String("Alice".to_string())]]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_full_table_projection_reuses_full_row_cache() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE full_project_cache (id INTEGER PRIMARY KEY, name TEXT, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO full_project_cache VALUES (1, 'Alice', 'a'), (2, 'Bob', 'b')",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT * FROM full_project_cache").await;
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::String("Alice".to_string()),
                Value::String("a".to_string())
            ],
            vec![
                Value::Integer(2),
                Value::String("Bob".to_string()),
                Value::String("b".to_string())
            ]
        ]
    );

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name, payload) in [(1_i64, "Alice", "a"), (2_i64, "Bob", "b")] {
            let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(name.to_string()),
                Value::String(payload.to_string()),
            ]);
            let corrupt_col_idx = 1usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start =
                u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut corrupt_row[start..] {
                *byte = 0xff;
            }

            let key = format!(
                "data:full_project_cache:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(&executor, "SELECT name FROM full_project_cache").await;
    assert_eq!(cols, vec!["name"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::String("Alice".to_string())],
            vec![Value::String("Bob".to_string())]
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_insert_overwrite_invalidates_full_scan_row_cache() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE full_scan_insert_cache (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO full_scan_insert_cache VALUES (1, 'Alice')",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT * FROM full_scan_insert_cache").await;
    assert_eq!(rows[0][1], Value::String("Alice".to_string()));

    exec_ok(
        &executor,
        "INSERT INTO full_scan_insert_cache VALUES (1, 'Bob')",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM full_scan_insert_cache").await;
    assert_eq!(rows[0][1], Value::String("Bob".to_string()));
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_with_where_eq() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM users WHERE id = 2").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::String("Bob".to_string()));
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_with_where_gt() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO nums VALUES (1, 10), (2, 20), (3, 30)",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM nums WHERE id > 1").await;
    assert_eq!(rows.len(), 2);
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_with_commuted_primary_key_range() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE range_commuted (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO range_commuted VALUES (1, 10), (2, 20), (3, 30)",
    )
    .await;

    let (cols, rows) = query(&executor, "SELECT id FROM range_commuted WHERE 1 < id").await;
    assert_eq!(cols, vec!["id"]);
    assert_eq!(rows, vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);

    let (_, rows) = query(&executor, "SELECT id FROM range_commuted WHERE 3 > id").await;
    assert_eq!(rows, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);

    let (_, rows) = query(&executor, "SELECT id FROM range_commuted WHERE 2 <= id").await;
    assert_eq!(rows, vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);

    let (_, rows) = query(&executor, "SELECT id FROM range_commuted WHERE 2 >= id").await;
    assert_eq!(rows, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_commuted_primary_key_range_skips_nonmatching_row_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE range_commuted_decode (id INTEGER PRIMARY KEY, val TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO range_commuted_decode VALUES (2, 'two')",
    )
    .await;

    let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(1),
        Value::String("one".to_string()),
    ]);
    let corrupt_col_idx = 1usize;
    let off_pos = 2 + corrupt_col_idx * 4;
    let start = u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
    for byte in &mut corrupt_row[start..] {
        *byte = 0xff;
    }

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(b"data:range_commuted_decode:8000000000000001", &corrupt_row)
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT val FROM range_commuted_decode WHERE 1 < id",
    )
    .await;
    assert_eq!(cols, vec!["val"]);
    assert_eq!(rows, vec![vec![Value::String("two".to_string())]]);
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_primary_key_range_reuses_row_cache() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE range_cache (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO range_cache VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT * FROM range_cache WHERE id > 0").await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::String("Alice".to_string())],
            vec![Value::Integer(2), Value::String("Bob".to_string())]
        ]
    );

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name) in [(1_i64, "Alice"), (2_i64, "Bob")] {
            let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(name.to_string()),
            ]);
            let corrupt_col_idx = 1usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start =
                u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut corrupt_row[start..] {
                *byte = 0xff;
            }

            let key = format!(
                "data:range_cache:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (_, rows) = query(&executor, "SELECT * FROM range_cache WHERE id > 0").await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::String("Alice".to_string())],
            vec![Value::Integer(2), Value::String("Bob".to_string())]
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_primary_key_range_projection_reuses_full_row_cache() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE range_project_cache (id INTEGER PRIMARY KEY, name TEXT, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO range_project_cache VALUES (1, 'Alice', 'a'), (2, 'Bob', 'b')",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT * FROM range_project_cache WHERE id > 0").await;
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::String("Alice".to_string()),
                Value::String("a".to_string())
            ],
            vec![
                Value::Integer(2),
                Value::String("Bob".to_string()),
                Value::String("b".to_string())
            ]
        ]
    );

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name, payload) in [(1_i64, "Alice", "a"), (2_i64, "Bob", "b")] {
            let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(name.to_string()),
                Value::String(payload.to_string()),
            ]);
            let corrupt_col_idx = 1usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start =
                u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut corrupt_row[start..] {
                *byte = 0xff;
            }

            let key = format!(
                "data:range_project_cache:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT name FROM range_project_cache WHERE id > 0",
    )
    .await;
    assert_eq!(cols, vec!["name"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::String("Alice".to_string())],
            vec![Value::String("Bob".to_string())]
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_select_with_limit() {
    let (executor, wal) = setup().await;
    exec_ok(&executor, "CREATE TABLE nums (id INTEGER PRIMARY KEY)").await;
    exec_ok(&executor, "INSERT INTO nums VALUES (1), (2), (3), (4), (5)").await;
    let (_, rows) = query(&executor, "SELECT * FROM nums LIMIT 3").await;
    assert_eq!(rows.len(), 3);
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_with_limit_offset() {
    let (executor, wal) = setup().await;
    exec_ok(&executor, "CREATE TABLE nums (id INTEGER PRIMARY KEY)").await;
    exec_ok(&executor, "INSERT INTO nums VALUES (1), (2), (3), (4), (5)").await;
    let (_, rows) = query(&executor, "SELECT * FROM nums LIMIT 2 OFFSET 2").await;
    assert_eq!(rows.len(), 2);
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_order_by_asc() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO nums VALUES (3, 30), (1, 10), (2, 20)",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM nums ORDER BY val ASC").await;
    assert_eq!(rows[0][1], Value::Integer(10));
    assert_eq!(rows[1][1], Value::Integer(20));
    assert_eq!(rows[2][1], Value::Integer(30));
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_order_by_desc() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO nums VALUES (1, 10), (2, 20), (3, 30)",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM nums ORDER BY val DESC").await;
    assert_eq!(rows[0][1], Value::Integer(30));
    assert_eq!(rows[2][1], Value::Integer(10));
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_order_by_limit_offset() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE order_window (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO order_window VALUES (1, 50), (2, 10), (3, 40), (4, 20), (5, 30)",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT id, val FROM order_window ORDER BY val ASC LIMIT 2 OFFSET 1",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(4), Value::Integer(20)],
            vec![Value::Integer(5), Value::Integer(30)]
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_order_by_alias() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE alias_sort (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO alias_sort VALUES (1, 10), (2, 30), (3, 20)",
    )
    .await;
    let (cols, rows) = query(
        &executor,
        "SELECT val * 2 AS doubled FROM alias_sort ORDER BY doubled DESC",
    )
    .await;
    assert_eq!(cols, vec!["doubled"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(60)],
            vec![Value::Integer(40)],
            vec![Value::Integer(20)]
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_order_by_ordinal() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE ordinal_sort (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO ordinal_sort VALUES (1, 10), (2, 30), (3, 20)",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT id, val FROM ordinal_sort ORDER BY 2 DESC",
    )
    .await;
    assert_eq!(rows[0], vec![Value::Integer(2), Value::Integer(30)]);
    assert_eq!(rows[1], vec![Value::Integer(3), Value::Integer(20)]);
    assert_eq!(rows[2], vec![Value::Integer(1), Value::Integer(10)]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_projection() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO users VALUES (1, 'Alice', 30)").await;
    let (cols, rows) = query(&executor, "SELECT name FROM users WHERE id = 1").await;
    assert_eq!(cols, vec!["name"]);
    assert_eq!(rows[0][0], Value::String("Alice".to_string()));
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_count_star() {
    let (executor, wal) = setup().await;
    exec_ok(&executor, "CREATE TABLE nums (id INTEGER PRIMARY KEY)").await;
    exec_ok(&executor, "INSERT INTO nums VALUES (1), (2), (3)").await;
    let (cols, rows) = query(&executor, "SELECT COUNT(*) FROM nums").await;
    assert_eq!(cols, vec!["COUNT(*)"]);
    assert_eq!(rows[0][0], Value::Integer(3));
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_constant_projection_from_table() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO nums VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;

    let (cols, rows) = query(&executor, "SELECT 1 FROM nums").await;
    assert_eq!(cols, vec!["1"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(1)],
            vec![Value::Integer(1)]
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_count_literal() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO nums VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;

    let (cols, rows) = query(&executor, "SELECT COUNT(1) FROM nums").await;
    assert_eq!(cols, vec!["COUNT(1)"]);
    assert_eq!(rows[0][0], Value::Integer(3));
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_count_primary_key_uses_prefix_count() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE count_pk (id INTEGER PRIMARY KEY, payload TEXT)",
    )
    .await;

    let mut rows = Vec::new();
    for id in [1_i64, 2] {
        let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(id),
            Value::String(format!("payload-{}", id)),
        ]);
        let off_pos = 2;
        let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
        let end = u32::from_le_bytes(row[off_pos + 4..off_pos + 8].try_into().unwrap()) as usize;
        for byte in &mut row[start..end] {
            *byte = 0xff;
        }
        rows.push((id, row));
    }

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, row) in rows {
            let key = format!(
                "data:count_pk:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(&executor, "SELECT COUNT(id) FROM count_pk").await;
    assert_eq!(cols, vec!["COUNT(id)"]);
    assert_eq!(rows[0][0], Value::Integer(2));

    let (cols, rows) = query(&executor, "SELECT COUNT(count_pk.id) FROM count_pk").await;
    assert_eq!(cols, vec!["COUNT(count_pk.id)"]);
    assert_eq!(rows[0][0], Value::Integer(2));
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_select_count_not_null_column_uses_prefix_count() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE count_nn (id INTEGER PRIMARY KEY, code TEXT NOT NULL, payload TEXT)",
    )
    .await;

    let mut rows = Vec::new();
    for id in [1_i64, 2] {
        let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(id),
            Value::String(format!("code-{}", id)),
            Value::String(format!("payload-{}", id)),
        ]);
        let corrupt_col_idx = 1usize;
        let off_pos = 2 + corrupt_col_idx * 4;
        let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
        let end = u32::from_le_bytes(row[off_pos + 4..off_pos + 8].try_into().unwrap()) as usize;
        for byte in &mut row[start..end] {
            *byte = 0xff;
        }
        rows.push((id, row));
    }

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, row) in rows {
            let key = format!(
                "data:count_nn:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(&executor, "SELECT COUNT(code) FROM count_nn").await;
    assert_eq!(cols, vec!["COUNT(code)"]);
    assert_eq!(rows[0][0], Value::Integer(2));

    let (cols, rows) = query(&executor, "SELECT COUNT(c.code) FROM count_nn c").await;
    assert_eq!(cols, vec!["COUNT(c.code)"]);
    assert_eq!(rows[0][0], Value::Integer(2));
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_select_count_null_literal() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO nums VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;

    let (cols, rows) = query(&executor, "SELECT COUNT(NULL) FROM nums").await;
    assert_eq!(cols, vec!["COUNT(NULL)"]);
    assert_eq!(rows[0][0], Value::Integer(0));
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_min_max_primary_key() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, label TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO nums VALUES (3, 'c'), (-5, 'neg'), (1, 'a'), (2, 'b')",
    )
    .await;

    let (cols, rows) = query(&executor, "SELECT MIN(id), MAX(id) FROM nums").await;
    assert_eq!(cols, vec!["MIN(id)", "MAX(id)"]);
    assert_eq!(rows[0], vec![Value::Integer(-5), Value::Integer(3)]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_qualified_min_max_primary_key_uses_key_bounds() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE minmax_pk (id INTEGER PRIMARY KEY, payload TEXT)",
    )
    .await;

    let mut rows = Vec::new();
    for id in [-5_i64, 3] {
        let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(id),
            Value::String(format!("payload-{}", id)),
        ]);
        let off_pos = 2;
        let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
        let end = u32::from_le_bytes(row[off_pos + 4..off_pos + 8].try_into().unwrap()) as usize;
        for byte in &mut row[start..end] {
            *byte = 0xff;
        }
        rows.push((id, row));
    }

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, row) in rows {
            let key = format!(
                "data:minmax_pk:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT MIN(minmax_pk.id), MAX(minmax_pk.id) FROM minmax_pk",
    )
    .await;
    assert_eq!(cols, vec!["MIN(minmax_pk.id)", "MAX(minmax_pk.id)"]);
    assert_eq!(rows[0], vec![Value::Integer(-5), Value::Integer(3)]);

    let (cols, rows) = query(&executor, "SELECT MIN(m.id), MAX(m.id) FROM minmax_pk m").await;
    assert_eq!(cols, vec!["MIN(m.id)", "MAX(m.id)"]);
    assert_eq!(rows[0], vec![Value::Integer(-5), Value::Integer(3)]);
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_select_in_list() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM users WHERE id IN (1, 3)").await;
    assert_eq!(rows.len(), 2);
    exec_ok(&executor, "CREATE INDEX idx_users_in_name ON users (name)").await;
    let (_, rows) = query(
        &executor,
        "SELECT id FROM users WHERE name IN ('Alice', 'Charlie') ORDER BY id",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
    cleanup(&wal);
}

// ==================== UPDATE Tests ====================

#[tokio::test]
async fn test_update_single_row() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;
    let msg = exec_ok(&executor, "UPDATE users SET name = 'Robert' WHERE id = 2").await;
    assert!(msg.contains("Updated 1"));
    let (_, rows) = query(&executor, "SELECT * FROM users WHERE id = 2").await;
    assert_eq!(rows[0][1], Value::String("Robert".to_string()));
    cleanup(&wal);
}

#[tokio::test]
async fn test_update_no_match() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO users VALUES (1, 'Alice')").await;
    let msg = exec_ok(&executor, "UPDATE users SET name = 'X' WHERE id = 999").await;
    assert!(msg.contains("Updated 0"));
    cleanup(&wal);
}

// ==================== DELETE Tests ====================

#[tokio::test]
async fn test_delete_with_where() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
    )
    .await;
    let msg = exec_ok(&executor, "DELETE FROM users WHERE id = 2").await;
    assert!(msg.contains("Deleted 1"));
    let (_, rows) = query(&executor, "SELECT * FROM users").await;
    assert_eq!(rows.len(), 2);
    cleanup(&wal);
}

#[tokio::test]
async fn test_delete_primary_key_without_secondary_index_skips_row_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE fast_del (id INTEGER PRIMARY KEY, payload TEXT)",
    )
    .await;

    let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(2),
        Value::String("payload".to_string()),
    ]);
    let corrupt_col_idx = 1usize;
    let off_pos = 2 + corrupt_col_idx * 4;
    let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
    for byte in &mut row[start..] {
        *byte = 0xff;
    }

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(b"data:fast_del:8000000000000002", &row)
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    let msg = exec_ok(&executor, "DELETE FROM fast_del WHERE id = 2").await;
    assert!(msg.contains("Deleted 1"));
    let (_, rows) = query(&executor, "SELECT * FROM fast_del").await;
    assert_eq!(rows.len(), 0);
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_delete_qualified_primary_key_without_secondary_index_skips_row_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE fast_del_q (id INTEGER PRIMARY KEY, payload TEXT)",
    )
    .await;

    let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(2),
        Value::String("payload".to_string()),
    ]);
    let corrupt_col_idx = 1usize;
    let off_pos = 2 + corrupt_col_idx * 4;
    let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
    for byte in &mut row[start..] {
        *byte = 0xff;
    }

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(b"data:fast_del_q:8000000000000002", &row)
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    let msg = exec_ok(&executor, "DELETE FROM fast_del_q WHERE fast_del_q.id = 2").await;
    assert!(msg.contains("Deleted 1"));
    let (_, rows) = query(&executor, "SELECT * FROM fast_del_q").await;
    assert_eq!(rows.len(), 0);
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_delete_commuted_primary_key_without_secondary_index_skips_row_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE fast_del_commuted (id INTEGER PRIMARY KEY, payload TEXT)",
    )
    .await;

    let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(2),
        Value::String("payload".to_string()),
    ]);
    let corrupt_col_idx = 1usize;
    let off_pos = 2 + corrupt_col_idx * 4;
    let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
    for byte in &mut row[start..] {
        *byte = 0xff;
    }

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(b"data:fast_del_commuted:8000000000000002", &row)
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    let msg = exec_ok(&executor, "DELETE FROM fast_del_commuted WHERE 2 = id").await;
    assert!(msg.contains("Deleted 1"));
    let (_, rows) = query(&executor, "SELECT * FROM fast_del_commuted").await;
    assert_eq!(rows.len(), 0);
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_delete_primary_key_updates_secondary_index() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(&executor, "CREATE INDEX idx_users_name ON users (name)").await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
    )
    .await;

    let msg = exec_ok(&executor, "DELETE FROM users WHERE id = 2").await;
    assert!(msg.contains("Deleted 1"));

    let (_, rows) = query(&executor, "SELECT * FROM users WHERE name = 'Bob'").await;
    assert_eq!(rows.len(), 0);
    cleanup(&wal);
}

#[tokio::test]
async fn test_delete_primary_key_reuses_row_cache_for_secondary_index() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE del_cache (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_del_cache_name ON del_cache (name)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO del_cache VALUES (2, 'Bob')").await;

    let (_, rows) = query(&executor, "SELECT * FROM del_cache WHERE id = 2").await;
    assert_eq!(
        rows,
        vec![vec![Value::Integer(2), Value::String("Bob".to_string())]]
    );

    let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(2),
        Value::String("Bob".to_string()),
    ]);
    let corrupt_col_idx = 1usize;
    let off_pos = 2 + corrupt_col_idx * 4;
    let start = u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
    for byte in &mut corrupt_row[start..] {
        *byte = 0xff;
    }

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(b"data:del_cache:8000000000000002", &corrupt_row)
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    let msg = exec_ok(&executor, "DELETE FROM del_cache WHERE id = 2").await;
    assert!(msg.contains("Deleted 1"));

    {
        let txn = storage.begin_transaction().await.unwrap();
        let index_key = b"index:del_cache:name:Bob:8000000000000002";
        assert!(txn.get(index_key).await.unwrap().is_none());
    }
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_delete_all() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;
    let msg = exec_ok(&executor, "DELETE FROM users").await;
    assert!(msg.contains("Deleted 2"));
    let (_, rows) = query(&executor, "SELECT * FROM users").await;
    assert_eq!(rows.len(), 0);
    cleanup(&wal);
}

#[tokio::test]
async fn test_delete_all_without_secondary_index_skips_row_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE fast_del_all (id INTEGER PRIMARY KEY, payload TEXT)",
    )
    .await;

    let mut rows = Vec::new();
    for id in [1_i64, 2] {
        let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(id),
            Value::String(format!("payload-{}", id)),
        ]);
        let corrupt_col_idx = 1usize;
        let off_pos = 2 + corrupt_col_idx * 4;
        let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
        for byte in &mut row[start..] {
            *byte = 0xff;
        }
        rows.push((id, row));
    }

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, row) in rows {
            let key = format!(
                "data:fast_del_all:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let msg = exec_ok(&executor, "DELETE FROM fast_del_all").await;
    assert!(msg.contains("Deleted 2"));
    let (_, rows) = query(&executor, "SELECT * FROM fast_del_all").await;
    assert_eq!(rows.len(), 0);
    cleanup(&wal_path);
}

// ==================== GROUP BY Tests ====================

#[tokio::test]
async fn test_group_by_count() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, category TEXT, amount INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO orders VALUES (1, 'A', 10), (2, 'B', 20), (3, 'A', 30), (4, 'B', 40)",
    )
    .await;
    let (_cols, rows) = query(
        &executor,
        "SELECT category, COUNT(*) FROM orders GROUP BY category",
    )
    .await;
    assert_eq!(rows.len(), 2);
    // Each group has 2 items
    for row in &rows {
        assert_eq!(row[1], Value::Integer(2));
    }
    cleanup(&wal);
}

#[tokio::test]
async fn test_group_by_sum() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, category TEXT, amount INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO orders VALUES (1, 'A', 10), (2, 'B', 20), (3, 'A', 30)",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT category, SUM(amount) FROM orders GROUP BY category",
    )
    .await;
    assert_eq!(rows.len(), 2);
    cleanup(&wal);
}

// ==================== JOIN Tests ====================

#[tokio::test]
async fn test_inner_join() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO orders VALUES (1, 1, 'Widget'), (2, 2, 'Gadget'), (3, 1, 'Doohickey')",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
    )
    .await;
    assert_eq!(rows.len(), 3);
    cleanup(&wal);
}

#[tokio::test]
async fn test_join_base_scan_reuses_row_cache() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE join_cache_users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE join_cache_orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO join_cache_users VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO join_cache_orders VALUES (1, 1, 'Widget'), (2, 2, 'Gadget')",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT * FROM join_cache_users").await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::String("Alice".to_string())],
            vec![Value::Integer(2), Value::String("Bob".to_string())]
        ]
    );

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name) in [(1_i64, "Alice"), (2_i64, "Bob")] {
            let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(name.to_string()),
            ]);
            let corrupt_col_idx = 1usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start =
                u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut corrupt_row[start..] {
                *byte = 0xff;
            }

            let key = format!(
                "data:join_cache_users:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT * FROM join_cache_users JOIN join_cache_orders ON join_cache_users.id = join_cache_orders.user_id",
    )
    .await;
    assert_eq!(
        cols,
        vec![
            "join_cache_users.id",
            "join_cache_users.name",
            "join_cache_orders.id",
            "join_cache_orders.user_id",
            "join_cache_orders.product"
        ]
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], Value::String("Alice".to_string()));
    assert_eq!(rows[1][1], Value::String("Bob".to_string()));
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_join_base_scan_populates_row_cache() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE join_warm_users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE join_warm_orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO join_warm_users VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO join_warm_orders VALUES (1, 1, 'Widget'), (2, 2, 'Gadget')",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT * FROM join_warm_users JOIN join_warm_orders ON join_warm_users.id = join_warm_orders.user_id",
    )
    .await;
    assert_eq!(rows.len(), 2);

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name) in [(1_i64, "Alice"), (2_i64, "Bob")] {
            let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(name.to_string()),
            ]);
            let corrupt_col_idx = 1usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start =
                u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut corrupt_row[start..] {
                *byte = 0xff;
            }

            let key = format!(
                "data:join_warm_users:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(&executor, "SELECT * FROM join_warm_users").await;
    assert_eq!(cols, vec!["id", "name"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::String("Alice".to_string())],
            vec![Value::Integer(2), Value::String("Bob".to_string())]
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_three_table_join_with_alias_projection() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, status TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_orders_user_id ON orders (user_id)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_order_items_order_id ON order_items (order_id)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO orders VALUES (10, 1, 'confirmed'), (11, 1, 'shipped'), (20, 2, 'pending')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO order_items VALUES (100, 10, 9001), (101, 10, 9002), (102, 11, 9003), (103, 20, 9004)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT u.name, o.id, oi.product_id FROM users u INNER JOIN orders o ON u.id = o.user_id INNER JOIN order_items oi ON o.id = oi.order_id LIMIT 100",
    )
    .await;

    assert_eq!(cols, vec!["u.name", "o.id", "oi.product_id"]);
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0][0], Value::String("Alice".to_string()));
    assert_eq!(rows[0][1], Value::Integer(10));
    assert_eq!(rows[0][2], Value::Integer(9001));
    assert!(rows
        .iter()
        .any(|row| row[1] == Value::Integer(11) && row[2] == Value::Integer(9003)));
    assert!(rows
        .iter()
        .any(|row| row[0] == Value::String("Bob".to_string()) && row[1] == Value::Integer(20)));

    let (cols, rows) = query(
        &executor,
        "SELECT u.name FROM users u INNER JOIN orders o ON u.id = o.user_id INNER JOIN order_items oi ON o.id = oi.order_id WHERE oi.product_id = 9003",
    )
    .await;

    assert_eq!(cols, vec!["u.name"]);
    assert_eq!(rows, vec![vec![Value::String("Alice".to_string())]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_inner_join_with_left_filter_and_indexed_right_probe() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_orders_user_id ON orders (user_id)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO orders VALUES (1, 1, 'Widget'), (2, 2, 'Gadget'), (3, 1, 'Cable'), (4, 3, 'Mouse')",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT users.id, users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = 1 ORDER BY orders.id",
    )
    .await;

    assert_eq!(cols, vec!["users.id", "users.name", "orders.product"]);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[0][1], Value::String("Alice".to_string()));
    assert_eq!(rows[0][2], Value::String("Widget".to_string()));
    assert_eq!(rows[1][2], Value::String("Cable".to_string()));
    cleanup(&wal);
}

#[tokio::test]
async fn test_inner_join_multi_key_uses_indexed_probe_column() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, city TEXT, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, city TEXT, product TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_orders_user_id ON orders (user_id)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Paris', 'Alice'), (2, 'Berlin', 'Bob')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO orders VALUES (1, 1, 'Paris', 'Keyboard'), (2, 1, 'London', 'Mouse'), (3, 2, 'Berlin', 'Cable')",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT users.id, users.city, orders.product FROM users INNER JOIN orders ON users.id = orders.user_id AND users.city = orders.city WHERE users.id = 1 ORDER BY orders.id",
    )
    .await;

    assert_eq!(cols, vec!["users.id", "users.city", "orders.product"]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[0][1], Value::String("Paris".to_string()));
    assert_eq!(rows[0][2], Value::String("Keyboard".to_string()));
    cleanup(&wal);
}

#[tokio::test]
async fn test_join_projection_pushdown_with_group_by() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE order_items (id INTEGER PRIMARY KEY, product_id INTEGER, quantity INTEGER, unit_price INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_order_items_product_id ON order_items (product_id)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO products VALUES (1, 'Hardware', 'Mouse'), (2, 'Hardware', 'Keyboard'), (3, 'Accessories', 'Cable')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO order_items VALUES (1, 1, 2, 50), (2, 1, 1, 50), (3, 2, 3, 80), (4, 3, 4, 10)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT products.category, SUM(order_items.quantity * order_items.unit_price) AS revenue FROM order_items INNER JOIN products ON order_items.product_id = products.id GROUP BY products.category ORDER BY SUM(order_items.quantity * order_items.unit_price) DESC",
    )
    .await;

    assert_eq!(cols, vec!["products.category", "revenue"]);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::String("Hardware".to_string()));
    assert_eq!(rows[0][1], Value::Integer(390));
    assert_eq!(rows[1][0], Value::String("Accessories".to_string()));
    assert_eq!(rows[1][1], Value::Integer(40));
    cleanup(&wal);
}

#[tokio::test]
async fn test_left_join() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
    )
    .await;
    exec_ok(&executor, "INSERT INTO orders VALUES (1, 1, 'Widget')").await;
    let (_, rows) = query(
        &executor,
        "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
    )
    .await;
    // Alice has 1 order, Bob/Charlie have 0 => 3 rows (with NULLs for Bob/Charlie)
    assert_eq!(rows.len(), 3);
    cleanup(&wal);
}

// ==================== Expression Tests ====================

#[tokio::test]
async fn test_arithmetic_expression() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, price INTEGER, qty INTEGER)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO items VALUES (1, 100, 5)").await;
    let (_, rows) = query(&executor, "SELECT price * qty FROM items WHERE id = 1").await;
    assert_eq!(rows[0][0], Value::Integer(500));
    cleanup(&wal);
}

#[tokio::test]
async fn test_like_pattern() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Alicia')",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM users WHERE name LIKE 'Ali%'").await;
    assert_eq!(rows.len(), 2);
    cleanup(&wal);
}

#[tokio::test]
async fn test_not_equal() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO nums VALUES (1, 10), (2, 20), (3, 10)",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM nums WHERE val != 10").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Integer(20));
    cleanup(&wal);
}

// ==================== Index Tests ====================

#[tokio::test]
async fn test_create_btree_index() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;
    let msg = exec_ok(&executor, "CREATE INDEX idx_name ON users (name)").await;
    assert!(msg.contains("indexed 2 rows"));
    // Index should be used for equality lookups
    let (_, rows) = query(&executor, "SELECT * FROM users WHERE name = 'Bob'").await;
    assert_eq!(rows.len(), 1);
    cleanup(&wal);
}

#[tokio::test]
async fn test_fts_match_against_multi_token_intersects_index_hits() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO docs VALUES (1, 'quick brown fox'), (2, 'quick blue hare'), (3, 'slow brown fox')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_docs_body ON docs (body) USING FTS",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('quick fox')",
    )
    .await;

    assert_eq!(cols, vec!["id"]);
    assert_eq!(rows, vec![vec![Value::Integer(1)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_parameter_placeholder_select_filter() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE placeholder_filter (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO placeholder_filter VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;

    let stmts = executor
        .prepare("SELECT name FROM placeholder_filter WHERE id = $1")
        .unwrap();
    let mut txn = storage.begin_transaction().await.unwrap();
    let result = executor
        .execute_in_transaction_with_params(&stmts[0], txn.as_mut(), &[Value::Integer(2)])
        .await
        .unwrap();

    if let QueryResult::Select { columns, rows } = result {
        assert_eq!(columns, vec!["name"]);
        assert_eq!(rows, vec![vec![Value::String("Bob".to_string())]]);
    } else {
        panic!("Expected Select result from parameterized query");
    }

    cleanup(&wal_path);
}

#[tokio::test]
async fn test_parameter_placeholder_match_against() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE placeholder_docs (id INTEGER PRIMARY KEY, body TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO placeholder_docs VALUES (1, 'quick brown fox'), (2, 'quick blue hare'), (3, 'slow brown fox')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_placeholder_docs_body ON placeholder_docs (body) USING FTS",
    )
    .await;

    let stmts = executor
        .prepare("SELECT id FROM placeholder_docs WHERE MATCH(body) AGAINST($1)")
        .unwrap();
    let mut txn = storage.begin_transaction().await.unwrap();
    let result = executor
        .execute_in_transaction_with_params(
            &stmts[0],
            txn.as_mut(),
            &[Value::String("quick fox".to_string())],
        )
        .await
        .unwrap();

    if let QueryResult::Select { columns, rows } = result {
        assert_eq!(columns, vec!["id"]);
        assert_eq!(rows, vec![vec![Value::Integer(1)]]);
    } else {
        panic!("Expected Select result from parameterized MATCH query");
    }

    cleanup(&wal_path);
}

#[tokio::test]
async fn test_create_index_reuses_row_cache_for_backfill() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE index_backfill_cache (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO index_backfill_cache VALUES (1, 'Alice', 30), (2, 'Bob', 42)",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT * FROM index_backfill_cache").await;
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::String("Alice".to_string()),
                Value::Integer(30)
            ],
            vec![
                Value::Integer(2),
                Value::String("Bob".to_string()),
                Value::Integer(42)
            ]
        ]
    );

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name, age) in [(1_i64, "Alice", 30_i64), (2_i64, "Bob", 42_i64)] {
            let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(name.to_string()),
                Value::Integer(age),
            ]);
            let corrupt_col_idx = 1usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start =
                u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut corrupt_row[start..] {
                *byte = 0xff;
            }

            let key = format!(
                "data:index_backfill_cache:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let msg = exec_ok(
        &executor,
        "CREATE INDEX idx_index_backfill_cache_name ON index_backfill_cache (name)",
    )
    .await;
    assert!(msg.contains("indexed 2 rows"));

    let (cols, rows) = query(
        &executor,
        "SELECT * FROM index_backfill_cache WHERE name = 'Bob'",
    )
    .await;
    assert_eq!(cols, vec!["id", "name", "age"]);
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(2),
            Value::String("Bob".to_string()),
            Value::Integer(42)
        ]]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_index_projection_does_not_poison_row_cache() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 42)",
    )
    .await;
    exec_ok(&executor, "CREATE INDEX idx_name ON users (name)").await;

    let (cols, rows) = query(&executor, "SELECT name FROM users WHERE name = 'Bob'").await;
    assert_eq!(cols, vec!["name"]);
    assert_eq!(rows, vec![vec![Value::String("Bob".to_string())]]);

    let (cols, rows) = query(&executor, "SELECT * FROM users WHERE name = 'Bob'").await;
    assert_eq!(cols, vec!["id", "name", "age"]);
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(2),
            Value::String("Bob".to_string()),
            Value::Integer(42)
        ]]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_update_invalidates_row_cache_for_index_lookup() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 42)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_users_name_cache ON users (name)",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT * FROM users WHERE name = 'Bob'").await;
    assert_eq!(rows[0][2], Value::Integer(42));

    exec_ok(&executor, "UPDATE users SET age = 43 WHERE id = 2").await;

    let (_, rows) = query(&executor, "SELECT * FROM users WHERE name = 'Bob'").await;
    assert_eq!(rows[0][2], Value::Integer(43));
    cleanup(&wal);
}

#[tokio::test]
async fn test_update_primary_key_reuses_row_cache_for_secondary_index() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE upd_cache (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_upd_cache_name ON upd_cache (name)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO upd_cache VALUES (2, 'Bob', 42)").await;

    let (_, rows) = query(&executor, "SELECT * FROM upd_cache WHERE id = 2").await;
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(2),
            Value::String("Bob".to_string()),
            Value::Integer(42)
        ]]
    );

    let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(2),
        Value::String("Bob".to_string()),
        Value::Integer(42),
    ]);
    let corrupt_col_idx = 1usize;
    let off_pos = 2 + corrupt_col_idx * 4;
    let start = u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
    for byte in &mut corrupt_row[start..] {
        *byte = 0xff;
    }

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(b"data:upd_cache:8000000000000002", &corrupt_row)
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    let msg = exec_ok(
        &executor,
        "UPDATE upd_cache SET name = 'Robert' WHERE id = 2",
    )
    .await;
    assert!(msg.contains("Updated 1"));

    let (_, rows) = query(&executor, "SELECT * FROM upd_cache WHERE name = 'Bob'").await;
    assert_eq!(rows.len(), 0);

    let (cols, rows) = query(&executor, "SELECT * FROM upd_cache WHERE name = 'Robert'").await;
    assert_eq!(cols, vec!["id", "name", "age"]);
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(2),
            Value::String("Robert".to_string()),
            Value::Integer(42)
        ]]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_update_qualified_primary_key_uses_point_lookup() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE upd_q (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO upd_q VALUES (2, 'Bob')").await;

    let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(1),
        Value::String("Alice".to_string()),
    ]);
    let corrupt_col_idx = 1usize;
    let off_pos = 2 + corrupt_col_idx * 4;
    let start = u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
    for byte in &mut corrupt_row[start..] {
        *byte = 0xff;
    }

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(b"data:upd_q:8000000000000001", &corrupt_row)
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    let msg = exec_ok(
        &executor,
        "UPDATE upd_q SET name = 'Robert' WHERE upd_q.id = 2",
    )
    .await;
    assert!(msg.contains("Updated 1"));

    let (cols, rows) = query(&executor, "SELECT id, name FROM upd_q WHERE id = 2").await;
    assert_eq!(cols, vec!["id", "name"]);
    assert_eq!(
        rows,
        vec![vec![Value::Integer(2), Value::String("Robert".to_string())]]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_update_commuted_primary_key_uses_point_lookup() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE upd_commuted (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO upd_commuted VALUES (2, 'Bob')").await;

    let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(1),
        Value::String("Alice".to_string()),
    ]);
    let corrupt_col_idx = 1usize;
    let off_pos = 2 + corrupt_col_idx * 4;
    let start = u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
    for byte in &mut corrupt_row[start..] {
        *byte = 0xff;
    }

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(b"data:upd_commuted:8000000000000001", &corrupt_row)
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    let msg = exec_ok(
        &executor,
        "UPDATE upd_commuted SET name = 'Robert' WHERE 2 = id",
    )
    .await;
    assert!(msg.contains("Updated 1"));

    let (cols, rows) = query(&executor, "SELECT id, name FROM upd_commuted WHERE id = 2").await;
    assert_eq!(cols, vec!["id", "name"]);
    assert_eq!(
        rows,
        vec![vec![Value::Integer(2), Value::String("Robert".to_string())]]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_primary_key_only_equality_projection() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, payload TEXT, score INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO nums VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)",
    )
    .await;

    let (cols, rows) = query(&executor, "SELECT id FROM nums WHERE id = 2").await;
    assert_eq!(cols, vec!["id"]);
    assert_eq!(rows, vec![vec![Value::Integer(2)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_primary_key_point_lookup_reuses_row_cache() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE pk_lookup_cache (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO pk_lookup_cache VALUES (1, 'Alice')").await;

    let (_, rows) = query(&executor, "SELECT * FROM pk_lookup_cache WHERE id = 1").await;
    assert_eq!(
        rows,
        vec![vec![Value::Integer(1), Value::String("Alice".to_string())]]
    );

    let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(1),
        Value::String("Alice".to_string()),
    ]);
    let corrupt_col_idx = 1usize;
    let off_pos = 2 + corrupt_col_idx * 4;
    let start = u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
    for byte in &mut corrupt_row[start..] {
        *byte = 0xff;
    }

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(b"data:pk_lookup_cache:8000000000000001", &corrupt_row)
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    let (_, rows) = query(&executor, "SELECT * FROM pk_lookup_cache WHERE id = 1").await;
    assert_eq!(
        rows,
        vec![vec![Value::Integer(1), Value::String("Alice".to_string())]]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_primary_key_projection_reuses_full_row_cache() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE pk_project_cache (id INTEGER PRIMARY KEY, name TEXT, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO pk_project_cache VALUES (1, 'Alice', 'payload')",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT * FROM pk_project_cache WHERE id = 1").await;
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(1),
            Value::String("Alice".to_string()),
            Value::String("payload".to_string())
        ]]
    );

    let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(1),
        Value::String("Alice".to_string()),
        Value::String("payload".to_string()),
    ]);
    let corrupt_col_idx = 1usize;
    let off_pos = 2 + corrupt_col_idx * 4;
    let start = u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
    for byte in &mut corrupt_row[start..] {
        *byte = 0xff;
    }

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(b"data:pk_project_cache:8000000000000001", &corrupt_row)
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(&executor, "SELECT name FROM pk_project_cache WHERE id = 1").await;
    assert_eq!(cols, vec!["name"]);
    assert_eq!(rows, vec![vec![Value::String("Alice".to_string())]]);
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_primary_key_equality_projection_skips_unused_column_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE pk_proj (id INTEGER PRIMARY KEY, name TEXT, payload TEXT)",
    )
    .await;

    let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(1),
        Value::String("Alice".to_string()),
        Value::String("large-unused-payload".to_string()),
    ]);
    let corrupt_col_idx = 2usize;
    let off_pos = 2 + corrupt_col_idx * 4;
    let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
    for byte in &mut row[start..] {
        *byte = 0xff;
    }

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let key = format!(
            "data:pk_proj:{}",
            fusiondb::common::encoding::encode_i64_comparable(1)
        );
        txn.put(key.as_bytes(), &row).await.unwrap();
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(&executor, "SELECT name FROM pk_proj WHERE id = 1").await;
    assert_eq!(cols, vec!["name"]);
    assert_eq!(rows, vec![vec![Value::String("Alice".to_string())]]);
    cleanup(&wal_path);
}

// ==================== EXPLAIN Tests ====================

#[tokio::test]
async fn test_explain() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    let (cols, rows) = query(&executor, "EXPLAIN SELECT * FROM users WHERE id = 1").await;
    assert_eq!(cols, vec!["EXPLAIN"]);
    assert_eq!(rows.len(), 1);
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Primary Key Lookup"));
    }
    cleanup(&wal);
}

#[tokio::test]
async fn test_explain_commuted_primary_key_lookup() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_pk_commuted (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    let (cols, rows) = query(
        &executor,
        "EXPLAIN SELECT * FROM explain_pk_commuted WHERE 1 = id",
    )
    .await;
    assert_eq!(cols, vec!["EXPLAIN"]);
    assert_eq!(rows.len(), 1);
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Primary Key Lookup"));
    }
    cleanup(&wal);
}

#[tokio::test]
async fn test_explain_commuted_btree_index_scan() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_idx_commuted (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_explain_idx_commuted_name ON explain_idx_commuted (name)",
    )
    .await;
    let (cols, rows) = query(
        &executor,
        "EXPLAIN SELECT * FROM explain_idx_commuted WHERE 'Bob' = name",
    )
    .await;
    assert_eq!(cols, vec!["EXPLAIN"]);
    assert_eq!(rows.len(), 1);
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Index Scan"));
    }
    cleanup(&wal);
}

#[tokio::test]
async fn test_explain_commuted_primary_key_range_scan() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_range_commuted (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    let (cols, rows) = query(
        &executor,
        "EXPLAIN SELECT * FROM explain_range_commuted WHERE 1 < id",
    )
    .await;
    assert_eq!(cols, vec!["EXPLAIN"]);
    assert_eq!(rows.len(), 1);
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Primary Key Range Scan"));
    }
    cleanup(&wal);
}

// ==================== Edge Case Tests ====================

#[tokio::test]
async fn test_select_empty_table() {
    let (executor, wal) = setup().await;
    exec_ok(&executor, "CREATE TABLE empty (id INTEGER PRIMARY KEY)").await;
    let (_, rows) = query(&executor, "SELECT * FROM empty").await;
    assert_eq!(rows.len(), 0);
    cleanup(&wal);
}

#[tokio::test]
async fn test_table_not_found_error() {
    let (executor, wal) = setup().await;
    let stmts = executor.prepare("SELECT * FROM nonexistent").unwrap();
    let result = executor.execute(&stmts[0]).await;
    assert!(result.is_err());
    cleanup(&wal);
}

#[tokio::test]
async fn test_insert_column_count_mismatch() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    )
    .await;
    let stmts = executor
        .prepare("INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();
    let result = executor.execute(&stmts[0]).await;
    assert!(result.is_err());
    cleanup(&wal);
}

#[tokio::test]
async fn test_null_handling() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO data VALUES (1, NULL)").await;
    let (_, rows) = query(&executor, "SELECT * FROM data WHERE id = 1").await;
    assert_eq!(rows[0][1], Value::Null);
    cleanup(&wal);
}

// ==================== Advanced Expression Tests ====================

#[tokio::test]
async fn test_where_and() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO items VALUES (1, 'A', 10), (2, 'B', 20), (3, 'A', 30)",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT * FROM items WHERE name = 'A' AND price > 15",
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][2], Value::Integer(30));
    cleanup(&wal);
}

#[tokio::test]
async fn test_where_or() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO items VALUES (1, 'A'), (2, 'B'), (3, 'C')",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT * FROM items WHERE name = 'A' OR name = 'C'",
    )
    .await;
    assert_eq!(rows.len(), 2);
    cleanup(&wal);
}

#[tokio::test]
async fn test_is_null() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO data VALUES (1, 'hello'), (2, NULL), (3, 'world')",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM data WHERE val IS NULL").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2));
    cleanup(&wal);
}

#[tokio::test]
async fn test_is_not_null() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO data VALUES (1, 'hello'), (2, NULL), (3, 'world')",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM data WHERE val IS NOT NULL").await;
    assert_eq!(rows.len(), 2);
    cleanup(&wal);
}

#[tokio::test]
async fn test_between() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO nums VALUES (1, 5), (2, 15), (3, 25), (4, 35)",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM nums WHERE val BETWEEN 10 AND 30").await;
    assert_eq!(rows.len(), 2);
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_distinct() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE tags (id INTEGER PRIMARY KEY, tag TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO tags VALUES (1, 'rust'), (2, 'db'), (3, 'rust'), (4, 'db'), (5, 'sql')",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT DISTINCT tag FROM tags").await;
    assert_eq!(rows.len(), 3);
    cleanup(&wal);
}

#[tokio::test]
async fn test_alter_table_add_column() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO items VALUES (1, 'apple')").await;
    exec_ok(&executor, "ALTER TABLE items ADD COLUMN price INTEGER").await;
    // Existing row should still be queryable (new column = NULL for old rows)
    let (cols, _) = query(&executor, "SELECT * FROM items").await;
    assert_eq!(cols.len(), 3);
    assert_eq!(cols[2], "price");
    cleanup(&wal);
}

#[tokio::test]
async fn test_alter_table_drop_column() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO people VALUES (1, 'Alice', 30), (2, 'Bob', 25)",
    )
    .await;
    exec_ok(&executor, "ALTER TABLE people DROP COLUMN age").await;
    let (cols, rows) = query(&executor, "SELECT * FROM people").await;
    assert_eq!(cols, vec!["id", "name"]);
    assert_eq!(rows.len(), 2);
    cleanup(&wal);
}

#[tokio::test]
async fn test_alter_table_drop_column_reuses_row_cache() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE drop_cache (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO drop_cache VALUES (1, 'Alice', 30), (2, 'Bob', 25)",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT * FROM drop_cache").await;
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::String("Alice".to_string()),
                Value::Integer(30)
            ],
            vec![
                Value::Integer(2),
                Value::String("Bob".to_string()),
                Value::Integer(25)
            ]
        ]
    );

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name, age) in [(1_i64, "Alice", 30_i64), (2_i64, "Bob", 25_i64)] {
            let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(name.to_string()),
                Value::Integer(age),
            ]);
            let corrupt_col_idx = 1usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start =
                u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut corrupt_row[start..] {
                *byte = 0xff;
            }

            let key = format!(
                "data:drop_cache:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    exec_ok(&executor, "ALTER TABLE drop_cache DROP COLUMN age").await;

    let (cols, rows) = query(&executor, "SELECT * FROM drop_cache").await;
    assert_eq!(cols, vec!["id", "name"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::String("Alice".to_string())],
            vec![Value::Integer(2), Value::String("Bob".to_string())]
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_alter_table_rename_column() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, old_name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "ALTER TABLE t1 RENAME COLUMN old_name TO new_name",
    )
    .await;
    let (cols, _) = query(&executor, "SELECT * FROM t1").await;
    assert!(cols.contains(&"new_name".to_string()));
    assert!(!cols.contains(&"old_name".to_string()));
    cleanup(&wal);
}

#[tokio::test]
async fn test_union_all() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO t1 VALUES (1, 'a'), (2, 'b')").await;
    exec_ok(&executor, "INSERT INTO t2 VALUES (2, 'b'), (3, 'c')").await;
    let (_, rows) = query(
        &executor,
        "SELECT name FROM t1 UNION ALL SELECT name FROM t2",
    )
    .await;
    assert_eq!(rows.len(), 4); // duplicates kept
    cleanup(&wal);
}

#[tokio::test]
async fn test_union_all_order_by_limit_offset() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE union_top_a (id INTEGER PRIMARY KEY, score INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE union_top_b (id INTEGER PRIMARY KEY, score INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO union_top_a VALUES (1, 50), (2, 10), (3, 40)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO union_top_b VALUES (4, 20), (5, 60), (6, 30)",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT score FROM union_top_a UNION ALL SELECT score FROM union_top_b ORDER BY score ASC LIMIT 3 OFFSET 1",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(20)],
            vec![Value::Integer(30)],
            vec![Value::Integer(40)]
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_union_distinct() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO t1 VALUES (1, 'a'), (2, 'b')").await;
    exec_ok(&executor, "INSERT INTO t2 VALUES (2, 'b'), (3, 'c')").await;
    let (_, rows) = query(&executor, "SELECT name FROM t1 UNION SELECT name FROM t2").await;
    assert_eq!(rows.len(), 3); // duplicates removed
    cleanup(&wal);
}

#[tokio::test]
async fn test_except() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    exec_ok(&executor, "INSERT INTO t2 VALUES (2, 'b')").await;
    let (_, rows) = query(&executor, "SELECT name FROM t1 EXCEPT SELECT name FROM t2").await;
    assert_eq!(rows.len(), 2); // 'a' and 'c'
    cleanup(&wal);
}

#[tokio::test]
async fn test_intersect() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO t2 VALUES (2, 'b'), (3, 'c'), (4, 'd')",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT name FROM t1 INTERSECT SELECT name FROM t2",
    )
    .await;
    assert_eq!(rows.len(), 2); // 'b' and 'c'
    cleanup(&wal);
}

#[tokio::test]
async fn test_subquery_in() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO orders VALUES (100, 1), (101, 2), (102, 1)",
    )
    .await;
    // Find customers who have orders
    let (_, rows) = query(
        &executor,
        "SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders)",
    )
    .await;
    assert_eq!(rows.len(), 2); // Alice and Bob
    cleanup(&wal);
}

#[tokio::test]
async fn test_subquery_not_in() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
    )
    .await;
    exec_ok(&executor, "INSERT INTO orders VALUES (100, 1), (101, 2)").await;
    // Find customers who have NO orders
    let (_, rows) = query(
        &executor,
        "SELECT name FROM customers WHERE id NOT IN (SELECT customer_id FROM orders)",
    )
    .await;
    assert_eq!(rows.len(), 1); // Carol
    cleanup(&wal);
}

#[tokio::test]
async fn test_scalar_subquery() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE scores (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO scores VALUES (1, 10), (2, 20), (3, 30)",
    )
    .await;
    // Scalar subquery: find rows above average (average = 20)
    let (_, rows) = query(&executor, "SELECT val FROM scores WHERE val > (SELECT 20)").await;
    assert_eq!(rows.len(), 1); // only 30
    cleanup(&wal);
}

#[tokio::test]
async fn test_show_create_table() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT, weight FLOAT)",
    )
    .await;
    let (cols, rows) = query(&executor, "SHOW CREATE TABLE widgets").await;
    assert_eq!(cols, vec!["Table", "Create Table"]);
    assert_eq!(rows.len(), 1);
    let ddl = match &rows[0][1] {
        fusiondb::common::Value::String(s) => s.clone(),
        _ => panic!("expected string"),
    };
    assert!(ddl.contains("CREATE TABLE widgets"));
    assert!(ddl.contains("id INTEGER PRIMARY KEY"));
    assert!(ddl.contains("name TEXT"));
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_without_from() {
    let (executor, wal) = setup().await;
    let (_cols, rows) = query(&executor, "SELECT 1 + 2").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(3));
    cleanup(&wal);
}

#[tokio::test]
async fn test_truncate_table() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE trunc_test (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO trunc_test VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM trunc_test").await;
    assert_eq!(rows.len(), 3);
    exec_ok(&executor, "TRUNCATE TABLE trunc_test").await;
    let (_, rows) = query(&executor, "SELECT * FROM trunc_test").await;
    assert_eq!(rows.len(), 0);
    // Table still exists, can insert again
    exec_ok(&executor, "INSERT INTO trunc_test VALUES (10, 'new')").await;
    let (_, rows) = query(&executor, "SELECT * FROM trunc_test").await;
    assert_eq!(rows.len(), 1);
    cleanup(&wal);
}

#[tokio::test]
async fn test_truncate_table_invalidates_row_cache() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE trunc_cache_stale (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO trunc_cache_stale VALUES (1, 'Alice')",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT * FROM trunc_cache_stale WHERE id = 1").await;
    assert_eq!(
        rows,
        vec![vec![Value::Integer(1), Value::String("Alice".to_string())]]
    );

    exec_ok(&executor, "TRUNCATE TABLE trunc_cache_stale").await;

    let (_, rows) = query(&executor, "SELECT * FROM trunc_cache_stale WHERE id = 1").await;
    assert_eq!(rows.len(), 0);

    exec_ok(&executor, "INSERT INTO trunc_cache_stale VALUES (1, 'Bob')").await;
    let (_, rows) = query(&executor, "SELECT * FROM trunc_cache_stale WHERE id = 1").await;
    assert_eq!(
        rows,
        vec![vec![Value::Integer(1), Value::String("Bob".to_string())]]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_case_when_searched() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE grades (id INTEGER PRIMARY KEY, score INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO grades VALUES (1, 95), (2, 72), (3, 45)",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' ELSE 'F' END FROM grades ORDER BY id").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::String("A".to_string()));
    assert_eq!(rows[1][0], fusiondb::common::Value::String("B".to_string()));
    assert_eq!(rows[2][0], fusiondb::common::Value::String("F".to_string()));
    cleanup(&wal);
}

#[tokio::test]
async fn test_string_functions() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(&executor, "SELECT UPPER('hello')").await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::String("HELLO".to_string())
    );
    let (_, rows) = query(&executor, "SELECT LOWER('WORLD')").await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::String("world".to_string())
    );
    let (_, rows) = query(&executor, "SELECT LENGTH('test')").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(4));
    let (_, rows) = query(&executor, "SELECT CONCAT('a', 'b', 'c')").await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::String("abc".to_string())
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_coalesce_nullif() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(&executor, "SELECT COALESCE(NULL, NULL, 42)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(42));
    let (_, rows) = query(&executor, "SELECT NULLIF(1, 1)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Null);
    let (_, rows) = query(&executor, "SELECT NULLIF(1, 2)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(1));
    cleanup(&wal);
}

#[tokio::test]
async fn test_abs_round() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(&executor, "SELECT ABS(-42)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(42));
    cleanup(&wal);
}

#[tokio::test]
async fn test_cte_basic() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary INTEGER)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO employees VALUES (1, 'Alice', 'eng', 100), (2, 'Bob', 'eng', 120), (3, 'Carol', 'sales', 80)").await;
    let (_, rows) = query(&executor, "WITH eng AS (SELECT name, salary FROM employees WHERE dept = 'eng') SELECT name FROM eng ORDER BY name").await;
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::String("Alice".to_string())
    );
    assert_eq!(
        rows[1][0],
        fusiondb::common::Value::String("Bob".to_string())
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_cte_multiple() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, price INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO items VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30), (4, 'B', 40)",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "WITH cat_a AS (SELECT price FROM items WHERE category = 'A'), \
              cat_b AS (SELECT price FROM items WHERE category = 'B') \
         SELECT price FROM cat_a UNION ALL SELECT price FROM cat_b",
    )
    .await;
    assert_eq!(rows.len(), 4);
    cleanup(&wal);
}

#[tokio::test]
async fn test_create_table_if_not_exists() {
    let (executor, wal) = setup().await;
    exec_ok(&executor, "CREATE TABLE dup_test (id INTEGER PRIMARY KEY)").await;
    // Should not error with IF NOT EXISTS
    exec_ok(
        &executor,
        "CREATE TABLE IF NOT EXISTS dup_test (id INTEGER PRIMARY KEY)",
    )
    .await;
    // Without IF NOT EXISTS, should error
    let stmts = executor
        .prepare("CREATE TABLE dup_test (id INTEGER PRIMARY KEY)")
        .unwrap();
    let result = executor.execute(&stmts[0]).await;
    assert!(result.is_err());
    cleanup(&wal);
}

#[tokio::test]
async fn test_count_distinct() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE colors (id INTEGER PRIMARY KEY, color TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO colors VALUES (1, 'red'), (2, 'blue'), (3, 'red'), (4, 'green'), (5, 'blue')",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT COUNT(DISTINCT color) FROM colors").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(3)); // red, blue, green
    cleanup(&wal);
}

#[tokio::test]
async fn test_insert_select() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE src (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO src VALUES (1, 'Alice'), (2, 'Bob')").await;
    exec_ok(&executor, "INSERT INTO dst SELECT * FROM src").await;
    let (_, rows) = query(&executor, "SELECT * FROM dst ORDER BY id").await;
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0][1],
        fusiondb::common::Value::String("Alice".to_string())
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_bare_aggregate_sum_avg() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO nums VALUES (1, 10), (2, 20), (3, 30)",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT SUM(val), AVG(val) FROM nums").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(60));
    cleanup(&wal);
}

#[tokio::test]
async fn test_cast_expressions() {
    let (executor, wal) = setup().await;
    // CAST string to integer
    let (_, rows) = query(&executor, "SELECT CAST('42' AS INTEGER)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(42));
    // CAST integer to text
    let (_, rows) = query(&executor, "SELECT CAST(123 AS TEXT)").await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::String("123".to_string())
    );
    // CAST float to integer
    let (_, rows) = query(&executor, "SELECT CAST(3.7 AS INTEGER)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(3));
    // CAST integer to float
    let (_, rows) = query(&executor, "SELECT CAST(5 AS FLOAT)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Float(5.0));
    // CAST integer to boolean
    let (_, rows) = query(&executor, "SELECT CAST(1 AS BOOLEAN)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Boolean(true));
    cleanup(&wal);
}

#[tokio::test]
async fn test_exists_subquery() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE ex_items (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO ex_items VALUES (1, 'a'), (2, 'b')").await;
    exec_ok(
        &executor,
        "CREATE TABLE ex_orders (id INTEGER PRIMARY KEY, item_id INTEGER)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO ex_orders VALUES (1, 1)").await;
    // EXISTS: items that have orders
    let (_, rows) = query(
        &executor,
        "SELECT name FROM ex_items WHERE EXISTS (SELECT 1 FROM ex_orders WHERE item_id = 1)",
    )
    .await;
    assert_eq!(rows.len(), 2); // EXISTS is not correlated, so all rows match
                               // NOT EXISTS with empty result
    let (_, rows) = query(
        &executor,
        "SELECT name FROM ex_items WHERE NOT EXISTS (SELECT 1 FROM ex_orders WHERE item_id = 999)",
    )
    .await;
    assert_eq!(rows.len(), 2); // NOT EXISTS on empty = true, all rows match
                               // NOT EXISTS with non-empty result
    let (_, rows) = query(
        &executor,
        "SELECT name FROM ex_items WHERE NOT EXISTS (SELECT 1 FROM ex_orders WHERE item_id = 1)",
    )
    .await;
    assert_eq!(rows.len(), 0); // NOT EXISTS on non-empty = false
    cleanup(&wal);
}

#[tokio::test]
async fn test_string_concat_operator() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(&executor, "SELECT 'hello' || ' ' || 'world'").await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::String("hello world".to_string())
    );
    // Concat with integer
    let (_, rows) = query(&executor, "SELECT 'id=' || 42").await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::String("id=42".to_string())
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_window_row_number() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE wf (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO wf VALUES (1, 'eng', 100), (2, 'eng', 200), (3, 'sales', 150), (4, 'sales', 250)").await;
    let (cols, rows) = query(
        &executor,
        "SELECT id, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) AS rn FROM wf",
    )
    .await;
    assert_eq!(cols.len(), 3);
    assert_eq!(rows.len(), 4);
    // Each partition should have row numbers 1, 2
    for row in &rows {
        let rn = &row[2];
        assert!(matches!(
            rn,
            fusiondb::common::Value::Integer(1) | fusiondb::common::Value::Integer(2)
        ));
    }
    cleanup(&wal);
}

#[tokio::test]
async fn test_window_rank() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE wr (id INTEGER PRIMARY KEY, score INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO wr VALUES (1, 100), (2, 200), (3, 200), (4, 300)",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT id, score, RANK() OVER (ORDER BY score) AS rnk FROM wr",
    )
    .await;
    assert_eq!(rows.len(), 4);
    // Ranks should be 1, 2, 2, 4 (ties get same rank, next skips)
    let ranks: Vec<i64> = rows
        .iter()
        .map(|r| {
            if let fusiondb::common::Value::Integer(n) = &r[2] {
                *n
            } else {
                0
            }
        })
        .collect();
    let mut sorted_ranks = ranks.clone();
    sorted_ranks.sort();
    assert_eq!(sorted_ranks, vec![1, 2, 2, 4]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_window_dense_rank() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE wdr (id INTEGER PRIMARY KEY, score INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO wdr VALUES (1, 100), (2, 200), (3, 200), (4, 300)",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT id, score, DENSE_RANK() OVER (ORDER BY score) AS drnk FROM wdr",
    )
    .await;
    assert_eq!(rows.len(), 4);
    // Dense ranks should be 1, 2, 2, 3
    let ranks: Vec<i64> = rows
        .iter()
        .map(|r| {
            if let fusiondb::common::Value::Integer(n) = &r[2] {
                *n
            } else {
                0
            }
        })
        .collect();
    let mut sorted_ranks = ranks.clone();
    sorted_ranks.sort();
    assert_eq!(sorted_ranks, vec![1, 2, 2, 3]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_window_lag_lead() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE wll (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO wll VALUES (1, 10), (2, 20), (3, 30)",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT id, val, LAG(val) OVER (ORDER BY id) AS prev_val FROM wll",
    )
    .await;
    assert_eq!(rows.len(), 3);
    // First row's LAG should be Null, second should be 10, third should be 20
    assert_eq!(rows[0][2], fusiondb::common::Value::Null);
    let (_, rows) = query(
        &executor,
        "SELECT id, val, LEAD(val) OVER (ORDER BY id) AS next_val FROM wll",
    )
    .await;
    assert_eq!(rows.len(), 3);
    // Last row's LEAD should be Null
    assert_eq!(rows[2][2], fusiondb::common::Value::Null);
    cleanup(&wal);
}

#[tokio::test]
async fn test_create_view_basic() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE vt (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO vt VALUES (1, 'Alice', 90), (2, 'Bob', 60), (3, 'Carol', 85)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE VIEW high_scorers AS SELECT id, name FROM vt WHERE score >= 80",
    )
    .await;
    let (cols, rows) = query(&executor, "SELECT * FROM high_scorers").await;
    assert_eq!(cols.len(), 2);
    assert_eq!(rows.len(), 2); // Alice and Carol
    cleanup(&wal);
}

#[tokio::test]
async fn test_create_or_replace_view() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE vt2 (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO vt2 VALUES (1, 10), (2, 20), (3, 30)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE VIEW vw2 AS SELECT val FROM vt2 WHERE val > 15",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM vw2").await;
    assert_eq!(rows.len(), 2); // 20, 30
                               // OR REPLACE should succeed
    exec_ok(
        &executor,
        "CREATE OR REPLACE VIEW vw2 AS SELECT val FROM vt2 WHERE val > 25",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM vw2").await;
    assert_eq!(rows.len(), 1); // 30
    cleanup(&wal);
}

#[tokio::test]
async fn test_drop_view() {
    let (executor, wal) = setup().await;
    exec_ok(&executor, "CREATE TABLE vt3 (id INTEGER PRIMARY KEY)").await;
    exec_ok(&executor, "CREATE VIEW vw3 AS SELECT id FROM vt3").await;
    exec_ok(&executor, "DROP VIEW vw3").await;
    // Querying dropped view should fail
    let stmts = executor.prepare("SELECT * FROM vw3").unwrap();
    let result = executor.execute(&stmts[0]).await;
    assert!(result.is_err());
    // DROP VIEW IF EXISTS on missing view should not error
    exec_ok(&executor, "DROP VIEW IF EXISTS vw3").await;
    cleanup(&wal);
}

#[tokio::test]
async fn test_ilike() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE il (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO il VALUES (1, 'Alice'), (2, 'ALICE'), (3, 'Bob')",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT name FROM il WHERE name ILIKE 'alice'").await;
    assert_eq!(rows.len(), 2); // Alice and ALICE
    let (_, rows) = query(
        &executor,
        "SELECT name FROM il WHERE name NOT ILIKE 'alice'",
    )
    .await;
    assert_eq!(rows.len(), 1); // Bob
    cleanup(&wal);
}

#[tokio::test]
async fn test_insert_with_column_list() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE icl (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO icl (id, name) VALUES (1, 'Alice')").await;
    let (_, rows) = query(&executor, "SELECT * FROM icl WHERE id = 1").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(1));
    assert_eq!(
        rows[0][1],
        fusiondb::common::Value::String("Alice".to_string())
    );
    assert_eq!(rows[0][2], fusiondb::common::Value::Null); // age not specified
    cleanup(&wal);
}

#[tokio::test]
async fn test_default_column_values() {
    let (executor, wal) = setup().await;
    exec_ok(&executor, "CREATE TABLE def_test (id INTEGER PRIMARY KEY, name TEXT DEFAULT 'unknown', active BOOLEAN DEFAULT true)").await;
    exec_ok(&executor, "INSERT INTO def_test (id) VALUES (1)").await;
    let (_, rows) = query(&executor, "SELECT * FROM def_test WHERE id = 1").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(1));
    assert_eq!(
        rows[0][1],
        fusiondb::common::Value::String("unknown".to_string())
    );
    assert_eq!(rows[0][2], fusiondb::common::Value::Boolean(true));
    // Override defaults
    exec_ok(
        &executor,
        "INSERT INTO def_test (id, name) VALUES (2, 'Alice')",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM def_test WHERE id = 2").await;
    assert_eq!(
        rows[0][1],
        fusiondb::common::Value::String("Alice".to_string())
    );
    assert_eq!(rows[0][2], fusiondb::common::Value::Boolean(true)); // still default
    cleanup(&wal);
}

#[tokio::test]
async fn test_not_null_constraint() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE nn (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
    )
    .await;
    // Valid insert
    exec_ok(&executor, "INSERT INTO nn VALUES (1, 'Alice')").await;
    // Should fail: NULL in NOT NULL column
    let stmts = executor.prepare("INSERT INTO nn (id) VALUES (2)").unwrap();
    let result = executor.execute(&stmts[0]).await;
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("NOT NULL"));
    cleanup(&wal);
}

#[tokio::test]
async fn test_unique_constraint() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE uq (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO uq VALUES (1, 'alice@test.com', 'Alice')",
    )
    .await;
    // Duplicate email should fail
    let stmts = executor
        .prepare("INSERT INTO uq VALUES (2, 'alice@test.com', 'Bob')")
        .unwrap();
    let result = executor.execute(&stmts[0]).await;
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("UNIQUE"));
    // Different email should succeed
    exec_ok(
        &executor,
        "INSERT INTO uq VALUES (2, 'bob@test.com', 'Bob')",
    )
    .await;
    // NULL values in UNIQUE column should be allowed (SQL standard)
    exec_ok(&executor, "INSERT INTO uq (id, name) VALUES (3, 'Carol')").await;
    let (_, rows) = query(&executor, "SELECT * FROM uq ORDER BY id").await;
    assert_eq!(rows.len(), 3);
    cleanup(&wal);
}

#[tokio::test]
async fn test_insert_unique_check_reuses_row_cache() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE uq_cache (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO uq_cache VALUES (1, 'alice@test.com', 'Alice')",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT * FROM uq_cache WHERE id = 1").await;
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(1),
            Value::String("alice@test.com".to_string()),
            Value::String("Alice".to_string())
        ]]
    );

    let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(1),
        Value::String("alice@test.com".to_string()),
        Value::String("Alice".to_string()),
    ]);
    let corrupt_col_idx = 1usize;
    let off_pos = 2 + corrupt_col_idx * 4;
    let start = u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
    for byte in &mut corrupt_row[start..] {
        *byte = 0xff;
    }

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(b"data:uq_cache:8000000000000001", &corrupt_row)
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    let msg = exec_ok(
        &executor,
        "INSERT INTO uq_cache VALUES (2, 'bob@test.com', 'Bob')",
    )
    .await;
    assert!(msg.contains("Inserted 1"));

    let (_, rows) = query(&executor, "SELECT * FROM uq_cache WHERE id = 2").await;
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(2),
            Value::String("bob@test.com".to_string()),
            Value::String("Bob".to_string())
        ]]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_like_full_patterns() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE lp (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO lp VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'Alicia')",
    )
    .await;
    // Prefix pattern
    let (_, rows) = query(&executor, "SELECT name FROM lp WHERE name LIKE 'Ali%'").await;
    assert_eq!(rows.len(), 2); // Alice, Alicia
                               // Suffix pattern
    let (_, rows) = query(&executor, "SELECT name FROM lp WHERE name LIKE '%ce'").await;
    assert_eq!(rows.len(), 1); // Alice
                               // Contains pattern
    let (_, rows) = query(&executor, "SELECT name FROM lp WHERE name LIKE '%li%'").await;
    assert_eq!(rows.len(), 3); // Alice, Charlie, Alicia
                               // Single char wildcard
    let (_, rows) = query(&executor, "SELECT name FROM lp WHERE name LIKE 'Bo_'").await;
    assert_eq!(rows.len(), 1); // Bob
    cleanup(&wal);
}

#[tokio::test]
async fn test_not_null_on_update() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE nnu (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO nnu VALUES (1, 'Alice')").await;
    // Should fail: setting NOT NULL column to NULL
    let stmts = executor
        .prepare("UPDATE nnu SET name = NULL WHERE id = 1")
        .unwrap();
    let result = executor.execute(&stmts[0]).await;
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("NOT NULL"));
    // Valid update should work
    exec_ok(&executor, "UPDATE nnu SET name = 'Bob' WHERE id = 1").await;
    let (_, rows) = query(&executor, "SELECT name FROM nnu WHERE id = 1").await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::String("Bob".to_string())
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_show_views() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE sv_t (id INTEGER PRIMARY KEY, val TEXT)",
    )
    .await;
    exec_ok(&executor, "CREATE VIEW sv_v1 AS SELECT id FROM sv_t").await;
    exec_ok(
        &executor,
        "CREATE VIEW sv_v2 AS SELECT val FROM sv_t WHERE id > 0",
    )
    .await;
    // Use execute_sql which handles custom SHOW VIEWS
    let results = executor.execute_sql("SHOW VIEWS").await.unwrap();
    if let fusiondb::execution::QueryResult::Select { columns, rows } = &results[0] {
        assert_eq!(columns[0], "View");
        assert_eq!(columns[1], "Definition");
        assert_eq!(rows.len(), 2);
    } else {
        panic!("Expected Select result from SHOW VIEWS");
    }
    // Drop one view and verify
    exec_ok(&executor, "DROP VIEW sv_v1").await;
    let results = executor.execute_sql("SHOW VIEWS").await.unwrap();
    if let fusiondb::execution::QueryResult::Select { rows, .. } = &results[0] {
        assert_eq!(rows.len(), 1);
    }
    cleanup(&wal);
}

#[tokio::test]
async fn test_drop_index() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE di (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO di VALUES (1, 'Alice'), (2, 'Bob')").await;
    exec_ok(&executor, "CREATE INDEX idx_di_name ON di (name)").await;
    // DROP INDEX should succeed
    exec_ok(&executor, "DROP INDEX idx_di_name").await;
    // DROP INDEX IF EXISTS on missing index should not error
    exec_ok(&executor, "DROP INDEX IF EXISTS idx_di_name").await;
    // DROP INDEX on missing index without IF EXISTS should error
    let stmts = executor.prepare("DROP INDEX idx_nonexistent").unwrap();
    let result = executor.execute(&stmts[0]).await;
    assert!(result.is_err());
    cleanup(&wal);
}

#[tokio::test]
async fn test_check_constraint() {
    let (executor, wal) = setup().await;
    exec_ok(&executor, "CREATE TABLE ck (id INTEGER PRIMARY KEY, age INTEGER CHECK(age > 0), score INTEGER CHECK(score >= 0))").await;
    // Valid insert
    exec_ok(&executor, "INSERT INTO ck VALUES (1, 25, 90)").await;
    // age <= 0 should fail
    let stmts = executor
        .prepare("INSERT INTO ck VALUES (2, -5, 80)")
        .unwrap();
    let result = executor.execute(&stmts[0]).await;
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("CHECK"));
    // score < 0 should fail
    let stmts = executor
        .prepare("INSERT INTO ck VALUES (3, 30, -1)")
        .unwrap();
    let result = executor.execute(&stmts[0]).await;
    assert!(result.is_err());
    // NULL should pass CHECK (SQL standard)
    exec_ok(&executor, "INSERT INTO ck (id, age) VALUES (4, 10)").await;
    // CHECK on UPDATE: setting age to 0 should fail
    let stmts = executor
        .prepare("UPDATE ck SET age = 0 WHERE id = 1")
        .unwrap();
    let result = executor.execute(&stmts[0]).await;
    assert!(result.is_err());
    // Valid update should work
    exec_ok(&executor, "UPDATE ck SET age = 30 WHERE id = 1").await;
    cleanup(&wal);
}

#[tokio::test]
async fn test_coalesce_multi_arg() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE co (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO co VALUES (1, NULL, NULL, 'third')").await;
    exec_ok(
        &executor,
        "INSERT INTO co VALUES (2, NULL, 'second', 'third')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO co VALUES (3, 'first', 'second', 'third')",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT COALESCE(a, b, c) FROM co ORDER BY id").await;
    assert_eq!(
        rows[0][0],
        fusiondb::common::Value::String("third".to_string())
    );
    assert_eq!(
        rows[1][0],
        fusiondb::common::Value::String("second".to_string())
    );
    assert_eq!(
        rows[2][0],
        fusiondb::common::Value::String("first".to_string())
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_math_functions() {
    let (executor, wal) = setup().await;
    // CEIL / FLOOR
    let (_, rows) = query(&executor, "SELECT CEIL(3.2), FLOOR(3.8)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(4));
    assert_eq!(rows[0][1], fusiondb::common::Value::Integer(3));
    // MOD
    let (_, rows) = query(&executor, "SELECT MOD(10, 3)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(1));
    // POWER / SQRT
    let (_, rows) = query(&executor, "SELECT POWER(2, 3), SQRT(16)").await;
    assert_eq!(rows[0][0], fusiondb::common::Value::Float(8.0));
    assert_eq!(rows[0][1], fusiondb::common::Value::Float(4.0));
    cleanup(&wal);
}

#[tokio::test]
async fn test_now_function() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(&executor, "SELECT NOW()").await;
    // NOW() returns unix epoch seconds as integer
    if let fusiondb::common::Value::Integer(ts) = &rows[0][0] {
        assert!(*ts > 1700000000); // After ~2023
    } else {
        panic!("NOW() should return Integer");
    }
    cleanup(&wal);
}

#[tokio::test]
async fn test_string_agg() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE sa (id INTEGER PRIMARY KEY, grp TEXT, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sa VALUES (1, 'A', 'Alice'), (2, 'A', 'Bob'), (3, 'B', 'Carol')",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT grp, STRING_AGG(name) FROM sa GROUP BY grp ORDER BY grp",
    )
    .await;
    assert_eq!(rows.len(), 2);
    // Group A should have Alice,Bob (or Bob,Alice depending on order)
    if let fusiondb::common::Value::String(s) = &rows[0][1] {
        assert!(s.contains("Alice") && s.contains("Bob"));
    }
    // Group B should have Carol
    assert_eq!(
        rows[1][1],
        fusiondb::common::Value::String("Carol".to_string())
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_insert_returning() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE ir (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)",
    )
    .await;
    let stmts = executor
        .prepare("INSERT INTO ir VALUES (1, 'Alice', 90) RETURNING *")
        .unwrap();
    let result = executor.execute(&stmts[0]).await.unwrap();
    if let fusiondb::execution::QueryResult::Select { columns, rows } = result {
        assert_eq!(columns.len(), 3);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], fusiondb::common::Value::Integer(1));
        assert_eq!(
            rows[0][1],
            fusiondb::common::Value::String("Alice".to_string())
        );
    } else {
        panic!("Expected Select result from INSERT RETURNING");
    }
    // RETURNING specific columns
    let stmts = executor
        .prepare("INSERT INTO ir VALUES (2, 'Bob', 80) RETURNING id, name")
        .unwrap();
    let result = executor.execute(&stmts[0]).await.unwrap();
    if let fusiondb::execution::QueryResult::Select { columns, rows } = result {
        assert_eq!(columns.len(), 2);
        assert_eq!(rows[0][0], fusiondb::common::Value::Integer(2));
        assert_eq!(
            rows[0][1],
            fusiondb::common::Value::String("Bob".to_string())
        );
    } else {
        panic!("Expected Select result from INSERT RETURNING id, name");
    }
    cleanup(&wal);
}

#[tokio::test]
async fn test_update_returning() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE ur (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO ur VALUES (1, 10), (2, 20)").await;
    let stmts = executor
        .prepare("UPDATE ur SET val = val + 5 WHERE id = 1 RETURNING *")
        .unwrap();
    let result = executor.execute(&stmts[0]).await.unwrap();
    if let fusiondb::execution::QueryResult::Select { columns, rows } = result {
        assert_eq!(columns.len(), 2);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], fusiondb::common::Value::Integer(1));
        assert_eq!(rows[0][1], fusiondb::common::Value::Integer(15));
    } else {
        panic!("Expected Select result from UPDATE RETURNING");
    }
    cleanup(&wal);
}

#[tokio::test]
async fn test_delete_returning() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE dr (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO dr VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
    )
    .await;
    let stmts = executor
        .prepare("DELETE FROM dr WHERE id = 2 RETURNING *")
        .unwrap();
    let result = executor.execute(&stmts[0]).await.unwrap();
    if let fusiondb::execution::QueryResult::Select { columns, rows } = result {
        assert_eq!(columns.len(), 2);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][1],
            fusiondb::common::Value::String("Bob".to_string())
        );
    } else {
        panic!("Expected Select result from DELETE RETURNING");
    }
    // Verify row was actually deleted
    let (_, rows) = query(&executor, "SELECT * FROM dr ORDER BY id").await;
    assert_eq!(rows.len(), 2);
    cleanup(&wal);
}

#[tokio::test]
async fn test_upsert_do_update() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE up (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO up VALUES (1, 'Alice', 10)").await;
    // UPSERT: conflict on id=1 should update
    exec_ok(&executor, "INSERT INTO up VALUES (1, 'Alice2', 99) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, val = EXCLUDED.val").await;
    let (_, rows) = query(&executor, "SELECT * FROM up WHERE id = 1").await;
    assert_eq!(
        rows[0][1],
        fusiondb::common::Value::String("Alice2".to_string())
    );
    assert_eq!(rows[0][2], fusiondb::common::Value::Integer(99));
    // New row should insert normally
    exec_ok(
        &executor,
        "INSERT INTO up VALUES (2, 'Bob', 20) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM up ORDER BY id").await;
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[1][1],
        fusiondb::common::Value::String("Bob".to_string())
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_upsert_do_update_invalidates_row_cache_for_index_lookup() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE up_cache (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO up_cache VALUES (1, 'Alice', 10)").await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_up_cache_name ON up_cache (name)",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT * FROM up_cache WHERE name = 'Alice'").await;
    assert_eq!(rows[0][2], Value::Integer(10));

    exec_ok(
        &executor,
        "INSERT INTO up_cache VALUES (1, 'Alice', 99) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT * FROM up_cache WHERE name = 'Alice'").await;
    assert_eq!(rows[0][2], Value::Integer(99));
    cleanup(&wal);
}

#[tokio::test]
async fn test_upsert_do_update_reuses_row_cache_for_existing_row() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE upsert_reuse (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_upsert_reuse_name ON upsert_reuse (name)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO upsert_reuse VALUES (1, 'Alice', 10)",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT * FROM upsert_reuse WHERE id = 1").await;
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(1),
            Value::String("Alice".to_string()),
            Value::Integer(10)
        ]]
    );

    let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(1),
        Value::String("Alice".to_string()),
        Value::Integer(10),
    ]);
    let corrupt_col_idx = 1usize;
    let off_pos = 2 + corrupt_col_idx * 4;
    let start = u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
    for byte in &mut corrupt_row[start..] {
        *byte = 0xff;
    }

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(b"data:upsert_reuse:8000000000000001", &corrupt_row)
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    exec_ok(
        &executor,
        "INSERT INTO upsert_reuse VALUES (1, 'Alice', 99) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val",
    )
    .await;

    let (cols, rows) = query(&executor, "SELECT * FROM upsert_reuse WHERE name = 'Alice'").await;
    assert_eq!(cols, vec!["id", "name", "val"]);
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(1),
            Value::String("Alice".to_string()),
            Value::Integer(99)
        ]]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_upsert_do_nothing() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE upn (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO upn VALUES (1, 'Alice')").await;
    // DO NOTHING: conflict on id=1 should skip
    exec_ok(
        &executor,
        "INSERT INTO upn VALUES (1, 'Bob') ON CONFLICT (id) DO NOTHING",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM upn WHERE id = 1").await;
    assert_eq!(
        rows[0][1],
        fusiondb::common::Value::String("Alice".to_string())
    ); // unchanged
    cleanup(&wal);
}

#[tokio::test]
async fn test_cross_join() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE cj1 (id INTEGER PRIMARY KEY, a TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE cj2 (id INTEGER PRIMARY KEY, b TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO cj1 VALUES (1, 'x'), (2, 'y')").await;
    exec_ok(&executor, "INSERT INTO cj2 VALUES (10, 'p'), (20, 'q')").await;
    let (_, rows) = query(&executor, "SELECT cj1.a, cj2.b FROM cj1 CROSS JOIN cj2").await;
    assert_eq!(rows.len(), 4); // 2 x 2 = 4
    cleanup(&wal);
}

#[tokio::test]
async fn test_multi_column_order_by() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE mco (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO mco VALUES (1, 'A', 30), (2, 'B', 10), (3, 'A', 10), (4, 'B', 30)",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT grp, val FROM mco ORDER BY grp ASC, val DESC",
    )
    .await;
    // A,30 then A,10 then B,30 then B,10
    assert_eq!(rows[0][0], fusiondb::common::Value::String("A".to_string()));
    assert_eq!(rows[0][1], fusiondb::common::Value::Integer(30));
    assert_eq!(rows[1][1], fusiondb::common::Value::Integer(10));
    assert_eq!(rows[2][0], fusiondb::common::Value::String("B".to_string()));
    assert_eq!(rows[2][1], fusiondb::common::Value::Integer(30));
    cleanup(&wal);
}

#[tokio::test]
async fn test_hnsw_order_by_projection() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE vec_items (id INTEGER PRIMARY KEY, embedding VECTOR, label TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO vec_items VALUES (1, EMBEDDING('red apple'), 'apple'), (2, EMBEDDING('blue ocean'), 'ocean'), (3, EMBEDDING('green apple'), 'green')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_vec_items_embedding ON vec_items (embedding) USING HNSW",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT id FROM vec_items ORDER BY VECTOR_DISTANCE(embedding, EMBEDDING('red apple')) LIMIT 1",
    )
    .await;

    assert_eq!(cols, vec!["id"]);
    assert_eq!(rows, vec![vec![fusiondb::common::Value::Integer(1)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_rbac_create_drop_user() {
    let (executor, wal) = setup().await;
    // Create user
    let results = executor
        .execute_sql("CREATE USER alice WITH PASSWORD 'secret123'")
        .await
        .unwrap();
    assert!(matches!(
        &results[0],
        fusiondb::execution::QueryResult::Success { .. }
    ));
    // Duplicate user should fail
    let result = executor
        .execute_sql("CREATE USER alice WITH PASSWORD 'other'")
        .await;
    assert!(result.is_err());
    // SHOW USERS should list alice
    let results = executor.execute_sql("SHOW USERS").await.unwrap();
    if let fusiondb::execution::QueryResult::Select { rows, .. } = &results[0] {
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0],
            fusiondb::common::Value::String("alice".to_string())
        );
        assert_eq!(rows[0][1], fusiondb::common::Value::Boolean(false));
    } else {
        panic!("Expected Select from SHOW USERS");
    }
    // Drop user
    let results = executor.execute_sql("DROP USER alice").await.unwrap();
    assert!(matches!(
        &results[0],
        fusiondb::execution::QueryResult::Success { .. }
    ));
    // Drop non-existent user should fail
    let result = executor.execute_sql("DROP USER alice").await;
    assert!(result.is_err());
    // DROP USER IF EXISTS should not fail
    let results = executor
        .execute_sql("DROP USER IF EXISTS alice")
        .await
        .unwrap();
    assert!(matches!(
        &results[0],
        fusiondb::execution::QueryResult::Success { .. }
    ));
    cleanup(&wal);
}

#[tokio::test]
async fn test_rbac_grant_revoke() {
    let (executor, wal) = setup().await;
    executor
        .execute_sql("CREATE USER bob WITH PASSWORD 'pass'")
        .await
        .unwrap();
    // Grant SELECT on a table
    executor
        .execute_sql("GRANT SELECT ON users TO bob")
        .await
        .unwrap();
    executor
        .execute_sql("GRANT INSERT, UPDATE ON users TO bob")
        .await
        .unwrap();
    // Verify permissions
    let results = executor.execute_sql("SHOW USERS").await.unwrap();
    if let fusiondb::execution::QueryResult::Select { rows, .. } = &results[0] {
        if let fusiondb::common::Value::String(perms) = &rows[0][2] {
            assert!(perms.contains("SELECT"));
            assert!(perms.contains("INSERT"));
            assert!(perms.contains("UPDATE"));
        }
    }
    // Revoke SELECT
    executor
        .execute_sql("REVOKE SELECT ON users FROM bob")
        .await
        .unwrap();
    let results = executor.execute_sql("SHOW USERS").await.unwrap();
    if let fusiondb::execution::QueryResult::Select { rows, .. } = &results[0] {
        if let fusiondb::common::Value::String(perms) = &rows[0][2] {
            assert!(!perms.contains("SELECT"));
            assert!(perms.contains("INSERT"));
        }
    }
    // Grant on non-existent user should fail
    let result = executor.execute_sql("GRANT ALL ON test TO nobody").await;
    assert!(result.is_err());
    cleanup(&wal);
}

#[tokio::test]
async fn test_rbac_superuser() {
    let (executor, wal) = setup().await;
    executor
        .execute_sql("CREATE USER admin WITH PASSWORD 'admin' SUPERUSER")
        .await
        .unwrap();
    let results = executor.execute_sql("SHOW USERS").await.unwrap();
    if let fusiondb::execution::QueryResult::Select { rows, .. } = &results[0] {
        assert_eq!(rows[0][1], fusiondb::common::Value::Boolean(true));
    }
    executor.execute_sql("DROP USER admin").await.unwrap();
    cleanup(&wal);
}

#[tokio::test]
async fn test_rbac_permission_check() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE sec_t (id INTEGER PRIMARY KEY, val TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO sec_t VALUES (1, 'hello')").await;
    // Create a user with only SELECT permission
    executor
        .execute_sql("CREATE USER reader WITH PASSWORD 'pass'")
        .await
        .unwrap();
    executor
        .execute_sql("GRANT SELECT ON sec_t TO reader")
        .await
        .unwrap();

    // Use check_table_permission on executor (public API)
    // reader should have SELECT
    let result = executor
        .check_table_permission("reader", "sec_t", "SELECT")
        .await;
    assert!(result.is_ok());
    // reader should NOT have INSERT
    let result = executor
        .check_table_permission("reader", "sec_t", "INSERT")
        .await;
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("Permission denied"));
    // Empty username (anonymous) should always pass
    let result = executor.check_table_permission("", "sec_t", "DELETE").await;
    assert!(result.is_ok());

    // Create superuser — should pass any check
    executor
        .execute_sql("CREATE USER boss WITH PASSWORD 'boss' SUPERUSER")
        .await
        .unwrap();
    let result = executor
        .check_table_permission("boss", "anything", "DELETE")
        .await;
    assert!(result.is_ok());

    executor.execute_sql("DROP USER reader").await.unwrap();
    executor.execute_sql("DROP USER boss").await.unwrap();
    cleanup(&wal);
}
