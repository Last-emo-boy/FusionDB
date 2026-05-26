use fusiondb::common::Value;
use fusiondb::execution::Executor;
use fusiondb::storage::memory::MemoryStorage;
use fusiondb::storage::Storage;
use std::sync::Arc;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

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
