use fusiondb::common::Value;
use fusiondb::execution::Executor;
use fusiondb::storage::memory::MemoryStorage;
use fusiondb::storage::Storage;
use std::sync::Arc;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

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
async fn test_upsert_do_update_maintains_composite_index() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE district_orders (id INTEGER PRIMARY KEY, warehouse_id INTEGER, district_id INTEGER, status TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_district_orders_wd ON district_orders (warehouse_id, district_id)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO district_orders VALUES (1, 1, 10, 'open')",
    )
    .await;

    exec_ok(
        &executor,
        "INSERT INTO district_orders VALUES (1, 2, 20, 'moved') ON CONFLICT (id) DO UPDATE SET warehouse_id = EXCLUDED.warehouse_id, district_id = EXCLUDED.district_id, status = EXCLUDED.status",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT status FROM district_orders WHERE warehouse_id = 1 AND district_id = 10",
    )
    .await;
    assert!(rows.is_empty());

    let (_, rows) = query(
        &executor,
        "SELECT status FROM district_orders WHERE warehouse_id = 2 AND district_id = 20",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::String("moved".to_string())]]);
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
async fn test_vector_distance_accepts_numeric_array_literals() {
    let (executor, wal) = setup().await;
    let (_, rows) = query(
        &executor,
        "SELECT VECTOR_DISTANCE(ARRAY[1, 2, 3], ARRAY[1, 4, 3])",
    )
    .await;

    match &rows[0][0] {
        Value::Float(distance) => assert!((*distance - 2.0).abs() < f64::EPSILON),
        other => panic!("Expected float vector distance, got {:?}", other),
    }
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
