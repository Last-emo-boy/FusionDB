use fusiondb::common::Value;
use fusiondb::execution::Executor;
use fusiondb::storage::memory::MemoryStorage;
use fusiondb::storage::Storage;
use std::sync::Arc;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

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
async fn test_insert_unique_check_from_storage_truth() {
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

    // Rewrite the stored bytes out of band: the INSERT unique check must
    // decode the new bytes instead of serving the stale cached row.
    let updated_row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(1),
        Value::String("alice-rewritten@test.com".to_string()),
        Value::String("Alice".to_string()),
    ]);

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(b"data:uq_cache:8000000000000001", &updated_row)
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    // The stale cached email is no longer present in storage, so inserting it
    // must succeed.
    let msg = exec_ok(
        &executor,
        "INSERT INTO uq_cache VALUES (2, 'alice@test.com', 'Bob')",
    )
    .await;
    assert!(msg.contains("Inserted 1"));

    // The rewritten storage email IS a duplicate, so inserting it must fail.
    let stmts = executor
        .prepare("INSERT INTO uq_cache VALUES (3, 'alice-rewritten@test.com', 'Carol')")
        .unwrap();
    let result = executor.execute(&stmts[0]).await;
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("UNIQUE"));

    let (_, rows) = query(&executor, "SELECT * FROM uq_cache WHERE id = 2").await;
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(2),
            Value::String("alice@test.com".to_string()),
            Value::String("Bob".to_string())
        ]]
    );
    cleanup(&wal_path);
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
async fn test_foreign_key_insert_update_and_parent_delete_checks() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE fk_customers (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE fk_orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES fk_customers(id), note TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO fk_customers VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO fk_orders VALUES (10, 1, 'ok'), (11, NULL, 'draft')",
    )
    .await;

    let result = executor
        .execute(
            &executor
                .prepare("INSERT INTO fk_orders VALUES (12, 99, 'bad')")
                .unwrap()[0],
        )
        .await;
    assert!(result.is_err());
    assert!(format!("{}", result.unwrap_err()).contains("FOREIGN KEY"));

    let result = executor
        .execute(
            &executor
                .prepare("UPDATE fk_orders SET customer_id = 99 WHERE id = 10")
                .unwrap()[0],
        )
        .await;
    assert!(result.is_err());
    assert!(format!("{}", result.unwrap_err()).contains("FOREIGN KEY"));

    let result = executor
        .execute(
            &executor
                .prepare("DELETE FROM fk_customers WHERE id = 1")
                .unwrap()[0],
        )
        .await;
    assert!(result.is_err());
    assert!(format!("{}", result.unwrap_err()).contains("FOREIGN KEY"));

    exec_ok(
        &executor,
        "UPDATE fk_orders SET customer_id = 2 WHERE id = 10",
    )
    .await;
    let result = executor
        .execute(
            &executor
                .prepare("UPDATE fk_customers SET id = 3 WHERE id = 2")
                .unwrap()[0],
        )
        .await;
    assert!(result.is_err());
    assert!(format!("{}", result.unwrap_err()).contains("FOREIGN KEY"));
    cleanup(&wal);
}

#[tokio::test]
async fn test_table_level_foreign_key_constraint() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE fk_parent (id INTEGER PRIMARY KEY, code TEXT UNIQUE)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE fk_child (id INTEGER PRIMARY KEY, parent_id INTEGER, CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES fk_parent(id))",
    )
    .await;
    exec_ok(&executor, "INSERT INTO fk_parent VALUES (1, 'p1')").await;
    exec_ok(&executor, "INSERT INTO fk_child VALUES (1, 1)").await;

    let result = executor
        .execute(
            &executor
                .prepare("INSERT INTO fk_child VALUES (2, 2)")
                .unwrap()[0],
        )
        .await;
    assert!(result.is_err());
    assert!(format!("{}", result.unwrap_err()).contains("fk_child_parent"));
    cleanup(&wal);
}

#[tokio::test]
async fn test_composite_foreign_key_insert_update_and_parent_checks() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE fk_comp_parent (w_id INTEGER, d_id INTEGER, name TEXT, PRIMARY KEY (w_id, d_id))",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE fk_comp_child (id INTEGER PRIMARY KEY, c_w_id INTEGER, c_d_id INTEGER, note TEXT, CONSTRAINT fk_comp_child_parent FOREIGN KEY (c_w_id, c_d_id) REFERENCES fk_comp_parent(w_id, d_id))",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO fk_comp_parent VALUES (1, 1, 'district 1'), (1, 2, 'district 2')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO fk_comp_child VALUES (10, 1, 1, 'ok'), (11, NULL, 9, 'partial-null-ok')",
    )
    .await;

    let result = executor
        .execute(
            &executor
                .prepare("INSERT INTO fk_comp_child VALUES (12, 9, 9, 'bad')")
                .unwrap()[0],
        )
        .await;
    assert!(result.is_err());
    assert!(format!("{}", result.unwrap_err()).contains("fk_comp_child_parent"));

    let result = executor
        .execute(
            &executor
                .prepare("UPDATE fk_comp_child SET c_d_id = 9 WHERE id = 10")
                .unwrap()[0],
        )
        .await;
    assert!(result.is_err());
    assert!(format!("{}", result.unwrap_err()).contains("FOREIGN KEY"));

    let result = executor
        .execute(
            &executor
                .prepare("DELETE FROM fk_comp_parent WHERE w_id = 1 AND d_id = 1")
                .unwrap()[0],
        )
        .await;
    assert!(result.is_err());
    assert!(format!("{}", result.unwrap_err()).contains("FOREIGN KEY"));

    let result = executor
        .execute(
            &executor
                .prepare("UPDATE fk_comp_parent SET d_id = 3 WHERE w_id = 1 AND d_id = 1")
                .unwrap()[0],
        )
        .await;
    assert!(result.is_err());
    assert!(format!("{}", result.unwrap_err()).contains("FOREIGN KEY"));

    cleanup(&wal);
}

#[tokio::test]
async fn test_foreign_key_blocks_dependent_alter_and_drop_table() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE fk_alter_parent (id INTEGER PRIMARY KEY)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE fk_alter_child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES fk_alter_parent(id))",
    )
    .await;

    let result = executor
        .execute(
            &executor
                .prepare("ALTER TABLE fk_alter_child DROP COLUMN parent_id")
                .unwrap()[0],
        )
        .await;
    assert!(result.is_err());
    assert!(format!("{}", result.unwrap_err()).contains("FOREIGN KEY"));

    let result = executor
        .execute(
            &executor
                .prepare("ALTER TABLE fk_alter_parent RENAME COLUMN id TO new_id")
                .unwrap()[0],
        )
        .await;
    assert!(result.is_err());
    assert!(format!("{}", result.unwrap_err()).contains("FOREIGN KEY"));

    let result = executor
        .execute(&executor.prepare("DROP TABLE fk_alter_parent").unwrap()[0])
        .await;
    assert!(result.is_err());
    assert!(format!("{}", result.unwrap_err()).contains("FOREIGN KEY"));
    cleanup(&wal);
}
