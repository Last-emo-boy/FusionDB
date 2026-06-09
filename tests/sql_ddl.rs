use fusiondb::common::Value;
use fusiondb::execution::{Executor, QueryResult};
use fusiondb::storage::memory::MemoryStorage;
use fusiondb::storage::Storage;
use std::sync::Arc;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

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
async fn test_show_all_returns_settings_rows() {
    let (executor, wal) = setup().await;
    let results = executor.execute_sql("SHOW ALL").await.unwrap();

    if let Some(QueryResult::Select { columns, rows }) = results.first() {
        assert_eq!(
            columns,
            &vec![
                "name".to_string(),
                "setting".to_string(),
                "description".to_string()
            ]
        );
        assert!(rows.iter().any(|row| {
            row[0] == Value::String("server_version".to_string())
                && row[1] == Value::String("15.0".to_string())
        }));
        assert!(rows
            .iter()
            .any(|row| row[0] == Value::String("max_index_keys".to_string())));
    } else {
        panic!("Expected Select result from SHOW ALL");
    }

    cleanup(&wal);
}

#[tokio::test]
async fn test_create_table_table_level_single_primary_key() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE table_pk (id INTEGER NOT NULL, name TEXT, PRIMARY KEY (id))",
    )
    .await;

    let (cols, rows) = query(&executor, "EXPLAIN table_pk").await;
    assert_eq!(cols, vec!["Field", "Type", "Key", "Index"]);
    assert_eq!(rows[0][0], Value::String("id".to_string()));
    assert_eq!(rows[0][2], Value::String("PRI".to_string()));
    assert_eq!(rows[0][3], Value::String("BTree".to_string()));

    exec_ok(&executor, "INSERT INTO table_pk VALUES (1, 'Alice')").await;
    let (_, rows) = query(&executor, "SELECT name FROM table_pk WHERE id = 1").await;
    assert_eq!(rows, vec![vec![Value::String("Alice".to_string())]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_create_table_table_level_composite_primary_key() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE composite_pk (w_id INTEGER, d_id INTEGER, name TEXT, PRIMARY KEY (w_id, d_id))",
    )
    .await;

    let indexes = executor
        .execute_sql("SHOW INDEXES FROM composite_pk")
        .await
        .unwrap();
    if let Some(QueryResult::Select { rows, .. }) = indexes.first() {
        assert!(rows.iter().any(|row| {
            row[0] == Value::String("composite_pk_pkey".to_string())
                && row[1] == Value::String("composite_pk".to_string())
                && row[2] == Value::String("w_id,d_id".to_string())
        }));
    } else {
        panic!("Expected Select result from SHOW INDEXES FROM");
    }

    exec_ok(
        &executor,
        "INSERT INTO composite_pk VALUES (1, 1, 'district-1')",
    )
    .await;
    let stmts = executor
        .prepare("INSERT INTO composite_pk VALUES (1, 1, 'duplicate')")
        .unwrap();
    let result = executor.execute(&stmts[0]).await;
    assert!(result.is_err());
    assert!(format!("{:?}", result.err().unwrap()).contains("PRIMARY KEY constraint violated"));

    let (cols, rows) = query(
        &executor,
        "SELECT name FROM composite_pk WHERE w_id = 1 AND d_id = 1",
    )
    .await;
    assert_eq!(cols, vec!["name"]);
    assert_eq!(rows, vec![vec![Value::String("district-1".to_string())]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_create_table_rejects_table_level_primary_key_not_first_column() {
    let (executor, wal) = setup().await;
    let stmts = executor
        .prepare("CREATE TABLE table_pk_not_first (tenant INTEGER, id INTEGER, PRIMARY KEY (id))")
        .unwrap();
    let result = executor.execute(&stmts[0]).await;
    assert!(result.is_err());
    assert!(format!("{:?}", result.err().unwrap())
        .contains("requires the primary key column to be the first column"));
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
async fn test_explain_qualified_primary_key_lookup() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_pk_qualified (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    let (cols, rows) = query(
        &executor,
        "EXPLAIN SELECT * FROM explain_pk_qualified WHERE explain_pk_qualified.id = 1",
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

#[tokio::test]
async fn test_analyze_table_collects_statistics() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE analyze_items (id INTEGER PRIMARY KEY, category TEXT, qty INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO analyze_items VALUES (1, 'book', 5), (2, 'book', 7), (3, 'toy', NULL)",
    )
    .await;

    let msg = exec_ok(&executor, "ANALYZE TABLE analyze_items COMPUTE STATISTICS").await;

    assert!(msg.contains("Analyzed table analyze_items"));
    assert!(msg.contains("3 rows"));
    assert!(msg.contains("3 columns"));
    cleanup(&wal);
}

#[tokio::test]
async fn test_explain_includes_analyze_statistics() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_stats (id INTEGER PRIMARY KEY, category TEXT, qty INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO explain_stats VALUES (1, 'book', 5), (2, 'book', 7), (3, 'toy', NULL)",
    )
    .await;
    exec_ok(&executor, "ANALYZE TABLE explain_stats COMPUTE STATISTICS").await;

    let (cols, rows) = query(
        &executor,
        "EXPLAIN SELECT * FROM explain_stats WHERE category = 'book'",
    )
    .await;
    assert_eq!(cols, vec!["EXPLAIN"]);
    assert_eq!(rows.len(), 1);
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Estimate: rows=2, cost="));
        assert!(plan.contains("Stats: rows=3"));
        assert!(plan.contains("category(distinct=2"));
        assert!(plan.contains("qty(distinct=2, nulls=1"));
    } else {
        panic!("expected explain text");
    }
    cleanup(&wal);
}

#[tokio::test]
async fn test_explain_join_order_includes_analyze_estimates() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_join_big (id INTEGER PRIMARY KEY, join_key INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_join_mid (id INTEGER PRIMARY KEY, join_key INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_join_small (id INTEGER PRIMARY KEY, join_key INTEGER)",
    )
    .await;

    exec_ok(
        &executor,
        "INSERT INTO explain_join_big VALUES (1, 1), (2, 1), (3, 1), (4, 1)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO explain_join_mid VALUES (1, 1), (2, 1)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO explain_join_small VALUES (1, 1)").await;
    exec_ok(
        &executor,
        "ANALYZE TABLE explain_join_big COMPUTE STATISTICS",
    )
    .await;
    exec_ok(
        &executor,
        "ANALYZE TABLE explain_join_mid COMPUTE STATISTICS",
    )
    .await;
    exec_ok(
        &executor,
        "ANALYZE TABLE explain_join_small COMPUTE STATISTICS",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "EXPLAIN SELECT *
           FROM explain_join_big, explain_join_mid, explain_join_small
          WHERE explain_join_big.join_key = explain_join_mid.join_key
            AND explain_join_mid.join_key = explain_join_small.join_key
            AND explain_join_big.join_key = explain_join_small.join_key",
    )
    .await;

    assert_eq!(cols, vec!["EXPLAIN"]);
    assert_eq!(rows.len(), 1);
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Join Order:"));
        assert!(plan.contains("Join Estimate: rows="));
        let small = plan
            .find("explain_join_small(rows=1)")
            .expect("small table estimate missing");
        let mid = plan
            .find("explain_join_mid(rows=2)")
            .expect("mid table estimate missing");
        let big = plan
            .find("explain_join_big(rows=4)")
            .expect("big table estimate missing");
        assert!(small < mid && mid < big, "unexpected join order: {plan}");
    } else {
        panic!("expected explain text");
    }
    cleanup(&wal);
}

#[tokio::test]
async fn test_explain_join_order_counts_rows_without_analyze_statistics() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_join_count_big (id INTEGER PRIMARY KEY, join_key INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_join_count_mid (id INTEGER PRIMARY KEY, join_key INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_join_count_small (id INTEGER PRIMARY KEY, join_key INTEGER)",
    )
    .await;

    exec_ok(
        &executor,
        "INSERT INTO explain_join_count_big VALUES (1, 1), (2, 1), (3, 1), (4, 1)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO explain_join_count_mid VALUES (1, 1), (2, 1)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO explain_join_count_small VALUES (1, 1)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "EXPLAIN SELECT *
           FROM explain_join_count_big, explain_join_count_mid, explain_join_count_small
          WHERE explain_join_count_big.join_key = explain_join_count_mid.join_key
            AND explain_join_count_mid.join_key = explain_join_count_small.join_key
            AND explain_join_count_big.join_key = explain_join_count_small.join_key",
    )
    .await;

    assert_eq!(cols, vec!["EXPLAIN"]);
    assert_eq!(rows.len(), 1);
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Join Order:"));
        let small = plan
            .find("explain_join_count_small(rows=1)")
            .expect("small table fallback estimate missing");
        let mid = plan
            .find("explain_join_count_mid(rows=2)")
            .expect("mid table fallback estimate missing");
        let big = plan
            .find("explain_join_count_big(rows=4)")
            .expect("big table fallback estimate missing");
        assert!(small < mid && mid < big, "unexpected join order: {plan}");
    } else {
        panic!("expected explain text");
    }
    cleanup(&wal);
}

// ==================== Edge Case Tests ====================

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
async fn test_alter_table_add_multiple_columns() {
    let (executor, wal) = setup().await;
    exec_ok(&executor, "CREATE TABLE multi_add (id INTEGER PRIMARY KEY)").await;
    let msg = exec_ok(
        &executor,
        "ALTER TABLE multi_add ADD COLUMN name TEXT, ADD COLUMN age INTEGER",
    )
    .await;
    assert!(msg.contains("Added column name"));
    assert!(msg.contains("Added column age"));

    let (cols, _) = query(&executor, "SELECT * FROM multi_add").await;
    assert_eq!(cols, vec!["id", "name", "age"]);
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
async fn test_alter_table_only_add_primary_key_pgbench_shape() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE pgbench_branches (bid INTEGER NOT NULL, bbalance INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO pgbench_branches VALUES (1, 0), (2, 10)",
    )
    .await;

    let msg = exec_ok(
        &executor,
        "ALTER TABLE ONLY pgbench_branches ADD CONSTRAINT pgbench_branches_pkey PRIMARY KEY (bid)",
    )
    .await;
    assert!(msg.contains("Added PRIMARY KEY pgbench_branches_pkey"));

    let (cols, rows) = query(&executor, "EXPLAIN pgbench_branches").await;
    assert_eq!(cols, vec!["Field", "Type", "Key", "Index"]);
    assert_eq!(rows[0][0], Value::String("bid".to_string()));
    assert_eq!(rows[0][2], Value::String("PRI".to_string()));
    assert_eq!(rows[0][3], Value::String("BTree".to_string()));

    let indexes = executor
        .execute_sql("SHOW INDEXES FROM pgbench_branches")
        .await
        .unwrap();
    if let Some(QueryResult::Select { rows, .. }) = indexes.first() {
        assert!(rows.iter().any(|row| {
            row[0] == Value::String("pgbench_branches_pkey".to_string())
                && row[1] == Value::String("pgbench_branches".to_string())
                && row[2] == Value::String("bid".to_string())
        }));
    } else {
        panic!("Expected Select result from SHOW INDEXES FROM");
    }

    let (cols, rows) = query(
        &executor,
        "SELECT bbalance FROM pgbench_branches WHERE bid = 2",
    )
    .await;
    assert_eq!(cols, vec!["bbalance"]);
    assert_eq!(rows, vec![vec![Value::Integer(10)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_alter_table_add_primary_key_rewrites_secondary_btree_index_row_ids() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE add_pk_secondary_index (id INTEGER NOT NULL, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO add_pk_secondary_index VALUES (10, 'alice'), (20, 'bob')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_add_pk_secondary_name ON add_pk_secondary_index (name)",
    )
    .await;

    exec_ok(
        &executor,
        "ALTER TABLE ONLY add_pk_secondary_index ADD CONSTRAINT add_pk_secondary_index_pkey PRIMARY KEY (id)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT id FROM add_pk_secondary_index WHERE name = 'bob'",
    )
    .await;
    assert_eq!(cols, vec!["id"]);
    assert_eq!(rows, vec![vec![Value::Integer(20)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_alter_table_add_primary_key_uses_default_index_name() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE alter_pk_default (id INTEGER NOT NULL, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO alter_pk_default VALUES (1, 'alice'), (2, 'bob')",
    )
    .await;

    let msg = exec_ok(
        &executor,
        "ALTER TABLE alter_pk_default ADD PRIMARY KEY (id)",
    )
    .await;
    assert!(msg.contains("Added PRIMARY KEY alter_pk_default_id_pkey"));

    let indexes = executor
        .execute_sql("SHOW INDEXES FROM alter_pk_default")
        .await
        .unwrap();
    if let Some(QueryResult::Select { rows, .. }) = indexes.first() {
        assert!(rows.iter().any(|row| {
            row[0] == Value::String("alter_pk_default_id_pkey".to_string())
                && row[1] == Value::String("alter_pk_default".to_string())
                && row[2] == Value::String("id".to_string())
        }));
    } else {
        panic!("Expected Select result from SHOW INDEXES FROM");
    }

    cleanup(&wal);
}

#[tokio::test]
async fn test_alter_table_add_primary_key_rejects_existing_primary_key() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE already_pk (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;

    let stmts = executor
        .prepare("ALTER TABLE already_pk ADD CONSTRAINT already_pk_pkey PRIMARY KEY (id)")
        .unwrap();
    let result = executor.execute(&stmts[0]).await;
    assert!(result.is_err());
    assert!(format!("{:?}", result.err().unwrap()).contains("already has a PRIMARY KEY"));
    cleanup(&wal);
}

#[tokio::test]
async fn test_alter_table_add_primary_key_requires_first_column() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE pk_not_first (tenant INTEGER, id INTEGER NOT NULL)",
    )
    .await;

    let stmts = executor
        .prepare("ALTER TABLE pk_not_first ADD CONSTRAINT pk_not_first_pkey PRIMARY KEY (id)")
        .unwrap();
    let result = executor.execute(&stmts[0]).await;
    assert!(result.is_err());
    assert!(format!("{:?}", result.err().unwrap())
        .contains("requires the primary key column to be the first column"));
    cleanup(&wal);
}

#[tokio::test]
async fn test_alter_table_add_primary_key_rejects_existing_nulls() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE pk_with_nulls (id INTEGER, name TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let key = "data:pk_with_nulls:manual_null";
        let row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Null,
            Value::String("bad".to_string()),
        ]);
        txn.put(key.as_bytes(), &row).await.unwrap();
        txn.commit().await.unwrap();
    }

    let stmts = executor
        .prepare("ALTER TABLE pk_with_nulls ADD CONSTRAINT pk_with_nulls_pkey PRIMARY KEY (id)")
        .unwrap();
    let result = executor.execute(&stmts[0]).await;
    assert!(result.is_err());
    assert!(format!("{:?}", result.err().unwrap()).contains("contains NULL values"));
    cleanup(&wal_path);
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
