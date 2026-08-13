use fusiondb::common::Value;
use fusiondb::config::StorageConfig;
use fusiondb::execution::{Executor, QueryResult};
use fusiondb::storage::{memory::MemoryStorage, FusionStorage, Storage};
use std::path::Path;
use std::sync::Arc;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

async fn setup_fusion_storage(
    test_name: &str,
) -> (Arc<Executor>, FusionStorage, std::path::PathBuf) {
    let data_dir =
        std::env::temp_dir().join(format!("fusiondb_{}_{}", test_name, uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&data_dir).unwrap();
    let mut config = StorageConfig::default();
    config.data_dir = data_dir.to_string_lossy().to_string();
    let wal_path = config.wal_path();
    let fusion = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
        .await
        .unwrap();
    let storage: Arc<dyn Storage> = Arc::new(fusion.clone());
    let executor = Arc::new(Executor::new(storage));
    (executor, fusion, data_dir)
}

fn cleanup_storage_dir(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
}

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
async fn test_vacuum_requires_fusion_storage() {
    let (executor, wal) = setup().await;

    let err = executor.execute_sql("VACUUM").await.unwrap_err();
    assert!(format!("{:?}", err).contains("VACUUM is only available for FusionStorage"));

    cleanup(&wal);
}

#[tokio::test]
async fn test_vacuum_runs_fusion_compaction_and_preserves_rows() {
    let (executor, fusion, data_dir) = setup_fusion_storage("vacuum_compaction").await;

    exec_ok(
        &executor,
        "CREATE TABLE vacuum_items (id INTEGER PRIMARY KEY, label TEXT)",
    )
    .await;

    for id in 1..=3 {
        exec_ok(
            &executor,
            &format!("INSERT INTO vacuum_items VALUES ({id}, 'before-{id}')"),
        )
        .await;
        fusion.create_snapshot_now().await.unwrap();
    }

    exec_ok(&executor, "INSERT INTO vacuum_items VALUES (4, 'after')").await;
    let results = executor.execute_sql("VACUUM").await.unwrap();
    match &results[0] {
        QueryResult::Success { message } => {
            assert!(message.contains("VACUUM completed"));
            assert!(message.contains("compaction completed"));
        }
        other => panic!("Expected VACUUM success, got {:?}", other),
    }

    let (_, rows) = query(&executor, "SELECT COUNT(*) FROM vacuum_items").await;
    assert_eq!(rows, vec![vec![Value::Integer(4)]]);

    cleanup_storage_dir(&data_dir);
}

#[tokio::test]
async fn test_vacuum_rejects_table_specific_syntax() {
    let (executor, _fusion, data_dir) = setup_fusion_storage("vacuum_table_specific").await;

    let err = executor
        .execute_sql("VACUUM vacuum_items")
        .await
        .unwrap_err();
    assert!(format!("{:?}", err).contains("Table-specific VACUUM is not supported yet"));

    cleanup_storage_dir(&data_dir);
}

#[tokio::test]
async fn test_unsupported_statement_returns_error() {
    let (executor, wal) = setup().await;

    let err = executor
        .execute_sql("CREATE DATABASE unsupported_db")
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("Unsupported SQL statement"));

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
async fn test_explain_order_by_secondary_btree_limit_index_scan() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_order_idx (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_explain_order_idx_score ON explain_order_idx (score)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "EXPLAIN SELECT id, score FROM explain_order_idx ORDER BY score ASC LIMIT 2",
    )
    .await;
    assert_eq!(cols, vec!["EXPLAIN"]);
    assert_eq!(rows.len(), 1);
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Index Scan using ordered secondary BTree"));
        assert!(plan.contains("ORDER BY/LIMIT"));
        assert!(plan.contains("score"));
        assert!(plan.contains("ASC"));
    } else {
        panic!("expected explain text");
    }

    let (cols, rows) = query(
        &executor,
        "EXPLAIN SELECT id, score FROM explain_order_idx ORDER BY score DESC LIMIT 2",
    )
    .await;
    assert_eq!(cols, vec!["EXPLAIN"]);
    assert_eq!(rows.len(), 1);
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Index Scan using ordered secondary BTree"));
        assert!(plan.contains("ORDER BY/LIMIT"));
        assert!(plan.contains("score"));
        assert!(plan.contains("DESC"));
    } else {
        panic!("expected explain text");
    }
    cleanup(&wal);
}

#[tokio::test]
async fn test_explain_order_by_composite_btree_limit_index_scan() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_order_comp (
            id INTEGER PRIMARY KEY,
            host_id INTEGER NOT NULL,
            ts INTEGER NOT NULL,
            payload TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_explain_order_comp_host_ts ON explain_order_comp (host_id, ts)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "EXPLAIN SELECT id, ts FROM explain_order_comp
         WHERE host_id = 1 AND ts >= 1000
         ORDER BY ts ASC LIMIT 2",
    )
    .await;
    assert_eq!(cols, vec!["EXPLAIN"]);
    assert_eq!(rows.len(), 1);
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Index Scan using ordered composite BTree"));
        assert!(plan.contains("idx_explain_order_comp_host_ts"));
        assert!(plan.contains("ORDER BY/LIMIT"));
        assert!(plan.contains("rows <= 2"));
        assert!(plan.contains("ts"));
        assert!(plan.contains("ASC"));
    } else {
        panic!("expected explain text");
    }

    let (_, rows) = query(
        &executor,
        "EXPLAIN SELECT id, ts FROM explain_order_comp
         WHERE host_id = 1 AND ts >= 1000
         ORDER BY ts DESC LIMIT 2",
    )
    .await;
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Index Scan using ordered composite BTree"));
        assert!(plan.contains("idx_explain_order_comp_host_ts"));
        assert!(plan.contains("ORDER BY/LIMIT"));
        assert!(plan.contains("rows <= 2"));
        assert!(plan.contains("DESC"));
    } else {
        panic!("expected explain text");
    }

    let (_, rows) = query(
        &executor,
        "EXPLAIN SELECT id, ts FROM explain_order_comp
         WHERE host_id = 1 AND ts >= 1000 AND payload = 'hot'
         ORDER BY ts DESC LIMIT 2",
    )
    .await;
    if let Value::String(plan) = &rows[0][0] {
        assert!(!plan.contains("ordered composite BTree"));
    } else {
        panic!("expected explain text");
    }
    cleanup(&wal);
}

#[tokio::test]
async fn test_explain_distinct_secondary_btree_index_scan() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_distinct_idx (
            id INTEGER PRIMARY KEY,
            k INTEGER NOT NULL,
            payload TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_explain_distinct_idx_k ON explain_distinct_idx (k)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "EXPLAIN SELECT DISTINCT k FROM explain_distinct_idx",
    )
    .await;
    assert_eq!(cols, vec!["EXPLAIN"]);
    assert_eq!(rows.len(), 1);
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Index Scan using distinct secondary BTree"));
        assert!(plan.contains("DISTINCT loose key seek"));
        assert!(plan.contains("k"));
        assert!(!plan.contains("Access Path: Full Table Scan"));
    } else {
        panic!("expected explain text");
    }

    let (cols, rows) = query(
        &executor,
        "EXPLAIN SELECT COUNT(DISTINCT k) FROM explain_distinct_idx",
    )
    .await;
    assert_eq!(cols, vec!["EXPLAIN"]);
    assert_eq!(rows.len(), 1);
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Index Scan using distinct secondary BTree"));
        assert!(plan.contains("COUNT DISTINCT loose key seek"));
        assert!(plan.contains("k"));
        assert!(!plan.contains("Access Path: Full Table Scan"));
    } else {
        panic!("expected explain text");
    }

    let (_, rows) = query(
        &executor,
        "EXPLAIN SELECT DISTINCT payload FROM explain_distinct_idx",
    )
    .await;
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Access Path: Full Table Scan"));
        assert!(!plan.contains("distinct secondary BTree"));
    } else {
        panic!("expected explain text");
    }

    let (_, rows) = query(
        &executor,
        "EXPLAIN SELECT DISTINCT k FROM explain_distinct_idx WHERE payload = 'x'",
    )
    .await;
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Access Path: Full Table Scan"));
        assert!(!plan.contains("distinct secondary BTree"));
    } else {
        panic!("expected explain text");
    }

    cleanup(&wal);
}

#[tokio::test]
async fn test_explain_group_by_secondary_btree_index_scan() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_group_idx (
            id INTEGER PRIMARY KEY,
            k INTEGER NOT NULL,
            nullable_k INTEGER,
            payload TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_explain_group_idx_k ON explain_group_idx (k)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_explain_group_idx_nullable_k ON explain_group_idx (nullable_k)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "EXPLAIN SELECT k, COUNT(*) FROM explain_group_idx GROUP BY k",
    )
    .await;
    assert_eq!(cols, vec!["EXPLAIN"]);
    assert_eq!(rows.len(), 1);
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Index Scan using group secondary BTree"));
        assert!(plan.contains("GROUP BY COUNT summary index"));
        assert!(plan.contains("k"));
        assert!(!plan.contains("Access Path: Full Table Scan"));
    } else {
        panic!("expected explain text");
    }

    exec_ok(
        &executor,
        "INSERT INTO explain_group_idx VALUES
            (1, 1, 1, 'a'),
            (2, 1, 1, 'b'),
            (3, 2, 2, 'c')",
    )
    .await;
    exec_ok(
        &executor,
        "ANALYZE TABLE explain_group_idx COMPUTE STATISTICS",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "EXPLAIN SELECT k, COUNT(*) FROM explain_group_idx GROUP BY k",
    )
    .await;
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Index Scan using group secondary BTree"));
        assert!(plan.contains("GROUP BY COUNT summary index"));
        assert!(!plan.contains("Access Path: Full Table Scan"));
    } else {
        panic!("expected explain text");
    }

    let (_, rows) = query(
        &executor,
        "EXPLAIN SELECT nullable_k, COUNT(*) FROM explain_group_idx GROUP BY nullable_k",
    )
    .await;
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Access Path: Full Table Scan"));
        assert!(!plan.contains("group secondary BTree"));
    } else {
        panic!("expected explain text");
    }

    let (_, rows) = query(
        &executor,
        "EXPLAIN SELECT k, COUNT(*) FROM explain_group_idx WHERE payload = 'x' GROUP BY k",
    )
    .await;
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Access Path: Full Table Scan"));
        assert!(!plan.contains("group secondary BTree"));
    } else {
        panic!("expected explain text");
    }

    cleanup(&wal);
}

#[tokio::test]
async fn test_explain_order_by_secondary_btree_limit_nullable_fallback() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_order_nullable (id INTEGER PRIMARY KEY, score INTEGER, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_explain_order_nullable_score ON explain_order_nullable (score)",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "EXPLAIN SELECT id FROM explain_order_nullable ORDER BY score ASC LIMIT 2",
    )
    .await;
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Full Table Scan"));
        assert!(!plan.contains("ordered secondary BTree"));
    } else {
        panic!("expected explain text");
    }

    let (_, rows) = query(
        &executor,
        "EXPLAIN SELECT id FROM explain_order_nullable ORDER BY score DESC LIMIT 2",
    )
    .await;
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Full Table Scan"));
        assert!(!plan.contains("ordered secondary BTree"));
    } else {
        panic!("expected explain text");
    }
    cleanup(&wal);
}

#[tokio::test]
async fn test_explain_order_by_secondary_btree_limit_alias_fallback() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_order_alias (id INTEGER PRIMARY KEY, score INTEGER NOT NULL)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_explain_order_alias_score ON explain_order_alias (score)",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "EXPLAIN SELECT id AS score FROM explain_order_alias ORDER BY score ASC LIMIT 2",
    )
    .await;
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Full Table Scan"));
        assert!(!plan.contains("ordered secondary BTree"));
    } else {
        panic!("expected explain text");
    }

    let (_, rows) = query(
        &executor,
        "EXPLAIN SELECT id AS score FROM explain_order_alias ORDER BY score DESC LIMIT 2",
    )
    .await;
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Full Table Scan"));
        assert!(!plan.contains("ordered secondary BTree"));
    } else {
        panic!("expected explain text");
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
async fn test_explain_conjunctive_primary_key_range_scan() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_range_and (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    let (cols, rows) = query(
        &executor,
        "EXPLAIN SELECT * FROM explain_range_and WHERE id >= 10 AND id < 20",
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
async fn test_explain_analyze_includes_actual_rows_and_q_error() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_analyze_stats (id INTEGER PRIMARY KEY, category TEXT, qty INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO explain_analyze_stats VALUES
            (1, 'book', 5),
            (2, 'book', 7),
            (3, 'toy', NULL)",
    )
    .await;
    exec_ok(
        &executor,
        "ANALYZE TABLE explain_analyze_stats COMPUTE STATISTICS",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "EXPLAIN ANALYZE SELECT id FROM explain_analyze_stats WHERE category = 'book' ORDER BY id",
    )
    .await;
    assert_eq!(cols, vec!["EXPLAIN ANALYZE"]);
    assert_eq!(rows.len(), 1);
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Planning Time: "));
        assert!(plan.contains("Execution Time: "));
        assert!(plan.contains("Actual Rows: 2"));
        assert!(plan.contains("Estimate Rows: 2"));
        assert!(plan.contains("Q-Error: 1.00"));
        assert!(plan.contains("Plan:\nSELECT"));
        assert!(plan.contains("Estimate: rows=2, cost="));
    } else {
        panic!("expected explain analyze text");
    }

    let (_, rows) = query(
        &executor,
        "EXPLAIN ANALYZE SELECT id FROM explain_analyze_stats WHERE category = 'book' ORDER BY id LIMIT 1",
    )
    .await;
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Actual Rows: 1"));
        assert!(plan.contains("Estimate Rows: 1"));
        assert!(plan.contains("Q-Error: 1.00"));
    } else {
        panic!("expected explain analyze text");
    }

    let (_, rows) = query(
        &executor,
        "EXPLAIN ANALYZE SELECT id FROM explain_analyze_stats WHERE category = 'missing'",
    )
    .await;
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Actual Rows: 0"));
        assert!(plan.contains("Estimate Rows: 2"));
        assert!(plan.contains("Q-Error: inf"));
    } else {
        panic!("expected explain analyze text");
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
        assert!(plan.contains("Join Estimate: rows=8"), "{plan}");
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

    let (cols, rows) = query(
        &executor,
        "EXPLAIN ANALYZE SELECT *
           FROM explain_join_big, explain_join_mid, explain_join_small
          WHERE explain_join_big.join_key = explain_join_mid.join_key
            AND explain_join_mid.join_key = explain_join_small.join_key
            AND explain_join_big.join_key = explain_join_small.join_key",
    )
    .await;

    assert_eq!(cols, vec!["EXPLAIN ANALYZE"]);
    assert_eq!(rows.len(), 1);
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Actual Rows: 8"), "{plan}");
        assert!(plan.contains("Estimate Rows: 8"), "{plan}");
        assert!(plan.contains("Q-Error: 1.00"), "{plan}");
    } else {
        panic!("expected explain analyze text");
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

#[tokio::test]
async fn test_explain_inner_join_chain_uses_analyze_estimates() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_inner_join_big (id INTEGER PRIMARY KEY, join_key INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_inner_join_mid (id INTEGER PRIMARY KEY, join_key INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE explain_inner_join_small (id INTEGER PRIMARY KEY, join_key INTEGER)",
    )
    .await;

    exec_ok(
        &executor,
        "INSERT INTO explain_inner_join_big VALUES (1, 1), (2, 1), (3, 1), (4, 1)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO explain_inner_join_mid VALUES (1, 1), (2, 1)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO explain_inner_join_small VALUES (1, 1)",
    )
    .await;
    exec_ok(
        &executor,
        "ANALYZE TABLE explain_inner_join_big COMPUTE STATISTICS",
    )
    .await;
    exec_ok(
        &executor,
        "ANALYZE TABLE explain_inner_join_mid COMPUTE STATISTICS",
    )
    .await;
    exec_ok(
        &executor,
        "ANALYZE TABLE explain_inner_join_small COMPUTE STATISTICS",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "EXPLAIN SELECT *
           FROM explain_inner_join_big
           INNER JOIN explain_inner_join_mid
             ON explain_inner_join_big.join_key = explain_inner_join_mid.join_key
           INNER JOIN explain_inner_join_small
             ON explain_inner_join_mid.join_key = explain_inner_join_small.join_key
          WHERE explain_inner_join_small.join_key = 1",
    )
    .await;

    assert_eq!(cols, vec!["EXPLAIN"]);
    assert_eq!(rows.len(), 1);
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Join Order:"));
        assert!(plan.contains("Join Estimate: rows="));
        assert!(plan.contains("Join Estimate: rows=8"), "{plan}");
        let small = plan
            .find("explain_inner_join_small(rows=1)")
            .expect("small table estimate missing");
        let mid = plan
            .find("explain_inner_join_mid(rows=2)")
            .expect("mid table estimate missing");
        let big = plan
            .find("explain_inner_join_big(rows=4)")
            .expect("big table estimate missing");
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
async fn test_alter_table_rejects_single_column_include_index_dependencies() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE include_deps (
            id INTEGER PRIMARY KEY,
            score INTEGER,
            payload TEXT,
            extra TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_include_deps_score ON include_deps (score) INCLUDE (payload)",
    )
    .await;

    let drop_payload = executor
        .execute_sql("ALTER TABLE include_deps DROP COLUMN payload")
        .await;
    assert!(drop_payload.is_err());
    assert!(format!("{}", drop_payload.unwrap_err()).contains("BTree index"));

    let drop_score = executor
        .execute_sql("ALTER TABLE include_deps DROP COLUMN score")
        .await;
    assert!(drop_score.is_err());
    assert!(format!("{}", drop_score.unwrap_err()).contains("BTree index"));

    let rename_payload = executor
        .execute_sql("ALTER TABLE include_deps RENAME COLUMN payload TO payload_new")
        .await;
    assert!(rename_payload.is_err());
    assert!(format!("{}", rename_payload.unwrap_err()).contains("BTree index"));

    exec_ok(&executor, "ALTER TABLE include_deps DROP COLUMN extra").await;
    let (cols, _) = query(&executor, "SELECT * FROM include_deps").await;
    assert_eq!(cols, vec!["id", "score", "payload"]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_alter_table_drop_column_rewrites_storage_truth() {
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

    // Rewrite the stored bytes out of band: the DROP COLUMN row rewrite must
    // work from the CURRENT storage bytes, never from stale cached rows.
    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name, age) in [
            (1_i64, "Alice-rewritten", 30_i64),
            (2_i64, "Bob-rewritten", 25_i64),
        ] {
            let updated_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(name.to_string()),
                Value::Integer(age),
            ]);

            let key = format!(
                "data:drop_cache:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &updated_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    exec_ok(&executor, "ALTER TABLE drop_cache DROP COLUMN age").await;

    let (cols, rows) = query(&executor, "SELECT * FROM drop_cache").await;
    assert_eq!(cols, vec!["id", "name"]);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::String("Alice-rewritten".to_string())
            ],
            vec![
                Value::Integer(2),
                Value::String("Bob-rewritten".to_string())
            ]
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
async fn test_alter_table_supports_quoted_delimiter_columns() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE alter_quoted_columns (id INTEGER PRIMARY KEY)",
    )
    .await;
    exec_ok(
        &executor,
        "ALTER TABLE alter_quoted_columns ADD COLUMN \"payload:value\" TEXT",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO alter_quoted_columns VALUES (1, 'before')",
    )
    .await;
    exec_ok(
        &executor,
        "ALTER TABLE alter_quoted_columns RENAME COLUMN \"payload:value\" TO \"payload,value\"",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT \"payload,value\" FROM alter_quoted_columns WHERE id = 1",
    )
    .await;
    assert_eq!(cols, vec!["payload,value"]);
    assert_eq!(rows, vec![vec![Value::String("before".to_string())]]);

    exec_ok(
        &executor,
        "ALTER TABLE alter_quoted_columns DROP COLUMN \"payload,value\"",
    )
    .await;
    let (cols, _) = query(&executor, "SELECT * FROM alter_quoted_columns").await;
    assert_eq!(cols, vec!["id"]);
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
async fn test_truncate_preserves_foreign_key_integrity() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE trunc_parent (id INTEGER PRIMARY KEY)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE trunc_child (\
             id INTEGER PRIMARY KEY, \
             parent_id INTEGER REFERENCES trunc_parent(id)\
         )",
    )
    .await;
    exec_ok(&executor, "INSERT INTO trunc_parent VALUES (1)").await;
    exec_ok(&executor, "INSERT INTO trunc_child VALUES (10, 1)").await;

    let error = executor
        .execute_sql("TRUNCATE TABLE trunc_parent")
        .await
        .expect_err("a referenced parent must not be truncated alone");
    assert!(
        error.to_string().contains("FOREIGN KEY") && error.to_string().contains("trunc_child"),
        "unexpected truncate error: {error}"
    );
    assert_eq!(
        query(&executor, "SELECT * FROM trunc_parent").await.1.len(),
        1
    );
    assert_eq!(
        query(&executor, "SELECT * FROM trunc_child").await.1.len(),
        1
    );

    exec_ok(&executor, "TRUNCATE TABLE trunc_child, trunc_parent").await;
    assert!(query(&executor, "SELECT * FROM trunc_parent")
        .await
        .1
        .is_empty());
    assert!(query(&executor, "SELECT * FROM trunc_child")
        .await
        .1
        .is_empty());
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
