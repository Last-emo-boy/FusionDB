use fusiondb::common::Value;
use fusiondb::config::StorageConfig;
use fusiondb::execution::{Executor, QueryResult};
use fusiondb::storage::{memory::MemoryStorage, FusionStorage, Storage};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

fn encoded_row_id(value: i64) -> String {
    fusiondb::common::encoding::encode_i64_comparable(value)
}

fn corrupt_only_encoded_column(row: &mut [u8], column_index: usize, column_count: usize) {
    let off_pos = 2 + column_index * 4;
    let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
    let end = if column_index + 1 < column_count {
        let next_off_pos = off_pos + 4;
        u32::from_le_bytes(row[next_off_pos..next_off_pos + 4].try_into().unwrap()) as usize
    } else {
        row.len()
    };
    for byte in &mut row[start..end] {
        *byte = 0xff;
    }
}

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

fn cleanup_storage_dir(path: &std::path::Path) {
    let _ = std::fs::remove_dir_all(path);
}

#[tokio::test]
async fn test_unbounded_fusion_full_scan_uses_no_fill_cache() {
    let (executor, fusion, data_dir) = setup_fusion_storage("sql_full_scan_no_fill").await;
    exec_ok(
        &executor,
        "CREATE TABLE sql_full_scan_no_fill (id INTEGER PRIMARY KEY, payload TEXT)",
    )
    .await;
    let payload = "x".repeat(512);
    for id in 0..64 {
        exec_ok(
            &executor,
            &format!("INSERT INTO sql_full_scan_no_fill VALUES ({id}, '{payload}')"),
        )
        .await;
    }
    fusion.create_snapshot_now().await.unwrap();

    let metrics = &fusiondb::monitor::GLOBAL_METRICS;
    let skips_before = metrics.block_cache_fill_skip_count.load(Relaxed);
    let (cols, rows) = query(&executor, "SELECT payload FROM sql_full_scan_no_fill").await;
    assert_eq!(cols, vec!["payload"]);
    assert_eq!(rows.len(), 64);
    let skip_delta = metrics
        .block_cache_fill_skip_count
        .load(Relaxed)
        .saturating_sub(skips_before);
    assert!(
        skip_delta > 0,
        "unbounded Fusion full scan should use no-fill cache reads"
    );

    cleanup_storage_dir(&data_dir);
}

#[tokio::test]
async fn test_analyze_and_create_index_backfill_use_no_fill_cache() {
    let (executor, fusion, data_dir) = setup_fusion_storage("sql_maintenance_no_fill").await;
    exec_ok(
        &executor,
        "CREATE TABLE sql_maintenance_no_fill (id INTEGER PRIMARY KEY, bucket INTEGER, payload TEXT)",
    )
    .await;
    let payload = "y".repeat(512);
    for id in 0..64 {
        let bucket = id % 8;
        exec_ok(
            &executor,
            &format!("INSERT INTO sql_maintenance_no_fill VALUES ({id}, {bucket}, '{payload}')"),
        )
        .await;
    }
    fusion.create_snapshot_now().await.unwrap();

    let metrics = &fusiondb::monitor::GLOBAL_METRICS;
    let analyze_skips_before = metrics.block_cache_fill_skip_count.load(Relaxed);
    let msg = exec_ok(
        &executor,
        "ANALYZE TABLE sql_maintenance_no_fill COMPUTE STATISTICS",
    )
    .await;
    assert!(msg.contains("Analyzed table sql_maintenance_no_fill"));
    let analyze_skip_delta = metrics
        .block_cache_fill_skip_count
        .load(Relaxed)
        .saturating_sub(analyze_skips_before);
    assert!(
        analyze_skip_delta > 0,
        "ANALYZE should use no-fill cache reads for Fusion SSTable scans"
    );

    let create_index_skips_before = metrics.block_cache_fill_skip_count.load(Relaxed);
    let msg = exec_ok(
        &executor,
        "CREATE INDEX idx_sql_maintenance_no_fill_bucket ON sql_maintenance_no_fill (bucket)",
    )
    .await;
    assert!(msg.contains("indexed 64 rows"));
    let create_index_skip_delta = metrics
        .block_cache_fill_skip_count
        .load(Relaxed)
        .saturating_sub(create_index_skips_before);
    assert!(
        create_index_skip_delta > 0,
        "CREATE INDEX backfill should use no-fill cache reads for Fusion SSTable scans"
    );

    let (cols, rows) = query(
        &executor,
        "SELECT id FROM sql_maintenance_no_fill WHERE bucket = 3",
    )
    .await;
    assert_eq!(cols, vec!["id"]);
    assert_eq!(rows.len(), 8);

    cleanup_storage_dir(&data_dir);
}

#[tokio::test]
async fn test_full_table_scan_row_cache_tracks_storage_bytes() {
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

    // Rewrite the stored bytes out of band: the row cache must notice the
    // byte change and decode the new bytes instead of serving the stale row.
    let updated_row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(1),
        Value::String("Alice-rewritten".to_string()),
    ]);

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let key = format!(
            "data:full_scan_cache:{}",
            fusiondb::common::encoding::encode_i64_comparable(1)
        );
        txn.put(key.as_bytes(), &updated_row).await.unwrap();
        txn.commit().await.unwrap();
    }

    let (_, rows) = query(&executor, "SELECT * FROM full_scan_cache").await;
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(1),
            Value::String("Alice-rewritten".to_string())
        ]]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_full_table_projection_row_cache_tracks_storage_bytes() {
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

    // Rewrite the stored bytes out of band: the projected read must decode
    // the new bytes instead of serving the stale cached full rows.
    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name, payload) in [
            (1_i64, "Alice-rewritten", "a"),
            (2_i64, "Bob-rewritten", "b"),
        ] {
            let updated_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(name.to_string()),
                Value::String(payload.to_string()),
            ]);

            let key = format!(
                "data:full_project_cache:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &updated_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(&executor, "SELECT name FROM full_project_cache").await;
    assert_eq!(cols, vec!["name"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::String("Alice-rewritten".to_string())],
            vec![Value::String("Bob-rewritten".to_string())]
        ]
    );
    cleanup(&wal_path);
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
async fn test_primary_key_range_row_cache_tracks_storage_bytes() {
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

    // Rewrite the stored bytes out of band: the range scan must decode the
    // new bytes instead of serving the stale cached rows.
    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name) in [(1_i64, "Alice-rewritten"), (2_i64, "Bob-rewritten")] {
            let updated_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(name.to_string()),
            ]);

            let key = format!(
                "data:range_cache:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &updated_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (_, rows) = query(&executor, "SELECT * FROM range_cache WHERE id > 0").await;
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
async fn test_primary_key_range_projection_row_cache_tracks_storage_bytes() {
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

    // Rewrite the stored bytes out of band: the projected range read must
    // decode the new bytes instead of serving the stale cached full rows.
    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name, payload) in [
            (1_i64, "Alice-rewritten", "a"),
            (2_i64, "Bob-rewritten", "b"),
        ] {
            let updated_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(name.to_string()),
                Value::String(payload.to_string()),
            ]);

            let key = format!(
                "data:range_project_cache:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &updated_row).await.unwrap();
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
            vec![Value::String("Alice-rewritten".to_string())],
            vec![Value::String("Bob-rewritten".to_string())]
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_select_order_by_primary_key_limit_offset() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE pk_order_window (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO pk_order_window VALUES (5, 50), (1, 10), (4, 40), (2, 20), (3, 30)",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT id, val FROM pk_order_window ORDER BY id ASC LIMIT 2 OFFSET 1",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(2), Value::Integer(20)],
            vec![Value::Integer(3), Value::Integer(30)]
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_primary_key_range_order_limit_pushdown_skips_rows_after_window() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE range_order_limit (id INTEGER PRIMARY KEY, val TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO range_order_limit VALUES (1, 'one'), (2, 'two'), (3, 'three')",
    )
    .await;

    let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(3),
        Value::String("three".to_string()),
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
            "data:range_order_limit:{}",
            fusiondb::common::encoding::encode_i64_comparable(3)
        );
        txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT id, val FROM range_order_limit WHERE id >= 1 ORDER BY id LIMIT 2",
    )
    .await;
    assert_eq!(cols, vec!["id", "val"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::String("one".to_string())],
            vec![Value::Integer(2), Value::String("two".to_string())]
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_primary_key_range_order_limit_offset_pushdown() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE range_order_offset (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO range_order_offset VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT id, val FROM range_order_offset WHERE id >= 1 ORDER BY id LIMIT 2 OFFSET 1",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(2), Value::Integer(20)],
            vec![Value::Integer(3), Value::Integer(30)]
        ]
    );
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
async fn test_create_integer_btree_index_after_load_uses_comparable_keys() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE int_idx (id INTEGER PRIMARY KEY, bucket INTEGER, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO int_idx VALUES (1, 7, 'a'), (2, 8, 'b'), (3, 7, 'c')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_int_idx_bucket ON int_idx (bucket)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT payload FROM int_idx WHERE bucket = 7 ORDER BY id",
    )
    .await;
    assert_eq!(cols, vec!["payload"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::String("a".to_string())],
            vec![Value::String("c".to_string())]
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_secondary_btree_between_skips_outside_range_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sec_range (id INTEGER PRIMARY KEY, score INTEGER, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_range VALUES
            (1, 10, 'bad-low'),
            (2, 20, 'ok-20'),
            (3, 30, 'ok-30'),
            (4, 40, 'bad-high')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_range_score ON sec_range (score)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, score, payload) in [(1, 10, "bad-low"), (4, 40, "bad-high")] {
            let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::Integer(score),
                Value::String(payload.to_string()),
            ]);
            let corrupt_col_idx = 2usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start =
                u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut corrupt_row[start..] {
                *byte = 0xff;
            }
            let key = format!("data:sec_range:{}", encoded_row_id(id));
            txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT payload FROM sec_range WHERE score BETWEEN 20 AND 30 ORDER BY id",
    )
    .await;
    assert_eq!(cols, vec!["payload"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::String("ok-20".to_string())],
            vec![Value::String("ok-30".to_string())],
        ]
    );

    let (_, rows) = query(
        &executor,
        "SELECT id FROM sec_range WHERE score >= 20 AND score < 40 ORDER BY id",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_secondary_btree_range_order_limit_skips_later_match_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sec_range_top (id INTEGER PRIMARY KEY, score INTEGER, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_range_top VALUES
            (1, 10, 'first'),
            (2, 20, 'second'),
            (3, 30, 'bad-third')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_range_top_score ON sec_range_top (score)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(3),
            Value::Integer(30),
            Value::String("bad-third".to_string()),
        ]);
        let corrupt_col_idx = 2usize;
        let off_pos = 2 + corrupt_col_idx * 4;
        let start =
            u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
        for byte in &mut corrupt_row[start..] {
            *byte = 0xff;
        }
        let key = format!("data:sec_range_top:{}", encoded_row_id(3));
        txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT payload FROM sec_range_top WHERE score >= 10 ORDER BY score LIMIT 2",
    )
    .await;
    assert_eq!(cols, vec!["payload"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::String("first".to_string())],
            vec![Value::String("second".to_string())],
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_secondary_btree_range_order_desc_limit_skips_lower_match_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sec_range_desc_top (id INTEGER PRIMARY KEY, score INTEGER, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_range_desc_top VALUES
            (1, 10, 'bad-low'),
            (2, 20, 'second'),
            (3, 30, 'third')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_range_desc_top_score ON sec_range_desc_top (score)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(1),
            Value::Integer(10),
            Value::String("bad-low".to_string()),
        ]);
        corrupt_only_encoded_column(&mut corrupt_row, 2, 3);
        let key = format!("data:sec_range_desc_top:{}", encoded_row_id(1));
        txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT payload FROM sec_range_desc_top WHERE score <= 30 ORDER BY score DESC LIMIT 2",
    )
    .await;
    assert_eq!(cols, vec!["payload"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::String("third".to_string())],
            vec![Value::String("second".to_string())],
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_secondary_btree_order_by_limit_offset_uses_index_order() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sec_order_topk (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_order_topk VALUES
            (1, 50, 'fifty'),
            (2, 10, 'ten'),
            (3, 30, 'thirty'),
            (4, 20, 'twenty')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_order_topk_score ON sec_order_topk (score)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT id, score FROM sec_order_topk ORDER BY score ASC LIMIT 2 OFFSET 1",
    )
    .await;
    assert_eq!(cols, vec!["id", "score"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(4), Value::Integer(20)],
            vec![Value::Integer(3), Value::Integer(30)],
        ]
    );

    let (cols, rows) = query(
        &executor,
        "SELECT id, score FROM sec_order_topk ORDER BY score DESC LIMIT 2 OFFSET 1",
    )
    .await;
    assert_eq!(cols, vec!["id", "score"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(3), Value::Integer(30)],
            vec![Value::Integer(4), Value::Integer(20)],
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_ordered_topk_metrics_count_index_and_sort_paths() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE topk_metrics (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO topk_metrics VALUES
            (1, 50, 'first'),
            (2, 10, 'second'),
            (3, 30, 'third'),
            (4, 20, 'fourth')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_topk_metrics_score ON topk_metrics (score)",
    )
    .await;

    let metrics = &fusiondb::monitor::GLOBAL_METRICS;
    let scans_before = metrics.index_ordered_topk_scan_count.load(Relaxed);
    let visits_before = metrics.index_ordered_topk_entry_visit_count.load(Relaxed);
    let reverse_before = metrics.index_ordered_topk_reverse_scan_count.load(Relaxed);
    let index_only_before = metrics
        .index_ordered_topk_index_only_row_count
        .load(Relaxed);
    let base_fetch_before = metrics
        .index_ordered_topk_base_row_fetch_count
        .load(Relaxed);
    let sort_before = metrics.query_sort_fallback_count.load(Relaxed);

    let (_, rows) = query(
        &executor,
        "SELECT id FROM topk_metrics ORDER BY score ASC LIMIT 2",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(2)], vec![Value::Integer(4)]]);

    let (_, rows) = query(
        &executor,
        "SELECT id FROM topk_metrics ORDER BY score DESC LIMIT 2",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);

    let (_, rows) = query(
        &executor,
        "SELECT payload FROM topk_metrics ORDER BY score ASC LIMIT 2",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Value::String("second".to_string())],
            vec![Value::String("fourth".to_string())],
        ]
    );

    let (_, rows) = query(
        &executor,
        "SELECT id FROM topk_metrics ORDER BY score + 0 ASC LIMIT 2",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(2)], vec![Value::Integer(4)]]);

    assert!(
        metrics.index_ordered_topk_scan_count.load(Relaxed) >= scans_before + 3,
        "ordered Top-K scan counter should increase for covered and base-fetch index paths"
    );
    assert!(
        metrics.index_ordered_topk_entry_visit_count.load(Relaxed) >= visits_before + 6,
        "ordered Top-K entry visits should include limited covered and base-fetch entries"
    );
    assert!(
        metrics.index_ordered_topk_reverse_scan_count.load(Relaxed) >= reverse_before + 1,
        "DESC index path should count a reverse ordered Top-K scan"
    );
    assert!(
        metrics
            .index_ordered_topk_index_only_row_count
            .load(Relaxed)
            >= index_only_before + 4,
        "covered ordered Top-K rows should count as index-only materialization"
    );
    assert!(
        metrics
            .index_ordered_topk_base_row_fetch_count
            .load(Relaxed)
            >= base_fetch_before + 2,
        "non-covered ordered Top-K rows should count base-row fetches"
    );
    assert!(
        metrics.query_sort_fallback_count.load(Relaxed) >= sort_before + 1,
        "expression ORDER BY fallback should count a query sort"
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_secondary_btree_order_by_limit_include_covers_top_row() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sec_order_cover_topk (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_order_cover_topk VALUES
            (1, 10, 'first'),
            (2, 20, 'second'),
            (3, 30, 'third')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_order_cover_topk_score ON sec_order_cover_topk (score) INCLUDE (payload)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(1),
            Value::Integer(10),
            Value::String("first".to_string()),
        ]);
        corrupt_only_encoded_column(&mut corrupt_row, 2, 3);
        txn.put(
            format!("data:sec_order_cover_topk:{}", encoded_row_id(1)).as_bytes(),
            &corrupt_row,
        )
        .await
        .unwrap();

        let mut corrupt_desc_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(3),
            Value::Integer(30),
            Value::String("third".to_string()),
        ]);
        corrupt_only_encoded_column(&mut corrupt_desc_row, 2, 3);
        txn.put(
            format!("data:sec_order_cover_topk:{}", encoded_row_id(3)).as_bytes(),
            &corrupt_desc_row,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
    }

    let fresh_executor = Arc::new(Executor::new(storage.clone()));
    let (cols, rows) = query(
        &fresh_executor,
        "SELECT payload FROM sec_order_cover_topk ORDER BY score ASC LIMIT 1",
    )
    .await;
    assert_eq!(cols, vec!["payload"]);
    assert_eq!(rows, vec![vec![Value::String("first".to_string())]]);

    let (cols, rows) = query(
        &fresh_executor,
        "SELECT payload FROM sec_order_cover_topk ORDER BY score DESC LIMIT 1",
    )
    .await;
    assert_eq!(cols, vec!["payload"]);
    assert_eq!(rows, vec![vec![Value::String("third".to_string())]]);
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_secondary_btree_include_metadata_preserves_delimiter_identifiers() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE \"sec:cover,quoted\" (
            id INTEGER PRIMARY KEY,
            score INTEGER NOT NULL,
            payload TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO \"sec:cover,quoted\" VALUES
            (1, 10, 'ten'),
            (2, 20, 'twenty')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX \"idx:sec,cover\" ON \"sec:cover,quoted\" (score) INCLUDE (payload)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(1),
            Value::Integer(10),
            Value::String("ten".to_string()),
        ]);
        corrupt_only_encoded_column(&mut corrupt_row, 2, 3);
        txn.put(
            format!("data:\"sec:cover,quoted\":{}", encoded_row_id(1)).as_bytes(),
            &corrupt_row,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
    }

    let fresh_executor = Arc::new(Executor::new(storage.clone()));
    let metrics = &fusiondb::monitor::GLOBAL_METRICS;
    let scans_before = metrics.index_ordered_topk_scan_count.load(Relaxed);
    let (cols, rows) = query(
        &fresh_executor,
        "SELECT payload
         FROM \"sec:cover,quoted\"
         ORDER BY score ASC
         LIMIT 1",
    )
    .await;

    assert_eq!(cols, vec!["payload"]);
    assert_eq!(rows, vec![vec![Value::String("ten".to_string())]]);
    assert!(
        metrics.index_ordered_topk_scan_count.load(Relaxed) > scans_before,
        "quoted delimiter identifiers should still load the secondary covering index"
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_secondary_btree_include_supports_quoted_delimiter_columns() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sec_cover_quoted_columns (
            id INTEGER PRIMARY KEY,
            \"score:rank\" INTEGER NOT NULL,
            \"payload,value\" TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_cover_quoted_columns VALUES
            (1, 10, 'ten'),
            (2, 20, 'twenty')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX \"idx:quoted,column\"
         ON sec_cover_quoted_columns (\"score:rank\") INCLUDE (\"payload,value\")",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(1),
            Value::Integer(10),
            Value::String("ten".to_string()),
        ]);
        corrupt_only_encoded_column(&mut corrupt_row, 2, 3);
        txn.put(
            format!("data:sec_cover_quoted_columns:{}", encoded_row_id(1)).as_bytes(),
            &corrupt_row,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
    }

    let fresh_executor = Arc::new(Executor::new(storage.clone()));
    let metrics = &fusiondb::monitor::GLOBAL_METRICS;
    let scans_before = metrics.index_ordered_topk_scan_count.load(Relaxed);
    let (cols, rows) = query(
        &fresh_executor,
        "SELECT \"payload,value\"
         FROM sec_cover_quoted_columns
         ORDER BY \"score:rank\" ASC
         LIMIT 1",
    )
    .await;

    assert_eq!(cols, vec!["payload,value"]);
    assert_eq!(rows, vec![vec![Value::String("ten".to_string())]]);
    assert!(
        metrics.index_ordered_topk_scan_count.load(Relaxed) > scans_before,
        "quoted delimiter columns should still load the secondary covering index"
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_composite_include_metadata_preserves_delimiter_identifiers() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE \"comp:cover,quoted\" (
            id INTEGER PRIMARY KEY,
            host_id INTEGER NOT NULL,
            ts_val INTEGER NOT NULL,
            payload_text TEXT,
            metric_value INTEGER
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO \"comp:cover,quoted\" VALUES
            (1, 7, 10, 'first', 11),
            (2, 7, 20, 'second', 22)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX \"idx:comp,cover\" ON \"comp:cover,quoted\" (host_id, ts_val)
         INCLUDE (payload_text, metric_value)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(1),
            Value::Integer(7),
            Value::Integer(10),
            Value::String("first".to_string()),
            Value::Integer(11),
        ]);
        corrupt_only_encoded_column(&mut corrupt_row, 3, 5);
        corrupt_only_encoded_column(&mut corrupt_row, 4, 5);
        txn.put(
            format!("data:\"comp:cover,quoted\":{}", encoded_row_id(1)).as_bytes(),
            &corrupt_row,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
    }

    let fresh_executor = Arc::new(Executor::new(storage.clone()));
    let metrics = &fusiondb::monitor::GLOBAL_METRICS;
    let scans_before = metrics.index_ordered_topk_scan_count.load(Relaxed);
    let (cols, rows) = query(
        &fresh_executor,
        "SELECT payload_text, metric_value
         FROM \"comp:cover,quoted\"
         WHERE host_id = 7 AND ts_val >= 0
         ORDER BY ts_val ASC
         LIMIT 1",
    )
    .await;

    assert_eq!(cols, vec!["payload_text", "metric_value"]);
    assert_eq!(
        rows,
        vec![vec![Value::String("first".to_string()), Value::Integer(11)]]
    );
    assert!(
        metrics.index_ordered_topk_scan_count.load(Relaxed) > scans_before,
        "quoted delimiter identifiers should still load the composite ordered index"
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_secondary_btree_order_by_limit_residual_where_does_not_consume_limit() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sec_order_residual_topk (
            id INTEGER PRIMARY KEY,
            score INTEGER NOT NULL,
            payload TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_order_residual_topk VALUES
            (1, 10, 'skip'),
            (2, 20, 'target'),
            (3, 30, 'target')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_order_residual_topk_score ON sec_order_residual_topk (score)",
    )
    .await;

    let (_, explain_rows) = query(
        &executor,
        "EXPLAIN SELECT id, score FROM sec_order_residual_topk
         WHERE payload = 'target'
         ORDER BY score ASC
         LIMIT 1",
    )
    .await;
    if let Value::String(plan) = &explain_rows[0][0] {
        assert!(!plan.contains("ordered secondary BTree"));
    } else {
        panic!("expected explain text");
    }

    let (cols, rows) = query(
        &executor,
        "SELECT id, score FROM sec_order_residual_topk
         WHERE payload = 'target'
         ORDER BY score ASC
         LIMIT 1",
    )
    .await;
    assert_eq!(cols, vec!["id", "score"]);
    assert_eq!(rows, vec![vec![Value::Integer(2), Value::Integer(20)]]);
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_secondary_btree_range_order_by_limit_residual_where_keeps_scanning_candidates() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sec_range_residual_topk (
            id INTEGER PRIMARY KEY,
            score INTEGER NOT NULL,
            payload TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_range_residual_topk VALUES
            (1, 10, 'target'),
            (2, 20, 'target'),
            (3, 30, 'skip')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_range_residual_topk_score ON sec_range_residual_topk (score)",
    )
    .await;

    let (_, explain_rows) = query(
        &executor,
        "EXPLAIN SELECT id, score FROM sec_range_residual_topk
         WHERE score <= 30 AND payload = 'target'
         ORDER BY score DESC
         LIMIT 1",
    )
    .await;
    if let Value::String(plan) = &explain_rows[0][0] {
        assert!(!plan.contains("ordered secondary BTree"));
    } else {
        panic!("expected explain text");
    }

    let (cols, rows) = query(
        &executor,
        "SELECT id, score FROM sec_range_residual_topk
         WHERE score <= 30 AND payload = 'target'
         ORDER BY score DESC
         LIMIT 1",
    )
    .await;
    assert_eq!(cols, vec!["id", "score"]);
    assert_eq!(rows, vec![vec![Value::Integer(2), Value::Integer(20)]]);
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_secondary_btree_range_order_by_limit_respects_projection_alias() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sec_range_alias_topk (
            id INTEGER PRIMARY KEY,
            score INTEGER NOT NULL
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_range_alias_topk VALUES
            (1, 100),
            (2, 10),
            (3, 50)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_range_alias_topk_score ON sec_range_alias_topk (score)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT id AS score FROM sec_range_alias_topk
         WHERE score >= 0
         ORDER BY score ASC
         LIMIT 2",
    )
    .await;
    assert_eq!(cols, vec!["score"]);
    assert_eq!(rows, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);

    let (_, rows) = query(
        &executor,
        "SELECT id AS score FROM sec_range_alias_topk
         WHERE score >= 0
         ORDER BY score DESC
         LIMIT 2",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(3)], vec![Value::Integer(2)]]);
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_fusion_secondary_btree_desc_topk_skips_mvcc_tombstones() {
    let (executor, fusion, data_dir) = setup_fusion_storage("desc_topk_mvcc").await;
    exec_ok(
        &executor,
        "CREATE TABLE fusion_desc_topk (
            id INTEGER PRIMARY KEY,
            score INTEGER NOT NULL,
            payload TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO fusion_desc_topk VALUES
            (1, 10, 'ten'),
            (2, 20, 'twenty'),
            (3, 30, 'thirty'),
            (4, 40, 'forty'),
            (5, 50, 'fifty')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_fusion_desc_topk_score ON fusion_desc_topk (score)",
    )
    .await;
    fusion.create_snapshot_now().await.unwrap();

    exec_ok(&executor, "DELETE FROM fusion_desc_topk WHERE id = 5").await;
    exec_ok(
        &executor,
        "UPDATE fusion_desc_topk SET score = 35, payload = 'thirty-five' WHERE id = 2",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO fusion_desc_topk VALUES
            (6, 45, 'forty-five'),
            (7, 15, 'fifteen')",
    )
    .await;
    fusion.create_snapshot_now().await.unwrap();

    let (cols, rows) = query(
        &executor,
        "EXPLAIN SELECT id, score, payload FROM fusion_desc_topk ORDER BY score DESC LIMIT 5",
    )
    .await;
    assert_eq!(cols, vec!["EXPLAIN"]);
    assert_eq!(rows.len(), 1);
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Index Scan using ordered secondary BTree"));
        assert!(plan.contains("DESC"));
    } else {
        panic!("expected explain text");
    }

    let (cols, rows) = query(
        &executor,
        "SELECT id, score, payload FROM fusion_desc_topk ORDER BY score DESC LIMIT 5",
    )
    .await;
    assert_eq!(cols, vec!["id", "score", "payload"]);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(6),
                Value::Integer(45),
                Value::String("forty-five".to_string()),
            ],
            vec![
                Value::Integer(4),
                Value::Integer(40),
                Value::String("forty".to_string()),
            ],
            vec![
                Value::Integer(2),
                Value::Integer(35),
                Value::String("thirty-five".to_string()),
            ],
            vec![
                Value::Integer(3),
                Value::Integer(30),
                Value::String("thirty".to_string()),
            ],
            vec![
                Value::Integer(7),
                Value::Integer(15),
                Value::String("fifteen".to_string()),
            ],
        ]
    );

    let (_, rows) = query(
        &executor,
        "SELECT id, score FROM fusion_desc_topk
         WHERE score <= 20
         ORDER BY score DESC
         LIMIT 2",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(7), Value::Integer(15)],
            vec![Value::Integer(1), Value::Integer(10)],
        ]
    );
    cleanup_storage_dir(&data_dir);
}

#[tokio::test]
async fn test_fusion_secondary_btree_desc_topk_include_uses_visible_payload() {
    let (executor, fusion, data_dir) = setup_fusion_storage("desc_topk_include_mvcc").await;
    exec_ok(
        &executor,
        "CREATE TABLE fusion_desc_topk_cover (
            id INTEGER PRIMARY KEY,
            score INTEGER NOT NULL,
            payload TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO fusion_desc_topk_cover VALUES
            (1, 10, 'ten'),
            (2, 60, 'old-high'),
            (3, 40, 'forty'),
            (4, 55, 'delete-me')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_fusion_desc_topk_cover_score
         ON fusion_desc_topk_cover (score) INCLUDE (payload)",
    )
    .await;
    fusion.create_snapshot_now().await.unwrap();

    exec_ok(
        &executor,
        "UPDATE fusion_desc_topk_cover
         SET score = 50, payload = 'updated-high'
         WHERE id = 2",
    )
    .await;
    exec_ok(
        &executor,
        "UPDATE fusion_desc_topk_cover SET payload = 'forty-new' WHERE id = 3",
    )
    .await;
    exec_ok(&executor, "DELETE FROM fusion_desc_topk_cover WHERE id = 4").await;
    fusion.create_snapshot_now().await.unwrap();

    {
        let mut txn = fusion.begin_transaction().await.unwrap();
        for (id, score, payload) in [(2, 50, "updated-high"), (3, 40, "forty-new")] {
            let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::Integer(score),
                Value::String(payload.to_string()),
            ]);
            corrupt_only_encoded_column(&mut corrupt_row, 2, 3);
            txn.put(
                format!("data:fusion_desc_topk_cover:{}", encoded_row_id(id)).as_bytes(),
                &corrupt_row,
            )
            .await
            .unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "EXPLAIN SELECT id, payload FROM fusion_desc_topk_cover ORDER BY score DESC LIMIT 2",
    )
    .await;
    assert_eq!(cols, vec!["EXPLAIN"]);
    assert_eq!(rows.len(), 1);
    if let Value::String(plan) = &rows[0][0] {
        assert!(plan.contains("Index Scan using ordered secondary BTree"));
        assert!(plan.contains("DESC"));
    } else {
        panic!("expected explain text");
    }

    let (cols, rows) = query(
        &executor,
        "SELECT id, payload FROM fusion_desc_topk_cover ORDER BY score DESC LIMIT 2",
    )
    .await;
    assert_eq!(cols, vec!["id", "payload"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(2), Value::String("updated-high".to_string()),],
            vec![Value::Integer(3), Value::String("forty-new".to_string())],
        ]
    );
    cleanup_storage_dir(&data_dir);
}

#[tokio::test]
async fn test_secondary_btree_order_by_limit_boolean_uses_index_order_and_cover() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sec_order_bool_topk (id INTEGER PRIMARY KEY, flag BOOLEAN NOT NULL, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_order_bool_topk VALUES
            (1, true, 'one'),
            (2, false, 'two'),
            (3, true, 'three')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_order_bool_topk_flag ON sec_order_bool_topk (flag)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(2),
            Value::Boolean(false),
            Value::String("two".to_string()),
        ]);
        corrupt_only_encoded_column(&mut corrupt_row, 1, 3);
        txn.put(
            format!("data:sec_order_bool_topk:{}", encoded_row_id(2)).as_bytes(),
            &corrupt_row,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
    }

    let fresh_executor = Arc::new(Executor::new(storage.clone()));
    let (cols, rows) = query(
        &fresh_executor,
        "SELECT flag FROM sec_order_bool_topk ORDER BY flag ASC LIMIT 1",
    )
    .await;
    assert_eq!(cols, vec!["flag"]);
    assert_eq!(rows, vec![vec![Value::Boolean(false)]]);

    let (_, explain_rows) = query(
        &fresh_executor,
        "EXPLAIN SELECT flag FROM sec_order_bool_topk ORDER BY flag ASC LIMIT 1",
    )
    .await;
    if let Value::String(plan) = &explain_rows[0][0] {
        assert!(plan.contains("Index Scan using ordered secondary BTree"));
        assert!(plan.contains("flag"));
    } else {
        panic!("expected explain text");
    }

    cleanup(&wal_path);
}

#[tokio::test]
async fn test_secondary_btree_order_by_limit_temporal_aliases_use_index_cover() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sec_order_alias_topk (
            id INTEGER PRIMARY KEY,
            d DATE32 NOT NULL,
            ts TIMESTAMPTZ NOT NULL,
            span INTERVAL DAY NOT NULL
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_order_alias_topk VALUES
            (1, '2024-02-01', '2024-02-01 00:00:00+00', '1 days'),
            (2, '2024-01-01', '2024-03-01 00:00:00+00', '3 days'),
            (3, '2024-03-01', '2024-01-01 00:00:00+00', '2 days')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_order_alias_topk_d ON sec_order_alias_topk (d)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_order_alias_topk_ts ON sec_order_alias_topk (ts)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_order_alias_topk_span ON sec_order_alias_topk (span)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();

        let mut corrupt_date_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(2),
            Value::date_from_str("2024-01-01").unwrap(),
            Value::timestamp_from_str("2024-03-01 00:00:00+00").unwrap(),
            Value::interval_from_str("3 days").unwrap(),
        ]);
        corrupt_only_encoded_column(&mut corrupt_date_row, 1, 4);
        txn.put(
            format!("data:sec_order_alias_topk:{}", encoded_row_id(2)).as_bytes(),
            &corrupt_date_row,
        )
        .await
        .unwrap();

        let mut corrupt_timestamp_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(3),
            Value::date_from_str("2024-03-01").unwrap(),
            Value::timestamp_from_str("2024-01-01 00:00:00+00").unwrap(),
            Value::interval_from_str("2 days").unwrap(),
        ]);
        corrupt_only_encoded_column(&mut corrupt_timestamp_row, 2, 4);
        txn.put(
            format!("data:sec_order_alias_topk:{}", encoded_row_id(3)).as_bytes(),
            &corrupt_timestamp_row,
        )
        .await
        .unwrap();

        let mut corrupt_interval_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(1),
            Value::date_from_str("2024-02-01").unwrap(),
            Value::timestamp_from_str("2024-02-01 00:00:00+00").unwrap(),
            Value::interval_from_str("1 days").unwrap(),
        ]);
        corrupt_only_encoded_column(&mut corrupt_interval_row, 3, 4);
        txn.put(
            format!("data:sec_order_alias_topk:{}", encoded_row_id(1)).as_bytes(),
            &corrupt_interval_row,
        )
        .await
        .unwrap();

        txn.commit().await.unwrap();
    }

    let fresh_executor = Arc::new(Executor::new(storage.clone()));
    let (cols, rows) = query(
        &fresh_executor,
        "SELECT d FROM sec_order_alias_topk ORDER BY d ASC LIMIT 1",
    )
    .await;
    assert_eq!(cols, vec!["d"]);
    assert_eq!(
        rows,
        vec![vec![Value::date_from_str("2024-01-01").unwrap()]]
    );

    let (cols, rows) = query(
        &fresh_executor,
        "SELECT ts FROM sec_order_alias_topk ORDER BY ts ASC LIMIT 1",
    )
    .await;
    assert_eq!(cols, vec!["ts"]);
    assert_eq!(
        rows,
        vec![vec![
            Value::timestamp_from_str("2024-01-01 00:00:00+00").unwrap()
        ]]
    );

    let (cols, rows) = query(
        &fresh_executor,
        "SELECT span FROM sec_order_alias_topk ORDER BY span ASC LIMIT 1",
    )
    .await;
    assert_eq!(cols, vec!["span"]);
    assert_eq!(
        rows,
        vec![vec![Value::interval_from_str("1 days").unwrap()]]
    );

    let (_, explain_rows) = query(
        &fresh_executor,
        "EXPLAIN SELECT span FROM sec_order_alias_topk ORDER BY span ASC LIMIT 1",
    )
    .await;
    if let Value::String(plan) = &explain_rows[0][0] {
        assert!(plan.contains("Index Scan using ordered secondary BTree"));
        assert!(plan.contains("span"));
    } else {
        panic!("expected explain text");
    }

    cleanup(&wal_path);
}

#[tokio::test]
async fn test_secondary_btree_order_by_limit_falls_back_for_nullable_column() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sec_order_nullable_topk (id INTEGER PRIMARY KEY, score INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_order_nullable_topk VALUES (1, 10), (2, NULL), (3, 20)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_order_nullable_topk_score ON sec_order_nullable_topk (score)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT id FROM sec_order_nullable_topk ORDER BY score ASC LIMIT 1",
    )
    .await;
    assert_eq!(cols, vec!["id"]);
    assert_eq!(rows, vec![vec![Value::Integer(2)]]);
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_secondary_btree_index_only_covers_pk_and_index_column() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sec_cover (id INTEGER PRIMARY KEY, score INTEGER, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_cover VALUES (1, 10, 'ten'), (2, 20, 'twenty')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_cover_score ON sec_cover (score)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(2),
            Value::Integer(20),
            Value::String("twenty".to_string()),
        ]);
        corrupt_only_encoded_column(&mut corrupt_row, 1, 3);
        txn.put(
            format!("data:sec_cover:{}", encoded_row_id(2)).as_bytes(),
            &corrupt_row,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
    }

    let fresh_executor = Arc::new(Executor::new(storage.clone()));
    let (cols, rows) = query(
        &fresh_executor,
        "SELECT id, score FROM sec_cover WHERE score = 20",
    )
    .await;
    assert_eq!(cols, vec!["id", "score"]);
    assert_eq!(rows, vec![vec![Value::Integer(2), Value::Integer(20)]]);

    let (_, payload_rows) = query(
        &fresh_executor,
        "SELECT payload FROM sec_cover WHERE id = 2",
    )
    .await;
    assert_eq!(
        payload_rows,
        vec![vec![Value::String("twenty".to_string())]]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_secondary_btree_range_index_only_decodes_value_from_index_key() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sec_cover_range (id INTEGER PRIMARY KEY, score INTEGER, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_cover_range VALUES
            (1, 10, 'ten'),
            (2, 20, 'twenty'),
            (3, 30, 'thirty')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_cover_range_score ON sec_cover_range (score)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(2),
            Value::Integer(20),
            Value::String("twenty".to_string()),
        ]);
        corrupt_only_encoded_column(&mut corrupt_row, 1, 3);
        txn.put(
            format!("data:sec_cover_range:{}", encoded_row_id(2)).as_bytes(),
            &corrupt_row,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
    }

    let fresh_executor = Arc::new(Executor::new(storage.clone()));
    let (_, rows) = query(
        &fresh_executor,
        "SELECT id, score FROM sec_cover_range
         WHERE score BETWEEN 10 AND 20
         ORDER BY score
         LIMIT 2",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::Integer(10)],
            vec![Value::Integer(2), Value::Integer(20)],
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_secondary_btree_index_only_respects_update_and_delete() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE sec_cover_dml (id INTEGER PRIMARY KEY, score INTEGER, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_cover_dml VALUES (1, 10, 'old'), (2, 20, 'keep')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_cover_dml_score ON sec_cover_dml (score)",
    )
    .await;
    exec_ok(
        &executor,
        "UPDATE sec_cover_dml SET score = 30 WHERE id = 2",
    )
    .await;
    exec_ok(&executor, "DELETE FROM sec_cover_dml WHERE id = 1").await;

    let (_, rows) = query(
        &executor,
        "SELECT id, score FROM sec_cover_dml WHERE score IN (10, 20, 30) ORDER BY id",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(2), Value::Integer(30)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_secondary_btree_include_index_covers_payload_from_backfill() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sec_cover_include (id INTEGER PRIMARY KEY, score INTEGER, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_cover_include VALUES (1, 10, 'ten'), (2, 20, 'twenty')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_cover_include_score ON sec_cover_include (score) INCLUDE (payload)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(2),
            Value::Integer(20),
            Value::String("twenty".to_string()),
        ]);
        corrupt_only_encoded_column(&mut corrupt_row, 2, 3);
        txn.put(
            format!("data:sec_cover_include:{}", encoded_row_id(2)).as_bytes(),
            &corrupt_row,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
    }

    let fresh_executor = Arc::new(Executor::new(storage.clone()));
    let (cols, rows) = query(
        &fresh_executor,
        "SELECT id, payload FROM sec_cover_include WHERE score = 20",
    )
    .await;
    assert_eq!(cols, vec!["id", "payload"]);
    assert_eq!(
        rows,
        vec![vec![Value::Integer(2), Value::String("twenty".to_string())]]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_secondary_btree_include_legacy_s2_metadata_covers_payload() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sec_cover_include_legacy (
            id INTEGER PRIMARY KEY,
            score INTEGER,
            payload TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_cover_include_legacy VALUES (1, 10, 'ten'), (2, 20, 'twenty')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_cover_include_legacy_score
         ON sec_cover_include_legacy (score) INCLUDE (payload)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(
            b"index_meta:idx_sec_cover_include_legacy_score",
            b"s2:sec_cover_include_legacy:score:payload",
        )
        .await
        .unwrap();
        let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(2),
            Value::Integer(20),
            Value::String("twenty".to_string()),
        ]);
        corrupt_only_encoded_column(&mut corrupt_row, 2, 3);
        txn.put(
            format!("data:sec_cover_include_legacy:{}", encoded_row_id(2)).as_bytes(),
            &corrupt_row,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
    }

    let fresh_executor = Arc::new(Executor::new(storage.clone()));
    let (cols, rows) = query(
        &fresh_executor,
        "SELECT id, payload FROM sec_cover_include_legacy WHERE score = 20",
    )
    .await;
    assert_eq!(cols, vec!["id", "payload"]);
    assert_eq!(
        rows,
        vec![vec![Value::Integer(2), Value::String("twenty".to_string())]]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_secondary_btree_include_malformed_s3_metadata_falls_back_to_base_row() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sec_cover_include_bad_meta (
            id INTEGER PRIMARY KEY,
            score INTEGER,
            payload TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_cover_include_bad_meta VALUES (1, 10, 'ten'), (2, 20, 'old-index')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_cover_include_bad_meta_score
         ON sec_cover_include_bad_meta (score) INCLUDE (payload)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(
            b"index_meta:idx_sec_cover_include_bad_meta_score",
            b"s3:5:stock1:5:score1:7:payloadjunk",
        )
        .await
        .unwrap();
        let base_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(2),
            Value::Integer(20),
            Value::String("base-fallback".to_string()),
        ]);
        txn.put(
            format!("data:sec_cover_include_bad_meta:{}", encoded_row_id(2)).as_bytes(),
            &base_row,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
    }

    let fresh_executor = Arc::new(Executor::new(storage.clone()));
    let (cols, rows) = query(
        &fresh_executor,
        "SELECT id, payload FROM sec_cover_include_bad_meta WHERE score = 20",
    )
    .await;
    assert_eq!(cols, vec!["id", "payload"]);
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(2),
            Value::String("base-fallback".to_string())
        ]]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_secondary_btree_include_malformed_payload_falls_back_to_base_rows() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sec_cover_include_bad_payload (
            id INTEGER PRIMARY KEY,
            score INTEGER,
            payload TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_cover_include_bad_payload VALUES (1, 10, 'ten'), (2, 20, 'old-index')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_cover_include_bad_payload_score
         ON sec_cover_include_bad_payload (score) INCLUDE (payload)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let score_key = fusiondb::common::encoding::encode_i64_comparable(20);
        let row_id = encoded_row_id(2);
        let index_key = format!(
            "index:sec_cover_include_bad_payload:score:{}:{}",
            score_key, row_id
        );
        txn.put(index_key.as_bytes(), &[]).await.unwrap();
        let base_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(2),
            Value::Integer(20),
            Value::String("base-fallback".to_string()),
        ]);
        txn.put(
            format!("data:sec_cover_include_bad_payload:{}", row_id).as_bytes(),
            &base_row,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
    }

    let fresh_executor = Arc::new(Executor::new(storage.clone()));
    let (cols, rows) = query(
        &fresh_executor,
        "SELECT id, payload FROM sec_cover_include_bad_payload WHERE score = 20",
    )
    .await;
    assert_eq!(cols, vec!["id", "payload"]);
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(2),
            Value::String("base-fallback".to_string())
        ]]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_secondary_btree_include_index_payload_tracks_insert_update_and_range() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sec_cover_include_dml (id INTEGER PRIMARY KEY, score INTEGER, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_cover_include_dml VALUES (1, 10, 'old')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_sec_cover_include_dml_score ON sec_cover_include_dml (score) INCLUDE (payload)",
    )
    .await;
    exec_ok(
        &executor,
        "UPDATE sec_cover_include_dml SET payload = 'new' WHERE id = 1",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sec_cover_include_dml VALUES (2, 20, 'inserted')",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, score, payload) in [(1, 10, "new"), (2, 20, "inserted")] {
            let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::Integer(score),
                Value::String(payload.to_string()),
            ]);
            corrupt_only_encoded_column(&mut corrupt_row, 2, 3);
            txn.put(
                format!("data:sec_cover_include_dml:{}", encoded_row_id(id)).as_bytes(),
                &corrupt_row,
            )
            .await
            .unwrap();
        }
        txn.commit().await.unwrap();
    }

    let fresh_executor = Arc::new(Executor::new(storage.clone()));
    let (_, range_rows) = query(
        &fresh_executor,
        "SELECT id, payload FROM sec_cover_include_dml
         WHERE score BETWEEN 10 AND 20
         ORDER BY score",
    )
    .await;
    assert_eq!(
        range_rows,
        vec![
            vec![Value::Integer(1), Value::String("new".to_string())],
            vec![Value::Integer(2), Value::String("inserted".to_string())],
        ]
    );

    let (_, in_rows) = query(
        &fresh_executor,
        "SELECT id, payload FROM sec_cover_include_dml
         WHERE score IN (10, 20)
         ORDER BY id",
    )
    .await;
    assert_eq!(
        in_rows,
        vec![
            vec![Value::Integer(1), Value::String("new".to_string())],
            vec![Value::Integer(2), Value::String("inserted".to_string())],
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_empty_secondary_index_lookup_skips_full_table_scan() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE empty_idx_probe (id INTEGER PRIMARY KEY, bucket INTEGER, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO empty_idx_probe VALUES (1, 1, 'ok'), (2, 2, 'bad')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_empty_idx_probe_bucket ON empty_idx_probe (bucket)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(2),
            Value::Integer(2),
            Value::String("bad".to_string()),
        ]);
        let corrupt_col_idx = 2usize;
        let off_pos = 2 + corrupt_col_idx * 4;
        let start =
            u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
        for byte in &mut corrupt_row[start..] {
            *byte = 0xff;
        }
        let key = format!(
            "data:empty_idx_probe:{}",
            fusiondb::common::encoding::encode_i64_comparable(2)
        );
        txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT payload FROM empty_idx_probe WHERE bucket = 999",
    )
    .await;

    assert_eq!(cols, vec!["payload"]);
    assert!(rows.is_empty());
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_create_composite_btree_index_and_lookup() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, warehouse_id INTEGER, district_id INTEGER, customer_id INTEGER, amount INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO orders VALUES (1, 1, 10, 100, 25), (2, 1, 10, 101, 30), (3, 2, 20, 100, 35)",
    )
    .await;

    let msg = exec_ok(
        &executor,
        "CREATE INDEX idx_orders_warehouse_district_customer ON orders (warehouse_id, district_id, customer_id)",
    )
    .await;
    assert!(msg.contains("indexed 3 rows"));

    let (cols, rows) = query(
        &executor,
        "SELECT amount FROM orders WHERE warehouse_id = 1 AND district_id = 10 AND customer_id = 101",
    )
    .await;
    assert_eq!(cols, vec!["amount"]);
    assert_eq!(rows, vec![vec![Value::Integer(30)]]);

    let (_, rows) = query(
        &executor,
        "EXPLAIN SELECT amount FROM orders WHERE warehouse_id = 1 AND district_id = 10 AND customer_id = 101",
    )
    .await;
    let Value::String(plan) = &rows[0][0] else {
        panic!("expected explain text");
    };
    assert!(plan.contains("idx_orders_warehouse_district_customer"));

    cleanup(&wal);
}

#[tokio::test]
async fn test_index_equality_order_by_primary_desc_limit_fetches_top_row_only() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE order_status_fast (o_id INTEGER PRIMARY KEY, c_id INTEGER, status TEXT, total INTEGER, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO order_status_fast VALUES (10, 1, 'old', 100, 'p10'), (20, 1, 'new', 200, 'p20'), (30, 2, 'other', 300, 'p30')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_order_status_fast_c_id ON order_status_fast (c_id)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(10),
            Value::Integer(1),
            Value::String("old".to_string()),
            Value::Integer(100),
            Value::String("p10".to_string()),
        ]);
        let corrupt_col_idx = 2usize;
        let off_pos = 2 + corrupt_col_idx * 4;
        let start =
            u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
        for byte in &mut corrupt_row[start..] {
            *byte = 0xff;
        }
        let key = format!(
            "data:order_status_fast:{}",
            fusiondb::common::encoding::encode_i64_comparable(10)
        );
        txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT o_id, status, total FROM order_status_fast WHERE c_id = 1 ORDER BY o_id DESC LIMIT 1",
    )
    .await;

    assert_eq!(cols, vec!["o_id", "status", "total"]);
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(20),
            Value::String("new".to_string()),
            Value::Integer(200)
        ]]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_composite_index_prefix_scan_skips_nonmatching_row_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE ts_range (id INTEGER PRIMARY KEY, host_id INTEGER, ts INTEGER, metric TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO ts_range VALUES (1, 1, 1000, 'a'), (2, 1, 2000, 'b'), (3, 2, 1000, 'bad')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_ts_range_host_ts ON ts_range (host_id, ts)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
            Value::Integer(3),
            Value::Integer(2),
            Value::Integer(1000),
            Value::String("bad".to_string()),
        ]);
        let corrupt_col_idx = 3usize;
        let off_pos = 2 + corrupt_col_idx * 4;
        let start =
            u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
        for byte in &mut corrupt_row[start..] {
            *byte = 0xff;
        }
        let key = format!(
            "data:ts_range:{}",
            fusiondb::common::encoding::encode_i64_comparable(3)
        );
        txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT ts, metric FROM ts_range WHERE host_id = 1 AND ts >= 1000 AND ts < 3000 ORDER BY ts",
    )
    .await;

    assert_eq!(cols, vec!["ts", "metric"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1000), Value::String("a".to_string())],
            vec![Value::Integer(2000), Value::String("b".to_string())],
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_composite_index_range_order_limit_skips_outside_range_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE tsbs_range (id INTEGER PRIMARY KEY, host_id INTEGER, ts INTEGER, usage_user INTEGER, usage_system INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO tsbs_range VALUES (1, 1, 60, 1, 10), (2, 1, 1000, 2, 20), (3, 1, 1060, 3, 30), (4, 1, 50000, 4, 40), (5, 2, 1000, 5, 50)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_tsbs_range_host_ts ON tsbs_range (host_id, ts)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, ts) in [(1, 60), (4, 50000), (5, 1000)] {
            let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::Integer(if id == 5 { 2 } else { 1 }),
                Value::Integer(ts),
                Value::Integer(999),
                Value::Integer(999),
            ]);
            let corrupt_col_idx = 3usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start =
                u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut corrupt_row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:tsbs_range:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT ts, usage_user, usage_system FROM tsbs_range WHERE host_id = 1 AND ts >= 1000 AND ts < 50000 ORDER BY ts LIMIT 2",
    )
    .await;

    assert_eq!(cols, vec!["ts", "usage_user", "usage_system"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1000), Value::Integer(2), Value::Integer(20)],
            vec![Value::Integer(1060), Value::Integer(3), Value::Integer(30)],
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_composite_index_order_limit_counts_after_residual_filter() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE comp_order_residual_topk (
            id INTEGER PRIMARY KEY,
            host_id INTEGER,
            ts INTEGER,
            keep INTEGER
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO comp_order_residual_topk VALUES
            (1, 1, 10, 0),
            (2, 1, 20, 0),
            (3, 1, 30, 1),
            (4, 1, 40, 1),
            (5, 2, 5, 1)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_comp_order_residual_host_ts
         ON comp_order_residual_topk (host_id, ts)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT id, ts FROM comp_order_residual_topk
         WHERE host_id = 1 AND keep = 1
         ORDER BY ts ASC
         LIMIT 2",
    )
    .await;
    assert_eq!(cols, vec!["id", "ts"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(3), Value::Integer(30)],
            vec![Value::Integer(4), Value::Integer(40)],
        ]
    );

    let (_, rows) = query(
        &executor,
        "SELECT id, ts FROM comp_order_residual_topk
         WHERE host_id = 1 AND keep = 1
         ORDER BY ts DESC
         LIMIT 2",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(4), Value::Integer(40)],
            vec![Value::Integer(3), Value::Integer(30)],
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_composite_index_range_order_desc_limit_skips_older_rows() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE tsbs_latest_desc (id INTEGER PRIMARY KEY, host_id INTEGER, ts INTEGER, usage_user INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO tsbs_latest_desc VALUES (1, 1, 1000, 10), (2, 1, 2000, 20), (3, 1, 3000, 30), (4, 1, 4000, 40), (5, 2, 5000, 50)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_tsbs_latest_desc_host_ts ON tsbs_latest_desc (host_id, ts)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for id in [1_i64, 2, 5] {
            let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::Integer(if id == 5 { 2 } else { 1 }),
                Value::Integer(id * 1000),
                Value::Integer(999),
            ]);
            let corrupt_col_idx = 3usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start =
                u32::from_le_bytes(corrupt_row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut corrupt_row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:tsbs_latest_desc:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT ts, usage_user FROM tsbs_latest_desc WHERE host_id = 1 ORDER BY ts DESC LIMIT 2",
    )
    .await;

    assert_eq!(cols, vec!["ts", "usage_user"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(4000), Value::Integer(40)],
            vec![Value::Integer(3000), Value::Integer(30)],
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_show_indexes_reports_composite_columns() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE idx_show_comp (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_show_comp_ab ON idx_show_comp (a, b)",
    )
    .await;

    let results = executor
        .execute_sql("SHOW INDEXES FROM idx_show_comp")
        .await
        .unwrap();
    if let QueryResult::Select { rows, .. } = &results[0] {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::String("idx_show_comp_ab".to_string()));
        assert_eq!(rows[0][2], Value::String("a,b".to_string()));
    } else {
        panic!("Expected Select result from SHOW INDEXES FROM");
    }

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
async fn test_plain_text_insert_skips_trigram_index_on_fusion_storage() {
    let (executor, fusion, data_dir) = setup_fusion_storage("plain_text_no_trigram").await;
    exec_ok(
        &executor,
        "CREATE TABLE plain_docs (id INTEGER PRIMARY KEY, body TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO plain_docs VALUES (1, 'plain needle text'), (2, 'other text')",
    )
    .await;

    let indexed_ids = fusion
        .trigram_index
        .read()
        .unwrap()
        .search("plain_docs", "body", "%needle%");
    assert!(
        indexed_ids.is_none(),
        "plain non-indexed TEXT columns should not populate trigram postings"
    );

    let (cols, rows) = query(
        &executor,
        "SELECT id FROM plain_docs WHERE body LIKE '%needle%'",
    )
    .await;
    assert_eq!(cols, vec!["id"]);
    assert_eq!(rows, vec![vec![Value::Integer(1)]]);
    cleanup_storage_dir(&data_dir);
}

#[tokio::test]
async fn test_indexed_text_insert_updates_trigram_index_on_fusion_storage() {
    let (executor, fusion, data_dir) = setup_fusion_storage("indexed_text_trigram").await;
    exec_ok(
        &executor,
        "CREATE TABLE indexed_docs (id INTEGER PRIMARY KEY, body TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_indexed_docs_body ON indexed_docs (body)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO indexed_docs VALUES (1, 'indexed needle text'), (2, 'other text')",
    )
    .await;

    let row_keys = {
        let guard = fusion.trigram_index.read().unwrap();
        let ids = guard
            .search("indexed_docs", "body", "%needle%")
            .expect("indexed TEXT should populate trigram postings");
        guard.map_ids_to_row_keys("indexed_docs", &ids)
    };
    assert_eq!(row_keys, vec![encoded_row_id(1)]);

    let (cols, rows) = query(
        &executor,
        "SELECT id FROM indexed_docs WHERE body LIKE '%needle%'",
    )
    .await;
    assert_eq!(cols, vec!["id"]);
    assert_eq!(rows, vec![vec![Value::Integer(1)]]);
    cleanup_storage_dir(&data_dir);
}

#[tokio::test]
async fn test_create_index_backfills_trigram_index_on_fusion_storage() {
    let (executor, fusion, data_dir) = setup_fusion_storage("create_index_backfills_trigram").await;
    exec_ok(
        &executor,
        "CREATE TABLE backfill_docs (id INTEGER PRIMARY KEY, body TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO backfill_docs VALUES (1, 'old needle text'), (2, 'other text')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_backfill_docs_body ON backfill_docs (body)",
    )
    .await;

    let row_keys = {
        let guard = fusion.trigram_index.read().unwrap();
        let ids = guard
            .search("backfill_docs", "body", "%needle%")
            .expect("CREATE INDEX should backfill trigram postings");
        guard.map_ids_to_row_keys("backfill_docs", &ids)
    };
    assert_eq!(row_keys, vec![encoded_row_id(1)]);

    let (cols, rows) = query(
        &executor,
        "SELECT id FROM backfill_docs WHERE body LIKE '%needle%'",
    )
    .await;
    assert_eq!(cols, vec!["id"]);
    assert_eq!(rows, vec![vec![Value::Integer(1)]]);
    cleanup_storage_dir(&data_dir);
}

#[tokio::test]
async fn test_update_refreshes_trigram_index_on_fusion_storage() {
    let (executor, fusion, data_dir) = setup_fusion_storage("update_refreshes_trigram").await;
    exec_ok(
        &executor,
        "CREATE TABLE update_docs (id INTEGER PRIMARY KEY, body TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_update_docs_body ON update_docs (body)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO update_docs VALUES (1, 'old needle text'), (2, 'other text')",
    )
    .await;
    exec_ok(
        &executor,
        "UPDATE update_docs SET body = 'fresh target text' WHERE id = 1",
    )
    .await;

    {
        let guard = fusion.trigram_index.read().unwrap();
        let old_ids = guard
            .search("update_docs", "body", "%needle%")
            .expect("old trigram lookup should still be searchable");
        assert!(guard
            .map_ids_to_row_keys("update_docs", &old_ids)
            .is_empty());

        let new_ids = guard
            .search("update_docs", "body", "%target%")
            .expect("updated TEXT should populate trigram postings");
        assert_eq!(
            guard.map_ids_to_row_keys("update_docs", &new_ids),
            vec![encoded_row_id(1)]
        );
    }

    let (cols, rows) = query(
        &executor,
        "SELECT id FROM update_docs WHERE body LIKE '%target%'",
    )
    .await;
    assert_eq!(cols, vec!["id"]);
    assert_eq!(rows, vec![vec![Value::Integer(1)]]);

    let (_, rows) = query(
        &executor,
        "SELECT id FROM update_docs WHERE body LIKE '%needle%'",
    )
    .await;
    assert!(rows.is_empty());
    cleanup_storage_dir(&data_dir);
}

#[tokio::test]
async fn test_delete_removes_trigram_index_on_fusion_storage() {
    let (executor, fusion, data_dir) = setup_fusion_storage("delete_removes_trigram").await;
    exec_ok(
        &executor,
        "CREATE TABLE delete_docs (id INTEGER PRIMARY KEY, body TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_delete_docs_body ON delete_docs (body)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO delete_docs VALUES (1, 'delete needle text'), (2, 'other text')",
    )
    .await;
    exec_ok(&executor, "DELETE FROM delete_docs WHERE id = 1").await;

    let row_keys = {
        let guard = fusion.trigram_index.read().unwrap();
        let ids = guard
            .search("delete_docs", "body", "%needle%")
            .expect("deleted trigram lookup should still be searchable");
        guard.map_ids_to_row_keys("delete_docs", &ids)
    };
    assert!(row_keys.is_empty());

    let (cols, rows) = query(
        &executor,
        "SELECT id FROM delete_docs WHERE body LIKE '%needle%'",
    )
    .await;
    assert_eq!(cols, vec!["id"]);
    assert!(rows.is_empty());
    cleanup_storage_dir(&data_dir);
}

#[tokio::test]
async fn test_create_index_backfill_indexes_storage_truth() {
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

    // Rewrite the stored bytes out of band: the index backfill must index
    // the current storage truth instead of the stale cached rows.
    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name, age) in [
            (1_i64, "Alice-rewritten", 30_i64),
            (2_i64, "Bob-rewritten", 42_i64),
        ] {
            let updated_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(name.to_string()),
                Value::Integer(age),
            ]);

            let key = format!(
                "data:index_backfill_cache:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &updated_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let msg = exec_ok(
        &executor,
        "CREATE INDEX idx_index_backfill_cache_name ON index_backfill_cache (name)",
    )
    .await;
    assert!(msg.contains("indexed 2 rows"));

    let (_, stale_rows) = query(
        &executor,
        "SELECT * FROM index_backfill_cache WHERE name = 'Bob'",
    )
    .await;
    assert!(stale_rows.is_empty());

    let (cols, rows) = query(
        &executor,
        "SELECT * FROM index_backfill_cache WHERE name = 'Bob-rewritten'",
    )
    .await;
    assert_eq!(cols, vec!["id", "name", "age"]);
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(2),
            Value::String("Bob-rewritten".to_string()),
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
async fn test_primary_key_only_projection_with_pk_order() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE pk_order_projection (id INTEGER PRIMARY KEY, payload TEXT, score INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO pk_order_projection VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT id FROM pk_order_projection WHERE id >= 2 ORDER BY id DESC LIMIT 2",
    )
    .await;
    assert_eq!(cols, vec!["id"]);
    assert_eq!(rows, vec![vec![Value::Integer(3)], vec![Value::Integer(2)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_primary_key_in_projection_stream_skips_payload_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE pk_in_stream (id INTEGER PRIMARY KEY, payload TEXT)",
    )
    .await;

    let values = (1..=80)
        .map(|id| format!("({}, 'payload-{}')", id, id))
        .collect::<Vec<_>>()
        .join(", ");
    exec_ok(
        &executor,
        &format!("INSERT INTO pk_in_stream VALUES {}", values),
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for id in 1..=80 {
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
            let key = format!(
                "data:pk_in_stream:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let in_list = (1..=80)
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let (cols, rows) = query(
        &executor,
        &format!("SELECT id FROM pk_in_stream WHERE id IN ({})", in_list),
    )
    .await;

    let expected = (1..=80)
        .map(|id| vec![Value::Integer(id)])
        .collect::<Vec<_>>();
    assert_eq!(cols, vec!["id"]);
    assert_eq!(rows, expected);
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_primary_key_point_lookup_row_cache_tracks_storage_bytes() {
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

    // Rewrite the stored bytes out of band: the point lookup must decode
    // the new bytes instead of serving the stale cached row.
    let updated_row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(1),
        Value::String("Alice-rewritten".to_string()),
    ]);

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(b"data:pk_lookup_cache:8000000000000001", &updated_row)
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    let (_, rows) = query(&executor, "SELECT * FROM pk_lookup_cache WHERE id = 1").await;
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(1),
            Value::String("Alice-rewritten".to_string())
        ]]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_primary_key_projection_row_cache_tracks_storage_bytes() {
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

    // Rewrite the stored bytes out of band: the projected point lookup must
    // decode the new bytes instead of serving the stale cached full row.
    let updated_row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(1),
        Value::String("Alice-rewritten".to_string()),
        Value::String("payload".to_string()),
    ]);

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(b"data:pk_project_cache:8000000000000001", &updated_row)
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(&executor, "SELECT name FROM pk_project_cache WHERE id = 1").await;
    assert_eq!(cols, vec!["name"]);
    assert_eq!(
        rows,
        vec![vec![Value::String("Alice-rewritten".to_string())]]
    );
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

#[tokio::test]
async fn test_primary_key_lookup_projection_skips_where_key_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE pk_get (key_id INTEGER PRIMARY KEY, value TEXT, flags INTEGER)",
    )
    .await;

    let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(1),
        Value::String("value-1".to_string()),
        Value::Integer(7),
    ]);
    let corrupt_col_idx = 0usize;
    let off_pos = 2 + corrupt_col_idx * 4;
    let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
    let end = u32::from_le_bytes(row[off_pos + 4..off_pos + 8].try_into().unwrap()) as usize;
    for byte in &mut row[start..end] {
        *byte = 0xff;
    }

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let key = format!(
            "data:pk_get:{}",
            fusiondb::common::encoding::encode_i64_comparable(1)
        );
        txn.put(key.as_bytes(), &row).await.unwrap();
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(&executor, "SELECT value FROM pk_get WHERE key_id = 1").await;
    assert_eq!(cols, vec!["value"]);
    assert_eq!(rows, vec![vec![Value::String("value-1".to_string())]]);
    cleanup(&wal_path);
}

// ==================== EXPLAIN Tests ====================

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
async fn test_show_indexes() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE si (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE si_other (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(&executor, "CREATE INDEX idx_si_name ON si (name)").await;
    exec_ok(&executor, "CREATE INDEX idx_si_age ON si (age)").await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_si_other_name ON si_other (name)",
    )
    .await;

    let results = executor.execute_sql("SHOW INDEXES").await.unwrap();
    if let QueryResult::Select { columns, rows } = &results[0] {
        assert_eq!(columns, &vec!["Index", "Table", "Column"]);
        assert_eq!(rows.len(), 3);
        assert!(rows.contains(&vec![
            Value::String("idx_si_name".to_string()),
            Value::String("si".to_string()),
            Value::String("name".to_string()),
        ]));
        assert!(rows.contains(&vec![
            Value::String("idx_si_age".to_string()),
            Value::String("si".to_string()),
            Value::String("age".to_string()),
        ]));
        assert!(rows.contains(&vec![
            Value::String("idx_si_other_name".to_string()),
            Value::String("si_other".to_string()),
            Value::String("name".to_string()),
        ]));
    } else {
        panic!("Expected Select result from SHOW INDEXES");
    }

    let results = executor.execute_sql("SHOW INDEXES FROM si").await.unwrap();
    if let QueryResult::Select { rows, .. } = &results[0] {
        assert_eq!(rows.len(), 2);
        assert!(rows
            .iter()
            .all(|row| row[1] == Value::String("si".to_string())));
    } else {
        panic!("Expected Select result from SHOW INDEXES FROM");
    }

    exec_ok(&executor, "DROP INDEX idx_si_name").await;
    let results = executor.execute_sql("SHOW INDEXES FROM si").await.unwrap();
    if let QueryResult::Select { rows, .. } = &results[0] {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::String("idx_si_age".to_string()));
    } else {
        panic!("Expected Select result from SHOW INDEXES after DROP INDEX");
    }

    cleanup(&wal);
}

/// BENCHPROD-465: side-index (trigram) mutations are commit-deferred — an
/// uncommitted transaction leaves no trace, a rolled-back one never touches
/// the shared index, and commit applies the buffered deltas.
#[tokio::test]
async fn test_trigram_index_mutations_are_commit_deferred() {
    let (executor, fusion, data_dir) = setup_fusion_storage("trigram_deferred").await;
    exec_ok(
        &executor,
        "CREATE TABLE defer_docs (id INTEGER PRIMARY KEY, body TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_defer_docs_body ON defer_docs (body)",
    )
    .await;

    let storage: Arc<dyn Storage> = Arc::new(fusion.clone());
    let insert = executor
        .prepare("INSERT INTO defer_docs VALUES (1, 'rollback needle text')")
        .unwrap();

    // Staged but uncommitted: the shared trigram index must be untouched.
    let mut txn = storage.begin_transaction().await.unwrap();
    executor
        .execute_in_transaction(&insert[0], &mut *txn)
        .await
        .unwrap();
    {
        let guard = fusion.trigram_index.read().unwrap();
        assert!(
            guard.search("defer_docs", "body", "%needle%").is_none(),
            "uncommitted insert must not touch the trigram index"
        );
    }

    // Rollback: still untouched.
    txn.rollback().await.unwrap();
    {
        let guard = fusion.trigram_index.read().unwrap();
        assert!(
            guard.search("defer_docs", "body", "%needle%").is_none(),
            "rolled-back insert must never reach the trigram index"
        );
    }

    // Commit path applies the deltas.
    exec_ok(
        &executor,
        "INSERT INTO defer_docs VALUES (2, 'committed needle text')",
    )
    .await;
    let row_keys = {
        let guard = fusion.trigram_index.read().unwrap();
        let ids = guard
            .search("defer_docs", "body", "%needle%")
            .expect("committed insert must populate trigram postings");
        guard.map_ids_to_row_keys("defer_docs", &ids)
    };
    assert_eq!(row_keys, vec![encoded_row_id(2)]);
    cleanup_storage_dir(&data_dir);
}

/// BENCHPROD-465: HNSW mutations are commit-deferred on the Fusion backend.
/// Uses EMBEDDING() because SQL array literals store as Value::Array and do
/// not reach the vector index (pre-existing gap, tracked separately).
#[tokio::test]
async fn test_vector_index_mutations_are_commit_deferred() {
    let (executor, fusion, data_dir) = setup_fusion_storage("vector_deferred").await;
    exec_ok(
        &executor,
        "CREATE TABLE defer_vecs (id INTEGER PRIMARY KEY, emb VECTOR(128))",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_defer_vecs_emb ON defer_vecs (emb) USING HNSW",
    )
    .await;

    let (_, erows) = query(&executor, "SELECT EMBEDDING('deferred vector probe')").await;
    let Value::Vector(query_vec) = &erows[0][0] else {
        panic!("EMBEDDING must return a vector");
    };
    let query_vec = query_vec.clone();

    let storage: Arc<dyn Storage> = Arc::new(fusion.clone());
    let insert = executor
        .prepare("INSERT INTO defer_vecs VALUES (1, EMBEDDING('deferred vector probe'))")
        .unwrap();

    let mut txn = storage.begin_transaction().await.unwrap();
    executor
        .execute_in_transaction(&insert[0], &mut *txn)
        .await
        .unwrap();
    let hits = fusion
        .vector_index
        .search("hnsw_defer_vecs_emb", &query_vec, 5)
        .unwrap();
    assert!(
        hits.is_empty(),
        "uncommitted vector insert must not reach the HNSW index"
    );

    txn.rollback().await.unwrap();
    let hits = fusion
        .vector_index
        .search("hnsw_defer_vecs_emb", &query_vec, 5)
        .unwrap();
    assert!(hits.is_empty(), "rolled-back vector insert must vanish");

    exec_ok(
        &executor,
        "INSERT INTO defer_vecs VALUES (2, EMBEDDING('deferred vector probe'))",
    )
    .await;
    let hits = fusion
        .vector_index
        .search("hnsw_defer_vecs_emb", &query_vec, 5)
        .unwrap();
    assert_eq!(hits.len(), 1, "committed vector insert must be searchable");
    cleanup_storage_dir(&data_dir);
}

/// BENCHPROD-465: an OCC-aborted transaction's side-index deltas are dropped
/// — only the winner's text reaches the trigram index.
#[tokio::test]
async fn test_occ_abort_drops_side_index_deltas() {
    let (executor, fusion, data_dir) = setup_fusion_storage("occ_abort_side_index").await;
    exec_ok(
        &executor,
        "CREATE TABLE occ_docs (id INTEGER PRIMARY KEY, body TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_occ_docs_body ON occ_docs (body)",
    )
    .await;

    let storage: Arc<dyn Storage> = Arc::new(fusion.clone());
    let insert_a = executor
        .prepare("INSERT INTO occ_docs VALUES (1, 'winner needle text')")
        .unwrap();
    let insert_b = executor
        .prepare("INSERT INTO occ_docs VALUES (1, 'loser needle text')")
        .unwrap();

    let mut txn_a = storage.begin_transaction().await.unwrap();
    let mut txn_b = storage.begin_transaction().await.unwrap();
    executor
        .execute_in_transaction(&insert_a[0], &mut *txn_a)
        .await
        .unwrap();
    executor
        .execute_in_transaction(&insert_b[0], &mut *txn_b)
        .await
        .unwrap();

    txn_a.commit().await.unwrap();
    assert!(
        txn_b.commit().await.is_err(),
        "same-PK concurrent insert must abort"
    );

    let row_keys = {
        let guard = fusion.trigram_index.read().unwrap();
        let ids = guard
            .search("occ_docs", "body", "%needle%")
            .expect("winner's postings must exist");
        guard.map_ids_to_row_keys("occ_docs", &ids)
    };
    assert_eq!(
        row_keys,
        vec![encoded_row_id(1)],
        "only one posting (the winner's) may exist"
    );
    let loser_ids = {
        let guard = fusion.trigram_index.read().unwrap();
        guard.search("occ_docs", "body", "%loser%")
    };
    assert!(
        loser_ids.is_none() || loser_ids.unwrap().is_empty(),
        "aborted transaction's trigram delta must be dropped"
    );
    cleanup_storage_dir(&data_dir);
}

/// BENCHPROD-466d: a Raft snapshot install (replace_visible_entries_for_snapshot)
/// previously rebuilt only the vector index; trigram postings survived from
/// pre-snapshot state (or were missing on a fresh follower). It must now
/// rebuild the trigram index from the installed rows.
#[tokio::test]
async fn test_snapshot_install_rebuilds_trigram_index() {
    let (executor, fusion, data_dir) = setup_fusion_storage("snapshot_trigram").await;
    exec_ok(
        &executor,
        "CREATE TABLE snap_docs (id INTEGER PRIMARY KEY, body TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_snap_docs_body ON snap_docs (body)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO snap_docs VALUES (1, 'snapshot needle text'), (2, 'other text')",
    )
    .await;

    // Export the visible state the way the Raft snapshot path does.
    let storage: Arc<dyn Storage> = Arc::new(fusion.clone());
    let entries = {
        let txn = storage.begin_transaction().await.unwrap();
        txn.scan_range(b"", &[0xff], None).await.unwrap()
    };

    // Simulate a fresh follower: wipe the in-memory trigram index, then
    // install the snapshot. The install must rebuild the postings.
    *fusion.trigram_index.write().unwrap() = fusiondb::storage::trigram::TrigramIndex::new();
    {
        let guard = fusion.trigram_index.read().unwrap();
        assert!(guard.search("snap_docs", "body", "%needle%").is_none());
    }

    fusion
        .replace_visible_entries_for_snapshot(b"", &[0xff], &entries)
        .await
        .unwrap();

    let row_keys = {
        let guard = fusion.trigram_index.read().unwrap();
        let ids = guard
            .search("snap_docs", "body", "%needle%")
            .expect("snapshot install must rebuild trigram postings");
        guard.map_ids_to_row_keys("snap_docs", &ids)
    };
    assert_eq!(row_keys, vec![encoded_row_id(1)]);

    let (_, rows) = query(
        &executor,
        "SELECT id FROM snap_docs WHERE body LIKE '%needle%'",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(1)]]);
    cleanup_storage_dir(&data_dir);
}
