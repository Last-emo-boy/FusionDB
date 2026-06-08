use fusiondb::common::Value;
use fusiondb::config::StorageConfig;
use fusiondb::execution::{Executor, QueryResult};
use fusiondb::storage::memory::MemoryStorage;
use fusiondb::storage::FusionStorage;
use fusiondb::storage::Storage;
use std::path::PathBuf;
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
    config.memtable_flush_mb = 0;
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

fn write_copy_fixture(name: &str, content: &str) -> String {
    let path: PathBuf =
        std::env::temp_dir().join(format!("fusiondb_{}_{}.csv", name, uuid::Uuid::new_v4()));
    std::fs::write(&path, content).unwrap();
    path.to_string_lossy().replace('\\', "/")
}

#[test]
fn test_timestamp_parser_accepts_timezone_offsets() {
    assert_eq!(
        Value::timestamp_from_str("2016-01-01 08:00:00+08:00"),
        Some(Value::Timestamp(1_451_606_400_000_000))
    );
    assert_eq!(
        Value::timestamp_from_str("2016-01-01 08:00:00.123+08"),
        Some(Value::Timestamp(1_451_606_400_123_000))
    );
    assert_eq!(
        Value::timestamp_from_str("2016-01-01T00:00:00Z"),
        Some(Value::Timestamp(1_451_606_400_000_000))
    );
}

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
async fn test_duplicate_primary_key_insert_is_rejected_and_upsert_updates_cache() {
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

    let stmts = executor
        .prepare("INSERT INTO full_scan_insert_cache VALUES (1, 'Rejected')")
        .unwrap();
    let result = executor.execute(&stmts[0]).await;
    assert!(result.is_err());
    assert!(format!("{:?}", result.err().unwrap()).contains("PRIMARY KEY constraint violated"));

    exec_ok(&executor, "INSERT INTO full_scan_insert_cache VALUES (1, 'Bob') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name").await;
    let (_, rows) = query(&executor, "SELECT * FROM full_scan_insert_cache").await;
    assert_eq!(rows[0][1], Value::String("Bob".to_string()));
    cleanup(&wal);
}

#[tokio::test]
async fn test_insert_table_without_primary_key_allows_duplicate_first_column() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE history_events (tid INTEGER, bid INTEGER, aid INTEGER, delta INTEGER, mtime TIMESTAMP, filler TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO history_events VALUES (1, 1, 1, 10, TIMESTAMP '2026-01-01 00:00:00', 'txn-1')",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO history_events VALUES (1, 1, 2, 20, TIMESTAMP '2026-01-01 00:00:00', 'txn-2')",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT COUNT(*) FROM history_events").await;
    assert_eq!(rows[0][0], Value::Integer(2));

    let (_, rows) = query(
        &executor,
        "SELECT aid, delta FROM history_events WHERE tid = 1 ORDER BY aid",
    )
    .await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![Value::Integer(1), Value::Integer(10)]);
    assert_eq!(rows[1], vec![Value::Integer(2), Value::Integer(20)]);
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
async fn test_update_primary_key_simple_table_fast_path() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE kv_update_fast (key_id INTEGER PRIMARY KEY, value TEXT, flags INTEGER, expires_at INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO kv_update_fast VALUES (1, 'value-1', 7, 1900000001)",
    )
    .await;

    let msg = exec_ok(
        &executor,
        "UPDATE kv_update_fast SET value = 'updated-1' WHERE key_id = 1",
    )
    .await;
    assert!(msg.contains("Updated 1"));

    let (cols, rows) = query(
        &executor,
        "SELECT key_id, value, flags, expires_at FROM kv_update_fast WHERE key_id = 1",
    )
    .await;
    assert_eq!(cols, vec!["key_id", "value", "flags", "expires_at"]);
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(1),
            Value::String("updated-1".to_string()),
            Value::Integer(7),
            Value::Integer(1900000001),
        ]]
    );
    cleanup(&wal_path);
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

#[tokio::test]
async fn test_unquoted_identifier_lookup_is_case_insensitive_for_tpcc_update_shape() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE warehouse (w_id INTEGER PRIMARY KEY, w_ytd DECIMAL(12, 2) NOT NULL)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO warehouse VALUES (1, CAST('300000.00' AS DECIMAL))",
    )
    .await;

    let msg = exec_ok(
        &executor,
        "UPDATE warehouse SET W_YTD = W_YTD + CAST('10.25' AS DECIMAL) WHERE W_ID = 1",
    )
    .await;
    assert!(msg.contains("Updated 1"));

    let (_, rows) = query(&executor, "SELECT W_YTD FROM warehouse WHERE W_ID = 1").await;
    assert_eq!(rows, vec![vec![Value::Decimal("300010.25".to_string())]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_tpcc_district_composite_primary_key_lookup_with_for_update() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE warehouse (w_id INTEGER PRIMARY KEY, w_name VARCHAR(10) NOT NULL)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO warehouse VALUES (1, 'w1')").await;
    exec_ok(
        &executor,
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
    .await;
    exec_ok(
        &executor,
        "INSERT INTO district VALUES (
            1,
            8,
            CAST('30000.00' AS DECIMAL),
            CAST('0.0527' AS DECIMAL),
            3001,
            'district8'
        )",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT D_NEXT_O_ID, D_TAX
         FROM district
         WHERE D_W_ID = 1 AND D_ID = 8
         FOR UPDATE",
    )
    .await;
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(3001),
            Value::Decimal("0.0527".to_string()),
        ]]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_fusion_storage_tpcc_payment_district_update_after_snapshot() {
    let (executor, fusion, data_dir) = setup_fusion_storage("tpcc_payment_district_snapshot").await;
    exec_ok(
        &executor,
        "CREATE TABLE warehouse (
            w_id INTEGER PRIMARY KEY,
            w_ytd DECIMAL(12, 2) NOT NULL,
            w_name VARCHAR(10) NOT NULL
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO warehouse VALUES (1, CAST('300000.00' AS DECIMAL), 'w1')",
    )
    .await;
    exec_ok(
        &executor,
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
    .await;
    exec_ok(
        &executor,
        "INSERT INTO district VALUES
            (1, 1, CAST('30000.00' AS DECIMAL), CAST('0.0511' AS DECIMAL), 3001, 'district1'),
            (1, 7, CAST('30000.00' AS DECIMAL), CAST('0.0579' AS DECIMAL), 3001, 'district7'),
            (1, 8, CAST('30000.00' AS DECIMAL), CAST('0.0527' AS DECIMAL), 3001, 'district8')",
    )
    .await;

    fusion.create_snapshot_now().await.unwrap();

    let msg = exec_ok(
        &executor,
        "UPDATE district
            SET D_YTD = D_YTD + CAST('2849.77' AS DECIMAL)
          WHERE D_W_ID = 1
            AND D_ID = 7",
    )
    .await;
    assert_eq!(msg, "Updated 1 rows");

    let (_, rows) = query(
        &executor,
        "SELECT D_ID, D_NAME
           FROM district
          WHERE D_W_ID = 1
            AND D_ID = 7",
    )
    .await;
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(7),
            Value::String("district7".to_string()),
        ]]
    );
    cleanup_storage_dir(&data_dir);
}

#[tokio::test]
async fn test_fusion_storage_sstable_seek_finds_tpcc_district_mid_block() {
    let (executor, fusion, data_dir) =
        setup_fusion_storage("tpcc_payment_district_sstable_seek").await;
    exec_ok(
        &executor,
        "CREATE TABLE warehouse (
            w_id INTEGER PRIMARY KEY,
            w_ytd DECIMAL(12, 2) NOT NULL,
            w_name VARCHAR(10) NOT NULL
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO warehouse VALUES (1, CAST('300000.00' AS DECIMAL), 'w1')",
    )
    .await;
    exec_ok(
        &executor,
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
    .await;

    for d_id in 1..=80 {
        exec_ok(
            &executor,
            &format!(
                "INSERT INTO district VALUES
                    (1, {}, CAST('30000.00' AS DECIMAL), CAST('0.0579' AS DECIMAL), 3001, 'district{}')",
                d_id, d_id
            ),
        )
        .await;
    }

    fusion.create_snapshot_now().await.unwrap();

    let msg = exec_ok(
        &executor,
        "UPDATE district
            SET D_YTD = D_YTD + CAST('2849.77' AS DECIMAL)
          WHERE D_W_ID = 1
            AND D_ID = 7",
    )
    .await;
    assert_eq!(msg, "Updated 1 rows");

    let (_, rows) = query(
        &executor,
        "SELECT D_ID, D_NAME
           FROM district
          WHERE D_W_ID = 1
            AND D_ID = 7",
    )
    .await;
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(7),
            Value::String("district7".to_string()),
        ]]
    );
    cleanup_storage_dir(&data_dir);
}

#[tokio::test]
async fn test_fusion_storage_prefix_scan_seeks_inside_sstable_block() {
    let (executor, fusion, data_dir) =
        setup_fusion_storage("prefix_scan_seek_inside_sstable_block").await;

    {
        let mut txn = fusion.begin_transaction().await.unwrap();
        for i in 0..300 {
            txn.put(format!("data:customer:{:04}", i).as_bytes(), b"customer")
                .await
                .unwrap();
        }
        txn.put(b"data:district:target", b"district").await.unwrap();
        txn.commit().await.unwrap();
    }

    fusion.create_snapshot_now().await.unwrap();

    let txn = fusion.begin_transaction().await.unwrap();
    let rows = txn.scan_prefix(b"data:district:", None).await.unwrap();
    assert_eq!(
        rows,
        vec![(b"data:district:target".to_vec(), b"district".to_vec())]
    );
    txn.rollback().await.unwrap();

    cleanup_storage_dir(&data_dir);
    drop(executor);
}

#[tokio::test]
async fn test_fusion_storage_tpcc_order_fk_chain_after_many_customers() {
    let (executor, fusion, data_dir) = setup_fusion_storage("tpcc_order_fk_chain").await;
    exec_ok(
        &executor,
        "CREATE TABLE warehouse (
            w_id INTEGER PRIMARY KEY,
            w_name VARCHAR(10) NOT NULL
        )",
    )
    .await;
    exec_ok(&executor, "INSERT INTO warehouse VALUES (1, 'w1')").await;
    exec_ok(
        &executor,
        "CREATE TABLE district (
            d_w_id INTEGER NOT NULL,
            d_id INTEGER NOT NULL,
            d_name VARCHAR(10) NOT NULL,
            FOREIGN KEY (d_w_id) REFERENCES warehouse (w_id),
            PRIMARY KEY (d_w_id, d_id)
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO district VALUES (1, 1, 'd1'), (1, 2, 'd2')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE customer (
            c_w_id INTEGER NOT NULL,
            c_d_id INTEGER NOT NULL,
            c_id INTEGER NOT NULL,
            c_state CHAR(2) NOT NULL,
            c_data VARCHAR(500) NOT NULL,
            FOREIGN KEY (c_w_id, c_d_id) REFERENCES district (d_w_id, d_id),
            PRIMARY KEY (c_w_id, c_d_id, c_id)
        )",
    )
    .await;

    let customer_payload = "x".repeat(400);
    let mut batch_values = Vec::new();
    for d_id in 1..=2 {
        for c_id in 1..=3000 {
            batch_values.push(format!(
                "(1, {}, {}, 'XYZ', '{}')",
                d_id, c_id, customer_payload
            ));
            if batch_values.len() == 100 {
                exec_ok(
                    &executor,
                    &format!("INSERT INTO customer VALUES {}", batch_values.join(",")),
                )
                .await;
                batch_values.clear();
            }
        }
    }
    if !batch_values.is_empty() {
        exec_ok(
            &executor,
            &format!("INSERT INTO customer VALUES {}", batch_values.join(",")),
        )
        .await;
    }

    fusion.create_snapshot_now().await.unwrap();

    exec_ok(
        &executor,
        "CREATE TABLE oorder (
            o_w_id INTEGER NOT NULL,
            o_d_id INTEGER NOT NULL,
            o_id INTEGER NOT NULL,
            o_c_id INTEGER NOT NULL,
            o_entry_d TIMESTAMP NOT NULL,
            PRIMARY KEY (o_w_id, o_d_id, o_id),
            FOREIGN KEY (o_w_id, o_d_id, o_c_id) REFERENCES customer (c_w_id, c_d_id, c_id),
            UNIQUE (o_w_id, o_d_id, o_c_id, o_id)
        )",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE TABLE new_order (
            no_w_id INTEGER NOT NULL,
            no_d_id INTEGER NOT NULL,
            no_o_id INTEGER NOT NULL,
            FOREIGN KEY (no_w_id, no_d_id, no_o_id) REFERENCES oorder (o_w_id, o_d_id, o_id),
            PRIMARY KEY (no_w_id, no_d_id, no_o_id)
        )",
    )
    .await;

    exec_ok(
        &executor,
        "INSERT INTO oorder VALUES
            (1, 1, 128, 2504, TIMESTAMP '2026-05-28 12:00:00'),
            (1, 1, 2101, 2101, TIMESTAMP '2026-05-28 12:00:00')",
    )
    .await;
    exec_ok(&executor, "INSERT INTO new_order VALUES (1, 1, 2101)").await;

    let (_, rows) = query(
        &executor,
        "SELECT COUNT(*) FROM new_order WHERE no_w_id = 1 AND no_d_id = 1",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(1)]]);

    cleanup_storage_dir(&data_dir);
}

#[tokio::test]
async fn test_tpcc_order_status_uses_filter_columns_outside_projection() {
    let wal = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));
    exec_ok(
        &executor,
        "CREATE TABLE oorder (
            o_w_id INTEGER NOT NULL,
            o_d_id INTEGER NOT NULL,
            o_id INTEGER NOT NULL,
            o_c_id INTEGER NOT NULL,
            o_carrier_id INTEGER,
            o_ol_cnt INTEGER NOT NULL,
            o_all_local INTEGER NOT NULL,
            o_entry_d TIMESTAMP NOT NULL,
            PRIMARY KEY (o_w_id, o_d_id, o_id)
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO oorder VALUES
            (1, 4, 847, 2542, 2, 8, 1, TIMESTAMP '2026-05-28 12:00:00'),
            (1, 4, 1515, 847, 8, 14, 1, TIMESTAMP '2026-05-28 12:01:00')",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT o_id, o_carrier_id, o_entry_d
           FROM oorder
          WHERE o_w_id = 1
            AND o_d_id = 4
            AND o_c_id = 847
          ORDER BY o_id DESC
          LIMIT 1",
    )
    .await;
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(1515),
            Value::Integer(8),
            Value::Timestamp(1_779_969_660_000_000),
        ]]
    );

    let stmts = executor
        .prepare(
            "SELECT o_id, o_carrier_id, o_entry_d
               FROM oorder
              WHERE o_w_id = $1
                AND o_d_id = $2
                AND o_c_id = $3
              ORDER BY o_id DESC
              LIMIT 1",
        )
        .unwrap();
    let mut txn = storage.begin_transaction().await.unwrap();
    let result = executor
        .execute_in_transaction_with_params(
            &stmts[0],
            txn.as_mut(),
            &[Value::Integer(1), Value::Integer(4), Value::Integer(847)],
        )
        .await
        .unwrap();
    txn.rollback().await.unwrap();
    match result {
        QueryResult::Select { rows, .. } => assert_eq!(
            rows,
            vec![vec![
                Value::Integer(1515),
                Value::Integer(8),
                Value::Timestamp(1_779_969_660_000_000),
            ]]
        ),
        other => panic!("Expected Select, got {:?}", other),
    }
    cleanup(&wal);
}

#[tokio::test]
async fn test_tpcc_order_status_limit_finds_late_composite_index_match() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE oorder (
            o_w_id INTEGER NOT NULL,
            o_d_id INTEGER NOT NULL,
            o_id INTEGER NOT NULL,
            o_c_id INTEGER NOT NULL,
            o_carrier_id INTEGER,
            o_ol_cnt INTEGER NOT NULL,
            o_all_local INTEGER NOT NULL,
            o_entry_d TIMESTAMP NOT NULL,
            PRIMARY KEY (o_w_id, o_d_id, o_id),
            UNIQUE (o_w_id, o_d_id, o_c_id, o_id)
        )",
    )
    .await;

    exec_ok(
        &executor,
        "INSERT INTO oorder VALUES
            (1, 2, 1, 1001, 1, 8, 1, TIMESTAMP '2026-05-28 12:00:01'),
            (1, 2, 2, 1002, 2, 8, 1, TIMESTAMP '2026-05-28 12:00:02'),
            (1, 2, 3, 1003, 3, 8, 1, TIMESTAMP '2026-05-28 12:00:03'),
            (1, 2, 4, 1004, 4, 8, 1, TIMESTAMP '2026-05-28 12:00:04'),
            (1, 2, 2135, 1480, NULL, 8, 1, TIMESTAMP '2026-05-28 12:35:35')",
    )
    .await;

    let (_, count_rows) = query(
        &executor,
        "SELECT count(*) FROM oorder WHERE o_w_id = 1 AND o_d_id = 2 AND o_c_id = 1480",
    )
    .await;
    assert_eq!(count_rows, vec![vec![Value::Integer(1)]]);

    let (_, rows) = query(
        &executor,
        "SELECT o_id, o_carrier_id, o_entry_d
           FROM oorder
          WHERE o_w_id = 1
            AND o_d_id = 2
            AND o_c_id = 1480
          ORDER BY o_id DESC
          LIMIT 1",
    )
    .await;
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(2135),
            Value::Null,
            Value::Timestamp(1_779_971_735_000_000),
        ]]
    );

    let (_, limit_only_rows) = query(
        &executor,
        "SELECT o_id, o_carrier_id, o_entry_d
           FROM oorder
          WHERE o_w_id = 1
            AND o_d_id = 2
            AND o_c_id = 1480
          LIMIT 1",
    )
    .await;
    assert_eq!(
        limit_only_rows,
        vec![vec![
            Value::Integer(2135),
            Value::Null,
            Value::Timestamp(1_779_971_735_000_000),
        ]]
    );

    cleanup(&wal_path);
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
async fn test_dml_maintains_composite_index_entries() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE stock (id INTEGER PRIMARY KEY, warehouse_id INTEGER, item_id INTEGER, qty INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_stock_warehouse_item ON stock (warehouse_id, item_id)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO stock VALUES (1, 1, 100, 50), (2, 1, 101, 60)",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT qty FROM stock WHERE warehouse_id = 1 AND item_id = 100",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(50)]]);

    exec_ok(
        &executor,
        "UPDATE stock SET item_id = 102, qty = 70 WHERE id = 1",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT qty FROM stock WHERE warehouse_id = 1 AND item_id = 100",
    )
    .await;
    assert!(rows.is_empty());
    let (_, rows) = query(
        &executor,
        "SELECT qty FROM stock WHERE warehouse_id = 1 AND item_id = 102",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(70)]]);

    exec_ok(&executor, "DELETE FROM stock WHERE id = 1").await;
    let (_, rows) = query(
        &executor,
        "SELECT qty FROM stock WHERE warehouse_id = 1 AND item_id = 102",
    )
    .await;
    assert!(rows.is_empty());
    cleanup(&wal);
}

#[tokio::test]
async fn test_update_primary_key_fast_path_preserves_untouched_secondary_index() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE stock_fast (id INTEGER PRIMARY KEY, warehouse_id INTEGER, quantity INTEGER, ytd INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO stock_fast VALUES (1, 7, 50, 0), (2, 8, 60, 0)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_stock_fast_warehouse ON stock_fast (warehouse_id)",
    )
    .await;

    let msg = exec_ok(
        &executor,
        "UPDATE stock_fast SET quantity = quantity - 3, ytd = ytd + 3 WHERE id = 1",
    )
    .await;
    assert!(msg.contains("Updated 1"));

    let (_, rows) = query(
        &executor,
        "SELECT quantity, ytd FROM stock_fast WHERE id = 1",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(47), Value::Integer(3)]]);

    let (_, rows) = query(
        &executor,
        "SELECT id, quantity, ytd FROM stock_fast WHERE warehouse_id = 7",
    )
    .await;
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(1),
            Value::Integer(47),
            Value::Integer(3),
        ]]
    );

    exec_ok(
        &executor,
        "UPDATE stock_fast SET warehouse_id = 9 WHERE id = 1",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT id FROM stock_fast WHERE warehouse_id = 7",
    )
    .await;
    assert!(rows.is_empty());
    let (_, rows) = query(
        &executor,
        "SELECT id FROM stock_fast WHERE warehouse_id = 9",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(1)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_execute_sql_multi_statement_commits_once() {
    let (executor, wal) = setup().await;
    let results = executor
        .execute_sql(
            "CREATE TABLE multi_tx (id INTEGER PRIMARY KEY, value INTEGER);
             INSERT INTO multi_tx VALUES (1, 10);
             UPDATE multi_tx SET value = value + 5 WHERE id = 1;
             SELECT value FROM multi_tx WHERE id = 1",
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 4);
    match &results[3] {
        QueryResult::Select { rows, .. } => {
            assert_eq!(rows, &vec![vec![Value::Integer(15)]]);
        }
        other => panic!("Expected Select, got {:?}", other),
    }

    let (_, rows) = query(&executor, "SELECT value FROM multi_tx WHERE id = 1").await;
    assert_eq!(rows, vec![vec![Value::Integer(15)]]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_execute_sql_multi_statement_rolls_back_on_error() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE multi_tx_rollback (id INTEGER PRIMARY KEY, value INTEGER)",
    )
    .await;

    let result = executor
        .execute_sql(
            "INSERT INTO multi_tx_rollback VALUES (1, 10);
             UPDATE missing_table SET value = 99 WHERE id = 1",
        )
        .await;
    assert!(result.is_err());

    let (_, rows) = query(&executor, "SELECT * FROM multi_tx_rollback").await;
    assert!(rows.is_empty());
    cleanup(&wal);
}

#[tokio::test]
async fn test_composite_index_dml_uses_table_metadata_directory() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE stock_dir (id INTEGER PRIMARY KEY, warehouse_id INTEGER, item_id INTEGER, qty INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_stock_dir_warehouse_item ON stock_dir (warehouse_id, item_id)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        assert!(txn
            .get(b"index_meta_table:stock_dir:idx_stock_dir_warehouse_item")
            .await
            .unwrap()
            .is_some());
        txn.delete(b"index_meta:idx_stock_dir_warehouse_item")
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    exec_ok(&executor, "INSERT INTO stock_dir VALUES (1, 1, 100, 50)").await;

    {
        let txn = storage.begin_transaction().await.unwrap();
        let entries = txn
            .scan_prefix(b"index:stock_dir:warehouse_id,item_id:", None)
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        txn.rollback().await.unwrap();
    }

    let (_, rows) = query(
        &executor,
        "SELECT qty FROM stock_dir WHERE warehouse_id = 1 AND item_id = 100",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(50)]]);
    cleanup(&wal_path);
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

#[tokio::test]
async fn test_insert_omitted_serial_primary_key_generates_ids() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE serial_tags (id SERIAL PRIMARY KEY, hostname TEXT, region TEXT)",
    )
    .await;
    let stmts = executor
        .prepare(
            "INSERT INTO serial_tags(hostname, region) VALUES ('host_0', 'us-east'), ('host_1', 'us-west') RETURNING *",
        )
        .unwrap();
    let result = executor.execute(&stmts[0]).await.unwrap();
    if let QueryResult::Select { rows, .. } = result {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Integer(1));
        assert_eq!(rows[1][0], Value::Integer(2));
        assert_eq!(rows[0][1], Value::String("host_0".to_string()));
        assert_eq!(rows[1][2], Value::String("us-west".to_string()));
    } else {
        panic!("expected INSERT RETURNING rows");
    }

    let (_, rows) = query(
        &executor,
        "SELECT id, hostname FROM serial_tags ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::String("host_0".to_string())],
            vec![Value::Integer(2), Value::String("host_1".to_string())],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_copy_from_csv_with_header_and_index_lookup() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE copy_users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_copy_users_age ON copy_users (age)",
    )
    .await;
    let csv_path = write_copy_fixture(
        "users",
        "id,name,age\n1,Alice,30\n2,\"Bob, Jr\",42\n3,Carol,NULL\n",
    );

    let msg = exec_ok(
        &executor,
        &format!(
            "COPY copy_users FROM '{}' WITH (FORMAT CSV, HEADER true, NULL 'NULL')",
            csv_path
        ),
    )
    .await;

    assert!(msg.contains("Copied 3 rows"));
    let (_, rows) = query(
        &executor,
        "SELECT id, name, age FROM copy_users WHERE age = 42",
    )
    .await;
    assert_eq!(
        rows,
        vec![vec![
            Value::Integer(2),
            Value::String("Bob, Jr".to_string()),
            Value::Integer(42)
        ]]
    );
    let _ = std::fs::remove_file(csv_path);
    cleanup(&wal);
}

#[tokio::test]
async fn test_copy_from_csv_with_column_list_and_defaults() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE copy_partial (id INTEGER PRIMARY KEY, name TEXT, age INTEGER DEFAULT 99)",
    )
    .await;
    let csv_path = write_copy_fixture("partial", "id,name\n1,Alice\n2,Bob\n");

    let msg = exec_ok(
        &executor,
        &format!(
            "COPY copy_partial (id, name) FROM '{}' WITH (FORMAT CSV, HEADER true)",
            csv_path
        ),
    )
    .await;

    assert!(msg.contains("Copied 2 rows"));
    let (_, rows) = query(&executor, "SELECT * FROM copy_partial ORDER BY id").await;
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::String("Alice".to_string()),
                Value::Integer(99)
            ],
            vec![
                Value::Integer(2),
                Value::String("Bob".to_string()),
                Value::Integer(99)
            ]
        ]
    );
    let _ = std::fs::remove_file(csv_path);
    cleanup(&wal);
}

#[tokio::test]
async fn test_copy_from_csv_accepts_quoted_table_and_columns() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE copy_quoted (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    let csv_path = write_copy_fixture("quoted", "id,name\n1,Alice\n2,Bob\n");

    let msg = exec_ok(
        &executor,
        &format!(
            "COPY \"copy_quoted\" (\"id\", \"name\") FROM '{}' WITH (FORMAT CSV, HEADER true)",
            csv_path
        ),
    )
    .await;

    assert!(msg.contains("Copied 2 rows"));
    let (_, rows) = query(&executor, "SELECT id, name FROM copy_quoted ORDER BY id").await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1), Value::String("Alice".to_string())],
            vec![Value::Integer(2), Value::String("Bob".to_string())],
        ]
    );
    let _ = std::fs::remove_file(csv_path);
    cleanup(&wal);
}

#[tokio::test]
async fn test_copy_from_csv_coerces_epoch_nanoseconds_to_timestamptz() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE copy_tsbs_time (time TIMESTAMPTZ, tags_id INTEGER, usage_user DOUBLE PRECISION)",
    )
    .await;
    let csv_path = write_copy_fixture(
        "tsbs_time",
        "time,tags_id,usage_user\n1451606400000000000,1,84.5\n",
    );

    let msg = exec_ok(
        &executor,
        &format!(
            "COPY \"copy_tsbs_time\" (\"time\", \"tags_id\", \"usage_user\") FROM '{}' WITH (FORMAT CSV, HEADER true)",
            csv_path
        ),
    )
    .await;

    assert!(msg.contains("Copied 1 rows"));
    let (_, rows) = query(
        &executor,
        "SELECT time, tags_id, usage_user FROM copy_tsbs_time",
    )
    .await;
    assert_eq!(
        rows,
        vec![vec![
            Value::Timestamp(1_451_606_400_000_000),
            Value::Integer(1),
            Value::Float(84.5),
        ]]
    );
    let _ = std::fs::remove_file(csv_path);
    cleanup(&wal);
}

#[tokio::test]
async fn test_copy_from_text_coerces_timezone_offset_to_timestamptz() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE copy_tsbs_text_time (time TIMESTAMPTZ, tags_id INTEGER, additional_tags JSONB, boot_time DOUBLE PRECISION)",
    )
    .await;
    let csv_path = write_copy_fixture("tsbs_text_time", "2016-01-01 08:00:00+08:00\t2\t\\N\t84\n");

    let msg = exec_ok(
        &executor,
        &format!(
            "COPY \"copy_tsbs_text_time\" (\"time\", \"tags_id\", \"additional_tags\", \"boot_time\") FROM '{}'",
            csv_path
        ),
    )
    .await;

    assert!(msg.contains("Copied 1 rows"));
    let (_, rows) = query(
        &executor,
        "SELECT time, tags_id, additional_tags, boot_time FROM copy_tsbs_text_time",
    )
    .await;
    assert_eq!(
        rows,
        vec![vec![
            Value::Timestamp(1_451_606_400_000_000),
            Value::Integer(2),
            Value::Null,
            Value::Float(84.0),
        ]]
    );
    let _ = std::fs::remove_file(csv_path);
    cleanup(&wal);
}
