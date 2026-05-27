use fusiondb::common::Value;
use fusiondb::execution::{Executor, QueryResult};
use fusiondb::storage::memory::MemoryStorage;
use fusiondb::storage::Storage;
use std::sync::Arc;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

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
