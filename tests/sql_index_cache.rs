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
