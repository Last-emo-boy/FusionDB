use fusiondb::common::Value;
use fusiondb::execution::Executor;
use fusiondb::storage::memory::MemoryStorage;
use fusiondb::storage::Storage;
use std::sync::Arc;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

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
async fn test_select_count_nullable_column_uses_column_scan() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE count_nullable (id INTEGER PRIMARY KEY, code TEXT, payload TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, code) in [(1_i64, Some("A")), (2, None), (3, Some("C"))] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                code.map(|value| Value::String(value.to_string()))
                    .unwrap_or(Value::Null),
                Value::String(format!("payload-{}", id)),
            ]);
            let corrupt_col_idx = 2usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:count_nullable:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT COUNT(code) AS non_null_codes FROM count_nullable",
    )
    .await;

    assert_eq!(cols, vec!["non_null_codes"]);
    assert_eq!(rows, vec![vec![Value::Integer(2)]]);
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_select_count_nullable_column_with_simple_where_column_scan() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE count_filtered (id INTEGER PRIMARY KEY, status TEXT, code TEXT, payload TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, status, code) in [
            (1_i64, "active", Some("A")),
            (2, "active", None),
            (3, "archived", Some("C")),
            (4, "active", Some("D")),
        ] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(status.to_string()),
                code.map(|value| Value::String(value.to_string()))
                    .unwrap_or(Value::Null),
                Value::String(format!("payload-{}", id)),
            ]);
            let corrupt_col_idx = 3usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:count_filtered:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT COUNT(code) AS active_codes FROM count_filtered WHERE status = 'active'",
    )
    .await;

    assert_eq!(cols, vec!["active_codes"]);
    assert_eq!(rows, vec![vec![Value::Integer(2)]]);
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_select_count_star_with_simple_where_column_scan() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE stock_level_fast (s_id INTEGER PRIMARY KEY, w_id INTEGER, quantity INTEGER, ytd INTEGER, payload TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (s_id, w_id, quantity, ytd) in [
            (1_i64, 1_i64, 10_i64, 100_i64),
            (2, 1, 25, 200),
            (3, 2, 5, 300),
            (4, 1, 19, 400),
        ] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(s_id),
                Value::Integer(w_id),
                Value::Integer(quantity),
                Value::Integer(ytd),
                Value::String(format!("payload-{}", s_id)),
            ]);
            let corrupt_col_idx = 3usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:stock_level_fast:{}",
                fusiondb::common::encoding::encode_i64_comparable(s_id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT COUNT(*) FROM stock_level_fast WHERE w_id = 1 AND quantity < 20",
    )
    .await;

    assert_eq!(cols, vec!["COUNT(*)"]);
    assert_eq!(rows, vec![vec![Value::Integer(2)]]);
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_select_count_reuses_predicate_column_value() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE count_reuse (id INTEGER PRIMARY KEY, category TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO count_reuse VALUES (1, 'A'), (2, 'B'), (3, NULL), (4, 'A')",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT COUNT(category) FROM count_reuse WHERE category = 'A'",
    )
    .await;

    assert_eq!(rows, vec![vec![Value::Integer(2)]]);
    cleanup(&wal);
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
async fn test_group_by_count_fast_path_preserves_null_and_alias() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE visits (id INTEGER PRIMARY KEY, city TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO visits VALUES (1, 'Paris'), (2, 'Rome'), (3, 'Paris'), (4, NULL)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT city AS place, COUNT(*) AS visits FROM visits GROUP BY city",
    )
    .await;

    assert_eq!(cols, vec!["place", "visits"]);
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().any(|row| {
        row[0] == Value::String("Paris".to_string()) && row[1] == Value::Integer(2)
    }));
    assert!(rows
        .iter()
        .any(|row| row[0] == Value::Null && row[1] == Value::Integer(1)));
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

#[tokio::test]
async fn test_group_by_column_aggregates_fast_path_preserves_alias_and_nulls() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, category TEXT, amount INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO orders VALUES (1, 'A', 10), (2, 'A', 30), (3, 'B', 5), (4, NULL, 7)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT category AS cat, SUM(amount) AS total, AVG(amount) AS avg_amount, MIN(amount) AS min_amount, MAX(amount) AS max_amount, COUNT(*) AS n FROM orders GROUP BY category",
    )
    .await;

    assert_eq!(
        cols,
        vec![
            "cat",
            "total",
            "avg_amount",
            "min_amount",
            "max_amount",
            "n"
        ]
    );
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().any(|row| {
        row == &vec![
            Value::String("A".to_string()),
            Value::Integer(40),
            Value::Float(20.0),
            Value::Integer(10),
            Value::Integer(30),
            Value::Integer(2),
        ]
    }));
    assert!(rows.iter().any(|row| {
        row == &vec![
            Value::String("B".to_string()),
            Value::Integer(5),
            Value::Float(5.0),
            Value::Integer(5),
            Value::Integer(5),
            Value::Integer(1),
        ]
    }));
    assert!(rows.iter().any(|row| {
        row == &vec![
            Value::Null,
            Value::Integer(7),
            Value::Float(7.0),
            Value::Integer(7),
            Value::Integer(7),
            Value::Integer(1),
        ]
    }));
    cleanup(&wal);
}

#[tokio::test]
async fn test_group_by_count_column_fast_path_ignores_nulls() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE visits (id INTEGER PRIMARY KEY, city TEXT, user_id INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO visits VALUES (1, 'Paris', 10), (2, 'Paris', NULL), (3, 'Rome', 20), (4, 'Rome', 30), (5, 'Rome', NULL)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT city AS place, COUNT(user_id) AS active_users, COUNT(*) AS visits FROM visits GROUP BY city ORDER BY place",
    )
    .await;

    assert_eq!(cols, vec!["place", "active_users", "visits"]);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::String("Paris".to_string()),
                Value::Integer(1),
                Value::Integer(2),
            ],
            vec![
                Value::String("Rome".to_string()),
                Value::Integer(2),
                Value::Integer(3),
            ],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_group_by_count_distinct_fast_path_ignores_nulls() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE visits (id INTEGER PRIMARY KEY, city TEXT, user_id INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO visits VALUES (1, 'Paris', 10), (2, 'Paris', 10), (3, 'Paris', NULL), (4, 'Rome', 20), (5, 'Rome', 30), (6, 'Rome', 30), (7, 'Rome', NULL)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT city AS place, COUNT(DISTINCT user_id) AS active_users, COUNT(*) AS visits FROM visits GROUP BY city ORDER BY place",
    )
    .await;

    assert_eq!(cols, vec!["place", "active_users", "visits"]);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::String("Paris".to_string()),
                Value::Integer(1),
                Value::Integer(3),
            ],
            vec![
                Value::String("Rome".to_string()),
                Value::Integer(2),
                Value::Integer(4),
            ],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_group_by_column_aggregates_fast_path_uses_only_group_and_aggregate_columns() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE metrics (id INTEGER PRIMARY KEY, category TEXT, amount INTEGER, note TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, category, amount) in [(1_i64, "A", 10_i64), (2, "A", 20), (3, "B", 7)] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(category.to_string()),
                Value::Integer(amount),
                Value::String(format!("note-{}", id)),
            ]);
            let corrupt_col_idx = 3usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:metrics:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (_, rows) = query(
        &executor,
        "SELECT category, SUM(amount), COUNT(*) FROM metrics GROUP BY category",
    )
    .await;

    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| {
        row == &vec![
            Value::String("A".to_string()),
            Value::Integer(30),
            Value::Integer(2),
        ]
    }));
    assert!(rows.iter().any(|row| {
        row == &vec![
            Value::String("B".to_string()),
            Value::Integer(7),
            Value::Integer(1),
        ]
    }));
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_group_by_count_distinct_fast_path_uses_only_group_and_distinct_columns() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE visits (id INTEGER PRIMARY KEY, city TEXT, user_id INTEGER, note TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, city, user_id) in [
            (1_i64, "Paris", 10_i64),
            (2, "Paris", 10),
            (3, "Paris", 20),
            (4, "Rome", 30),
        ] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(city.to_string()),
                Value::Integer(user_id),
                Value::String(format!("note-{}", id)),
            ]);
            let corrupt_col_idx = 3usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:visits:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (_, rows) = query(
        &executor,
        "SELECT city, COUNT(DISTINCT user_id) FROM visits GROUP BY city ORDER BY city",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            vec![Value::String("Paris".to_string()), Value::Integer(2)],
            vec![Value::String("Rome".to_string()), Value::Integer(1)],
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_group_by_count_with_simple_where_uses_column_scan() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE grouped_events (id INTEGER PRIMARY KEY, event_type TEXT, status TEXT, payload TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, event_type, status) in [
            (1_i64, "click", "active"),
            (2, "click", "archived"),
            (3, "search", "active"),
            (4, "click", "active"),
            (5, "signup", "active"),
        ] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(event_type.to_string()),
                Value::String(status.to_string()),
                Value::String(format!("payload-{}", id)),
            ]);
            let corrupt_col_idx = 3usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:grouped_events:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (_, rows) = query(
        &executor,
        "SELECT event_type, COUNT(*) FROM grouped_events WHERE status = 'active' GROUP BY event_type ORDER BY event_type",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            vec![Value::String("click".to_string()), Value::Integer(2)],
            vec![Value::String("search".to_string()), Value::Integer(1)],
            vec![Value::String("signup".to_string()), Value::Integer(1)],
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_group_by_count_with_simple_where_streams_only_needed_columns() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE ldbc_tags (id INTEGER PRIMARY KEY, creation_day INTEGER, tag TEXT, payload TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, creation_day, tag) in [
            (1_i64, 10_i64, "old"),
            (2, 30, "database"),
            (3, 31, "graph"),
            (4, 32, "database"),
            (5, 33, "storage"),
            (6, 34, "database"),
        ] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::Integer(creation_day),
                Value::String(tag.to_string()),
                Value::String(format!("payload-{}", id)),
            ]);
            let corrupt_col_idx = 3usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:ldbc_tags:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT tag, COUNT(*) FROM ldbc_tags WHERE creation_day >= 30 GROUP BY tag ORDER BY COUNT(*) DESC LIMIT 2",
    )
    .await;

    assert_eq!(cols, vec!["tag", "COUNT(*)"]);
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0],
        vec![Value::String("database".to_string()), Value::Integer(3)]
    );
    assert_eq!(rows[1][1], Value::Integer(1));
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_group_by_aggregates_with_simple_where_uses_column_scan() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE grouped_orders (id INTEGER PRIMARY KEY, status TEXT, category TEXT, total INTEGER, payload TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, status, category, total) in [
            (1_i64, "completed", "A", 10_i64),
            (2, "failed", "A", 99),
            (3, "completed", "A", 30),
            (4, "completed", "B", 7),
        ] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(status.to_string()),
                Value::String(category.to_string()),
                Value::Integer(total),
                Value::String(format!("payload-{}", id)),
            ]);
            let corrupt_col_idx = 4usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:grouped_orders:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (_, rows) = query(
        &executor,
        "SELECT category, SUM(total), COUNT(*) FROM grouped_orders WHERE status = 'completed' GROUP BY category ORDER BY category",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            vec![
                Value::String("A".to_string()),
                Value::Integer(40),
                Value::Integer(2),
            ],
            vec![
                Value::String("B".to_string()),
                Value::Integer(7),
                Value::Integer(1),
            ],
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_group_by_aggregates_with_and_where_uses_column_scan() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE grouped_metrics (id INTEGER PRIMARY KEY, region TEXT, ts INTEGER, usage_user FLOAT, usage_system FLOAT, payload TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, region, ts, usage_user, usage_system) in [
            (1_i64, "east", 500_i64, 99.0_f64, 99.0_f64),
            (2, "east", 1000, 10.0, 5.0),
            (3, "east", 2000, 30.0, 8.0),
            (4, "west", 2000, 7.0, 9.0),
            (5, "west", 50000, 100.0, 100.0),
        ] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(region.to_string()),
                Value::Integer(ts),
                Value::Float(usage_user),
                Value::Float(usage_system),
                Value::String(format!("payload-{}", id)),
            ]);
            let corrupt_col_idx = 5usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:grouped_metrics:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (_, rows) = query(
        &executor,
        "SELECT region, AVG(usage_user), MAX(usage_system) FROM grouped_metrics WHERE ts >= 1000 AND ts < 50000 GROUP BY region ORDER BY region",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            vec![
                Value::String("east".to_string()),
                Value::Float(20.0),
                Value::Float(8.0),
            ],
            vec![
                Value::String("west".to_string()),
                Value::Float(7.0),
                Value::Float(9.0),
            ],
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_group_by_aggregates_with_multi_predicate_partial_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE tsbs_rollup (id INTEGER PRIMARY KEY, host_id INTEGER, region TEXT, rack TEXT, ts INTEGER, usage_user FLOAT, usage_system FLOAT, payload TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, region, rack, ts, usage_user, usage_system) in [
            (1_i64, "east", "rack-a", 500_i64, 99.0_f64, 99.0_f64),
            (2, "east", "rack-a", 1000, 10.0, 5.0),
            (3, "east", "rack-b", 2000, 30.0, 8.0),
            (4, "west", "rack-c", 2000, 7.0, 9.0),
            (5, "west", "rack-d", 50000, 100.0, 100.0),
        ] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::Integer(id * 10),
                Value::String(region.to_string()),
                Value::String(rack.to_string()),
                Value::Integer(ts),
                Value::Float(usage_user),
                Value::Float(usage_system),
                Value::String(format!("payload-{}", id)),
            ]);
            for corrupt_col_idx in [1usize, 3usize, 7usize] {
                let off_pos = 2 + corrupt_col_idx * 4;
                let start =
                    u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
                let end = if corrupt_col_idx + 1 < 8 {
                    let next_off_pos = off_pos + 4;
                    u32::from_le_bytes(row[next_off_pos..next_off_pos + 4].try_into().unwrap())
                        as usize
                } else {
                    row.len()
                };
                for byte in &mut row[start..end] {
                    *byte = 0xff;
                }
            }
            let key = format!(
                "data:tsbs_rollup:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (_, rows) = query(
        &executor,
        "SELECT region, AVG(usage_user), MAX(usage_system) FROM tsbs_rollup WHERE ts >= 1000 AND ts < 50000 GROUP BY region ORDER BY region",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            vec![
                Value::String("east".to_string()),
                Value::Float(20.0),
                Value::Float(8.0),
            ],
            vec![
                Value::String("west".to_string()),
                Value::Float(7.0),
                Value::Float(9.0),
            ],
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_group_by_aggregates_reuses_multi_predicate_column_values() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE tsbs_reuse (id INTEGER PRIMARY KEY, region TEXT, ts INTEGER, usage_user FLOAT, payload TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, region, ts, usage_user) in [
            (1_i64, "east", 500_i64, 99.0_f64),
            (2, "east", 1000, 10.0),
            (3, "east", 2000, 30.0),
            (4, "west", 2000, 7.0),
            (5, "west", 50000, 100.0),
        ] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(region.to_string()),
                Value::Integer(ts),
                Value::Float(usage_user),
                Value::String(format!("payload-{}", id)),
            ]);
            let corrupt_col_idx = 4usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:tsbs_reuse:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (_, rows) = query(
        &executor,
        "SELECT region, MIN(ts), MAX(ts), AVG(usage_user) FROM tsbs_reuse WHERE ts >= 1000 AND ts < 50000 GROUP BY region ORDER BY region",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            vec![
                Value::String("east".to_string()),
                Value::Integer(1000),
                Value::Integer(2000),
                Value::Float(20.0),
            ],
            vec![
                Value::String("west".to_string()),
                Value::Integer(2000),
                Value::Integer(2000),
                Value::Float(7.0),
            ],
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_group_by_reuses_predicate_group_column_value() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE grouped_reuse (id INTEGER PRIMARY KEY, category TEXT, total INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO grouped_reuse VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 99), (4, NULL, 5)",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT category, SUM(total), COUNT(*) FROM grouped_reuse WHERE category = 'A' GROUP BY category",
    )
    .await;

    assert_eq!(
        rows,
        vec![vec![
            Value::String("A".to_string()),
            Value::Integer(30),
            Value::Integer(2),
        ]]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_group_by_column_aggregates_fast_path_order_by_limit() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE sales (id INTEGER PRIMARY KEY, category TEXT, amount INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO sales VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 100), (4, 'C', 5), (5, 'C', 6)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT category, SUM(amount) AS total FROM sales GROUP BY category ORDER BY total DESC LIMIT 2",
    )
    .await;

    assert_eq!(cols, vec!["category", "total"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::String("B".to_string()), Value::Integer(100)],
            vec![Value::String("A".to_string()), Value::Integer(30)],
        ]
    );

    let (_, rows) = query(
        &executor,
        "SELECT category, COUNT(*) FROM sales GROUP BY category ORDER BY COUNT(*) DESC LIMIT 1",
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Integer(2));
    assert!(matches!(
        &rows[0][0],
        Value::String(category) if category == "A" || category == "C"
    ));
    cleanup(&wal);
}

#[tokio::test]
async fn test_multi_column_group_by_aggregates_fast_path_order_by_limit() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE ch_orders (o_id INTEGER PRIMARY KEY, w_id INTEGER, status TEXT, total INTEGER, payload TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (o_id, w_id, status, total) in [
            (1_i64, 1_i64, "new", 10_i64),
            (2, 1, "new", 30),
            (3, 1, "paid", 100),
            (4, 2, "paid", 40),
            (5, 2, "paid", 70),
            (6, 2, "new", 5),
        ] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(o_id),
                Value::Integer(w_id),
                Value::String(status.to_string()),
                Value::Integer(total),
                Value::String(format!("payload-{}", o_id)),
            ]);
            let corrupt_col_idx = 4usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:ch_orders:{}",
                fusiondb::common::encoding::encode_i64_comparable(o_id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT w_id, status, SUM(total), COUNT(*) FROM ch_orders GROUP BY w_id, status ORDER BY SUM(total) DESC LIMIT 3",
    )
    .await;

    assert_eq!(cols, vec!["w_id", "status", "SUM(total)", "COUNT(*)"]);
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(2),
                Value::String("paid".to_string()),
                Value::Integer(110),
                Value::Integer(2),
            ],
            vec![
                Value::Integer(1),
                Value::String("paid".to_string()),
                Value::Integer(100),
                Value::Integer(1),
            ],
            vec![
                Value::Integer(1),
                Value::String("new".to_string()),
                Value::Integer(40),
                Value::Integer(2),
            ],
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_group_by_aggregate_order_by_limit_offset_topn_window() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE topn_sales (id INTEGER PRIMARY KEY, category TEXT, amount INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO topn_sales VALUES (1, 'A', 10), (2, 'B', 90), (3, 'C', 50), (4, 'D', 70), (5, 'E', 30)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT category, SUM(amount) FROM topn_sales GROUP BY category ORDER BY SUM(amount) DESC LIMIT 2 OFFSET 1",
    )
    .await;

    assert_eq!(cols, vec!["category", "SUM(amount)"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::String("D".to_string()), Value::Integer(70)],
            vec![Value::String("C".to_string()), Value::Integer(50)],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_group_by_sum_multiply_expr() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, quantity INTEGER, unit_price INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO items VALUES (1, 'A', 2, 10), (2, 'A', 3, 20), (3, 'B', 4, 5)",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT category, SUM(quantity * unit_price) FROM items GROUP BY category ORDER BY category",
    )
    .await;

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0],
        vec![Value::String("A".to_string()), Value::Integer(80)]
    );
    assert_eq!(
        rows[1],
        vec![Value::String("B".to_string()), Value::Integer(20)]
    );
    cleanup(&wal);
}

// ==================== JOIN Tests ====================

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
async fn test_count_distinct_fast_path_ignores_null_with_alias() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE visits (id INTEGER PRIMARY KEY, user_id INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO visits VALUES (1, 10), (2, 20), (3, 10), (4, NULL), (5, 30)",
    )
    .await;

    let (cols, rows) = query(
        &executor,
        "SELECT COUNT(DISTINCT user_id) AS active_users FROM visits",
    )
    .await;

    assert_eq!(cols, vec!["active_users"]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(3));
    cleanup(&wal);
}

#[tokio::test]
async fn test_count_distinct_with_simple_where_uses_column_scan() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE filtered_visits (id INTEGER PRIMARY KEY, status TEXT, user_id INTEGER, payload TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, status, user_id) in [
            (1_i64, "active", Some(10_i64)),
            (2, "active", Some(20)),
            (3, "archived", Some(30)),
            (4, "active", Some(10)),
            (5, "active", None),
        ] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(status.to_string()),
                user_id.map(Value::Integer).unwrap_or(Value::Null),
                Value::String(format!("payload-{}", id)),
            ]);
            let corrupt_col_idx = 3usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:filtered_visits:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT COUNT(DISTINCT user_id) AS active_users FROM filtered_visits WHERE status = 'active'",
    )
    .await;

    assert_eq!(cols, vec!["active_users"]);
    assert_eq!(rows, vec![vec![Value::Integer(2)]]);
    cleanup(&wal_path);
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
async fn test_bare_sum_avg_column_scan_uses_only_aggregate_columns() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sales (id INTEGER PRIMARY KEY, amount INTEGER, note TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, amount) in [(1_i64, 10_i64), (2, 20), (3, 30)] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::Integer(amount),
                Value::String(format!("note-{}", id)),
            ]);
            let corrupt_col_idx = 2usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:sales:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT SUM(amount) AS total_amount, AVG(amount) AS avg_amount FROM sales",
    )
    .await;

    assert_eq!(cols, vec!["total_amount", "avg_amount"]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(60));
    assert_eq!(rows[0][1], Value::Float(20.0));
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_bare_min_max_column_scan_uses_only_aggregate_columns() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE metrics (id INTEGER PRIMARY KEY, score INTEGER, label TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, score) in [(1_i64, 30_i64), (2, 10), (3, 50), (4, 20)] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::Integer(score),
                Value::String(format!("label-{}", id)),
            ]);
            let corrupt_col_idx = 2usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:metrics:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT MIN(score) AS low_score, MAX(score) AS high_score FROM metrics",
    )
    .await;

    assert_eq!(cols, vec!["low_score", "high_score"]);
    assert_eq!(rows, vec![vec![Value::Integer(10), Value::Integer(50)]]);
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_bare_sum_avg_with_simple_where_column_scan() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, total INTEGER, note TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, status, total) in [
            (1_i64, "delivered", 10_i64),
            (2, "cancelled", 99),
            (3, "delivered", 30),
            (4, "shipped", 20),
        ] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(status.to_string()),
                Value::Integer(total),
                Value::String(format!("note-{}", id)),
            ]);
            let corrupt_col_idx = 3usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:orders:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT SUM(total) AS revenue, AVG(total) AS avg_order FROM orders WHERE status != 'cancelled'",
    )
    .await;
    assert_eq!(cols, vec!["revenue", "avg_order"]);
    assert_eq!(rows, vec![vec![Value::Integer(60), Value::Float(20.0)]]);

    let (_, rows) = query(
        &executor,
        "SELECT AVG(total) FROM orders WHERE status = 'delivered'",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Float(20.0)]]);
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_bare_min_max_with_simple_where_column_scan() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, total INTEGER, note TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, status, total) in [
            (1_i64, "delivered", 10_i64),
            (2, "cancelled", 99),
            (3, "delivered", 30),
            (4, "shipped", 20),
        ] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(status.to_string()),
                Value::Integer(total),
                Value::String(format!("note-{}", id)),
            ]);
            let corrupt_col_idx = 3usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:orders:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT MIN(total) AS min_total, MAX(total) AS max_total FROM orders WHERE status != 'cancelled'",
    )
    .await;

    assert_eq!(cols, vec!["min_total", "max_total"]);
    assert_eq!(rows, vec![vec![Value::Integer(10), Value::Integer(30)]]);
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_bare_string_agg_column_scan_uses_only_aggregate_columns() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE names (id INTEGER PRIMARY KEY, name TEXT, note TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name) in [(1_i64, "Alice"), (2, "Bob"), (3, "Carol")] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(name.to_string()),
                Value::String(format!("note-{}", id)),
            ]);
            let corrupt_col_idx = 2usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:names:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(&executor, "SELECT STRING_AGG(name) AS all_names FROM names").await;

    assert_eq!(cols, vec!["all_names"]);
    assert_eq!(rows.len(), 1);
    if let Value::String(names) = &rows[0][0] {
        assert!(names.contains("Alice"));
        assert!(names.contains("Bob"));
        assert!(names.contains("Carol"));
    } else {
        panic!("STRING_AGG should return a string for non-empty input");
    }
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_bare_group_concat_with_simple_where_column_scan() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE events (id INTEGER PRIMARY KEY, status TEXT, name TEXT, note TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, status, name) in [
            (1_i64, "active", "Alice"),
            (2, "archived", "Bob"),
            (3, "active", "Carol"),
            (4, "active", "Dave"),
        ] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(status.to_string()),
                Value::String(name.to_string()),
                Value::String(format!("note-{}", id)),
            ]);
            let corrupt_col_idx = 3usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:events:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT GROUP_CONCAT(name) AS active_names FROM events WHERE status = 'active'",
    )
    .await;

    assert_eq!(cols, vec!["active_names"]);
    assert_eq!(rows.len(), 1);
    if let Value::String(names) = &rows[0][0] {
        assert!(names.contains("Alice"));
        assert!(!names.contains("Bob"));
        assert!(names.contains("Carol"));
        assert!(names.contains("Dave"));
    } else {
        panic!("GROUP_CONCAT should return a string for non-empty input");
    }
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_bare_aggregate_sum_multiply_expr() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, quantity INTEGER, unit_price INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO items VALUES (1, 2, 10), (2, 3, 20), (3, 4, 5)",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT SUM(quantity * unit_price) FROM items").await;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(100));
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
async fn test_group_concat_group_by_fast_path_ignores_nulls() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE gc (id INTEGER PRIMARY KEY, grp TEXT, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO gc VALUES (1, 'A', 'Alice'), (2, 'A', NULL), (3, 'A', 'Bob'), (4, 'B', 'Carol')",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT grp, GROUP_CONCAT(name) FROM gc GROUP BY grp ORDER BY grp",
    )
    .await;

    assert_eq!(rows.len(), 2);
    if let Value::String(names) = &rows[0][1] {
        assert!(names.contains("Alice"));
        assert!(names.contains("Bob"));
        assert!(!names.contains("NULL"));
    } else {
        panic!("GROUP_CONCAT should return a string for non-empty groups");
    }
    assert_eq!(rows[1][1], Value::String("Carol".to_string()));
    cleanup(&wal);
}

#[tokio::test]
async fn test_string_agg_group_by_fast_path_uses_only_group_and_value_columns() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE sa_fast (id INTEGER PRIMARY KEY, grp TEXT, name TEXT, note TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, grp, name) in [(1_i64, "A", "Alice"), (2, "A", "Bob"), (3, "B", "Carol")] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(grp.to_string()),
                Value::String(name.to_string()),
                Value::String(format!("note-{}", id)),
            ]);
            let corrupt_col_idx = 3usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:sa_fast:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (_, rows) = query(
        &executor,
        "SELECT grp, STRING_AGG(name) FROM sa_fast GROUP BY grp ORDER BY grp",
    )
    .await;

    assert_eq!(rows.len(), 2);
    if let Value::String(names) = &rows[0][1] {
        assert!(names.contains("Alice"));
        assert!(names.contains("Bob"));
    } else {
        panic!("STRING_AGG should return a string for group A");
    }
    assert_eq!(rows[1][1], Value::String("Carol".to_string()));
    cleanup(&wal_path);
}
