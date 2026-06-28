use fusiondb::common::Value;
use fusiondb::execution::Executor;
use fusiondb::storage::memory::MemoryStorage;
use fusiondb::storage::Storage;
use std::sync::Arc;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

#[tokio::test]
async fn test_select_all() {
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
    let (cols, rows) = query(&executor, "SELECT * FROM users").await;
    assert_eq!(cols, vec!["id", "name"]);
    assert_eq!(rows.len(), 2);
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_with_where_eq() {
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
    let (_, rows) = query(&executor, "SELECT * FROM users WHERE id = 2").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::String("Bob".to_string()));
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_with_where_gt() {
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
    let (_, rows) = query(&executor, "SELECT * FROM nums WHERE id > 1").await;
    assert_eq!(rows.len(), 2);
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_with_limit() {
    let (executor, wal) = setup().await;
    exec_ok(&executor, "CREATE TABLE nums (id INTEGER PRIMARY KEY)").await;
    exec_ok(&executor, "INSERT INTO nums VALUES (1), (2), (3), (4), (5)").await;
    let (_, rows) = query(&executor, "SELECT * FROM nums LIMIT 3").await;
    assert_eq!(rows.len(), 3);
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_with_limit_offset() {
    let (executor, wal) = setup().await;
    exec_ok(&executor, "CREATE TABLE nums (id INTEGER PRIMARY KEY)").await;
    exec_ok(&executor, "INSERT INTO nums VALUES (1), (2), (3), (4), (5)").await;
    let (_, rows) = query(&executor, "SELECT * FROM nums LIMIT 2 OFFSET 2").await;
    assert_eq!(rows.len(), 2);
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_order_by_asc() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO nums VALUES (3, 30), (1, 10), (2, 20)",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM nums ORDER BY val ASC").await;
    assert_eq!(rows[0][1], Value::Integer(10));
    assert_eq!(rows[1][1], Value::Integer(20));
    assert_eq!(rows[2][1], Value::Integer(30));
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_order_by_desc() {
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
    let (_, rows) = query(&executor, "SELECT * FROM nums ORDER BY val DESC").await;
    assert_eq!(rows[0][1], Value::Integer(30));
    assert_eq!(rows[2][1], Value::Integer(10));
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_order_by_limit_offset() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE order_window (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO order_window VALUES (1, 50), (2, 10), (3, 40), (4, 20), (5, 30)",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT id, val FROM order_window ORDER BY val ASC LIMIT 2 OFFSET 1",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(4), Value::Integer(20)],
            vec![Value::Integer(5), Value::Integer(30)]
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_order_by_alias() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE alias_sort (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO alias_sort VALUES (1, 10), (2, 30), (3, 20)",
    )
    .await;
    let (cols, rows) = query(
        &executor,
        "SELECT val * 2 AS doubled FROM alias_sort ORDER BY doubled DESC",
    )
    .await;
    assert_eq!(cols, vec!["doubled"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(60)],
            vec![Value::Integer(40)],
            vec![Value::Integer(20)]
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_order_by_ordinal() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE ordinal_sort (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO ordinal_sort VALUES (1, 10), (2, 30), (3, 20)",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT id, val FROM ordinal_sort ORDER BY 2 DESC",
    )
    .await;
    assert_eq!(rows[0], vec![Value::Integer(2), Value::Integer(30)]);
    assert_eq!(rows[1], vec![Value::Integer(3), Value::Integer(20)]);
    assert_eq!(rows[2], vec![Value::Integer(1), Value::Integer(10)]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_projection() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO users VALUES (1, 'Alice', 30)").await;
    let (cols, rows) = query(&executor, "SELECT name FROM users WHERE id = 1").await;
    assert_eq!(cols, vec!["name"]);
    assert_eq!(rows[0][0], Value::String("Alice".to_string()));
    cleanup(&wal);
}

#[tokio::test]
async fn test_wide_select_projection_skips_unused_tail_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE wide_projection (
            id INTEGER PRIMARY KEY,
            c01 TEXT, c02 TEXT, c03 TEXT, c04 TEXT,
            c05 TEXT, c06 TEXT, c07 TEXT, c08 TEXT,
            c09 TEXT, c10 TEXT, c11 TEXT, c12 TEXT
        )",
    )
    .await;

    let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
        Value::Integer(1),
        Value::String("v01".to_string()),
        Value::String("v02".to_string()),
        Value::String("v03".to_string()),
        Value::String("v04".to_string()),
        Value::String("v05".to_string()),
        Value::String("v06".to_string()),
        Value::String("v07".to_string()),
        Value::String("v08".to_string()),
        Value::String("v09".to_string()),
        Value::String("v10".to_string()),
        Value::String("v11".to_string()),
        Value::String("unused-tail".to_string()),
    ]);
    let corrupt_col_idx = 12usize;
    let off_pos = 2 + corrupt_col_idx * 4;
    let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
    for byte in &mut row[start..] {
        *byte = 0xff;
    }

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let key = format!(
            "data:wide_projection:{}",
            fusiondb::common::encoding::encode_i64_comparable(1)
        );
        txn.put(key.as_bytes(), &row).await.unwrap();
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(&executor, "SELECT c11, c03, c07, c01 FROM wide_projection").await;

    assert_eq!(cols, vec!["c11", "c03", "c07", "c01"]);
    assert_eq!(
        rows,
        vec![vec![
            Value::String("v11".to_string()),
            Value::String("v03".to_string()),
            Value::String("v07".to_string()),
            Value::String("v01".to_string()),
        ]]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_select_constant_projection_from_table() {
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

    let (cols, rows) = query(&executor, "SELECT 1 FROM nums").await;
    assert_eq!(cols, vec!["1"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(1)],
            vec![Value::Integer(1)]
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_in_list() {
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
    let (_, rows) = query(&executor, "SELECT * FROM users WHERE id IN (1, 3)").await;
    assert_eq!(rows.len(), 2);
    exec_ok(&executor, "CREATE INDEX idx_users_in_name ON users (name)").await;
    let (_, rows) = query(
        &executor,
        "SELECT id FROM users WHERE name IN ('Alice', 'Charlie') ORDER BY id",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
    cleanup(&wal);
}

// ==================== UPDATE Tests ====================

#[tokio::test]
async fn test_select_empty_table() {
    let (executor, wal) = setup().await;
    exec_ok(&executor, "CREATE TABLE empty (id INTEGER PRIMARY KEY)").await;
    let (_, rows) = query(&executor, "SELECT * FROM empty").await;
    assert_eq!(rows.len(), 0);
    cleanup(&wal);
}

#[tokio::test]
async fn test_table_not_found_error() {
    let (executor, wal) = setup().await;
    let stmts = executor.prepare("SELECT * FROM nonexistent").unwrap();
    let result = executor.execute(&stmts[0]).await;
    assert!(result.is_err());
    cleanup(&wal);
}

#[tokio::test]
async fn test_null_handling() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO data VALUES (1, NULL)").await;
    let (_, rows) = query(&executor, "SELECT * FROM data WHERE id = 1").await;
    assert_eq!(rows[0][1], Value::Null);
    cleanup(&wal);
}

// ==================== Advanced Expression Tests ====================

#[tokio::test]
async fn test_where_and() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO items VALUES (1, 'A', 10), (2, 'B', 20), (3, 'A', 30)",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT * FROM items WHERE name = 'A' AND price > 15",
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][2], Value::Integer(30));
    cleanup(&wal);
}

#[tokio::test]
async fn test_where_or() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO items VALUES (1, 'A'), (2, 'B'), (3, 'C')",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT * FROM items WHERE name = 'A' OR name = 'C'",
    )
    .await;
    assert_eq!(rows.len(), 2);
    cleanup(&wal);
}

#[tokio::test]
async fn test_is_null() {
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
    let (_, rows) = query(&executor, "SELECT * FROM data WHERE val IS NULL").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2));
    cleanup(&wal);
}

#[tokio::test]
async fn test_between() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO nums VALUES (1, 5), (2, 15), (3, 25), (4, 35)",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT * FROM nums WHERE val BETWEEN 10 AND 30").await;
    assert_eq!(rows.len(), 2);
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_distinct() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE tags (id INTEGER PRIMARY KEY, tag TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO tags VALUES (1, 'rust'), (2, 'db'), (3, 'rust'), (4, 'db'), (5, 'sql')",
    )
    .await;
    let (_, rows) = query(&executor, "SELECT DISTINCT tag FROM tags").await;
    assert_eq!(rows.len(), 3);
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_distinct_fast_path_preserves_null_and_alias() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE visits (id INTEGER PRIMARY KEY, city TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO visits VALUES (1, 'Paris'), (2, 'Rome'), (3, 'Paris'), (4, NULL), (5, NULL)",
    )
    .await;

    let (cols, rows) = query(&executor, "SELECT DISTINCT city AS place FROM visits").await;

    assert_eq!(cols, vec!["place"]);
    assert_eq!(rows.len(), 3);
    assert!(rows
        .iter()
        .any(|row| row[0] == fusiondb::common::Value::Null));
    cleanup(&wal);
}

#[tokio::test]
async fn test_select_distinct_with_simple_where_uses_column_scan() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE distinct_visits (id INTEGER PRIMARY KEY, status TEXT, city TEXT, payload TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, status, city) in [
            (1_i64, "active", Some("Paris")),
            (2, "active", Some("Rome")),
            (3, "archived", Some("Berlin")),
            (4, "active", Some("Paris")),
            (5, "active", None),
        ] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(status.to_string()),
                city.map(|value| Value::String(value.to_string()))
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
                "data:distinct_visits:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT DISTINCT city AS place FROM distinct_visits WHERE status = 'active'",
    )
    .await;

    assert_eq!(cols, vec!["place"]);
    assert_eq!(rows.len(), 3);
    assert!(rows
        .iter()
        .any(|row| row[0] == Value::String("Paris".to_string())));
    assert!(rows
        .iter()
        .any(|row| row[0] == Value::String("Rome".to_string())));
    assert!(rows.iter().any(|row| row[0] == Value::Null));
    assert!(!rows
        .iter()
        .any(|row| row[0] == Value::String("Berlin".to_string())));
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_select_distinct_order_limit_uses_column_scan() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE distinct_order (id INTEGER PRIMARY KEY, city TEXT, payload TEXT)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, city) in [
            (1_i64, "Paris"),
            (2, "Berlin"),
            (3, "Tokyo"),
            (4, "Paris"),
            (5, "Rome"),
        ] {
            let mut row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::String(city.to_string()),
                Value::String(format!("payload-{}", id)),
            ]);
            let corrupt_col_idx = 2usize;
            let off_pos = 2 + corrupt_col_idx * 4;
            let start = u32::from_le_bytes(row[off_pos..off_pos + 4].try_into().unwrap()) as usize;
            for byte in &mut row[start..] {
                *byte = 0xff;
            }
            let key = format!(
                "data:distinct_order:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT DISTINCT city FROM distinct_order ORDER BY city LIMIT 2 OFFSET 1",
    )
    .await;

    assert_eq!(cols, vec!["city"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::String("Paris".to_string())],
            vec![Value::String("Rome".to_string())],
        ]
    );
    cleanup(&wal_path);
}

#[tokio::test]
async fn test_select_without_from() {
    let (executor, wal) = setup().await;
    let (_cols, rows) = query(&executor, "SELECT 1 + 2").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], fusiondb::common::Value::Integer(3));
    cleanup(&wal);
}

#[tokio::test]
async fn test_multi_column_order_by() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE mco (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO mco VALUES (1, 'A', 30), (2, 'B', 10), (3, 'A', 10), (4, 'B', 30)",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT grp, val FROM mco ORDER BY grp ASC, val DESC",
    )
    .await;
    // A,30 then A,10 then B,30 then B,10
    assert_eq!(rows[0][0], fusiondb::common::Value::String("A".to_string()));
    assert_eq!(rows[0][1], fusiondb::common::Value::Integer(30));
    assert_eq!(rows[1][1], fusiondb::common::Value::Integer(10));
    assert_eq!(rows[2][0], fusiondb::common::Value::String("B".to_string()));
    assert_eq!(rows[2][1], fusiondb::common::Value::Integer(30));
    cleanup(&wal);
}

// --- BENCHPROD-437: LIMIT pushdown into filtered (unordered) scans ---

async fn seed_limit_table(executor: &Executor) {
    exec_ok(
        executor,
        "CREATE TABLE lp (id INTEGER PRIMARY KEY, v INTEGER, cat TEXT)",
    )
    .await;
    // 50 rows: v == id (1..=50), cat alternates 'a'/'b'. No secondary index -> full scan path.
    for i in 1..=50 {
        let cat = if i % 2 == 0 { "a" } else { "b" };
        exec_ok(
            executor,
            &format!("INSERT INTO lp VALUES ({}, {}, '{}')", i, i, cat),
        )
        .await;
    }
}

#[tokio::test]
async fn test_filtered_limit_returns_exactly_n_matching_rows() {
    let (executor, wal) = setup().await;
    seed_limit_table(&executor).await;
    // 41 rows match (v >= 10); LIMIT 5 must return exactly 5, all satisfying the predicate.
    let (_, rows) = query(&executor, "SELECT * FROM lp WHERE v >= 10 LIMIT 5").await;
    assert_eq!(rows.len(), 5, "filtered LIMIT must return exactly N rows");
    for row in &rows {
        match &row[1] {
            Value::Integer(v) => assert!(*v >= 10, "row violates predicate: {}", v),
            other => panic!("unexpected v: {:?}", other),
        }
    }
    cleanup(&wal);
}

#[tokio::test]
async fn test_filtered_limit_offset_returns_exactly_n_rows() {
    let (executor, wal) = setup().await;
    seed_limit_table(&executor).await;
    let (_, rows) = query(&executor, "SELECT * FROM lp WHERE v >= 10 LIMIT 5 OFFSET 3").await;
    assert_eq!(rows.len(), 5);
    for row in &rows {
        if let Value::Integer(v) = &row[1] {
            assert!(*v >= 10);
        }
    }
    cleanup(&wal);
}

#[tokio::test]
async fn test_filtered_limit_does_not_truncate_bare_aggregate() {
    // Critical guard: a bare aggregate with WHERE + LIMIT must aggregate over ALL matching
    // rows, not be early-broken by the pushed scan limit.
    let (executor, wal) = setup().await;
    seed_limit_table(&executor).await;
    let (_, rows) = query(&executor, "SELECT COUNT(*) FROM lp WHERE v >= 10 LIMIT 1").await;
    assert_eq!(
        rows[0][0],
        Value::Integer(41),
        "COUNT must see all matching rows"
    );

    let (_, sum_rows) = query(&executor, "SELECT SUM(v) FROM lp WHERE v >= 10 LIMIT 1").await;
    // sum of 10..=50 = (10+50)*41/2 = 1230
    assert_eq!(
        sum_rows[0][0],
        Value::Integer(1230),
        "SUM must see all matching rows"
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_filtered_limit_distinct_not_truncated_before_dedup() {
    // DISTINCT with WHERE + LIMIT must dedup over all matching rows first; only 2 distinct cats.
    let (executor, wal) = setup().await;
    seed_limit_table(&executor).await;
    let (_, rows) = query(
        &executor,
        "SELECT DISTINCT cat FROM lp WHERE v >= 10 LIMIT 5",
    )
    .await;
    assert_eq!(
        rows.len(),
        2,
        "DISTINCT must yield all distinct values, not be early-broken"
    );
    cleanup(&wal);
}
