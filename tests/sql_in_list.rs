use fusiondb::common::Value;
use fusiondb::execution::Executor;
use fusiondb::storage::{memory::MemoryStorage, Storage};
use std::sync::Arc;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

// BENCHPROD-438: IN-list evaluation resolves the comparison column's data type once
// per row and coerces each list item to it, instead of re-running the full alignment
// (which re-resolved the column index) for every list item. These tests prove the
// result is identical to the equivalent OR-expansion, including type coercion.

fn corrupt_encoded_column(row: &mut [u8], column_index: usize, column_count: usize) {
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

#[tokio::test]
async fn test_in_list_mixed_numeric_matches_or_expansion() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO nums VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f')",
    )
    .await;

    // Mixed numeric IN list (integer literals and a float literal) against an INTEGER
    // column: the float `4.0` must coerce to integer 4 and match row id = 4.
    let (_, in_rows) = query(
        &executor,
        "SELECT id FROM nums WHERE id IN (2, 4.0, 6) ORDER BY id",
    )
    .await;
    let (_, or_rows) = query(
        &executor,
        "SELECT id FROM nums WHERE id = 2 OR id = 4.0 OR id = 6 ORDER BY id",
    )
    .await;

    assert_eq!(in_rows, or_rows);
    assert_eq!(
        in_rows,
        vec![
            vec![Value::Integer(2)],
            vec![Value::Integer(4)],
            vec![Value::Integer(6)],
        ]
    );

    cleanup(&wal);
}

#[tokio::test]
async fn test_not_in_list_mixed_numeric_matches_or_expansion() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE nums (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO nums VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f')",
    )
    .await;

    let (_, not_in_rows) = query(
        &executor,
        "SELECT id FROM nums WHERE id NOT IN (2, 4.0, 6) ORDER BY id",
    )
    .await;
    let (_, or_rows) = query(
        &executor,
        "SELECT id FROM nums WHERE NOT (id = 2 OR id = 4.0 OR id = 6) ORDER BY id",
    )
    .await;

    assert_eq!(not_in_rows, or_rows);
    assert_eq!(
        not_in_rows,
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(3)],
            vec![Value::Integer(5)],
        ]
    );

    cleanup(&wal);
}

#[tokio::test]
async fn test_in_list_null_membership_uses_sql_three_valued_logic() {
    let (executor, wal) = setup().await;
    exec_ok(&executor, "CREATE TABLE nums (id INTEGER, label TEXT)").await;
    exec_ok(
        &executor,
        "INSERT INTO nums VALUES (1, 'one'), (2, 'two'), (NULL, 'null-probe')",
    )
    .await;

    let (_, in_rows) = query(
        &executor,
        "SELECT label FROM nums WHERE id IN (2, NULL) ORDER BY label",
    )
    .await;
    assert_eq!(in_rows, vec![vec![Value::String("two".to_string())]]);

    let (_, not_in_rows) = query(
        &executor,
        "SELECT label FROM nums WHERE id NOT IN (2, NULL) ORDER BY label",
    )
    .await;
    assert!(not_in_rows.is_empty());

    let (_, not_in_wrapped_rows) = query(
        &executor,
        "SELECT label FROM nums WHERE NOT (id IN (2, NULL)) ORDER BY label",
    )
    .await;
    assert_eq!(not_in_wrapped_rows, not_in_rows);

    let (_, not_wrapped_rows) = query(
        &executor,
        "SELECT label FROM nums WHERE NOT (id NOT IN (2, NULL)) ORDER BY label",
    )
    .await;
    assert_eq!(
        not_wrapped_rows,
        vec![vec![Value::String("two".to_string())]]
    );

    cleanup(&wal);
}

#[tokio::test]
async fn test_in_list_string_column_matches_or_expansion() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO people VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'Dave')",
    )
    .await;

    let (_, in_rows) = query(
        &executor,
        "SELECT id, name FROM people WHERE name IN ('Bob', 'Dave', 'Zed') ORDER BY id",
    )
    .await;
    let (_, or_rows) = query(
        &executor,
        "SELECT id, name FROM people WHERE name = 'Bob' OR name = 'Dave' OR name = 'Zed' ORDER BY id",
    )
    .await;

    assert_eq!(in_rows, or_rows);
    assert_eq!(
        in_rows,
        vec![
            vec![Value::Integer(2), Value::String("Bob".to_string())],
            vec![Value::Integer(4), Value::String("Dave".to_string())],
        ]
    );

    cleanup(&wal);
}

#[tokio::test]
async fn test_predicate_first_in_list_small_scan_matches_expected_order() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE in_pf_small (id INTEGER PRIMARY KEY, bucket INTEGER, payload TEXT)",
    )
    .await;

    let mut sql = String::from("INSERT INTO in_pf_small VALUES ");
    for id in 1..=80 {
        if id > 1 {
            sql.push(',');
        }
        sql.push_str(&format!("({}, {}, 'payload_{}')", id, id % 10, id));
    }
    sql.push_str(",(81, NULL, 'payload_null')");
    exec_ok(&executor, &sql).await;

    let (_, all_rows) = query(&executor, "SELECT id, bucket FROM in_pf_small").await;
    let expected: Vec<Vec<Value>> = all_rows
        .into_iter()
        .filter(|row| matches!(row[1], Value::Integer(2 | 4)))
        .map(|row| vec![row[0].clone()])
        .collect();

    let (_, in_rows) = query(
        &executor,
        "SELECT id FROM in_pf_small WHERE bucket IN (2, 4.0, NULL)",
    )
    .await;
    assert_eq!(
        in_rows, expected,
        "serial predicate-first IN must preserve scan order and filter NULL probes"
    );

    cleanup(&wal);
}

#[tokio::test]
async fn test_predicate_first_in_list_large_scan_matches_expected_order() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE in_pf (id INTEGER PRIMARY KEY, bucket INTEGER, payload TEXT)",
    )
    .await;

    let mut sql = String::from("INSERT INTO in_pf VALUES ");
    for id in 1..=1500 {
        if id > 1 {
            sql.push(',');
        }
        sql.push_str(&format!("({}, {}, 'payload_{}')", id, id % 10, id));
    }
    sql.push_str(",(1501, NULL, 'payload_null')");
    exec_ok(&executor, &sql).await;

    let (_, all_rows) = query(&executor, "SELECT id, bucket FROM in_pf").await;
    let expected: Vec<Vec<Value>> = all_rows
        .into_iter()
        .filter(|row| matches!(row[1], Value::Integer(2 | 4)))
        .map(|row| vec![row[0].clone()])
        .collect();
    assert!(expected.len() > 100);

    let (_, in_rows) = query(
        &executor,
        "SELECT id FROM in_pf WHERE bucket IN (2, 4.0, NULL)",
    )
    .await;
    let (_, or_rows) = query(
        &executor,
        "SELECT id FROM in_pf WHERE bucket = 2 OR bucket = 4.0 OR bucket = NULL",
    )
    .await;
    assert_eq!(or_rows, expected);
    assert_eq!(
        in_rows, expected,
        "predicate-first IN must preserve scan order and SQL NULL semantics"
    );

    cleanup(&wal);
}

#[tokio::test]
async fn test_predicate_first_in_list_limit_matches_expected_order() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE in_pf_limit (id INTEGER PRIMARY KEY, bucket INTEGER, payload TEXT)",
    )
    .await;

    let mut sql = String::from("INSERT INTO in_pf_limit VALUES ");
    for id in 1..=120 {
        if id > 1 {
            sql.push(',');
        }
        sql.push_str(&format!("({}, {}, 'payload_{}')", id, id % 10, id));
    }
    sql.push_str(",(121, NULL, 'payload_null')");
    exec_ok(&executor, &sql).await;

    let (_, all_rows) = query(&executor, "SELECT id, bucket FROM in_pf_limit").await;
    let expected: Vec<Vec<Value>> = all_rows
        .into_iter()
        .filter(|row| matches!(row[1], Value::Integer(2 | 4)))
        .take(7)
        .map(|row| vec![row[0].clone()])
        .collect();

    let (_, in_rows) = query(
        &executor,
        "SELECT id FROM in_pf_limit WHERE bucket IN (2, 4.0, NULL) LIMIT 7",
    )
    .await;
    assert_eq!(
        in_rows, expected,
        "LIMIT predicate-first IN must stop after matching rows, not visited rows"
    );

    cleanup(&wal);
}

#[tokio::test]
async fn test_predicate_first_in_list_reuses_row_cache() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE in_pf_cache (id INTEGER PRIMARY KEY, bucket INTEGER, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO in_pf_cache VALUES (1, 1, 'one'), (2, 2, 'two')",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT * FROM in_pf_cache").await;
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(1),
                Value::Integer(1),
                Value::String("one".to_string())
            ],
            vec![
                Value::Integer(2),
                Value::Integer(2),
                Value::String("two".to_string())
            ]
        ]
    );

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, bucket, payload) in [(1_i64, 1_i64, "one"), (2_i64, 2_i64, "two")] {
            let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::Integer(bucket),
                Value::String(payload.to_string()),
            ]);
            corrupt_encoded_column(&mut corrupt_row, 1, 3);
            let key = format!(
                "data:in_pf_cache:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT id, payload FROM in_pf_cache WHERE bucket IN (2, NULL)",
    )
    .await;
    assert_eq!(cols, vec!["id", "payload"]);
    assert_eq!(
        rows,
        vec![vec![Value::Integer(2), Value::String("two".to_string())]]
    );

    cleanup(&wal_path);
}

#[tokio::test]
async fn test_predicate_first_or_equality_skips_nonmatching_row_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE or_pf_decode (id INTEGER PRIMARY KEY, bucket INTEGER, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO or_pf_decode VALUES (1, 1, 'one'), (2, 2, 'two'), (3, NULL, 'null-probe')",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, bucket, payload) in [
            (1_i64, Value::Integer(1), "one"),
            (3_i64, Value::Null, "null-probe"),
        ] {
            let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                bucket,
                Value::String(payload.to_string()),
            ]);
            corrupt_encoded_column(&mut corrupt_row, 2, 3);
            let key = format!(
                "data:or_pf_decode:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let query_executor = Arc::new(Executor::new(storage.clone()));
    let (cols, rows) = query(
        &query_executor,
        "SELECT payload FROM or_pf_decode WHERE bucket = 2 OR bucket = 4.0 OR bucket = NULL",
    )
    .await;
    assert_eq!(cols, vec!["payload"]);
    assert_eq!(rows, vec![vec![Value::String("two".to_string())]]);

    cleanup(&wal_path);
}

#[tokio::test]
async fn test_predicate_first_or_equality_large_scan_matches_expected_order() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE or_pf_large (id INTEGER PRIMARY KEY, bucket INTEGER, payload TEXT)",
    )
    .await;

    let mut sql = String::from("INSERT INTO or_pf_large VALUES ");
    for id in 1..=1500 {
        if id > 1 {
            sql.push(',');
        }
        sql.push_str(&format!("({}, {}, 'payload_{}')", id, id % 10, id));
    }
    sql.push_str(",(1501, NULL, 'payload_null')");
    exec_ok(&executor, &sql).await;

    let (_, all_rows) = query(&executor, "SELECT id, bucket FROM or_pf_large").await;
    let expected: Vec<Vec<Value>> = all_rows
        .into_iter()
        .filter(|row| matches!(row[1], Value::Integer(2 | 4)))
        .map(|row| vec![row[0].clone()])
        .collect();

    let (_, or_rows) = query(
        &executor,
        "SELECT id FROM or_pf_large WHERE bucket = 2 OR bucket = 4.0 OR bucket = NULL",
    )
    .await;
    assert_eq!(
        or_rows, expected,
        "parallel predicate-first OR equality must preserve scan order and 3VL"
    );

    cleanup(&wal);
}

#[tokio::test]
async fn test_predicate_first_or_equality_limit_matches_expected_order() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE or_pf_limit (id INTEGER PRIMARY KEY, bucket INTEGER, payload TEXT)",
    )
    .await;

    let mut sql = String::from("INSERT INTO or_pf_limit VALUES ");
    for id in 1..=120 {
        if id > 1 {
            sql.push(',');
        }
        sql.push_str(&format!("({}, {}, 'payload_{}')", id, id % 10, id));
    }
    sql.push_str(",(121, NULL, 'payload_null')");
    exec_ok(&executor, &sql).await;

    let (_, all_rows) = query(&executor, "SELECT id, bucket FROM or_pf_limit").await;
    let expected: Vec<Vec<Value>> = all_rows
        .into_iter()
        .filter(|row| matches!(row[1], Value::Integer(2 | 4)))
        .take(7)
        .map(|row| vec![row[0].clone()])
        .collect();

    let (_, actual) = query(
        &executor,
        "SELECT id FROM or_pf_limit WHERE bucket = 2 OR bucket = 4.0 OR bucket = NULL LIMIT 7",
    )
    .await;
    assert_eq!(
        actual, expected,
        "OR equality LIMIT must count matched rows, not visited rows"
    );

    cleanup(&wal);
}

#[tokio::test]
async fn test_predicate_first_or_equality_reversed_and_all_null_edges() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE or_pf_edges (id INTEGER PRIMARY KEY, bucket INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO or_pf_edges VALUES (1, 2), (2, 4), (3, NULL), (4, 5)",
    )
    .await;

    let (_, reversed_rows) = query(
        &executor,
        "SELECT id FROM or_pf_edges WHERE 2 = bucket OR 4.0 = bucket",
    )
    .await;
    assert_eq!(
        reversed_rows,
        vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
    );

    let (_, all_null_rows) = query(
        &executor,
        "SELECT id FROM or_pf_edges WHERE bucket = NULL OR NULL = bucket",
    )
    .await;
    assert!(all_null_rows.is_empty());

    cleanup(&wal);
}

#[tokio::test]
async fn test_predicate_first_common_or_lift_combines_with_or_equality() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE or_pf_lift (id INTEGER PRIMARY KEY, tenant INTEGER, bucket INTEGER)",
    )
    .await;

    let mut sql = String::from("INSERT INTO or_pf_lift VALUES ");
    for id in 1..=120 {
        if id > 1 {
            sql.push(',');
        }
        sql.push_str(&format!("({}, {}, {})", id, id % 3, id % 10));
    }
    exec_ok(&executor, &sql).await;

    let (_, all_rows) = query(&executor, "SELECT id, tenant, bucket FROM or_pf_lift").await;
    let expected: Vec<Vec<Value>> = all_rows
        .into_iter()
        .filter(|row| {
            matches!(row[1], Value::Integer(1)) && matches!(row[2], Value::Integer(2 | 4))
        })
        .map(|row| vec![row[0].clone()])
        .collect();
    assert!(!expected.is_empty());

    let (_, actual) = query(
        &executor,
        "SELECT id FROM or_pf_lift WHERE (tenant = 1 AND bucket = 2) OR (tenant = 1 AND bucket = 4.0)",
    )
    .await;
    assert_eq!(
        actual, expected,
        "common OR conjunct lifting must compose with OR equality predicate-first terms"
    );

    cleanup(&wal);
}
