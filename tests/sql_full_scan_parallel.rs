// BENCHPROD-440: the unindexed full-table-scan decode+filter loop is now
// parallelized via rayon for large scans (selection present, no pushed limit,
// > 1000 rows). These tests prove the parallel path returns exactly the same
// rows in the same order as the serial path.

use fusiondb::common::Value;
use fusiondb::execution::Executor;
use fusiondb::storage::{memory::MemoryStorage, Storage};
use std::sync::Arc;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

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

// Seed a table with `n` rows on a non-PK, non-indexed column so WHERE filters
// take the full-table-scan path (no index scan).
async fn seed(executor: &fusiondb::execution::Executor, n: i64) {
    exec_ok(
        executor,
        "CREATE TABLE bench (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)",
    )
    .await;

    // Single multi-row INSERT to populate > 1000 rows so the parallel scan path
    // engages.
    let mut sql = String::with_capacity(32 * n as usize);
    sql.push_str("INSERT INTO bench VALUES ");
    for id in 1..=n {
        if id > 1 {
            sql.push(',');
        }
        // val spreads across 0..100; name is deterministic per row.
        sql.push_str(&format!("({}, {}, 'user_{}')", id, id % 100, id));
    }
    exec_ok(executor, &sql).await;
}

async fn seed_like_prefix(executor: &Executor, n: i64) {
    exec_ok(
        executor,
        "CREATE TABLE like_prefix_bench (id INTEGER PRIMARY KEY, name TEXT)",
    )
    .await;

    let mut sql = String::with_capacity(24 * n as usize);
    sql.push_str("INSERT INTO like_prefix_bench VALUES ");
    for id in 1..=n {
        if id > 1 {
            sql.push(',');
        }
        let name_prefix = if id % 3 == 0 { "beta" } else { "alpha" };
        sql.push_str(&format!("({}, '{}{}')", id, name_prefix, id));
    }
    exec_ok(executor, &sql).await;
}

async fn select_result(
    executor: &fusiondb::execution::Executor,
    sql: &str,
) -> fusiondb::common::Result<fusiondb::execution::QueryResult> {
    let stmts = executor.prepare(sql).unwrap();
    executor.execute(&stmts[0]).await
}

#[tokio::test]
async fn test_predicate_first_parallel_and_filter_matches_serial() {
    let (executor, wal) = setup().await;
    seed(&executor, 1500).await;

    let (_, all_rows) = query(&executor, "SELECT id, val FROM bench").await;
    assert_eq!(all_rows.len(), 1500);

    let expected: Vec<Vec<Value>> = all_rows
        .into_iter()
        .filter(|row| matches!(row[1], Value::Integer(v) if (20..=40).contains(&v)))
        .map(|row| vec![row[0].clone()])
        .collect();
    assert!(expected.len() > 1, "filter should match many rows");

    // Simple AND-connected comparisons are predicate-first eligible and, above
    // the threshold, now use the parallel no-LIMIT full-scan path.
    let (_, actual) = query(
        &executor,
        "SELECT id FROM bench WHERE val >= 20 AND val <= 40",
    )
    .await;

    assert_eq!(
        actual, expected,
        "parallel predicate-first scan must match serial rows and order"
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_parallel_full_scan_propagates_filter_errors() {
    let (parallel_executor, parallel_wal) = setup().await;
    seed(&parallel_executor, 1500).await;

    // Unsupported predicate shape => fallback parallel full-scan path. The
    // division-by-zero error must propagate instead of being treated as false.
    let parallel_err = select_result(
        &parallel_executor,
        "SELECT * FROM bench WHERE 10 / (val - val) > 0",
    )
    .await
    .unwrap_err();
    assert!(
        format!("{:?}", parallel_err).contains("Division by zero"),
        "unexpected parallel error: {:?}",
        parallel_err
    );
    cleanup(&parallel_wal);

    let (serial_executor, serial_wal) = setup().await;
    seed(&serial_executor, 500).await;
    let serial_err = select_result(
        &serial_executor,
        "SELECT * FROM bench WHERE 10 / (val - val) > 0",
    )
    .await
    .unwrap_err();
    assert!(
        format!("{:?}", serial_err).contains("Division by zero"),
        "unexpected serial error: {:?}",
        serial_err
    );
    cleanup(&serial_wal);
}

#[tokio::test]
async fn test_mixed_or_full_scan_propagates_filter_errors() {
    let (parallel_executor, parallel_wal) = setup().await;
    seed(&parallel_executor, 1500).await;

    let parallel_err = select_result(
        &parallel_executor,
        "SELECT * FROM bench WHERE val = 2 OR 10 / (val - val) > 0",
    )
    .await
    .unwrap_err();
    assert!(
        format!("{:?}", parallel_err).contains("Division by zero"),
        "unexpected parallel mixed OR error: {:?}",
        parallel_err
    );
    cleanup(&parallel_wal);

    let (serial_executor, serial_wal) = setup().await;
    seed(&serial_executor, 500).await;
    let serial_err = select_result(
        &serial_executor,
        "SELECT * FROM bench WHERE val = 2 OR 10 / (val - val) > 0",
    )
    .await
    .unwrap_err();
    assert!(
        format!("{:?}", serial_err).contains("Division by zero"),
        "unexpected serial mixed OR error: {:?}",
        serial_err
    );
    cleanup(&serial_wal);
}

#[tokio::test]
async fn test_not_between_full_scan_propagates_filter_errors() {
    let (parallel_executor, parallel_wal) = setup().await;
    seed(&parallel_executor, 1500).await;

    let parallel_err = select_result(
        &parallel_executor,
        "SELECT * FROM bench WHERE val NOT BETWEEN 20 AND 10 / (val - val)",
    )
    .await
    .unwrap_err();
    assert!(
        format!("{:?}", parallel_err).contains("Division by zero"),
        "unexpected parallel NOT BETWEEN error: {:?}",
        parallel_err
    );
    cleanup(&parallel_wal);

    let (serial_executor, serial_wal) = setup().await;
    seed(&serial_executor, 500).await;
    let serial_err = select_result(
        &serial_executor,
        "SELECT * FROM bench WHERE val NOT BETWEEN 20 AND 10 / (val - val)",
    )
    .await
    .unwrap_err();
    assert!(
        format!("{:?}", serial_err).contains("Division by zero"),
        "unexpected serial NOT BETWEEN error: {:?}",
        serial_err
    );
    cleanup(&serial_wal);
}

#[tokio::test]
async fn test_between_bound_expression_errors_propagate() {
    let (parallel_executor, parallel_wal) = setup().await;
    seed(&parallel_executor, 1500).await;

    let parallel_err = select_result(
        &parallel_executor,
        "SELECT * FROM bench WHERE val BETWEEN 20 AND 10 / (1 - 1)",
    )
    .await
    .unwrap_err();
    assert!(
        format!("{:?}", parallel_err).contains("Division by zero"),
        "unexpected parallel BETWEEN bound error: {:?}",
        parallel_err
    );
    cleanup(&parallel_wal);

    let (serial_executor, serial_wal) = setup().await;
    seed(&serial_executor, 500).await;
    let serial_err = select_result(
        &serial_executor,
        "SELECT * FROM bench WHERE val BETWEEN 20 AND 10 / (1 - 1)",
    )
    .await
    .unwrap_err();
    assert!(
        format!("{:?}", serial_err).contains("Division by zero"),
        "unexpected serial BETWEEN bound error: {:?}",
        serial_err
    );
    cleanup(&serial_wal);
}

#[tokio::test]
async fn test_between_column_bound_fallback_matches_full_evaluator() {
    let (executor, wal) = setup().await;
    seed(&executor, 1500).await;

    let (_, all_rows) = query(&executor, "SELECT id, val FROM bench").await;
    let expected: Vec<Vec<Value>> = all_rows
        .into_iter()
        .filter(|row| {
            matches!((&row[0], &row[1]), (Value::Integer(id), Value::Integer(val)) if *val >= 0 && *val <= *id)
        })
        .map(|row| vec![row[0].clone()])
        .collect();

    let (_, actual) = query(&executor, "SELECT id FROM bench WHERE val BETWEEN 0 AND id").await;
    assert_eq!(
        actual, expected,
        "BETWEEN with a column bound must fall back without panicking or losing projection columns"
    );

    cleanup(&wal);
}

#[tokio::test]
async fn test_full_scan_between_parallel_matches_serial() {
    let (executor, wal) = setup().await;
    // 1500 rows > the 1000-row threshold => parallel decode+filter engages.
    seed(&executor, 1500).await;

    // No-WHERE scan uses the serial path (selection is None), giving the
    // canonical scan order of every row.
    let (_, all_rows) = query(&executor, "SELECT id, val FROM bench").await;
    assert_eq!(all_rows.len(), 1500);

    // Expected = the same rows, in the same order, filtered in Rust exactly as
    // the serial WHERE path would.
    let expected: Vec<Vec<Value>> = all_rows
        .into_iter()
        .filter(|row| matches!(row[1], Value::Integer(v) if (20..=40).contains(&v)))
        .collect();
    assert!(expected.len() > 1, "filter should match many rows");

    // WHERE over > 1000 rows with no limit => parallel path.
    let (_, actual) = query(
        &executor,
        "SELECT id, val FROM bench WHERE val BETWEEN 20 AND 40",
    )
    .await;

    assert_eq!(
        actual, expected,
        "parallel full-scan must match serial rows and order"
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_predicate_first_between_skips_nonmatching_row_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE between_decode (id INTEGER PRIMARY KEY, val INTEGER, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO between_decode VALUES (1, 10, 'low'), (2, 20, 'twenty'), (3, 30, 'thirty'), (4, 40, 'high')",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, val, payload) in [(1_i64, 10_i64, "low"), (4_i64, 40_i64, "high")] {
            let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                Value::Integer(val),
                Value::String(payload.to_string()),
            ]);
            corrupt_encoded_column(&mut corrupt_row, 2, 3);
            let key = format!(
                "data:between_decode:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let query_executor = Arc::new(Executor::new(storage.clone()));
    let (cols, rows) = query(
        &query_executor,
        "SELECT payload FROM between_decode WHERE val BETWEEN 20 AND 30",
    )
    .await;
    assert_eq!(cols, vec!["payload"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::String("twenty".to_string())],
            vec![Value::String("thirty".to_string())]
        ]
    );

    cleanup(&wal_path);
}

#[tokio::test]
async fn test_predicate_first_between_limit_matches_expected_order() {
    let (executor, wal) = setup().await;
    seed(&executor, 1500).await;

    let (_, all_rows) = query(&executor, "SELECT id, val FROM bench").await;
    let expected: Vec<Vec<Value>> = all_rows
        .into_iter()
        .filter(|row| matches!(row[1], Value::Integer(v) if (20..=40).contains(&v)))
        .take(9)
        .map(|row| vec![row[0].clone()])
        .collect();

    let (_, actual) = query(
        &executor,
        "SELECT id FROM bench WHERE val BETWEEN 20 AND 40 LIMIT 9",
    )
    .await;
    assert_eq!(
        actual, expected,
        "BETWEEN LIMIT must count matched rows, not visited rows"
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_predicate_first_between_null_and_inverted_bounds() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE between_edges (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO between_edges VALUES (1, 10), (2, 20), (3, NULL), (4, 30)",
    )
    .await;

    let (_, rows) = query(
        &executor,
        "SELECT id FROM between_edges WHERE val BETWEEN 10 AND 20",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);

    let (_, rows) = query(
        &executor,
        "SELECT id FROM between_edges WHERE val BETWEEN NULL AND 20",
    )
    .await;
    assert!(rows.is_empty());

    let (_, rows) = query(
        &executor,
        "SELECT id FROM between_edges WHERE val BETWEEN 10 AND NULL",
    )
    .await;
    assert!(rows.is_empty());

    let (_, rows) = query(
        &executor,
        "SELECT id FROM between_edges WHERE val BETWEEN 20 AND 10",
    )
    .await;
    assert!(rows.is_empty());

    cleanup(&wal);
}

#[tokio::test]
async fn test_full_scan_like_parallel_matches_serial() {
    let (executor, wal) = setup().await;
    seed(&executor, 1500).await;

    let (_, all_rows) = query(&executor, "SELECT id, name FROM bench").await;
    assert_eq!(all_rows.len(), 1500);

    // Rows whose name starts with "user_1" (matches the LIKE pattern below).
    let expected: Vec<Vec<Value>> = all_rows
        .into_iter()
        .filter(|row| matches!(&row[1], Value::String(s) if s.starts_with("user_1")))
        .collect();
    assert!(expected.len() > 1, "filter should match many rows");

    let (_, actual) = query(
        &executor,
        "SELECT id, name FROM bench WHERE name LIKE 'user_1%'",
    )
    .await;

    assert_eq!(
        actual, expected,
        "parallel full-scan LIKE must match serial rows and order"
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_predicate_first_like_prefix_parallel_matches_expected_order() {
    let (executor, wal) = setup().await;
    seed_like_prefix(&executor, 1500).await;

    let (_, all_rows) = query(&executor, "SELECT id, name FROM like_prefix_bench").await;
    let expected: Vec<Vec<Value>> = all_rows
        .into_iter()
        .filter(|row| matches!(&row[1], Value::String(s) if s.starts_with("alpha")))
        .collect();
    assert!(expected.len() > 1, "filter should match many rows");

    let (_, actual) = query(
        &executor,
        "SELECT id, name FROM like_prefix_bench WHERE name LIKE 'alpha%'",
    )
    .await;
    assert_eq!(
        actual, expected,
        "LIKE pure prefix predicate-first scan must match rows and order"
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_predicate_first_like_prefix_skips_nonmatching_row_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE like_decode (id INTEGER PRIMARY KEY, name TEXT, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO like_decode VALUES (1, 'alpha100', 'match-a'), (2, 'beta200', 'skip'), (3, NULL, 'skip-null'), (4, 'alpha101', 'match-b')",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name, payload) in [
            (2_i64, Value::String("beta200".to_string()), "skip"),
            (3_i64, Value::Null, "skip-null"),
        ] {
            let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                name,
                Value::String(payload.to_string()),
            ]);
            corrupt_encoded_column(&mut corrupt_row, 2, 3);
            let key = format!(
                "data:like_decode:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let query_executor = Arc::new(Executor::new(storage.clone()));
    let (cols, rows) = query(
        &query_executor,
        "SELECT payload FROM like_decode WHERE name LIKE 'alpha%'",
    )
    .await;
    assert_eq!(cols, vec!["payload"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::String("match-a".to_string())],
            vec![Value::String("match-b".to_string())]
        ]
    );

    cleanup(&wal_path);
}

#[tokio::test]
async fn test_predicate_first_like_prefix_limit_matches_expected_order() {
    let (executor, wal) = setup().await;
    seed_like_prefix(&executor, 1500).await;

    let (_, all_rows) = query(&executor, "SELECT id, name FROM like_prefix_bench").await;
    let expected: Vec<Vec<Value>> = all_rows
        .into_iter()
        .filter(|row| matches!(&row[1], Value::String(s) if s.starts_with("alpha")))
        .take(8)
        .map(|row| vec![row[0].clone()])
        .collect();

    let (_, actual) = query(
        &executor,
        "SELECT id FROM like_prefix_bench WHERE name LIKE 'alpha%' LIMIT 8",
    )
    .await;
    assert_eq!(
        actual, expected,
        "LIKE prefix LIMIT must count matched rows, not visited rows"
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_predicate_first_like_complex_wildcard_matches_expected_order() {
    let (executor, wal) = setup().await;
    seed(&executor, 1500).await;

    let (_, all_rows) = query(&executor, "SELECT id, name FROM bench").await;
    let expected: Vec<Vec<Value>> = all_rows
        .into_iter()
        .filter(|row| matches!(&row[1], Value::String(s) if s.starts_with("user_1") && s.len() >= "user_10".len()))
        .map(|row| vec![row[0].clone()])
        .collect();

    let (_, actual) = query(&executor, "SELECT id FROM bench WHERE name LIKE 'user_1_%'").await;
    assert_eq!(
        actual, expected,
        "predicate-first LIKE wildcard matching must match rows and order"
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_predicate_first_like_complex_wildcard_skips_nonmatching_row_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE like_wildcard_decode (id INTEGER PRIMARY KEY, name TEXT, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO like_wildcard_decode VALUES (1, 'alpha100', 'match-a'), (2, 'beta200', 'skip'), (3, NULL, 'skip-null'), (4, 'alpha101', 'match-b')",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name, payload) in [
            (2_i64, Value::String("beta200".to_string()), "skip"),
            (3_i64, Value::Null, "skip-null"),
        ] {
            let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                name,
                Value::String(payload.to_string()),
            ]);
            corrupt_encoded_column(&mut corrupt_row, 2, 3);
            let key = format!(
                "data:like_wildcard_decode:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let query_executor = Arc::new(Executor::new(storage.clone()));
    let (cols, rows) = query(
        &query_executor,
        "SELECT payload FROM like_wildcard_decode WHERE name LIKE 'al_ha%'",
    )
    .await;
    assert_eq!(cols, vec!["payload"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::String("match-a".to_string())],
            vec![Value::String("match-b".to_string())]
        ]
    );

    cleanup(&wal_path);
}

#[tokio::test]
async fn test_predicate_first_not_like_skips_nonmatching_row_decode() {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));

    exec_ok(
        &executor,
        "CREATE TABLE not_like_decode (id INTEGER PRIMARY KEY, name TEXT, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO not_like_decode VALUES (1, 'alpha100', 'match-a'), (2, 'beta200', 'skip'), (3, NULL, 'skip-null'), (4, 'alpha101', 'match-b')",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, name, payload) in [
            (2_i64, Value::String("beta200".to_string()), "skip"),
            (3_i64, Value::Null, "skip-null"),
        ] {
            let mut corrupt_row = fusiondb::common::encoding::RowEncoder::encode(&[
                Value::Integer(id),
                name,
                Value::String(payload.to_string()),
            ]);
            corrupt_encoded_column(&mut corrupt_row, 2, 3);
            let key = format!(
                "data:not_like_decode:{}",
                fusiondb::common::encoding::encode_i64_comparable(id)
            );
            txn.put(key.as_bytes(), &corrupt_row).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    let query_executor = Arc::new(Executor::new(storage.clone()));
    let (cols, rows) = query(
        &query_executor,
        "SELECT payload FROM not_like_decode WHERE name NOT LIKE 'beta%'",
    )
    .await;
    assert_eq!(cols, vec!["payload"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::String("match-a".to_string())],
            vec![Value::String("match-b".to_string())]
        ]
    );

    cleanup(&wal_path);
}

// Below the threshold the serial path is used; the result must be identical so
// the optimization is purely a performance change.
#[tokio::test]
async fn test_full_scan_small_serial_path_same_result() {
    let (executor, wal) = setup().await;
    seed(&executor, 500).await;

    let (_, all_rows) = query(&executor, "SELECT id, val FROM bench").await;
    let expected: Vec<Vec<Value>> = all_rows
        .into_iter()
        .filter(|row| matches!(row[1], Value::Integer(v) if (20..=40).contains(&v)))
        .collect();

    let (_, actual) = query(
        &executor,
        "SELECT id, val FROM bench WHERE val BETWEEN 20 AND 40",
    )
    .await;
    assert_eq!(actual, expected);
    cleanup(&wal);
}
