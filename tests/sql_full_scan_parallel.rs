// BENCHPROD-440: the unindexed full-table-scan decode+filter loop is now
// parallelized via rayon for large scans (selection present, no pushed limit,
// > 1000 rows). These tests prove the parallel path returns exactly the same
// rows in the same order as the serial path.

use fusiondb::common::Value;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

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
