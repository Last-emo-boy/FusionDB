#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

#[tokio::test]
async fn test_window_row_number() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE wf (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO wf VALUES (1, 'eng', 100), (2, 'eng', 200), (3, 'sales', 150), (4, 'sales', 250)").await;
    let (cols, rows) = query(
        &executor,
        "SELECT id, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) AS rn FROM wf",
    )
    .await;
    assert_eq!(cols.len(), 3);
    assert_eq!(rows.len(), 4);
    // Each partition should have row numbers 1, 2
    for row in &rows {
        let rn = &row[2];
        assert!(matches!(
            rn,
            fusiondb::common::Value::Integer(1) | fusiondb::common::Value::Integer(2)
        ));
    }
    cleanup(&wal);
}

#[tokio::test]
async fn test_window_rank() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE wr (id INTEGER PRIMARY KEY, score INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO wr VALUES (1, 100), (2, 200), (3, 200), (4, 300)",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT id, score, RANK() OVER (ORDER BY score) AS rnk FROM wr",
    )
    .await;
    assert_eq!(rows.len(), 4);
    // Ranks should be 1, 2, 2, 4 (ties get same rank, next skips)
    let ranks: Vec<i64> = rows
        .iter()
        .map(|r| {
            if let fusiondb::common::Value::Integer(n) = &r[2] {
                *n
            } else {
                0
            }
        })
        .collect();
    let mut sorted_ranks = ranks.clone();
    sorted_ranks.sort();
    assert_eq!(sorted_ranks, vec![1, 2, 2, 4]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_window_dense_rank() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE wdr (id INTEGER PRIMARY KEY, score INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO wdr VALUES (1, 100), (2, 200), (3, 200), (4, 300)",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT id, score, DENSE_RANK() OVER (ORDER BY score) AS drnk FROM wdr",
    )
    .await;
    assert_eq!(rows.len(), 4);
    // Dense ranks should be 1, 2, 2, 3
    let ranks: Vec<i64> = rows
        .iter()
        .map(|r| {
            if let fusiondb::common::Value::Integer(n) = &r[2] {
                *n
            } else {
                0
            }
        })
        .collect();
    let mut sorted_ranks = ranks.clone();
    sorted_ranks.sort();
    assert_eq!(sorted_ranks, vec![1, 2, 2, 3]);
    cleanup(&wal);
}

#[tokio::test]
async fn test_window_lag_lead() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE wll (id INTEGER PRIMARY KEY, val INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO wll VALUES (1, 10), (2, 20), (3, 30)",
    )
    .await;
    let (_, rows) = query(
        &executor,
        "SELECT id, val, LAG(val) OVER (ORDER BY id) AS prev_val FROM wll",
    )
    .await;
    assert_eq!(rows.len(), 3);
    // First row's LAG should be Null, second should be 10, third should be 20
    assert_eq!(rows[0][2], fusiondb::common::Value::Null);
    let (_, rows) = query(
        &executor,
        "SELECT id, val, LEAD(val) OVER (ORDER BY id) AS next_val FROM wll",
    )
    .await;
    assert_eq!(rows.len(), 3);
    // Last row's LEAD should be Null
    assert_eq!(rows[2][2], fusiondb::common::Value::Null);
    cleanup(&wal);
}
