use fusiondb::common::Value;
use fusiondb::execution::Executor;
use fusiondb::storage::memory::MemoryStorage;
use fusiondb::storage::Storage;
use std::sync::Arc;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

// BENCHPROD-444: filtered + LIMIT full scans stream through a ScanVisitor and stop early.
// These prove the streamed result matches the non-streamed path and respects the limit.

async fn seed(executor: &Executor, n: i64) {
    exec_ok(
        executor,
        "CREATE TABLE st (id INTEGER PRIMARY KEY, v INTEGER, tag TEXT)",
    )
    .await;
    for i in 1..=n {
        let tag = if i % 2 == 0 { "even" } else { "odd" };
        exec_ok(
            executor,
            &format!("INSERT INTO st VALUES ({}, {}, '{}')", i, i, tag),
        )
        .await;
    }
}

#[tokio::test]
async fn test_streaming_filtered_limit_first_n_in_pk_order() {
    let (executor, wal) = setup().await;
    seed(&executor, 30).await;
    // Scan order is PK ascending; first 3 rows with v >= 5 are ids 5,6,7.
    let (_, rows) = query(&executor, "SELECT id FROM st WHERE v >= 5 LIMIT 3").await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(5)],
            vec![Value::Integer(6)],
            vec![Value::Integer(7)],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_streaming_filtered_limit_fewer_matches_than_limit() {
    let (executor, wal) = setup().await;
    seed(&executor, 30).await;
    // Only ids 28,29,30 match; LIMIT 10 must return exactly those 3, not hang or over-return.
    let (_, rows) = query(&executor, "SELECT id FROM st WHERE v >= 28 LIMIT 10").await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(28)],
            vec![Value::Integer(29)],
            vec![Value::Integer(30)],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_streaming_filtered_limit_projected_and_wildcard_match_serial() {
    let (executor, wal) = setup().await;
    seed(&executor, 30).await;
    // Streamed (LIMIT present) vs non-streamed (no LIMIT) must agree on the full match set.
    // 15 rows are 'even'. A LIMIT >= match-count exercises streaming over the whole set.
    let (_, streamed) = query(&executor, "SELECT * FROM st WHERE tag = 'even' LIMIT 1000").await;
    let (_, serial) = query(&executor, "SELECT * FROM st WHERE tag = 'even'").await;
    let mut a = streamed;
    let mut b = serial;
    a.sort_by_key(|r| format!("{:?}", r));
    b.sort_by_key(|r| format!("{:?}", r));
    assert_eq!(a, b);
    assert_eq!(a.len(), 15);

    // Projected single column streaming path.
    let (_, vproj) = query(&executor, "SELECT v FROM st WHERE v >= 5 LIMIT 3").await;
    assert_eq!(
        vproj,
        vec![
            vec![Value::Integer(5)],
            vec![Value::Integer(6)],
            vec![Value::Integer(7)],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_streaming_filtered_limit_offset() {
    let (executor, wal) = setup().await;
    seed(&executor, 30).await;
    // OFFSET 2 over matches v>=5 (5,6,7,8,...) LIMIT 2 -> ids 7,8.
    let (_, rows) = query(&executor, "SELECT id FROM st WHERE v >= 5 LIMIT 2 OFFSET 2").await;
    assert_eq!(rows, vec![vec![Value::Integer(7)], vec![Value::Integer(8)]]);
    cleanup(&wal);
}
