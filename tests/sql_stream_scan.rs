use fusiondb::common::encoding::RowEncoder;
use fusiondb::common::Value;
use fusiondb::execution::Executor;
use fusiondb::storage::{memory::MemoryStorage, Storage};
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

#[tokio::test]
async fn test_streaming_order_by_limit_offset_topk() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE st_topk (id INTEGER PRIMARY KEY, score INTEGER, tag TEXT)",
    )
    .await;
    for (id, score, tag) in [
        (1, 20, "a"),
        (2, 10, "b"),
        (3, 50, "c"),
        (4, 40, "d"),
        (5, 30, "e"),
        (6, 40, "f"),
    ] {
        exec_ok(
            &executor,
            &format!("INSERT INTO st_topk VALUES ({}, {}, '{}')", id, score, tag),
        )
        .await;
    }

    let (_, rows) = query(
        &executor,
        "SELECT id FROM st_topk ORDER BY score DESC, id ASC LIMIT 3 OFFSET 1",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(4)],
            vec![Value::Integer(6)],
            vec![Value::Integer(5)],
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_streaming_order_by_wildcard_late_materialization_preserves_rows() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE st_topk_wide (id INTEGER PRIMARY KEY, score INTEGER, payload TEXT, extra TEXT)",
    )
    .await;
    for (id, score, payload, extra) in [
        (1, 20, "payload-1", "extra-1"),
        (2, 10, "payload-2", "extra-2"),
        (3, 50, "payload-3", "extra-3"),
        (4, 40, "payload-4", "extra-4"),
        (5, 30, "payload-5", "extra-5"),
        (6, 40, "payload-6", "extra-6"),
        (7, 50, "payload-7", "extra-7"),
    ] {
        exec_ok(
            &executor,
            &format!("INSERT INTO st_topk_wide VALUES ({id}, {score}, '{payload}', '{extra}')"),
        )
        .await;
    }

    // The two 50s tie on the first key; the second key must retain the
    // existing stable ordering before OFFSET is applied.
    let (_, rows) = query(
        &executor,
        "SELECT * FROM st_topk_wide ORDER BY score DESC, id ASC LIMIT 3 OFFSET 1",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Integer(7),
                Value::Integer(50),
                Value::String("payload-7".to_string()),
                Value::String("extra-7".to_string()),
            ],
            vec![
                Value::Integer(4),
                Value::Integer(40),
                Value::String("payload-4".to_string()),
                Value::String("extra-4".to_string()),
            ],
            vec![
                Value::Integer(6),
                Value::Integer(40),
                Value::String("payload-6".to_string()),
                Value::String("extra-6".to_string()),
            ],
        ]
    );

    let (_, rows) = query(
        &executor,
        "SELECT * FROM st_topk_wide ORDER BY score ASC, id ASC LIMIT 4 OFFSET 1",
    )
    .await;
    assert_eq!(
        rows.iter().map(|row| row[0].clone()).collect::<Vec<_>>(),
        vec![
            Value::Integer(1),
            Value::Integer(5),
            Value::Integer(4),
            Value::Integer(6),
        ]
    );
    assert!(rows.iter().all(|row| row.len() == 4));
    cleanup(&wal);
}

#[tokio::test]
async fn test_streaming_order_by_wildcard_preserves_null_ordering() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE st_topk_null (id INTEGER PRIMARY KEY, score INTEGER, payload TEXT)",
    )
    .await;
    for sql in [
        "INSERT INTO st_topk_null VALUES (1, NULL, 'null-1')",
        "INSERT INTO st_topk_null VALUES (2, 20, 'value-2')",
        "INSERT INTO st_topk_null VALUES (3, NULL, 'null-3')",
        "INSERT INTO st_topk_null VALUES (4, 10, 'value-4')",
    ] {
        exec_ok(&executor, sql).await;
    }

    let (_, rows) = query(
        &executor,
        "SELECT * FROM st_topk_null ORDER BY score ASC, id ASC LIMIT 3",
    )
    .await;
    assert_eq!(
        rows.iter().map(|row| row[0].clone()).collect::<Vec<_>>(),
        vec![Value::Integer(1), Value::Integer(3), Value::Integer(4)]
    );

    let (_, rows) = query(
        &executor,
        "SELECT * FROM st_topk_null ORDER BY score DESC, id ASC LIMIT 4",
    )
    .await;
    assert_eq!(
        rows.iter().map(|row| row[0].clone()).collect::<Vec<_>>(),
        vec![
            Value::Integer(2),
            Value::Integer(4),
            Value::Integer(1),
            Value::Integer(3),
        ]
    );
    cleanup(&wal);
}

#[tokio::test]
async fn test_streaming_order_by_skips_corrupt_best_candidate() {
    let wal = format!("test_topk_corrupt_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal).unwrap());
    let executor = Executor::new(storage.clone());
    exec_ok(
        &executor,
        "CREATE TABLE st_topk_corrupt (id INTEGER PRIMARY KEY, score INTEGER, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO st_topk_corrupt VALUES (2, 50, 'valid')",
    )
    .await;

    // Keep the id and ordering-column spans intact, but truncate the payload
    // span. The old full-row visitor skips this row; the late path must not
    // let it evict the valid LIMIT 1 candidate before final materialization.
    let mut corrupt = RowEncoder::encode(&[
        Value::Integer(1),
        Value::Integer(100),
        Value::String("corrupt".to_string()),
    ]);
    let payload_start = u32::from_le_bytes(corrupt[10..14].try_into().unwrap()) as usize;
    corrupt.truncate(payload_start);
    let mut txn = storage.begin_transaction().await.unwrap();
    txn.put(b"data:st_topk_corrupt:8000000000000001", &corrupt)
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let (_, rows) = query(
        &executor,
        "SELECT * FROM st_topk_corrupt ORDER BY score DESC LIMIT 1",
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2));
    assert_eq!(rows[0][1], Value::Integer(50));
    assert_eq!(rows[0][2], Value::String("valid".to_string()));
    cleanup(&wal);
}

#[tokio::test]
async fn test_streaming_order_by_alias_shadow_falls_back_to_query_sort() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE st_alias_topk (id INTEGER PRIMARY KEY, score INTEGER)",
    )
    .await;
    for (id, score) in [(1, 100), (2, 10), (3, 50)] {
        exec_ok(
            &executor,
            &format!("INSERT INTO st_alias_topk VALUES ({}, {})", id, score),
        )
        .await;
    }

    let (_, rows) = query(
        &executor,
        "SELECT id AS score FROM st_alias_topk ORDER BY score ASC LIMIT 2",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
    cleanup(&wal);
}
