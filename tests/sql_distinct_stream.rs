use fusiondb::common::{encoding::RowEncoder, Value};
use fusiondb::execution::Executor;
use fusiondb::storage::{memory::MemoryStorage, Storage};
use std::sync::Arc;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

// BENCHPROD-439: COUNT(DISTINCT col) and SELECT DISTINCT col are gathered via a
// streaming ScanVisitor instead of materializing all KV pairs. These tests pin
// the result/count semantics (dedup, NULL handling, duplicates) with and
// without a WHERE clause so the optimization stays result-preserving.

async fn seed(executor: &fusiondb::execution::Executor) {
    exec_ok(
        executor,
        "CREATE TABLE events (id INTEGER PRIMARY KEY, category TEXT, region INTEGER)",
    )
    .await;
    // category has duplicates ('a' x3, 'b' x2, 'c' x1) plus two NULLs.
    // region pairs let us exercise a WHERE filter.
    exec_ok(
        executor,
        "INSERT INTO events VALUES \
         (1, 'a', 1), \
         (2, 'a', 2), \
         (3, 'b', 1), \
         (4, 'a', 2), \
         (5, 'b', 1), \
         (6, 'c', 2), \
         (7, NULL, 1), \
         (8, NULL, 2)",
    )
    .await;
}

fn sorted_strings(rows: Vec<Vec<Value>>) -> Vec<Value> {
    let mut values: Vec<Value> = rows.into_iter().map(|mut row| row.remove(0)).collect();
    values.sort_by(|a, b| format!("{:?}", a).cmp(&format!("{:?}", b)));
    values
}

fn encoded_row_id(value: i64) -> String {
    fusiondb::common::encoding::encode_i64_comparable(value)
}

fn corrupt_only_encoded_column(row: &mut [u8], column_index: usize, column_count: usize) {
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

async fn setup_with_storage() -> (Arc<Executor>, Arc<dyn Storage>, String) {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage.clone()));
    (executor, storage, wal_path)
}

#[tokio::test]
async fn test_count_distinct_no_where_excludes_null() {
    let (executor, wal) = setup().await;
    seed(&executor).await;

    // Distinct non-null categories: a, b, c => 3. NULLs are excluded.
    let (_, rows) = query(&executor, "SELECT COUNT(DISTINCT category) FROM events").await;
    assert_eq!(rows, vec![vec![Value::Integer(3)]]);

    cleanup(&wal);
}

#[tokio::test]
async fn test_count_distinct_with_where() {
    let (executor, wal) = setup().await;
    seed(&executor).await;

    // region = 1 rows: categories a, b, b, NULL => distinct non-null = {a, b} = 2.
    let (_, rows) = query(
        &executor,
        "SELECT COUNT(DISTINCT category) FROM events WHERE region = 1",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(2)]]);

    // region = 2 rows: categories a, a, c, NULL => distinct non-null = {a, c} = 2.
    let (_, rows) = query(
        &executor,
        "SELECT COUNT(DISTINCT category) FROM events WHERE region = 2",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(2)]]);

    cleanup(&wal);
}

#[tokio::test]
async fn test_select_distinct_no_where_includes_null() {
    let (executor, wal) = setup().await;
    seed(&executor).await;

    // SELECT DISTINCT keeps NULL as its own distinct value.
    let (_, rows) = query(&executor, "SELECT DISTINCT category FROM events").await;
    let values = sorted_strings(rows);
    assert_eq!(
        values,
        vec![
            Value::Null,
            Value::String("a".to_string()),
            Value::String("b".to_string()),
            Value::String("c".to_string()),
        ]
    );

    cleanup(&wal);
}

#[tokio::test]
async fn test_select_distinct_with_where() {
    let (executor, wal) = setup().await;
    seed(&executor).await;

    // region = 1 rows: categories a, b, b, NULL => distinct = {a, b, NULL}.
    let (_, rows) = query(
        &executor,
        "SELECT DISTINCT category FROM events WHERE region = 1",
    )
    .await;
    let values = sorted_strings(rows);
    assert_eq!(
        values,
        vec![
            Value::Null,
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ]
    );

    cleanup(&wal);
}

#[tokio::test]
async fn test_select_distinct_integer_column() {
    let (executor, wal) = setup().await;
    seed(&executor).await;

    // region values: 1 and 2, each repeated => distinct = {1, 2}.
    let (_, rows) = query(&executor, "SELECT DISTINCT region FROM events").await;
    let mut values: Vec<Value> = rows.into_iter().map(|mut row| row.remove(0)).collect();
    values.sort_by(|a, b| format!("{:?}", a).cmp(&format!("{:?}", b)));
    assert_eq!(values, vec![Value::Integer(1), Value::Integer(2)]);

    cleanup(&wal);
}

#[tokio::test]
async fn test_count_distinct_index_key_scan_avoids_base_row_decode() {
    let (executor, storage, wal) = setup_with_storage().await;

    exec_ok(
        &executor,
        "CREATE TABLE idx_count_distinct_keys (id INTEGER PRIMARY KEY, bucket INTEGER, payload TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO idx_count_distinct_keys VALUES
            (1, 2, 'p1'),
            (2, 1, 'p2'),
            (3, 2, 'p3'),
            (4, 3, 'p4'),
            (5, 1, 'p5'),
            (6, NULL, 'p6')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_count_distinct_keys_bucket ON idx_count_distinct_keys (bucket)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let rows = [
            (1, Value::Integer(2), "p1"),
            (2, Value::Integer(1), "p2"),
            (3, Value::Integer(2), "p3"),
            (4, Value::Integer(3), "p4"),
            (5, Value::Integer(1), "p5"),
            (6, Value::Null, "p6"),
        ];
        for (id, bucket, payload) in rows {
            let mut corrupt_row = RowEncoder::encode(&[
                Value::Integer(id),
                bucket,
                Value::String(payload.to_string()),
            ]);
            corrupt_only_encoded_column(&mut corrupt_row, 1, 3);
            txn.put(
                format!("data:idx_count_distinct_keys:{}", encoded_row_id(id)).as_bytes(),
                &corrupt_row,
            )
            .await
            .unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (_, rows) = query(
        &executor,
        "SELECT COUNT(DISTINCT bucket) FROM idx_count_distinct_keys",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(3)]]);

    cleanup(&wal);
}

#[tokio::test]
async fn test_count_distinct_text_key_stream_handles_colon_prefix_values() {
    let (executor, storage, wal) = setup_with_storage().await;

    exec_ok(
        &executor,
        "CREATE TABLE idx_count_distinct_text_keys (
            id INTEGER PRIMARY KEY,
            label TEXT,
            payload TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO idx_count_distinct_text_keys VALUES
            (1, 'a:b', 'p1'),
            (2, 'a:b:c', 'p2'),
            (3, 'z', 'p3'),
            (4, 'a:b', 'p4'),
            (5, NULL, 'p5')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_count_distinct_text_keys_label
            ON idx_count_distinct_text_keys (label)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        for (id, label, payload) in [
            (1_i64, Value::String("a:b".to_string()), "p1"),
            (2, Value::String("a:b:c".to_string()), "p2"),
            (3, Value::String("z".to_string()), "p3"),
            (4, Value::String("a:b".to_string()), "p4"),
            (5, Value::Null, "p5"),
        ] {
            let mut corrupt_row = RowEncoder::encode(&[
                Value::Integer(id),
                label,
                Value::String(payload.to_string()),
            ]);
            corrupt_only_encoded_column(&mut corrupt_row, 1, 3);
            txn.put(
                format!("data:idx_count_distinct_text_keys:{}", encoded_row_id(id)).as_bytes(),
                &corrupt_row,
            )
            .await
            .unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (_, rows) = query(
        &executor,
        "SELECT COUNT(DISTINCT label) FROM idx_count_distinct_text_keys",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(3)]]);

    cleanup(&wal);
}

#[tokio::test]
async fn test_count_distinct_malformed_index_key_falls_back_to_rows() {
    let (executor, storage, wal) = setup_with_storage().await;

    exec_ok(
        &executor,
        "CREATE TABLE idx_count_distinct_malformed (
            id INTEGER PRIMARY KEY,
            bucket INTEGER,
            payload TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO idx_count_distinct_malformed VALUES
            (1, 2, 'p1'),
            (2, 1, 'p2'),
            (3, 2, 'p3'),
            (4, 3, 'p4'),
            (5, NULL, 'p5')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_count_distinct_malformed_bucket
            ON idx_count_distinct_malformed (bucket)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        txn.put(b"index:idx_count_distinct_malformed:bucket:!malformed", &[])
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    let (_, rows) = query(
        &executor,
        "SELECT COUNT(DISTINCT bucket) FROM idx_count_distinct_malformed",
    )
    .await;
    assert_eq!(rows, vec![vec![Value::Integer(3)]]);

    cleanup(&wal);
}

#[tokio::test]
async fn test_select_distinct_index_key_scan_avoids_base_row_decode() {
    let (executor, storage, wal) = setup_with_storage().await;

    exec_ok(
        &executor,
        "CREATE TABLE idx_select_distinct_keys (
            id INTEGER PRIMARY KEY,
            bucket INTEGER NOT NULL,
            payload TEXT
        )",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO idx_select_distinct_keys VALUES
            (1, 2, 'p1'),
            (2, 1, 'p2'),
            (3, 2, 'p3'),
            (4, 3, 'p4'),
            (5, 1, 'p5')",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX idx_select_distinct_keys_bucket ON idx_select_distinct_keys (bucket)",
    )
    .await;

    {
        let mut txn = storage.begin_transaction().await.unwrap();
        let rows = [
            (1, 2, "p1"),
            (2, 1, "p2"),
            (3, 2, "p3"),
            (4, 3, "p4"),
            (5, 1, "p5"),
        ];
        for (id, bucket, payload) in rows {
            let mut corrupt_row = RowEncoder::encode(&[
                Value::Integer(id),
                Value::Integer(bucket),
                Value::String(payload.to_string()),
            ]);
            corrupt_only_encoded_column(&mut corrupt_row, 1, 3);
            txn.put(
                format!("data:idx_select_distinct_keys:{}", encoded_row_id(id)).as_bytes(),
                &corrupt_row,
            )
            .await
            .unwrap();
        }
        txn.commit().await.unwrap();
    }

    let (cols, rows) = query(
        &executor,
        "SELECT DISTINCT bucket FROM idx_select_distinct_keys ORDER BY bucket",
    )
    .await;
    assert_eq!(cols, vec!["bucket"]);
    assert_eq!(
        rows,
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)]
        ]
    );

    cleanup(&wal);
}
