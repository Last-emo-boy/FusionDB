use fusiondb::common::Value;

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
