use fusiondb::common::Value;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

// BENCHPROD-438: IN-list evaluation resolves the comparison column's data type once
// per row and coerces each list item to it, instead of re-running the full alignment
// (which re-resolved the column index) for every list item. These tests prove the
// result is identical to the equivalent OR-expansion, including type coercion.

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
