use fusiondb::common::Value;

#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query, setup};

#[tokio::test]
async fn text_primary_key_with_colon_survives_data_key_scans() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE colon_pk (id TEXT PRIMARY KEY, label TEXT)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO colon_pk VALUES ('org:42', 'west'), ('org:7', 'east'), ('team:1', 'north')",
    )
    .await;

    let (_, rows) = query(&executor, "SELECT id FROM colon_pk ORDER BY id").await;
    assert_eq!(
        rows,
        vec![
            vec![Value::String("org:42".to_string())],
            vec![Value::String("org:7".to_string())],
            vec![Value::String("team:1".to_string())],
        ]
    );

    let (_, rows) = query(
        &executor,
        "SELECT id FROM colon_pk WHERE id LIKE 'org:%' ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Value::String("org:42".to_string())],
            vec![Value::String("org:7".to_string())],
        ]
    );

    let (_, rows) = query(
        &executor,
        "SELECT id FROM colon_pk WHERE id BETWEEN 'org:0' AND 'org:zz' ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Value::String("org:42".to_string())],
            vec![Value::String("org:7".to_string())],
        ]
    );

    cleanup(&wal);
}

#[tokio::test]
async fn text_primary_key_with_colon_bypasses_ambiguous_legacy_indexes() {
    let (executor, wal) = setup().await;
    exec_ok(
        &executor,
        "CREATE TABLE colon_index (id TEXT PRIMARY KEY, label TEXT, amount INTEGER)",
    )
    .await;
    exec_ok(
        &executor,
        "CREATE INDEX colon_index_label_idx ON colon_index (label)",
    )
    .await;
    exec_ok(
        &executor,
        "INSERT INTO colon_index VALUES ('b:c', 'a', 1), ('c', 'a:b', 2)",
    )
    .await;

    // These two logical entries share one legacy physical key:
    // index:colon_index:label:a:b:c.
    let (_, rows) = query(&executor, "SELECT id FROM colon_index WHERE label = 'a'").await;
    assert_eq!(rows, vec![vec![Value::String("b:c".to_string())]]);
    let (_, rows) = query(&executor, "SELECT id FROM colon_index WHERE label = 'a:b'").await;
    assert_eq!(rows, vec![vec![Value::String("c".to_string())]]);

    let (_, rows) = query(
        &executor,
        "SELECT id FROM colon_index WHERE label IN ('a', 'a:b') ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Value::String("b:c".to_string())],
            vec![Value::String("c".to_string())],
        ]
    );
    let (_, rows) = query(
        &executor,
        "SELECT id FROM colon_index WHERE label LIKE 'a%' ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Value::String("b:c".to_string())],
            vec![Value::String("c".to_string())],
        ]
    );
    let (_, rows) = query(
        &executor,
        "SELECT id FROM colon_index ORDER BY label LIMIT 10",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Value::String("b:c".to_string())],
            vec![Value::String("c".to_string())],
        ]
    );

    let (_, rows) = query(&executor, "SELECT COUNT(DISTINCT label) FROM colon_index").await;
    assert_eq!(rows, vec![vec![Value::Integer(2)]]);
    let (_, rows) = query(
        &executor,
        "SELECT label, COUNT(*) FROM colon_index GROUP BY label ORDER BY label",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Value::String("a".to_string()), Value::Integer(1)],
            vec![Value::String("a:b".to_string()), Value::Integer(1)],
        ]
    );

    exec_ok(
        &executor,
        "CREATE TABLE colon_labels (label TEXT PRIMARY KEY)",
    )
    .await;
    exec_ok(&executor, "INSERT INTO colon_labels VALUES ('a'), ('a:b')").await;
    let (_, rows) = query(
        &executor,
        "SELECT c.id FROM colon_labels l JOIN colon_index c ON c.label = l.label ORDER BY c.id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            vec![Value::String("b:c".to_string())],
            vec![Value::String("c".to_string())],
        ]
    );

    exec_ok(&executor, "DELETE FROM colon_index WHERE id = 'b:c'").await;
    let (_, rows) = query(&executor, "SELECT id FROM colon_index WHERE label = 'a:b'").await;
    assert_eq!(rows, vec![vec![Value::String("c".to_string())]]);

    cleanup(&wal);
}
