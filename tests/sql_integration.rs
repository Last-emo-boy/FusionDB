#[path = "sql/common.rs"]
mod common;
use common::{cleanup, exec_ok, query};

#[tokio::test]
async fn test_sql_integration_smoke_create_table() {
    let (executor, wal) = common::setup().await;
    let msg = exec_ok(
        &executor,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    )
    .await;
    assert!(msg.contains("created"));
    let (cols, rows) = query(&executor, "SHOW TABLES").await;
    assert_eq!(cols, vec!["Table"]);
    assert_eq!(rows.len(), 1);
    cleanup(&wal);
}
