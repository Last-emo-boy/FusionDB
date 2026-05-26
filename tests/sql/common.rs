use fusiondb::common::Value;
use fusiondb::execution::{Executor, QueryResult};
use fusiondb::storage::memory::MemoryStorage;
use fusiondb::storage::Storage;
use std::sync::Arc;

pub async fn setup() -> (Arc<Executor>, String) {
    let wal_path = format!("test_{}.wal", uuid::Uuid::new_v4());
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).unwrap());
    let executor = Arc::new(Executor::new(storage));
    (executor, wal_path)
}

async fn exec(executor: &Executor, sql: &str) -> QueryResult {
    let stmts = executor.prepare(sql).unwrap();
    executor.execute(&stmts[0]).await.unwrap()
}

pub async fn query(executor: &Executor, sql: &str) -> (Vec<String>, Vec<Vec<Value>>) {
    match exec(executor, sql).await {
        QueryResult::Select { columns, rows } => (columns, rows),
        other => panic!("Expected Select, got {:?}", other),
    }
}

pub async fn exec_ok(executor: &Executor, sql: &str) -> String {
    match exec(executor, sql).await {
        QueryResult::Success { message } => message,
        other => panic!("Expected Success, got {:?}", other),
    }
}

pub fn cleanup(wal_path: &str) {
    let _ = std::fs::remove_file(wal_path);
}
