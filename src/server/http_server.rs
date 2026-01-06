use axum::{
    routing::{get, post},
    Router,
    Json,
    extract::State,
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;

use crate::execution::Executor;
// use crate::parser::parse_sql;
use crate::common::Value;
use crate::storage::Storage;
use crate::catalog::TableSchema;
use crate::monitor;

    use crate::storage::fusion::FusionStorage;

    // Zero-Copy Vector Search
    // Bypass SQL parser and plan, call FusionStorage::vector_search directly
    
    #[derive(Deserialize)]
    pub struct VectorSearchRequest {
        query: Vec<f32>,
        limit: usize,
    }

    #[derive(Deserialize)]
    pub struct HybridSearchRequest {
        text_query: String,
        vector_query: Vec<f32>,
        limit: usize,
    }

    #[derive(Serialize)]
    pub struct VectorSearchResponse {
        results: Vec<VectorSearchResult>,
    }

    #[derive(Serialize)]
    pub struct VectorSearchResult {
        id: String,
        distance: f32,
    }

    #[deprecated(note = "Use TCP Server for high performance. HTTP is kept only for backward compatibility and basic testing.")]
    pub async fn start_http_server(executor: Arc<Executor>, storage: Arc<dyn Storage>, start_port: u16) {
        let state = AppState { executor, storage };
    
        let app = Router::new()
            .route("/health", get(health_check))
            .route("/query", post(handle_query))
            .route("/prepare", post(handle_prepare))
            .route("/execute", post(handle_execute))
            .route("/tables", get(handle_tables))
            .route("/metrics", get(handle_metrics))
            .route("/checkpoint", post(handle_checkpoint))
            .route("/vector_search", post(handle_vector_search))
            .route("/hybrid_search", post(handle_hybrid_search)) // New Endpoint
            .layer(CorsLayer::permissive())
            .with_state(state);
    
        let mut port = start_port;
    let listener = loop {
        let addr = format!("127.0.0.1:{}", port);
        match TcpListener::bind(&addr).await {
            Ok(l) => break l,
            Err(_) => {
                if port >= start_port + 100 {
                     panic!("Could not bind to any port from {} to {}", start_port, port);
                }
                port += 1;
            }
        }
    };
    
    let addr = listener.local_addr().unwrap();
    println!("FusionDB HTTP Server running on http://{}", addr);
    
    // Write port to file for test scripts
    if let Ok(mut file) = std::fs::File::create("server_port.txt") {
        use std::io::Write;
        let _ = write!(file, "{}", addr.port());
    }

    axum::serve(listener, app).await.unwrap();
}

async fn health_check() -> &'static str {
    "OK"
}

async fn handle_metrics() -> Json<crate::monitor::Metrics> {
    Json(crate::monitor::Metrics {
        sql_parse_count: std::sync::atomic::AtomicU64::new(crate::monitor::GLOBAL_METRICS.sql_parse_count.load(std::sync::atomic::Ordering::Relaxed)),
        sql_plan_count: std::sync::atomic::AtomicU64::new(crate::monitor::GLOBAL_METRICS.sql_plan_count.load(std::sync::atomic::Ordering::Relaxed)),
        row_read_count: std::sync::atomic::AtomicU64::new(crate::monitor::GLOBAL_METRICS.row_read_count.load(std::sync::atomic::Ordering::Relaxed)),
        row_cache_hit_count: std::sync::atomic::AtomicU64::new(crate::monitor::GLOBAL_METRICS.row_cache_hit_count.load(std::sync::atomic::Ordering::Relaxed)),
        row_write_count: std::sync::atomic::AtomicU64::new(crate::monitor::GLOBAL_METRICS.row_write_count.load(std::sync::atomic::Ordering::Relaxed)),
        fts_search_count: std::sync::atomic::AtomicU64::new(crate::monitor::GLOBAL_METRICS.fts_search_count.load(std::sync::atomic::Ordering::Relaxed)),
        fts_doc_hits: std::sync::atomic::AtomicU64::new(crate::monitor::GLOBAL_METRICS.fts_doc_hits.load(std::sync::atomic::Ordering::Relaxed)),
        wal_write_count: std::sync::atomic::AtomicU64::new(crate::monitor::GLOBAL_METRICS.wal_write_count.load(std::sync::atomic::Ordering::Relaxed)),
        wal_write_bytes: std::sync::atomic::AtomicU64::new(crate::monitor::GLOBAL_METRICS.wal_write_bytes.load(std::sync::atomic::Ordering::Relaxed)),
    })
}

async fn handle_vector_search(
    State(state): State<AppState>,
    Json(payload): Json<VectorSearchRequest>,
) -> (StatusCode, Json<VectorSearchResponse>) {
    if let Some(fusion) = state.storage.as_any().downcast_ref::<FusionStorage>() {
        let results = fusion.vector_search(&payload.query, payload.limit);
        let resp = VectorSearchResponse {
            results: results.into_iter().map(|(id, dist)| VectorSearchResult { id, distance: dist }).collect(),
        };
        (StatusCode::OK, Json(resp))
    } else {
        (StatusCode::NOT_IMPLEMENTED, Json(VectorSearchResponse { results: vec![] }))
    }
}

async fn handle_hybrid_search(
    State(state): State<AppState>,
    Json(payload): Json<HybridSearchRequest>,
) -> (StatusCode, Json<VectorSearchResponse>) {
    if let Some(fusion) = state.storage.as_any().downcast_ref::<FusionStorage>() {
        let results = fusion.hybrid_search(&payload.text_query, &payload.vector_query, payload.limit);
        let resp = VectorSearchResponse {
            // Reusing VectorSearchResult but distance field is now RRF score
            results: results.into_iter().map(|(id, score)| VectorSearchResult { id, distance: score }).collect(),
        };
        (StatusCode::OK, Json(resp))
    } else {
        (StatusCode::NOT_IMPLEMENTED, Json(VectorSearchResponse { results: vec![] }))
    }
}

async fn handle_checkpoint(
    State(state): State<AppState>,
) -> (StatusCode, Json<serde_json::Value>) {
    match state.storage.create_snapshot().await {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({ "status": "ok", "message": "Checkpoint created" }))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "status": "error", "error": format!("{:?}", e) }))),
    }
}

async fn handle_query(
    State(state): State<AppState>,
    Json(payload): Json<QueryRequest>,
) -> (StatusCode, Json<QueryResponse>) {
    // println!("Received query: {}", payload.sql);
    
    match state.executor.prepare(&payload.sql) {
        Ok(statements) => {
            let mut results = Vec::new();
            
            // Start transaction
            match state.storage.begin_transaction().await {
                Ok(mut txn) => {
                    for stmt in statements {
                        match state.executor.execute_in_transaction(&stmt, &mut *txn).await {
                            Ok(res) => results.push(res.into()),
                            Err(e) => {
                                // Rollback on error
                                let _ = txn.rollback().await;
                                return (StatusCode::BAD_REQUEST, Json(QueryResponse {
                                    result: None,
                                    error: Some(format!("Execution Error: {:?}", e)),
                                }));
                            }
                        }
                    }
                    // Commit if all successful
                    if let Err(e) = txn.commit().await {
                         return (StatusCode::INTERNAL_SERVER_ERROR, Json(QueryResponse {
                            result: None,
                            error: Some(format!("Commit Error: {:?}", e)),
                        }));
                    }
                },
                Err(e) => {
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(QueryResponse {
                        result: None,
                        error: Some(format!("Transaction Error: {:?}", e)),
                    }));
                }
            }

            (StatusCode::OK, Json(QueryResponse {
                result: Some(results),
                error: None,
            }))
        },
        Err(e) => (StatusCode::BAD_REQUEST, Json(QueryResponse {
            result: None,
            error: Some(format!("Parse Error: {:?}", e)),
        })),
    }
}

async fn handle_tables(
    State(state): State<AppState>,
) -> (StatusCode, Json<Vec<TableInfo>>) {
    // Start a read-only transaction (or just transaction, effectively read-only if we don't write)
    match state.storage.begin_transaction().await {
        Ok(txn) => {
            // Scan for keys starting with "schema:"
            match txn.scan_prefix(b"schema:").await {
                Ok(pairs) => {
                    let mut tables = Vec::new();
                    for (_, value) in pairs {
                         if let Ok(schema) = bincode::deserialize::<TableSchema>(&value) {
                             tables.push(TableInfo {
                                 name: schema.name,
                                 columns: schema.columns.into_iter().map(|c| ColumnInfo {
                                     name: c.name,
                                     data_type: c.data_type,
                                     is_primary: c.is_primary,
                                     is_indexed: c.is_indexed,
                                 }).collect(),
                             });
                         }
                    }
                    (StatusCode::OK, Json(tables))
                },
                Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(vec![])),
            }
        },
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(vec![])),
    }
}

async fn handle_prepare(
    State(state): State<AppState>,
    Json(payload): Json<PrepareRequest>,
) -> (StatusCode, Json<PrepareResponse>) {
    match state.executor.register_prepared_statement(&payload.sql) {
        Ok(id) => (StatusCode::OK, Json(PrepareResponse { statement_id: id, error: None })),
        Err(e) => (StatusCode::BAD_REQUEST, Json(PrepareResponse { statement_id: "".to_string(), error: Some(format!("Prepare Error: {:?}", e)) })),
    }
}

async fn handle_execute(
    State(state): State<AppState>,
    Json(payload): Json<ExecuteRequest>,
) -> (StatusCode, Json<QueryResponse>) {
    match state.executor.get_prepared_statement(&payload.statement_id) {
        Ok(statements) => {
             let mut results = Vec::new();
             let params: Vec<Value> = payload.params.iter().map(|v| Value::from_json(v)).collect();

             match state.storage.begin_transaction().await {
                 Ok(mut txn) => {
                     for stmt in statements {
                         match state.executor.execute_in_transaction_with_params(&stmt, &mut *txn, &params).await {
                             Ok(res) => results.push(res.into()),
                             Err(e) => {
                                 let _ = txn.rollback().await;
                                 return (StatusCode::BAD_REQUEST, Json(QueryResponse { result: None, error: Some(format!("Execution Error: {:?}", e)) }));
                             }
                         }
                     }
                     if let Err(e) = txn.commit().await {
                         return (StatusCode::INTERNAL_SERVER_ERROR, Json(QueryResponse { result: None, error: Some(format!("Commit Error: {:?}", e)) }));
                     }
                 },
                 Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(QueryResponse { result: None, error: Some(format!("Transaction Error: {:?}", e)) })),
             }
             (StatusCode::OK, Json(QueryResponse { result: Some(results), error: None }))
        },
        Err(e) => (StatusCode::NOT_FOUND, Json(QueryResponse { result: None, error: Some(format!("Statement Not Found: {:?}", e)) })),
    }
}

#[derive(Clone)]
pub struct AppState {
    executor: Arc<Executor>,
    storage: Arc<dyn Storage>,
}

#[derive(Deserialize)]
pub struct QueryRequest {
    sql: String,
}

#[derive(Serialize)]
pub struct QueryResponse {
    result: Option<Vec<QueryResultJson>>,
    error: Option<String>,
}

#[derive(Deserialize)]
pub struct PrepareRequest {
    sql: String,
}

#[derive(Serialize)]
pub struct PrepareResponse {
    statement_id: String,
    error: Option<String>,
}

#[derive(Deserialize)]
pub struct ExecuteRequest {
    statement_id: String,
    params: Vec<serde_json::Value>,
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum QueryResultJson {
    Select { columns: Vec<String>, rows: Vec<Vec<serde_json::Value>> },
    Success { message: String },
}

impl From<crate::execution::QueryResult> for QueryResultJson {
    fn from(res: crate::execution::QueryResult) -> Self {
        match res {
            crate::execution::QueryResult::Select { columns, rows } => {
                let json_rows = rows.into_iter().map(|row| {
                    row.iter().map(|v| v.to_json()).collect()
                }).collect();
                QueryResultJson::Select { columns, rows: json_rows }
            },
            crate::execution::QueryResult::Success { message } => QueryResultJson::Success { message },
        }
    }
}

#[derive(Serialize)]
pub struct TableInfo {
    name: String,
    columns: Vec<ColumnInfo>,
}

#[derive(Serialize)]
pub struct ColumnInfo {
    name: String,
    data_type: String,
    is_primary: bool,
    is_indexed: bool,
}
