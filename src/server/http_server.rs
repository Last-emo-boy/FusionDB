use axum::{
    extract::{Extension, Path, Query, Request, State},
    http::{header::AUTHORIZATION, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;

use crate::catalog::TableSchema;
use crate::common::{FusionError, Value};
use crate::execution::{Executor, PreparedStatementRecord};
use crate::storage::Storage;

use crate::storage::fusion::{CdcEvent, FusionStorage};
use crate::storage::memory::MemoryStorage;

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

fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/query", post(handle_query))
        .route("/prepare", post(handle_prepare).get(handle_list_prepared))
        .route(
            "/prepare/{statement_id}",
            delete(handle_deallocate_prepared),
        )
        .route("/execute", post(handle_execute))
        .route("/tables", get(handle_tables))
        .route("/metrics", get(handle_metrics))
        .route("/metrics/prometheus", get(handle_prometheus))
        .route("/slow_queries", get(handle_slow_queries))
        .route("/checkpoint", post(handle_checkpoint))
        .route("/compact", post(handle_compact))
        .route("/cdc/events", get(handle_cdc_events))
        .route("/capabilities", get(handle_capabilities))
        .route("/auth/context", get(handle_auth_context))
        .route("/vector_search", post(handle_vector_search))
        .route("/hybrid_search", post(handle_hybrid_search))
        .layer(middleware::from_fn(auth_context_middleware))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

#[deprecated(
    note = "Use TCP Server for high performance. HTTP is kept only for backward compatibility and basic testing."
)]
pub async fn start_http_server(
    executor: Arc<Executor>,
    storage: Arc<dyn Storage>,
    bind: &str,
    start_port: u16,
    _tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
) {
    let state = AppState { executor, storage };

    let app = build_router(state);

    let mut port = start_port;
    let listener = loop {
        let addr = format!("{}:{}", bind, port);
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
    let scheme = if _tls_acceptor.is_some() {
        "https"
    } else {
        "http"
    };
    println!("FusionDB HTTP Server running on {}://{}", scheme, addr);

    // Write port to file for test scripts
    if let Ok(mut file) = std::fs::File::create("server_port.txt") {
        use std::io::Write;
        let _ = write!(file, "{}", addr.port());
    }

    // TLS support: if acceptor provided, use axum-server with rustls
    // For now, we fall back to plain axum::serve since axum-server API differs.
    // TLS is primarily used for pgwire; HTTP can be put behind a reverse proxy.
    axum::serve(listener, app).await.unwrap();
}

async fn auth_context_middleware(mut request: Request, next: Next) -> Response {
    let username = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(extract_bearer_username)
        .or_else(|| {
            request
                .headers()
                .get("x-fusiondb-user")
                .and_then(|value| value.to_str().ok())
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        });

    request.extensions_mut().insert(RequestContext { username });
    next.run(request).await
}

fn extract_bearer_username(header: &str) -> Option<String> {
    let token = header.strip_prefix("Bearer ")?.trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_string())
    }
}

async fn health_check() -> &'static str {
    "OK"
}

async fn handle_metrics() -> ApiResponse<MetricsSnapshot> {
    json_ok(MetricsSnapshot::capture())
}

async fn handle_slow_queries() -> ApiResponse<Vec<crate::monitor::SlowQueryEntry>> {
    json_ok(crate::monitor::SLOW_QUERY_LOG.recent())
}

async fn handle_auth_context(
    Extension(context): Extension<RequestContext>,
) -> ApiResponse<AuthContextInfo> {
    json_ok(AuthContextInfo::from_request_context(&context))
}

async fn handle_prometheus() -> String {
    use std::sync::atomic::Ordering::Relaxed;
    let m = &crate::monitor::GLOBAL_METRICS;
    format!(
        "# HELP fusiondb_query_count Total queries executed\n\
         # TYPE fusiondb_query_count counter\n\
         fusiondb_query_count {}\n\
         # HELP fusiondb_slow_query_count Queries exceeding slow threshold\n\
         # TYPE fusiondb_slow_query_count counter\n\
         fusiondb_slow_query_count {}\n\
         # HELP fusiondb_query_duration_us Total query time in microseconds\n\
         # TYPE fusiondb_query_duration_us counter\n\
         fusiondb_query_duration_us {}\n\
         # HELP fusiondb_row_read_count Rows read\n\
         # TYPE fusiondb_row_read_count counter\n\
         fusiondb_row_read_count {}\n\
         # HELP fusiondb_row_write_count Rows written\n\
         # TYPE fusiondb_row_write_count counter\n\
         fusiondb_row_write_count {}\n\
         # HELP fusiondb_row_cache_hit_count Row cache hits\n\
         # TYPE fusiondb_row_cache_hit_count counter\n\
         fusiondb_row_cache_hit_count {}\n\
         # HELP fusiondb_wal_write_count WAL syncs\n\
         # TYPE fusiondb_wal_write_count counter\n\
         fusiondb_wal_write_count {}\n\
         # HELP fusiondb_wal_write_bytes WAL bytes written\n\
         # TYPE fusiondb_wal_write_bytes counter\n\
         fusiondb_wal_write_bytes {}\n",
        m.query_count.load(Relaxed),
        m.slow_query_count.load(Relaxed),
        m.query_total_us.load(Relaxed),
        m.row_read_count.load(Relaxed),
        m.row_write_count.load(Relaxed),
        m.row_cache_hit_count.load(Relaxed),
        m.wal_write_count.load(Relaxed),
        m.wal_write_bytes.load(Relaxed),
    )
}

async fn handle_vector_search(
    State(state): State<AppState>,
    Json(payload): Json<VectorSearchRequest>,
) -> (StatusCode, Json<VectorSearchResponse>) {
    if let Some(fusion) = state.storage.as_any().downcast_ref::<FusionStorage>() {
        let results = fusion.vector_search(&payload.query, payload.limit);
        let resp = VectorSearchResponse {
            results: results
                .into_iter()
                .map(|(id, dist)| VectorSearchResult { id, distance: dist })
                .collect(),
        };
        (StatusCode::OK, Json(resp))
    } else {
        (
            StatusCode::NOT_IMPLEMENTED,
            Json(VectorSearchResponse { results: vec![] }),
        )
    }
}

async fn handle_hybrid_search(
    State(state): State<AppState>,
    Json(payload): Json<HybridSearchRequest>,
) -> (StatusCode, Json<VectorSearchResponse>) {
    if let Some(fusion) = state.storage.as_any().downcast_ref::<FusionStorage>() {
        let results =
            fusion.hybrid_search(&payload.text_query, &payload.vector_query, payload.limit);
        let resp = VectorSearchResponse {
            // Reusing VectorSearchResult but distance field is now RRF score
            results: results
                .into_iter()
                .map(|(id, score)| VectorSearchResult {
                    id,
                    distance: score,
                })
                .collect(),
        };
        (StatusCode::OK, Json(resp))
    } else {
        (
            StatusCode::NOT_IMPLEMENTED,
            Json(VectorSearchResponse { results: vec![] }),
        )
    }
}

async fn handle_checkpoint(
    State(state): State<AppState>,
) -> (StatusCode, Json<Envelope<OperationResponse>>) {
    match state.storage.create_snapshot().await {
        Ok(_) => json_ok(OperationResponse {
            operation: "checkpoint".to_string(),
            message: Some("Checkpoint created".to_string()),
            supported: true,
        }),
        Err(e) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Checkpoint Error: {:?}", e),
        ),
    }
}

async fn handle_compact(
    State(state): State<AppState>,
) -> (StatusCode, Json<Envelope<OperationResponse>>) {
    if let Some(fusion) = state.storage.as_any().downcast_ref::<FusionStorage>() {
        match fusion.compact_now().await {
            Ok(true) => json_ok(OperationResponse {
                operation: "compact".to_string(),
                message: Some("Compaction completed".to_string()),
                supported: true,
            }),
            Ok(false) => json_ok(OperationResponse {
                operation: "compact".to_string(),
                message: Some("Compaction skipped: not enough SSTables".to_string()),
                supported: true,
            }),
            Err(e) => json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Compaction Error: {:?}", e),
            ),
        }
    } else {
        json_error(
            StatusCode::NOT_IMPLEMENTED,
            "Compaction is only available for FusionStorage",
        )
    }
}

async fn handle_cdc_events(
    State(state): State<AppState>,
    Extension(context): Extension<RequestContext>,
    Query(params): Query<CdcEventsRequest>,
) -> (StatusCode, Json<Envelope<CdcEventsResponse>>) {
    let username = context.username.unwrap_or_default();
    if let Err(e) = state.executor.require_superuser(&username).await {
        return json_error(
            StatusCode::FORBIDDEN,
            format!("Authorization Error: {:?}", e),
        );
    }

    if let Some(fusion) = state.storage.as_any().downcast_ref::<FusionStorage>() {
        let since = params.since.unwrap_or(0);
        let limit = params.limit.unwrap_or(100).min(1000);
        match fusion.cdc_events_since(since, limit).await {
            Ok(events) => {
                let next_since = events.last().map(|event| event.sequence).unwrap_or(since);
                match fusion.cdc_latest_sequence().await {
                    Ok(latest_sequence) => json_ok(CdcEventsResponse {
                        events,
                        next_since,
                        latest_sequence,
                    }),
                    Err(e) => json_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("CDC latest sequence error: {:?}", e),
                    ),
                }
            }
            Err(e) => json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("CDC Error: {:?}", e),
            ),
        }
    } else {
        json_error(
            StatusCode::NOT_IMPLEMENTED,
            "CDC is only available for FusionStorage",
        )
    }
}

async fn handle_capabilities(
    State(state): State<AppState>,
) -> (StatusCode, Json<Envelope<CapabilityInfo>>) {
    json_ok(CapabilityInfo::from_storage(&state.storage))
}

async fn handle_query(
    State(state): State<AppState>,
    Extension(context): Extension<RequestContext>,
    Json(payload): Json<QueryRequest>,
) -> ApiResponse<Vec<QueryResultJson>> {
    let username = context.username.unwrap_or_default();

    if let Err(e) = state.executor.authorize_sql(&username, &payload.sql).await {
        return json_error(StatusCode::FORBIDDEN, format!("{:?}", e));
    }

    match state.executor.execute_sql(&payload.sql).await {
        Ok(results) => json_ok(results.into_iter().map(|r| r.into()).collect()),
        Err(e) => json_error(StatusCode::BAD_REQUEST, format!("{:?}", e)),
    }
}

async fn handle_tables(
    State(state): State<AppState>,
    Extension(context): Extension<RequestContext>,
) -> ApiResponse<Vec<TableInfo>> {
    let username = context.username.unwrap_or_default();

    match state.storage.begin_transaction().await {
        Ok(txn) => match txn.scan_prefix(b"schema:", None).await {
            Ok(pairs) => {
                let mut tables = Vec::new();
                for (_, value) in pairs {
                    if let Ok(schema) = bincode::deserialize::<TableSchema>(&value) {
                        if !username.is_empty()
                            && state
                                .executor
                                .check_table_permission(&username, &schema.name, "SELECT")
                                .await
                                .is_err()
                        {
                            continue;
                        }
                        tables.push(TableInfo {
                            name: schema.name,
                            columns: schema
                                .columns
                                .into_iter()
                                .map(|c| ColumnInfo {
                                    name: c.name,
                                    data_type: c.data_type,
                                    is_primary: c.is_primary,
                                    is_indexed: c.is_indexed,
                                    is_nullable: c.is_nullable,
                                    is_unique: c.is_unique,
                                    default_value: c.default_value,
                                    index_type: format!("{:?}", c.index_type),
                                })
                                .collect(),
                        });
                    }
                }
                json_ok(tables)
            }
            Err(e) => json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Table listing error: {:?}", e),
            ),
        },
        Err(e) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Transaction Error: {:?}", e),
        ),
    }
}

async fn handle_prepare(
    State(state): State<AppState>,
    Extension(context): Extension<RequestContext>,
    Json(payload): Json<PrepareRequest>,
) -> (StatusCode, Json<Envelope<PreparedStatementInfo>>) {
    let username = context.username.unwrap_or_default();

    if let Err(e) = state.executor.authorize_sql(&username, &payload.sql).await {
        return json_error(
            StatusCode::FORBIDDEN,
            format!("Authorization Error: {:?}", e),
        );
    }

    match state
        .executor
        .register_prepared_statement(&payload.sql, Some(&username))
    {
        Ok(record) => json_ok(record.into()),
        Err(e) => json_error(StatusCode::BAD_REQUEST, format!("Prepare Error: {:?}", e)),
    }
}

async fn handle_list_prepared(
    State(state): State<AppState>,
    Extension(context): Extension<RequestContext>,
) -> (StatusCode, Json<Envelope<Vec<PreparedStatementInfo>>>) {
    let username = context.username.unwrap_or_default();
    let prepared = state
        .executor
        .list_prepared_statements(Some(&username))
        .into_iter()
        .map(PreparedStatementInfo::from)
        .collect();
    json_ok(prepared)
}

async fn handle_deallocate_prepared(
    State(state): State<AppState>,
    Extension(context): Extension<RequestContext>,
    Path(statement_id): Path<String>,
) -> (StatusCode, Json<Envelope<PreparedStatementInfo>>) {
    let username = context.username.unwrap_or_default();
    match state
        .executor
        .remove_prepared_statement(&statement_id, Some(&username))
    {
        Ok(record) => json_ok(record.into()),
        Err(e) => json_error(
            prepared_statement_error_status(&e),
            format!("Deallocate Error: {:?}", e),
        ),
    }
}

async fn handle_execute(
    State(state): State<AppState>,
    Extension(context): Extension<RequestContext>,
    Json(payload): Json<ExecuteRequest>,
) -> ApiResponse<Vec<QueryResultJson>> {
    let username = context.username.unwrap_or_default();

    match state
        .executor
        .get_prepared_statement_for_owner(&payload.statement_id, Some(&username))
    {
        Ok(record) => {
            for statement in record.statements.iter() {
                if let Err(e) = state
                    .executor
                    .authorize_statement(&username, statement)
                    .await
                {
                    return json_error(
                        StatusCode::FORBIDDEN,
                        format!("Authorization Error: {:?}", e),
                    );
                }
            }

            let mut results = Vec::new();
            let params: Vec<Value> = payload.params.iter().map(Value::from_json).collect();
            let return_results = payload.return_results.unwrap_or(true);

            let mut may_change_query_results = false;
            match state.storage.begin_transaction().await {
                Ok(mut txn) => {
                    for stmt in record.statements.iter() {
                        may_change_query_results |=
                            Executor::statement_may_change_query_results(stmt);
                        match state
                            .executor
                            .execute_in_transaction_with_params(stmt, &mut *txn, &params)
                            .await
                        {
                            Ok(res) => {
                                if return_results {
                                    results.push(res.into());
                                }
                            }
                            Err(e) => {
                                let _ = txn.rollback().await;
                                return json_error(
                                    StatusCode::BAD_REQUEST,
                                    format!("Execution Error: {:?}", e),
                                );
                            }
                        }
                    }
                    if let Err(e) = txn.commit().await {
                        return json_error(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Commit Error: {:?}", e),
                        );
                    }
                    if may_change_query_results {
                        state.executor.invalidate_query_result_cache();
                    }
                }
                Err(e) => {
                    return json_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Transaction Error: {:?}", e),
                    );
                }
            }
            json_ok(results)
        }
        Err(e) => json_error(
            prepared_statement_error_status(&e),
            format!("Statement Error: {:?}", e),
        ),
    }
}

#[derive(Clone)]
pub struct AppState {
    executor: Arc<Executor>,
    storage: Arc<dyn Storage>,
}

#[derive(Clone, Default)]
struct RequestContext {
    username: Option<String>,
}

#[derive(Deserialize)]
pub struct QueryRequest {
    sql: String,
}

#[derive(Deserialize)]
pub struct CdcEventsRequest {
    since: Option<u64>,
    limit: Option<usize>,
}

pub type ApiResponse<T> = (StatusCode, Json<Envelope<T>>);
pub type QueryResponse = Envelope<Vec<QueryResultJson>>;

#[derive(Serialize, Deserialize)]
pub struct Envelope<T> {
    status: String,
    data: Option<T>,
    error: Option<String>,
}

#[derive(Serialize)]
pub struct MetricsSnapshot {
    sql_parse_count: u64,
    sql_plan_count: u64,
    row_read_count: u64,
    row_cache_hit_count: u64,
    row_write_count: u64,
    fts_search_count: u64,
    fts_doc_hits: u64,
    wal_write_count: u64,
    wal_write_bytes: u64,
    query_count: u64,
    slow_query_count: u64,
    query_total_us: u64,
}

#[derive(Deserialize)]
pub struct PrepareRequest {
    sql: String,
}

#[derive(Serialize, Deserialize)]
pub struct OperationResponse {
    operation: String,
    message: Option<String>,
    supported: bool,
}

#[derive(Serialize, Deserialize)]
pub struct PreparedStatementInfo {
    statement_id: String,
    sql: String,
    statement_count: usize,
    owner: Option<String>,
    created_at_epoch_ms: u128,
}

#[derive(Serialize, Deserialize)]
pub struct CapabilityInfo {
    backend: String,
    snapshot_supported: bool,
    compact_supported: bool,
    cdc_supported: bool,
    prepared_statement_ownership: bool,
    distributed_mode: String,
}

#[derive(Serialize, Deserialize)]
pub struct CdcEventsResponse {
    events: Vec<CdcEvent>,
    next_since: u64,
    latest_sequence: u64,
}

#[derive(Serialize, Deserialize)]
pub struct AuthContextInfo {
    username: Option<String>,
    authenticated: bool,
    mode: String,
}

#[derive(Deserialize)]
pub struct ExecuteRequest {
    statement_id: String,
    params: Vec<serde_json::Value>,
    return_results: Option<bool>,
}

#[derive(Serialize, Deserialize)]
pub enum QueryResultJson {
    Select {
        r#type: String,
        columns: Vec<String>,
        rows: Vec<Vec<serde_json::Value>>,
    },
    Success {
        r#type: String,
        message: String,
    },
}

impl From<crate::execution::QueryResult> for QueryResultJson {
    fn from(res: crate::execution::QueryResult) -> Self {
        match res {
            crate::execution::QueryResult::Select { columns, rows } => {
                let json_rows = rows
                    .into_iter()
                    .map(|row| row.iter().map(|v| v.to_json()).collect())
                    .collect();
                QueryResultJson::Select {
                    r#type: "select".to_string(),
                    columns,
                    rows: json_rows,
                }
            }
            crate::execution::QueryResult::Success { message } => QueryResultJson::Success {
                r#type: "success".to_string(),
                message,
            },
        }
    }
}

impl From<PreparedStatementRecord> for PreparedStatementInfo {
    fn from(record: PreparedStatementRecord) -> Self {
        Self {
            statement_id: record.id,
            sql: record.sql,
            statement_count: record.statements.len(),
            owner: record.owner,
            created_at_epoch_ms: record.created_at_epoch_ms,
        }
    }
}

impl CapabilityInfo {
    fn from_storage(storage: &Arc<dyn Storage>) -> Self {
        let compact_supported = storage.as_any().downcast_ref::<FusionStorage>().is_some();
        let cdc_supported = compact_supported;
        let backend = if compact_supported {
            "FusionStorage"
        } else if storage.as_any().downcast_ref::<MemoryStorage>().is_some() {
            "MemoryStorage"
        } else {
            "GenericStorage"
        };
        Self {
            backend: backend.to_string(),
            snapshot_supported: true,
            compact_supported,
            cdc_supported,
            prepared_statement_ownership: true,
            distributed_mode: "isolated".to_string(),
        }
    }
}

impl AuthContextInfo {
    fn from_request_context(context: &RequestContext) -> Self {
        let username = context.username.clone();
        let authenticated = username.is_some();
        Self {
            username,
            authenticated,
            mode: if authenticated {
                "explicit_user".to_string()
            } else {
                "legacy_anonymous".to_string()
            },
        }
    }
}

impl MetricsSnapshot {
    fn capture() -> Self {
        use std::sync::atomic::Ordering::Relaxed;

        let metrics = &crate::monitor::GLOBAL_METRICS;
        Self {
            sql_parse_count: metrics.sql_parse_count.load(Relaxed),
            sql_plan_count: metrics.sql_plan_count.load(Relaxed),
            row_read_count: metrics.row_read_count.load(Relaxed),
            row_cache_hit_count: metrics.row_cache_hit_count.load(Relaxed),
            row_write_count: metrics.row_write_count.load(Relaxed),
            fts_search_count: metrics.fts_search_count.load(Relaxed),
            fts_doc_hits: metrics.fts_doc_hits.load(Relaxed),
            wal_write_count: metrics.wal_write_count.load(Relaxed),
            wal_write_bytes: metrics.wal_write_bytes.load(Relaxed),
            query_count: metrics.query_count.load(Relaxed),
            slow_query_count: metrics.slow_query_count.load(Relaxed),
            query_total_us: metrics.query_total_us.load(Relaxed),
        }
    }
}

fn prepared_statement_error_status(error: &FusionError) -> StatusCode {
    match error {
        FusionError::Execution(message) if message.contains("belongs to") => StatusCode::FORBIDDEN,
        FusionError::Execution(message) if message.contains("not found") => StatusCode::NOT_FOUND,
        _ => StatusCode::BAD_REQUEST,
    }
}

fn json_error<T>(
    status_code: StatusCode,
    error: impl Into<String>,
) -> (StatusCode, Json<Envelope<T>>)
where
    T: Serialize,
{
    (
        status_code,
        Json(Envelope {
            status: "error".to_string(),
            data: None,
            error: Some(error.into()),
        }),
    )
}

fn json_ok<T>(data: T) -> (StatusCode, Json<Envelope<T>>)
where
    T: Serialize,
{
    (
        StatusCode::OK,
        Json(Envelope {
            status: "ok".to_string(),
            data: Some(data),
            error: None,
        }),
    )
}

#[derive(Serialize, Deserialize)]
pub struct TableInfo {
    name: String,
    columns: Vec<ColumnInfo>,
}

#[derive(Serialize, Deserialize)]
pub struct ColumnInfo {
    name: String,
    data_type: String,
    is_primary: bool,
    is_indexed: bool,
    is_nullable: bool,
    is_unique: bool,
    default_value: Option<String>,
    index_type: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::Request as HttpRequest;
    use tower::util::ServiceExt;

    use crate::auth::{save_user, UserRecord};
    use crate::config::StorageConfig;
    use crate::execution::Executor;
    use crate::storage::memory::MemoryStorage;
    use crate::storage::Storage;

    async fn response_json<T: serde::de::DeserializeOwned>(response: Response) -> T {
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        assert!(
            status.is_success()
                || status == StatusCode::FORBIDDEN
                || status == StatusCode::NOT_FOUND,
            "unexpected status: {status}, body: {}",
            String::from_utf8_lossy(&body)
        );
        serde_json::from_slice(&body).expect("decode json")
    }

    fn test_app(storage: Arc<dyn Storage>) -> Router {
        let executor = Arc::new(Executor::new(storage.clone()));
        build_router(AppState { executor, storage })
    }

    #[tokio::test]
    async fn http_prepare_execute_and_deallocate_respect_owner_scope() {
        let wal_path = format!("test_http_prepare_{}.wal", std::process::id());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).expect("storage"));
        let app = test_app(storage.clone());

        let prepare_request = HttpRequest::builder()
            .method("POST")
            .uri("/prepare")
            .header("content-type", "application/json")
            .header("x-fusiondb-user", "alice")
            .body(Body::from(r#"{"sql":"SELECT 1 AS value"}"#))
            .expect("prepare request");
        let prepare_response = app
            .clone()
            .oneshot(prepare_request)
            .await
            .expect("prepare response");
        let prepare_envelope: Envelope<PreparedStatementInfo> =
            response_json(prepare_response).await;
        let prepared = prepare_envelope.data.expect("prepared data");
        assert_eq!(prepared.owner.as_deref(), Some("alice"));
        assert_eq!(prepared.statement_count, 1);

        let list_request = HttpRequest::builder()
            .method("GET")
            .uri("/prepare")
            .header("x-fusiondb-user", "alice")
            .body(Body::empty())
            .expect("list request");
        let list_response = app
            .clone()
            .oneshot(list_request)
            .await
            .expect("list response");
        let list_envelope: Envelope<Vec<PreparedStatementInfo>> =
            response_json(list_response).await;
        assert_eq!(list_envelope.data.expect("list data").len(), 1);

        let execute_request = HttpRequest::builder()
            .method("POST")
            .uri("/execute")
            .header("content-type", "application/json")
            .header("x-fusiondb-user", "alice")
            .body(Body::from(format!(
                r#"{{"statement_id":"{}","params":[]}}"#,
                prepared.statement_id
            )))
            .expect("execute request");
        let execute_response = app
            .clone()
            .oneshot(execute_request)
            .await
            .expect("execute response");
        let execute_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(execute_response).await;
        assert!(execute_envelope.data.expect("execute data").len() == 1);

        let forbidden_request = HttpRequest::builder()
            .method("POST")
            .uri("/execute")
            .header("content-type", "application/json")
            .header("x-fusiondb-user", "bob")
            .body(Body::from(format!(
                r#"{{"statement_id":"{}","params":[]}}"#,
                prepared.statement_id
            )))
            .expect("forbidden request");
        let forbidden_response = app
            .clone()
            .oneshot(forbidden_request)
            .await
            .expect("forbidden response");
        assert_eq!(forbidden_response.status(), StatusCode::FORBIDDEN);

        let delete_request = HttpRequest::builder()
            .method("DELETE")
            .uri(format!("/prepare/{}", prepared.statement_id))
            .header("x-fusiondb-user", "alice")
            .body(Body::empty())
            .expect("delete request");
        let delete_response = app
            .clone()
            .oneshot(delete_request)
            .await
            .expect("delete response");
        let delete_envelope: Envelope<PreparedStatementInfo> = response_json(delete_response).await;
        assert_eq!(
            delete_envelope.data.expect("deleted data").statement_id,
            prepared.statement_id
        );

        let _ = std::fs::remove_file(&wal_path);
    }

    #[tokio::test]
    async fn http_prepared_multi_statement_executes_with_params_in_one_transaction() {
        let wal_path = format!("test_http_prepare_multi_{}.wal", std::process::id());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).expect("storage"));
        let app = test_app(storage.clone());

        let sql = "CREATE TABLE http_prepared_multi (id INTEGER PRIMARY KEY, value INTEGER); \
                   INSERT INTO http_prepared_multi VALUES ($1, $2); \
                   UPDATE http_prepared_multi SET value = value + $3 WHERE id = $1; \
                   SELECT value FROM http_prepared_multi WHERE id = $1";
        let prepare_request = HttpRequest::builder()
            .method("POST")
            .uri("/prepare")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::json!({ "sql": sql }).to_string()))
            .expect("prepare request");
        let prepare_response = app
            .clone()
            .oneshot(prepare_request)
            .await
            .expect("prepare response");
        let prepare_envelope: Envelope<PreparedStatementInfo> =
            response_json(prepare_response).await;
        let prepared = prepare_envelope.data.expect("prepared data");
        assert_eq!(prepared.statement_count, 4);

        let execute_request = HttpRequest::builder()
            .method("POST")
            .uri("/execute")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "statement_id": prepared.statement_id,
                    "params": [1, 10, 5]
                })
                .to_string(),
            ))
            .expect("execute request");
        let execute_response = app
            .clone()
            .oneshot(execute_request)
            .await
            .expect("execute response");
        let execute_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(execute_response).await;
        let results = execute_envelope.data.expect("execute data");
        assert_eq!(results.len(), 4);
        match &results[3] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(15)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected final select"),
        }

        let _ = std::fs::remove_file(&wal_path);
    }

    #[tokio::test]
    async fn http_prepared_execute_can_suppress_result_payload() {
        let wal_path = format!("test_http_prepare_no_results_{}.wal", std::process::id());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).expect("storage"));
        let app = test_app(storage.clone());

        let sql = "CREATE TABLE http_prepared_no_results (id INTEGER PRIMARY KEY, value INTEGER); \
                   INSERT INTO http_prepared_no_results VALUES ($1, $2)";
        let prepare_request = HttpRequest::builder()
            .method("POST")
            .uri("/prepare")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::json!({ "sql": sql }).to_string()))
            .expect("prepare request");
        let prepare_response = app
            .clone()
            .oneshot(prepare_request)
            .await
            .expect("prepare response");
        let prepare_envelope: Envelope<PreparedStatementInfo> =
            response_json(prepare_response).await;
        let prepared = prepare_envelope.data.expect("prepared data");

        let execute_request = HttpRequest::builder()
            .method("POST")
            .uri("/execute")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "statement_id": prepared.statement_id,
                    "params": [1, 10],
                    "return_results": false
                })
                .to_string(),
            ))
            .expect("execute request");
        let execute_response = app
            .clone()
            .oneshot(execute_request)
            .await
            .expect("execute response");
        let execute_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(execute_response).await;
        assert!(execute_envelope.data.expect("execute data").is_empty());

        let query_request = HttpRequest::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"sql":"SELECT value FROM http_prepared_no_results WHERE id = 1"}"#,
            ))
            .expect("query request");
        let query_response = app
            .clone()
            .oneshot(query_request)
            .await
            .expect("query response");
        let query_envelope: Envelope<Vec<QueryResultJson>> = response_json(query_response).await;
        match &query_envelope.data.expect("query data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(10)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected select"),
        }

        let _ = std::fs::remove_file(&wal_path);
    }

    #[tokio::test]
    async fn http_capabilities_and_auth_context_are_exposed() {
        let wal_path = format!("test_http_caps_{}.wal", std::process::id());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).expect("storage"));
        let app = test_app(storage.clone());

        let capabilities_request = HttpRequest::builder()
            .method("GET")
            .uri("/capabilities")
            .body(Body::empty())
            .expect("capabilities request");
        let capabilities_response = app
            .clone()
            .oneshot(capabilities_request)
            .await
            .expect("capabilities response");
        let capabilities: Envelope<CapabilityInfo> = response_json(capabilities_response).await;
        let capability_data = capabilities.data.expect("capability data");
        assert_eq!(capability_data.backend, "MemoryStorage");
        assert!(!capability_data.compact_supported);
        assert!(!capability_data.cdc_supported);
        assert_eq!(capability_data.distributed_mode, "isolated");

        let auth_request = HttpRequest::builder()
            .method("GET")
            .uri("/auth/context")
            .header("x-fusiondb-user", "alice")
            .body(Body::empty())
            .expect("auth request");
        let auth_response = app
            .clone()
            .oneshot(auth_request)
            .await
            .expect("auth response");
        let auth_context: Envelope<AuthContextInfo> = response_json(auth_response).await;
        let auth_data = auth_context.data.expect("auth data");
        assert_eq!(auth_data.username.as_deref(), Some("alice"));
        assert!(auth_data.authenticated);
        assert_eq!(auth_data.mode, "explicit_user");

        let _ = std::fs::remove_file(&wal_path);
    }

    #[tokio::test]
    async fn http_cdc_events_returns_committed_fusion_changes() {
        let data_dir =
            std::env::temp_dir().join(format!("fusiondb_http_cdc_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = StorageConfig::default();
        config.data_dir = data_dir.to_string_lossy().to_string();
        let wal_path = config.wal_path();
        let fusion = FusionStorage::with_config(&wal_path.to_string_lossy(), &config)
            .await
            .expect("fusion storage");

        {
            let mut txn = fusion.begin_transaction().await.expect("begin txn");
            txn.put(b"data:http_cdc:001", b"one")
                .await
                .expect("put row");
            txn.commit().await.expect("commit");
        }

        let storage: Arc<dyn Storage> = Arc::new(fusion.clone());
        let app = test_app(storage);
        let request = HttpRequest::builder()
            .method("GET")
            .uri("/cdc/events?since=0&limit=10")
            .body(Body::empty())
            .expect("cdc request");
        let response = app.clone().oneshot(request).await.expect("cdc response");
        let envelope: Envelope<CdcEventsResponse> = response_json(response).await;
        let data = envelope.data.expect("cdc data");

        assert_eq!(data.events.len(), 1);
        assert_eq!(data.events[0].key.data, "data:http_cdc:001");
        assert_eq!(data.events[0].value.as_ref().unwrap().data, "one");
        assert_eq!(data.next_since, data.events[0].sequence);
        assert_eq!(data.latest_sequence, data.events[0].sequence);

        let resume_request = HttpRequest::builder()
            .method("GET")
            .uri(format!("/cdc/events?since={}&limit=10", data.next_since))
            .body(Body::empty())
            .expect("cdc resume request");
        let resume_response = app
            .clone()
            .oneshot(resume_request)
            .await
            .expect("cdc resume response");
        let resume_envelope: Envelope<CdcEventsResponse> = response_json(resume_response).await;
        let resumed = resume_envelope.data.expect("resume cdc data");
        assert!(resumed.events.is_empty());
        assert_eq!(resumed.next_since, data.next_since);
        assert_eq!(resumed.latest_sequence, data.latest_sequence);

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn http_cdc_events_requires_registered_superuser() {
        let wal_path = format!("test_http_cdc_auth_{}.wal", std::process::id());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).expect("storage"));

        {
            let mut txn = storage.begin_transaction().await.expect("begin txn");
            let alice = UserRecord::new("fusiondb", false);
            save_user(&mut *txn, "alice", &alice)
                .await
                .expect("save user");
            txn.commit().await.expect("commit user txn");
        }

        let app = test_app(storage.clone());
        let request = HttpRequest::builder()
            .method("GET")
            .uri("/cdc/events")
            .header("x-fusiondb-user", "alice")
            .body(Body::empty())
            .expect("cdc auth request");
        let response = app.oneshot(request).await.expect("cdc auth response");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let _ = std::fs::remove_file(&wal_path);
    }

    #[tokio::test]
    async fn http_query_and_tables_enforce_rbac_for_registered_users() {
        let wal_path = format!("test_http_rbac_{}.wal", std::process::id());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).expect("storage"));

        {
            let mut txn = storage.begin_transaction().await.expect("begin txn");
            let mut alice = UserRecord::new("fusiondb", false);
            alice.grant("allowed_http", "SELECT");
            save_user(&mut *txn, "alice", &alice)
                .await
                .expect("save user");
            txn.commit().await.expect("commit user txn");
        }

        let app = test_app(storage.clone());

        let create_request = HttpRequest::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"sql":"CREATE TABLE allowed_http (id INTEGER PRIMARY KEY, val TEXT)"}"#,
            ))
            .expect("create request");
        let create_response = app
            .clone()
            .oneshot(create_request)
            .await
            .expect("create response");
        assert_eq!(create_response.status(), StatusCode::OK);

        let insert_request = HttpRequest::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"sql":"INSERT INTO allowed_http VALUES (1, 'ok')"}"#,
            ))
            .expect("insert request");
        let insert_response = app
            .clone()
            .oneshot(insert_request)
            .await
            .expect("insert response");
        assert_eq!(insert_response.status(), StatusCode::OK);

        let allowed_query = HttpRequest::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .header("x-fusiondb-user", "alice")
            .body(Body::from(r#"{"sql":"SELECT * FROM allowed_http"}"#))
            .expect("allowed query");
        let allowed_response = app
            .clone()
            .oneshot(allowed_query)
            .await
            .expect("allowed response");
        assert_eq!(allowed_response.status(), StatusCode::OK);

        let forbidden_query = HttpRequest::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .header("x-fusiondb-user", "alice")
            .body(Body::from(
                r#"{"sql":"CREATE TABLE forbidden_http (id INTEGER PRIMARY KEY)"}"#,
            ))
            .expect("forbidden query");
        let forbidden_response = app
            .clone()
            .oneshot(forbidden_query)
            .await
            .expect("forbidden response");
        assert_eq!(forbidden_response.status(), StatusCode::FORBIDDEN);

        let tables_request = HttpRequest::builder()
            .method("GET")
            .uri("/tables")
            .header("x-fusiondb-user", "alice")
            .body(Body::empty())
            .expect("tables request");
        let tables_response = app
            .clone()
            .oneshot(tables_request)
            .await
            .expect("tables response");
        let tables_envelope: Envelope<Vec<TableInfo>> = response_json(tables_response).await;
        assert_eq!(tables_envelope.data.expect("tables data").len(), 1);

        let _ = std::fs::remove_file(&wal_path);
    }
}
