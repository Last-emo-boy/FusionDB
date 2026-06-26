use axum::{
    extract::{Extension, Path, Query, Request, State},
    http::{header::AUTHORIZATION, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{delete, get, post},
    Json, Router,
};
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;

use crate::catalog::TableSchema;
use crate::common::{FusionError, Value};
use crate::distributed::api::{raft_routes, submit_raft_write, RaftAppState};
use crate::distributed::sharding::ShardRouter;
use crate::distributed::FusionRaft;
use crate::execution::{Executor, PreparedStatementRecord, SqlShardOwner, SqlShardRoutingDecision};
use crate::parser::parse_sql;
use crate::storage::Storage;

use crate::storage::fusion::{CdcEvent, FusionStorage};
use crate::storage::memory::MemoryStorage;

const SHARD_OWNER_FORWARD_HEADER: &str = "x-fusiondb-forwarded";
const SHARD_OWNER_FORWARD_VALUE: &str = "shard-owner";

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
    let mut app = Router::new()
        .route("/health", get(health_check))
        .route("/query", post(handle_query))
        .route("/copy_stdin", post(handle_copy_stdin))
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
        .with_state(state.clone());

    if let Some(raft) = state.raft.clone() {
        app = app.merge(raft_routes(RaftAppState {
            raft,
            executor: state.executor.clone(),
            client: state.raft_client.clone(),
            shard_router: state.shard_router.clone(),
        }));
    }

    app
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
    raft: Option<FusionRaft>,
    distributed_mode: String,
    shard_router: Option<ShardRouter>,
) {
    let state = AppState {
        executor,
        storage,
        raft,
        raft_client: reqwest::Client::new(),
        distributed_mode,
        shard_router,
        shard_owner_forwarding_enabled: true,
    };

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
    let headers = request.headers();
    let username = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(extract_bearer_username)
        .or_else(|| {
            headers
                .get("x-fusiondb-user")
                .and_then(|value| value.to_str().ok())
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        });
    let shard_forwarded = headers
        .get(SHARD_OWNER_FORWARD_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.eq_ignore_ascii_case(SHARD_OWNER_FORWARD_VALUE))
        .unwrap_or(false);

    request.extensions_mut().insert(RequestContext {
        username,
        shard_forwarded,
    });
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
         fusiondb_wal_write_bytes {}\n\
         # HELP fusiondb_pg_active_connections Active PostgreSQL wire protocol connections\n\
         # TYPE fusiondb_pg_active_connections gauge\n\
         fusiondb_pg_active_connections {}\n\
         # HELP fusiondb_pg_connection_limit Configured PostgreSQL wire protocol connection limit\n\
         # TYPE fusiondb_pg_connection_limit gauge\n\
         fusiondb_pg_connection_limit {}\n\
         # HELP fusiondb_pg_connection_rejected_count PostgreSQL wire protocol connections rejected after the limit was reached\n\
         # TYPE fusiondb_pg_connection_rejected_count counter\n\
         fusiondb_pg_connection_rejected_count {}\n",
        m.query_count.load(Relaxed),
        m.slow_query_count.load(Relaxed),
        m.query_total_us.load(Relaxed),
        m.row_read_count.load(Relaxed),
        m.row_write_count.load(Relaxed),
        m.row_cache_hit_count.load(Relaxed),
        m.wal_write_count.load(Relaxed),
        m.wal_write_bytes.load(Relaxed),
        m.pg_active_connection_count.load(Relaxed),
        m.pg_connection_limit.load(Relaxed),
        m.pg_connection_rejected_count.load(Relaxed),
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
    json_ok(CapabilityInfo::from_state(&state))
}

async fn handle_query(
    State(state): State<AppState>,
    Extension(context): Extension<RequestContext>,
    Json(payload): Json<QueryRequest>,
) -> ApiResponse<Vec<QueryResultJson>> {
    let username = context.username.clone().unwrap_or_default();

    if let Err(e) = state.executor.authorize_sql(&username, &payload.sql).await {
        return json_error(StatusCode::FORBIDDEN, format!("{:?}", e));
    }

    match shard_write_route_action_for_sql(&state, &payload.sql, &[], context.shard_forwarded).await
    {
        Ok(ShardWriteRouteAction::Local) => {}
        Ok(ShardWriteRouteAction::Forward(decision)) => {
            return forward_query_to_shard_owner(&state, &context, &payload, &decision).await;
        }
        Ok(ShardWriteRouteAction::Conflict(message)) => {
            return json_error(StatusCode::CONFLICT, message);
        }
        Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
    }

    match shard_read_route_action_for_sql(&state, &payload.sql, &[], context.shard_forwarded).await
    {
        Ok(ShardReadRouteAction::Local) => {}
        Ok(ShardReadRouteAction::Forward(decision)) => {
            return forward_query_to_shard_owner(&state, &context, &payload, &decision).await;
        }
        Ok(ShardReadRouteAction::Conflict(message)) => {
            return json_error(StatusCode::CONFLICT, message);
        }
        Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
    }

    if let Some(response) = try_fanout_query_to_shard_owners(&state, &context, &payload.sql).await {
        return response;
    }

    if let Some(raft) = &state.raft {
        match state.executor.sql_requires_raft_write(&payload.sql) {
            Ok(true) => {
                return match submit_raft_write(raft, &state.raft_client, payload.sql).await {
                    Ok(resp) => json_ok(vec![QueryResultJson::Success {
                        r#type: "success".to_string(),
                        message: resp.message,
                    }]),
                    Err(e) => json_error(StatusCode::BAD_REQUEST, e),
                };
            }
            Ok(false) => {}
            Err(e) => return json_error(StatusCode::BAD_REQUEST, format!("{:?}", e)),
        }
    }

    match state.executor.execute_sql(&payload.sql).await {
        Ok(results) => json_ok(results.into_iter().map(|r| r.into()).collect()),
        Err(e) => json_error(StatusCode::BAD_REQUEST, format!("{:?}", e)),
    }
}

async fn handle_copy_stdin(
    State(state): State<AppState>,
    Extension(context): Extension<RequestContext>,
    Json(payload): Json<CopyStdinRequest>,
) -> ApiResponse<Vec<QueryResultJson>> {
    let username = context.username.clone().unwrap_or_default();
    let copy_sql = copy_stdin_sql_for_parse(&payload.sql);

    if let Err(e) = state.executor.authorize_sql(&username, &copy_sql).await {
        return json_error(StatusCode::FORBIDDEN, format!("{:?}", e));
    }

    let copy_payload =
        match base64::engine::general_purpose::STANDARD.decode(&payload.payload_base64) {
            Ok(payload) => payload,
            Err(e) => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    format!("COPY payload decode error: {}", e),
                );
            }
        };
    let statements = match parse_sql(&copy_sql) {
        Ok(statements) => statements,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, format!("Parse Error: {:?}", e)),
    };
    let [statement] = statements.as_slice() else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "COPY STDIN forwarding requires exactly one COPY statement",
        );
    };

    let mut txn = match state.storage.begin_transaction().await {
        Ok(txn) => txn,
        Err(e) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("COPY failed to begin transaction: {:?}", e),
            );
        }
    };

    match state
        .executor
        .execute_copy_stdin_payload(statement, &copy_payload, &mut *txn)
        .await
    {
        Ok(count) => match txn.commit().await {
            Ok(_) => {
                state.executor.invalidate_query_result_cache();
                json_ok(vec![QueryResultJson::Success {
                    r#type: "success".to_string(),
                    message: format!("Copied {} rows", count),
                }])
            }
            Err(e) => json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("COPY failed to commit transaction: {:?}", e),
            ),
        },
        Err(e) => {
            let _ = txn.rollback().await;
            json_error(copy_stdin_error_status(&e), format!("{:?}", e))
        }
    }
}

fn copy_stdin_sql_for_parse(sql: &str) -> String {
    let trimmed = sql.trim_end();
    if trimmed.ends_with(';') {
        trimmed.to_string()
    } else {
        format!("{};", trimmed)
    }
}

enum ShardWriteRouteAction {
    Local,
    Forward(SqlShardRoutingDecision),
    Conflict(String),
}

enum ShardReadRouteAction {
    Local,
    Forward(SqlShardRoutingDecision),
    Conflict(String),
}

async fn shard_write_route_action_for_sql(
    state: &AppState,
    sql: &str,
    params: &[Value],
    shard_forwarded: bool,
) -> std::result::Result<ShardWriteRouteAction, String> {
    let decisions = state
        .executor
        .shard_routing_decisions_for_sql(sql, params)
        .await
        .map_err(|e| format!("Shard routing error: {:?}", e))?;
    Ok(shard_write_route_action(state, decisions, shard_forwarded))
}

async fn shard_write_route_action_for_statements(
    state: &AppState,
    statements: &[sqlparser::ast::Statement],
    params: &[Value],
    shard_forwarded: bool,
) -> std::result::Result<ShardWriteRouteAction, String> {
    let decisions = state
        .executor
        .shard_routing_decisions_for_statements(statements, params)
        .await
        .map_err(|e| format!("Shard routing error: {:?}", e))?;
    Ok(shard_write_route_action(state, decisions, shard_forwarded))
}

async fn shard_read_route_action_for_sql(
    state: &AppState,
    sql: &str,
    params: &[Value],
    shard_forwarded: bool,
) -> std::result::Result<ShardReadRouteAction, String> {
    let decision = state
        .executor
        .shard_read_route_decision_for_sql(sql, params)
        .await
        .map_err(|e| format!("Shard read routing error: {:?}", e))?;
    Ok(shard_read_route_action(state, decision, shard_forwarded))
}

async fn shard_read_route_action_for_statements(
    state: &AppState,
    statements: &[sqlparser::ast::Statement],
    params: &[Value],
    shard_forwarded: bool,
) -> std::result::Result<ShardReadRouteAction, String> {
    let decision = state
        .executor
        .shard_read_route_decision_for_statements(statements, params)
        .await
        .map_err(|e| format!("Shard read routing error: {:?}", e))?;
    Ok(shard_read_route_action(state, decision, shard_forwarded))
}

fn shard_write_route_action(
    state: &AppState,
    decisions: Vec<SqlShardRoutingDecision>,
    shard_forwarded: bool,
) -> ShardWriteRouteAction {
    let mut local_decisions = Vec::new();
    let mut non_local_decisions = Vec::new();
    for decision in decisions {
        if decision.is_local_owner() {
            local_decisions.push(decision);
        } else {
            non_local_decisions.push(decision);
        }
    }

    if non_local_decisions.is_empty() {
        return ShardWriteRouteAction::Local;
    }

    if shard_forwarded || !state.shard_owner_forwarding_enabled {
        return ShardWriteRouteAction::Conflict(non_local_shard_write_message(
            &non_local_decisions[0],
        ));
    }

    if !local_decisions.is_empty() {
        return ShardWriteRouteAction::Conflict(multi_shard_forwarding_message(
            &non_local_decisions[0],
            "automatic forwarding currently supports SQL requests whose routed point writes all target one non-local owner",
        ));
    }

    let first = &non_local_decisions[0];
    if non_local_decisions.iter().any(|decision| {
        decision.route.owner_node_id != first.route.owner_node_id
            || decision.route.owner_addr != first.route.owner_addr
    }) {
        return ShardWriteRouteAction::Conflict(multi_shard_forwarding_message(
            first,
            "automatic forwarding currently supports one non-local owner per SQL request",
        ));
    }

    ShardWriteRouteAction::Forward(first.clone())
}

fn shard_read_route_action(
    state: &AppState,
    decision: Option<SqlShardRoutingDecision>,
    shard_forwarded: bool,
) -> ShardReadRouteAction {
    let Some(decision) = decision else {
        return ShardReadRouteAction::Local;
    };
    if decision.is_local_owner() {
        return ShardReadRouteAction::Local;
    }
    if shard_forwarded || !state.shard_owner_forwarding_enabled {
        return ShardReadRouteAction::Conflict(non_local_shard_write_message(&decision));
    }
    ShardReadRouteAction::Forward(decision)
}

fn non_local_shard_write_message(decision: &SqlShardRoutingDecision) -> String {
    format!(
        "Shard route conflict: {} on table {} with shard key {} routes to shard {} owned by node {} at {}; local node {} is not the owner for this routed operation",
        decision.operation,
        decision.route.table,
        decision.route.shard_key,
        decision.route.shard_id,
        decision.route.owner_node_id,
        decision.route.owner_addr,
        decision.local_node_id
    )
}

fn multi_shard_forwarding_message(decision: &SqlShardRoutingDecision, reason: &str) -> String {
    format!("{}; {}", non_local_shard_write_message(decision), reason)
}

async fn forward_query_to_shard_owner(
    state: &AppState,
    context: &RequestContext,
    payload: &QueryRequest,
    decision: &SqlShardRoutingDecision,
) -> ApiResponse<Vec<QueryResultJson>> {
    let url = format!("http://{}/query", decision.route.owner_addr);
    let mut request = state
        .raft_client
        .post(&url)
        .header(SHARD_OWNER_FORWARD_HEADER, SHARD_OWNER_FORWARD_VALUE)
        .json(payload);
    if let Some(username) = context.username.as_deref() {
        request = request.header("x-fusiondb-user", username);
    }

    let response = match request.send().await {
        Ok(response) => response,
        Err(e) => {
            return json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard owner forwarding error: {} on table {} to node {} at {} failed: {}",
                    decision.operation,
                    decision.route.table,
                    decision.route.owner_node_id,
                    decision.route.owner_addr,
                    e
                ),
            );
        }
    };
    let status = response.status();
    match response.json::<Envelope<Vec<QueryResultJson>>>().await {
        Ok(envelope) => (status, Json(envelope)),
        Err(e) => json_error(
            StatusCode::BAD_GATEWAY,
            format!(
                "Shard owner forwarding error: response from node {} at {} could not be decoded: {}",
                decision.route.owner_node_id, decision.route.owner_addr, e
            ),
        ),
    }
}

async fn try_fanout_query_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    sql: &str,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled {
        return None;
    }
    let owners = match state
        .executor
        .shard_select_fanout_owners_for_sql(sql, &[])
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard select fan-out planning error: {:?}", e),
            ));
        }
    };

    let local_results = match state.executor.execute_sql(sql).await {
        Ok(results) => results.into_iter().map(QueryResultJson::from).collect(),
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Execution Error: {:?}", e),
            ));
        }
    };

    let mut columns = None;
    let mut rows = Vec::new();
    if let Err(message) = append_fanout_select_results(&mut columns, &mut rows, local_results) {
        return Some(json_error(StatusCode::BAD_GATEWAY, message));
    }

    for owner in owners {
        let owner_results = match query_remote_shard_owner(state, context, sql, &owner).await {
            Ok(results) => results,
            Err((status, message)) => return Some(json_error(status, message)),
        };
        if let Err(message) = append_fanout_select_results(&mut columns, &mut rows, owner_results) {
            return Some(json_error(StatusCode::BAD_GATEWAY, message));
        }
    }

    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns: columns.unwrap_or_default(),
        rows,
    }]))
}

async fn query_remote_shard_owner(
    state: &AppState,
    context: &RequestContext,
    sql: &str,
    owner: &SqlShardOwner,
) -> std::result::Result<Vec<QueryResultJson>, (StatusCode, String)> {
    let url = format!("http://{}/query", owner.addr);
    let payload = QueryRequest {
        sql: sql.to_string(),
    };
    let response = apply_forwarding_headers(state.raft_client.post(&url), context)
        .json(&payload)
        .send()
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard select fan-out error: forwarding to node {} at {} failed: {}",
                    owner.node_id, owner.addr, e
                ),
            )
        })?;
    let status = response.status();
    let envelope = response
        .json::<Envelope<Vec<QueryResultJson>>>()
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard select fan-out error: response from node {} at {} could not be decoded: {}",
                    owner.node_id, owner.addr, e
                ),
            )
        })?;

    if !status.is_success() || envelope.status != "ok" {
        return Err((
            status,
            format!(
                "Shard select fan-out error: node {} at {} rejected query: {}",
                owner.node_id,
                owner.addr,
                envelope
                    .error
                    .unwrap_or_else(|| "owner node returned no error message".to_string())
            ),
        ));
    }
    envelope.data.ok_or_else(|| {
        (
            StatusCode::BAD_GATEWAY,
            format!(
                "Shard select fan-out error: node {} at {} returned no query data",
                owner.node_id, owner.addr
            ),
        )
    })
}

fn append_fanout_select_results(
    columns: &mut Option<Vec<String>>,
    rows: &mut Vec<Vec<serde_json::Value>>,
    results: Vec<QueryResultJson>,
) -> std::result::Result<(), String> {
    let [result] = results.as_slice() else {
        return Err("Shard select fan-out expected exactly one SELECT result".to_string());
    };
    let QueryResultJson::Select {
        columns: result_columns,
        rows: result_rows,
        ..
    } = result
    else {
        return Err("Shard select fan-out received a non-SELECT result".to_string());
    };

    if let Some(columns) = columns.as_ref() {
        if columns != result_columns {
            return Err(format!(
                "Shard select fan-out column mismatch: expected {:?}, got {:?}",
                columns, result_columns
            ));
        }
    } else {
        *columns = Some(result_columns.clone());
    }
    rows.extend(result_rows.iter().cloned());
    Ok(())
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
    let username = context.username.clone().unwrap_or_default();

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

            match shard_write_route_action_for_statements(
                &state,
                &record.statements,
                &params,
                context.shard_forwarded,
            )
            .await
            {
                Ok(ShardWriteRouteAction::Local) => {}
                Ok(ShardWriteRouteAction::Forward(decision)) => {
                    return forward_execute_to_shard_owner(
                        &state, &context, &record, &payload, &decision,
                    )
                    .await;
                }
                Ok(ShardWriteRouteAction::Conflict(message)) => {
                    return json_error(StatusCode::CONFLICT, message);
                }
                Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
            }

            match shard_read_route_action_for_statements(
                &state,
                &record.statements,
                &params,
                context.shard_forwarded,
            )
            .await
            {
                Ok(ShardReadRouteAction::Local) => {}
                Ok(ShardReadRouteAction::Forward(decision)) => {
                    return forward_execute_to_shard_owner(
                        &state, &context, &record, &payload, &decision,
                    )
                    .await;
                }
                Ok(ShardReadRouteAction::Conflict(message)) => {
                    return json_error(StatusCode::CONFLICT, message);
                }
                Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
            }

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

async fn forward_execute_to_shard_owner(
    state: &AppState,
    context: &RequestContext,
    record: &PreparedStatementRecord,
    payload: &ExecuteRequest,
    decision: &SqlShardRoutingDecision,
) -> ApiResponse<Vec<QueryResultJson>> {
    let prepare_url = format!("http://{}/prepare", decision.route.owner_addr);
    let prepare_payload = PrepareRequest {
        sql: record.sql.clone(),
    };
    let prepare_response =
        match apply_forwarding_headers(state.raft_client.post(&prepare_url), context)
            .json(&prepare_payload)
            .send()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                return json_error(
                    StatusCode::BAD_GATEWAY,
                    shard_forwarding_transport_error(decision, "prepare", e),
                );
            }
        };
    let prepare_status = prepare_response.status();
    let prepare_envelope = match prepare_response
        .json::<Envelope<PreparedStatementInfo>>()
        .await
    {
        Ok(envelope) => envelope,
        Err(e) => {
            return json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard owner forwarding error: prepare response from node {} at {} could not be decoded: {}",
                    decision.route.owner_node_id, decision.route.owner_addr, e
                ),
            );
        }
    };
    if !prepare_status.is_success() || prepare_envelope.status != "ok" {
        return json_error(
            prepare_status,
            format!(
                "Shard owner forwarding prepare error: {}",
                prepare_envelope
                    .error
                    .unwrap_or_else(|| "owner node rejected prepare".to_string())
            ),
        );
    }
    let Some(prepared) = prepare_envelope.data else {
        return json_error(
            StatusCode::BAD_GATEWAY,
            "Shard owner forwarding prepare error: owner node returned no prepared statement",
        );
    };

    let execute_url = format!("http://{}/execute", decision.route.owner_addr);
    let execute_payload = ExecuteRequest {
        statement_id: prepared.statement_id.clone(),
        params: payload.params.clone(),
        return_results: payload.return_results,
    };
    let execute_response =
        match apply_forwarding_headers(state.raft_client.post(&execute_url), context)
            .json(&execute_payload)
            .send()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                best_effort_deallocate_forwarded_statement(state, context, decision, &prepared)
                    .await;
                return json_error(
                    StatusCode::BAD_GATEWAY,
                    shard_forwarding_transport_error(decision, "execute", e),
                );
            }
        };
    let execute_status = execute_response.status();
    let execute_envelope = match execute_response
        .json::<Envelope<Vec<QueryResultJson>>>()
        .await
    {
        Ok(envelope) => envelope,
        Err(e) => {
            best_effort_deallocate_forwarded_statement(state, context, decision, &prepared).await;
            return json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard owner forwarding error: execute response from node {} at {} could not be decoded: {}",
                    decision.route.owner_node_id, decision.route.owner_addr, e
                ),
            );
        }
    };
    best_effort_deallocate_forwarded_statement(state, context, decision, &prepared).await;
    (execute_status, Json(execute_envelope))
}

fn apply_forwarding_headers(
    request: reqwest::RequestBuilder,
    context: &RequestContext,
) -> reqwest::RequestBuilder {
    let request = request.header(SHARD_OWNER_FORWARD_HEADER, SHARD_OWNER_FORWARD_VALUE);
    if let Some(username) = context.username.as_deref() {
        request.header("x-fusiondb-user", username)
    } else {
        request
    }
}

fn shard_forwarding_transport_error(
    decision: &SqlShardRoutingDecision,
    phase: &str,
    error: reqwest::Error,
) -> String {
    format!(
        "Shard owner forwarding error: {} on table {} to node {} at {} failed during {}: {}",
        decision.operation,
        decision.route.table,
        decision.route.owner_node_id,
        decision.route.owner_addr,
        phase,
        error
    )
}

async fn best_effort_deallocate_forwarded_statement(
    state: &AppState,
    context: &RequestContext,
    decision: &SqlShardRoutingDecision,
    prepared: &PreparedStatementInfo,
) {
    let url = format!(
        "http://{}/prepare/{}",
        decision.route.owner_addr, prepared.statement_id
    );
    let _ = apply_forwarding_headers(state.raft_client.delete(url), context)
        .send()
        .await;
}

#[derive(Clone)]
pub struct AppState {
    executor: Arc<Executor>,
    storage: Arc<dyn Storage>,
    raft: Option<FusionRaft>,
    raft_client: reqwest::Client,
    distributed_mode: String,
    shard_router: Option<ShardRouter>,
    shard_owner_forwarding_enabled: bool,
}

#[derive(Clone, Default)]
struct RequestContext {
    username: Option<String>,
    shard_forwarded: bool,
}

#[derive(Deserialize, Serialize)]
pub struct QueryRequest {
    sql: String,
}

#[derive(Deserialize, Serialize)]
pub struct CopyStdinRequest {
    sql: String,
    payload_base64: String,
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
    pg_active_connection_count: u64,
    pg_connection_rejected_count: u64,
    pg_connection_limit: u64,
}

#[derive(Deserialize, Serialize)]
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
    sharding_enabled: bool,
    sharding_strategy: Option<String>,
    shard_count: Option<u64>,
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

#[derive(Deserialize, Serialize)]
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
    fn from_state(state: &AppState) -> Self {
        let compact_supported = state
            .storage
            .as_any()
            .downcast_ref::<FusionStorage>()
            .is_some();
        let cdc_supported = compact_supported;
        let backend = if compact_supported {
            "FusionStorage"
        } else if state
            .storage
            .as_any()
            .downcast_ref::<MemoryStorage>()
            .is_some()
        {
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
            distributed_mode: state.distributed_mode.clone(),
            sharding_enabled: state.shard_router.is_some(),
            sharding_strategy: state
                .shard_router
                .as_ref()
                .map(|router| router.strategy_name().to_string()),
            shard_count: state.shard_router.as_ref().map(ShardRouter::shard_count),
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
            pg_active_connection_count: metrics.pg_active_connection_count.load(Relaxed),
            pg_connection_rejected_count: metrics.pg_connection_rejected_count.load(Relaxed),
            pg_connection_limit: metrics.pg_connection_limit.load(Relaxed),
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

fn copy_stdin_error_status(error: &FusionError) -> StatusCode {
    match error {
        FusionError::ShardRouteConflict(_) => StatusCode::CONFLICT,
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
    use crate::config::{
        Config, DistributedPeerConfig, ShardingConfig, ShardingStrategy, StorageConfig,
    };
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
                || status == StatusCode::NOT_FOUND
                || status == StatusCode::CONFLICT,
            "unexpected status: {status}, body: {}",
            String::from_utf8_lossy(&body)
        );
        serde_json::from_slice(&body).expect("decode json")
    }

    async fn response_text(response: Response) -> String {
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        assert!(
            status.is_success(),
            "unexpected status: {status}, body: {}",
            String::from_utf8_lossy(&body)
        );
        String::from_utf8(body.to_vec()).expect("utf8 response")
    }

    fn test_app(storage: Arc<dyn Storage>) -> Router {
        let executor = Arc::new(Executor::new(storage.clone()));
        build_router(AppState {
            executor,
            storage,
            raft: None,
            raft_client: reqwest::Client::new(),
            distributed_mode: "isolated".to_string(),
            shard_router: None,
            shard_owner_forwarding_enabled: false,
        })
    }

    fn test_app_with_distributed_mode(storage: Arc<dyn Storage>, distributed_mode: &str) -> Router {
        let executor = Arc::new(Executor::new(storage.clone()));
        build_router(AppState {
            executor,
            storage,
            raft: None,
            raft_client: reqwest::Client::new(),
            distributed_mode: distributed_mode.to_string(),
            shard_router: None,
            shard_owner_forwarding_enabled: false,
        })
    }

    fn test_app_with_shard_router(storage: Arc<dyn Storage>, shard_router: ShardRouter) -> Router {
        test_app_with_shard_router_forwarding(storage, shard_router, false)
    }

    fn test_app_with_shard_router_forwarding(
        storage: Arc<dyn Storage>,
        shard_router: ShardRouter,
        shard_owner_forwarding_enabled: bool,
    ) -> Router {
        let executor = Arc::new(Executor::with_config_and_shard_router(
            storage.clone(),
            &StorageConfig::default(),
            Some(shard_router.clone()),
        ));
        build_router(AppState {
            executor,
            storage,
            raft: None,
            raft_client: reqwest::Client::new(),
            distributed_mode: "raft(node_id=1)".to_string(),
            shard_router: Some(shard_router),
            shard_owner_forwarding_enabled,
        })
    }

    fn sharded_http_test_config(shard_count: u64) -> Config {
        sharded_http_test_config_for_node(
            shard_count,
            1,
            "127.0.0.1:8091".to_string(),
            "127.0.0.1:8093".to_string(),
        )
    }

    fn sharded_http_test_config_for_node(
        shard_count: u64,
        node_id: u64,
        node1_addr: String,
        node2_addr: String,
    ) -> Config {
        let mut config = Config::default();
        config.distributed.enabled = true;
        config.distributed.node_id = node_id;
        config.distributed.initial_members = vec![
            DistributedPeerConfig {
                node_id: 1,
                addr: node1_addr,
            },
            DistributedPeerConfig {
                node_id: 2,
                addr: node2_addr,
            },
        ];
        config.distributed.sharding = ShardingConfig {
            enabled: true,
            strategy: ShardingStrategy::Hash,
            shard_count,
            range_boundaries: Vec::new(),
        };
        config
    }

    async fn bind_test_http_listener() -> (tokio::net::TcpListener, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test http listener");
        let addr = listener.local_addr().expect("test http addr");
        (listener, format!("127.0.0.1:{}", addr.port()))
    }

    fn integer_primary_key_for_owner(
        router: &ShardRouter,
        table_name: &str,
        owner_node_id: u64,
    ) -> i64 {
        for value in 1_i64..10_000 {
            let row_id = crate::common::encoding::encode_i64_comparable(value);
            if router.route_key(table_name, &row_id).owner_node_id == owner_node_id {
                return value;
            }
        }
        panic!("no integer key routed to owner node {}", owner_node_id);
    }

    async fn post_query(app: &Router, sql: &str) -> Response {
        let request = HttpRequest::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::json!({ "sql": sql }).to_string()))
            .expect("query request");
        app.clone().oneshot(request).await.expect("query response")
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
    async fn http_capabilities_reports_distributed_mode() {
        let wal_path = format!("test_http_distributed_mode_{}.wal", std::process::id());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).expect("storage"));
        let app = test_app_with_distributed_mode(storage.clone(), "raft(node_id=7)");

        let capabilities_request = HttpRequest::builder()
            .method("GET")
            .uri("/capabilities")
            .body(Body::empty())
            .expect("capabilities request");
        let capabilities_response = app
            .oneshot(capabilities_request)
            .await
            .expect("capabilities response");
        let capabilities: Envelope<CapabilityInfo> = response_json(capabilities_response).await;
        let capability_data = capabilities.data.expect("capability data");
        assert_eq!(capability_data.distributed_mode, "raft(node_id=7)");

        let _ = std::fs::remove_file(&wal_path);
    }

    #[tokio::test]
    async fn http_capabilities_reports_sharding_control_plane() {
        let wal_path = format!(
            "test_http_sharding_capabilities_{}.wal",
            uuid::Uuid::new_v4()
        );
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).expect("storage"));
        let mut config = Config::default();
        config.distributed.enabled = true;
        config.distributed.initial_members = vec![
            DistributedPeerConfig {
                node_id: 1,
                addr: "127.0.0.1:8091".to_string(),
            },
            DistributedPeerConfig {
                node_id: 2,
                addr: "127.0.0.1:8093".to_string(),
            },
        ];
        config.distributed.sharding = ShardingConfig {
            enabled: true,
            strategy: ShardingStrategy::Hash,
            shard_count: 8,
            range_boundaries: Vec::new(),
        };
        let shard_router = ShardRouter::from_config(&config).expect("shard router");
        let app = test_app_with_shard_router(storage.clone(), shard_router);

        let capabilities_request = HttpRequest::builder()
            .method("GET")
            .uri("/capabilities")
            .body(Body::empty())
            .expect("capabilities request");
        let capabilities_response = app
            .oneshot(capabilities_request)
            .await
            .expect("capabilities response");
        let capabilities: Envelope<CapabilityInfo> = response_json(capabilities_response).await;
        let capability_data = capabilities.data.expect("capability data");
        assert!(capability_data.sharding_enabled);
        assert_eq!(capability_data.sharding_strategy.as_deref(), Some("hash"));
        assert_eq!(capability_data.shard_count, Some(8));

        let _ = std::fs::remove_file(&wal_path);
    }

    #[tokio::test]
    async fn http_query_forwards_non_local_shard_owner_insert_to_owner() {
        let local_wal_path = format!(
            "test_http_shard_owner_forward_local_{}.wal",
            uuid::Uuid::new_v4()
        );
        let owner_wal_path = format!(
            "test_http_shard_owner_forward_owner_{}.wal",
            uuid::Uuid::new_v4()
        );
        let local_storage: Arc<dyn Storage> =
            Arc::new(MemoryStorage::new(&local_wal_path).expect("local storage"));
        let owner_storage: Arc<dyn Storage> =
            Arc::new(MemoryStorage::new(&owner_wal_path).expect("owner storage"));
        let (owner_listener, owner_addr) = bind_test_http_listener().await;
        let local_addr = "127.0.0.1:8091".to_string();
        let local_config =
            sharded_http_test_config_for_node(4, 1, local_addr.clone(), owner_addr.clone());
        let owner_config = sharded_http_test_config_for_node(4, 2, local_addr, owner_addr.clone());
        let local_shard_router = ShardRouter::from_config(&local_config).expect("local router");
        let owner_shard_router = ShardRouter::from_config(&owner_config).expect("owner router");
        let remote_key = integer_primary_key_for_owner(&local_shard_router, "forward_users", 2);

        let local_app =
            test_app_with_shard_router_forwarding(local_storage.clone(), local_shard_router, true);
        let owner_app =
            test_app_with_shard_router_forwarding(owner_storage.clone(), owner_shard_router, true);
        tokio::spawn(async move {
            axum::serve(owner_listener, owner_app)
                .await
                .expect("owner http server");
        });

        let client = reqwest::Client::new();
        let create_sql = "CREATE TABLE forward_users (id INTEGER PRIMARY KEY, name TEXT)";
        let local_create = post_query(&local_app, create_sql).await;
        assert_eq!(local_create.status(), StatusCode::OK);
        let owner_create = client
            .post(format!("http://{}/query", owner_addr))
            .json(&serde_json::json!({ "sql": create_sql }))
            .send()
            .await
            .expect("owner create response");
        assert_eq!(owner_create.status(), StatusCode::OK);

        let forwarded_insert = post_query(
            &local_app,
            &format!(
                "INSERT INTO forward_users (id, name) VALUES ({}, 'remote')",
                remote_key
            ),
        )
        .await;
        assert_eq!(forwarded_insert.status(), StatusCode::OK);
        let forwarded_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(forwarded_insert).await;
        assert_eq!(forwarded_envelope.status, "ok");

        let local_select = post_query(
            &local_app,
            &format!("SELECT name FROM forward_users WHERE id = {}", remote_key),
        )
        .await;
        let local_envelope: Envelope<Vec<QueryResultJson>> = response_json(local_select).await;
        match &local_envelope.data.expect("local select data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!("remote")]]);
            }
            QueryResultJson::Success { .. } => panic!("expected local select"),
        }
        let local_storage_probe = Executor::new(local_storage.clone())
            .execute_sql(&format!(
                "SELECT name FROM forward_users WHERE id = {}",
                remote_key
            ))
            .await
            .expect("raw local storage select");
        match &local_storage_probe[0] {
            crate::execution::QueryResult::Select { rows, .. } => assert!(rows.is_empty()),
            crate::execution::QueryResult::Success { .. } => panic!("expected raw local select"),
        }

        let owner_select = client
            .post(format!("http://{}/query", owner_addr))
            .json(&serde_json::json!({
                "sql": format!("SELECT name FROM forward_users WHERE id = {}", remote_key)
            }))
            .send()
            .await
            .expect("owner select response");
        assert_eq!(owner_select.status(), StatusCode::OK);
        let owner_envelope = owner_select
            .json::<Envelope<Vec<QueryResultJson>>>()
            .await
            .expect("owner select envelope");
        match &owner_envelope.data.expect("owner select data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!("remote")]]);
            }
            QueryResultJson::Success { .. } => panic!("expected owner select"),
        }

        let _ = std::fs::remove_file(&local_wal_path);
        let _ = std::fs::remove_file(&owner_wal_path);
    }

    #[tokio::test]
    async fn http_query_fanouts_simple_select_across_shard_owners() {
        let local_wal_path = format!(
            "test_http_shard_owner_select_fanout_local_{}.wal",
            uuid::Uuid::new_v4()
        );
        let owner_wal_path = format!(
            "test_http_shard_owner_select_fanout_owner_{}.wal",
            uuid::Uuid::new_v4()
        );
        let local_storage: Arc<dyn Storage> =
            Arc::new(MemoryStorage::new(&local_wal_path).expect("local storage"));
        let owner_storage: Arc<dyn Storage> =
            Arc::new(MemoryStorage::new(&owner_wal_path).expect("owner storage"));
        let (owner_listener, owner_addr) = bind_test_http_listener().await;
        let local_addr = "127.0.0.1:8091".to_string();
        let local_config =
            sharded_http_test_config_for_node(4, 1, local_addr.clone(), owner_addr.clone());
        let owner_config = sharded_http_test_config_for_node(4, 2, local_addr, owner_addr.clone());
        let local_shard_router = ShardRouter::from_config(&local_config).expect("local router");
        let owner_shard_router = ShardRouter::from_config(&owner_config).expect("owner router");
        let local_key = integer_primary_key_for_owner(&local_shard_router, "fanout_users", 1);
        let remote_key = integer_primary_key_for_owner(&local_shard_router, "fanout_users", 2);

        let local_app =
            test_app_with_shard_router_forwarding(local_storage.clone(), local_shard_router, true);
        let owner_app =
            test_app_with_shard_router_forwarding(owner_storage.clone(), owner_shard_router, true);
        tokio::spawn(async move {
            axum::serve(owner_listener, owner_app)
                .await
                .expect("owner http server");
        });

        let client = reqwest::Client::new();
        let create_sql = "CREATE TABLE fanout_users (id INTEGER PRIMARY KEY, name TEXT)";
        assert_eq!(
            post_query(&local_app, create_sql).await.status(),
            StatusCode::OK
        );
        let owner_create = client
            .post(format!("http://{}/query", owner_addr))
            .json(&serde_json::json!({ "sql": create_sql }))
            .send()
            .await
            .expect("owner create response");
        assert_eq!(owner_create.status(), StatusCode::OK);

        assert_eq!(
            post_query(
                &local_app,
                &format!(
                    "INSERT INTO fanout_users (id, name) VALUES ({}, 'local')",
                    local_key
                ),
            )
            .await
            .status(),
            StatusCode::OK
        );
        assert_eq!(
            post_query(
                &local_app,
                &format!(
                    "INSERT INTO fanout_users (id, name) VALUES ({}, 'remote')",
                    remote_key
                ),
            )
            .await
            .status(),
            StatusCode::OK
        );

        let fanout_select = post_query(&local_app, "SELECT id, name FROM fanout_users").await;
        assert_eq!(fanout_select.status(), StatusCode::OK);
        let envelope: Envelope<Vec<QueryResultJson>> = response_json(fanout_select).await;
        match &envelope.data.expect("fanout data")[0] {
            QueryResultJson::Select { rows, .. } => {
                let mut rows = rows.clone();
                rows.sort_by_key(|row| row[0].as_i64().unwrap());
                let mut expected = vec![
                    vec![serde_json::json!(local_key), serde_json::json!("local")],
                    vec![serde_json::json!(remote_key), serde_json::json!("remote")],
                ];
                expected.sort_by_key(|row| row[0].as_i64().unwrap());
                assert_eq!(rows, expected);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout select"),
        }

        let _ = std::fs::remove_file(&local_wal_path);
        let _ = std::fs::remove_file(&owner_wal_path);
    }

    #[tokio::test]
    async fn http_execute_forwards_non_local_shard_owner_insert_to_owner() {
        let local_wal_path = format!(
            "test_http_shard_owner_execute_forward_local_{}.wal",
            uuid::Uuid::new_v4()
        );
        let owner_wal_path = format!(
            "test_http_shard_owner_execute_forward_owner_{}.wal",
            uuid::Uuid::new_v4()
        );
        let local_storage: Arc<dyn Storage> =
            Arc::new(MemoryStorage::new(&local_wal_path).expect("local storage"));
        let owner_storage: Arc<dyn Storage> =
            Arc::new(MemoryStorage::new(&owner_wal_path).expect("owner storage"));
        let (owner_listener, owner_addr) = bind_test_http_listener().await;
        let local_addr = "127.0.0.1:8091".to_string();
        let local_config =
            sharded_http_test_config_for_node(4, 1, local_addr.clone(), owner_addr.clone());
        let owner_config = sharded_http_test_config_for_node(4, 2, local_addr, owner_addr.clone());
        let local_shard_router = ShardRouter::from_config(&local_config).expect("local router");
        let owner_shard_router = ShardRouter::from_config(&owner_config).expect("owner router");
        let remote_key =
            integer_primary_key_for_owner(&local_shard_router, "forward_exec_users", 2);

        let local_app =
            test_app_with_shard_router_forwarding(local_storage.clone(), local_shard_router, true);
        let owner_app =
            test_app_with_shard_router_forwarding(owner_storage.clone(), owner_shard_router, true);
        tokio::spawn(async move {
            axum::serve(owner_listener, owner_app)
                .await
                .expect("owner http server");
        });

        let client = reqwest::Client::new();
        let create_sql = "CREATE TABLE forward_exec_users (id INTEGER PRIMARY KEY, name TEXT)";
        let local_create = post_query(&local_app, create_sql).await;
        assert_eq!(local_create.status(), StatusCode::OK);
        let owner_create = client
            .post(format!("http://{}/query", owner_addr))
            .json(&serde_json::json!({ "sql": create_sql }))
            .send()
            .await
            .expect("owner create response");
        assert_eq!(owner_create.status(), StatusCode::OK);

        let prepare_request = HttpRequest::builder()
            .method("POST")
            .uri("/prepare")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "sql": "INSERT INTO forward_exec_users (id, name) VALUES ($1, $2); SELECT name FROM forward_exec_users WHERE id = $1"
                })
                .to_string(),
            ))
            .expect("prepare request");
        let prepare_response = local_app
            .clone()
            .oneshot(prepare_request)
            .await
            .expect("prepare response");
        let prepare_envelope: Envelope<PreparedStatementInfo> =
            response_json(prepare_response).await;
        let prepared = prepare_envelope.data.expect("prepared statement");

        let execute_request = HttpRequest::builder()
            .method("POST")
            .uri("/execute")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "statement_id": prepared.statement_id,
                    "params": [remote_key, "remote-exec"]
                })
                .to_string(),
            ))
            .expect("execute request");
        let execute_response = local_app
            .clone()
            .oneshot(execute_request)
            .await
            .expect("execute response");
        assert_eq!(execute_response.status(), StatusCode::OK);
        let execute_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(execute_response).await;
        let execute_results = execute_envelope.data.expect("execute data");
        assert_eq!(execute_results.len(), 2);
        match &execute_results[1] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!("remote-exec")]]);
            }
            QueryResultJson::Success { .. } => panic!("expected forwarded select"),
        }

        let local_select = post_query(
            &local_app,
            &format!(
                "SELECT name FROM forward_exec_users WHERE id = {}",
                remote_key
            ),
        )
        .await;
        let local_envelope: Envelope<Vec<QueryResultJson>> = response_json(local_select).await;
        match &local_envelope.data.expect("local select data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!("remote-exec")]]);
            }
            QueryResultJson::Success { .. } => panic!("expected local select"),
        }

        let prepare_select_request = HttpRequest::builder()
            .method("POST")
            .uri("/prepare")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "sql": "SELECT name FROM forward_exec_users WHERE id = $1"
                })
                .to_string(),
            ))
            .expect("prepare select request");
        let prepare_select_response = local_app
            .clone()
            .oneshot(prepare_select_request)
            .await
            .expect("prepare select response");
        let prepare_select_envelope: Envelope<PreparedStatementInfo> =
            response_json(prepare_select_response).await;
        let prepared_select = prepare_select_envelope
            .data
            .expect("prepared select statement");
        let execute_select_request = HttpRequest::builder()
            .method("POST")
            .uri("/execute")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "statement_id": prepared_select.statement_id,
                    "params": [remote_key]
                })
                .to_string(),
            ))
            .expect("execute select request");
        let execute_select_response = local_app
            .clone()
            .oneshot(execute_select_request)
            .await
            .expect("execute select response");
        assert_eq!(execute_select_response.status(), StatusCode::OK);
        let execute_select_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(execute_select_response).await;
        match &execute_select_envelope.data.expect("execute select data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!("remote-exec")]]);
            }
            QueryResultJson::Success { .. } => panic!("expected forwarded select"),
        }

        let local_storage_probe = Executor::new(local_storage.clone())
            .execute_sql(&format!(
                "SELECT name FROM forward_exec_users WHERE id = {}",
                remote_key
            ))
            .await
            .expect("raw local storage select");
        match &local_storage_probe[0] {
            crate::execution::QueryResult::Select { rows, .. } => assert!(rows.is_empty()),
            crate::execution::QueryResult::Success { .. } => panic!("expected raw local select"),
        }

        let owner_prepared = client
            .get(format!("http://{}/prepare", owner_addr))
            .send()
            .await
            .expect("owner prepared list response");
        assert_eq!(owner_prepared.status(), StatusCode::OK);
        let owner_prepared_envelope = owner_prepared
            .json::<Envelope<Vec<PreparedStatementInfo>>>()
            .await
            .expect("owner prepared list envelope");
        assert!(owner_prepared_envelope
            .data
            .expect("owner prepared list data")
            .is_empty());

        let _ = std::fs::remove_file(&local_wal_path);
        let _ = std::fs::remove_file(&owner_wal_path);
    }

    #[tokio::test]
    async fn http_query_rejects_non_local_shard_owner_point_update() {
        let wal_path = format!("test_http_shard_owner_query_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).expect("storage"));
        let config = sharded_http_test_config(4);
        let shard_router = ShardRouter::from_config(&config).expect("shard router");
        let local_key = integer_primary_key_for_owner(
            &shard_router,
            "route_users",
            shard_router.local_node_id(),
        );
        let remote_key = integer_primary_key_for_owner(&shard_router, "route_users", 2);
        let remote_route = shard_router.route_key(
            "route_users",
            &crate::common::encoding::encode_i64_comparable(remote_key),
        );
        let app = test_app_with_shard_router(storage.clone(), shard_router);

        let create_response = post_query(
            &app,
            "CREATE TABLE route_users (id INTEGER PRIMARY KEY, name TEXT)",
        )
        .await;
        assert_eq!(create_response.status(), StatusCode::OK);

        let local_response = post_query(
            &app,
            &format!(
                "UPDATE route_users SET name = 'local' WHERE id = {}",
                local_key
            ),
        )
        .await;
        assert_eq!(local_response.status(), StatusCode::OK);

        let remote_response = post_query(
            &app,
            &format!(
                "UPDATE route_users SET name = 'remote' WHERE id = {}",
                remote_key
            ),
        )
        .await;
        assert_eq!(remote_response.status(), StatusCode::CONFLICT);
        let envelope: Envelope<Vec<QueryResultJson>> = response_json(remote_response).await;
        let error = envelope.error.expect("route conflict error");
        assert!(error.contains("UPDATE"));
        assert!(error.contains("route_users"));
        assert!(error.contains(&format!("shard {}", remote_route.shard_id)));
        assert!(error.contains("owned by node 2"));
        assert!(error.contains("127.0.0.1:8093"));
        assert!(error.contains("local node 1"));

        let _ = std::fs::remove_file(&wal_path);
    }

    #[tokio::test]
    async fn http_execute_rejects_non_local_shard_owner_point_update_with_params() {
        let wal_path = format!("test_http_shard_owner_execute_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).expect("storage"));
        let config = sharded_http_test_config(4);
        let shard_router = ShardRouter::from_config(&config).expect("shard router");
        let remote_key = integer_primary_key_for_owner(&shard_router, "route_users", 2);
        let app = test_app_with_shard_router(storage.clone(), shard_router);

        let create_response = post_query(
            &app,
            "CREATE TABLE route_users (id INTEGER PRIMARY KEY, name TEXT)",
        )
        .await;
        assert_eq!(create_response.status(), StatusCode::OK);

        let prepare_request = HttpRequest::builder()
            .method("POST")
            .uri("/prepare")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"sql":"UPDATE route_users SET name = $1 WHERE id = $2"}"#,
            ))
            .expect("prepare request");
        let prepare_response = app
            .clone()
            .oneshot(prepare_request)
            .await
            .expect("prepare response");
        let prepare_envelope: Envelope<PreparedStatementInfo> =
            response_json(prepare_response).await;
        let prepared = prepare_envelope.data.expect("prepared statement");

        let execute_request = HttpRequest::builder()
            .method("POST")
            .uri("/execute")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "statement_id": prepared.statement_id,
                    "params": ["remote", remote_key]
                })
                .to_string(),
            ))
            .expect("execute request");
        let execute_response = app
            .oneshot(execute_request)
            .await
            .expect("execute response");
        assert_eq!(execute_response.status(), StatusCode::CONFLICT);
        let envelope: Envelope<Vec<QueryResultJson>> = response_json(execute_response).await;
        let error = envelope.error.expect("route conflict error");
        assert!(error.contains("UPDATE"));
        assert!(error.contains("owned by node 2"));
        assert!(error.contains("local node 1"));

        let _ = std::fs::remove_file(&wal_path);
    }

    #[tokio::test]
    async fn http_query_rejects_non_local_shard_owner_point_delete() {
        let wal_path = format!("test_http_shard_owner_delete_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).expect("storage"));
        let config = sharded_http_test_config(4);
        let shard_router = ShardRouter::from_config(&config).expect("shard router");
        let remote_key = integer_primary_key_for_owner(&shard_router, "route_deletes", 2);
        let app = test_app_with_shard_router(storage.clone(), shard_router);

        let create_response = post_query(
            &app,
            "CREATE TABLE route_deletes (id INTEGER PRIMARY KEY, name TEXT)",
        )
        .await;
        assert_eq!(create_response.status(), StatusCode::OK);

        let delete_response = post_query(
            &app,
            &format!("DELETE FROM route_deletes WHERE id = {}", remote_key),
        )
        .await;
        assert_eq!(delete_response.status(), StatusCode::CONFLICT);
        let envelope: Envelope<Vec<QueryResultJson>> = response_json(delete_response).await;
        let error = envelope.error.expect("route conflict error");
        assert!(error.contains("DELETE"));
        assert!(error.contains("route_deletes"));
        assert!(error.contains("owned by node 2"));
        assert!(error.contains("local node 1"));

        let _ = std::fs::remove_file(&wal_path);
    }

    #[tokio::test]
    async fn http_query_rejects_non_local_shard_owner_insert_values() {
        let wal_path = format!("test_http_shard_owner_insert_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).expect("storage"));
        let config = sharded_http_test_config(4);
        let shard_router = ShardRouter::from_config(&config).expect("shard router");
        let local_key = integer_primary_key_for_owner(
            &shard_router,
            "route_inserts",
            shard_router.local_node_id(),
        );
        let remote_key = integer_primary_key_for_owner(&shard_router, "route_inserts", 2);
        let app = test_app_with_shard_router(storage.clone(), shard_router);

        let create_response = post_query(
            &app,
            "CREATE TABLE route_inserts (id INTEGER PRIMARY KEY, name TEXT)",
        )
        .await;
        assert_eq!(create_response.status(), StatusCode::OK);

        let local_response = post_query(
            &app,
            &format!("INSERT INTO route_inserts VALUES ({}, 'local')", local_key),
        )
        .await;
        assert_eq!(local_response.status(), StatusCode::OK);

        let remote_response = post_query(
            &app,
            &format!(
                "INSERT INTO route_inserts (name, id) VALUES ('remote', {})",
                remote_key
            ),
        )
        .await;
        assert_eq!(remote_response.status(), StatusCode::CONFLICT);
        let envelope: Envelope<Vec<QueryResultJson>> = response_json(remote_response).await;
        let error = envelope.error.expect("route conflict error");
        assert!(error.contains("INSERT"));
        assert!(error.contains("route_inserts"));
        assert!(error.contains("owned by node 2"));
        assert!(error.contains("local node 1"));

        let _ = std::fs::remove_file(&wal_path);
    }

    #[tokio::test]
    async fn http_execute_rejects_non_local_shard_owner_insert_values_with_params() {
        let wal_path = format!(
            "test_http_shard_owner_insert_execute_{}.wal",
            uuid::Uuid::new_v4()
        );
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).expect("storage"));
        let config = sharded_http_test_config(4);
        let shard_router = ShardRouter::from_config(&config).expect("shard router");
        let remote_key = integer_primary_key_for_owner(&shard_router, "route_insert_exec", 2);
        let app = test_app_with_shard_router(storage.clone(), shard_router);

        let create_response = post_query(
            &app,
            "CREATE TABLE route_insert_exec (id INTEGER PRIMARY KEY, name TEXT)",
        )
        .await;
        assert_eq!(create_response.status(), StatusCode::OK);

        let prepare_request = HttpRequest::builder()
            .method("POST")
            .uri("/prepare")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"sql":"INSERT INTO route_insert_exec (name, id) VALUES ($1, $2)"}"#,
            ))
            .expect("prepare request");
        let prepare_response = app
            .clone()
            .oneshot(prepare_request)
            .await
            .expect("prepare response");
        let prepare_envelope: Envelope<PreparedStatementInfo> =
            response_json(prepare_response).await;
        let prepared = prepare_envelope.data.expect("prepared statement");

        let execute_request = HttpRequest::builder()
            .method("POST")
            .uri("/execute")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "statement_id": prepared.statement_id,
                    "params": ["remote", remote_key]
                })
                .to_string(),
            ))
            .expect("execute request");
        let execute_response = app
            .oneshot(execute_request)
            .await
            .expect("execute response");
        assert_eq!(execute_response.status(), StatusCode::CONFLICT);
        let envelope: Envelope<Vec<QueryResultJson>> = response_json(execute_response).await;
        let error = envelope.error.expect("route conflict error");
        assert!(error.contains("INSERT"));
        assert!(error.contains("route_insert_exec"));
        assert!(error.contains("owned by node 2"));
        assert!(error.contains("local node 1"));

        let _ = std::fs::remove_file(&wal_path);
    }

    #[tokio::test]
    async fn http_metrics_include_pg_connection_pool_fields() {
        crate::monitor::set_pg_connection_limit(123);

        let wal_path = format!("test_http_metrics_pg_conn_{}.wal", std::process::id());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).expect("storage"));
        let app = test_app(storage.clone());

        let metrics_request = HttpRequest::builder()
            .method("GET")
            .uri("/metrics")
            .body(Body::empty())
            .expect("metrics request");
        let metrics_response = app
            .clone()
            .oneshot(metrics_request)
            .await
            .expect("metrics response");
        let metrics: Envelope<serde_json::Value> = response_json(metrics_response).await;
        let data = metrics.data.expect("metrics data");
        assert_eq!(data["pg_connection_limit"], serde_json::json!(123));
        assert!(data.get("pg_active_connection_count").is_some());
        assert!(data.get("pg_connection_rejected_count").is_some());

        let prometheus_request = HttpRequest::builder()
            .method("GET")
            .uri("/metrics/prometheus")
            .body(Body::empty())
            .expect("prometheus request");
        let prometheus_response = app
            .oneshot(prometheus_request)
            .await
            .expect("prometheus response");
        let prometheus = response_text(prometheus_response).await;
        assert!(prometheus.contains("fusiondb_pg_active_connections"));
        assert!(prometheus.contains("fusiondb_pg_connection_limit 123"));
        assert!(prometheus.contains("fusiondb_pg_connection_rejected_count"));

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
