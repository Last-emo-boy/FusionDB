use axum::{
    extract::{DefaultBodyLimit, Extension, Path, Query, Request, State},
    http::{
        header::{AUTHORIZATION, WWW_AUTHENTICATE},
        HeaderValue, StatusCode,
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tower_http::cors::CorsLayer;

use crate::catalog::TableSchema;
use crate::common::{FusionError, Value};
use crate::distributed::api::{raft_routes, submit_raft_write, RaftAppState};
use crate::distributed::sharding::ShardRouter;
use crate::distributed::typ::{ReplicatedQueryResult, ReplicatedValue};
use crate::distributed::FusionRaft;
use crate::execution::{
    Executor, GroupMultiAggregate, PreparedStatementRecord, SqlShardExtremum,
    SqlShardGroupAggregateKind, SqlShardOwner, SqlShardRoutingDecision,
};
use crate::parser::parse_sql;
use crate::storage::Storage;

use crate::server::security::{
    ForwardingAuth, FORWARDED_HEADER, FORWARDED_USER_HEADER, FORWARDED_VALUE,
};
use crate::storage::fusion::{CdcEvent, FusionStorage};
use crate::storage::memory::MemoryStorage;

const DISABLE_SQL_BLOCK_ZONE_MAP_PRUNE_HINT: &str =
    "/*+ FUSIONDB_DISABLE_SQL_BLOCK_ZONE_MAP_PRUNE */";
const MAX_FORWARDED_BODY_BYTES: usize = 16 * 1024 * 1024;
const MAX_DIRECT_SEARCH_LIMIT: usize = 1_000;

fn strip_sql_block_zone_map_prune_hint(sql: &str) -> (&str, bool) {
    let trimmed = sql.trim_start();
    if let Some(rest) = trimmed.strip_prefix(DISABLE_SQL_BLOCK_ZONE_MAP_PRUNE_HINT) {
        (rest.trim_start(), true)
    } else {
        (sql, false)
    }
}

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
    let protected = Router::new()
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
        .route("/hybrid_search", post(handle_hybrid_search));

    let mut protected = protected
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_context_middleware,
        ))
        .with_state::<()>(state.clone());

    if let Some(raft) = state.raft.clone() {
        protected = protected.merge(
            raft_routes(RaftAppState {
                raft,
                executor: state.executor.clone(),
                client: state.raft_client.clone(),
                shard_router: state.shard_router.clone(),
                forwarding_auth: state.forwarding_auth.clone(),
                peer_scheme: state.peer_scheme.clone(),
            })
            .route_layer(middleware::from_fn_with_state(
                state.clone(),
                auth_context_middleware,
            )),
        );
    }

    Router::new()
        .route("/health", get(health_check))
        .merge(protected)
        .layer(DefaultBodyLimit::max(MAX_FORWARDED_BODY_BYTES))
        .layer(CorsLayer::permissive())
}

#[derive(Clone)]
pub struct HttpServerSecurity {
    pub postgres_password: String,
    pub http_legacy_unsafe: bool,
    pub forwarding_secret: String,
    pub peer_scheme: String,
}

impl HttpServerSecurity {
    fn legacy_unsafe() -> Self {
        Self {
            postgres_password: "fusiondb".to_string(),
            http_legacy_unsafe: true,
            forwarding_secret: String::new(),
            peer_scheme: "http".to_string(),
        }
    }
}

fn validate_http_server_security_boundary(
    security: &HttpServerSecurity,
    raft_enabled: bool,
    distributed_mode: &str,
    sharding_enabled: bool,
) -> std::io::Result<()> {
    let distributed_topology = raft_enabled
        || sharding_enabled
        || !distributed_mode.trim().eq_ignore_ascii_case("isolated");
    if !distributed_topology {
        return Ok(());
    }
    if security.http_legacy_unsafe {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "legacy unauthenticated HTTP is forbidden for distributed or sharded servers",
        ));
    }
    if security.forwarding_secret.trim().is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "a forwarding secret is required for distributed or sharded HTTP servers",
        ));
    }
    Ok(())
}

#[deprecated(
    note = "Legacy unauthenticated HTTP entry point. Use start_http_server_with_security."
)]
pub async fn start_http_server(
    executor: Arc<Executor>,
    storage: Arc<dyn Storage>,
    bind: &str,
    start_port: u16,
    tls_config: Option<Arc<rustls::ServerConfig>>,
    raft: Option<FusionRaft>,
    distributed_mode: String,
    shard_router: Option<ShardRouter>,
) {
    if let Err(e) = start_http_server_with_security(
        executor,
        storage,
        bind,
        start_port,
        tls_config,
        raft,
        distributed_mode,
        shard_router,
        HttpServerSecurity::legacy_unsafe(),
    )
    .await
    {
        eprintln!("FusionDB HTTP server stopped: {}", e);
    }
}

pub async fn start_http_server_with_security(
    executor: Arc<Executor>,
    storage: Arc<dyn Storage>,
    bind: &str,
    start_port: u16,
    tls_config: Option<Arc<rustls::ServerConfig>>,
    raft: Option<FusionRaft>,
    distributed_mode: String,
    shard_router: Option<ShardRouter>,
    security: HttpServerSecurity,
) -> std::io::Result<()> {
    validate_http_server_security_boundary(
        &security,
        raft.is_some(),
        &distributed_mode,
        shard_router.is_some(),
    )?;
    let forwarding_auth = ForwardingAuth::new(security.forwarding_secret.as_bytes());
    let state = AppState {
        executor,
        storage,
        raft,
        raft_client: reqwest::Client::new(),
        distributed_mode,
        shard_router,
        shard_owner_forwarding_enabled: true,
        postgres_password: Arc::from(security.postgres_password),
        http_legacy_unsafe: security.http_legacy_unsafe,
        forwarding_auth,
        peer_scheme: security.peer_scheme,
    };

    let app = build_router(state);

    let mut port = start_port;
    let listener = loop {
        let addr = format!("{}:{}", bind, port);
        match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => break l,
            Err(e) => {
                if port >= start_port + 100 {
                    return Err(std::io::Error::new(
                        e.kind(),
                        format!(
                            "Could not bind to any port from {} to {}: {}",
                            start_port, port, e
                        ),
                    ));
                }
                port += 1;
            }
        }
    };

    let addr = listener.local_addr()?;
    let scheme = if tls_config.is_some() {
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

    if let Some(tls_config) = tls_config {
        let listener = listener.into_std()?;
        let tls_config = axum_server::tls_rustls::RustlsConfig::from_config(tls_config);
        axum_server::from_tcp_rustls(listener, tls_config)
            .serve(app.into_make_service())
            .await
    } else {
        axum::serve(listener, app).await
    }
}

async fn auth_context_middleware(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let has_forwarded_marker = request.headers().contains_key(FORWARDED_HEADER);
    let internal_username = if has_forwarded_marker {
        let (parts, body) = request.into_parts();
        let body = match axum::body::to_bytes(body, MAX_FORWARDED_BODY_BYTES).await {
            Ok(body) => body,
            Err(_) => {
                return (
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "forwarded request body exceeds the authentication limit",
                )
                    .into_response();
            }
        };
        let request_target = parts
            .uri
            .path_and_query()
            .map(|target| target.as_str())
            .unwrap_or(parts.uri.path());
        let username = state
            .forwarding_auth
            .as_ref()
            .and_then(|auth| auth.verify(&parts.headers, &parts.method, request_target, &body));
        request = Request::from_parts(parts, axum::body::Body::from(body));
        username
    } else {
        None
    };
    let headers = request.headers();

    let (username, shard_forwarded, auth_mode) = if let Some(username) = internal_username {
        (Some(username), true, "internal_hmac")
    } else if has_forwarded_marker {
        return unauthorized_response();
    } else if let Some((username, password)) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(extract_basic_credentials)
    {
        let Some(username) = authenticate_http_user(&state, &username, &password).await else {
            return unauthorized_response();
        };
        (Some(username), false, "basic")
    } else if state.http_legacy_unsafe {
        let username = headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(extract_bearer_username)
            .or_else(|| {
                headers
                    .get(FORWARDED_USER_HEADER)
                    .and_then(|value| value.to_str().ok())
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            });
        let shard_forwarded = headers
            .get(FORWARDED_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.eq_ignore_ascii_case(FORWARDED_VALUE))
            .unwrap_or(false);
        let auth_mode = if username.is_some() {
            "explicit_user"
        } else {
            "legacy_anonymous"
        };
        (username, shard_forwarded, auth_mode)
    } else {
        return unauthorized_response();
    };

    if !management_access_allowed(
        &state,
        username.as_deref().unwrap_or_default(),
        request.uri().path(),
        auth_mode == "internal_hmac",
    )
    .await
    {
        return (StatusCode::FORBIDDEN, "superuser access required").into_response();
    }

    request.extensions_mut().insert(RequestContext {
        username,
        shard_forwarded,
        auth_mode,
        forwarding_auth: state.forwarding_auth.clone(),
        legacy_unsafe: state.http_legacy_unsafe,
    });
    next.run(request).await
}

async fn management_access_allowed(
    state: &AppState,
    username: &str,
    path: &str,
    internal_hmac: bool,
) -> bool {
    if internal_hmac || !is_management_path(path) {
        return true;
    }
    state.executor.require_superuser(username).await.is_ok()
}

fn is_management_path(path: &str) -> bool {
    path.starts_with("/raft/")
        || matches!(
            path,
            "/checkpoint" | "/compact" | "/slow_queries" | "/cdc/events"
        )
}

async fn authenticate_http_user(
    state: &AppState,
    username: &str,
    password: &str,
) -> Option<String> {
    if username.is_empty() {
        return None;
    }
    if username.eq_ignore_ascii_case("postgres") {
        return (state.postgres_password.as_ref() == password).then(|| "postgres".to_string());
    }

    let mut txn = state.storage.begin_transaction().await.ok()?;
    let user = crate::auth::get_user(&mut *txn, username).await.ok()?;
    let _ = txn.rollback().await;
    user.filter(|user| user.verify_password(password))
        .map(|_| username.to_string())
}

fn extract_basic_credentials(header: &str) -> Option<(String, String)> {
    let (scheme, encoded) = header.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Basic") {
        return None;
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (username, password) = decoded.split_once(':')?;
    if username.is_empty() {
        None
    } else {
        Some((username.to_string(), password.to_string()))
    }
}

fn unauthorized_response() -> Response {
    let mut response = (StatusCode::UNAUTHORIZED, "authentication required").into_response();
    response.headers_mut().insert(
        WWW_AUTHENTICATE,
        HeaderValue::from_static("Basic realm=\"FusionDB\", charset=\"UTF-8\""),
    );
    response
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
         # HELP fusiondb_query_result_cache_eligible_count Query-result cache eligible lookups\n\
         # TYPE fusiondb_query_result_cache_eligible_count counter\n\
         fusiondb_query_result_cache_eligible_count {}\n\
         # HELP fusiondb_query_result_cache_hit_count Query-result cache hits\n\
         # TYPE fusiondb_query_result_cache_hit_count counter\n\
         fusiondb_query_result_cache_hit_count {}\n\
         # HELP fusiondb_query_result_cache_miss_count Query-result cache misses\n\
         # TYPE fusiondb_query_result_cache_miss_count counter\n\
         fusiondb_query_result_cache_miss_count {}\n\
         # HELP fusiondb_query_result_cache_stale_count Query-result cache stale-entry misses\n\
         # TYPE fusiondb_query_result_cache_stale_count counter\n\
         fusiondb_query_result_cache_stale_count {}\n\
         # HELP fusiondb_query_result_cache_insert_count Query-result cache inserts\n\
         # TYPE fusiondb_query_result_cache_insert_count counter\n\
         fusiondb_query_result_cache_insert_count {}\n\
         # HELP fusiondb_query_result_cache_invalidation_count Query-result cache invalidations\n\
         # TYPE fusiondb_query_result_cache_invalidation_count counter\n\
         fusiondb_query_result_cache_invalidation_count {}\n\
         # HELP fusiondb_block_cache_hit_count SSTable block cache hits\n\
         # TYPE fusiondb_block_cache_hit_count counter\n\
         fusiondb_block_cache_hit_count {}\n\
         # HELP fusiondb_block_cache_miss_count SSTable block cache misses\n\
         # TYPE fusiondb_block_cache_miss_count counter\n\
         fusiondb_block_cache_miss_count {}\n\
         # HELP fusiondb_block_cache_insert_count SSTable block cache inserts\n\
         # TYPE fusiondb_block_cache_insert_count counter\n\
         fusiondb_block_cache_insert_count {}\n\
         # HELP fusiondb_block_cache_insert_bytes SSTable block cache inserted bytes\n\
         # TYPE fusiondb_block_cache_insert_bytes counter\n\
         fusiondb_block_cache_insert_bytes {}\n\
         # HELP fusiondb_block_cache_fill_skip_count SSTable block cache fills skipped by no-fill reads\n\
         # TYPE fusiondb_block_cache_fill_skip_count counter\n\
         fusiondb_block_cache_fill_skip_count {}\n\
         # HELP fusiondb_block_cache_eviction_count SSTable block cache evictions\n\
         # TYPE fusiondb_block_cache_eviction_count counter\n\
         fusiondb_block_cache_eviction_count {}\n\
         # HELP fusiondb_block_cache_eviction_bytes SSTable block cache evicted bytes\n\
         # TYPE fusiondb_block_cache_eviction_bytes counter\n\
         fusiondb_block_cache_eviction_bytes {}\n\
         # HELP fusiondb_sstable_block_file_open_count SSTable data block file opens\n\
         # TYPE fusiondb_sstable_block_file_open_count counter\n\
         fusiondb_sstable_block_file_open_count {}\n\
         # HELP fusiondb_sstable_block_read_bytes SSTable data block bytes read from files\n\
         # TYPE fusiondb_sstable_block_read_bytes counter\n\
         fusiondb_sstable_block_read_bytes {}\n\
         # HELP fusiondb_sstable_open_count SSTable files opened\n\
         # TYPE fusiondb_sstable_open_count counter\n\
         fusiondb_sstable_open_count {}\n\
         # HELP fusiondb_sstable_open_total_us Total SSTable open time in microseconds\n\
         # TYPE fusiondb_sstable_open_total_us counter\n\
         fusiondb_sstable_open_total_us {}\n\
         # HELP fusiondb_sstable_open_index_bytes SSTable index bytes read during open\n\
         # TYPE fusiondb_sstable_open_index_bytes counter\n\
         fusiondb_sstable_open_index_bytes {}\n\
         # HELP fusiondb_sstable_open_index_read_us SSTable index read time during open in microseconds\n\
         # TYPE fusiondb_sstable_open_index_read_us counter\n\
         fusiondb_sstable_open_index_read_us {}\n\
         # HELP fusiondb_sstable_open_index_decode_us SSTable index decode time during open in microseconds\n\
         # TYPE fusiondb_sstable_open_index_decode_us counter\n\
         fusiondb_sstable_open_index_decode_us {}\n\
         # HELP fusiondb_sstable_open_filter_bytes SSTable filter bytes read during open\n\
         # TYPE fusiondb_sstable_open_filter_bytes counter\n\
         fusiondb_sstable_open_filter_bytes {}\n\
         # HELP fusiondb_sstable_open_filter_read_us SSTable filter read time during open in microseconds\n\
         # TYPE fusiondb_sstable_open_filter_read_us counter\n\
         fusiondb_sstable_open_filter_read_us {}\n\
         # HELP fusiondb_sstable_open_filter_decode_us SSTable filter decode time during open in microseconds\n\
         # TYPE fusiondb_sstable_open_filter_decode_us counter\n\
         fusiondb_sstable_open_filter_decode_us {}\n\
         # HELP fusiondb_sstable_open_meta_bytes SSTable metadata bytes read during open\n\
         # TYPE fusiondb_sstable_open_meta_bytes counter\n\
         fusiondb_sstable_open_meta_bytes {}\n\
         # HELP fusiondb_sstable_open_meta_read_us SSTable metadata read time during open in microseconds\n\
         # TYPE fusiondb_sstable_open_meta_read_us counter\n\
         fusiondb_sstable_open_meta_read_us {}\n\
         # HELP fusiondb_sstable_open_meta_decode_us SSTable metadata decode time during open in microseconds\n\
         # TYPE fusiondb_sstable_open_meta_decode_us counter\n\
         fusiondb_sstable_open_meta_decode_us {}\n\
         # HELP fusiondb_sstable_open_index_entries SSTable index entries decoded during open\n\
         # TYPE fusiondb_sstable_open_index_entries counter\n\
         fusiondb_sstable_open_index_entries {}\n\
         # HELP fusiondb_sstable_open_block_property_count SSTable block property entries decoded during open\n\
         # TYPE fusiondb_sstable_open_block_property_count counter\n\
         fusiondb_sstable_open_block_property_count {}\n\
         # HELP fusiondb_sstable_index_cache_hit_count SSTable index sidecar cache hits during open\n\
         # TYPE fusiondb_sstable_index_cache_hit_count counter\n\
         fusiondb_sstable_index_cache_hit_count {}\n\
         # HELP fusiondb_sstable_index_cache_miss_count SSTable index sidecar cache misses during open\n\
         # TYPE fusiondb_sstable_index_cache_miss_count counter\n\
         fusiondb_sstable_index_cache_miss_count {}\n\
         # HELP fusiondb_sstable_index_cache_stale_count SSTable index sidecar cache stale entries during open\n\
         # TYPE fusiondb_sstable_index_cache_stale_count counter\n\
         fusiondb_sstable_index_cache_stale_count {}\n\
         # HELP fusiondb_sstable_index_cache_invalid_count SSTable index sidecar cache invalid entries during open\n\
         # TYPE fusiondb_sstable_index_cache_invalid_count counter\n\
         fusiondb_sstable_index_cache_invalid_count {}\n\
         # HELP fusiondb_sstable_index_cache_write_count SSTable index sidecar cache writes\n\
         # TYPE fusiondb_sstable_index_cache_write_count counter\n\
         fusiondb_sstable_index_cache_write_count {}\n\
         # HELP fusiondb_sstable_index_cache_write_error_count SSTable index sidecar cache write errors\n\
         # TYPE fusiondb_sstable_index_cache_write_error_count counter\n\
         fusiondb_sstable_index_cache_write_error_count {}\n\
         # HELP fusiondb_sstable_prefix_filter_check_count SSTable prefix Bloom filter probes\n\
         # TYPE fusiondb_sstable_prefix_filter_check_count counter\n\
         fusiondb_sstable_prefix_filter_check_count {}\n\
         # HELP fusiondb_sstable_prefix_filter_positive_count SSTable prefix Bloom filter positive probes\n\
         # TYPE fusiondb_sstable_prefix_filter_positive_count counter\n\
         fusiondb_sstable_prefix_filter_positive_count {}\n\
         # HELP fusiondb_sstable_prefix_filter_skip_count SSTable prefix Bloom filter negative skips\n\
         # TYPE fusiondb_sstable_prefix_filter_skip_count counter\n\
         fusiondb_sstable_prefix_filter_skip_count {}\n\
         # HELP fusiondb_sstable_prefix_filter_fail_open_count SSTable prefix Bloom filter fail-open probes\n\
         # TYPE fusiondb_sstable_prefix_filter_fail_open_count counter\n\
         fusiondb_sstable_prefix_filter_fail_open_count {}\n\
         # HELP fusiondb_sstable_index_prefix_filter_check_count SSTable SQL index-prefix Bloom filter probes\n\
         # TYPE fusiondb_sstable_index_prefix_filter_check_count counter\n\
         fusiondb_sstable_index_prefix_filter_check_count {}\n\
         # HELP fusiondb_sstable_index_prefix_filter_positive_count SSTable SQL index-prefix Bloom filter positive probes\n\
         # TYPE fusiondb_sstable_index_prefix_filter_positive_count counter\n\
         fusiondb_sstable_index_prefix_filter_positive_count {}\n\
         # HELP fusiondb_sstable_index_prefix_filter_skip_count SSTable SQL index-prefix Bloom filter negative skips\n\
         # TYPE fusiondb_sstable_index_prefix_filter_skip_count counter\n\
         fusiondb_sstable_index_prefix_filter_skip_count {}\n\
         # HELP fusiondb_sstable_index_prefix_filter_fail_open_count SSTable SQL index-prefix Bloom filter fail-open probes\n\
         # TYPE fusiondb_sstable_index_prefix_filter_fail_open_count counter\n\
         fusiondb_sstable_index_prefix_filter_fail_open_count {}\n\
         # HELP fusiondb_sstable_user_key_filter_check_count SSTable MVCC user-key Bloom filter probes\n\
         # TYPE fusiondb_sstable_user_key_filter_check_count counter\n\
         fusiondb_sstable_user_key_filter_check_count {}\n\
         # HELP fusiondb_sstable_user_key_filter_positive_count SSTable MVCC user-key Bloom filter positive probes\n\
         # TYPE fusiondb_sstable_user_key_filter_positive_count counter\n\
         fusiondb_sstable_user_key_filter_positive_count {}\n\
         # HELP fusiondb_sstable_user_key_filter_skip_count SSTable MVCC user-key Bloom filter negative skips\n\
         # TYPE fusiondb_sstable_user_key_filter_skip_count counter\n\
         fusiondb_sstable_user_key_filter_skip_count {}\n\
         # HELP fusiondb_sstable_user_key_filter_fail_open_count SSTable MVCC user-key Bloom filter fail-open probes\n\
         # TYPE fusiondb_sstable_user_key_filter_fail_open_count counter\n\
         fusiondb_sstable_user_key_filter_fail_open_count {}\n\
         # HELP fusiondb_sstable_block_prefix_filter_check_count SSTable block table-prefix property probes\n\
         # TYPE fusiondb_sstable_block_prefix_filter_check_count counter\n\
         fusiondb_sstable_block_prefix_filter_check_count {}\n\
         # HELP fusiondb_sstable_block_prefix_filter_positive_count SSTable block table-prefix property positive probes\n\
         # TYPE fusiondb_sstable_block_prefix_filter_positive_count counter\n\
         fusiondb_sstable_block_prefix_filter_positive_count {}\n\
         # HELP fusiondb_sstable_block_prefix_filter_skip_count SSTable block table-prefix property negative skips\n\
         # TYPE fusiondb_sstable_block_prefix_filter_skip_count counter\n\
         fusiondb_sstable_block_prefix_filter_skip_count {}\n\
         # HELP fusiondb_sstable_block_prefix_filter_fail_open_count SSTable block table-prefix property fail-open probes\n\
         # TYPE fusiondb_sstable_block_prefix_filter_fail_open_count counter\n\
         fusiondb_sstable_block_prefix_filter_fail_open_count {}\n\
         # HELP fusiondb_sstable_block_index_prefix_filter_check_count SSTable block SQL index-prefix property probes\n\
         # TYPE fusiondb_sstable_block_index_prefix_filter_check_count counter\n\
         fusiondb_sstable_block_index_prefix_filter_check_count {}\n\
         # HELP fusiondb_sstable_block_index_prefix_filter_positive_count SSTable block SQL index-prefix property positive probes\n\
         # TYPE fusiondb_sstable_block_index_prefix_filter_positive_count counter\n\
         fusiondb_sstable_block_index_prefix_filter_positive_count {}\n\
         # HELP fusiondb_sstable_block_index_prefix_filter_skip_count SSTable block SQL index-prefix property negative skips\n\
         # TYPE fusiondb_sstable_block_index_prefix_filter_skip_count counter\n\
         fusiondb_sstable_block_index_prefix_filter_skip_count {}\n\
         # HELP fusiondb_sstable_block_index_prefix_filter_fail_open_count SSTable block SQL index-prefix property fail-open probes\n\
         # TYPE fusiondb_sstable_block_index_prefix_filter_fail_open_count counter\n\
         fusiondb_sstable_block_index_prefix_filter_fail_open_count {}\n\
         # HELP fusiondb_sstable_block_zone_map_filter_check_count SSTable block SQL zone-map property probes\n\
         # TYPE fusiondb_sstable_block_zone_map_filter_check_count counter\n\
         fusiondb_sstable_block_zone_map_filter_check_count {}\n\
         # HELP fusiondb_sstable_block_zone_map_filter_positive_count SSTable block SQL zone-map property positive probes\n\
         # TYPE fusiondb_sstable_block_zone_map_filter_positive_count counter\n\
         fusiondb_sstable_block_zone_map_filter_positive_count {}\n\
         # HELP fusiondb_sstable_block_zone_map_filter_skip_count SSTable block SQL zone-map property negative skips\n\
         # TYPE fusiondb_sstable_block_zone_map_filter_skip_count counter\n\
         fusiondb_sstable_block_zone_map_filter_skip_count {}\n\
         # HELP fusiondb_sstable_block_zone_map_filter_fail_open_count SSTable block SQL zone-map property fail-open probes\n\
         # TYPE fusiondb_sstable_block_zone_map_filter_fail_open_count counter\n\
         fusiondb_sstable_block_zone_map_filter_fail_open_count {}\n\
         # HELP fusiondb_sstable_block_zone_map_metadata_bytes SSTable block SQL zone-map metadata bytes observed\n\
         # TYPE fusiondb_sstable_block_zone_map_metadata_bytes counter\n\
         fusiondb_sstable_block_zone_map_metadata_bytes {}\n\
         # HELP fusiondb_sstable_block_zone_map_mvcc_overlap_fail_open_count SSTable block SQL zone-map fail-opens caused by MVCC overlap risk\n\
         # TYPE fusiondb_sstable_block_zone_map_mvcc_overlap_fail_open_count counter\n\
         fusiondb_sstable_block_zone_map_mvcc_overlap_fail_open_count {}\n\
         # HELP fusiondb_sstable_block_zone_map_mvcc_boundary_split_fail_open_count SSTable block SQL zone-map MVCC fail-opens caused by same-user-key block boundary splits\n\
         # TYPE fusiondb_sstable_block_zone_map_mvcc_boundary_split_fail_open_count counter\n\
         fusiondb_sstable_block_zone_map_mvcc_boundary_split_fail_open_count {}\n\
         # HELP fusiondb_sstable_block_zone_map_mvcc_write_buffer_overlap_fail_open_count SSTable block SQL zone-map MVCC fail-opens caused by write-buffer overlap risk\n\
         # TYPE fusiondb_sstable_block_zone_map_mvcc_write_buffer_overlap_fail_open_count counter\n\
         fusiondb_sstable_block_zone_map_mvcc_write_buffer_overlap_fail_open_count {}\n\
         # HELP fusiondb_sstable_block_zone_map_mvcc_memtable_overlap_fail_open_count SSTable block SQL zone-map MVCC fail-opens caused by memtable overlap risk\n\
         # TYPE fusiondb_sstable_block_zone_map_mvcc_memtable_overlap_fail_open_count counter\n\
         fusiondb_sstable_block_zone_map_mvcc_memtable_overlap_fail_open_count {}\n\
         # HELP fusiondb_sstable_block_zone_map_mvcc_sstable_overlap_fail_open_count SSTable block SQL zone-map MVCC fail-opens caused by overlapping SSTables\n\
         # TYPE fusiondb_sstable_block_zone_map_mvcc_sstable_overlap_fail_open_count counter\n\
         fusiondb_sstable_block_zone_map_mvcc_sstable_overlap_fail_open_count {}\n\
         # HELP fusiondb_sstable_block_zone_map_schema_fail_open_count SSTable block SQL zone-map fail-opens caused by schema or type mismatch\n\
         # TYPE fusiondb_sstable_block_zone_map_schema_fail_open_count counter\n\
         fusiondb_sstable_block_zone_map_schema_fail_open_count {}\n\
         # HELP fusiondb_sstable_point_probe_count SSTable point-read probes\n\
         # TYPE fusiondb_sstable_point_probe_count counter\n\
         fusiondb_sstable_point_probe_count {}\n\
         # HELP fusiondb_sstable_point_overlap_skip_count SSTable point probes skipped by key-range overlap\n\
         # TYPE fusiondb_sstable_point_overlap_skip_count counter\n\
         fusiondb_sstable_point_overlap_skip_count {}\n\
         # HELP fusiondb_sstable_range_probe_count SSTable range-read probes\n\
         # TYPE fusiondb_sstable_range_probe_count counter\n\
         fusiondb_sstable_range_probe_count {}\n\
         # HELP fusiondb_sstable_range_overlap_skip_count SSTable range probes skipped by key-range overlap\n\
         # TYPE fusiondb_sstable_range_overlap_skip_count counter\n\
         fusiondb_sstable_range_overlap_skip_count {}\n\
         # HELP fusiondb_sstable_iterator_open_count SSTable iterators opened for reads\n\
         # TYPE fusiondb_sstable_iterator_open_count counter\n\
         fusiondb_sstable_iterator_open_count {}\n\
         # HELP fusiondb_columnar_single_source_aggregate_fast_path_count Columnar single-source aggregate fast-path executions\n\
         # TYPE fusiondb_columnar_single_source_aggregate_fast_path_count counter\n\
         fusiondb_columnar_single_source_aggregate_fast_path_count {}\n\
         # HELP fusiondb_sstable_reverse_iterator_open_count SSTable reverse iterators opened for reads\n\
         # TYPE fusiondb_sstable_reverse_iterator_open_count counter\n\
         fusiondb_sstable_reverse_iterator_open_count {}\n\
         # HELP fusiondb_sstable_reverse_block_read_count SSTable data blocks read by reverse iterators\n\
         # TYPE fusiondb_sstable_reverse_block_read_count counter\n\
         fusiondb_sstable_reverse_block_read_count {}\n\
         # HELP fusiondb_sstable_reverse_block_entry_decode_count SSTable entries decoded inside reverse iterator blocks\n\
         # TYPE fusiondb_sstable_reverse_block_entry_decode_count counter\n\
         fusiondb_sstable_reverse_block_entry_decode_count {}\n\
         # HELP fusiondb_sstable_reverse_block_entry_yield_count SSTable entries yielded by reverse iterator block bounds\n\
         # TYPE fusiondb_sstable_reverse_block_entry_yield_count counter\n\
         fusiondb_sstable_reverse_block_entry_yield_count {}\n\
         # HELP fusiondb_sstable_reverse_block_span_scan_count SSTable reverse iterator blocks parsed by runtime span scanning\n\
         # TYPE fusiondb_sstable_reverse_block_span_scan_count counter\n\
         fusiondb_sstable_reverse_block_span_scan_count {}\n\
         # HELP fusiondb_sstable_reverse_block_span_scan_entry_count SSTable reverse iterator entries parsed into runtime block spans\n\
         # TYPE fusiondb_sstable_reverse_block_span_scan_entry_count counter\n\
         fusiondb_sstable_reverse_block_span_scan_entry_count {}\n\
         # HELP fusiondb_sstable_reverse_block_span_materialize_entry_count SSTable reverse iterator entries materialized after runtime span scanning\n\
         # TYPE fusiondb_sstable_reverse_block_span_materialize_entry_count counter\n\
         fusiondb_sstable_reverse_block_span_materialize_entry_count {}\n\
         # HELP fusiondb_sstable_reverse_seek_sidecar_hit_count SSTable reverse seek sidecar cache hits\n\
         # TYPE fusiondb_sstable_reverse_seek_sidecar_hit_count counter\n\
         fusiondb_sstable_reverse_seek_sidecar_hit_count {}\n\
         # HELP fusiondb_sstable_reverse_seek_sidecar_miss_count SSTable reverse seek sidecar cache misses\n\
         # TYPE fusiondb_sstable_reverse_seek_sidecar_miss_count counter\n\
         fusiondb_sstable_reverse_seek_sidecar_miss_count {}\n\
         # HELP fusiondb_sstable_reverse_seek_sidecar_stale_count SSTable reverse seek sidecar stale files\n\
         # TYPE fusiondb_sstable_reverse_seek_sidecar_stale_count counter\n\
         fusiondb_sstable_reverse_seek_sidecar_stale_count {}\n\
         # HELP fusiondb_sstable_reverse_seek_sidecar_invalid_count SSTable reverse seek sidecar invalid files\n\
         # TYPE fusiondb_sstable_reverse_seek_sidecar_invalid_count counter\n\
         fusiondb_sstable_reverse_seek_sidecar_invalid_count {}\n\
         # HELP fusiondb_sstable_reverse_seek_sidecar_write_count SSTable reverse seek sidecar writes\n\
         # TYPE fusiondb_sstable_reverse_seek_sidecar_write_count counter\n\
         fusiondb_sstable_reverse_seek_sidecar_write_count {}\n\
         # HELP fusiondb_sstable_reverse_seek_sidecar_write_error_count SSTable reverse seek sidecar write errors\n\
         # TYPE fusiondb_sstable_reverse_seek_sidecar_write_error_count counter\n\
         fusiondb_sstable_reverse_seek_sidecar_write_error_count {}\n\
         # HELP fusiondb_sstable_reverse_seek_sidecar_use_count SSTable reverse iterator blocks served by reverse seek sidecars\n\
         # TYPE fusiondb_sstable_reverse_seek_sidecar_use_count counter\n\
         fusiondb_sstable_reverse_seek_sidecar_use_count {}\n\
         # HELP fusiondb_sstable_reverse_seek_sidecar_fail_open_count SSTable reverse seek sidecar block-level fail-open fallbacks\n\
         # TYPE fusiondb_sstable_reverse_seek_sidecar_fail_open_count counter\n\
         fusiondb_sstable_reverse_seek_sidecar_fail_open_count {}\n\
         # HELP fusiondb_sstable_reverse_seek_sidecar_index_entry_count SSTable reverse seek sidecar indexed entries covered by successful block uses\n\
         # TYPE fusiondb_sstable_reverse_seek_sidecar_index_entry_count counter\n\
         fusiondb_sstable_reverse_seek_sidecar_index_entry_count {}\n\
         # HELP fusiondb_sstable_reverse_seek_sidecar_entry_materialize_count SSTable reverse seek sidecar entries materialized by successful block uses\n\
         # TYPE fusiondb_sstable_reverse_seek_sidecar_entry_materialize_count counter\n\
         fusiondb_sstable_reverse_seek_sidecar_entry_materialize_count {}\n\
         # HELP fusiondb_sstable_reverse_seek_sidecar_offset_probe_count SSTable reverse seek sidecar entry-offset probes used for block bounds\n\
         # TYPE fusiondb_sstable_reverse_seek_sidecar_offset_probe_count counter\n\
         fusiondb_sstable_reverse_seek_sidecar_offset_probe_count {}\n\
         # HELP fusiondb_fusion_reverse_scan_count Fusion visible reverse range scans\n\
         # TYPE fusiondb_fusion_reverse_scan_count counter\n\
         fusiondb_fusion_reverse_scan_count {}\n\
         # HELP fusiondb_fusion_reverse_source_open_count Sources opened by Fusion reverse range merges\n\
         # TYPE fusiondb_fusion_reverse_source_open_count counter\n\
         fusiondb_fusion_reverse_source_open_count {}\n\
         # HELP fusiondb_fusion_reverse_sstable_frontier_probe_count SSTables probed for Fusion reverse range-local frontier after overlap and Bloom filters\n\
         # TYPE fusiondb_fusion_reverse_sstable_frontier_probe_count counter\n\
         fusiondb_fusion_reverse_sstable_frontier_probe_count {}\n\
         # HELP fusiondb_fusion_reverse_sstable_frontier_in_range_count Fusion reverse SSTable frontiers derived from aligned in-range block properties\n\
         # TYPE fusiondb_fusion_reverse_sstable_frontier_in_range_count counter\n\
         fusiondb_fusion_reverse_sstable_frontier_in_range_count {}\n\
         # HELP fusiondb_fusion_reverse_sstable_frontier_file_count Fusion reverse SSTable frontiers using file-level fail-open fallback\n\
         # TYPE fusiondb_fusion_reverse_sstable_frontier_file_count counter\n\
         fusiondb_fusion_reverse_sstable_frontier_file_count {}\n\
         # HELP fusiondb_fusion_reverse_sstable_frontier_tighten_count Fusion reverse SSTable frontiers lower than the file-level max user key\n\
         # TYPE fusiondb_fusion_reverse_sstable_frontier_tighten_count counter\n\
         fusiondb_fusion_reverse_sstable_frontier_tighten_count {}\n\
         # HELP fusiondb_fusion_reverse_sstable_frontier_empty_skip_count Fusion reverse SSTables skipped because range-local frontier proved no in-range key\n\
         # TYPE fusiondb_fusion_reverse_sstable_frontier_empty_skip_count counter\n\
         fusiondb_fusion_reverse_sstable_frontier_empty_skip_count {}\n\
         # HELP fusiondb_fusion_reverse_sstable_frontier_fail_open_count Fusion reverse SSTable frontier probes that failed open to file-level frontier\n\
         # TYPE fusiondb_fusion_reverse_sstable_frontier_fail_open_count counter\n\
         fusiondb_fusion_reverse_sstable_frontier_fail_open_count {}\n\
         # HELP fusiondb_fusion_reverse_sstable_pending_count SSTables queued in Fusion reverse lazy pending heap\n\
         # TYPE fusiondb_fusion_reverse_sstable_pending_count counter\n\
         fusiondb_fusion_reverse_sstable_pending_count {}\n\
         # HELP fusiondb_fusion_reverse_sstable_activation_count SSTables activated from Fusion reverse lazy pending heap\n\
         # TYPE fusiondb_fusion_reverse_sstable_activation_count counter\n\
         fusiondb_fusion_reverse_sstable_activation_count {}\n\
         # HELP fusiondb_fusion_reverse_sstable_deferred_unopened_count SSTables left unopened in Fusion reverse pending heap when the scan stopped\n\
         # TYPE fusiondb_fusion_reverse_sstable_deferred_unopened_count counter\n\
         fusiondb_fusion_reverse_sstable_deferred_unopened_count {}\n\
         # HELP fusiondb_fusion_reverse_sstable_activation_equal_frontier_count SSTable activations where pending frontier equaled active top user key\n\
         # TYPE fusiondb_fusion_reverse_sstable_activation_equal_frontier_count counter\n\
         fusiondb_fusion_reverse_sstable_activation_equal_frontier_count {}\n\
         # HELP fusiondb_fusion_reverse_raw_entry_read_count Raw internal entries read by Fusion reverse merges\n\
         # TYPE fusiondb_fusion_reverse_raw_entry_read_count counter\n\
         fusiondb_fusion_reverse_raw_entry_read_count {}\n\
         # HELP fusiondb_fusion_reverse_visible_candidate_count Visible per-user-key candidates produced by Fusion reverse sources\n\
         # TYPE fusiondb_fusion_reverse_visible_candidate_count counter\n\
         fusiondb_fusion_reverse_visible_candidate_count {}\n\
         # HELP fusiondb_fusion_reverse_visible_put_count Visible PUT rows emitted by Fusion reverse merges\n\
         # TYPE fusiondb_fusion_reverse_visible_put_count counter\n\
         fusiondb_fusion_reverse_visible_put_count {}\n\
         # HELP fusiondb_index_key_stream_entry_visit_count Secondary index entries visited by tight SQL key-stream scans\n\
         # TYPE fusiondb_index_key_stream_entry_visit_count counter\n\
         fusiondb_index_key_stream_entry_visit_count {}\n\
         # HELP fusiondb_index_ordered_topk_scan_count SQL ordered index Top-K scans\n\
         # TYPE fusiondb_index_ordered_topk_scan_count counter\n\
         fusiondb_index_ordered_topk_scan_count {}\n\
         # HELP fusiondb_index_ordered_topk_entry_visit_count Index entries visited by SQL ordered Top-K scans\n\
         # TYPE fusiondb_index_ordered_topk_entry_visit_count counter\n\
         fusiondb_index_ordered_topk_entry_visit_count {}\n\
         # HELP fusiondb_index_ordered_topk_reverse_scan_count SQL ordered index Top-K scans using reverse range scan\n\
         # TYPE fusiondb_index_ordered_topk_reverse_scan_count counter\n\
         fusiondb_index_ordered_topk_reverse_scan_count {}\n\
         # HELP fusiondb_index_ordered_topk_index_only_row_count Rows materialized directly from SQL ordered index Top-K scans without base-row lookup\n\
         # TYPE fusiondb_index_ordered_topk_index_only_row_count counter\n\
         fusiondb_index_ordered_topk_index_only_row_count {}\n\
         # HELP fusiondb_index_ordered_topk_base_row_fetch_count Base rows looked up by SQL ordered index Top-K scans\n\
         # TYPE fusiondb_index_ordered_topk_base_row_fetch_count counter\n\
         fusiondb_index_ordered_topk_base_row_fetch_count {}\n\
         # HELP fusiondb_index_group_count_summary_entry_visit_count Secondary index count-summary entries visited by GROUP BY COUNT scans\n\
         # TYPE fusiondb_index_group_count_summary_entry_visit_count counter\n\
         fusiondb_index_group_count_summary_entry_visit_count {}\n\
         # HELP fusiondb_index_loose_seek_count Secondary index first() seeks issued by SQL loose scans\n\
         # TYPE fusiondb_index_loose_seek_count counter\n\
         fusiondb_index_loose_seek_count {}\n\
         # HELP fusiondb_index_loose_value_count Distinct secondary index value groups emitted by SQL loose scans\n\
         # TYPE fusiondb_index_loose_value_count counter\n\
         fusiondb_index_loose_value_count {}\n\
         # HELP fusiondb_index_loose_run_skip_count Secondary index advances to the next value run by SQL loose scans\n\
         # TYPE fusiondb_index_loose_run_skip_count counter\n\
         fusiondb_index_loose_run_skip_count {}\n\
         # HELP fusiondb_compaction_run_count Completed SSTable compaction runs\n\
         # TYPE fusiondb_compaction_run_count counter\n\
         fusiondb_compaction_run_count {}\n\
         # HELP fusiondb_compaction_input_bytes SSTable bytes read as compaction input\n\
         # TYPE fusiondb_compaction_input_bytes counter\n\
         fusiondb_compaction_input_bytes {}\n\
         # HELP fusiondb_compaction_output_bytes SSTable bytes written as compaction output\n\
         # TYPE fusiondb_compaction_output_bytes counter\n\
         fusiondb_compaction_output_bytes {}\n\
         # HELP fusiondb_compaction_dropped_version_count MVCC versions dropped during compaction\n\
         # TYPE fusiondb_compaction_dropped_version_count counter\n\
         fusiondb_compaction_dropped_version_count {}\n\
         # HELP fusiondb_live_sstable_count Live SSTable files currently registered\n\
         # TYPE fusiondb_live_sstable_count gauge\n\
         fusiondb_live_sstable_count {}\n\
         # HELP fusiondb_sstable_manifest_load_count SSTable manifest load attempts during startup\n\
         # TYPE fusiondb_sstable_manifest_load_count counter\n\
         fusiondb_sstable_manifest_load_count {}\n\
         # HELP fusiondb_sstable_manifest_load_total_us Total SSTable manifest load time in microseconds\n\
         # TYPE fusiondb_sstable_manifest_load_total_us counter\n\
         fusiondb_sstable_manifest_load_total_us {}\n\
         # HELP fusiondb_sstable_manifest_load_error_count SSTable manifest load validation failures\n\
         # TYPE fusiondb_sstable_manifest_load_error_count counter\n\
         fusiondb_sstable_manifest_load_error_count {}\n\
         # HELP fusiondb_sstable_manifest_live_file_count SSTable files listed by the startup manifest\n\
         # TYPE fusiondb_sstable_manifest_live_file_count gauge\n\
         fusiondb_sstable_manifest_live_file_count {}\n\
         # HELP fusiondb_sstable_manifest_legacy_scan_count Legacy SSTable directory scans during startup\n\
         # TYPE fusiondb_sstable_manifest_legacy_scan_count counter\n\
         fusiondb_sstable_manifest_legacy_scan_count {}\n\
         # HELP fusiondb_sstable_manifest_legacy_scan_candidate_count SSTable candidates found by legacy startup scans\n\
         # TYPE fusiondb_sstable_manifest_legacy_scan_candidate_count counter\n\
         fusiondb_sstable_manifest_legacy_scan_candidate_count {}\n\
         # HELP fusiondb_sstable_manifest_open_error_count SSTable open failures for startup manifest or legacy scan candidates\n\
         # TYPE fusiondb_sstable_manifest_open_error_count counter\n\
         fusiondb_sstable_manifest_open_error_count {}\n\
         # HELP fusiondb_wal_write_count WAL syncs\n\
         # TYPE fusiondb_wal_write_count counter\n\
         fusiondb_wal_write_count {}\n\
         # HELP fusiondb_wal_write_bytes WAL bytes written\n\
         # TYPE fusiondb_wal_write_bytes counter\n\
         fusiondb_wal_write_bytes {}\n\
         # HELP fusiondb_wal_replay_count WAL replay attempts during startup\n\
         # TYPE fusiondb_wal_replay_count counter\n\
         fusiondb_wal_replay_count {}\n\
         # HELP fusiondb_wal_replay_total_us Total WAL replay read time in microseconds\n\
         # TYPE fusiondb_wal_replay_total_us counter\n\
         fusiondb_wal_replay_total_us {}\n\
         # HELP fusiondb_wal_replay_segment_count WAL segments observed during replay\n\
         # TYPE fusiondb_wal_replay_segment_count counter\n\
         fusiondb_wal_replay_segment_count {}\n\
         # HELP fusiondb_wal_replay_bytes WAL bytes observed during replay\n\
         # TYPE fusiondb_wal_replay_bytes counter\n\
         fusiondb_wal_replay_bytes {}\n\
         # HELP fusiondb_wal_replay_valid_bytes WAL bytes through the last complete replayed record\n\
         # TYPE fusiondb_wal_replay_valid_bytes counter\n\
         fusiondb_wal_replay_valid_bytes {}\n\
         # HELP fusiondb_wal_replay_last_segment_id Last WAL segment ID observed during replay\n\
         # TYPE fusiondb_wal_replay_last_segment_id gauge\n\
         fusiondb_wal_replay_last_segment_id {}\n\
         # HELP fusiondb_wal_replay_last_valid_offset Last valid WAL replay offset in the last observed segment\n\
         # TYPE fusiondb_wal_replay_last_valid_offset gauge\n\
         fusiondb_wal_replay_last_valid_offset {}\n\
         # HELP fusiondb_wal_replay_entry_count WAL entries decoded during replay\n\
         # TYPE fusiondb_wal_replay_entry_count counter\n\
         fusiondb_wal_replay_entry_count {}\n\
         # HELP fusiondb_wal_replay_put_count WAL put entries decoded during replay\n\
         # TYPE fusiondb_wal_replay_put_count counter\n\
         fusiondb_wal_replay_put_count {}\n\
         # HELP fusiondb_wal_replay_delete_count WAL delete entries decoded during replay\n\
         # TYPE fusiondb_wal_replay_delete_count counter\n\
         fusiondb_wal_replay_delete_count {}\n\
         # HELP fusiondb_wal_replay_partial_tail_count WAL partial tails found during replay\n\
         # TYPE fusiondb_wal_replay_partial_tail_count counter\n\
         fusiondb_wal_replay_partial_tail_count {}\n\
         # HELP fusiondb_wal_replay_truncate_count WAL files truncated after partial-tail replay\n\
         # TYPE fusiondb_wal_replay_truncate_count counter\n\
         fusiondb_wal_replay_truncate_count {}\n\
         # HELP fusiondb_wal_replay_error_count WAL replay errors\n\
         # TYPE fusiondb_wal_replay_error_count counter\n\
         fusiondb_wal_replay_error_count {}\n\
         # HELP fusiondb_wal_replay_apply_count WAL replay apply passes during startup\n\
         # TYPE fusiondb_wal_replay_apply_count counter\n\
         fusiondb_wal_replay_apply_count {}\n\
         # HELP fusiondb_wal_replay_apply_total_us Total WAL replay apply time in microseconds\n\
         # TYPE fusiondb_wal_replay_apply_total_us counter\n\
         fusiondb_wal_replay_apply_total_us {}\n\
         # HELP fusiondb_wal_replay_max_ts Last startup WAL replay max MVCC timestamp\n\
         # TYPE fusiondb_wal_replay_max_ts gauge\n\
         fusiondb_wal_replay_max_ts {}\n\
         # HELP fusiondb_query_sort_fallback_count SQL ORDER BY operations that sorted rows after scan\n\
         # TYPE fusiondb_query_sort_fallback_count counter\n\
         fusiondb_query_sort_fallback_count {}\n\
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
        m.query_result_cache_eligible_count.load(Relaxed),
        m.query_result_cache_hit_count.load(Relaxed),
        m.query_result_cache_miss_count.load(Relaxed),
        m.query_result_cache_stale_count.load(Relaxed),
        m.query_result_cache_insert_count.load(Relaxed),
        m.query_result_cache_invalidation_count.load(Relaxed),
        m.block_cache_hit_count.load(Relaxed),
        m.block_cache_miss_count.load(Relaxed),
        m.block_cache_insert_count.load(Relaxed),
        m.block_cache_insert_bytes.load(Relaxed),
        m.block_cache_fill_skip_count.load(Relaxed),
        m.block_cache_eviction_count.load(Relaxed),
        m.block_cache_eviction_bytes.load(Relaxed),
        m.sstable_block_file_open_count.load(Relaxed),
        m.sstable_block_read_bytes.load(Relaxed),
        m.sstable_open_count.load(Relaxed),
        m.sstable_open_total_us.load(Relaxed),
        m.sstable_open_index_bytes.load(Relaxed),
        m.sstable_open_index_read_us.load(Relaxed),
        m.sstable_open_index_decode_us.load(Relaxed),
        m.sstable_open_filter_bytes.load(Relaxed),
        m.sstable_open_filter_read_us.load(Relaxed),
        m.sstable_open_filter_decode_us.load(Relaxed),
        m.sstable_open_meta_bytes.load(Relaxed),
        m.sstable_open_meta_read_us.load(Relaxed),
        m.sstable_open_meta_decode_us.load(Relaxed),
        m.sstable_open_index_entries.load(Relaxed),
        m.sstable_open_block_property_count.load(Relaxed),
        m.sstable_index_cache_hit_count.load(Relaxed),
        m.sstable_index_cache_miss_count.load(Relaxed),
        m.sstable_index_cache_stale_count.load(Relaxed),
        m.sstable_index_cache_invalid_count.load(Relaxed),
        m.sstable_index_cache_write_count.load(Relaxed),
        m.sstable_index_cache_write_error_count.load(Relaxed),
        m.sstable_prefix_filter_check_count.load(Relaxed),
        m.sstable_prefix_filter_positive_count.load(Relaxed),
        m.sstable_prefix_filter_skip_count.load(Relaxed),
        m.sstable_prefix_filter_fail_open_count.load(Relaxed),
        m.sstable_index_prefix_filter_check_count.load(Relaxed),
        m.sstable_index_prefix_filter_positive_count.load(Relaxed),
        m.sstable_index_prefix_filter_skip_count.load(Relaxed),
        m.sstable_index_prefix_filter_fail_open_count
            .load(Relaxed),
        m.sstable_user_key_filter_check_count.load(Relaxed),
        m.sstable_user_key_filter_positive_count.load(Relaxed),
        m.sstable_user_key_filter_skip_count.load(Relaxed),
        m.sstable_user_key_filter_fail_open_count.load(Relaxed),
        m.sstable_block_prefix_filter_check_count.load(Relaxed),
        m.sstable_block_prefix_filter_positive_count.load(Relaxed),
        m.sstable_block_prefix_filter_skip_count.load(Relaxed),
        m.sstable_block_prefix_filter_fail_open_count
            .load(Relaxed),
        m.sstable_block_index_prefix_filter_check_count
            .load(Relaxed),
        m.sstable_block_index_prefix_filter_positive_count
            .load(Relaxed),
        m.sstable_block_index_prefix_filter_skip_count
            .load(Relaxed),
        m.sstable_block_index_prefix_filter_fail_open_count
            .load(Relaxed),
        m.sstable_block_zone_map_filter_check_count
            .load(Relaxed),
        m.sstable_block_zone_map_filter_positive_count
            .load(Relaxed),
        m.sstable_block_zone_map_filter_skip_count
            .load(Relaxed),
        m.sstable_block_zone_map_filter_fail_open_count
            .load(Relaxed),
        m.sstable_block_zone_map_metadata_bytes.load(Relaxed),
        m.sstable_block_zone_map_mvcc_overlap_fail_open_count
            .load(Relaxed),
        m.sstable_block_zone_map_mvcc_boundary_split_fail_open_count
            .load(Relaxed),
        m.sstable_block_zone_map_mvcc_write_buffer_overlap_fail_open_count
            .load(Relaxed),
        m.sstable_block_zone_map_mvcc_memtable_overlap_fail_open_count
            .load(Relaxed),
        m.sstable_block_zone_map_mvcc_sstable_overlap_fail_open_count
            .load(Relaxed),
        m.sstable_block_zone_map_schema_fail_open_count
            .load(Relaxed),
        m.sstable_point_probe_count.load(Relaxed),
        m.sstable_point_overlap_skip_count.load(Relaxed),
        m.sstable_range_probe_count.load(Relaxed),
        m.sstable_range_overlap_skip_count.load(Relaxed),
        m.sstable_iterator_open_count.load(Relaxed),
        m.columnar_single_source_aggregate_fast_path_count
            .load(Relaxed),
        m.sstable_reverse_iterator_open_count.load(Relaxed),
        m.sstable_reverse_block_read_count.load(Relaxed),
        m.sstable_reverse_block_entry_decode_count.load(Relaxed),
        m.sstable_reverse_block_entry_yield_count.load(Relaxed),
        m.sstable_reverse_block_span_scan_count.load(Relaxed),
        m.sstable_reverse_block_span_scan_entry_count
            .load(Relaxed),
        m.sstable_reverse_block_span_materialize_entry_count
            .load(Relaxed),
        m.sstable_reverse_seek_sidecar_hit_count.load(Relaxed),
        m.sstable_reverse_seek_sidecar_miss_count.load(Relaxed),
        m.sstable_reverse_seek_sidecar_stale_count.load(Relaxed),
        m.sstable_reverse_seek_sidecar_invalid_count.load(Relaxed),
        m.sstable_reverse_seek_sidecar_write_count.load(Relaxed),
        m.sstable_reverse_seek_sidecar_write_error_count.load(Relaxed),
        m.sstable_reverse_seek_sidecar_use_count.load(Relaxed),
        m.sstable_reverse_seek_sidecar_fail_open_count.load(Relaxed),
        m.sstable_reverse_seek_sidecar_index_entry_count
            .load(Relaxed),
        m.sstable_reverse_seek_sidecar_entry_materialize_count
            .load(Relaxed),
        m.sstable_reverse_seek_sidecar_offset_probe_count
            .load(Relaxed),
        m.fusion_reverse_scan_count.load(Relaxed),
        m.fusion_reverse_source_open_count.load(Relaxed),
        m.fusion_reverse_sstable_frontier_probe_count
            .load(Relaxed),
        m.fusion_reverse_sstable_frontier_in_range_count
            .load(Relaxed),
        m.fusion_reverse_sstable_frontier_file_count
            .load(Relaxed),
        m.fusion_reverse_sstable_frontier_tighten_count
            .load(Relaxed),
        m.fusion_reverse_sstable_frontier_empty_skip_count
            .load(Relaxed),
        m.fusion_reverse_sstable_frontier_fail_open_count
            .load(Relaxed),
        m.fusion_reverse_sstable_pending_count.load(Relaxed),
        m.fusion_reverse_sstable_activation_count.load(Relaxed),
        m.fusion_reverse_sstable_deferred_unopened_count
            .load(Relaxed),
        m.fusion_reverse_sstable_activation_equal_frontier_count
            .load(Relaxed),
        m.fusion_reverse_raw_entry_read_count.load(Relaxed),
        m.fusion_reverse_visible_candidate_count.load(Relaxed),
        m.fusion_reverse_visible_put_count.load(Relaxed),
        m.index_key_stream_entry_visit_count.load(Relaxed),
        m.index_ordered_topk_scan_count.load(Relaxed),
        m.index_ordered_topk_entry_visit_count.load(Relaxed),
        m.index_ordered_topk_reverse_scan_count.load(Relaxed),
        m.index_ordered_topk_index_only_row_count.load(Relaxed),
        m.index_ordered_topk_base_row_fetch_count.load(Relaxed),
        m.index_group_count_summary_entry_visit_count
            .load(Relaxed),
        m.index_loose_seek_count.load(Relaxed),
        m.index_loose_value_count.load(Relaxed),
        m.index_loose_run_skip_count.load(Relaxed),
        m.compaction_run_count.load(Relaxed),
        m.compaction_input_bytes.load(Relaxed),
        m.compaction_output_bytes.load(Relaxed),
        m.compaction_dropped_version_count.load(Relaxed),
        m.live_sstable_count.load(Relaxed),
        m.sstable_manifest_load_count.load(Relaxed),
        m.sstable_manifest_load_total_us.load(Relaxed),
        m.sstable_manifest_load_error_count.load(Relaxed),
        m.sstable_manifest_live_file_count.load(Relaxed),
        m.sstable_manifest_legacy_scan_count.load(Relaxed),
        m.sstable_manifest_legacy_scan_candidate_count
            .load(Relaxed),
        m.sstable_manifest_open_error_count.load(Relaxed),
        m.wal_write_count.load(Relaxed),
        m.wal_write_bytes.load(Relaxed),
        m.wal_replay_count.load(Relaxed),
        m.wal_replay_total_us.load(Relaxed),
        m.wal_replay_segment_count.load(Relaxed),
        m.wal_replay_bytes.load(Relaxed),
        m.wal_replay_valid_bytes.load(Relaxed),
        m.wal_replay_last_segment_id.load(Relaxed),
        m.wal_replay_last_valid_offset.load(Relaxed),
        m.wal_replay_entry_count.load(Relaxed),
        m.wal_replay_put_count.load(Relaxed),
        m.wal_replay_delete_count.load(Relaxed),
        m.wal_replay_partial_tail_count.load(Relaxed),
        m.wal_replay_truncate_count.load(Relaxed),
        m.wal_replay_error_count.load(Relaxed),
        m.wal_replay_apply_count.load(Relaxed),
        m.wal_replay_apply_total_us.load(Relaxed),
        m.wal_replay_max_ts.load(Relaxed),
        m.query_sort_fallback_count.load(Relaxed),
        m.pg_active_connection_count.load(Relaxed),
        m.pg_connection_limit.load(Relaxed),
        m.pg_connection_rejected_count.load(Relaxed),
    )
}

async fn handle_vector_search(
    State(state): State<AppState>,
    Extension(context): Extension<RequestContext>,
    Json(payload): Json<VectorSearchRequest>,
) -> ApiResponse<VectorSearchResponse> {
    match authorize_direct_search(&state, &context, payload.limit, 1).await {
        Ok(()) => {}
        Err(response) => return response,
    }

    if let Some(fusion) = state.storage.as_any().downcast_ref::<FusionStorage>() {
        let results = fusion.vector_search(&payload.query, payload.limit).await;
        json_ok(VectorSearchResponse {
            results: results
                .into_iter()
                .map(|(id, dist)| VectorSearchResult { id, distance: dist })
                .collect(),
        })
    } else {
        json_error(
            StatusCode::NOT_IMPLEMENTED,
            "Direct vector search is not available on this backend",
        )
    }
}

async fn handle_hybrid_search(
    State(state): State<AppState>,
    Extension(context): Extension<RequestContext>,
    Json(payload): Json<HybridSearchRequest>,
) -> ApiResponse<VectorSearchResponse> {
    match authorize_direct_search(&state, &context, payload.limit, 2).await {
        Ok(()) => {}
        Err(response) => return response,
    }

    if let Some(fusion) = state.storage.as_any().downcast_ref::<FusionStorage>() {
        let results = fusion
            .hybrid_search(&payload.text_query, &payload.vector_query, payload.limit)
            .await;
        json_ok(VectorSearchResponse {
            // Reusing VectorSearchResult but distance field is now RRF score
            results: results
                .into_iter()
                .map(|(id, score)| VectorSearchResult {
                    id,
                    distance: score,
                })
                .collect(),
        })
    } else {
        json_error(
            StatusCode::NOT_IMPLEMENTED,
            "Direct hybrid search is not available on this backend",
        )
    }
}

async fn authorize_direct_search(
    state: &AppState,
    context: &RequestContext,
    limit: usize,
    candidate_multiplier: usize,
) -> std::result::Result<(), ApiResponse<VectorSearchResponse>> {
    if limit == 0 || limit > MAX_DIRECT_SEARCH_LIMIT {
        return Err(json_error(
            StatusCode::BAD_REQUEST,
            format!("Search limit must be between 1 and {MAX_DIRECT_SEARCH_LIMIT}"),
        ));
    }
    if limit.checked_mul(candidate_multiplier).is_none() {
        return Err(json_error(
            StatusCode::BAD_REQUEST,
            "Search candidate limit overflow",
        ));
    }

    let username = context.username.as_deref().unwrap_or_default();
    // The current default vector and BM25 indexes are global resources: IDs
    // are not namespaced by SQL table/column. A caller cannot be authorized
    // safely with table-level grants until those index keys are scoped, so
    // direct search remains superuser-only.
    if let Err(error) = state.executor.require_superuser(username).await {
        return Err(json_error(
            StatusCode::FORBIDDEN,
            format!("Authorization Error: {error:?}"),
        ));
    }

    Ok(())
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
    let (sql, disable_sql_block_zone_map_pruning) =
        strip_sql_block_zone_map_prune_hint(&payload.sql);

    if let Err(e) = state.executor.authorize_sql(&username, sql).await {
        return json_error(StatusCode::FORBIDDEN, format!("{:?}", e));
    }

    match shard_write_route_action_for_sql(&state, sql, &[], context.shard_forwarded).await {
        Ok(ShardWriteRouteAction::Local) => {}
        Ok(ShardWriteRouteAction::Forward(decision)) => {
            return forward_query_to_shard_owner(&state, &context, &payload, &decision).await;
        }
        Ok(ShardWriteRouteAction::Conflict(message)) => {
            return json_error(StatusCode::CONFLICT, message);
        }
        Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
    }

    match shard_read_route_action_for_sql(&state, sql, &[], context.shard_forwarded).await {
        Ok(ShardReadRouteAction::Local) => {}
        Ok(ShardReadRouteAction::Forward(decision)) => {
            return forward_query_to_shard_owner(&state, &context, &payload, &decision).await;
        }
        Ok(ShardReadRouteAction::Conflict(message)) => {
            return json_error(StatusCode::CONFLICT, message);
        }
        Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
    }

    if let Some(response) = try_fanout_count_query_to_shard_owners(&state, &context, sql).await {
        return response;
    }

    if let Some(response) =
        try_fanout_group_count_query_to_shard_owners(&state, &context, sql).await
    {
        return response;
    }

    if let Some(response) =
        try_fanout_group_aggregate_query_to_shard_owners(&state, &context, sql).await
    {
        return response;
    }

    if let Some(response) = try_fanout_group_avg_query_to_shard_owners(&state, &context, sql).await
    {
        return response;
    }

    if let Some(response) =
        try_fanout_group_multi_aggregate_query_to_shard_owners(&state, &context, sql).await
    {
        return response;
    }

    if let Some(response) = try_unsupported_group_by_fanout_error(&state, &context, sql).await {
        return response;
    }

    if let Some(response) =
        try_fanout_count_distinct_query_to_shard_owners(&state, &context, sql).await
    {
        return response;
    }

    if let Some(response) =
        try_fanout_sum_distinct_query_to_shard_owners(&state, &context, sql).await
    {
        return response;
    }

    if let Some(response) =
        try_fanout_avg_distinct_query_to_shard_owners(&state, &context, sql).await
    {
        return response;
    }

    if let Some(response) = try_fanout_sum_query_to_shard_owners(&state, &context, sql).await {
        return response;
    }

    if let Some(response) = try_fanout_min_max_query_to_shard_owners(&state, &context, sql).await {
        return response;
    }

    if let Some(response) = try_fanout_avg_query_to_shard_owners(&state, &context, sql).await {
        return response;
    }

    if let Some(response) = try_fanout_query_to_shard_owners(&state, &context, sql).await {
        return response;
    }

    if let Some(raft) = &state.raft {
        match state.executor.sql_requires_raft_write(sql) {
            Ok(true) => {
                return match submit_raft_write(
                    raft,
                    &state.executor,
                    &state.raft_client,
                    sql.to_string(),
                    state.forwarding_auth.as_ref(),
                    &state.peer_scheme,
                )
                .await
                {
                    Ok(resp) if !resp.results.is_empty() => json_ok(
                        resp.results
                            .into_iter()
                            .map(QueryResultJson::from)
                            .collect(),
                    ),
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

    let execution_result = if disable_sql_block_zone_map_pruning {
        state
            .executor
            .execute_sql_with_sql_block_zone_map_pruning(sql, false)
            .await
    } else {
        state.executor.execute_sql(sql).await
    };

    match execution_result {
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
    if state.raft.is_some() {
        return json_error(
            StatusCode::NOT_IMPLEMENTED,
            "COPY FROM STDIN is disabled in distributed Raft mode until its decoded mutation batch can be replicated",
        );
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
    let url = format!(
        "{}://{}/query",
        state.peer_scheme, decision.route.owner_addr
    );
    let request = apply_forwarding_headers(state.raft_client.post(&url).json(payload), context);

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

async fn try_fanout_count_query_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    sql: &str,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled {
        return None;
    }
    let owners = match state
        .executor
        .shard_count_select_fanout_owners_for_sql(sql, &[])
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard count fan-out planning error: {:?}", e),
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

    let (columns, mut total) = match fanout_count_from_select_results(local_results) {
        Ok(count) => count,
        Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
    };

    for owner in owners {
        let owner_results = match query_remote_shard_owner(state, context, sql, &owner).await {
            Ok(results) => results,
            Err((status, message)) => return Some(json_error(status, message)),
        };
        let (owner_columns, owner_count) = match fanout_count_from_select_results(owner_results) {
            Ok(count) => count,
            Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
        };
        if owner_columns != columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard count fan-out column mismatch: expected {:?}, got {:?}",
                    columns, owner_columns
                ),
            ));
        }
        total = match total.checked_add(owner_count) {
            Some(total) => total,
            None => {
                return Some(json_error(
                    StatusCode::BAD_GATEWAY,
                    "Shard count fan-out overflow",
                ));
            }
        };
    }

    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns,
        rows: vec![vec![serde_json::json!(total)]],
    }]))
}

async fn try_fanout_group_count_query_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    sql: &str,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled {
        return None;
    }
    let plan = match state
        .executor
        .shard_group_count_select_fanout_plan_for_sql(sql)
    {
        Ok(Some(plan)) => plan,
        Ok(None) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard group count fan-out planning error: {:?}", e),
            ));
        }
    };
    let owners = match state
        .executor
        .shard_group_count_select_fanout_owners_for_sql(sql, &[])
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard group count fan-out planning error: {:?}", e),
            ));
        }
    };

    // With ORDER BY / LIMIT / OFFSET, owners run the stripped SQL (all groups); clauses are applied
    // once, post-merge. Otherwise the original query is forwarded verbatim.
    let per_owner_sql = plan
        .post_merge
        .as_ref()
        .map_or(sql, |spec| spec.per_owner_sql.as_str());
    let local_results = match state.executor.execute_sql(per_owner_sql).await {
        Ok(results) => results.into_iter().map(QueryResultJson::from).collect(),
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Execution Error: {:?}", e),
            ));
        }
    };
    let mut groups = BTreeMap::new();
    let columns = match accumulate_fanout_group_counts(
        &mut groups,
        local_results,
        &plan.group_indices,
        plan.count_index,
    ) {
        Ok(columns) => columns,
        Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
    };

    for owner in owners {
        let owner_results =
            match query_remote_shard_owner(state, context, per_owner_sql, &owner).await {
                Ok(results) => results,
                Err((status, message)) => return Some(json_error(status, message)),
            };
        let owner_columns = match accumulate_fanout_group_counts(
            &mut groups,
            owner_results,
            &plan.group_indices,
            plan.count_index,
        ) {
            Ok(columns) => columns,
            Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
        };
        if owner_columns != columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard group count fan-out column mismatch: expected {:?}, got {:?}",
                    columns, owner_columns
                ),
            ));
        }
    }

    let mut rows = group_count_rows(groups, &plan.group_indices, plan.count_index);
    if let Some(spec) = &plan.post_merge {
        crate::execution::apply_grouped_order_limit(&mut rows, spec);
    }
    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns,
        rows,
    }]))
}

async fn try_fanout_group_aggregate_query_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    sql: &str,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled {
        return None;
    }
    let plan = match state
        .executor
        .shard_group_aggregate_select_fanout_plan_for_sql(sql)
    {
        Ok(Some(plan)) => plan,
        Ok(None) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard group aggregate fan-out planning error: {:?}", e),
            ));
        }
    };
    let owners = match state
        .executor
        .shard_group_aggregate_select_fanout_owners_for_sql(sql, &[])
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard group aggregate fan-out planning error: {:?}", e),
            ));
        }
    };

    // When the query carries ORDER BY / LIMIT / OFFSET, owners must run the stripped SQL so each
    // returns ALL its groups; the clauses are applied once, post-merge, on the combined rows.
    let per_owner_sql = plan
        .post_merge
        .as_ref()
        .map_or(sql, |spec| spec.per_owner_sql.as_str());
    let local_results = match state.executor.execute_sql(per_owner_sql).await {
        Ok(results) => results.into_iter().map(QueryResultJson::from).collect(),
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Execution Error: {:?}", e),
            ));
        }
    };
    let mut groups = BTreeMap::new();
    let columns = match accumulate_fanout_group_aggregates(
        &mut groups,
        local_results,
        &plan.group_indices,
        plan.agg_index,
        plan.kind,
    ) {
        Ok(columns) => columns,
        Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
    };

    for owner in owners {
        let owner_results =
            match query_remote_shard_owner(state, context, per_owner_sql, &owner).await {
                Ok(results) => results,
                Err((status, message)) => return Some(json_error(status, message)),
            };
        let owner_columns = match accumulate_fanout_group_aggregates(
            &mut groups,
            owner_results,
            &plan.group_indices,
            plan.agg_index,
            plan.kind,
        ) {
            Ok(columns) => columns,
            Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
        };
        if owner_columns != columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard group aggregate fan-out column mismatch: expected {:?}, got {:?}",
                    columns, owner_columns
                ),
            ));
        }
    }

    let mut rows = group_aggregate_rows(groups, &plan.group_indices, plan.agg_index);
    if let Some(spec) = &plan.post_merge {
        crate::execution::apply_grouped_order_limit(&mut rows, spec);
    }
    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns,
        rows,
    }]))
}

async fn try_fanout_group_avg_query_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    sql: &str,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled {
        return None;
    }
    let plan = match state
        .executor
        .shard_group_avg_select_fanout_plan_for_sql(sql)
    {
        Ok(Some(plan)) => plan,
        Ok(None) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard group avg fan-out planning error: {:?}", e),
            ));
        }
    };
    let owners = match state
        .executor
        .shard_group_avg_select_fanout_owners_for_sql(sql, &[])
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard group avg fan-out planning error: {:?}", e),
            ));
        }
    };

    let local_results = match state.executor.execute_sql(&plan.rewritten_sql).await {
        Ok(results) => results.into_iter().map(QueryResultJson::from).collect(),
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Execution Error: {:?}", e),
            ));
        }
    };
    let mut groups = BTreeMap::new();
    let columns = match accumulate_fanout_group_avg(
        &mut groups,
        local_results,
        &plan.group_indices,
        plan.sum_index,
        plan.count_index,
    ) {
        Ok(columns) => columns,
        Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
    };

    for owner in owners {
        let owner_results =
            match query_remote_shard_owner(state, context, &plan.rewritten_sql, &owner).await {
                Ok(results) => results,
                Err((status, message)) => return Some(json_error(status, message)),
            };
        let owner_columns = match accumulate_fanout_group_avg(
            &mut groups,
            owner_results,
            &plan.group_indices,
            plan.sum_index,
            plan.count_index,
        ) {
            Ok(columns) => columns,
            Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
        };
        if owner_columns != columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard group avg fan-out column mismatch: expected {:?}, got {:?}",
                    columns, owner_columns
                ),
            ));
        }
    }

    let mut rows = group_avg_rows(groups, plan.avg_output_index, plan.output_columns.len());
    if let Some(spec) = &plan.post_merge {
        crate::execution::apply_grouped_order_limit(&mut rows, spec);
    }
    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns: plan.output_columns.clone(),
        rows,
    }]))
}

async fn try_fanout_group_multi_aggregate_query_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    sql: &str,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled {
        return None;
    }
    let plan = match state
        .executor
        .shard_group_multi_aggregate_select_fanout_plan_for_sql(sql)
    {
        Ok(Some(plan)) => plan,
        Ok(None) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!(
                    "Shard group multi-aggregate fan-out planning error: {:?}",
                    e
                ),
            ));
        }
    };
    let owners = match state
        .executor
        .shard_group_multi_aggregate_select_fanout_owners_for_sql(sql, &[])
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!(
                    "Shard group multi-aggregate fan-out planning error: {:?}",
                    e
                ),
            ));
        }
    };

    let per_owner_sql = plan
        .post_merge
        .as_ref()
        .map_or(sql, |spec| spec.per_owner_sql.as_str());
    let local_results = match state.executor.execute_sql(per_owner_sql).await {
        Ok(results) => results.into_iter().map(QueryResultJson::from).collect(),
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Execution Error: {:?}", e),
            ));
        }
    };
    let mut groups = BTreeMap::new();
    let columns = match accumulate_fanout_group_multi_aggregates(
        &mut groups,
        local_results,
        &plan.group_indices,
        &plan.aggregates,
    ) {
        Ok(columns) => columns,
        Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
    };

    for owner in owners {
        let owner_results =
            match query_remote_shard_owner(state, context, per_owner_sql, &owner).await {
                Ok(results) => results,
                Err((status, message)) => return Some(json_error(status, message)),
            };
        let owner_columns = match accumulate_fanout_group_multi_aggregates(
            &mut groups,
            owner_results,
            &plan.group_indices,
            &plan.aggregates,
        ) {
            Ok(columns) => columns,
            Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
        };
        if owner_columns != columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard group multi-aggregate fan-out column mismatch: expected {:?}, got {:?}",
                    columns, owner_columns
                ),
            ));
        }
    }

    let mut rows = group_multi_aggregate_rows(groups, &plan.group_indices, &plan.aggregates);
    if let Some(spec) = &plan.post_merge {
        crate::execution::apply_grouped_order_limit(&mut rows, spec);
    }
    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns,
        rows,
    }]))
}

async fn try_unsupported_group_by_fanout_error(
    state: &AppState,
    context: &RequestContext,
    sql: &str,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled {
        return None;
    }
    match state
        .executor
        .shard_unsupported_group_by_fanout_error_for_sql(sql, &[])
        .await
    {
        Ok(Some(message)) => Some(json_error(StatusCode::BAD_REQUEST, message)),
        Ok(None) => None,
        Err(e) => Some(json_error(
            StatusCode::BAD_REQUEST,
            format!("Shard group by fan-out planning error: {:?}", e),
        )),
    }
}

async fn try_fanout_count_distinct_query_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    sql: &str,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled {
        return None;
    }
    let plan = match state
        .executor
        .shard_count_distinct_select_fanout_plan_for_sql(sql)
    {
        Ok(Some(plan)) => plan,
        Ok(None) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard count distinct fan-out planning error: {:?}", e),
            ));
        }
    };
    let owners = match state
        .executor
        .shard_count_distinct_select_fanout_owners_for_sql(sql, &[])
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard count distinct fan-out planning error: {:?}", e),
            ));
        }
    };

    let local_results = match state.executor.execute_sql(&plan.rewritten_sql).await {
        Ok(results) => results.into_iter().map(QueryResultJson::from).collect(),
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Execution Error: {:?}", e),
            ));
        }
    };
    let (rewrite_columns, local_values) =
        match fanout_distinct_values_from_select_results(local_results) {
            Ok(values) => values,
            Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
        };
    let mut distinct_values = BTreeSet::new();
    if let Err(message) = add_fanout_distinct_values(&mut distinct_values, local_values) {
        return Some(json_error(StatusCode::BAD_GATEWAY, message));
    }

    for owner in owners {
        let owner_results =
            match query_remote_shard_owner(state, context, &plan.rewritten_sql, &owner).await {
                Ok(results) => results,
                Err((status, message)) => return Some(json_error(status, message)),
            };
        let (owner_columns, owner_values) =
            match fanout_distinct_values_from_select_results(owner_results) {
                Ok(values) => values,
                Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
            };
        if owner_columns != rewrite_columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard count distinct fan-out column mismatch: expected {:?}, got {:?}",
                    rewrite_columns, owner_columns
                ),
            ));
        }
        if let Err(message) = add_fanout_distinct_values(&mut distinct_values, owner_values) {
            return Some(json_error(StatusCode::BAD_GATEWAY, message));
        }
    }

    let count = match i64::try_from(distinct_values.len()) {
        Ok(count) => count,
        Err(_) => {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                "Shard count distinct fan-out overflow",
            ));
        }
    };
    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns: vec![plan.output_column],
        rows: vec![vec![serde_json::json!(count)]],
    }]))
}

async fn try_fanout_sum_query_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    sql: &str,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled {
        return None;
    }
    let owners = match state
        .executor
        .shard_sum_select_fanout_owners_for_sql(sql, &[])
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard sum fan-out planning error: {:?}", e),
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

    let (columns, mut total) = match fanout_sum_from_select_results(local_results) {
        Ok(sum) => sum,
        Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
    };

    for owner in owners {
        let owner_results = match query_remote_shard_owner(state, context, sql, &owner).await {
            Ok(results) => results,
            Err((status, message)) => return Some(json_error(status, message)),
        };
        let (owner_columns, owner_sum) = match fanout_sum_from_select_results(owner_results) {
            Ok(sum) => sum,
            Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
        };
        if owner_columns != columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard sum fan-out column mismatch: expected {:?}, got {:?}",
                    columns, owner_columns
                ),
            ));
        }
        if let Err(message) = add_fanout_sum(&mut total, owner_sum) {
            return Some(json_error(StatusCode::BAD_GATEWAY, message));
        }
    }

    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns,
        rows: vec![vec![fanout_sum_to_json(total)]],
    }]))
}

async fn try_fanout_min_max_query_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    sql: &str,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled {
        return None;
    }
    let kind = match state.executor.shard_min_max_select_fanout_kind_for_sql(sql) {
        Ok(Some(kind)) => kind,
        Ok(None) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard min/max fan-out planning error: {:?}", e),
            ));
        }
    };
    let owners = match state
        .executor
        .shard_min_max_select_fanout_owners_for_sql(sql, &[])
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard min/max fan-out planning error: {:?}", e),
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

    let (columns, mut total) = match fanout_extremum_from_select_results(local_results) {
        Ok(extremum) => extremum,
        Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
    };

    for owner in owners {
        let owner_results = match query_remote_shard_owner(state, context, sql, &owner).await {
            Ok(results) => results,
            Err((status, message)) => return Some(json_error(status, message)),
        };
        let (owner_columns, owner_extremum) =
            match fanout_extremum_from_select_results(owner_results) {
                Ok(extremum) => extremum,
                Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
            };
        if owner_columns != columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard min/max fan-out column mismatch: expected {:?}, got {:?}",
                    columns, owner_columns
                ),
            ));
        }
        if let Err(message) = merge_fanout_extremum(&mut total, owner_extremum, kind) {
            return Some(json_error(StatusCode::BAD_GATEWAY, message));
        }
    }

    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns,
        rows: vec![vec![total.unwrap_or(serde_json::Value::Null)]],
    }]))
}

async fn try_fanout_avg_query_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    sql: &str,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled {
        return None;
    }
    let plan = match state.executor.shard_avg_select_fanout_plan_for_sql(sql) {
        Ok(Some(plan)) => plan,
        Ok(None) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard avg fan-out planning error: {:?}", e),
            ));
        }
    };
    let owners = match state
        .executor
        .shard_avg_select_fanout_owners_for_sql(sql, &[])
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard avg fan-out planning error: {:?}", e),
            ));
        }
    };

    let local_results = match state.executor.execute_sql(&plan.rewritten_sql).await {
        Ok(results) => results.into_iter().map(QueryResultJson::from).collect(),
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Execution Error: {:?}", e),
            ));
        }
    };
    let (rewrite_columns, mut total_sum, mut total_count) =
        match fanout_avg_parts_from_select_results(local_results) {
            Ok(parts) => parts,
            Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
        };

    for owner in owners {
        let owner_results =
            match query_remote_shard_owner(state, context, &plan.rewritten_sql, &owner).await {
                Ok(results) => results,
                Err((status, message)) => return Some(json_error(status, message)),
            };
        let (owner_columns, owner_sum, owner_count) =
            match fanout_avg_parts_from_select_results(owner_results) {
                Ok(parts) => parts,
                Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
            };
        if owner_columns != rewrite_columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard avg fan-out column mismatch: expected {:?}, got {:?}",
                    rewrite_columns, owner_columns
                ),
            ));
        }
        if let Err(message) = add_fanout_sum(&mut total_sum, owner_sum) {
            return Some(json_error(StatusCode::BAD_GATEWAY, message));
        }
        total_count = match total_count.checked_add(owner_count) {
            Some(total) => total,
            None => {
                return Some(json_error(
                    StatusCode::BAD_GATEWAY,
                    "Shard avg fan-out count overflow",
                ));
            }
        };
    }

    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns: vec![plan.output_column],
        rows: vec![vec![fanout_avg_to_json(total_sum, total_count)]],
    }]))
}

async fn try_fanout_sum_distinct_query_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    sql: &str,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled {
        return None;
    }
    let plan = match state
        .executor
        .shard_sum_distinct_select_fanout_plan_for_sql(sql)
    {
        Ok(Some(plan)) => plan,
        Ok(None) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard sum distinct fan-out planning error: {:?}", e),
            ));
        }
    };
    let owners = match state
        .executor
        .shard_sum_distinct_select_fanout_owners_for_sql(sql, &[])
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard sum distinct fan-out planning error: {:?}", e),
            ));
        }
    };

    let local_results = match state.executor.execute_sql(&plan.rewritten_sql).await {
        Ok(results) => results.into_iter().map(QueryResultJson::from).collect(),
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Execution Error: {:?}", e),
            ));
        }
    };
    let mut distinct_values = BTreeMap::new();
    let rewrite_columns =
        match collect_fanout_distinct_value_map(&mut distinct_values, local_results) {
            Ok(columns) => columns,
            Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
        };

    for owner in owners {
        let owner_results =
            match query_remote_shard_owner(state, context, &plan.rewritten_sql, &owner).await {
                Ok(results) => results,
                Err((status, message)) => return Some(json_error(status, message)),
            };
        let owner_columns =
            match collect_fanout_distinct_value_map(&mut distinct_values, owner_results) {
                Ok(columns) => columns,
                Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
            };
        if owner_columns != rewrite_columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard sum distinct fan-out column mismatch: expected {:?}, got {:?}",
                    rewrite_columns, owner_columns
                ),
            ));
        }
    }

    let total = match fanout_sum_over_distinct_values(&distinct_values) {
        Ok(total) => total,
        Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
    };
    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns: vec![plan.output_column],
        rows: vec![vec![fanout_sum_to_json(total)]],
    }]))
}

async fn try_fanout_avg_distinct_query_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    sql: &str,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled {
        return None;
    }
    let plan = match state
        .executor
        .shard_avg_distinct_select_fanout_plan_for_sql(sql)
    {
        Ok(Some(plan)) => plan,
        Ok(None) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard avg distinct fan-out planning error: {:?}", e),
            ));
        }
    };
    let owners = match state
        .executor
        .shard_avg_distinct_select_fanout_owners_for_sql(sql, &[])
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard avg distinct fan-out planning error: {:?}", e),
            ));
        }
    };

    let local_results = match state.executor.execute_sql(&plan.rewritten_sql).await {
        Ok(results) => results.into_iter().map(QueryResultJson::from).collect(),
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Execution Error: {:?}", e),
            ));
        }
    };
    let mut distinct_values = BTreeMap::new();
    let rewrite_columns =
        match collect_fanout_distinct_value_map(&mut distinct_values, local_results) {
            Ok(columns) => columns,
            Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
        };

    for owner in owners {
        let owner_results =
            match query_remote_shard_owner(state, context, &plan.rewritten_sql, &owner).await {
                Ok(results) => results,
                Err((status, message)) => return Some(json_error(status, message)),
            };
        let owner_columns =
            match collect_fanout_distinct_value_map(&mut distinct_values, owner_results) {
                Ok(columns) => columns,
                Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
            };
        if owner_columns != rewrite_columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard avg distinct fan-out column mismatch: expected {:?}, got {:?}",
                    rewrite_columns, owner_columns
                ),
            ));
        }
    }

    let total_sum = match fanout_sum_over_distinct_values(&distinct_values) {
        Ok(total) => total,
        Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
    };
    let total_count = match i64::try_from(distinct_values.len()) {
        Ok(count) => count,
        Err(_) => {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                "Shard avg distinct fan-out overflow",
            ));
        }
    };
    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns: vec![plan.output_column],
        rows: vec![vec![fanout_avg_to_json(total_sum, total_count)]],
    }]))
}

fn collect_fanout_distinct_value_map(
    distinct_values: &mut BTreeMap<String, serde_json::Value>,
    results: Vec<QueryResultJson>,
) -> std::result::Result<Vec<String>, String> {
    let (columns, values) = fanout_distinct_values_from_select_results(results)?;
    add_fanout_distinct_value_map(distinct_values, values)?;
    Ok(columns)
}

async fn query_remote_shard_owner(
    state: &AppState,
    context: &RequestContext,
    sql: &str,
    owner: &SqlShardOwner,
) -> std::result::Result<Vec<QueryResultJson>, (StatusCode, String)> {
    let url = format!("{}://{}/query", state.peer_scheme, owner.addr);
    let payload = QueryRequest {
        sql: sql.to_string(),
    };
    let response = apply_forwarding_headers(state.raft_client.post(&url).json(&payload), context)
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

fn fanout_count_from_select_results(
    results: Vec<QueryResultJson>,
) -> std::result::Result<(Vec<String>, i64), String> {
    let [result] = results.as_slice() else {
        return Err("Shard count fan-out expected exactly one SELECT result".to_string());
    };
    let QueryResultJson::Select { columns, rows, .. } = result else {
        return Err("Shard count fan-out received a non-SELECT result".to_string());
    };
    if columns.len() != 1 || rows.len() != 1 || rows[0].len() != 1 {
        return Err("Shard count fan-out expected one row with one count column".to_string());
    }
    let value = &rows[0][0];
    let count = value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|count| i64::try_from(count).ok()));
    let Some(count) = count else {
        return Err(format!(
            "Shard count fan-out received a non-integer count value: {}",
            value
        ));
    };
    Ok((columns.clone(), count))
}

/// Accumulate one owner's `SELECT col, COUNT(*) ... GROUP BY col` rows into `groups`, keyed by the
/// canonical JSON of the group value (so NULL is its own group), summing the counts. Returns the
/// owner's column names for cross-owner consistency checking.
fn accumulate_fanout_group_counts(
    groups: &mut BTreeMap<String, (Vec<serde_json::Value>, i64)>,
    results: Vec<QueryResultJson>,
    group_indices: &[usize],
    count_index: usize,
) -> std::result::Result<Vec<String>, String> {
    let [result] = results.as_slice() else {
        return Err("Shard group count fan-out expected exactly one SELECT result".to_string());
    };
    let QueryResultJson::Select { columns, rows, .. } = result else {
        return Err("Shard group count fan-out received a non-SELECT result".to_string());
    };
    let expected = group_indices.len() + 1;
    if columns.len() != expected {
        return Err(format!(
            "Shard group count fan-out expected {} output columns",
            expected
        ));
    }
    for row in rows {
        if row.len() != expected {
            return Err(format!(
                "Shard group count fan-out expected {} values per row",
                expected
            ));
        }
        let count_value = &row[count_index];
        let count = count_value.as_i64().or_else(|| {
            count_value
                .as_u64()
                .and_then(|count| i64::try_from(count).ok())
        });
        let Some(count) = count else {
            return Err(format!(
                "Shard group count fan-out received a non-integer count value: {}",
                count_value
            ));
        };
        let group_values: Vec<serde_json::Value> =
            group_indices.iter().map(|&i| row[i].clone()).collect();
        let key = serde_json::to_string(&group_values).map_err(|e| {
            format!(
                "Shard group count fan-out could not encode group key {:?}: {}",
                group_values, e
            )
        })?;
        let entry = groups.entry(key).or_insert_with(|| (group_values, 0));
        entry.1 = entry
            .1
            .checked_add(count)
            .ok_or("Shard group count fan-out overflow")?;
    }
    Ok(columns.clone())
}

/// Build the merged result rows from accumulated group counts, in projection column order, sorted
/// by group key (BTreeMap iteration order) for determinism (GROUP BY without ORDER BY is unordered).
fn group_count_rows(
    groups: BTreeMap<String, (Vec<serde_json::Value>, i64)>,
    group_indices: &[usize],
    count_index: usize,
) -> Vec<Vec<serde_json::Value>> {
    let width = group_indices.len() + 1;
    let mut rows = Vec::with_capacity(groups.len());
    for (_, (group_values, count)) in groups {
        let mut row = vec![serde_json::Value::Null; width];
        for (k, &gi) in group_indices.iter().enumerate() {
            row[gi] = group_values[k].clone();
        }
        row[count_index] = serde_json::json!(count);
        rows.push(row);
    }
    rows
}

/// Per-group running accumulator for grouped SUM/MIN/MAX fan-out.
enum GroupAggAcc {
    Sum(Option<FanoutSum>),
    Extremum(Option<serde_json::Value>),
}

impl GroupAggAcc {
    fn new(kind: SqlShardGroupAggregateKind) -> Self {
        match kind {
            SqlShardGroupAggregateKind::Sum => GroupAggAcc::Sum(None),
            SqlShardGroupAggregateKind::Min | SqlShardGroupAggregateKind::Max => {
                GroupAggAcc::Extremum(None)
            }
        }
    }

    fn update(
        &mut self,
        value: &serde_json::Value,
        kind: SqlShardGroupAggregateKind,
    ) -> std::result::Result<(), String> {
        match self {
            GroupAggAcc::Sum(total) => add_fanout_sum(total, fanout_sum_json_value(value)?),
            GroupAggAcc::Extremum(total) => {
                let ext_kind = match kind {
                    SqlShardGroupAggregateKind::Min => SqlShardExtremum::Min,
                    _ => SqlShardExtremum::Max,
                };
                let candidate = if value.is_null() {
                    None
                } else if value.as_f64().is_some_and(|v| v.is_finite())
                    || fanout_decimal_f64(value).is_some()
                {
                    Some(value.clone())
                } else {
                    return Err(format!(
                        "Shard group min/max fan-out received a non-numeric value: {}",
                        value
                    ));
                };
                merge_fanout_extremum(total, candidate, ext_kind)
            }
        }
    }

    fn finalize(self) -> serde_json::Value {
        match self {
            GroupAggAcc::Sum(total) => fanout_sum_to_json(total),
            GroupAggAcc::Extremum(total) => total.unwrap_or(serde_json::Value::Null),
        }
    }
}

fn accumulate_fanout_group_aggregates(
    groups: &mut BTreeMap<String, (Vec<serde_json::Value>, GroupAggAcc)>,
    results: Vec<QueryResultJson>,
    group_indices: &[usize],
    agg_index: usize,
    kind: SqlShardGroupAggregateKind,
) -> std::result::Result<Vec<String>, String> {
    let [result] = results.as_slice() else {
        return Err("Shard group aggregate fan-out expected exactly one SELECT result".to_string());
    };
    let QueryResultJson::Select { columns, rows, .. } = result else {
        return Err("Shard group aggregate fan-out received a non-SELECT result".to_string());
    };
    let expected = group_indices.len() + 1;
    if columns.len() != expected {
        return Err(format!(
            "Shard group aggregate fan-out expected {} output columns",
            expected
        ));
    }
    for row in rows {
        if row.len() != expected {
            return Err(format!(
                "Shard group aggregate fan-out expected {} values per row",
                expected
            ));
        }
        let group_values: Vec<serde_json::Value> =
            group_indices.iter().map(|&i| row[i].clone()).collect();
        let key = serde_json::to_string(&group_values).map_err(|e| {
            format!(
                "Shard group aggregate fan-out could not encode group key {:?}: {}",
                group_values, e
            )
        })?;
        let entry = groups
            .entry(key)
            .or_insert_with(|| (group_values, GroupAggAcc::new(kind)));
        entry.1.update(&row[agg_index], kind)?;
    }
    Ok(columns.clone())
}

fn group_aggregate_rows(
    groups: BTreeMap<String, (Vec<serde_json::Value>, GroupAggAcc)>,
    group_indices: &[usize],
    agg_index: usize,
) -> Vec<Vec<serde_json::Value>> {
    let width = group_indices.len() + 1;
    let mut rows = Vec::with_capacity(groups.len());
    for (_, (group_values, acc)) in groups {
        let mut row = vec![serde_json::Value::Null; width];
        for (k, &gi) in group_indices.iter().enumerate() {
            row[gi] = group_values[k].clone();
        }
        row[agg_index] = acc.finalize();
        rows.push(row);
    }
    rows
}

/// Accumulate one owner's multi-aggregate grouped result, re-grouping on the composite key and
/// reducing EACH aggregate independently into its own per-group accumulator (COUNT/SUM by adding,
/// MIN/MAX by extremum). Returns the result columns for cross-owner consistency checks.
fn accumulate_fanout_group_multi_aggregates(
    groups: &mut BTreeMap<String, (Vec<serde_json::Value>, Vec<GroupAggAcc>)>,
    results: Vec<QueryResultJson>,
    group_indices: &[usize],
    aggregates: &[GroupMultiAggregate],
) -> std::result::Result<Vec<String>, String> {
    let [result] = results.as_slice() else {
        return Err(
            "Shard group multi-aggregate fan-out expected exactly one SELECT result".to_string(),
        );
    };
    let QueryResultJson::Select { columns, rows, .. } = result else {
        return Err("Shard group multi-aggregate fan-out received a non-SELECT result".to_string());
    };
    let expected = group_indices.len() + aggregates.len();
    if columns.len() != expected {
        return Err(format!(
            "Shard group multi-aggregate fan-out expected {} output columns",
            expected
        ));
    }
    for row in rows {
        if row.len() != expected {
            return Err(format!(
                "Shard group multi-aggregate fan-out expected {} values per row",
                expected
            ));
        }
        let group_values: Vec<serde_json::Value> =
            group_indices.iter().map(|&i| row[i].clone()).collect();
        let key = serde_json::to_string(&group_values).map_err(|e| {
            format!(
                "Shard group multi-aggregate fan-out could not encode group key {:?}: {}",
                group_values, e
            )
        })?;
        let entry = groups.entry(key).or_insert_with(|| {
            (
                group_values,
                aggregates
                    .iter()
                    .map(|a| GroupAggAcc::new(a.kind))
                    .collect(),
            )
        });
        for (slot, agg) in entry.1.iter_mut().zip(aggregates.iter()) {
            slot.update(&row[agg.output_index], agg.kind)?;
        }
    }
    Ok(columns.clone())
}

fn group_multi_aggregate_rows(
    groups: BTreeMap<String, (Vec<serde_json::Value>, Vec<GroupAggAcc>)>,
    group_indices: &[usize],
    aggregates: &[GroupMultiAggregate],
) -> Vec<Vec<serde_json::Value>> {
    let width = group_indices.len() + aggregates.len();
    let mut rows = Vec::with_capacity(groups.len());
    for (_, (group_values, accs)) in groups {
        let mut row = vec![serde_json::Value::Null; width];
        for (k, &gi) in group_indices.iter().enumerate() {
            row[gi] = group_values[k].clone();
        }
        for (acc, agg) in accs.into_iter().zip(aggregates.iter()) {
            row[agg.output_index] = acc.finalize();
        }
        rows.push(row);
    }
    rows
}

/// Accumulate partial `SUM`/`COUNT` per group from one owner's rewritten grouped-AVG result. Keys on
/// the canonical JSON array of the group tuple at `group_indices`, adding the sum at `sum_index` and
/// the (non-null) count at `count_index`. Returns the rewritten result columns for cross-owner
/// consistency checks.
fn accumulate_fanout_group_avg(
    groups: &mut BTreeMap<String, (Vec<serde_json::Value>, Option<FanoutSum>, i64)>,
    results: Vec<QueryResultJson>,
    group_indices: &[usize],
    sum_index: usize,
    count_index: usize,
) -> std::result::Result<Vec<String>, String> {
    let [result] = results.as_slice() else {
        return Err("Shard group avg fan-out expected exactly one SELECT result".to_string());
    };
    let QueryResultJson::Select { columns, rows, .. } = result else {
        return Err("Shard group avg fan-out received a non-SELECT result".to_string());
    };
    let expected = group_indices.len() + 2;
    if columns.len() != expected {
        return Err(format!(
            "Shard group avg fan-out expected {} output columns",
            expected
        ));
    }
    for row in rows {
        if row.len() != expected {
            return Err(format!(
                "Shard group avg fan-out expected {} values per row",
                expected
            ));
        }
        let group_values: Vec<serde_json::Value> =
            group_indices.iter().map(|&i| row[i].clone()).collect();
        let key = serde_json::to_string(&group_values).map_err(|e| {
            format!(
                "Shard group avg fan-out could not encode group key {:?}: {}",
                group_values, e
            )
        })?;
        let count = row[count_index].as_i64().or_else(|| {
            row[count_index]
                .as_u64()
                .and_then(|count| i64::try_from(count).ok())
        });
        let Some(count) = count else {
            return Err(format!(
                "Shard group avg fan-out received a non-integer count value: {}",
                row[count_index]
            ));
        };
        let entry = groups
            .entry(key)
            .or_insert_with(|| (group_values, None, 0i64));
        add_fanout_sum(&mut entry.1, fanout_sum_json_value(&row[sum_index])?)?;
        entry.2 = entry
            .2
            .checked_add(count)
            .ok_or_else(|| "Shard group avg fan-out count overflow".to_string())?;
    }
    Ok(columns.clone())
}

/// Rebuild grouped-AVG output rows in the original projection layout: group values in place plus the
/// computed average (`sum / count`) at `avg_output_index`.
fn group_avg_rows(
    groups: BTreeMap<String, (Vec<serde_json::Value>, Option<FanoutSum>, i64)>,
    avg_output_index: usize,
    width: usize,
) -> Vec<Vec<serde_json::Value>> {
    let mut rows = Vec::with_capacity(groups.len());
    for (_, (group_values, sum, count)) in groups {
        let avg_value = fanout_avg_to_json(sum, count);
        let mut row = vec![serde_json::Value::Null; width];
        let mut g = 0;
        for (pos, cell) in row.iter_mut().enumerate() {
            if pos == avg_output_index {
                *cell = avg_value.clone();
            } else {
                *cell = group_values[g].clone();
                g += 1;
            }
        }
        rows.push(row);
    }
    rows
}

fn fanout_distinct_values_from_select_results(
    results: Vec<QueryResultJson>,
) -> std::result::Result<(Vec<String>, Vec<serde_json::Value>), String> {
    let [result] = results.as_slice() else {
        return Err("Shard count distinct fan-out expected exactly one SELECT result".to_string());
    };
    let QueryResultJson::Select { columns, rows, .. } = result else {
        return Err("Shard count distinct fan-out received a non-SELECT result".to_string());
    };
    if columns.len() != 1 {
        return Err("Shard count distinct fan-out expected one distinct value column".to_string());
    }
    let mut values = Vec::with_capacity(rows.len());
    for row in rows {
        let [value] = row.as_slice() else {
            return Err(
                "Shard count distinct fan-out expected one value per distinct row".to_string(),
            );
        };
        values.push(value.clone());
    }
    Ok((columns.clone(), values))
}

fn add_fanout_distinct_values(
    distinct_values: &mut BTreeSet<String>,
    values: Vec<serde_json::Value>,
) -> std::result::Result<(), String> {
    for value in values {
        if value.is_null() {
            continue;
        }
        let key = serde_json::to_string(&value).map_err(|e| {
            format!(
                "Shard count distinct fan-out could not encode distinct value {}: {}",
                value, e
            )
        })?;
        distinct_values.insert(key);
    }
    Ok(())
}

fn add_fanout_distinct_value_map(
    distinct_values: &mut BTreeMap<String, serde_json::Value>,
    values: Vec<serde_json::Value>,
) -> std::result::Result<(), String> {
    for value in values {
        if value.is_null() {
            continue;
        }
        let key = serde_json::to_string(&value).map_err(|e| {
            format!(
                "Shard distinct aggregate fan-out could not encode distinct value {}: {}",
                value, e
            )
        })?;
        distinct_values.insert(key, value);
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum FanoutSum {
    Integer(i64),
    Float(f64),
}

/// Detect a fan-out aggregate value that is a JSON STRING holding a finite DECIMAL/NUMERIC number (the
/// JSON representation of `Value::Decimal`). `MIN`/`MAX` over a DECIMAL column return the value itself,
/// which serializes as such a string, and the DISTINCT-aggregate paths merge raw column values;
/// `SUM`/`AVG` over DECIMAL return a float, so only those paths need this. Returns `None` for any
/// non-string / non-finite value.
fn fanout_decimal_f64(value: &serde_json::Value) -> Option<f64> {
    match value {
        serde_json::Value::String(s) => s.parse::<f64>().ok().filter(|v| v.is_finite()),
        _ => None,
    }
}

/// Wrap a fan-out value for MIN/MAX comparison: a JSON string holding a finite decimal becomes
/// `Value::Decimal` (numeric compare via `compare_decimal_strings`) rather than `Value::String`
/// (lexical compare). Only decimal/int/float columns reach the extremum path (the numeric gate rejects
/// text), so treating a numeric-looking string as a decimal is safe here.
fn fanout_extremum_value(value: &serde_json::Value) -> Value {
    match value {
        serde_json::Value::String(s) if fanout_decimal_f64(value).is_some() => {
            Value::Decimal(s.clone())
        }
        _ => Value::from_json(value),
    }
}

fn fanout_sum_from_select_results(
    results: Vec<QueryResultJson>,
) -> std::result::Result<(Vec<String>, Option<FanoutSum>), String> {
    let [result] = results.as_slice() else {
        return Err("Shard sum fan-out expected exactly one SELECT result".to_string());
    };
    let QueryResultJson::Select { columns, rows, .. } = result else {
        return Err("Shard sum fan-out received a non-SELECT result".to_string());
    };
    if columns.len() != 1 || rows.len() != 1 || rows[0].len() != 1 {
        return Err("Shard sum fan-out expected one row with one sum column".to_string());
    }
    Ok((columns.clone(), fanout_sum_json_value(&rows[0][0])?))
}

fn fanout_sum_json_value(
    value: &serde_json::Value,
) -> std::result::Result<Option<FanoutSum>, String> {
    if value.is_null() {
        return Ok(None);
    }
    if let Some(value) = value.as_i64() {
        return Ok(Some(FanoutSum::Integer(value)));
    }
    if let Some(value) = value.as_u64() {
        let value = i64::try_from(value).map_err(|_| {
            format!(
                "Shard sum fan-out received an out-of-range integer sum value: {}",
                value
            )
        })?;
        return Ok(Some(FanoutSum::Integer(value)));
    }
    if let Some(value) = value.as_f64() {
        if value.is_finite() {
            return Ok(Some(FanoutSum::Float(value)));
        }
    }
    Err(format!(
        "Shard sum fan-out received a non-numeric sum value: {}",
        value
    ))
}

fn add_fanout_sum(
    total: &mut Option<FanoutSum>,
    value: Option<FanoutSum>,
) -> std::result::Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    *total = Some(match (*total, value) {
        (None, value) => value,
        (Some(FanoutSum::Integer(left)), FanoutSum::Integer(right)) => FanoutSum::Integer(
            left.checked_add(right)
                .ok_or("Shard sum fan-out overflow")?,
        ),
        (Some(FanoutSum::Integer(left)), FanoutSum::Float(right)) => {
            FanoutSum::Float(left as f64 + right)
        }
        (Some(FanoutSum::Float(left)), FanoutSum::Integer(right)) => {
            FanoutSum::Float(left + right as f64)
        }
        (Some(FanoutSum::Float(left)), FanoutSum::Float(right)) => FanoutSum::Float(left + right),
    });
    Ok(())
}

fn fanout_sum_to_json(total: Option<FanoutSum>) -> serde_json::Value {
    match total {
        None => serde_json::Value::Null,
        Some(FanoutSum::Integer(value)) => serde_json::json!(value),
        Some(FanoutSum::Float(value)) => serde_json::json!(value),
    }
}

fn fanout_sum_over_distinct_values(
    distinct_values: &BTreeMap<String, serde_json::Value>,
) -> std::result::Result<Option<FanoutSum>, String> {
    let mut total = None;
    for value in distinct_values.values() {
        // Distinct values are raw column values, so a DECIMAL column arrives
        // in its JSON string form; map it like the extremum path does.
        let parsed = if let Some(decimal) = fanout_decimal_f64(value) {
            Some(FanoutSum::Float(decimal))
        } else {
            fanout_sum_json_value(value)?
        };
        add_fanout_sum(&mut total, parsed)?;
    }
    Ok(total)
}

fn fanout_avg_parts_from_select_results(
    results: Vec<QueryResultJson>,
) -> std::result::Result<(Vec<String>, Option<FanoutSum>, i64), String> {
    let [result] = results.as_slice() else {
        return Err("Shard avg fan-out expected exactly one SELECT result".to_string());
    };
    let QueryResultJson::Select { columns, rows, .. } = result else {
        return Err("Shard avg fan-out received a non-SELECT result".to_string());
    };
    if columns.len() != 2 || rows.len() != 1 || rows[0].len() != 2 {
        return Err("Shard avg fan-out expected one row with sum and count columns".to_string());
    }
    let sum = fanout_sum_json_value(&rows[0][0])?;
    let count = rows[0][1].as_i64().or_else(|| {
        rows[0][1]
            .as_u64()
            .and_then(|count| i64::try_from(count).ok())
    });
    let Some(count) = count else {
        return Err(format!(
            "Shard avg fan-out received a non-integer count value: {}",
            rows[0][1]
        ));
    };
    Ok((columns.clone(), sum, count))
}

fn fanout_avg_to_json(total_sum: Option<FanoutSum>, total_count: i64) -> serde_json::Value {
    if total_count <= 0 {
        return serde_json::Value::Null;
    }
    let Some(total_sum) = total_sum else {
        return serde_json::Value::Null;
    };
    let sum = match total_sum {
        FanoutSum::Integer(value) => value as f64,
        FanoutSum::Float(value) => value,
    };
    serde_json::json!(sum / total_count as f64)
}

fn fanout_extremum_from_select_results(
    results: Vec<QueryResultJson>,
) -> std::result::Result<(Vec<String>, Option<serde_json::Value>), String> {
    let [result] = results.as_slice() else {
        return Err("Shard min/max fan-out expected exactly one SELECT result".to_string());
    };
    let QueryResultJson::Select { columns, rows, .. } = result else {
        return Err("Shard min/max fan-out received a non-SELECT result".to_string());
    };
    if columns.len() != 1 || rows.len() != 1 || rows[0].len() != 1 {
        return Err("Shard min/max fan-out expected one row with one min/max column".to_string());
    }
    let value = &rows[0][0];
    if value.is_null() {
        return Ok((columns.clone(), None));
    }
    if value.as_f64().is_some_and(|value| value.is_finite()) || fanout_decimal_f64(value).is_some()
    {
        return Ok((columns.clone(), Some(value.clone())));
    }
    Err(format!(
        "Shard min/max fan-out received a non-numeric min/max value: {}",
        value
    ))
}

fn merge_fanout_extremum(
    total: &mut Option<serde_json::Value>,
    value: Option<serde_json::Value>,
    kind: SqlShardExtremum,
) -> std::result::Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    let Some(current) = total.as_ref() else {
        *total = Some(value);
        return Ok(());
    };
    let candidate_value = fanout_extremum_value(&value);
    let current_value = fanout_extremum_value(current);
    let should_replace = match kind {
        SqlShardExtremum::Min => candidate_value.compare(&current_value).is_lt(),
        SqlShardExtremum::Max => candidate_value.compare(&current_value).is_gt(),
    };
    if should_replace {
        *total = Some(value);
    }
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

            if state.raft.is_some()
                && record
                    .statements
                    .iter()
                    .any(Executor::statement_may_change_query_results)
            {
                return json_error(
                    StatusCode::NOT_IMPLEMENTED,
                    "Prepared writes are disabled in distributed Raft mode until bound parameters can be captured in a replicated mutation batch",
                );
            }

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

            if let Some(response) = try_fanout_count_execute_to_shard_owners(
                &state,
                &context,
                &record,
                &payload,
                &params,
                return_results,
            )
            .await
            {
                return response;
            }

            if let Some(response) = try_fanout_group_count_execute_to_shard_owners(
                &state,
                &context,
                &record,
                &payload,
                &params,
                return_results,
            )
            .await
            {
                return response;
            }

            if let Some(response) = try_fanout_group_aggregate_execute_to_shard_owners(
                &state,
                &context,
                &record,
                &payload,
                &params,
                return_results,
            )
            .await
            {
                return response;
            }

            if let Some(response) = try_fanout_group_avg_execute_to_shard_owners(
                &state,
                &context,
                &record,
                &payload,
                &params,
                return_results,
            )
            .await
            {
                return response;
            }

            if let Some(response) = try_fanout_group_multi_aggregate_execute_to_shard_owners(
                &state,
                &context,
                &record,
                &payload,
                &params,
                return_results,
            )
            .await
            {
                return response;
            }

            if let Some(response) = try_unsupported_group_by_fanout_execute_error(
                &state,
                &context,
                &record,
                &params,
                return_results,
            )
            .await
            {
                return response;
            }

            if let Some(response) = try_fanout_count_distinct_execute_to_shard_owners(
                &state,
                &context,
                &record,
                &payload,
                &params,
                return_results,
            )
            .await
            {
                return response;
            }

            if let Some(response) = try_fanout_sum_distinct_execute_to_shard_owners(
                &state,
                &context,
                &record,
                &payload,
                &params,
                return_results,
            )
            .await
            {
                return response;
            }

            if let Some(response) = try_fanout_avg_distinct_execute_to_shard_owners(
                &state,
                &context,
                &record,
                &payload,
                &params,
                return_results,
            )
            .await
            {
                return response;
            }

            if let Some(response) = try_fanout_sum_execute_to_shard_owners(
                &state,
                &context,
                &record,
                &payload,
                &params,
                return_results,
            )
            .await
            {
                return response;
            }

            if let Some(response) = try_fanout_min_max_execute_to_shard_owners(
                &state,
                &context,
                &record,
                &payload,
                &params,
                return_results,
            )
            .await
            {
                return response;
            }

            if let Some(response) = try_fanout_avg_execute_to_shard_owners(
                &state,
                &context,
                &record,
                &payload,
                &params,
                return_results,
            )
            .await
            {
                return response;
            }

            if let Some(response) = try_fanout_execute_to_shard_owners(
                &state,
                &context,
                &record,
                &payload,
                &params,
                return_results,
            )
            .await
            {
                return response;
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

async fn try_fanout_count_execute_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    record: &PreparedStatementRecord,
    payload: &ExecuteRequest,
    params: &[Value],
    return_results: bool,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled || !return_results {
        return None;
    }
    let owners = match state
        .executor
        .shard_count_select_fanout_owners_for_statements(&record.statements, params)
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard count fan-out planning error: {:?}", e),
            ));
        }
    };

    let local_results = match execute_prepared_locally_for_fanout(state, record, params).await {
        Ok(results) => results,
        Err(response) => return Some(response),
    };
    let (columns, mut total) = match fanout_count_from_select_results(local_results) {
        Ok(count) => count,
        Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
    };

    for owner in owners {
        let owner_results = match query_remote_prepared_shard_owner(
            state, context, record, payload, &owner,
        )
        .await
        {
            Ok(results) => results,
            Err((status, message)) => return Some(json_error(status, message)),
        };
        let (owner_columns, owner_count) = match fanout_count_from_select_results(owner_results) {
            Ok(count) => count,
            Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
        };
        if owner_columns != columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard count fan-out column mismatch: expected {:?}, got {:?}",
                    columns, owner_columns
                ),
            ));
        }
        total = match total.checked_add(owner_count) {
            Some(total) => total,
            None => {
                return Some(json_error(
                    StatusCode::BAD_GATEWAY,
                    "Shard count fan-out overflow",
                ));
            }
        };
    }

    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns,
        rows: vec![vec![serde_json::json!(total)]],
    }]))
}

async fn try_fanout_group_count_execute_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    record: &PreparedStatementRecord,
    payload: &ExecuteRequest,
    params: &[Value],
    return_results: bool,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled || !return_results {
        return None;
    }
    let Some(plan) =
        Executor::shard_group_count_select_fanout_plan_for_statements(&record.statements)
    else {
        return None;
    };
    let owners = match state
        .executor
        .shard_group_count_select_fanout_owners_for_statements(&record.statements, params)
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard group count fan-out planning error: {:?}", e),
            ));
        }
    };

    // With ORDER BY / LIMIT / OFFSET, run the stripped SQL on every owner (all groups) and apply the
    // clauses post-merge; otherwise forward the prepared statement verbatim as before.
    let local_results = match &plan.post_merge {
        Some(spec) => execute_sql_locally_for_fanout(state, &spec.per_owner_sql, params).await,
        None => execute_prepared_locally_for_fanout(state, record, params).await,
    };
    let local_results = match local_results {
        Ok(results) => results,
        Err(response) => return Some(response),
    };
    let mut groups = BTreeMap::new();
    let columns = match accumulate_fanout_group_counts(
        &mut groups,
        local_results,
        &plan.group_indices,
        plan.count_index,
    ) {
        Ok(columns) => columns,
        Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
    };

    for owner in owners {
        let owner_results = match &plan.post_merge {
            Some(spec) => {
                query_remote_prepared_sql_shard_owner(
                    state,
                    context,
                    &spec.per_owner_sql,
                    payload,
                    &owner,
                )
                .await
            }
            None => {
                query_remote_prepared_shard_owner(state, context, record, payload, &owner).await
            }
        };
        let owner_results = match owner_results {
            Ok(results) => results,
            Err((status, message)) => return Some(json_error(status, message)),
        };
        let owner_columns = match accumulate_fanout_group_counts(
            &mut groups,
            owner_results,
            &plan.group_indices,
            plan.count_index,
        ) {
            Ok(columns) => columns,
            Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
        };
        if owner_columns != columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard group count fan-out column mismatch: expected {:?}, got {:?}",
                    columns, owner_columns
                ),
            ));
        }
    }

    let mut rows = group_count_rows(groups, &plan.group_indices, plan.count_index);
    if let Some(spec) = &plan.post_merge {
        crate::execution::apply_grouped_order_limit(&mut rows, spec);
    }
    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns,
        rows,
    }]))
}

async fn try_fanout_group_aggregate_execute_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    record: &PreparedStatementRecord,
    payload: &ExecuteRequest,
    params: &[Value],
    return_results: bool,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled || !return_results {
        return None;
    }
    let Some(plan) =
        Executor::shard_group_aggregate_select_fanout_plan_for_statements(&record.statements)
    else {
        return None;
    };
    let owners = match state
        .executor
        .shard_group_aggregate_select_fanout_owners_for_statements(&record.statements, params)
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard group aggregate fan-out planning error: {:?}", e),
            ));
        }
    };

    // With ORDER BY / LIMIT / OFFSET, run the stripped SQL on every owner (all groups) and apply the
    // clauses post-merge; otherwise forward the prepared statement verbatim as before.
    let local_results = match &plan.post_merge {
        Some(spec) => execute_sql_locally_for_fanout(state, &spec.per_owner_sql, params).await,
        None => execute_prepared_locally_for_fanout(state, record, params).await,
    };
    let local_results = match local_results {
        Ok(results) => results,
        Err(response) => return Some(response),
    };
    let mut groups = BTreeMap::new();
    let columns = match accumulate_fanout_group_aggregates(
        &mut groups,
        local_results,
        &plan.group_indices,
        plan.agg_index,
        plan.kind,
    ) {
        Ok(columns) => columns,
        Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
    };

    for owner in owners {
        let owner_results = match &plan.post_merge {
            Some(spec) => {
                query_remote_prepared_sql_shard_owner(
                    state,
                    context,
                    &spec.per_owner_sql,
                    payload,
                    &owner,
                )
                .await
            }
            None => {
                query_remote_prepared_shard_owner(state, context, record, payload, &owner).await
            }
        };
        let owner_results = match owner_results {
            Ok(results) => results,
            Err((status, message)) => return Some(json_error(status, message)),
        };
        let owner_columns = match accumulate_fanout_group_aggregates(
            &mut groups,
            owner_results,
            &plan.group_indices,
            plan.agg_index,
            plan.kind,
        ) {
            Ok(columns) => columns,
            Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
        };
        if owner_columns != columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard group aggregate fan-out column mismatch: expected {:?}, got {:?}",
                    columns, owner_columns
                ),
            ));
        }
    }

    let mut rows = group_aggregate_rows(groups, &plan.group_indices, plan.agg_index);
    if let Some(spec) = &plan.post_merge {
        crate::execution::apply_grouped_order_limit(&mut rows, spec);
    }
    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns,
        rows,
    }]))
}

async fn try_fanout_group_avg_execute_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    record: &PreparedStatementRecord,
    payload: &ExecuteRequest,
    params: &[Value],
    return_results: bool,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled || !return_results {
        return None;
    }
    let Some(plan) =
        Executor::shard_group_avg_select_fanout_plan_for_statements(&record.statements)
    else {
        return None;
    };
    let owners = match state
        .executor
        .shard_group_avg_select_fanout_owners_for_statements(&record.statements, params)
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard group avg fan-out planning error: {:?}", e),
            ));
        }
    };

    let local_results =
        match execute_sql_locally_for_fanout(state, &plan.rewritten_sql, params).await {
            Ok(results) => results,
            Err(response) => return Some(response),
        };
    let mut groups = BTreeMap::new();
    let columns = match accumulate_fanout_group_avg(
        &mut groups,
        local_results,
        &plan.group_indices,
        plan.sum_index,
        plan.count_index,
    ) {
        Ok(columns) => columns,
        Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
    };

    for owner in owners {
        let owner_results = match query_remote_prepared_sql_shard_owner(
            state,
            context,
            &plan.rewritten_sql,
            payload,
            &owner,
        )
        .await
        {
            Ok(results) => results,
            Err((status, message)) => return Some(json_error(status, message)),
        };
        let owner_columns = match accumulate_fanout_group_avg(
            &mut groups,
            owner_results,
            &plan.group_indices,
            plan.sum_index,
            plan.count_index,
        ) {
            Ok(columns) => columns,
            Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
        };
        if owner_columns != columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard group avg fan-out column mismatch: expected {:?}, got {:?}",
                    columns, owner_columns
                ),
            ));
        }
    }

    let mut rows = group_avg_rows(groups, plan.avg_output_index, plan.output_columns.len());
    if let Some(spec) = &plan.post_merge {
        crate::execution::apply_grouped_order_limit(&mut rows, spec);
    }
    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns: plan.output_columns.clone(),
        rows,
    }]))
}

async fn try_fanout_group_multi_aggregate_execute_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    record: &PreparedStatementRecord,
    payload: &ExecuteRequest,
    params: &[Value],
    return_results: bool,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled || !return_results {
        return None;
    }
    let Some(plan) =
        Executor::shard_group_multi_aggregate_select_fanout_plan_for_statements(&record.statements)
    else {
        return None;
    };
    let owners = match state
        .executor
        .shard_group_multi_aggregate_select_fanout_owners_for_statements(&record.statements, params)
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!(
                    "Shard group multi-aggregate fan-out planning error: {:?}",
                    e
                ),
            ));
        }
    };

    let local_results = match &plan.post_merge {
        Some(spec) => execute_sql_locally_for_fanout(state, &spec.per_owner_sql, params).await,
        None => execute_prepared_locally_for_fanout(state, record, params).await,
    };
    let local_results = match local_results {
        Ok(results) => results,
        Err(response) => return Some(response),
    };
    let mut groups = BTreeMap::new();
    let columns = match accumulate_fanout_group_multi_aggregates(
        &mut groups,
        local_results,
        &plan.group_indices,
        &plan.aggregates,
    ) {
        Ok(columns) => columns,
        Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
    };

    for owner in owners {
        let owner_results = match &plan.post_merge {
            Some(spec) => {
                query_remote_prepared_sql_shard_owner(
                    state,
                    context,
                    &spec.per_owner_sql,
                    payload,
                    &owner,
                )
                .await
            }
            None => {
                query_remote_prepared_shard_owner(state, context, record, payload, &owner).await
            }
        };
        let owner_results = match owner_results {
            Ok(results) => results,
            Err((status, message)) => return Some(json_error(status, message)),
        };
        let owner_columns = match accumulate_fanout_group_multi_aggregates(
            &mut groups,
            owner_results,
            &plan.group_indices,
            &plan.aggregates,
        ) {
            Ok(columns) => columns,
            Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
        };
        if owner_columns != columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard group multi-aggregate fan-out column mismatch: expected {:?}, got {:?}",
                    columns, owner_columns
                ),
            ));
        }
    }

    let mut rows = group_multi_aggregate_rows(groups, &plan.group_indices, &plan.aggregates);
    if let Some(spec) = &plan.post_merge {
        crate::execution::apply_grouped_order_limit(&mut rows, spec);
    }
    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns,
        rows,
    }]))
}

async fn try_unsupported_group_by_fanout_execute_error(
    state: &AppState,
    context: &RequestContext,
    record: &PreparedStatementRecord,
    params: &[Value],
    return_results: bool,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled || !return_results {
        return None;
    }
    match state
        .executor
        .shard_unsupported_group_by_fanout_error_for_statements(&record.statements, params)
        .await
    {
        Ok(Some(message)) => Some(json_error(StatusCode::BAD_REQUEST, message)),
        Ok(None) => None,
        Err(e) => Some(json_error(
            StatusCode::BAD_REQUEST,
            format!("Shard group by fan-out planning error: {:?}", e),
        )),
    }
}

async fn try_fanout_count_distinct_execute_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    record: &PreparedStatementRecord,
    payload: &ExecuteRequest,
    params: &[Value],
    return_results: bool,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled || !return_results {
        return None;
    }
    let Some(plan) =
        Executor::shard_count_distinct_select_fanout_plan_for_statements(&record.statements)
    else {
        return None;
    };
    let owners = match state
        .executor
        .shard_count_distinct_select_fanout_owners_for_statements(&record.statements, params)
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard count distinct fan-out planning error: {:?}", e),
            ));
        }
    };

    let local_results =
        match execute_sql_locally_for_fanout(state, &plan.rewritten_sql, params).await {
            Ok(results) => results,
            Err(response) => return Some(response),
        };
    let (rewrite_columns, local_values) =
        match fanout_distinct_values_from_select_results(local_results) {
            Ok(values) => values,
            Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
        };
    let mut distinct_values = BTreeSet::new();
    if let Err(message) = add_fanout_distinct_values(&mut distinct_values, local_values) {
        return Some(json_error(StatusCode::BAD_GATEWAY, message));
    }

    for owner in owners {
        let owner_results = match query_remote_prepared_sql_shard_owner(
            state,
            context,
            &plan.rewritten_sql,
            payload,
            &owner,
        )
        .await
        {
            Ok(results) => results,
            Err((status, message)) => return Some(json_error(status, message)),
        };
        let (owner_columns, owner_values) =
            match fanout_distinct_values_from_select_results(owner_results) {
                Ok(values) => values,
                Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
            };
        if owner_columns != rewrite_columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard count distinct fan-out column mismatch: expected {:?}, got {:?}",
                    rewrite_columns, owner_columns
                ),
            ));
        }
        if let Err(message) = add_fanout_distinct_values(&mut distinct_values, owner_values) {
            return Some(json_error(StatusCode::BAD_GATEWAY, message));
        }
    }

    let count = match i64::try_from(distinct_values.len()) {
        Ok(count) => count,
        Err(_) => {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                "Shard count distinct fan-out overflow",
            ));
        }
    };
    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns: vec![plan.output_column],
        rows: vec![vec![serde_json::json!(count)]],
    }]))
}

async fn try_fanout_sum_execute_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    record: &PreparedStatementRecord,
    payload: &ExecuteRequest,
    params: &[Value],
    return_results: bool,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled || !return_results {
        return None;
    }
    let owners = match state
        .executor
        .shard_sum_select_fanout_owners_for_statements(&record.statements, params)
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard sum fan-out planning error: {:?}", e),
            ));
        }
    };

    let local_results = match execute_prepared_locally_for_fanout(state, record, params).await {
        Ok(results) => results,
        Err(response) => return Some(response),
    };
    let (columns, mut total) = match fanout_sum_from_select_results(local_results) {
        Ok(sum) => sum,
        Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
    };

    for owner in owners {
        let owner_results = match query_remote_prepared_shard_owner(
            state, context, record, payload, &owner,
        )
        .await
        {
            Ok(results) => results,
            Err((status, message)) => return Some(json_error(status, message)),
        };
        let (owner_columns, owner_sum) = match fanout_sum_from_select_results(owner_results) {
            Ok(sum) => sum,
            Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
        };
        if owner_columns != columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard sum fan-out column mismatch: expected {:?}, got {:?}",
                    columns, owner_columns
                ),
            ));
        }
        if let Err(message) = add_fanout_sum(&mut total, owner_sum) {
            return Some(json_error(StatusCode::BAD_GATEWAY, message));
        }
    }

    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns,
        rows: vec![vec![fanout_sum_to_json(total)]],
    }]))
}

async fn try_fanout_min_max_execute_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    record: &PreparedStatementRecord,
    payload: &ExecuteRequest,
    params: &[Value],
    return_results: bool,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled || !return_results {
        return None;
    }
    let Some(kind) = Executor::shard_min_max_select_fanout_kind_for_statements(&record.statements)
    else {
        return None;
    };
    let owners = match state
        .executor
        .shard_min_max_select_fanout_owners_for_statements(&record.statements, params)
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard min/max fan-out planning error: {:?}", e),
            ));
        }
    };

    let local_results = match execute_prepared_locally_for_fanout(state, record, params).await {
        Ok(results) => results,
        Err(response) => return Some(response),
    };
    let (columns, mut total) = match fanout_extremum_from_select_results(local_results) {
        Ok(extremum) => extremum,
        Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
    };

    for owner in owners {
        let owner_results = match query_remote_prepared_shard_owner(
            state, context, record, payload, &owner,
        )
        .await
        {
            Ok(results) => results,
            Err((status, message)) => return Some(json_error(status, message)),
        };
        let (owner_columns, owner_extremum) =
            match fanout_extremum_from_select_results(owner_results) {
                Ok(extremum) => extremum,
                Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
            };
        if owner_columns != columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard min/max fan-out column mismatch: expected {:?}, got {:?}",
                    columns, owner_columns
                ),
            ));
        }
        if let Err(message) = merge_fanout_extremum(&mut total, owner_extremum, kind) {
            return Some(json_error(StatusCode::BAD_GATEWAY, message));
        }
    }

    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns,
        rows: vec![vec![total.unwrap_or(serde_json::Value::Null)]],
    }]))
}

async fn try_fanout_avg_execute_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    record: &PreparedStatementRecord,
    payload: &ExecuteRequest,
    params: &[Value],
    return_results: bool,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled || !return_results {
        return None;
    }
    let Some(plan) = Executor::shard_avg_select_fanout_plan_for_statements(&record.statements)
    else {
        return None;
    };
    let owners = match state
        .executor
        .shard_avg_select_fanout_owners_for_statements(&record.statements, params)
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard avg fan-out planning error: {:?}", e),
            ));
        }
    };

    let local_results =
        match execute_sql_locally_for_fanout(state, &plan.rewritten_sql, params).await {
            Ok(results) => results,
            Err(response) => return Some(response),
        };
    let (rewrite_columns, mut total_sum, mut total_count) =
        match fanout_avg_parts_from_select_results(local_results) {
            Ok(parts) => parts,
            Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
        };

    for owner in owners {
        let owner_results = match query_remote_prepared_sql_shard_owner(
            state,
            context,
            &plan.rewritten_sql,
            payload,
            &owner,
        )
        .await
        {
            Ok(results) => results,
            Err((status, message)) => return Some(json_error(status, message)),
        };
        let (owner_columns, owner_sum, owner_count) =
            match fanout_avg_parts_from_select_results(owner_results) {
                Ok(parts) => parts,
                Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
            };
        if owner_columns != rewrite_columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard avg fan-out column mismatch: expected {:?}, got {:?}",
                    rewrite_columns, owner_columns
                ),
            ));
        }
        if let Err(message) = add_fanout_sum(&mut total_sum, owner_sum) {
            return Some(json_error(StatusCode::BAD_GATEWAY, message));
        }
        total_count = match total_count.checked_add(owner_count) {
            Some(total) => total,
            None => {
                return Some(json_error(
                    StatusCode::BAD_GATEWAY,
                    "Shard avg fan-out count overflow",
                ));
            }
        };
    }

    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns: vec![plan.output_column],
        rows: vec![vec![fanout_avg_to_json(total_sum, total_count)]],
    }]))
}

async fn try_fanout_sum_distinct_execute_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    record: &PreparedStatementRecord,
    payload: &ExecuteRequest,
    params: &[Value],
    return_results: bool,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled || !return_results {
        return None;
    }
    let Some(plan) =
        Executor::shard_sum_distinct_select_fanout_plan_for_statements(&record.statements)
    else {
        return None;
    };
    let owners = match state
        .executor
        .shard_sum_distinct_select_fanout_owners_for_statements(&record.statements, params)
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard sum distinct fan-out planning error: {:?}", e),
            ));
        }
    };

    let local_results =
        match execute_sql_locally_for_fanout(state, &plan.rewritten_sql, params).await {
            Ok(results) => results,
            Err(response) => return Some(response),
        };
    let mut distinct_values = BTreeMap::new();
    let rewrite_columns =
        match collect_fanout_distinct_value_map(&mut distinct_values, local_results) {
            Ok(columns) => columns,
            Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
        };

    for owner in owners {
        let owner_results = match query_remote_prepared_sql_shard_owner(
            state,
            context,
            &plan.rewritten_sql,
            payload,
            &owner,
        )
        .await
        {
            Ok(results) => results,
            Err((status, message)) => return Some(json_error(status, message)),
        };
        let owner_columns =
            match collect_fanout_distinct_value_map(&mut distinct_values, owner_results) {
                Ok(columns) => columns,
                Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
            };
        if owner_columns != rewrite_columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard sum distinct fan-out column mismatch: expected {:?}, got {:?}",
                    rewrite_columns, owner_columns
                ),
            ));
        }
    }

    let total = match fanout_sum_over_distinct_values(&distinct_values) {
        Ok(total) => total,
        Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
    };
    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns: vec![plan.output_column],
        rows: vec![vec![fanout_sum_to_json(total)]],
    }]))
}

async fn try_fanout_avg_distinct_execute_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    record: &PreparedStatementRecord,
    payload: &ExecuteRequest,
    params: &[Value],
    return_results: bool,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled || !return_results {
        return None;
    }
    let Some(plan) =
        Executor::shard_avg_distinct_select_fanout_plan_for_statements(&record.statements)
    else {
        return None;
    };
    let owners = match state
        .executor
        .shard_avg_distinct_select_fanout_owners_for_statements(&record.statements, params)
        .await
    {
        Ok(owners) if !owners.is_empty() => owners,
        Ok(_) => return None,
        Err(e) => {
            return Some(json_error(
                StatusCode::BAD_REQUEST,
                format!("Shard avg distinct fan-out planning error: {:?}", e),
            ));
        }
    };

    let local_results =
        match execute_sql_locally_for_fanout(state, &plan.rewritten_sql, params).await {
            Ok(results) => results,
            Err(response) => return Some(response),
        };
    let mut distinct_values = BTreeMap::new();
    let rewrite_columns =
        match collect_fanout_distinct_value_map(&mut distinct_values, local_results) {
            Ok(columns) => columns,
            Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
        };

    for owner in owners {
        let owner_results = match query_remote_prepared_sql_shard_owner(
            state,
            context,
            &plan.rewritten_sql,
            payload,
            &owner,
        )
        .await
        {
            Ok(results) => results,
            Err((status, message)) => return Some(json_error(status, message)),
        };
        let owner_columns =
            match collect_fanout_distinct_value_map(&mut distinct_values, owner_results) {
                Ok(columns) => columns,
                Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
            };
        if owner_columns != rewrite_columns {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard avg distinct fan-out column mismatch: expected {:?}, got {:?}",
                    rewrite_columns, owner_columns
                ),
            ));
        }
    }

    let total_sum = match fanout_sum_over_distinct_values(&distinct_values) {
        Ok(total) => total,
        Err(message) => return Some(json_error(StatusCode::BAD_GATEWAY, message)),
    };
    let total_count = match i64::try_from(distinct_values.len()) {
        Ok(count) => count,
        Err(_) => {
            return Some(json_error(
                StatusCode::BAD_GATEWAY,
                "Shard avg distinct fan-out overflow",
            ));
        }
    };
    Some(json_ok(vec![QueryResultJson::Select {
        r#type: "select".to_string(),
        columns: vec![plan.output_column],
        rows: vec![vec![fanout_avg_to_json(total_sum, total_count)]],
    }]))
}

async fn try_fanout_execute_to_shard_owners(
    state: &AppState,
    context: &RequestContext,
    record: &PreparedStatementRecord,
    payload: &ExecuteRequest,
    params: &[Value],
    return_results: bool,
) -> Option<ApiResponse<Vec<QueryResultJson>>> {
    if context.shard_forwarded || !state.shard_owner_forwarding_enabled || !return_results {
        return None;
    }
    let owners = match state
        .executor
        .shard_select_fanout_owners_for_statements(&record.statements, params)
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

    let local_results = match execute_prepared_locally_for_fanout(state, record, params).await {
        Ok(results) => results,
        Err(response) => return Some(response),
    };

    let mut columns = None;
    let mut rows = Vec::new();
    if let Err(message) = append_fanout_select_results(&mut columns, &mut rows, local_results) {
        return Some(json_error(StatusCode::BAD_GATEWAY, message));
    }

    for owner in owners {
        let owner_results = match query_remote_prepared_shard_owner(
            state, context, record, payload, &owner,
        )
        .await
        {
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

async fn execute_prepared_locally_for_fanout(
    state: &AppState,
    record: &PreparedStatementRecord,
    params: &[Value],
) -> std::result::Result<Vec<QueryResultJson>, ApiResponse<Vec<QueryResultJson>>> {
    execute_statements_locally_for_fanout(state, &record.statements, params).await
}

async fn execute_sql_locally_for_fanout(
    state: &AppState,
    sql: &str,
    params: &[Value],
) -> std::result::Result<Vec<QueryResultJson>, ApiResponse<Vec<QueryResultJson>>> {
    match state.executor.prepare(sql) {
        Ok(statements) => execute_statements_locally_for_fanout(state, &statements, params).await,
        Err(e) => Err(json_error(
            StatusCode::BAD_REQUEST,
            format!("Statement Error: {:?}", e),
        )),
    }
}

async fn execute_statements_locally_for_fanout(
    state: &AppState,
    statements: &[sqlparser::ast::Statement],
    params: &[Value],
) -> std::result::Result<Vec<QueryResultJson>, ApiResponse<Vec<QueryResultJson>>> {
    match state.storage.begin_transaction().await {
        Ok(mut txn) => {
            let mut results = Vec::new();
            for stmt in statements {
                match state
                    .executor
                    .execute_in_transaction_with_params(stmt, &mut *txn, params)
                    .await
                {
                    Ok(result) => results.push(result.into()),
                    Err(e) => {
                        let _ = txn.rollback().await;
                        return Err(json_error(
                            StatusCode::BAD_REQUEST,
                            format!("Execution Error: {:?}", e),
                        ));
                    }
                }
            }
            if let Err(e) = txn.commit().await {
                return Err(json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Commit Error: {:?}", e),
                ));
            }
            Ok(results)
        }
        Err(e) => Err(json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Transaction Error: {:?}", e),
        )),
    }
}

async fn query_remote_prepared_shard_owner(
    state: &AppState,
    context: &RequestContext,
    record: &PreparedStatementRecord,
    payload: &ExecuteRequest,
    owner: &SqlShardOwner,
) -> std::result::Result<Vec<QueryResultJson>, (StatusCode, String)> {
    query_remote_prepared_sql_shard_owner(state, context, &record.sql, payload, owner).await
}

async fn query_remote_prepared_sql_shard_owner(
    state: &AppState,
    context: &RequestContext,
    sql: &str,
    payload: &ExecuteRequest,
    owner: &SqlShardOwner,
) -> std::result::Result<Vec<QueryResultJson>, (StatusCode, String)> {
    let prepare_url = format!("{}://{}/prepare", state.peer_scheme, owner.addr);
    let prepare_payload = PrepareRequest {
        sql: sql.to_string(),
    };
    let prepare_response = apply_forwarding_headers(
        state.raft_client.post(&prepare_url).json(&prepare_payload),
        context,
    )
    .send()
    .await
    .map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            format!(
                "Shard select fan-out prepare error: forwarding to node {} at {} failed: {}",
                owner.node_id, owner.addr, e
            ),
        )
    })?;
    let prepare_status = prepare_response.status();
    let prepare_envelope = prepare_response
        .json::<Envelope<PreparedStatementInfo>>()
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard select fan-out prepare error: response from node {} at {} could not be decoded: {}",
                    owner.node_id, owner.addr, e
                ),
            )
        })?;
    if !prepare_status.is_success() || prepare_envelope.status != "ok" {
        return Err((
            prepare_status,
            format!(
                "Shard select fan-out prepare error: node {} at {} rejected query: {}",
                owner.node_id,
                owner.addr,
                prepare_envelope
                    .error
                    .unwrap_or_else(|| "owner node rejected prepare".to_string())
            ),
        ));
    }
    let prepared = prepare_envelope.data.ok_or_else(|| {
        (
            StatusCode::BAD_GATEWAY,
            format!(
                "Shard select fan-out prepare error: node {} at {} returned no prepared statement",
                owner.node_id, owner.addr
            ),
        )
    })?;

    let execute_url = format!("{}://{}/execute", state.peer_scheme, owner.addr);
    let execute_payload = ExecuteRequest {
        statement_id: prepared.statement_id.clone(),
        params: payload.params.clone(),
        return_results: Some(true),
    };
    let execute_response = match apply_forwarding_headers(
        state.raft_client.post(&execute_url).json(&execute_payload),
        context,
    )
    .send()
    .await
    {
        Ok(response) => response,
        Err(e) => {
            best_effort_deallocate_statement_on_owner(state, context, owner, &prepared).await;
            return Err((
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard select fan-out execute error: forwarding to node {} at {} failed: {}",
                    owner.node_id, owner.addr, e
                ),
            ));
        }
    };
    let execute_status = execute_response.status();
    let execute_envelope = match execute_response
        .json::<Envelope<Vec<QueryResultJson>>>()
        .await
    {
        Ok(envelope) => envelope,
        Err(e) => {
            best_effort_deallocate_statement_on_owner(state, context, owner, &prepared).await;
            return Err((
                StatusCode::BAD_GATEWAY,
                format!(
                    "Shard select fan-out execute error: response from node {} at {} could not be decoded: {}",
                    owner.node_id, owner.addr, e
                ),
            ));
        }
    };
    best_effort_deallocate_statement_on_owner(state, context, owner, &prepared).await;

    if !execute_status.is_success() || execute_envelope.status != "ok" {
        return Err((
            execute_status,
            format!(
                "Shard select fan-out execute error: node {} at {} rejected query: {}",
                owner.node_id,
                owner.addr,
                execute_envelope
                    .error
                    .unwrap_or_else(|| "owner node rejected execute".to_string())
            ),
        ));
    }
    execute_envelope.data.ok_or_else(|| {
        (
            StatusCode::BAD_GATEWAY,
            format!(
                "Shard select fan-out execute error: node {} at {} returned no query data",
                owner.node_id, owner.addr
            ),
        )
    })
}

async fn forward_execute_to_shard_owner(
    state: &AppState,
    context: &RequestContext,
    record: &PreparedStatementRecord,
    payload: &ExecuteRequest,
    decision: &SqlShardRoutingDecision,
) -> ApiResponse<Vec<QueryResultJson>> {
    let prepare_url = format!(
        "{}://{}/prepare",
        state.peer_scheme, decision.route.owner_addr
    );
    let prepare_payload = PrepareRequest {
        sql: record.sql.clone(),
    };
    let prepare_response = match apply_forwarding_headers(
        state.raft_client.post(&prepare_url).json(&prepare_payload),
        context,
    )
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

    let execute_url = format!(
        "{}://{}/execute",
        state.peer_scheme, decision.route.owner_addr
    );
    let execute_payload = ExecuteRequest {
        statement_id: prepared.statement_id.clone(),
        params: payload.params.clone(),
        return_results: payload.return_results,
    };
    let execute_response = match apply_forwarding_headers(
        state.raft_client.post(&execute_url).json(&execute_payload),
        context,
    )
    .send()
    .await
    {
        Ok(response) => response,
        Err(e) => {
            best_effort_deallocate_forwarded_statement(state, context, decision, &prepared).await;
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
    let username = context.username.as_deref().unwrap_or("postgres");
    if let Some(auth) = context.forwarding_auth.as_ref() {
        auth.apply(request, username)
    } else if context.legacy_unsafe {
        request
            .header(FORWARDED_HEADER, FORWARDED_VALUE)
            .header(FORWARDED_USER_HEADER, username)
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
        "{}://{}/prepare/{}",
        state.peer_scheme, decision.route.owner_addr, prepared.statement_id
    );
    let _ = apply_forwarding_headers(state.raft_client.delete(url), context)
        .send()
        .await;
}

async fn best_effort_deallocate_statement_on_owner(
    state: &AppState,
    context: &RequestContext,
    owner: &SqlShardOwner,
    prepared: &PreparedStatementInfo,
) {
    let url = format!(
        "{}://{}/prepare/{}",
        state.peer_scheme, owner.addr, prepared.statement_id
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
    postgres_password: Arc<str>,
    http_legacy_unsafe: bool,
    forwarding_auth: Option<ForwardingAuth>,
    peer_scheme: String,
}

#[derive(Clone)]
struct RequestContext {
    username: Option<String>,
    shard_forwarded: bool,
    auth_mode: &'static str,
    forwarding_auth: Option<ForwardingAuth>,
    legacy_unsafe: bool,
}

impl Default for RequestContext {
    fn default() -> Self {
        Self {
            username: None,
            shard_forwarded: false,
            auth_mode: "legacy_unsafe",
            forwarding_auth: None,
            legacy_unsafe: true,
        }
    }
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
    query_result_cache_eligible_count: u64,
    query_result_cache_hit_count: u64,
    query_result_cache_miss_count: u64,
    query_result_cache_stale_count: u64,
    query_result_cache_insert_count: u64,
    query_result_cache_invalidation_count: u64,
    block_cache_hit_count: u64,
    block_cache_miss_count: u64,
    block_cache_insert_count: u64,
    block_cache_insert_bytes: u64,
    block_cache_fill_skip_count: u64,
    block_cache_eviction_count: u64,
    block_cache_eviction_bytes: u64,
    sstable_block_file_open_count: u64,
    sstable_block_read_bytes: u64,
    sstable_open_count: u64,
    sstable_open_total_us: u64,
    sstable_open_index_bytes: u64,
    sstable_open_index_read_us: u64,
    sstable_open_index_decode_us: u64,
    sstable_open_filter_bytes: u64,
    sstable_open_filter_read_us: u64,
    sstable_open_filter_decode_us: u64,
    sstable_open_meta_bytes: u64,
    sstable_open_meta_read_us: u64,
    sstable_open_meta_decode_us: u64,
    sstable_open_index_entries: u64,
    sstable_open_block_property_count: u64,
    sstable_index_cache_hit_count: u64,
    sstable_index_cache_miss_count: u64,
    sstable_index_cache_stale_count: u64,
    sstable_index_cache_invalid_count: u64,
    sstable_index_cache_write_count: u64,
    sstable_index_cache_write_error_count: u64,
    sstable_prefix_filter_check_count: u64,
    sstable_prefix_filter_positive_count: u64,
    sstable_prefix_filter_skip_count: u64,
    sstable_prefix_filter_fail_open_count: u64,
    sstable_index_prefix_filter_check_count: u64,
    sstable_index_prefix_filter_positive_count: u64,
    sstable_index_prefix_filter_skip_count: u64,
    sstable_index_prefix_filter_fail_open_count: u64,
    sstable_user_key_filter_check_count: u64,
    sstable_user_key_filter_positive_count: u64,
    sstable_user_key_filter_skip_count: u64,
    sstable_user_key_filter_fail_open_count: u64,
    sstable_block_prefix_filter_check_count: u64,
    sstable_block_prefix_filter_positive_count: u64,
    sstable_block_prefix_filter_skip_count: u64,
    sstable_block_prefix_filter_fail_open_count: u64,
    sstable_block_index_prefix_filter_check_count: u64,
    sstable_block_index_prefix_filter_positive_count: u64,
    sstable_block_index_prefix_filter_skip_count: u64,
    sstable_block_index_prefix_filter_fail_open_count: u64,
    sstable_block_zone_map_filter_check_count: u64,
    sstable_block_zone_map_filter_positive_count: u64,
    sstable_block_zone_map_filter_skip_count: u64,
    sstable_block_zone_map_filter_fail_open_count: u64,
    sstable_block_zone_map_metadata_bytes: u64,
    sstable_block_zone_map_mvcc_overlap_fail_open_count: u64,
    sstable_block_zone_map_mvcc_boundary_split_fail_open_count: u64,
    sstable_block_zone_map_mvcc_write_buffer_overlap_fail_open_count: u64,
    sstable_block_zone_map_mvcc_memtable_overlap_fail_open_count: u64,
    sstable_block_zone_map_mvcc_sstable_overlap_fail_open_count: u64,
    sstable_block_zone_map_schema_fail_open_count: u64,
    sstable_point_probe_count: u64,
    sstable_point_overlap_skip_count: u64,
    sstable_range_probe_count: u64,
    sstable_range_overlap_skip_count: u64,
    sstable_iterator_open_count: u64,
    columnar_single_source_aggregate_fast_path_count: u64,
    sstable_reverse_iterator_open_count: u64,
    sstable_reverse_block_read_count: u64,
    sstable_reverse_block_entry_decode_count: u64,
    sstable_reverse_block_entry_yield_count: u64,
    sstable_reverse_block_span_scan_count: u64,
    sstable_reverse_block_span_scan_entry_count: u64,
    sstable_reverse_block_span_materialize_entry_count: u64,
    sstable_reverse_seek_sidecar_hit_count: u64,
    sstable_reverse_seek_sidecar_miss_count: u64,
    sstable_reverse_seek_sidecar_stale_count: u64,
    sstable_reverse_seek_sidecar_invalid_count: u64,
    sstable_reverse_seek_sidecar_write_count: u64,
    sstable_reverse_seek_sidecar_write_error_count: u64,
    sstable_reverse_seek_sidecar_use_count: u64,
    sstable_reverse_seek_sidecar_fail_open_count: u64,
    sstable_reverse_seek_sidecar_index_entry_count: u64,
    sstable_reverse_seek_sidecar_entry_materialize_count: u64,
    sstable_reverse_seek_sidecar_offset_probe_count: u64,
    fusion_reverse_scan_count: u64,
    fusion_reverse_source_open_count: u64,
    fusion_reverse_sstable_frontier_probe_count: u64,
    fusion_reverse_sstable_frontier_in_range_count: u64,
    fusion_reverse_sstable_frontier_file_count: u64,
    fusion_reverse_sstable_frontier_tighten_count: u64,
    fusion_reverse_sstable_frontier_empty_skip_count: u64,
    fusion_reverse_sstable_frontier_fail_open_count: u64,
    fusion_reverse_sstable_pending_count: u64,
    fusion_reverse_sstable_activation_count: u64,
    fusion_reverse_sstable_deferred_unopened_count: u64,
    fusion_reverse_sstable_activation_equal_frontier_count: u64,
    fusion_reverse_raw_entry_read_count: u64,
    fusion_reverse_visible_candidate_count: u64,
    fusion_reverse_visible_put_count: u64,
    index_key_stream_entry_visit_count: u64,
    index_ordered_topk_scan_count: u64,
    index_ordered_topk_entry_visit_count: u64,
    index_ordered_topk_reverse_scan_count: u64,
    index_ordered_topk_index_only_row_count: u64,
    index_ordered_topk_base_row_fetch_count: u64,
    index_group_count_summary_entry_visit_count: u64,
    index_loose_seek_count: u64,
    index_loose_value_count: u64,
    index_loose_run_skip_count: u64,
    compaction_run_count: u64,
    compaction_input_bytes: u64,
    compaction_output_bytes: u64,
    compaction_dropped_version_count: u64,
    live_sstable_count: u64,
    sstable_manifest_load_count: u64,
    sstable_manifest_load_total_us: u64,
    sstable_manifest_load_error_count: u64,
    sstable_manifest_live_file_count: u64,
    sstable_manifest_legacy_scan_count: u64,
    sstable_manifest_legacy_scan_candidate_count: u64,
    sstable_manifest_open_error_count: u64,
    row_write_count: u64,
    fts_search_count: u64,
    fts_doc_hits: u64,
    wal_write_count: u64,
    wal_write_bytes: u64,
    wal_replay_count: u64,
    wal_replay_total_us: u64,
    wal_replay_segment_count: u64,
    wal_replay_bytes: u64,
    wal_replay_valid_bytes: u64,
    wal_replay_last_segment_id: u64,
    wal_replay_last_valid_offset: u64,
    wal_replay_entry_count: u64,
    wal_replay_put_count: u64,
    wal_replay_delete_count: u64,
    wal_replay_partial_tail_count: u64,
    wal_replay_truncate_count: u64,
    wal_replay_error_count: u64,
    wal_replay_apply_count: u64,
    wal_replay_apply_total_us: u64,
    wal_replay_max_ts: u64,
    query_count: u64,
    slow_query_count: u64,
    query_total_us: u64,
    query_sort_fallback_count: u64,
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

impl From<ReplicatedQueryResult> for QueryResultJson {
    fn from(result: ReplicatedQueryResult) -> Self {
        match result {
            ReplicatedQueryResult::Select { columns, rows } => QueryResultJson::Select {
                r#type: "select".to_string(),
                columns,
                rows: rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|value| replicated_value_to_common(value).to_json())
                            .collect()
                    })
                    .collect(),
            },
            ReplicatedQueryResult::Success { message } => QueryResultJson::Success {
                r#type: "success".to_string(),
                message,
            },
        }
    }
}

fn replicated_value_to_common(value: ReplicatedValue) -> crate::common::Value {
    match value {
        ReplicatedValue::Null => crate::common::Value::Null,
        ReplicatedValue::Boolean(value) => crate::common::Value::Boolean(value),
        ReplicatedValue::Integer(value) => crate::common::Value::Integer(value),
        ReplicatedValue::FloatBits(value) => crate::common::Value::Float(f64::from_bits(value)),
        ReplicatedValue::Decimal(value) => crate::common::Value::Decimal(value),
        ReplicatedValue::String(value) => crate::common::Value::String(value),
        ReplicatedValue::Date(value) => crate::common::Value::Date(value),
        ReplicatedValue::Timestamp(value) => crate::common::Value::Timestamp(value),
        ReplicatedValue::Interval(value) => crate::common::Value::Interval(value),
        ReplicatedValue::Blob(value) => crate::common::Value::Blob(value),
        ReplicatedValue::VectorBits(value) => {
            crate::common::Value::Vector(value.into_iter().map(f32::from_bits).collect())
        }
        ReplicatedValue::Array(values) => crate::common::Value::Array(
            values.into_iter().map(replicated_value_to_common).collect(),
        ),
        ReplicatedValue::Object(values) => crate::common::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, replicated_value_to_common(value)))
                .collect(),
        ),
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
            mode: context.auth_mode.to_string(),
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
            query_result_cache_eligible_count: metrics
                .query_result_cache_eligible_count
                .load(Relaxed),
            query_result_cache_hit_count: metrics.query_result_cache_hit_count.load(Relaxed),
            query_result_cache_miss_count: metrics.query_result_cache_miss_count.load(Relaxed),
            query_result_cache_stale_count: metrics.query_result_cache_stale_count.load(Relaxed),
            query_result_cache_insert_count: metrics.query_result_cache_insert_count.load(Relaxed),
            query_result_cache_invalidation_count: metrics
                .query_result_cache_invalidation_count
                .load(Relaxed),
            block_cache_hit_count: metrics.block_cache_hit_count.load(Relaxed),
            block_cache_miss_count: metrics.block_cache_miss_count.load(Relaxed),
            block_cache_insert_count: metrics.block_cache_insert_count.load(Relaxed),
            block_cache_insert_bytes: metrics.block_cache_insert_bytes.load(Relaxed),
            block_cache_fill_skip_count: metrics.block_cache_fill_skip_count.load(Relaxed),
            block_cache_eviction_count: metrics.block_cache_eviction_count.load(Relaxed),
            block_cache_eviction_bytes: metrics.block_cache_eviction_bytes.load(Relaxed),
            sstable_block_file_open_count: metrics.sstable_block_file_open_count.load(Relaxed),
            sstable_block_read_bytes: metrics.sstable_block_read_bytes.load(Relaxed),
            sstable_open_count: metrics.sstable_open_count.load(Relaxed),
            sstable_open_total_us: metrics.sstable_open_total_us.load(Relaxed),
            sstable_open_index_bytes: metrics.sstable_open_index_bytes.load(Relaxed),
            sstable_open_index_read_us: metrics.sstable_open_index_read_us.load(Relaxed),
            sstable_open_index_decode_us: metrics.sstable_open_index_decode_us.load(Relaxed),
            sstable_open_filter_bytes: metrics.sstable_open_filter_bytes.load(Relaxed),
            sstable_open_filter_read_us: metrics.sstable_open_filter_read_us.load(Relaxed),
            sstable_open_filter_decode_us: metrics.sstable_open_filter_decode_us.load(Relaxed),
            sstable_open_meta_bytes: metrics.sstable_open_meta_bytes.load(Relaxed),
            sstable_open_meta_read_us: metrics.sstable_open_meta_read_us.load(Relaxed),
            sstable_open_meta_decode_us: metrics.sstable_open_meta_decode_us.load(Relaxed),
            sstable_open_index_entries: metrics.sstable_open_index_entries.load(Relaxed),
            sstable_open_block_property_count: metrics
                .sstable_open_block_property_count
                .load(Relaxed),
            sstable_index_cache_hit_count: metrics.sstable_index_cache_hit_count.load(Relaxed),
            sstable_index_cache_miss_count: metrics.sstable_index_cache_miss_count.load(Relaxed),
            sstable_index_cache_stale_count: metrics.sstable_index_cache_stale_count.load(Relaxed),
            sstable_index_cache_invalid_count: metrics
                .sstable_index_cache_invalid_count
                .load(Relaxed),
            sstable_index_cache_write_count: metrics.sstable_index_cache_write_count.load(Relaxed),
            sstable_index_cache_write_error_count: metrics
                .sstable_index_cache_write_error_count
                .load(Relaxed),
            sstable_prefix_filter_check_count: metrics
                .sstable_prefix_filter_check_count
                .load(Relaxed),
            sstable_prefix_filter_positive_count: metrics
                .sstable_prefix_filter_positive_count
                .load(Relaxed),
            sstable_prefix_filter_skip_count: metrics
                .sstable_prefix_filter_skip_count
                .load(Relaxed),
            sstable_prefix_filter_fail_open_count: metrics
                .sstable_prefix_filter_fail_open_count
                .load(Relaxed),
            sstable_index_prefix_filter_check_count: metrics
                .sstable_index_prefix_filter_check_count
                .load(Relaxed),
            sstable_index_prefix_filter_positive_count: metrics
                .sstable_index_prefix_filter_positive_count
                .load(Relaxed),
            sstable_index_prefix_filter_skip_count: metrics
                .sstable_index_prefix_filter_skip_count
                .load(Relaxed),
            sstable_index_prefix_filter_fail_open_count: metrics
                .sstable_index_prefix_filter_fail_open_count
                .load(Relaxed),
            sstable_user_key_filter_check_count: metrics
                .sstable_user_key_filter_check_count
                .load(Relaxed),
            sstable_user_key_filter_positive_count: metrics
                .sstable_user_key_filter_positive_count
                .load(Relaxed),
            sstable_user_key_filter_skip_count: metrics
                .sstable_user_key_filter_skip_count
                .load(Relaxed),
            sstable_user_key_filter_fail_open_count: metrics
                .sstable_user_key_filter_fail_open_count
                .load(Relaxed),
            sstable_block_prefix_filter_check_count: metrics
                .sstable_block_prefix_filter_check_count
                .load(Relaxed),
            sstable_block_prefix_filter_positive_count: metrics
                .sstable_block_prefix_filter_positive_count
                .load(Relaxed),
            sstable_block_prefix_filter_skip_count: metrics
                .sstable_block_prefix_filter_skip_count
                .load(Relaxed),
            sstable_block_prefix_filter_fail_open_count: metrics
                .sstable_block_prefix_filter_fail_open_count
                .load(Relaxed),
            sstable_block_index_prefix_filter_check_count: metrics
                .sstable_block_index_prefix_filter_check_count
                .load(Relaxed),
            sstable_block_index_prefix_filter_positive_count: metrics
                .sstable_block_index_prefix_filter_positive_count
                .load(Relaxed),
            sstable_block_index_prefix_filter_skip_count: metrics
                .sstable_block_index_prefix_filter_skip_count
                .load(Relaxed),
            sstable_block_index_prefix_filter_fail_open_count: metrics
                .sstable_block_index_prefix_filter_fail_open_count
                .load(Relaxed),
            sstable_block_zone_map_filter_check_count: metrics
                .sstable_block_zone_map_filter_check_count
                .load(Relaxed),
            sstable_block_zone_map_filter_positive_count: metrics
                .sstable_block_zone_map_filter_positive_count
                .load(Relaxed),
            sstable_block_zone_map_filter_skip_count: metrics
                .sstable_block_zone_map_filter_skip_count
                .load(Relaxed),
            sstable_block_zone_map_filter_fail_open_count: metrics
                .sstable_block_zone_map_filter_fail_open_count
                .load(Relaxed),
            sstable_block_zone_map_metadata_bytes: metrics
                .sstable_block_zone_map_metadata_bytes
                .load(Relaxed),
            sstable_block_zone_map_mvcc_overlap_fail_open_count: metrics
                .sstable_block_zone_map_mvcc_overlap_fail_open_count
                .load(Relaxed),
            sstable_block_zone_map_mvcc_boundary_split_fail_open_count: metrics
                .sstable_block_zone_map_mvcc_boundary_split_fail_open_count
                .load(Relaxed),
            sstable_block_zone_map_mvcc_write_buffer_overlap_fail_open_count: metrics
                .sstable_block_zone_map_mvcc_write_buffer_overlap_fail_open_count
                .load(Relaxed),
            sstable_block_zone_map_mvcc_memtable_overlap_fail_open_count: metrics
                .sstable_block_zone_map_mvcc_memtable_overlap_fail_open_count
                .load(Relaxed),
            sstable_block_zone_map_mvcc_sstable_overlap_fail_open_count: metrics
                .sstable_block_zone_map_mvcc_sstable_overlap_fail_open_count
                .load(Relaxed),
            sstable_block_zone_map_schema_fail_open_count: metrics
                .sstable_block_zone_map_schema_fail_open_count
                .load(Relaxed),
            sstable_point_probe_count: metrics.sstable_point_probe_count.load(Relaxed),
            sstable_point_overlap_skip_count: metrics
                .sstable_point_overlap_skip_count
                .load(Relaxed),
            sstable_range_probe_count: metrics.sstable_range_probe_count.load(Relaxed),
            sstable_range_overlap_skip_count: metrics
                .sstable_range_overlap_skip_count
                .load(Relaxed),
            sstable_iterator_open_count: metrics.sstable_iterator_open_count.load(Relaxed),
            columnar_single_source_aggregate_fast_path_count: metrics
                .columnar_single_source_aggregate_fast_path_count
                .load(Relaxed),
            sstable_reverse_iterator_open_count: metrics
                .sstable_reverse_iterator_open_count
                .load(Relaxed),
            sstable_reverse_block_read_count: metrics
                .sstable_reverse_block_read_count
                .load(Relaxed),
            sstable_reverse_block_entry_decode_count: metrics
                .sstable_reverse_block_entry_decode_count
                .load(Relaxed),
            sstable_reverse_block_entry_yield_count: metrics
                .sstable_reverse_block_entry_yield_count
                .load(Relaxed),
            sstable_reverse_block_span_scan_count: metrics
                .sstable_reverse_block_span_scan_count
                .load(Relaxed),
            sstable_reverse_block_span_scan_entry_count: metrics
                .sstable_reverse_block_span_scan_entry_count
                .load(Relaxed),
            sstable_reverse_block_span_materialize_entry_count: metrics
                .sstable_reverse_block_span_materialize_entry_count
                .load(Relaxed),
            sstable_reverse_seek_sidecar_hit_count: metrics
                .sstable_reverse_seek_sidecar_hit_count
                .load(Relaxed),
            sstable_reverse_seek_sidecar_miss_count: metrics
                .sstable_reverse_seek_sidecar_miss_count
                .load(Relaxed),
            sstable_reverse_seek_sidecar_stale_count: metrics
                .sstable_reverse_seek_sidecar_stale_count
                .load(Relaxed),
            sstable_reverse_seek_sidecar_invalid_count: metrics
                .sstable_reverse_seek_sidecar_invalid_count
                .load(Relaxed),
            sstable_reverse_seek_sidecar_write_count: metrics
                .sstable_reverse_seek_sidecar_write_count
                .load(Relaxed),
            sstable_reverse_seek_sidecar_write_error_count: metrics
                .sstable_reverse_seek_sidecar_write_error_count
                .load(Relaxed),
            sstable_reverse_seek_sidecar_use_count: metrics
                .sstable_reverse_seek_sidecar_use_count
                .load(Relaxed),
            sstable_reverse_seek_sidecar_fail_open_count: metrics
                .sstable_reverse_seek_sidecar_fail_open_count
                .load(Relaxed),
            sstable_reverse_seek_sidecar_index_entry_count: metrics
                .sstable_reverse_seek_sidecar_index_entry_count
                .load(Relaxed),
            sstable_reverse_seek_sidecar_entry_materialize_count: metrics
                .sstable_reverse_seek_sidecar_entry_materialize_count
                .load(Relaxed),
            sstable_reverse_seek_sidecar_offset_probe_count: metrics
                .sstable_reverse_seek_sidecar_offset_probe_count
                .load(Relaxed),
            fusion_reverse_scan_count: metrics.fusion_reverse_scan_count.load(Relaxed),
            fusion_reverse_source_open_count: metrics
                .fusion_reverse_source_open_count
                .load(Relaxed),
            fusion_reverse_sstable_frontier_probe_count: metrics
                .fusion_reverse_sstable_frontier_probe_count
                .load(Relaxed),
            fusion_reverse_sstable_frontier_in_range_count: metrics
                .fusion_reverse_sstable_frontier_in_range_count
                .load(Relaxed),
            fusion_reverse_sstable_frontier_file_count: metrics
                .fusion_reverse_sstable_frontier_file_count
                .load(Relaxed),
            fusion_reverse_sstable_frontier_tighten_count: metrics
                .fusion_reverse_sstable_frontier_tighten_count
                .load(Relaxed),
            fusion_reverse_sstable_frontier_empty_skip_count: metrics
                .fusion_reverse_sstable_frontier_empty_skip_count
                .load(Relaxed),
            fusion_reverse_sstable_frontier_fail_open_count: metrics
                .fusion_reverse_sstable_frontier_fail_open_count
                .load(Relaxed),
            fusion_reverse_sstable_pending_count: metrics
                .fusion_reverse_sstable_pending_count
                .load(Relaxed),
            fusion_reverse_sstable_activation_count: metrics
                .fusion_reverse_sstable_activation_count
                .load(Relaxed),
            fusion_reverse_sstable_deferred_unopened_count: metrics
                .fusion_reverse_sstable_deferred_unopened_count
                .load(Relaxed),
            fusion_reverse_sstable_activation_equal_frontier_count: metrics
                .fusion_reverse_sstable_activation_equal_frontier_count
                .load(Relaxed),
            fusion_reverse_raw_entry_read_count: metrics
                .fusion_reverse_raw_entry_read_count
                .load(Relaxed),
            fusion_reverse_visible_candidate_count: metrics
                .fusion_reverse_visible_candidate_count
                .load(Relaxed),
            fusion_reverse_visible_put_count: metrics
                .fusion_reverse_visible_put_count
                .load(Relaxed),
            index_key_stream_entry_visit_count: metrics
                .index_key_stream_entry_visit_count
                .load(Relaxed),
            index_ordered_topk_scan_count: metrics.index_ordered_topk_scan_count.load(Relaxed),
            index_ordered_topk_entry_visit_count: metrics
                .index_ordered_topk_entry_visit_count
                .load(Relaxed),
            index_ordered_topk_reverse_scan_count: metrics
                .index_ordered_topk_reverse_scan_count
                .load(Relaxed),
            index_ordered_topk_index_only_row_count: metrics
                .index_ordered_topk_index_only_row_count
                .load(Relaxed),
            index_ordered_topk_base_row_fetch_count: metrics
                .index_ordered_topk_base_row_fetch_count
                .load(Relaxed),
            index_group_count_summary_entry_visit_count: metrics
                .index_group_count_summary_entry_visit_count
                .load(Relaxed),
            index_loose_seek_count: metrics.index_loose_seek_count.load(Relaxed),
            index_loose_value_count: metrics.index_loose_value_count.load(Relaxed),
            index_loose_run_skip_count: metrics.index_loose_run_skip_count.load(Relaxed),
            compaction_run_count: metrics.compaction_run_count.load(Relaxed),
            compaction_input_bytes: metrics.compaction_input_bytes.load(Relaxed),
            compaction_output_bytes: metrics.compaction_output_bytes.load(Relaxed),
            compaction_dropped_version_count: metrics
                .compaction_dropped_version_count
                .load(Relaxed),
            live_sstable_count: metrics.live_sstable_count.load(Relaxed),
            sstable_manifest_load_count: metrics.sstable_manifest_load_count.load(Relaxed),
            sstable_manifest_load_total_us: metrics.sstable_manifest_load_total_us.load(Relaxed),
            sstable_manifest_load_error_count: metrics
                .sstable_manifest_load_error_count
                .load(Relaxed),
            sstable_manifest_live_file_count: metrics
                .sstable_manifest_live_file_count
                .load(Relaxed),
            sstable_manifest_legacy_scan_count: metrics
                .sstable_manifest_legacy_scan_count
                .load(Relaxed),
            sstable_manifest_legacy_scan_candidate_count: metrics
                .sstable_manifest_legacy_scan_candidate_count
                .load(Relaxed),
            sstable_manifest_open_error_count: metrics
                .sstable_manifest_open_error_count
                .load(Relaxed),
            row_write_count: metrics.row_write_count.load(Relaxed),
            fts_search_count: metrics.fts_search_count.load(Relaxed),
            fts_doc_hits: metrics.fts_doc_hits.load(Relaxed),
            wal_write_count: metrics.wal_write_count.load(Relaxed),
            wal_write_bytes: metrics.wal_write_bytes.load(Relaxed),
            wal_replay_count: metrics.wal_replay_count.load(Relaxed),
            wal_replay_total_us: metrics.wal_replay_total_us.load(Relaxed),
            wal_replay_segment_count: metrics.wal_replay_segment_count.load(Relaxed),
            wal_replay_bytes: metrics.wal_replay_bytes.load(Relaxed),
            wal_replay_valid_bytes: metrics.wal_replay_valid_bytes.load(Relaxed),
            wal_replay_last_segment_id: metrics.wal_replay_last_segment_id.load(Relaxed),
            wal_replay_last_valid_offset: metrics.wal_replay_last_valid_offset.load(Relaxed),
            wal_replay_entry_count: metrics.wal_replay_entry_count.load(Relaxed),
            wal_replay_put_count: metrics.wal_replay_put_count.load(Relaxed),
            wal_replay_delete_count: metrics.wal_replay_delete_count.load(Relaxed),
            wal_replay_partial_tail_count: metrics.wal_replay_partial_tail_count.load(Relaxed),
            wal_replay_truncate_count: metrics.wal_replay_truncate_count.load(Relaxed),
            wal_replay_error_count: metrics.wal_replay_error_count.load(Relaxed),
            wal_replay_apply_count: metrics.wal_replay_apply_count.load(Relaxed),
            wal_replay_apply_total_us: metrics.wal_replay_apply_total_us.load(Relaxed),
            wal_replay_max_ts: metrics.wal_replay_max_ts.load(Relaxed),
            query_count: metrics.query_count.load(Relaxed),
            slow_query_count: metrics.slow_query_count.load(Relaxed),
            query_total_us: metrics.query_total_us.load(Relaxed),
            query_sort_fallback_count: metrics.query_sort_fallback_count.load(Relaxed),
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

    #[test]
    fn distributed_http_boundary_rejects_legacy_auth_and_missing_hmac_secret() {
        let legacy = HttpServerSecurity::legacy_unsafe();
        for (raft_enabled, mode, sharding_enabled) in [
            (true, "isolated", false),
            (false, "raft(node_id=1)", false),
            (false, "isolated", true),
        ] {
            let error = validate_http_server_security_boundary(
                &legacy,
                raft_enabled,
                mode,
                sharding_enabled,
            )
            .expect_err("distributed topology must reject legacy unauthenticated HTTP");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert!(error.to_string().contains("legacy unauthenticated HTTP"));
        }

        let secure_without_secret = HttpServerSecurity {
            postgres_password: "fusiondb".to_string(),
            http_legacy_unsafe: false,
            forwarding_secret: String::new(),
            peer_scheme: "https".to_string(),
        };
        let error = validate_http_server_security_boundary(
            &secure_without_secret,
            true,
            "raft(node_id=1)",
            false,
        )
        .expect_err("distributed topology must require an HMAC secret");
        assert!(error.to_string().contains("forwarding secret"));

        validate_http_server_security_boundary(&legacy, false, "isolated", false)
            .expect("legacy mode remains an explicit isolated-server compatibility option");
    }

    #[test]
    fn strips_sql_block_zone_map_prune_hint_from_leading_comment() {
        let sql = "  /*+ FUSIONDB_DISABLE_SQL_BLOCK_ZONE_MAP_PRUNE */ SELECT id FROM metrics";
        let (stripped, disabled) = strip_sql_block_zone_map_prune_hint(sql);
        assert!(disabled);
        assert_eq!(stripped, "SELECT id FROM metrics");

        let (plain, disabled) = strip_sql_block_zone_map_prune_hint("SELECT id FROM metrics");
        assert!(!disabled);
        assert_eq!(plain, "SELECT id FROM metrics");
    }

    #[test]
    fn fanout_extremum_compares_decimal_strings_numerically() {
        use serde_json::json;
        // MIN/MAX over a DECIMAL column return the value as a JSON string. The merge must compare them
        // NUMERICALLY, not lexically: lexically "9.50" > "30.25" (since '9' > '3'), but numerically
        // 9.50 < 30.25. (SUM/AVG over DECIMAL return floats, so only the extremum path sees strings.)
        let mut mx = None;
        merge_fanout_extremum(&mut mx, Some(json!("9.50")), SqlShardExtremum::Max).unwrap();
        merge_fanout_extremum(&mut mx, Some(json!("30.25")), SqlShardExtremum::Max).unwrap();
        assert_eq!(mx, Some(json!("30.25")));
        let mut mn = None;
        merge_fanout_extremum(&mut mn, Some(json!("9.50")), SqlShardExtremum::Min).unwrap();
        merge_fanout_extremum(&mut mn, Some(json!("30.25")), SqlShardExtremum::Min).unwrap();
        assert_eq!(mn, Some(json!("9.50")));
    }

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
            postgres_password: Arc::from("fusiondb"),
            http_legacy_unsafe: true,
            forwarding_auth: ForwardingAuth::new(b"test-shard-forwarding-secret"),
            peer_scheme: "http".to_string(),
        })
    }

    fn secure_test_state(storage: Arc<dyn Storage>, forwarding_secret: &str) -> AppState {
        let executor = Arc::new(Executor::new(storage.clone()));
        AppState {
            executor,
            storage,
            raft: None,
            raft_client: reqwest::Client::new(),
            distributed_mode: "isolated".to_string(),
            shard_router: None,
            shard_owner_forwarding_enabled: false,
            postgres_password: Arc::from("secure-password"),
            http_legacy_unsafe: false,
            forwarding_auth: ForwardingAuth::new(forwarding_secret.as_bytes()),
            peer_scheme: "http".to_string(),
        }
    }

    fn secure_test_app(storage: Arc<dyn Storage>, forwarding_secret: &str) -> Router {
        build_router(secure_test_state(storage, forwarding_secret))
    }

    fn basic_authorization(username: &str, password: &str) -> String {
        format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", username, password))
        )
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
            postgres_password: Arc::from("fusiondb"),
            http_legacy_unsafe: true,
            forwarding_auth: None,
            peer_scheme: "http".to_string(),
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
            postgres_password: Arc::from("fusiondb"),
            http_legacy_unsafe: true,
            forwarding_auth: ForwardingAuth::new(b"test-shard-forwarding-secret"),
            peer_scheme: "http".to_string(),
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

    fn integer_primary_keys_for_owner(
        router: &ShardRouter,
        table_name: &str,
        owner_node_id: u64,
        count: usize,
    ) -> Vec<i64> {
        let mut keys = Vec::with_capacity(count);
        for value in 1_i64..100_000 {
            let row_id = crate::common::encoding::encode_i64_comparable(value);
            if router.route_key(table_name, &row_id).owner_node_id == owner_node_id {
                keys.push(value);
                if keys.len() == count {
                    return keys;
                }
            }
        }
        panic!(
            "not enough integer keys routed to owner node {}",
            owner_node_id
        );
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
    async fn secure_http_keeps_health_public_and_requires_basic_auth_elsewhere() {
        let wal_path = format!("test_http_secure_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).expect("storage"));
        let app = secure_test_app(storage, "cluster-forwarding-secret");

        let health = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);

        for request in [
            HttpRequest::builder()
                .uri("/auth/context")
                .body(Body::empty())
                .unwrap(),
            HttpRequest::builder()
                .uri("/auth/context")
                .header(AUTHORIZATION, "Bearer postgres")
                .body(Body::empty())
                .unwrap(),
            HttpRequest::builder()
                .uri("/auth/context")
                .header(FORWARDED_USER_HEADER, "postgres")
                .body(Body::empty())
                .unwrap(),
        ] {
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert!(response.headers().contains_key(WWW_AUTHENTICATE));
        }

        let response = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri("/auth/context")
                    .header(
                        AUTHORIZATION,
                        basic_authorization("postgres", "secure-password"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let envelope: Envelope<AuthContextInfo> = response_json(response).await;
        let context = envelope.data.unwrap();
        assert_eq!(context.username.as_deref(), Some("postgres"));
        assert!(context.authenticated);
        assert_eq!(context.mode, "basic");

        let _ = std::fs::remove_file(wal_path);
    }

    #[tokio::test]
    async fn secure_http_authenticates_rbac_hash_and_signed_forwarding() {
        let wal_path = format!("test_http_secure_rbac_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).expect("storage"));
        let mut txn = storage.begin_transaction().await.unwrap();
        save_user(
            &mut *txn,
            "alice",
            &UserRecord::new("alice-password", false),
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        let secret = "cluster-forwarding-secret";
        let state = secure_test_state(storage, secret);
        assert!(is_management_path("/raft/change-membership"));
        assert!(
            !management_access_allowed(&state, "alice", "/raft/change-membership", false,).await
        );
        assert!(!management_access_allowed(&state, "alice", "/compact", false).await);
        assert!(management_access_allowed(&state, "alice", "/raft/change-membership", true,).await);
        let app = build_router(state);
        let basic_response = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri("/auth/context")
                    .header(
                        AUTHORIZATION,
                        basic_authorization("alice", "alice-password"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(basic_response.status(), StatusCode::OK);

        let compact_response = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/compact")
                    .header(
                        AUTHORIZATION,
                        basic_authorization("alice", "alice-password"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(compact_response.status(), StatusCode::FORBIDDEN);

        let signed = ForwardingAuth::new(secret)
            .unwrap()
            .apply(
                reqwest::Client::new().get("http://127.0.0.1/auth/context"),
                "alice",
            )
            .build()
            .unwrap();
        let mut internal_request = HttpRequest::builder()
            .uri("/auth/context")
            .body(Body::empty())
            .unwrap();
        *internal_request.headers_mut() = signed.headers().clone();
        let internal_response = app.clone().oneshot(internal_request).await.unwrap();
        let envelope: Envelope<AuthContextInfo> = response_json(internal_response).await;
        assert_eq!(envelope.data.unwrap().mode, "internal_hmac");

        let forged = HttpRequest::builder()
            .uri("/auth/context")
            .header(FORWARDED_HEADER, FORWARDED_VALUE)
            .header(FORWARDED_USER_HEADER, "postgres")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(forged).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );

        let _ = std::fs::remove_file(wal_path);
    }

    #[tokio::test]
    async fn legacy_unsafe_http_never_trusts_an_unsigned_forwarded_identity() {
        let wal_path = format!("test_http_unsigned_forwarded_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).expect("storage"));
        let app = test_app_with_distributed_mode(storage, "raft(node_id=1)");
        let forged = HttpRequest::builder()
            .uri("/auth/context")
            .header(FORWARDED_HEADER, FORWARDED_VALUE)
            .header(FORWARDED_USER_HEADER, "postgres")
            .body(Body::empty())
            .unwrap();

        assert_eq!(
            app.oneshot(forged).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        let _ = std::fs::remove_file(wal_path);
    }

    #[tokio::test]
    async fn json_extractors_accept_authenticated_bodies_above_axum_default_limit() {
        let wal_path = format!("test_http_body_limit_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).expect("storage"));
        let app = test_app(storage);
        let body = serde_json::json!({
            "sql": " ".repeat(2 * 1024 * 1024 + 1024),
        })
        .to_string();
        let request = HttpRequest::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_ne!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let _ = std::fs::remove_file(wal_path);
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
        let create_sql =
            "CREATE TABLE fanout_users (id INTEGER PRIMARY KEY, name TEXT, amount INTEGER, bucket TEXT)";
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
                    "INSERT INTO fanout_users (id, name, amount, bucket) VALUES ({}, 'local', 10, 'shared')",
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
                    "INSERT INTO fanout_users (id, name, amount, bucket) VALUES ({}, 'remote', 20, 'shared')",
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

        let count_select = post_query(&local_app, "SELECT COUNT(*) FROM fanout_users").await;
        assert_eq!(count_select.status(), StatusCode::OK);
        let count_envelope: Envelope<Vec<QueryResultJson>> = response_json(count_select).await;
        match &count_envelope.data.expect("count data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(2)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout count"),
        }

        let count_distinct_select = post_query(
            &local_app,
            "SELECT COUNT(DISTINCT bucket) FROM fanout_users",
        )
        .await;
        assert_eq!(count_distinct_select.status(), StatusCode::OK);
        let count_distinct_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(count_distinct_select).await;
        match &count_distinct_envelope.data.expect("count distinct data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(1)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout count distinct"),
        }

        let sum_select = post_query(&local_app, "SELECT SUM(amount) FROM fanout_users").await;
        assert_eq!(sum_select.status(), StatusCode::OK);
        let sum_envelope: Envelope<Vec<QueryResultJson>> = response_json(sum_select).await;
        match &sum_envelope.data.expect("sum data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(30)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout sum"),
        }

        let min_select = post_query(&local_app, "SELECT MIN(amount) FROM fanout_users").await;
        assert_eq!(min_select.status(), StatusCode::OK);
        let min_envelope: Envelope<Vec<QueryResultJson>> = response_json(min_select).await;
        match &min_envelope.data.expect("min data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(10)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout min"),
        }

        let max_select = post_query(&local_app, "SELECT MAX(amount) FROM fanout_users").await;
        assert_eq!(max_select.status(), StatusCode::OK);
        let max_envelope: Envelope<Vec<QueryResultJson>> = response_json(max_select).await;
        match &max_envelope.data.expect("max data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(20)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout max"),
        }

        let avg_select = post_query(&local_app, "SELECT AVG(amount) FROM fanout_users").await;
        assert_eq!(avg_select.status(), StatusCode::OK);
        let avg_envelope: Envelope<Vec<QueryResultJson>> = response_json(avg_select).await;
        match &avg_envelope.data.expect("avg data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(15.0)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout avg"),
        }

        let sum_distinct_select =
            post_query(&local_app, "SELECT SUM(DISTINCT amount) FROM fanout_users").await;
        assert_eq!(sum_distinct_select.status(), StatusCode::OK);
        let sum_distinct_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(sum_distinct_select).await;
        match &sum_distinct_envelope.data.expect("sum distinct data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(30)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout sum distinct"),
        }

        let avg_distinct_select =
            post_query(&local_app, "SELECT AVG(DISTINCT amount) FROM fanout_users").await;
        assert_eq!(avg_distinct_select.status(), StatusCode::OK);
        let avg_distinct_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(avg_distinct_select).await;
        match &avg_distinct_envelope.data.expect("avg distinct data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(15.0)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout avg distinct"),
        }

        let _ = std::fs::remove_file(&local_wal_path);
        let _ = std::fs::remove_file(&owner_wal_path);
    }

    #[tokio::test]
    async fn http_query_fanouts_distinct_numeric_aggregates_across_shard_owners() {
        let local_wal_path = format!(
            "test_http_shard_owner_distinct_agg_local_{}.wal",
            uuid::Uuid::new_v4()
        );
        let owner_wal_path = format!(
            "test_http_shard_owner_distinct_agg_owner_{}.wal",
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
        let local_keys =
            integer_primary_keys_for_owner(&local_shard_router, "distinct_fanout", 1, 2);
        let remote_keys =
            integer_primary_keys_for_owner(&local_shard_router, "distinct_fanout", 2, 2);

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
        let create_sql = "CREATE TABLE distinct_fanout (id INTEGER PRIMARY KEY, score INTEGER)";
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

        // Local owner holds scores {10, 20}; remote owner holds scores {20, 20}.
        // The value 20 appears on both owners, so the distinct union is {10, 20}.
        for (key, score) in [(local_keys[0], 10), (local_keys[1], 20)] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!(
                        "INSERT INTO distinct_fanout (id, score) VALUES ({}, {})",
                        key, score
                    ),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }
        for (key, score) in [(remote_keys[0], 20), (remote_keys[1], 20)] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!(
                        "INSERT INTO distinct_fanout (id, score) VALUES ({}, {})",
                        key, score
                    ),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }

        // Plain SUM counts every row: 10 + 20 + 20 + 20 = 70.
        let sum_select = post_query(&local_app, "SELECT SUM(score) FROM distinct_fanout").await;
        assert_eq!(sum_select.status(), StatusCode::OK);
        let sum_envelope: Envelope<Vec<QueryResultJson>> = response_json(sum_select).await;
        match &sum_envelope.data.expect("sum data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(70)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout sum"),
        }

        // SUM(DISTINCT) deduplicates the cross-owner union {10, 20} = 30.
        let sum_distinct_select = post_query(
            &local_app,
            "SELECT SUM(DISTINCT score) FROM distinct_fanout",
        )
        .await;
        assert_eq!(sum_distinct_select.status(), StatusCode::OK);
        let sum_distinct_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(sum_distinct_select).await;
        match &sum_distinct_envelope.data.expect("sum distinct data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(30)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout sum distinct"),
        }

        // COUNT(DISTINCT) over the union {10, 20} = 2.
        let count_distinct_select = post_query(
            &local_app,
            "SELECT COUNT(DISTINCT score) FROM distinct_fanout",
        )
        .await;
        assert_eq!(count_distinct_select.status(), StatusCode::OK);
        let count_distinct_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(count_distinct_select).await;
        match &count_distinct_envelope.data.expect("count distinct data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(2)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout count distinct"),
        }

        // AVG(DISTINCT) = sum(union)/count(union) = 30 / 2 = 15.0 (plain AVG would be 70/4 = 17.5).
        let avg_distinct_select = post_query(
            &local_app,
            "SELECT AVG(DISTINCT score) FROM distinct_fanout",
        )
        .await;
        assert_eq!(avg_distinct_select.status(), StatusCode::OK);
        let avg_distinct_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(avg_distinct_select).await;
        match &avg_distinct_envelope.data.expect("avg distinct data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(15.0)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout avg distinct"),
        }

        let _ = std::fs::remove_file(&local_wal_path);
        let _ = std::fs::remove_file(&owner_wal_path);
    }

    #[test]
    fn fanout_sum_over_distinct_values_maps_decimal_strings() {
        let mut distinct_values = BTreeMap::new();
        distinct_values.insert("\"10.50\"".to_string(), serde_json::json!("10.50"));
        distinct_values.insert("\"20.25\"".to_string(), serde_json::json!("20.25"));
        distinct_values.insert("5".to_string(), serde_json::json!(5));
        let total = fanout_sum_over_distinct_values(&distinct_values).expect("decimal sum");
        let Some(FanoutSum::Float(total)) = total else {
            panic!("expected float total");
        };
        assert!((total - 35.75).abs() < 1e-9);

        // Non-numeric strings still fail closed.
        distinct_values.insert("\"abc\"".to_string(), serde_json::json!("abc"));
        assert!(fanout_sum_over_distinct_values(&distinct_values).is_err());
    }

    #[tokio::test]
    async fn http_query_fanouts_distinct_decimal_aggregates_across_shard_owners() {
        let local_wal_path = format!(
            "test_http_shard_owner_distinct_decimal_local_{}.wal",
            uuid::Uuid::new_v4()
        );
        let owner_wal_path = format!(
            "test_http_shard_owner_distinct_decimal_owner_{}.wal",
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
        let local_keys = integer_primary_keys_for_owner(&local_shard_router, "distinct_dec", 1, 2);
        let remote_keys = integer_primary_keys_for_owner(&local_shard_router, "distinct_dec", 2, 2);

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
        let create_sql = "CREATE TABLE distinct_dec (id INTEGER PRIMARY KEY, amount DECIMAL(6, 2))";
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

        // Local owner holds {10.50, 20.25}; remote owner holds {20.25, 20.25}.
        // The decimal 20.25 appears on both owners: distinct union {10.50, 20.25}.
        for (key, amount) in [(local_keys[0], "10.50"), (local_keys[1], "20.25")] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!(
                        "INSERT INTO distinct_dec (id, amount) VALUES ({}, CAST('{}' AS DECIMAL))",
                        key, amount
                    ),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }
        for (key, amount) in [(remote_keys[0], "20.25"), (remote_keys[1], "20.25")] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!(
                        "INSERT INTO distinct_dec (id, amount) VALUES ({}, CAST('{}' AS DECIMAL))",
                        key, amount
                    ),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }

        // SUM(DISTINCT) over the cross-owner decimal union {10.50, 20.25} = 30.75.
        let sum_distinct_select =
            post_query(&local_app, "SELECT SUM(DISTINCT amount) FROM distinct_dec").await;
        assert_eq!(sum_distinct_select.status(), StatusCode::OK);
        let sum_distinct_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(sum_distinct_select).await;
        match &sum_distinct_envelope.data.expect("sum distinct data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(30.75)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout sum distinct"),
        }

        // AVG(DISTINCT) = 30.75 / 2 = 15.375.
        let avg_distinct_select =
            post_query(&local_app, "SELECT AVG(DISTINCT amount) FROM distinct_dec").await;
        assert_eq!(avg_distinct_select.status(), StatusCode::OK);
        let avg_distinct_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(avg_distinct_select).await;
        match &avg_distinct_envelope.data.expect("avg distinct data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(15.375)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout avg distinct"),
        }

        let _ = std::fs::remove_file(&local_wal_path);
        let _ = std::fs::remove_file(&owner_wal_path);
    }

    #[tokio::test]
    async fn http_query_fanouts_group_count_across_shard_owners() {
        let local_wal_path = format!(
            "test_http_shard_owner_group_count_local_{}.wal",
            uuid::Uuid::new_v4()
        );
        let owner_wal_path = format!(
            "test_http_shard_owner_group_count_owner_{}.wal",
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
        let local_keys = integer_primary_keys_for_owner(&local_shard_router, "gc", 1, 2);
        let remote_keys = integer_primary_keys_for_owner(&local_shard_router, "gc", 2, 2);

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
        let create_sql = "CREATE TABLE gc (id INTEGER PRIMARY KEY, grp TEXT)";
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

        // Group 'a' spans BOTH owners (local + remote) and must sum to 2 after re-grouping.
        for (key, grp) in [(local_keys[0], "a"), (local_keys[1], "b")] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!("INSERT INTO gc (id, grp) VALUES ({}, '{}')", key, grp),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }
        for (key, grp) in [(remote_keys[0], "a"), (remote_keys[1], "c")] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!("INSERT INTO gc (id, grp) VALUES ({}, '{}')", key, grp),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }

        let group_count = post_query(&local_app, "SELECT grp, COUNT(*) FROM gc GROUP BY grp").await;
        assert_eq!(group_count.status(), StatusCode::OK);
        let envelope: Envelope<Vec<QueryResultJson>> = response_json(group_count).await;
        match &envelope.data.expect("group count data")[0] {
            QueryResultJson::Select { rows, .. } => {
                let mut rows = rows.clone();
                rows.sort_by_key(|row| format!("{:?}", row));
                assert_eq!(
                    rows,
                    vec![
                        vec![serde_json::json!("a"), serde_json::json!(2)],
                        vec![serde_json::json!("b"), serde_json::json!(1)],
                        vec![serde_json::json!("c"), serde_json::json!(1)],
                    ]
                );
            }
            QueryResultJson::Success { .. } => panic!("expected fanout group count"),
        }

        let _ = std::fs::remove_file(&local_wal_path);
        let _ = std::fs::remove_file(&owner_wal_path);
    }

    #[tokio::test]
    async fn http_query_fanouts_group_aggregate_across_shard_owners() {
        let local_wal_path = format!(
            "test_http_shard_owner_group_agg_local_{}.wal",
            uuid::Uuid::new_v4()
        );
        let owner_wal_path = format!(
            "test_http_shard_owner_group_agg_owner_{}.wal",
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
        let local_keys = integer_primary_keys_for_owner(&local_shard_router, "ga", 1, 2);
        let remote_keys = integer_primary_keys_for_owner(&local_shard_router, "ga", 2, 2);

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
        let create_sql = "CREATE TABLE ga (id INTEGER PRIMARY KEY, grp TEXT, amt INTEGER)";
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

        // Group 'a' spans both owners: SUM must add 10 (local) + 30 (remote) = 40; MAX = 30.
        for (key, grp, amt) in [(local_keys[0], "a", 10), (local_keys[1], "b", 20)] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!(
                        "INSERT INTO ga (id, grp, amt) VALUES ({}, '{}', {})",
                        key, grp, amt
                    ),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }
        for (key, grp, amt) in [(remote_keys[0], "a", 30), (remote_keys[1], "c", 40)] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!(
                        "INSERT INTO ga (id, grp, amt) VALUES ({}, '{}', {})",
                        key, grp, amt
                    ),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }

        let sum_q = post_query(&local_app, "SELECT grp, SUM(amt) FROM ga GROUP BY grp").await;
        assert_eq!(sum_q.status(), StatusCode::OK);
        let sum_env: Envelope<Vec<QueryResultJson>> = response_json(sum_q).await;
        match &sum_env.data.expect("sum data")[0] {
            QueryResultJson::Select { rows, .. } => {
                let mut rows = rows.clone();
                rows.sort_by_key(|row| format!("{:?}", row));
                assert_eq!(
                    rows,
                    vec![
                        vec![serde_json::json!("a"), serde_json::json!(40)],
                        vec![serde_json::json!("b"), serde_json::json!(20)],
                        vec![serde_json::json!("c"), serde_json::json!(40)],
                    ]
                );
            }
            QueryResultJson::Success { .. } => panic!("expected fanout group sum"),
        }

        let max_q = post_query(&local_app, "SELECT grp, MAX(amt) FROM ga GROUP BY grp").await;
        assert_eq!(max_q.status(), StatusCode::OK);
        let max_env: Envelope<Vec<QueryResultJson>> = response_json(max_q).await;
        match &max_env.data.expect("max data")[0] {
            QueryResultJson::Select { rows, .. } => {
                let mut rows = rows.clone();
                rows.sort_by_key(|row| format!("{:?}", row));
                assert_eq!(
                    rows,
                    vec![
                        vec![serde_json::json!("a"), serde_json::json!(30)],
                        vec![serde_json::json!("b"), serde_json::json!(20)],
                        vec![serde_json::json!("c"), serde_json::json!(40)],
                    ]
                );
            }
            QueryResultJson::Success { .. } => panic!("expected fanout group max"),
        }

        let _ = std::fs::remove_file(&local_wal_path);
        let _ = std::fs::remove_file(&owner_wal_path);
    }

    #[tokio::test]
    async fn http_query_fanouts_group_aggregate_order_by_limit_global_top_k() {
        let local_wal_path = format!(
            "test_http_shard_owner_group_agg_topk_local_{}.wal",
            uuid::Uuid::new_v4()
        );
        let owner_wal_path = format!(
            "test_http_shard_owner_group_agg_topk_owner_{}.wal",
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
        let local_keys = integer_primary_keys_for_owner(&local_shard_router, "gao", 1, 3);
        let remote_keys = integer_primary_keys_for_owner(&local_shard_router, "gao", 2, 3);

        let local_app =
            test_app_with_shard_router_forwarding(local_storage.clone(), local_shard_router, true);
        let owner_app =
            test_app_with_shard_router_forwarding(owner_storage.clone(), owner_shard_router, true);
        tokio::spawn(async move {
            axum::serve(owner_listener, owner_app)
                .await
                .expect("owner http server");
        });

        let create_sql = "CREATE TABLE gao (id INTEGER PRIMARY KEY, grp TEXT, amt INTEGER)";
        let client = reqwest::Client::new();
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

        // Local owner holds groups a=5, b=20, d=15; remote owner holds a=100, c=10, b=1.
        // Global sums: a=105, b=21, d=15, c=10. Group 'a' has a TINY local partial (5) that a buggy
        // per-owner `LIMIT` would drop, even though 'a' is the global top group — so ORDER BY/LIMIT
        // applied per-owner instead of post-merge would yield the wrong rows AND wrong sums.
        for (key, grp, amt) in [
            (local_keys[0], "a", 5),
            (local_keys[1], "b", 20),
            (local_keys[2], "d", 15),
        ] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!(
                        "INSERT INTO gao (id, grp, amt) VALUES ({}, '{}', {})",
                        key, grp, amt
                    ),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }
        for (key, grp, amt) in [
            (remote_keys[0], "a", 100),
            (remote_keys[1], "c", 10),
            (remote_keys[2], "b", 1),
        ] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!(
                        "INSERT INTO gao (id, grp, amt) VALUES ({}, '{}', {})",
                        key, grp, amt
                    ),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }

        // Global top-2 by SUM DESC — order and values are asserted exactly (NOT re-sorted).
        let top2 = post_query(
            &local_app,
            "SELECT grp, SUM(amt) FROM gao GROUP BY grp ORDER BY SUM(amt) DESC LIMIT 2",
        )
        .await;
        assert_eq!(top2.status(), StatusCode::OK);
        let top2_env: Envelope<Vec<QueryResultJson>> = response_json(top2).await;
        match &top2_env.data.expect("top2 data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(
                    rows,
                    &vec![
                        vec![serde_json::json!("a"), serde_json::json!(105)],
                        vec![serde_json::json!("b"), serde_json::json!(21)],
                    ]
                );
            }
            QueryResultJson::Success { .. } => panic!("expected fanout group order/limit"),
        }

        // OFFSET past the global top-1 returns the runner-up.
        let offset_q = post_query(
            &local_app,
            "SELECT grp, SUM(amt) FROM gao GROUP BY grp ORDER BY SUM(amt) DESC LIMIT 1 OFFSET 1",
        )
        .await;
        assert_eq!(offset_q.status(), StatusCode::OK);
        let offset_env: Envelope<Vec<QueryResultJson>> = response_json(offset_q).await;
        match &offset_env.data.expect("offset data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(
                    rows,
                    &vec![vec![serde_json::json!("b"), serde_json::json!(21)]]
                );
            }
            QueryResultJson::Success { .. } => panic!("expected fanout group offset"),
        }

        // ORDER BY a group column ASC returns ALL groups, globally ordered.
        let by_grp = post_query(
            &local_app,
            "SELECT grp, SUM(amt) FROM gao GROUP BY grp ORDER BY grp ASC",
        )
        .await;
        assert_eq!(by_grp.status(), StatusCode::OK);
        let by_grp_env: Envelope<Vec<QueryResultJson>> = response_json(by_grp).await;
        match &by_grp_env.data.expect("by_grp data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(
                    rows,
                    &vec![
                        vec![serde_json::json!("a"), serde_json::json!(105)],
                        vec![serde_json::json!("b"), serde_json::json!(21)],
                        vec![serde_json::json!("c"), serde_json::json!(10)],
                        vec![serde_json::json!("d"), serde_json::json!(15)],
                    ]
                );
            }
            QueryResultJson::Success { .. } => panic!("expected fanout group order by grp"),
        }

        let _ = std::fs::remove_file(&local_wal_path);
        let _ = std::fs::remove_file(&owner_wal_path);
    }

    #[tokio::test]
    async fn http_query_fanouts_group_count_order_by_limit_global_top_k() {
        let local_wal_path = format!(
            "test_http_shard_owner_group_count_topk_local_{}.wal",
            uuid::Uuid::new_v4()
        );
        let owner_wal_path = format!(
            "test_http_shard_owner_group_count_topk_owner_{}.wal",
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
        let local_keys = integer_primary_keys_for_owner(&local_shard_router, "gco", 1, 3);
        let remote_keys = integer_primary_keys_for_owner(&local_shard_router, "gco", 2, 4);

        let local_app =
            test_app_with_shard_router_forwarding(local_storage.clone(), local_shard_router, true);
        let owner_app =
            test_app_with_shard_router_forwarding(owner_storage.clone(), owner_shard_router, true);
        tokio::spawn(async move {
            axum::serve(owner_listener, owner_app)
                .await
                .expect("owner http server");
        });

        let create_sql = "CREATE TABLE gco (id INTEGER PRIMARY KEY, grp TEXT)";
        let client = reqwest::Client::new();
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

        // Local owner holds a×1, b×2; remote owner holds a×3, c×1. Global counts: a=4, b=2, c=1.
        // Group 'a' has only ONE local row, which a buggy per-owner LIMIT 1 would drop (local top is
        // b=2), so the global top group + its count would be wrong without post-merge.
        for (key, grp) in [
            (local_keys[0], "a"),
            (local_keys[1], "b"),
            (local_keys[2], "b"),
        ] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!("INSERT INTO gco (id, grp) VALUES ({}, '{}')", key, grp),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }
        for (key, grp) in [
            (remote_keys[0], "a"),
            (remote_keys[1], "a"),
            (remote_keys[2], "a"),
            (remote_keys[3], "c"),
        ] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!("INSERT INTO gco (id, grp) VALUES ({}, '{}')", key, grp),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }

        // Global top-1 by COUNT(*) DESC.
        let top1 = post_query(
            &local_app,
            "SELECT grp, COUNT(*) FROM gco GROUP BY grp ORDER BY COUNT(*) DESC LIMIT 1",
        )
        .await;
        assert_eq!(top1.status(), StatusCode::OK);
        let top1_env: Envelope<Vec<QueryResultJson>> = response_json(top1).await;
        match &top1_env.data.expect("top1 data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(
                    rows,
                    &vec![vec![serde_json::json!("a"), serde_json::json!(4)]]
                );
            }
            QueryResultJson::Success { .. } => panic!("expected fanout group count order/limit"),
        }

        // OFFSET past the top group returns the runner-up.
        let offset_q = post_query(
            &local_app,
            "SELECT grp, COUNT(*) FROM gco GROUP BY grp ORDER BY COUNT(*) DESC LIMIT 1 OFFSET 1",
        )
        .await;
        assert_eq!(offset_q.status(), StatusCode::OK);
        let offset_env: Envelope<Vec<QueryResultJson>> = response_json(offset_q).await;
        match &offset_env.data.expect("offset data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(
                    rows,
                    &vec![vec![serde_json::json!("b"), serde_json::json!(2)]]
                );
            }
            QueryResultJson::Success { .. } => panic!("expected fanout group count offset"),
        }

        let _ = std::fs::remove_file(&local_wal_path);
        let _ = std::fs::remove_file(&owner_wal_path);
    }

    #[tokio::test]
    async fn http_query_fanouts_group_avg_order_by_limit_global_top_k() {
        let local_wal_path = format!(
            "test_http_shard_owner_group_avg_topk_local_{}.wal",
            uuid::Uuid::new_v4()
        );
        let owner_wal_path = format!(
            "test_http_shard_owner_group_avg_topk_owner_{}.wal",
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
        let local_keys = integer_primary_keys_for_owner(&local_shard_router, "gavo", 1, 2);
        let remote_keys = integer_primary_keys_for_owner(&local_shard_router, "gavo", 2, 3);

        let local_app =
            test_app_with_shard_router_forwarding(local_storage.clone(), local_shard_router, true);
        let owner_app =
            test_app_with_shard_router_forwarding(owner_storage.clone(), owner_shard_router, true);
        tokio::spawn(async move {
            axum::serve(owner_listener, owner_app)
                .await
                .expect("owner http server");
        });

        let create_sql = "CREATE TABLE gavo (id INTEGER PRIMARY KEY, grp TEXT, amt INTEGER)";
        let client = reqwest::Client::new();
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

        // Local: a=[10], b=[20]; remote: a=[40,40], c=[10]. Global AVG: a=(10+40+40)/3=30, b=20, c=10.
        // AVG is not mergeable per-owner (local avg of a is 10, remote 40) — only the post-merge
        // partial-sum/count division gives the correct global avg used for ordering.
        for (key, grp, amt) in [(local_keys[0], "a", 10), (local_keys[1], "b", 20)] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!(
                        "INSERT INTO gavo (id, grp, amt) VALUES ({}, '{}', {})",
                        key, grp, amt
                    ),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }
        for (key, grp, amt) in [
            (remote_keys[0], "a", 40),
            (remote_keys[1], "a", 40),
            (remote_keys[2], "c", 10),
        ] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!(
                        "INSERT INTO gavo (id, grp, amt) VALUES ({}, '{}', {})",
                        key, grp, amt
                    ),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }

        // Global top-2 by AVG DESC: a (30) then b (20). Assert ordering by group (avg float exact).
        let top2 = post_query(
            &local_app,
            "SELECT grp, AVG(amt) FROM gavo GROUP BY grp ORDER BY AVG(amt) DESC LIMIT 2",
        )
        .await;
        assert_eq!(top2.status(), StatusCode::OK);
        let top2_env: Envelope<Vec<QueryResultJson>> = response_json(top2).await;
        match &top2_env.data.expect("avg top2 data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows.len(), 2, "expected global top-2 avg");
                assert_eq!(rows[0][0], serde_json::json!("a"));
                assert_eq!(rows[1][0], serde_json::json!("b"));
                assert_eq!(rows[0][1].as_f64(), Some(30.0));
                assert_eq!(rows[1][1].as_f64(), Some(20.0));
            }
            QueryResultJson::Success { .. } => panic!("expected fanout group avg order/limit"),
        }

        let _ = std::fs::remove_file(&local_wal_path);
        let _ = std::fs::remove_file(&owner_wal_path);
    }

    #[tokio::test]
    async fn http_query_fanouts_group_aggregate_having_global_filter() {
        let local_wal_path = format!(
            "test_http_shard_owner_group_having_local_{}.wal",
            uuid::Uuid::new_v4()
        );
        let owner_wal_path = format!(
            "test_http_shard_owner_group_having_owner_{}.wal",
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
        let local_keys = integer_primary_keys_for_owner(&local_shard_router, "ghav", 1, 3);
        let remote_keys = integer_primary_keys_for_owner(&local_shard_router, "ghav", 2, 3);

        let local_app =
            test_app_with_shard_router_forwarding(local_storage.clone(), local_shard_router, true);
        let owner_app =
            test_app_with_shard_router_forwarding(owner_storage.clone(), owner_shard_router, true);
        tokio::spawn(async move {
            axum::serve(owner_listener, owner_app)
                .await
                .expect("owner http server");
        });

        let create_sql = "CREATE TABLE ghav (id INTEGER PRIMARY KEY, grp TEXT, amt INTEGER)";
        let client = reqwest::Client::new();
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

        // Local: a=60, b=30, c=80; remote: a=60, b=10, d=50. Global sums: a=120, b=40, c=80, d=50.
        // Group 'a' is below the HAVING threshold (100) on EACH owner (60 < 100) but above it globally
        // (120) — a per-owner HAVING would drop 'a' on both owners and return nothing; only post-merge
        // HAVING is correct.
        for (key, grp, amt) in [
            (local_keys[0], "a", 60),
            (local_keys[1], "b", 30),
            (local_keys[2], "c", 80),
        ] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!(
                        "INSERT INTO ghav (id, grp, amt) VALUES ({}, '{}', {})",
                        key, grp, amt
                    ),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }
        for (key, grp, amt) in [
            (remote_keys[0], "a", 60),
            (remote_keys[1], "b", 10),
            (remote_keys[2], "d", 50),
        ] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!(
                        "INSERT INTO ghav (id, grp, amt) VALUES ({}, '{}', {})",
                        key, grp, amt
                    ),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }

        // HAVING on the GLOBAL sum: only 'a' (120) exceeds 100.
        let having = post_query(
            &local_app,
            "SELECT grp, SUM(amt) FROM ghav GROUP BY grp HAVING SUM(amt) > 100",
        )
        .await;
        assert_eq!(having.status(), StatusCode::OK);
        let having_env: Envelope<Vec<QueryResultJson>> = response_json(having).await;
        match &having_env.data.expect("having data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(
                    rows,
                    &vec![vec![serde_json::json!("a"), serde_json::json!(120)]]
                );
            }
            QueryResultJson::Success { .. } => panic!("expected fanout group having"),
        }

        // HAVING + ORDER BY + LIMIT together: groups with global sum >= 50 are a=120, c=80, d=50
        // (b=40 dropped); ORDER BY SUM DESC LIMIT 2 → a, c.
        let combo = post_query(
            &local_app,
            "SELECT grp, SUM(amt) FROM ghav GROUP BY grp HAVING SUM(amt) >= 50 ORDER BY SUM(amt) DESC LIMIT 2",
        )
        .await;
        assert_eq!(combo.status(), StatusCode::OK);
        let combo_env: Envelope<Vec<QueryResultJson>> = response_json(combo).await;
        match &combo_env.data.expect("combo data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(
                    rows,
                    &vec![
                        vec![serde_json::json!("a"), serde_json::json!(120)],
                        vec![serde_json::json!("c"), serde_json::json!(80)],
                    ]
                );
            }
            QueryResultJson::Success { .. } => panic!("expected fanout group having+order+limit"),
        }

        let _ = std::fs::remove_file(&local_wal_path);
        let _ = std::fs::remove_file(&owner_wal_path);
    }

    #[tokio::test]
    async fn http_query_fanouts_group_multi_aggregate_across_shard_owners() {
        let local_wal_path = format!(
            "test_http_shard_owner_group_multiagg_local_{}.wal",
            uuid::Uuid::new_v4()
        );
        let owner_wal_path = format!(
            "test_http_shard_owner_group_multiagg_owner_{}.wal",
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
        let local_keys = integer_primary_keys_for_owner(&local_shard_router, "gma", 1, 2);
        let remote_keys = integer_primary_keys_for_owner(&local_shard_router, "gma", 2, 3);

        let local_app =
            test_app_with_shard_router_forwarding(local_storage.clone(), local_shard_router, true);
        let owner_app =
            test_app_with_shard_router_forwarding(owner_storage.clone(), owner_shard_router, true);
        tokio::spawn(async move {
            axum::serve(owner_listener, owner_app)
                .await
                .expect("owner http server");
        });

        let create_sql = "CREATE TABLE gma (id INTEGER PRIMARY KEY, grp TEXT, amt INTEGER)";
        let client = reqwest::Client::new();
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

        // Local: a/10, b/20; remote: a/30, a/5, c/40. Group 'a' spans owners: global COUNT(*)=3
        // (1 local + 2 remote) and SUM=45 (10 + 35). Each aggregate must merge INDEPENDENTLY per group.
        for (key, grp, amt) in [(local_keys[0], "a", 10), (local_keys[1], "b", 20)] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!(
                        "INSERT INTO gma (id, grp, amt) VALUES ({}, '{}', {})",
                        key, grp, amt
                    ),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }
        for (key, grp, amt) in [
            (remote_keys[0], "a", 30),
            (remote_keys[1], "a", 5),
            (remote_keys[2], "c", 40),
        ] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!(
                        "INSERT INTO gma (id, grp, amt) VALUES ({}, '{}', {})",
                        key, grp, amt
                    ),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }

        // COUNT(*) + SUM merged independently per global group.
        let q = post_query(
            &local_app,
            "SELECT grp, COUNT(*), SUM(amt) FROM gma GROUP BY grp",
        )
        .await;
        assert_eq!(q.status(), StatusCode::OK);
        let env: Envelope<Vec<QueryResultJson>> = response_json(q).await;
        match &env.data.expect("multi-agg data")[0] {
            QueryResultJson::Select { rows, .. } => {
                let mut rows = rows.clone();
                rows.sort_by_key(|row| format!("{:?}", row));
                assert_eq!(
                    rows,
                    vec![
                        vec![
                            serde_json::json!("a"),
                            serde_json::json!(3),
                            serde_json::json!(45)
                        ],
                        vec![
                            serde_json::json!("b"),
                            serde_json::json!(1),
                            serde_json::json!(20)
                        ],
                        vec![
                            serde_json::json!("c"),
                            serde_json::json!(1),
                            serde_json::json!(40)
                        ],
                    ]
                );
            }
            QueryResultJson::Success { .. } => panic!("expected multi-aggregate fanout"),
        }

        // Post-merge HAVING + ORDER BY on a multi-aggregate result: SUM(amt) >= 40 keeps a (45) and
        // c (40); ORDER BY SUM(amt) DESC → a then c.
        let combo = post_query(
            &local_app,
            "SELECT grp, COUNT(*), SUM(amt) FROM gma GROUP BY grp HAVING SUM(amt) >= 40 ORDER BY SUM(amt) DESC",
        )
        .await;
        assert_eq!(combo.status(), StatusCode::OK);
        let combo_env: Envelope<Vec<QueryResultJson>> = response_json(combo).await;
        match &combo_env.data.expect("multi-agg combo data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(
                    rows,
                    &vec![
                        vec![
                            serde_json::json!("a"),
                            serde_json::json!(3),
                            serde_json::json!(45)
                        ],
                        vec![
                            serde_json::json!("c"),
                            serde_json::json!(1),
                            serde_json::json!(40)
                        ],
                    ]
                );
            }
            QueryResultJson::Success { .. } => panic!("expected multi-aggregate having+order"),
        }

        let _ = std::fs::remove_file(&local_wal_path);
        let _ = std::fs::remove_file(&owner_wal_path);
    }

    #[tokio::test]
    async fn http_query_fanouts_group_decimal_aggregates_across_shard_owners() {
        let local_wal_path = format!(
            "test_http_shard_owner_group_decimal_local_{}.wal",
            uuid::Uuid::new_v4()
        );
        let owner_wal_path = format!(
            "test_http_shard_owner_group_decimal_owner_{}.wal",
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
        let local_keys = integer_primary_keys_for_owner(&local_shard_router, "gdec", 1, 2);
        let remote_keys = integer_primary_keys_for_owner(&local_shard_router, "gdec", 2, 2);

        let local_app =
            test_app_with_shard_router_forwarding(local_storage.clone(), local_shard_router, true);
        let owner_app =
            test_app_with_shard_router_forwarding(owner_storage.clone(), owner_shard_router, true);
        tokio::spawn(async move {
            axum::serve(owner_listener, owner_app)
                .await
                .expect("owner http server");
        });

        let create_sql = "CREATE TABLE gdec (id INTEGER PRIMARY KEY, grp TEXT, amt DECIMAL(10,2))";
        let client = reqwest::Client::new();
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

        // Local: a=9.50, b=5.25; remote: a=30.25, c=100.00. Group 'a' spans owners. Global a:
        // SUM=39.75, MIN=9.50, MAX=30.25. MIN/MAX must compare NUMERICALLY: lexically "9.50" > "30.25",
        // so a string-compare bug would return the wrong extremum.
        for (key, grp, amt) in [(local_keys[0], "a", "9.50"), (local_keys[1], "b", "5.25")] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!(
                        "INSERT INTO gdec (id, grp, amt) VALUES ({}, '{}', {})",
                        key, grp, amt
                    ),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }
        for (key, grp, amt) in [
            (remote_keys[0], "a", "30.25"),
            (remote_keys[1], "c", "100.00"),
        ] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!(
                        "INSERT INTO gdec (id, grp, amt) VALUES ({}, '{}', {})",
                        key, grp, amt
                    ),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }

        // Multi-aggregate over DECIMAL: SUM / MIN / MAX merged per global group.
        let q = post_query(
            &local_app,
            "SELECT grp, SUM(amt), MIN(amt), MAX(amt) FROM gdec GROUP BY grp",
        )
        .await;
        assert_eq!(q.status(), StatusCode::OK);
        let env: Envelope<Vec<QueryResultJson>> = response_json(q).await;
        // DECIMAL aggregate values may come back as a JSON string (MIN/MAX preserve the decimal value)
        // or a JSON number (SUM returns a float); accept either and compare numerically.
        let parse = |v: &serde_json::Value| {
            v.as_f64()
                .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
        };
        match &env.data.expect("decimal data")[0] {
            QueryResultJson::Select { rows, .. } => {
                let mut got: Vec<(String, Option<f64>, Option<f64>, Option<f64>)> = rows
                    .iter()
                    .map(|r| {
                        (
                            r[0].as_str().unwrap_or_default().to_string(),
                            parse(&r[1]),
                            parse(&r[2]),
                            parse(&r[3]),
                        )
                    })
                    .collect();
                got.sort_by(|a, b| a.0.cmp(&b.0));
                assert_eq!(
                    got,
                    vec![
                        ("a".to_string(), Some(39.75), Some(9.50), Some(30.25)),
                        ("b".to_string(), Some(5.25), Some(5.25), Some(5.25)),
                        ("c".to_string(), Some(100.00), Some(100.00), Some(100.00)),
                    ]
                );
            }
            QueryResultJson::Success { .. } => panic!("expected decimal multi-aggregate fanout"),
        }

        // AVG over DECIMAL: global a = (9.50 + 30.25) / 2 = 19.875.
        let avg = post_query(&local_app, "SELECT grp, AVG(amt) FROM gdec GROUP BY grp").await;
        assert_eq!(avg.status(), StatusCode::OK);
        let avg_env: Envelope<Vec<QueryResultJson>> = response_json(avg).await;
        match &avg_env.data.expect("decimal avg data")[0] {
            QueryResultJson::Select { rows, .. } => {
                let a_row = rows
                    .iter()
                    .find(|r| r[0].as_str() == Some("a"))
                    .expect("group a");
                assert_eq!(parse(&a_row[1]), Some(19.875));
            }
            QueryResultJson::Success { .. } => panic!("expected decimal avg fanout"),
        }

        let _ = std::fs::remove_file(&local_wal_path);
        let _ = std::fs::remove_file(&owner_wal_path);
    }

    #[tokio::test]
    async fn http_query_fanouts_group_avg_across_shard_owners() {
        let local_wal_path = format!(
            "test_http_shard_owner_group_avg_local_{}.wal",
            uuid::Uuid::new_v4()
        );
        let owner_wal_path = format!(
            "test_http_shard_owner_group_avg_owner_{}.wal",
            uuid::Uuid::new_v4()
        );
        let local_storage: Arc<dyn Storage> =
            Arc::new(MemoryStorage::new(&local_wal_path).expect("local storage"));
        let owner_storage: Arc<dyn Storage> =
            Arc::new(MemoryStorage::new(&owner_wal_path).expect("owner storage"));
        let (owner_listener, owner_addr) = bind_test_http_listener().await;
        let local_addr = "127.0.0.1:8092".to_string();
        let local_config =
            sharded_http_test_config_for_node(4, 1, local_addr.clone(), owner_addr.clone());
        let owner_config = sharded_http_test_config_for_node(4, 2, local_addr, owner_addr.clone());
        let local_shard_router = ShardRouter::from_config(&local_config).expect("local router");
        let owner_shard_router = ShardRouter::from_config(&owner_config).expect("owner router");
        let local_keys = integer_primary_keys_for_owner(&local_shard_router, "gavg", 1, 2);
        let remote_keys = integer_primary_keys_for_owner(&local_shard_router, "gavg", 2, 2);

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
        let create_sql = "CREATE TABLE gavg (id INTEGER PRIMARY KEY, grp TEXT, amt INTEGER)";
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

        // Group 'a' spans both owners: AVG must merge partial sums and counts, not average the
        // per-owner averages: (10 + 50) / 2 = 30, not (10 + 50) / 2-of-averages. 'b' = 20, 'c' = 40.
        for (key, grp, amt) in [(local_keys[0], "a", 10), (local_keys[1], "b", 20)] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!(
                        "INSERT INTO gavg (id, grp, amt) VALUES ({}, '{}', {})",
                        key, grp, amt
                    ),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }
        for (key, grp, amt) in [(remote_keys[0], "a", 50), (remote_keys[1], "c", 40)] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!(
                        "INSERT INTO gavg (id, grp, amt) VALUES ({}, '{}', {})",
                        key, grp, amt
                    ),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }

        let avg_q = post_query(&local_app, "SELECT grp, AVG(amt) FROM gavg GROUP BY grp").await;
        assert_eq!(avg_q.status(), StatusCode::OK);
        let avg_env: Envelope<Vec<QueryResultJson>> = response_json(avg_q).await;
        match &avg_env.data.expect("avg data")[0] {
            QueryResultJson::Select { rows, .. } => {
                let mut rows = rows.clone();
                rows.sort_by_key(|row| format!("{:?}", row));
                assert_eq!(
                    rows,
                    vec![
                        vec![serde_json::json!("a"), serde_json::json!(30.0)],
                        vec![serde_json::json!("b"), serde_json::json!(20.0)],
                        vec![serde_json::json!("c"), serde_json::json!(40.0)],
                    ]
                );
            }
            QueryResultJson::Success { .. } => panic!("expected fanout group avg"),
        }

        let _ = std::fs::remove_file(&local_wal_path);
        let _ = std::fs::remove_file(&owner_wal_path);
    }

    #[tokio::test]
    async fn http_query_rejects_unsupported_multiowner_group_by() {
        let local_wal_path = format!(
            "test_http_shard_owner_group_unsupported_local_{}.wal",
            uuid::Uuid::new_v4()
        );
        let owner_wal_path = format!(
            "test_http_shard_owner_group_unsupported_owner_{}.wal",
            uuid::Uuid::new_v4()
        );
        let local_storage: Arc<dyn Storage> =
            Arc::new(MemoryStorage::new(&local_wal_path).expect("local storage"));
        let owner_storage: Arc<dyn Storage> =
            Arc::new(MemoryStorage::new(&owner_wal_path).expect("owner storage"));
        let (owner_listener, owner_addr) = bind_test_http_listener().await;
        let local_addr = "127.0.0.1:8093".to_string();
        let local_config =
            sharded_http_test_config_for_node(4, 1, local_addr.clone(), owner_addr.clone());
        let owner_config = sharded_http_test_config_for_node(4, 2, local_addr, owner_addr.clone());
        let local_shard_router = ShardRouter::from_config(&local_config).expect("local router");
        let owner_shard_router = ShardRouter::from_config(&owner_config).expect("owner router");
        let local_keys = integer_primary_keys_for_owner(&local_shard_router, "gx", 1, 2);
        let remote_keys = integer_primary_keys_for_owner(&local_shard_router, "gx", 2, 2);

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
        let create_sql = "CREATE TABLE gx (id INTEGER PRIMARY KEY, grp TEXT, amt INTEGER)";
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

        for (key, grp, amt) in [(local_keys[0], "a", 10), (remote_keys[0], "a", 30)] {
            assert_eq!(
                post_query(
                    &local_app,
                    &format!(
                        "INSERT INTO gx (id, grp, amt) VALUES ({}, '{}', {})",
                        key, grp, amt
                    ),
                )
                .await
                .status(),
                StatusCode::OK
            );
        }

        // A grouped COUNT(DISTINCT) matches none of the supported grouped fan-out plans (COUNT/SUM/
        // MIN/MAX/AVG and multi-aggregate, none of which handle DISTINCT); over multiple owners this
        // would otherwise silently return only local groups, so the safety net must fire.
        let resp = post_query(
            &local_app,
            "SELECT grp, COUNT(DISTINCT amt) FROM gx GROUP BY grp",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // A bare primary-key equality is recognized as a single-owner point read, so the query never
        // scatters: the safety net correctly does NOT fire even for an unsupported grouped shape, and
        // it runs locally/forwards to the owner instead of erroring.
        let pinned = post_query(
            &local_app,
            &format!(
                "SELECT grp, COUNT(DISTINCT amt) FROM gx WHERE id = {} GROUP BY grp",
                local_keys[0]
            ),
        )
        .await;
        assert_eq!(pinned.status(), StatusCode::OK);

        // Supported grouped shapes over the same owners still succeed (no false positive): a single
        // aggregate and a multi-aggregate projection (the latter now handled by the multi-agg fan-out).
        let ok = post_query(&local_app, "SELECT grp, SUM(amt) FROM gx GROUP BY grp").await;
        assert_eq!(ok.status(), StatusCode::OK);
        let ok_multi = post_query(
            &local_app,
            "SELECT grp, COUNT(*), SUM(amt) FROM gx GROUP BY grp",
        )
        .await;
        assert_eq!(ok_multi.status(), StatusCode::OK);

        // A multi-aggregate whose MIN/MAX/SUM argument is a NON-numeric column matches the multi-agg
        // shape but its owner-eligibility (numeric check) declines, so it must fail LOUDLY across owners
        // rather than degrade to silent local-only results (regression guard from the 460 review).
        let non_numeric = post_query(
            &local_app,
            "SELECT grp, COUNT(*), MAX(grp) FROM gx GROUP BY grp",
        )
        .await;
        assert_eq!(non_numeric.status(), StatusCode::BAD_REQUEST);

        // Same for the SINGLE-aggregate planners: a non-numeric MAX/SUM/AVG argument matches the shape
        // but is type-ineligible, so it must also fail loudly (BENCHPROD-461 — previously silent
        // local-only because the structural is_some() short-circuit suppressed the safety net).
        for sql in [
            "SELECT grp, MAX(grp) FROM gx GROUP BY grp",
            "SELECT grp, SUM(grp) FROM gx GROUP BY grp",
            "SELECT grp, AVG(grp) FROM gx GROUP BY grp",
        ] {
            assert_eq!(
                post_query(&local_app, sql).await.status(),
                StatusCode::BAD_REQUEST,
                "non-numeric single aggregate must error loudly: {sql}"
            );
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
        let local_key = integer_primary_key_for_owner(&local_shard_router, "forward_exec_users", 1);
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
        let create_sql =
            "CREATE TABLE forward_exec_users (id INTEGER PRIMARY KEY, name TEXT, amount INTEGER, bucket TEXT)";
        let local_create = post_query(&local_app, create_sql).await;
        assert_eq!(local_create.status(), StatusCode::OK);
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
                    "INSERT INTO forward_exec_users (id, name, amount, bucket) VALUES ({}, 'local-exec', 10, 'shared')",
                    local_key
                ),
            )
            .await
            .status(),
            StatusCode::OK
        );

        let prepare_request = HttpRequest::builder()
            .method("POST")
            .uri("/prepare")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "sql": "INSERT INTO forward_exec_users (id, name, amount, bucket) VALUES ($1, $2, $3, $4); SELECT name FROM forward_exec_users WHERE id = $1"
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
                    "params": [remote_key, "remote-exec", 20, "shared"]
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

        let prepare_fanout_request = HttpRequest::builder()
            .method("POST")
            .uri("/prepare")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "sql": "SELECT id, name FROM forward_exec_users"
                })
                .to_string(),
            ))
            .expect("prepare fanout request");
        let prepare_fanout_response = local_app
            .clone()
            .oneshot(prepare_fanout_request)
            .await
            .expect("prepare fanout response");
        let prepare_fanout_envelope: Envelope<PreparedStatementInfo> =
            response_json(prepare_fanout_response).await;
        let prepared_fanout = prepare_fanout_envelope
            .data
            .expect("prepared fanout statement");
        let execute_fanout_request = HttpRequest::builder()
            .method("POST")
            .uri("/execute")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "statement_id": prepared_fanout.statement_id,
                    "params": []
                })
                .to_string(),
            ))
            .expect("execute fanout request");
        let execute_fanout_response = local_app
            .clone()
            .oneshot(execute_fanout_request)
            .await
            .expect("execute fanout response");
        assert_eq!(execute_fanout_response.status(), StatusCode::OK);
        let execute_fanout_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(execute_fanout_response).await;
        match &execute_fanout_envelope.data.expect("execute fanout data")[0] {
            QueryResultJson::Select { rows, .. } => {
                let mut rows = rows.clone();
                rows.sort_by_key(|row| row[0].as_i64().unwrap());
                let mut expected = vec![
                    vec![
                        serde_json::json!(local_key),
                        serde_json::json!("local-exec"),
                    ],
                    vec![
                        serde_json::json!(remote_key),
                        serde_json::json!("remote-exec"),
                    ],
                ];
                expected.sort_by_key(|row| row[0].as_i64().unwrap());
                assert_eq!(rows, expected);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout select"),
        }

        let prepare_count_request = HttpRequest::builder()
            .method("POST")
            .uri("/prepare")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "sql": "SELECT COUNT(*) FROM forward_exec_users"
                })
                .to_string(),
            ))
            .expect("prepare count request");
        let prepare_count_response = local_app
            .clone()
            .oneshot(prepare_count_request)
            .await
            .expect("prepare count response");
        let prepare_count_envelope: Envelope<PreparedStatementInfo> =
            response_json(prepare_count_response).await;
        let prepared_count = prepare_count_envelope
            .data
            .expect("prepared count statement");
        let execute_count_request = HttpRequest::builder()
            .method("POST")
            .uri("/execute")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "statement_id": prepared_count.statement_id,
                    "params": []
                })
                .to_string(),
            ))
            .expect("execute count request");
        let execute_count_response = local_app
            .clone()
            .oneshot(execute_count_request)
            .await
            .expect("execute count response");
        assert_eq!(execute_count_response.status(), StatusCode::OK);
        let execute_count_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(execute_count_response).await;
        match &execute_count_envelope.data.expect("execute count data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(2)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout count"),
        }

        let prepare_count_distinct_request = HttpRequest::builder()
            .method("POST")
            .uri("/prepare")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "sql": "SELECT COUNT(DISTINCT bucket) FROM forward_exec_users"
                })
                .to_string(),
            ))
            .expect("prepare count distinct request");
        let prepare_count_distinct_response = local_app
            .clone()
            .oneshot(prepare_count_distinct_request)
            .await
            .expect("prepare count distinct response");
        let prepare_count_distinct_envelope: Envelope<PreparedStatementInfo> =
            response_json(prepare_count_distinct_response).await;
        let prepared_count_distinct = prepare_count_distinct_envelope
            .data
            .expect("prepared count distinct statement");
        let execute_count_distinct_request = HttpRequest::builder()
            .method("POST")
            .uri("/execute")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "statement_id": prepared_count_distinct.statement_id,
                    "params": []
                })
                .to_string(),
            ))
            .expect("execute count distinct request");
        let execute_count_distinct_response = local_app
            .clone()
            .oneshot(execute_count_distinct_request)
            .await
            .expect("execute count distinct response");
        assert_eq!(execute_count_distinct_response.status(), StatusCode::OK);
        let execute_count_distinct_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(execute_count_distinct_response).await;
        match &execute_count_distinct_envelope
            .data
            .expect("execute count distinct data")[0]
        {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(1)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout count distinct"),
        }

        let prepare_sum_request = HttpRequest::builder()
            .method("POST")
            .uri("/prepare")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "sql": "SELECT SUM(amount) FROM forward_exec_users"
                })
                .to_string(),
            ))
            .expect("prepare sum request");
        let prepare_sum_response = local_app
            .clone()
            .oneshot(prepare_sum_request)
            .await
            .expect("prepare sum response");
        let prepare_sum_envelope: Envelope<PreparedStatementInfo> =
            response_json(prepare_sum_response).await;
        let prepared_sum = prepare_sum_envelope.data.expect("prepared sum statement");
        let execute_sum_request = HttpRequest::builder()
            .method("POST")
            .uri("/execute")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "statement_id": prepared_sum.statement_id,
                    "params": []
                })
                .to_string(),
            ))
            .expect("execute sum request");
        let execute_sum_response = local_app
            .clone()
            .oneshot(execute_sum_request)
            .await
            .expect("execute sum response");
        assert_eq!(execute_sum_response.status(), StatusCode::OK);
        let execute_sum_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(execute_sum_response).await;
        match &execute_sum_envelope.data.expect("execute sum data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(30)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout sum"),
        }

        let prepare_min_request = HttpRequest::builder()
            .method("POST")
            .uri("/prepare")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "sql": "SELECT MIN(amount) FROM forward_exec_users"
                })
                .to_string(),
            ))
            .expect("prepare min request");
        let prepare_min_response = local_app
            .clone()
            .oneshot(prepare_min_request)
            .await
            .expect("prepare min response");
        let prepare_min_envelope: Envelope<PreparedStatementInfo> =
            response_json(prepare_min_response).await;
        let prepared_min = prepare_min_envelope.data.expect("prepared min statement");
        let execute_min_request = HttpRequest::builder()
            .method("POST")
            .uri("/execute")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "statement_id": prepared_min.statement_id,
                    "params": []
                })
                .to_string(),
            ))
            .expect("execute min request");
        let execute_min_response = local_app
            .clone()
            .oneshot(execute_min_request)
            .await
            .expect("execute min response");
        assert_eq!(execute_min_response.status(), StatusCode::OK);
        let execute_min_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(execute_min_response).await;
        match &execute_min_envelope.data.expect("execute min data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(10)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout min"),
        }

        let prepare_max_request = HttpRequest::builder()
            .method("POST")
            .uri("/prepare")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "sql": "SELECT MAX(amount) FROM forward_exec_users"
                })
                .to_string(),
            ))
            .expect("prepare max request");
        let prepare_max_response = local_app
            .clone()
            .oneshot(prepare_max_request)
            .await
            .expect("prepare max response");
        let prepare_max_envelope: Envelope<PreparedStatementInfo> =
            response_json(prepare_max_response).await;
        let prepared_max = prepare_max_envelope.data.expect("prepared max statement");
        let execute_max_request = HttpRequest::builder()
            .method("POST")
            .uri("/execute")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "statement_id": prepared_max.statement_id,
                    "params": []
                })
                .to_string(),
            ))
            .expect("execute max request");
        let execute_max_response = local_app
            .clone()
            .oneshot(execute_max_request)
            .await
            .expect("execute max response");
        assert_eq!(execute_max_response.status(), StatusCode::OK);
        let execute_max_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(execute_max_response).await;
        match &execute_max_envelope.data.expect("execute max data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(20)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout max"),
        }

        let prepare_avg_request = HttpRequest::builder()
            .method("POST")
            .uri("/prepare")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "sql": "SELECT AVG(amount) FROM forward_exec_users"
                })
                .to_string(),
            ))
            .expect("prepare avg request");
        let prepare_avg_response = local_app
            .clone()
            .oneshot(prepare_avg_request)
            .await
            .expect("prepare avg response");
        let prepare_avg_envelope: Envelope<PreparedStatementInfo> =
            response_json(prepare_avg_response).await;
        let prepared_avg = prepare_avg_envelope.data.expect("prepared avg statement");
        let execute_avg_request = HttpRequest::builder()
            .method("POST")
            .uri("/execute")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "statement_id": prepared_avg.statement_id,
                    "params": []
                })
                .to_string(),
            ))
            .expect("execute avg request");
        let execute_avg_response = local_app
            .clone()
            .oneshot(execute_avg_request)
            .await
            .expect("execute avg response");
        assert_eq!(execute_avg_response.status(), StatusCode::OK);
        let execute_avg_envelope: Envelope<Vec<QueryResultJson>> =
            response_json(execute_avg_response).await;
        match &execute_avg_envelope.data.expect("execute avg data")[0] {
            QueryResultJson::Select { rows, .. } => {
                assert_eq!(rows, &vec![vec![serde_json::json!(15.0)]]);
            }
            QueryResultJson::Success { .. } => panic!("expected fanout avg"),
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
        assert!(data.get("query_result_cache_eligible_count").is_some());
        assert!(data.get("query_result_cache_hit_count").is_some());
        assert!(data.get("query_result_cache_miss_count").is_some());
        assert!(data.get("query_result_cache_stale_count").is_some());
        assert!(data.get("query_result_cache_insert_count").is_some());
        assert!(data.get("query_result_cache_invalidation_count").is_some());
        assert!(data.get("block_cache_hit_count").is_some());
        assert!(data.get("block_cache_miss_count").is_some());
        assert!(data.get("block_cache_insert_count").is_some());
        assert!(data.get("block_cache_fill_skip_count").is_some());
        assert!(data.get("block_cache_eviction_count").is_some());
        assert!(data.get("sstable_block_file_open_count").is_some());
        assert!(data.get("sstable_block_read_bytes").is_some());
        assert!(data.get("sstable_open_count").is_some());
        assert!(data.get("sstable_open_total_us").is_some());
        assert!(data.get("sstable_open_index_bytes").is_some());
        assert!(data.get("sstable_open_index_read_us").is_some());
        assert!(data.get("sstable_open_index_decode_us").is_some());
        assert!(data.get("sstable_open_filter_bytes").is_some());
        assert!(data.get("sstable_open_filter_read_us").is_some());
        assert!(data.get("sstable_open_filter_decode_us").is_some());
        assert!(data.get("sstable_open_meta_bytes").is_some());
        assert!(data.get("sstable_open_meta_read_us").is_some());
        assert!(data.get("sstable_open_meta_decode_us").is_some());
        assert!(data.get("sstable_open_index_entries").is_some());
        assert!(data.get("sstable_open_block_property_count").is_some());
        assert!(data.get("sstable_index_cache_hit_count").is_some());
        assert!(data.get("sstable_index_cache_miss_count").is_some());
        assert!(data.get("sstable_index_cache_stale_count").is_some());
        assert!(data.get("sstable_index_cache_invalid_count").is_some());
        assert!(data.get("sstable_index_cache_write_count").is_some());
        assert!(data.get("sstable_index_cache_write_error_count").is_some());
        assert!(data.get("sstable_prefix_filter_check_count").is_some());
        assert!(data.get("sstable_prefix_filter_positive_count").is_some());
        assert!(data.get("sstable_prefix_filter_skip_count").is_some());
        assert!(data.get("sstable_prefix_filter_fail_open_count").is_some());
        assert!(data
            .get("sstable_index_prefix_filter_check_count")
            .is_some());
        assert!(data
            .get("sstable_index_prefix_filter_positive_count")
            .is_some());
        assert!(data.get("sstable_index_prefix_filter_skip_count").is_some());
        assert!(data
            .get("sstable_index_prefix_filter_fail_open_count")
            .is_some());
        assert!(data.get("sstable_user_key_filter_check_count").is_some());
        assert!(data.get("sstable_user_key_filter_positive_count").is_some());
        assert!(data.get("sstable_user_key_filter_skip_count").is_some());
        assert!(data
            .get("sstable_user_key_filter_fail_open_count")
            .is_some());
        assert!(data
            .get("sstable_block_prefix_filter_check_count")
            .is_some());
        assert!(data
            .get("sstable_block_prefix_filter_positive_count")
            .is_some());
        assert!(data.get("sstable_block_prefix_filter_skip_count").is_some());
        assert!(data
            .get("sstable_block_prefix_filter_fail_open_count")
            .is_some());
        assert!(data
            .get("sstable_block_index_prefix_filter_check_count")
            .is_some());
        assert!(data
            .get("sstable_block_index_prefix_filter_positive_count")
            .is_some());
        assert!(data
            .get("sstable_block_index_prefix_filter_skip_count")
            .is_some());
        assert!(data
            .get("sstable_block_index_prefix_filter_fail_open_count")
            .is_some());
        assert!(data
            .get("sstable_block_zone_map_filter_check_count")
            .is_some());
        assert!(data
            .get("sstable_block_zone_map_filter_positive_count")
            .is_some());
        assert!(data
            .get("sstable_block_zone_map_filter_skip_count")
            .is_some());
        assert!(data
            .get("sstable_block_zone_map_filter_fail_open_count")
            .is_some());
        assert!(data.get("sstable_block_zone_map_metadata_bytes").is_some());
        assert!(data
            .get("sstable_block_zone_map_mvcc_overlap_fail_open_count")
            .is_some());
        assert!(data
            .get("sstable_block_zone_map_mvcc_boundary_split_fail_open_count")
            .is_some());
        assert!(data
            .get("sstable_block_zone_map_mvcc_write_buffer_overlap_fail_open_count")
            .is_some());
        assert!(data
            .get("sstable_block_zone_map_mvcc_memtable_overlap_fail_open_count")
            .is_some());
        assert!(data
            .get("sstable_block_zone_map_mvcc_sstable_overlap_fail_open_count")
            .is_some());
        assert!(data
            .get("sstable_block_zone_map_schema_fail_open_count")
            .is_some());
        assert!(data.get("sstable_point_probe_count").is_some());
        assert!(data.get("sstable_point_overlap_skip_count").is_some());
        assert!(data.get("sstable_range_probe_count").is_some());
        assert!(data.get("sstable_range_overlap_skip_count").is_some());
        assert!(data.get("sstable_iterator_open_count").is_some());
        assert!(data
            .get("columnar_single_source_aggregate_fast_path_count")
            .is_some());
        assert!(data.get("sstable_reverse_iterator_open_count").is_some());
        assert!(data.get("sstable_reverse_block_read_count").is_some());
        assert!(data
            .get("sstable_reverse_block_entry_decode_count")
            .is_some());
        assert!(data
            .get("sstable_reverse_block_entry_yield_count")
            .is_some());
        assert!(data.get("sstable_reverse_block_span_scan_count").is_some());
        assert!(data
            .get("sstable_reverse_block_span_scan_entry_count")
            .is_some());
        assert!(data
            .get("sstable_reverse_block_span_materialize_entry_count")
            .is_some());
        assert!(data.get("sstable_reverse_seek_sidecar_hit_count").is_some());
        assert!(data
            .get("sstable_reverse_seek_sidecar_miss_count")
            .is_some());
        assert!(data
            .get("sstable_reverse_seek_sidecar_stale_count")
            .is_some());
        assert!(data
            .get("sstable_reverse_seek_sidecar_invalid_count")
            .is_some());
        assert!(data
            .get("sstable_reverse_seek_sidecar_write_count")
            .is_some());
        assert!(data
            .get("sstable_reverse_seek_sidecar_write_error_count")
            .is_some());
        assert!(data.get("sstable_reverse_seek_sidecar_use_count").is_some());
        assert!(data
            .get("sstable_reverse_seek_sidecar_fail_open_count")
            .is_some());
        assert!(data
            .get("sstable_reverse_seek_sidecar_index_entry_count")
            .is_some());
        assert!(data
            .get("sstable_reverse_seek_sidecar_entry_materialize_count")
            .is_some());
        assert!(data
            .get("sstable_reverse_seek_sidecar_offset_probe_count")
            .is_some());
        assert!(data.get("fusion_reverse_scan_count").is_some());
        assert!(data.get("fusion_reverse_source_open_count").is_some());
        assert!(data
            .get("fusion_reverse_sstable_frontier_probe_count")
            .is_some());
        assert!(data
            .get("fusion_reverse_sstable_frontier_in_range_count")
            .is_some());
        assert!(data
            .get("fusion_reverse_sstable_frontier_file_count")
            .is_some());
        assert!(data
            .get("fusion_reverse_sstable_frontier_tighten_count")
            .is_some());
        assert!(data
            .get("fusion_reverse_sstable_frontier_empty_skip_count")
            .is_some());
        assert!(data
            .get("fusion_reverse_sstable_frontier_fail_open_count")
            .is_some());
        assert!(data.get("fusion_reverse_sstable_pending_count").is_some());
        assert!(data
            .get("fusion_reverse_sstable_activation_count")
            .is_some());
        assert!(data
            .get("fusion_reverse_sstable_deferred_unopened_count")
            .is_some());
        assert!(data
            .get("fusion_reverse_sstable_activation_equal_frontier_count")
            .is_some());
        assert!(data.get("fusion_reverse_raw_entry_read_count").is_some());
        assert!(data.get("fusion_reverse_visible_candidate_count").is_some());
        assert!(data.get("fusion_reverse_visible_put_count").is_some());
        assert!(data.get("index_key_stream_entry_visit_count").is_some());
        assert!(data.get("index_ordered_topk_scan_count").is_some());
        assert!(data.get("index_ordered_topk_entry_visit_count").is_some());
        assert!(data.get("index_ordered_topk_reverse_scan_count").is_some());
        assert!(data
            .get("index_ordered_topk_index_only_row_count")
            .is_some());
        assert!(data
            .get("index_ordered_topk_base_row_fetch_count")
            .is_some());
        assert!(data
            .get("index_group_count_summary_entry_visit_count")
            .is_some());
        assert!(data.get("index_loose_seek_count").is_some());
        assert!(data.get("index_loose_value_count").is_some());
        assert!(data.get("index_loose_run_skip_count").is_some());
        assert!(data.get("compaction_run_count").is_some());
        assert!(data.get("compaction_input_bytes").is_some());
        assert!(data.get("compaction_output_bytes").is_some());
        assert!(data.get("compaction_dropped_version_count").is_some());
        assert!(data.get("live_sstable_count").is_some());
        assert!(data.get("sstable_manifest_load_count").is_some());
        assert!(data.get("sstable_manifest_load_total_us").is_some());
        assert!(data.get("sstable_manifest_load_error_count").is_some());
        assert!(data.get("sstable_manifest_live_file_count").is_some());
        assert!(data.get("sstable_manifest_legacy_scan_count").is_some());
        assert!(data
            .get("sstable_manifest_legacy_scan_candidate_count")
            .is_some());
        assert!(data.get("sstable_manifest_open_error_count").is_some());
        assert!(data.get("wal_replay_count").is_some());
        assert!(data.get("wal_replay_total_us").is_some());
        assert!(data.get("wal_replay_segment_count").is_some());
        assert!(data.get("wal_replay_bytes").is_some());
        assert!(data.get("wal_replay_valid_bytes").is_some());
        assert!(data.get("wal_replay_last_segment_id").is_some());
        assert!(data.get("wal_replay_last_valid_offset").is_some());
        assert!(data.get("wal_replay_entry_count").is_some());
        assert!(data.get("wal_replay_put_count").is_some());
        assert!(data.get("wal_replay_delete_count").is_some());
        assert!(data.get("wal_replay_partial_tail_count").is_some());
        assert!(data.get("wal_replay_truncate_count").is_some());
        assert!(data.get("wal_replay_error_count").is_some());
        assert!(data.get("wal_replay_apply_count").is_some());
        assert!(data.get("wal_replay_apply_total_us").is_some());
        assert!(data.get("wal_replay_max_ts").is_some());
        assert!(data.get("query_sort_fallback_count").is_some());

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
        assert!(prometheus.contains("fusiondb_query_result_cache_eligible_count"));
        assert!(prometheus.contains("fusiondb_query_result_cache_hit_count"));
        assert!(prometheus.contains("fusiondb_query_result_cache_miss_count"));
        assert!(prometheus.contains("fusiondb_query_result_cache_stale_count"));
        assert!(prometheus.contains("fusiondb_query_result_cache_insert_count"));
        assert!(prometheus.contains("fusiondb_query_result_cache_invalidation_count"));
        assert!(prometheus.contains("fusiondb_block_cache_hit_count"));
        assert!(prometheus.contains("fusiondb_block_cache_miss_count"));
        assert!(prometheus.contains("fusiondb_block_cache_insert_count"));
        assert!(prometheus.contains("fusiondb_block_cache_fill_skip_count"));
        assert!(prometheus.contains("fusiondb_block_cache_eviction_count"));
        assert!(prometheus.contains("fusiondb_sstable_block_file_open_count"));
        assert!(prometheus.contains("fusiondb_sstable_block_read_bytes"));
        assert!(prometheus.contains("fusiondb_sstable_open_count"));
        assert!(prometheus.contains("fusiondb_sstable_open_total_us"));
        assert!(prometheus.contains("fusiondb_sstable_open_index_bytes"));
        assert!(prometheus.contains("fusiondb_sstable_open_index_read_us"));
        assert!(prometheus.contains("fusiondb_sstable_open_index_decode_us"));
        assert!(prometheus.contains("fusiondb_sstable_open_filter_bytes"));
        assert!(prometheus.contains("fusiondb_sstable_open_filter_read_us"));
        assert!(prometheus.contains("fusiondb_sstable_open_filter_decode_us"));
        assert!(prometheus.contains("fusiondb_sstable_open_meta_bytes"));
        assert!(prometheus.contains("fusiondb_sstable_open_meta_read_us"));
        assert!(prometheus.contains("fusiondb_sstable_open_meta_decode_us"));
        assert!(prometheus.contains("fusiondb_sstable_open_index_entries"));
        assert!(prometheus.contains("fusiondb_sstable_open_block_property_count"));
        assert!(prometheus.contains("fusiondb_sstable_index_cache_hit_count"));
        assert!(prometheus.contains("fusiondb_sstable_index_cache_miss_count"));
        assert!(prometheus.contains("fusiondb_sstable_index_cache_stale_count"));
        assert!(prometheus.contains("fusiondb_sstable_index_cache_invalid_count"));
        assert!(prometheus.contains("fusiondb_sstable_index_cache_write_count"));
        assert!(prometheus.contains("fusiondb_sstable_index_cache_write_error_count"));
        assert!(prometheus.contains("fusiondb_sstable_prefix_filter_check_count"));
        assert!(prometheus.contains("fusiondb_sstable_prefix_filter_positive_count"));
        assert!(prometheus.contains("fusiondb_sstable_prefix_filter_skip_count"));
        assert!(prometheus.contains("fusiondb_sstable_prefix_filter_fail_open_count"));
        assert!(prometheus.contains("fusiondb_sstable_index_prefix_filter_check_count"));
        assert!(prometheus.contains("fusiondb_sstable_index_prefix_filter_positive_count"));
        assert!(prometheus.contains("fusiondb_sstable_index_prefix_filter_skip_count"));
        assert!(prometheus.contains("fusiondb_sstable_index_prefix_filter_fail_open_count"));
        assert!(prometheus.contains("fusiondb_sstable_user_key_filter_check_count"));
        assert!(prometheus.contains("fusiondb_sstable_user_key_filter_positive_count"));
        assert!(prometheus.contains("fusiondb_sstable_user_key_filter_skip_count"));
        assert!(prometheus.contains("fusiondb_sstable_user_key_filter_fail_open_count"));
        assert!(prometheus.contains("fusiondb_sstable_block_prefix_filter_check_count"));
        assert!(prometheus.contains("fusiondb_sstable_block_prefix_filter_positive_count"));
        assert!(prometheus.contains("fusiondb_sstable_block_prefix_filter_skip_count"));
        assert!(prometheus.contains("fusiondb_sstable_block_prefix_filter_fail_open_count"));
        assert!(prometheus.contains("fusiondb_sstable_block_index_prefix_filter_check_count"));
        assert!(prometheus.contains("fusiondb_sstable_block_index_prefix_filter_positive_count"));
        assert!(prometheus.contains("fusiondb_sstable_block_index_prefix_filter_skip_count"));
        assert!(prometheus.contains("fusiondb_sstable_block_index_prefix_filter_fail_open_count"));
        assert!(prometheus.contains("fusiondb_sstable_block_zone_map_filter_check_count"));
        assert!(prometheus.contains("fusiondb_sstable_block_zone_map_filter_positive_count"));
        assert!(prometheus.contains("fusiondb_sstable_block_zone_map_filter_skip_count"));
        assert!(prometheus.contains("fusiondb_sstable_block_zone_map_filter_fail_open_count"));
        assert!(prometheus.contains("fusiondb_sstable_block_zone_map_metadata_bytes"));
        assert!(prometheus.contains("fusiondb_sstable_block_zone_map_mvcc_overlap_fail_open_count"));
        assert!(prometheus
            .contains("fusiondb_sstable_block_zone_map_mvcc_boundary_split_fail_open_count"));
        assert!(prometheus
            .contains("fusiondb_sstable_block_zone_map_mvcc_write_buffer_overlap_fail_open_count"));
        assert!(prometheus
            .contains("fusiondb_sstable_block_zone_map_mvcc_memtable_overlap_fail_open_count"));
        assert!(prometheus
            .contains("fusiondb_sstable_block_zone_map_mvcc_sstable_overlap_fail_open_count"));
        assert!(prometheus.contains("fusiondb_sstable_block_zone_map_schema_fail_open_count"));
        assert!(prometheus.contains("fusiondb_sstable_point_probe_count"));
        assert!(prometheus.contains("fusiondb_sstable_point_overlap_skip_count"));
        assert!(prometheus.contains("fusiondb_sstable_range_probe_count"));
        assert!(prometheus.contains("fusiondb_sstable_range_overlap_skip_count"));
        assert!(prometheus.contains("fusiondb_sstable_iterator_open_count"));
        assert!(prometheus.contains("fusiondb_columnar_single_source_aggregate_fast_path_count"));
        assert!(prometheus.contains("fusiondb_sstable_reverse_iterator_open_count"));
        assert!(prometheus.contains("fusiondb_sstable_reverse_block_read_count"));
        assert!(prometheus.contains("fusiondb_sstable_reverse_block_entry_decode_count"));
        assert!(prometheus.contains("fusiondb_sstable_reverse_block_entry_yield_count"));
        assert!(prometheus.contains("fusiondb_sstable_reverse_block_span_scan_count"));
        assert!(prometheus.contains("fusiondb_sstable_reverse_block_span_scan_entry_count"));
        assert!(prometheus.contains("fusiondb_sstable_reverse_block_span_materialize_entry_count"));
        assert!(prometheus.contains("fusiondb_sstable_reverse_seek_sidecar_hit_count"));
        assert!(prometheus.contains("fusiondb_sstable_reverse_seek_sidecar_miss_count"));
        assert!(prometheus.contains("fusiondb_sstable_reverse_seek_sidecar_stale_count"));
        assert!(prometheus.contains("fusiondb_sstable_reverse_seek_sidecar_invalid_count"));
        assert!(prometheus.contains("fusiondb_sstable_reverse_seek_sidecar_write_count"));
        assert!(prometheus.contains("fusiondb_sstable_reverse_seek_sidecar_write_error_count"));
        assert!(prometheus.contains("fusiondb_sstable_reverse_seek_sidecar_use_count"));
        assert!(prometheus.contains("fusiondb_sstable_reverse_seek_sidecar_fail_open_count"));
        assert!(prometheus.contains("fusiondb_sstable_reverse_seek_sidecar_index_entry_count"));
        assert!(
            prometheus.contains("fusiondb_sstable_reverse_seek_sidecar_entry_materialize_count")
        );
        assert!(prometheus.contains("fusiondb_sstable_reverse_seek_sidecar_offset_probe_count"));
        assert!(prometheus.contains("fusiondb_fusion_reverse_scan_count"));
        assert!(prometheus.contains("fusiondb_fusion_reverse_source_open_count"));
        assert!(prometheus.contains("fusiondb_fusion_reverse_sstable_frontier_probe_count"));
        assert!(prometheus.contains("fusiondb_fusion_reverse_sstable_frontier_in_range_count"));
        assert!(prometheus.contains("fusiondb_fusion_reverse_sstable_frontier_file_count"));
        assert!(prometheus.contains("fusiondb_fusion_reverse_sstable_frontier_tighten_count"));
        assert!(prometheus.contains("fusiondb_fusion_reverse_sstable_frontier_empty_skip_count"));
        assert!(prometheus.contains("fusiondb_fusion_reverse_sstable_frontier_fail_open_count"));
        assert!(prometheus.contains("fusiondb_fusion_reverse_sstable_pending_count"));
        assert!(prometheus.contains("fusiondb_fusion_reverse_sstable_activation_count"));
        assert!(prometheus.contains("fusiondb_fusion_reverse_sstable_deferred_unopened_count"));
        assert!(
            prometheus.contains("fusiondb_fusion_reverse_sstable_activation_equal_frontier_count")
        );
        assert!(prometheus.contains("fusiondb_fusion_reverse_raw_entry_read_count"));
        assert!(prometheus.contains("fusiondb_fusion_reverse_visible_candidate_count"));
        assert!(prometheus.contains("fusiondb_fusion_reverse_visible_put_count"));
        assert!(prometheus.contains("fusiondb_index_key_stream_entry_visit_count"));
        assert!(prometheus.contains("fusiondb_index_ordered_topk_scan_count"));
        assert!(prometheus.contains("fusiondb_index_ordered_topk_entry_visit_count"));
        assert!(prometheus.contains("fusiondb_index_ordered_topk_reverse_scan_count"));
        assert!(prometheus.contains("fusiondb_index_ordered_topk_index_only_row_count"));
        assert!(prometheus.contains("fusiondb_index_ordered_topk_base_row_fetch_count"));
        assert!(prometheus.contains("fusiondb_index_group_count_summary_entry_visit_count"));
        assert!(prometheus.contains("fusiondb_index_loose_seek_count"));
        assert!(prometheus.contains("fusiondb_index_loose_value_count"));
        assert!(prometheus.contains("fusiondb_index_loose_run_skip_count"));
        assert!(prometheus.contains("fusiondb_compaction_run_count"));
        assert!(prometheus.contains("fusiondb_compaction_input_bytes"));
        assert!(prometheus.contains("fusiondb_compaction_output_bytes"));
        assert!(prometheus.contains("fusiondb_compaction_dropped_version_count"));
        assert!(prometheus.contains("fusiondb_live_sstable_count"));
        assert!(prometheus.contains("fusiondb_sstable_manifest_load_count"));
        assert!(prometheus.contains("fusiondb_sstable_manifest_load_total_us"));
        assert!(prometheus.contains("fusiondb_sstable_manifest_load_error_count"));
        assert!(prometheus.contains("fusiondb_sstable_manifest_live_file_count"));
        assert!(prometheus.contains("fusiondb_sstable_manifest_legacy_scan_count"));
        assert!(prometheus.contains("fusiondb_sstable_manifest_legacy_scan_candidate_count"));
        assert!(prometheus.contains("fusiondb_sstable_manifest_open_error_count"));
        assert!(prometheus.contains("fusiondb_wal_replay_count"));
        assert!(prometheus.contains("fusiondb_wal_replay_total_us"));
        assert!(prometheus.contains("fusiondb_wal_replay_segment_count"));
        assert!(prometheus.contains("fusiondb_wal_replay_bytes"));
        assert!(prometheus.contains("fusiondb_wal_replay_valid_bytes"));
        assert!(prometheus.contains("fusiondb_wal_replay_last_segment_id"));
        assert!(prometheus.contains("fusiondb_wal_replay_last_valid_offset"));
        assert!(prometheus.contains("fusiondb_wal_replay_entry_count"));
        assert!(prometheus.contains("fusiondb_wal_replay_put_count"));
        assert!(prometheus.contains("fusiondb_wal_replay_delete_count"));
        assert!(prometheus.contains("fusiondb_wal_replay_partial_tail_count"));
        assert!(prometheus.contains("fusiondb_wal_replay_truncate_count"));
        assert!(prometheus.contains("fusiondb_wal_replay_error_count"));
        assert!(prometheus.contains("fusiondb_wal_replay_apply_count"));
        assert!(prometheus.contains("fusiondb_wal_replay_apply_total_us"));
        assert!(prometheus.contains("fusiondb_wal_replay_max_ts"));
        assert!(prometheus.contains("fusiondb_query_sort_fallback_count"));

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

    #[tokio::test]
    async fn direct_search_requires_superuser_for_global_indexes() {
        let wal_path = format!("test_http_search_rbac_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).expect("storage"));

        {
            let mut txn = storage.begin_transaction().await.expect("begin txn");
            let mut alice = UserRecord::new("alice-password", false);
            alice.grant("allowed_search", "SELECT");
            save_user(&mut *txn, "alice", &alice)
                .await
                .expect("save user");
            txn.commit().await.expect("commit user txn");
        }

        let app = secure_test_app(storage, "search-forwarding-secret");
        for (path, body) in [
            ("/vector_search", r#"{"query":[0.1,0.2],"limit":5}"#),
            (
                "/hybrid_search",
                r#"{"text_query":"needle","vector_query":[0.1,0.2],"limit":5}"#,
            ),
        ] {
            let forbidden = app
                .clone()
                .oneshot(
                    HttpRequest::builder()
                        .method("POST")
                        .uri(path)
                        .header("content-type", "application/json")
                        .header(
                            "authorization",
                            basic_authorization("alice", "alice-password"),
                        )
                        .body(Body::from(body))
                        .expect("non-superuser search request"),
                )
                .await
                .expect("non-superuser search response");
            assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

            let allowed = app
                .clone()
                .oneshot(
                    HttpRequest::builder()
                        .method("POST")
                        .uri(path)
                        .header("content-type", "application/json")
                        .header(
                            "authorization",
                            basic_authorization("postgres", "secure-password"),
                        )
                        .body(Body::from(body))
                        .expect("superuser search request"),
                )
                .await
                .expect("superuser search response");
            assert_eq!(allowed.status(), StatusCode::NOT_IMPLEMENTED);
        }

        let _ = std::fs::remove_file(&wal_path);
    }

    #[tokio::test]
    async fn direct_search_rejects_missing_zero_and_oversized_limits() {
        let wal_path = format!("test_http_search_limit_{}.wal", uuid::Uuid::new_v4());
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new(&wal_path).expect("storage"));
        let app = secure_test_app(storage, "search-forwarding-secret");
        let authorization = basic_authorization("postgres", "secure-password");

        for (path, body, expected) in [
            (
                "/vector_search",
                r#"{"query":[0.1,0.2]}"#,
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                "/vector_search",
                r#"{"query":[0.1,0.2],"limit":0}"#,
                StatusCode::BAD_REQUEST,
            ),
            (
                "/hybrid_search",
                r#"{"text_query":"needle","vector_query":[0.1,0.2],"limit":1001}"#,
                StatusCode::BAD_REQUEST,
            ),
        ] {
            let response = app
                .clone()
                .oneshot(
                    HttpRequest::builder()
                        .method("POST")
                        .uri(path)
                        .header("content-type", "application/json")
                        .header("authorization", &authorization)
                        .body(Body::from(body))
                        .expect("search request"),
                )
                .await
                .expect("search response");
            assert_eq!(response.status(), expected);
        }

        let _ = std::fs::remove_file(&wal_path);
    }
}
