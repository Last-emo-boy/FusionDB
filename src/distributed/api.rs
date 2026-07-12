use std::collections::BTreeSet;

use async_trait::async_trait;
use axum::extract::State;
use axum::response::Json;
use axum::routing::{get, post};
use axum::Router;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use std::sync::Arc;

use super::sharding::{ShardMap, ShardRoute, ShardRouter};
use super::typ::{
    KvMutation, KvPrecondition, MutationBatch, NodeId, ReplicatedQueryResult, ReplicatedValue,
    Request, Response, SideIndexMutation, TypeConfig, MUTATION_BATCH_VERSION,
};
use super::FusionRaft;
use crate::common::Result as FusionResult;
use crate::execution::{Executor, QueryResult};
use crate::server::security::ForwardingAuth;
use crate::storage::fusion::{FusionTransaction, SideIndexDelta};
use crate::storage::{ScanVisitor, StorageScanOptions, Transaction};

static RAFT_WRITE_EVALUATION_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Shared application state for Raft HTTP endpoints.
#[derive(Clone)]
pub struct RaftAppState {
    pub raft: FusionRaft,
    pub executor: Arc<Executor>,
    pub client: reqwest::Client,
    pub shard_router: Option<ShardRouter>,
    pub(crate) forwarding_auth: Option<ForwardingAuth>,
    pub(crate) peer_scheme: String,
}

/// Build the Raft API router.
pub fn raft_routes(state: RaftAppState) -> Router {
    Router::new()
        .route("/raft/append", post(raft_append))
        .route("/raft/snapshot", post(raft_snapshot))
        .route("/raft/vote", post(raft_vote))
        .route("/raft/write", post(raft_write))
        .route("/raft/query", post(raft_query))
        .route("/raft/add-learner", post(raft_add_learner))
        .route("/raft/change-membership", post(raft_change_membership))
        .route("/raft/metrics", post(raft_metrics))
        .route("/raft/shards", get(raft_shards))
        .route("/raft/shards/route", post(raft_route_shard))
        .with_state(state)
}

// --- Raft internal RPCs ---

async fn raft_append(
    State(state): State<RaftAppState>,
    Json(req): Json<AppendEntriesRequest<TypeConfig>>,
) -> Json<AppendEntriesResponse<NodeId>> {
    let resp = state.raft.append_entries(req).await.unwrap();
    Json(resp)
}

async fn raft_snapshot(
    State(state): State<RaftAppState>,
    Json(req): Json<InstallSnapshotRequest<TypeConfig>>,
) -> Json<InstallSnapshotResponse<NodeId>> {
    let resp = state.raft.install_snapshot(req).await.unwrap();
    Json(resp)
}

async fn raft_vote(
    State(state): State<RaftAppState>,
    Json(req): Json<VoteRequest<NodeId>>,
) -> Json<VoteResponse<NodeId>> {
    let resp = state.raft.vote(req).await.unwrap();
    Json(resp)
}

// --- Client-facing RPCs ---

#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub struct WriteRequest {
    pub sql: String,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct WriteResponse {
    pub success: bool,
    pub message: String,
    #[serde(default)]
    pub results: Vec<ReplicatedQueryResult>,
}

struct RecordingTransaction {
    inner: Box<dyn Transaction>,
    preconditions: Vec<KvPrecondition>,
    precondition_keys: BTreeSet<Vec<u8>>,
    mutations: Vec<KvMutation>,
}

impl RecordingTransaction {
    fn new(inner: Box<dyn Transaction>) -> Self {
        Self {
            inner,
            preconditions: Vec::new(),
            precondition_keys: BTreeSet::new(),
            mutations: Vec::new(),
        }
    }

    async fn record_precondition(&mut self, key: &[u8]) -> FusionResult<()> {
        if self.precondition_keys.insert(key.to_vec()) {
            self.preconditions.push(KvPrecondition {
                key: key.to_vec(),
                expected: self.inner.get(key).await?,
            });
        }
        Ok(())
    }

    fn take_side_index_mutations(&self) -> Vec<SideIndexMutation> {
        self.inner
            .as_any()
            .downcast_ref::<FusionTransaction>()
            .map(FusionTransaction::take_side_index_deltas)
            .unwrap_or_default()
            .into_iter()
            .map(side_index_mutation_from_delta)
            .collect()
    }
}

#[async_trait]
impl Transaction for RecordingTransaction {
    async fn get(&self, key: &[u8]) -> FusionResult<Option<Vec<u8>>> {
        self.inner.get(key).await
    }

    async fn put(&mut self, key: &[u8], value: &[u8]) -> FusionResult<()> {
        self.record_precondition(key).await?;
        self.inner.put(key, value).await?;
        self.mutations.push(KvMutation::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        });
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> FusionResult<()> {
        self.record_precondition(key).await?;
        self.inner.delete(key).await?;
        self.mutations
            .push(KvMutation::Delete { key: key.to_vec() });
        Ok(())
    }

    async fn scan_prefix(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
    ) -> FusionResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_prefix(prefix, limit).await
    }

    async fn scan_prefix_with_options(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
        options: StorageScanOptions,
    ) -> FusionResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner
            .scan_prefix_with_options(prefix, limit, options)
            .await
    }

    async fn scan_prefix_parallel(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
    ) -> FusionResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_prefix_parallel(prefix, limit).await
    }

    async fn scan_prefix_parallel_with_options(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
        options: StorageScanOptions,
    ) -> FusionResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner
            .scan_prefix_parallel_with_options(prefix, limit, options)
            .await
    }

    async fn scan_prefix_for_each(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
    ) -> FusionResult<usize> {
        self.inner
            .scan_prefix_for_each(prefix, limit, visitor)
            .await
    }

    async fn scan_prefix_for_each_with_options(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
        options: StorageScanOptions,
    ) -> FusionResult<usize> {
        self.inner
            .scan_prefix_for_each_with_options(prefix, limit, visitor, options)
            .await
    }

    async fn scan_prefix_parallel_for_each(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
    ) -> FusionResult<Option<usize>> {
        self.inner
            .scan_prefix_parallel_for_each(prefix, limit, visitor)
            .await
    }

    async fn scan_prefix_parallel_for_each_with_options(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
        options: StorageScanOptions,
    ) -> FusionResult<Option<usize>> {
        self.inner
            .scan_prefix_parallel_for_each_with_options(prefix, limit, visitor, options)
            .await
    }

    async fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
    ) -> FusionResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_range(start, end, limit).await
    }

    async fn scan_range_with_options(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        options: StorageScanOptions,
    ) -> FusionResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner
            .scan_range_with_options(start, end, limit, options)
            .await
    }

    async fn scan_range_for_each(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
    ) -> FusionResult<usize> {
        self.inner
            .scan_range_for_each(start, end, limit, visitor)
            .await
    }

    async fn scan_range_for_each_with_options(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
        options: StorageScanOptions,
    ) -> FusionResult<usize> {
        self.inner
            .scan_range_for_each_with_options(start, end, limit, visitor, options)
            .await
    }

    fn supports_bounded_scan_range_reverse(&self) -> bool {
        self.inner.supports_bounded_scan_range_reverse()
    }

    async fn scan_range_reverse(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
    ) -> FusionResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_range_reverse(start, end, limit).await
    }

    async fn scan_range_reverse_with_options(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        options: StorageScanOptions,
    ) -> FusionResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner
            .scan_range_reverse_with_options(start, end, limit, options)
            .await
    }

    async fn scan_range_reverse_for_each(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
    ) -> FusionResult<usize> {
        self.inner
            .scan_range_reverse_for_each(start, end, limit, visitor)
            .await
    }

    async fn scan_range_reverse_for_each_with_options(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        visitor: &mut dyn ScanVisitor,
        options: StorageScanOptions,
    ) -> FusionResult<usize> {
        self.inner
            .scan_range_reverse_for_each_with_options(start, end, limit, visitor, options)
            .await
    }

    async fn count_prefix(&self, prefix: &[u8]) -> FusionResult<usize> {
        self.inner.count_prefix(prefix).await
    }

    async fn first(&self, start: &[u8], end: &[u8]) -> FusionResult<Option<(Vec<u8>, Vec<u8>)>> {
        self.inner.first(start, end).await
    }

    async fn last(&self, start: &[u8], end: &[u8]) -> FusionResult<Option<(Vec<u8>, Vec<u8>)>> {
        self.inner.last(start, end).await
    }

    async fn commit(self: Box<Self>) -> FusionResult<()> {
        let this = *self;
        this.inner.commit().await
    }

    async fn rollback(self: Box<Self>) -> FusionResult<()> {
        let this = *self;
        this.inner.rollback().await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.inner.as_any()
    }
}

fn side_index_mutation_from_delta(delta: SideIndexDelta) -> SideIndexMutation {
    match delta {
        SideIndexDelta::TrigramAdd {
            table,
            column,
            numeric_id,
            row_id,
            text,
        } => SideIndexMutation::TrigramAdd {
            table,
            column,
            numeric_id,
            row_id,
            text,
        },
        SideIndexDelta::TrigramRemove {
            table,
            column,
            numeric_id,
            text,
        } => SideIndexMutation::TrigramRemove {
            table,
            column,
            numeric_id,
            text,
        },
        SideIndexDelta::VectorInsert { index, id, vector } => SideIndexMutation::VectorInsert {
            index,
            id,
            vector_bits: vector.into_iter().map(f32::to_bits).collect(),
        },
        SideIndexDelta::VectorDelete { index, id } => SideIndexMutation::VectorDelete { index, id },
    }
}

fn query_result_message(result: &QueryResult) -> String {
    match result {
        QueryResult::Success { message } => message.clone(),
        QueryResult::Select { columns, rows } => {
            format!("{} columns, {} rows", columns.len(), rows.len())
        }
    }
}

fn replicated_query_result(result: QueryResult) -> ReplicatedQueryResult {
    match result {
        QueryResult::Success { message } => ReplicatedQueryResult::Success { message },
        QueryResult::Select { columns, rows } => ReplicatedQueryResult::Select {
            columns,
            rows: rows
                .into_iter()
                .map(|row| row.into_iter().map(replicated_value).collect())
                .collect(),
        },
    }
}

fn replicated_value(value: crate::common::Value) -> ReplicatedValue {
    match value {
        crate::common::Value::Null => ReplicatedValue::Null,
        crate::common::Value::Boolean(value) => ReplicatedValue::Boolean(value),
        crate::common::Value::Integer(value) => ReplicatedValue::Integer(value),
        crate::common::Value::Float(value) => ReplicatedValue::FloatBits(value.to_bits()),
        crate::common::Value::Decimal(value) => ReplicatedValue::Decimal(value),
        crate::common::Value::String(value) => ReplicatedValue::String(value),
        crate::common::Value::Date(value) => ReplicatedValue::Date(value),
        crate::common::Value::Timestamp(value) => ReplicatedValue::Timestamp(value),
        crate::common::Value::Interval(value) => ReplicatedValue::Interval(value),
        crate::common::Value::Blob(value) => ReplicatedValue::Blob(value),
        crate::common::Value::Vector(value) => {
            ReplicatedValue::VectorBits(value.into_iter().map(f32::to_bits).collect())
        }
        crate::common::Value::Array(values) => {
            ReplicatedValue::Array(values.into_iter().map(replicated_value).collect())
        }
        crate::common::Value::Object(values) => {
            let mut values: Vec<_> = values
                .into_iter()
                .map(|(key, value)| (key, replicated_value(value)))
                .collect();
            values.sort_by(|left, right| left.0.cmp(&right.0));
            ReplicatedValue::Object(values)
        }
    }
}

fn statement_is_unsafe_to_stage(statement: &sqlparser::ast::Statement) -> bool {
    use sqlparser::ast::{IndexOption, IndexType, Statement};

    match statement {
        Statement::Vacuum(_) | Statement::Copy { to: true, .. } => true,
        Statement::CreateIndex(create_index) => create_index.index_options.iter().any(|option| {
            matches!(
                option,
                IndexOption::Using(IndexType::Custom(ident))
                    if ident.value.eq_ignore_ascii_case("HNSW")
            )
        }),
        Statement::Explain { statement, .. } => statement_is_unsafe_to_stage(statement),
        _ => false,
    }
}

pub(crate) async fn evaluate_sql_to_request(
    executor: &Executor,
    sql: &str,
) -> Result<Request, String> {
    let statements = executor
        .prepare(sql)
        .map_err(|error| format!("Raft SQL parse error: {error}"))?;
    if statements.is_empty() {
        return Err(
            "Raft deterministic writes do not support legacy/custom SQL statements".to_string(),
        );
    }
    if !statements
        .iter()
        .any(Executor::statement_may_change_query_results)
    {
        return Err("Raft write endpoint requires a mutating SQL statement".to_string());
    }
    if statements.iter().any(statement_is_unsafe_to_stage) {
        return Err(
            "Raft deterministic writes do not support VACUUM, COPY TO, or CREATE INDEX USING HNSW"
                .to_string(),
        );
    }

    let inner = executor
        .storage
        .begin_transaction()
        .await
        .map_err(|error| format!("Raft leader evaluation transaction error: {error}"))?;
    let mut transaction = RecordingTransaction::new(inner);
    let mut messages = Vec::with_capacity(statements.len());
    let mut results = Vec::with_capacity(statements.len());

    for statement in &statements {
        match executor
            .execute_in_transaction(statement, &mut transaction)
            .await
        {
            Ok(result) => {
                messages.push(query_result_message(&result));
                results.push(replicated_query_result(result));
            }
            Err(error) => {
                Box::new(transaction)
                    .rollback()
                    .await
                    .map_err(|rollback_error| {
                        format!(
                            "Raft leader evaluation failed: {error}; rollback failed: {rollback_error}"
                        )
                    })?;
                return Err(format!("Raft leader evaluation failed: {error}"));
            }
        }
    }

    let side_index_mutations = transaction.take_side_index_mutations();
    let preconditions = std::mem::take(&mut transaction.preconditions);
    let mutations = std::mem::take(&mut transaction.mutations);
    Box::new(transaction)
        .rollback()
        .await
        .map_err(|error| format!("Raft leader evaluation rollback failed: {error}"))?;

    let batch = MutationBatch {
        version: MUTATION_BATCH_VERSION,
        preconditions,
        mutations,
        side_index_mutations,
        response: Response::success_with_results(messages.join("; "), results),
    };
    super::store::validate_mutation_batch(&batch)?;
    Ok(Request::MutationBatch(batch))
}

pub(crate) async fn submit_raft_write(
    raft: &FusionRaft,
    executor: &Executor,
    client: &reqwest::Client,
    sql: String,
    forwarding_auth: Option<&ForwardingAuth>,
    peer_scheme: &str,
) -> Result<super::typ::Response, String> {
    if let Err(error) = raft.ensure_linearizable().await {
        if let Some(forward) = error.forward_to_leader() {
            if let Some(node) = &forward.leader_node {
                return forward_write_to_leader(
                    client,
                    &node.addr,
                    WriteRequest { sql },
                    forwarding_auth,
                    peer_scheme,
                )
                .await;
            }
        }
        return Err(format!("Raft leader check error: {error}"));
    }

    let evaluation_guard = RAFT_WRITE_EVALUATION_LOCK.lock().await;
    if let Err(error) = raft.ensure_linearizable().await {
        drop(evaluation_guard);
        if let Some(forward) = error.forward_to_leader() {
            if let Some(node) = &forward.leader_node {
                return forward_write_to_leader(
                    client,
                    &node.addr,
                    WriteRequest { sql },
                    forwarding_auth,
                    peer_scheme,
                )
                .await;
            }
        }
        return Err(format!("Raft leader check error: {error}"));
    }

    let raft_req = evaluate_sql_to_request(executor, &sql).await?;
    match raft.client_write(raft_req).await {
        Ok(resp) if resp.data.success => Ok(resp.data),
        Ok(resp) => Err(resp.data.message),
        Err(e) => {
            drop(evaluation_guard);
            Err(format!(
                "Raft write outcome is unknown after proposal; the request was not automatically replayed: {e}"
            ))
        }
    }
}

async fn forward_write_to_leader(
    client: &reqwest::Client,
    leader_addr: &str,
    req: WriteRequest,
    forwarding_auth: Option<&ForwardingAuth>,
    peer_scheme: &str,
) -> Result<super::typ::Response, String> {
    let url = format!("{}://{}/raft/write", peer_scheme, leader_addr);
    let request = client.post(&url).json(&req);
    let request = if let Some(auth) = forwarding_auth {
        auth.apply(request, "postgres")
    } else {
        request
    };
    let resp = request
        .send()
        .await
        .map_err(|e| format!("Raft leader forwarding error: {}", e))?;
    let status = resp.status();
    let body = resp
        .json::<WriteResponse>()
        .await
        .map_err(|e| format!("Raft leader response decode error: {}", e))?;
    if status.is_success() && body.success {
        Ok(super::typ::Response {
            success: true,
            message: body.message,
            results: body.results,
        })
    } else {
        Err(body.message)
    }
}

async fn raft_write(
    State(state): State<RaftAppState>,
    Json(req): Json<WriteRequest>,
) -> Json<WriteResponse> {
    match submit_raft_write(
        &state.raft,
        &state.executor,
        &state.client,
        req.sql,
        state.forwarding_auth.as_ref(),
        &state.peer_scheme,
    )
    .await
    {
        Ok(resp) => Json(WriteResponse {
            success: true,
            message: resp.message,
            results: resp.results,
        }),
        Err(e) => Json(WriteResponse {
            success: false,
            message: e,
            results: Vec::new(),
        }),
    }
}

#[derive(serde::Deserialize)]
pub struct ReadRequest {
    pub sql: String,
    pub linearizable: Option<bool>,
}

#[derive(serde::Serialize)]
pub struct ReadResponse {
    pub success: bool,
    pub message: String,
    pub results: Vec<RaftQueryResult>,
}

#[derive(serde::Serialize)]
#[serde(tag = "type")]
pub enum RaftQueryResult {
    Select {
        columns: Vec<String>,
        rows: Vec<Vec<serde_json::Value>>,
    },
    Success {
        message: String,
    },
}

impl From<QueryResult> for RaftQueryResult {
    fn from(value: QueryResult) -> Self {
        match value {
            QueryResult::Select { columns, rows } => RaftQueryResult::Select {
                columns,
                rows: rows
                    .into_iter()
                    .map(|row| row.iter().map(|value| value.to_json()).collect())
                    .collect(),
            },
            QueryResult::Success { message } => RaftQueryResult::Success { message },
        }
    }
}

async fn raft_query(
    State(state): State<RaftAppState>,
    Json(req): Json<ReadRequest>,
) -> Json<ReadResponse> {
    match state.executor.sql_requires_raft_write(&req.sql) {
        Ok(true) => {
            return Json(ReadResponse {
                success: false,
                message:
                    "Raft query endpoint only accepts read-only SQL; use /raft/write for writes"
                        .to_string(),
                results: Vec::new(),
            });
        }
        Err(e) => {
            return Json(ReadResponse {
                success: false,
                message: format!("SQL classification error: {}", e),
                results: Vec::new(),
            });
        }
        Ok(false) => {}
    }

    if req.linearizable.unwrap_or(false) {
        if let Err(e) = state.raft.ensure_linearizable().await {
            return Json(ReadResponse {
                success: false,
                message: format!("Raft linearizable read error: {}", e),
                results: Vec::new(),
            });
        }
    }

    match state.executor.execute_sql(&req.sql).await {
        Ok(results) => Json(ReadResponse {
            success: true,
            message: "OK".to_string(),
            results: results.into_iter().map(RaftQueryResult::from).collect(),
        }),
        Err(e) => Json(ReadResponse {
            success: false,
            message: format!("Query error: {}", e),
            results: Vec::new(),
        }),
    }
}

#[derive(serde::Deserialize)]
pub struct AddLearnerRequest {
    pub node_id: NodeId,
    pub addr: String,
}

async fn raft_add_learner(
    State(state): State<RaftAppState>,
    Json(req): Json<AddLearnerRequest>,
) -> Json<WriteResponse> {
    let node = BasicNode { addr: req.addr };
    match state.raft.add_learner(req.node_id, node, true).await {
        Ok(_) => Json(WriteResponse {
            success: true,
            message: "Learner added".to_string(),
            results: Vec::new(),
        }),
        Err(e) => Json(WriteResponse {
            success: false,
            message: format!("{}", e),
            results: Vec::new(),
        }),
    }
}

#[derive(serde::Deserialize)]
pub struct ChangeMembershipRequest {
    pub members: Vec<NodeId>,
}

async fn raft_change_membership(
    State(state): State<RaftAppState>,
    Json(req): Json<ChangeMembershipRequest>,
) -> Json<WriteResponse> {
    let members: BTreeSet<NodeId> = req.members.into_iter().collect();
    match state.raft.change_membership(members, true).await {
        Ok(_) => Json(WriteResponse {
            success: true,
            message: "Membership changed".to_string(),
            results: Vec::new(),
        }),
        Err(e) => Json(WriteResponse {
            success: false,
            message: format!("{}", e),
            results: Vec::new(),
        }),
    }
}

#[derive(serde::Serialize)]
pub struct MetricsResponse {
    pub id: NodeId,
    pub state: String,
    pub current_leader: Option<NodeId>,
    pub current_term: u64,
    pub last_log_index: Option<u64>,
    pub last_applied_index: Option<u64>,
}

async fn raft_metrics(State(state): State<RaftAppState>) -> Json<MetricsResponse> {
    let m = state.raft.metrics().borrow().clone();
    Json(MetricsResponse {
        id: m.id,
        state: format!("{:?}", m.state),
        current_leader: m.current_leader,
        current_term: m.current_term,
        last_log_index: m.last_log_index,
        last_applied_index: m.last_applied.map(|id| id.index),
    })
}

#[derive(serde::Serialize)]
pub struct ShardMapResponse {
    pub enabled: bool,
    pub map: Option<ShardMap>,
}

async fn raft_shards(State(state): State<RaftAppState>) -> Json<ShardMapResponse> {
    let map = state.shard_router.as_ref().map(ShardRouter::describe);
    Json(ShardMapResponse {
        enabled: map.is_some(),
        map,
    })
}

#[derive(serde::Deserialize)]
pub struct RouteShardRequest {
    pub table: String,
    pub key: String,
}

#[derive(serde::Serialize)]
pub struct RouteShardResponse {
    pub success: bool,
    pub message: String,
    pub route: Option<ShardRoute>,
}

async fn raft_route_shard(
    State(state): State<RaftAppState>,
    Json(req): Json<RouteShardRequest>,
) -> Json<RouteShardResponse> {
    let Some(router) = state.shard_router.as_ref() else {
        return Json(RouteShardResponse {
            success: false,
            message: "sharding is disabled".to_string(),
            route: None,
        });
    };

    if req.table.trim().is_empty() || req.key.trim().is_empty() {
        return Json(RouteShardResponse {
            success: false,
            message: "table and key are required".to_string(),
            route: None,
        });
    }

    Json(RouteShardResponse {
        success: true,
        message: "OK".to_string(),
        route: Some(router.route_key(req.table.trim(), req.key.trim())),
    })
}
