use std::collections::BTreeSet;

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
use super::typ::{NodeId, Request, TypeConfig};
use super::FusionRaft;
use crate::execution::{Executor, QueryResult};
use crate::server::security::ForwardingAuth;

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
}

pub(crate) async fn submit_raft_write(
    raft: &FusionRaft,
    client: &reqwest::Client,
    sql: String,
    forwarding_auth: Option<&ForwardingAuth>,
    peer_scheme: &str,
) -> Result<super::typ::Response, String> {
    let raft_req = Request { sql: sql.clone() };
    match raft.client_write(raft_req).await {
        Ok(resp) => Ok(resp.data),
        Err(e) => {
            if let Some(forward) = e.forward_to_leader() {
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
            Err(format!("Raft write error: {}", e))
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
            message: body.message,
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
        }),
        Err(e) => Json(WriteResponse {
            success: false,
            message: e,
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
        }),
        Err(e) => Json(WriteResponse {
            success: false,
            message: format!("{}", e),
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
        }),
        Err(e) => Json(WriteResponse {
            success: false,
            message: format!("{}", e),
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
