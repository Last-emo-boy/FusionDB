use std::collections::BTreeSet;

use axum::extract::State;
use axum::response::Json;
use axum::routing::post;
use axum::Router;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::BasicNode;

use super::typ::{NodeId, Request, TypeConfig};
use super::FusionRaft;

/// Shared application state for Raft HTTP endpoints.
#[derive(Clone)]
pub struct RaftAppState {
    pub raft: FusionRaft,
}

/// Build the Raft API router.
pub fn raft_routes(state: RaftAppState) -> Router {
    Router::new()
        .route("/raft/append", post(raft_append))
        .route("/raft/snapshot", post(raft_snapshot))
        .route("/raft/vote", post(raft_vote))
        .route("/raft/write", post(raft_write))
        .route("/raft/add-learner", post(raft_add_learner))
        .route("/raft/change-membership", post(raft_change_membership))
        .route("/raft/metrics", post(raft_metrics))
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

#[derive(serde::Deserialize)]
pub struct WriteRequest {
    pub sql: String,
}

#[derive(serde::Serialize)]
pub struct WriteResponse {
    pub success: bool,
    pub message: String,
}

async fn raft_write(
    State(state): State<RaftAppState>,
    Json(req): Json<WriteRequest>,
) -> Json<WriteResponse> {
    let raft_req = Request { sql: req.sql };
    match state.raft.client_write(raft_req).await {
        Ok(resp) => Json(WriteResponse {
            success: true,
            message: resp.data.message,
        }),
        Err(e) => Json(WriteResponse {
            success: false,
            message: format!("Raft write error: {}", e),
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
    let node = BasicNode {
        addr: req.addr,
    };
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

async fn raft_metrics(
    State(state): State<RaftAppState>,
) -> Json<MetricsResponse> {
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
