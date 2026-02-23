use std::io::Cursor;
use openraft::BasicNode;

pub type NodeId = u64;

/// Request applied to the Raft state machine (SQL command).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Request {
    pub sql: String,
}

/// Response from the state machine after applying a request.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Response {
    pub message: String,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D            = Request,
        R            = Response,
        NodeId       = NodeId,
        Node         = BasicNode,
        Entry        = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);
