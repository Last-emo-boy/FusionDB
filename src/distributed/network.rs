use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::RaftNetworkFactory;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetwork};

use super::typ::{NodeId, TypeConfig};
use crate::server::security::ForwardingAuth;

/// Network factory that creates HTTP-based connections to peer nodes.
pub struct FusionNetworkFactory {
    pub client: reqwest::Client,
    forwarding_auth: Option<ForwardingAuth>,
    peer_scheme: String,
}

impl FusionNetworkFactory {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            forwarding_auth: None,
            peer_scheme: "http".to_string(),
        }
    }

    pub fn with_security(forwarding_secret: &str, peer_scheme: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            forwarding_auth: ForwardingAuth::new(forwarding_secret.as_bytes()),
            peer_scheme: peer_scheme.to_string(),
        }
    }
}

pub struct FusionNetwork {
    #[allow(dead_code)]
    target: NodeId,
    target_addr: String,
    client: reqwest::Client,
    forwarding_auth: Option<ForwardingAuth>,
    peer_scheme: String,
}

impl RaftNetworkFactory<TypeConfig> for FusionNetworkFactory {
    type Network = FusionNetwork;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        FusionNetwork {
            target,
            target_addr: node.addr.clone(),
            client: self.client.clone(),
            forwarding_auth: self.forwarding_auth.clone(),
            peer_scheme: self.peer_scheme.clone(),
        }
    }
}

impl RaftNetwork<TypeConfig> for FusionNetwork {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let url = format!("{}://{}/raft/append", self.peer_scheme, self.target_addr);
        let request = self.client.post(&url).json(&req);
        let request = if let Some(auth) = self.forwarding_auth.as_ref() {
            auth.apply(request, "postgres")
        } else {
            request
        };
        let resp = request
            .send()
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        let result: AppendEntriesResponse<NodeId> = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;
        Ok(result)
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let url = format!("{}://{}/raft/snapshot", self.peer_scheme, self.target_addr);
        let request = self.client.post(&url).json(&req);
        let request = if let Some(auth) = self.forwarding_auth.as_ref() {
            auth.apply(request, "postgres")
        } else {
            request
        };
        let resp = request
            .send()
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        let result: InstallSnapshotResponse<NodeId> = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;
        Ok(result)
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: openraft::network::RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let url = format!("{}://{}/raft/vote", self.peer_scheme, self.target_addr);
        let request = self.client.post(&url).json(&req);
        let request = if let Some(auth) = self.forwarding_auth.as_ref() {
            auth.apply(request, "postgres")
        } else {
            request
        };
        let resp = request
            .send()
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        let result: VoteResponse<NodeId> = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;
        Ok(result)
    }
}
