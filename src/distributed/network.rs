use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::RaftNetworkFactory;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetwork};

use super::typ::{NodeId, TypeConfig};

/// Network factory that creates HTTP-based connections to peer nodes.
pub struct FusionNetworkFactory {
    pub client: reqwest::Client,
}

impl FusionNetworkFactory {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

pub struct FusionNetwork {
    #[allow(dead_code)]
    target: NodeId,
    target_addr: String,
    client: reqwest::Client,
}

impl RaftNetworkFactory<TypeConfig> for FusionNetworkFactory {
    type Network = FusionNetwork;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        FusionNetwork {
            target,
            target_addr: node.addr.clone(),
            client: self.client.clone(),
        }
    }
}

impl RaftNetwork<TypeConfig> for FusionNetwork {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let url = format!("http://{}/raft/append", self.target_addr);
        let resp = self
            .client
            .post(&url)
            .json(&req)
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
        let url = format!("http://{}/raft/snapshot", self.target_addr);
        let resp = self
            .client
            .post(&url)
            .json(&req)
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
        let url = format!("http://{}/raft/vote", self.target_addr);
        let resp = self
            .client
            .post(&url)
            .json(&req)
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
