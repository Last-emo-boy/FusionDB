pub mod api;
pub mod network;
pub mod sharding;
pub mod store;
pub mod typ;

use openraft::storage::Adaptor;
use openraft::Config;
use std::sync::Arc;

pub type FusionRaft = openraft::Raft<typ::TypeConfig>;

/// Create a new Raft node for FusionDB cluster.
pub async fn new_raft_node(
    node_id: typ::NodeId,
    config: Config,
    raft_store: store::FusionRaftStore,
    network: network::FusionNetworkFactory,
) -> Result<FusionRaft, Box<dyn std::error::Error>> {
    let (log_store, state_machine) = Adaptor::new(raft_store);
    let raft =
        openraft::Raft::new(node_id, Arc::new(config), network, log_store, state_machine).await?;
    Ok(raft)
}
