use crate::config::{Config, DistributedPeerConfig, ShardingStrategy};
use crate::distributed::typ::NodeId;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ShardNode {
    pub node_id: NodeId,
    pub addr: String,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ShardAssignment {
    pub shard_id: u64,
    pub owner_node_id: NodeId,
    pub owner_addr: String,
    pub lower_bound: Option<String>,
    pub upper_bound: Option<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ShardRoute {
    pub table: String,
    pub shard_key: String,
    pub strategy: String,
    pub shard_id: u64,
    pub owner_node_id: NodeId,
    pub owner_addr: String,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ShardMap {
    pub enabled: bool,
    pub strategy: String,
    pub shard_count: u64,
    pub local_node_id: NodeId,
    pub nodes: Vec<ShardNode>,
    pub assignments: Vec<ShardAssignment>,
}

#[derive(Clone, Debug)]
pub struct ShardRouter {
    local_node_id: NodeId,
    strategy: ShardingStrategy,
    shard_count: u64,
    range_boundaries: Vec<String>,
    nodes: Vec<ShardNode>,
    assignments: Vec<ShardAssignment>,
}

impl ShardRouter {
    pub fn from_config(config: &Config) -> Option<Self> {
        if !config.distributed.sharding.enabled {
            return None;
        }

        let nodes = shard_nodes_from_config(config);
        let strategy = config.distributed.sharding.strategy.clone();
        let mut range_boundaries = config.distributed.sharding.range_boundaries.clone();
        range_boundaries.sort();
        range_boundaries.dedup();

        let shard_count = match strategy {
            ShardingStrategy::Hash => config.distributed.sharding.shard_count.max(1),
            ShardingStrategy::Range if !range_boundaries.is_empty() => {
                range_boundaries.len() as u64 + 1
            }
            ShardingStrategy::Range => config.distributed.sharding.shard_count.max(1),
        };
        let assignments = build_assignments(&strategy, shard_count, &range_boundaries, &nodes);

        Some(Self {
            local_node_id: config.distributed.node_id,
            strategy,
            shard_count,
            range_boundaries,
            nodes,
            assignments,
        })
    }

    pub fn describe(&self) -> ShardMap {
        ShardMap {
            enabled: true,
            strategy: self.strategy_name().to_string(),
            shard_count: self.shard_count,
            local_node_id: self.local_node_id,
            nodes: self.nodes.clone(),
            assignments: self.assignments.clone(),
        }
    }

    pub fn route_key(&self, table: &str, shard_key: &str) -> ShardRoute {
        let shard_id = match self.strategy {
            ShardingStrategy::Hash => self.hash_shard(table, shard_key),
            ShardingStrategy::Range => self.range_shard(shard_key),
        };
        let assignment = &self.assignments[shard_id as usize];
        ShardRoute {
            table: table.to_string(),
            shard_key: shard_key.to_string(),
            strategy: self.strategy_name().to_string(),
            shard_id,
            owner_node_id: assignment.owner_node_id,
            owner_addr: assignment.owner_addr.clone(),
        }
    }

    pub fn strategy_name(&self) -> &'static str {
        match self.strategy {
            ShardingStrategy::Hash => "hash",
            ShardingStrategy::Range => "range",
        }
    }

    pub fn shard_count(&self) -> u64 {
        self.shard_count
    }

    fn hash_shard(&self, table: &str, shard_key: &str) -> u64 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(table.as_bytes());
        hasher.update(&[0]);
        hasher.update(shard_key.as_bytes());
        u64::from(hasher.finalize()) % self.shard_count
    }

    fn range_shard(&self, shard_key: &str) -> u64 {
        if self.range_boundaries.is_empty() {
            return 0;
        }
        self.range_boundaries
            .iter()
            .position(|upper| shard_key < upper.as_str())
            .unwrap_or(self.range_boundaries.len()) as u64
    }
}

fn shard_nodes_from_config(config: &Config) -> Vec<ShardNode> {
    let peers = if config.distributed.initial_members.is_empty() {
        vec![DistributedPeerConfig {
            node_id: config.distributed.node_id,
            addr: config.distributed.effective_advertise_addr(&config.server),
        }]
    } else {
        config.distributed.initial_members.clone()
    };

    let mut nodes: Vec<_> = peers
        .into_iter()
        .map(|peer| ShardNode {
            node_id: peer.node_id,
            addr: peer.addr,
        })
        .collect();
    nodes.sort_by_key(|node| node.node_id);
    nodes.dedup_by_key(|node| node.node_id);
    nodes
}

fn build_assignments(
    strategy: &ShardingStrategy,
    shard_count: u64,
    range_boundaries: &[String],
    nodes: &[ShardNode],
) -> Vec<ShardAssignment> {
    let mut assignments = Vec::with_capacity(shard_count as usize);
    for shard_id in 0..shard_count {
        let node = &nodes[shard_id as usize % nodes.len()];
        let (lower_bound, upper_bound) = match strategy {
            ShardingStrategy::Hash => (None, None),
            ShardingStrategy::Range if range_boundaries.is_empty() => (None, None),
            ShardingStrategy::Range => (
                shard_id
                    .checked_sub(1)
                    .and_then(|idx| range_boundaries.get(idx as usize).cloned()),
                range_boundaries.get(shard_id as usize).cloned(),
            ),
        };
        assignments.push(ShardAssignment {
            shard_id,
            owner_node_id: node.node_id,
            owner_addr: node.addr.clone(),
            lower_bound,
            upper_bound,
        });
    }
    assignments
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DistributedPeerConfig, ShardingConfig};

    fn sharded_config() -> Config {
        let mut config = Config::default();
        config.distributed.enabled = true;
        config.distributed.node_id = 1;
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
            shard_count: 4,
            range_boundaries: Vec::new(),
        };
        config
    }

    #[test]
    fn disabled_config_does_not_build_router() {
        let config = Config::default();

        assert!(ShardRouter::from_config(&config).is_none());
    }

    #[test]
    fn hash_assignments_are_round_robin_across_nodes() {
        let router = ShardRouter::from_config(&sharded_config()).unwrap();
        let map = router.describe();

        assert_eq!(map.shard_count, 4);
        assert_eq!(map.assignments[0].owner_node_id, 1);
        assert_eq!(map.assignments[1].owner_node_id, 2);
        assert_eq!(map.assignments[2].owner_node_id, 1);
        assert_eq!(map.assignments[3].owner_node_id, 2);
    }

    #[test]
    fn hash_route_is_stable_for_same_table_and_key() {
        let router = ShardRouter::from_config(&sharded_config()).unwrap();
        let first = router.route_key("users", "42");
        let second = router.route_key("users", "42");

        assert_eq!(first, second);
        assert!(first.shard_id < 4);
    }

    #[test]
    fn range_routing_uses_sorted_boundaries() {
        let mut config = sharded_config();
        config.distributed.sharding.strategy = ShardingStrategy::Range;
        config.distributed.sharding.range_boundaries =
            vec!["t".to_string(), "m".to_string(), "m".to_string()];
        let router = ShardRouter::from_config(&config).unwrap();
        let map = router.describe();

        assert_eq!(map.shard_count, 3);
        assert_eq!(router.route_key("users", "alice").shard_id, 0);
        assert_eq!(router.route_key("users", "mary").shard_id, 1);
        assert_eq!(router.route_key("users", "zoe").shard_id, 2);
        assert_eq!(map.assignments[0].upper_bound.as_deref(), Some("m"));
        assert_eq!(map.assignments[1].lower_bound.as_deref(), Some("m"));
        assert_eq!(map.assignments[1].upper_bound.as_deref(), Some("t"));
        assert_eq!(map.assignments[2].lower_bound.as_deref(), Some("t"));
    }
}
