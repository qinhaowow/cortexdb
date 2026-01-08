use std::sync::{Arc, RwLock};
use std::collections::{HashMap, HashSet};
use crate::distributed::cluster::Node;

pub struct ShardingManager {
    shards: Arc<RwLock<HashMap<ShardId, Shard>>>,
    nodes: Arc<RwLock<HashMap<String, Node>>>,
    strategy: Box<dyn ShardingStrategy>,
    metadata: Arc<ShardingMetadata>,
}

pub type ShardId = usize;

pub struct Shard {
    id: ShardId,
    primary_node: String,
    replica_nodes: Vec<String>,
    status: ShardStatus,
    size: usize,
    range: Option<KeyRange>,
    metrics: ShardMetrics,
}

pub enum ShardStatus {
    Active,
    Inactive,
    Migrating,
    Rebalancing,
    Recovering,
}

pub struct KeyRange {
    start: Vec<u8>,
    end: Vec<u8>,
}

pub struct ShardMetrics {
    read_count: usize,
    write_count: usize,
    latency_ms: f64,
    error_count: usize,
    replication_lag_ms: f64,
}

pub trait ShardingStrategy: Send + Sync {
    fn compute_shard(&self, key: &[u8]) -> ShardId;
    fn create_shards(&self, node_count: usize) -> Vec<ShardId>;
    fn should_split(&self, shard: &Shard) -> bool;
    fn should_merge(&self, shards: &[Shard]) -> bool;
    fn rebalance(&self, shards: &[Shard], nodes: &[Node]) -> Vec<ShardMovement>;
}

pub struct ShardingMetadata {
    shard_count: usize,
    replication_factor: usize,
    split_threshold: usize,
    merge_threshold: usize,
    max_shards_per_node: usize,
}

pub struct ShardMovement {
    shard_id: ShardId,
    from_node: String,
    to_node: String,
    movement_type: MovementType,
}

pub enum MovementType {
    PrimaryTransfer,
    ReplicaAdd,
    ReplicaRemove,
    ShardSplit,
    ShardMerge,
}

pub struct HashShardingStrategy {
    num_shards: usize,
}

impl HashShardingStrategy {
    pub fn new(num_shards: usize) -> Self {
        Self { num_shards }
    }
}

impl ShardingStrategy for HashShardingStrategy {
    fn compute_shard(&self, key: &[u8]) -> ShardId {
        let hash = self.hash_key(key);
        hash % self.num_shards
    }

    fn create_shards(&self, node_count: usize) -> Vec<ShardId> {
        (0..self.num_shards).collect()
    }

    fn should_split(&self, shard: &Shard) -> bool {
        shard.size > 1024 * 1024 * 1024 // 1GB
    }

    fn should_merge(&self, shards: &[Shard]) -> bool {
        let total_size: usize = shards.iter().map(|s| s.size).sum();
        total_size < 512 * 1024 * 1024 // 512MB
    }

    fn rebalance(&self, shards: &[Shard], nodes: &[Node]) -> Vec<ShardMovement> {
        let mut movements = Vec::new();
        let ideal_shards_per_node = shards.len() / nodes.len();
        
        let mut node_shard_counts = HashMap::new();
        for shard in shards {
            *node_shard_counts.entry(&shard.primary_node).or_insert(0) += 1;
        }

        // Find underutilized nodes
        let mut underutilized_nodes = Vec::new();
        for node in nodes {
            let count = node_shard_counts.get(&node.id).unwrap_or(&0);
            if *count < ideal_shards_per_node {
                underutilized_nodes.push(node.id.clone());
            }
        }

        // Move excess shards from overutilized to underutilized nodes
        for (node_id, count) in node_shard_counts {
            if count > ideal_shards_per_node && !underutilized_nodes.is_empty() {
                let excess = count - ideal_shards_per_node;
                for _ in 0..excess {
                    if let Some(target_node) = underutilized_nodes.pop() {
                        // Find a shard to move
                        if let Some(shard) = shards.iter().find(|s| s.primary_node == *node_id) {
                            let movement = ShardMovement {
                                shard_id: shard.id,
                                from_node: node_id.to_string(),
                                to_node: target_node.clone(),
                                movement_type: MovementType::PrimaryTransfer,
                            };
                            movements.push(movement);
                        }
                    }
                }
            }
        }

        movements
    }
}

impl HashShardingStrategy {
    fn hash_key(&self, key: &[u8]) -> ShardId {
        let mut hash = 0;
        for &byte in key {
            hash = (hash << 5) - hash + byte as ShardId;
        }
        hash
    }
}

pub struct RangeShardingStrategy {
    num_shards: usize,
}

impl RangeShardingStrategy {
    pub fn new(num_shards: usize) -> Self {
        Self { num_shards }
    }
}

impl ShardingStrategy for RangeShardingStrategy {
    fn compute_shard(&self, key: &[u8]) -> ShardId {
        let range = self.key_to_range(key);
        range % self.num_shards
    }

    fn create_shards(&self, node_count: usize) -> Vec<ShardId> {
        (0..self.num_shards).collect()
    }

    fn should_split(&self, shard: &Shard) -> bool {
        shard.size > 1024 * 1024 * 1024 // 1GB
    }

    fn should_merge(&self, shards: &[Shard]) -> bool {
        let total_size: usize = shards.iter().map(|s| s.size).sum();
        total_size < 512 * 1024 * 1024 // 512MB
    }

    fn rebalance(&self, shards: &[Shard], nodes: &[Node]) -> Vec<ShardMovement> {
        Vec::new()
    }
}

impl RangeShardingStrategy {
    fn key_to_range(&self, key: &[u8]) -> ShardId {
        let mut value = 0;
        for &byte in key {
            value = (value << 8) | byte as ShardId;
        }
        value
    }
}

impl ShardingManager {
    pub fn new(strategy: Box<dyn ShardingStrategy>, replication_factor: usize) -> Self {
        let shards = Arc::new(RwLock::new(HashMap::new()));
        let nodes = Arc::new(RwLock::new(HashMap::new()));
        let metadata = Arc::new(ShardingMetadata {
            shard_count: 0,
            replication_factor,
            split_threshold: 1024 * 1024 * 1024, // 1GB
            merge_threshold: 512 * 1024 * 1024,  // 512MB
            max_shards_per_node: 100,
        });

        Self {
            shards,
            nodes,
            strategy,
            metadata,
        }
    }

    pub fn create_shards(&self, node_count: usize) -> Result<(), ShardingError> {
        let shard_ids = self.strategy.create_shards(node_count);
        let mut shards = self.shards.write().unwrap();

        for shard_id in shard_ids {
            let shard = Shard {
                id: shard_id,
                primary_node: "node1".to_string(),
                replica_nodes: Vec::new(),
                status: ShardStatus::Active,
                size: 0,
                range: None,
                metrics: ShardMetrics::new(),
            };
            shards.insert(shard_id, shard);
        }

        Ok(())
    }

    pub fn get_shard(&self, key: &[u8]) -> Result<ShardId, ShardingError> {
        let shard_id = self.strategy.compute_shard(key);
        Ok(shard_id)
    }

    pub fn add_shard(&self, shard: Shard) -> Result<(), ShardingError> {
        let mut shards = self.shards.write().unwrap();
        if shards.contains_key(&shard.id) {
            return Err(ShardingError::ShardAlreadyExists(shard.id));
        }
        shards.insert(shard.id, shard);
        Ok(())
    }

    pub fn remove_shard(&self, shard_id: ShardId) -> Result<(), ShardingError> {
        let mut shards = self.shards.write().unwrap();
        if !shards.contains_key(&shard_id) {
            return Err(ShardingError::ShardNotFound(shard_id));
        }
        shards.remove(&shard_id);
        Ok(())
    }

    pub fn rebalance(&self) -> Result<Vec<ShardMovement>, ShardingError> {
        let shards = self.shards.read().unwrap();
        let nodes = self.nodes.read().unwrap();

        let shards_vec: Vec<Shard> = shards.values().cloned().collect();
        let nodes_vec: Vec<Node> = nodes.values().cloned().collect();

        let movements = self.strategy.rebalance(&shards_vec, &nodes_vec);
        Ok(movements)
    }

    pub fn split_shard(&self, shard_id: ShardId) -> Result<(ShardId, ShardId), ShardingError> {
        let mut shards = self.shards.write().unwrap();
        let shard = shards.get(&shard_id).ok_or(ShardingError::ShardNotFound(shard_id))?;

        if !self.strategy.should_split(shard) {
            return Err(ShardingError::SplitNotNeeded(shard_id));
        }

        let new_shard_id1 = shard_id * 2;
        let new_shard_id2 = shard_id * 2 + 1;

        let new_shard1 = Shard {
            id: new_shard_id1,
            primary_node: shard.primary_node.clone(),
            replica_nodes: shard.replica_nodes.clone(),
            status: ShardStatus::Active,
            size: shard.size / 2,
            range: None,
            metrics: ShardMetrics::new(),
        };

        let new_shard2 = Shard {
            id: new_shard_id2,
            primary_node: shard.primary_node.clone(),
            replica_nodes: shard.replica_nodes.clone(),
            status: ShardStatus::Active,
            size: shard.size / 2,
            range: None,
            metrics: ShardMetrics::new(),
        };

        shards.remove(&shard_id);
        shards.insert(new_shard_id1, new_shard1);
        shards.insert(new_shard_id2, new_shard2);

        Ok((new_shard_id1, new_shard_id2))
    }

    pub fn update_shard_status(&self, shard_id: ShardId, status: ShardStatus) -> Result<(), ShardingError> {
        let mut shards = self.shards.write().unwrap();
        if let Some(shard) = shards.get_mut(&shard_id) {
            shard.status = status;
            Ok(())
        } else {
            Err(ShardingError::ShardNotFound(shard_id))
        }
    }

    pub fn shard_count(&self) -> usize {
        self.shards.read().unwrap().len()
    }

    pub fn get_shards(&self) -> Vec<Shard> {
        self.shards.read().unwrap().values().cloned().collect()
    }
}

impl ShardMetrics {
    pub fn new() -> Self {
        Self {
            read_count: 0,
            write_count: 0,
            latency_ms: 0.0,
            error_count: 0,
            replication_lag_ms: 0.0,
        }
    }

    pub fn update(&mut self, reads: usize, writes: usize, latency: f64, errors: usize, lag: f64) {
        self.read_count = reads;
        self.write_count = writes;
        self.latency_ms = latency;
        self.error_count = errors;
        self.replication_lag_ms = lag;
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ShardingError {
    #[error("Shard already exists: {0}")]
    ShardAlreadyExists(ShardId),

    #[error("Shard not found: {0}")]
    ShardNotFound(ShardId),

    #[error("Split not needed for shard: {0}")]
    SplitNotNeeded(ShardId),

    #[error("Merge not possible")]
    MergeNotPossible,

    #[error("Insufficient nodes for replication")]
    InsufficientNodes,

    #[error("Internal error: {0}")]
    InternalError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_sharding() {
        let strategy = HashShardingStrategy::new(16);
        let shard1 = strategy.compute_shard(b"key1");
        let shard2 = strategy.compute_shard(b"key1");
        assert_eq!(shard1, shard2);
    }

    #[test]
    fn test_sharding_manager() {
        let strategy = Box::new(HashShardingStrategy::new(16));
        let manager = ShardingManager::new(strategy, 3);
        manager.create_shards(3).unwrap();
        assert_eq!(manager.shard_count(), 16);
    }
}
