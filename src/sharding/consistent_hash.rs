use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use rand::Rng;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info, warn};

#[derive(Debug, Error)]
pub enum HashError {
    #[error("No nodes available")]
    NoNodesAvailable,
    #[error("Node not found: {0}")]
    NodeNotFound(String),
    #[error("Invalid virtual node count: {0}")]
    InvalidVirtualNodeCount(usize),
    #[error("Hash computation failed")]
    HashComputationFailed,
}

pub trait HashFunction {
    fn hash(&self, data: &[u8]) -> u64;
    fn hash_str(&self, data: &str) -> u64;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MurmurHash3 {
    seed: u32,
}

impl MurmurHash3 {
    pub fn new(seed: u32) -> Self {
        Self { seed }
    }

    fn murmur3_hash(&self, data: &[u8]) -> u64 {
        let mut hasher = Murmur3Hasher::with_seed(self.seed);
        hasher.write(data);
        hasher.finish()
    }
}

impl HashFunction for MurmurHash3 {
    fn hash(&self, data: &[u8]) -> u64 {
        self.murmur3_hash(data)
    }

    fn hash_str(&self, data: &str) -> u64 {
        self.murmur3_hash(data.as_bytes())
    }
}

struct Murmur3Hasher {
    h1: u32,
    c1: u32,
    c2: u32,
    len: u32,
}

impl Murmur3Hasher {
    fn with_seed(seed: u32) -> Self {
        Self {
            h1: seed,
            c1: 0xcc9e2d51,
            c2: 0x1b873593,
            len: 0,
        }
    }
}

impl std::hash::Hasher for Murmur3Hasher {
    fn finish(&self) -> u64 {
        let mut h1 = self.h1;
        h1 ^= self.len as u32;
        h1 = h1.wrapping_mul(0x85ebca6b);
        h1 ^= h1 >> 13;
        h1 = h1.wrapping_mul(0xc2b2ae35);
        h1 ^= h1 >> 16;
        (h1 as u64) << 32 | (h1 as u64)
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut h1 = self.h1;
        let mut length = self.len;
        let mut k1: u32;

        let mut i = 0;
        while i + 3 < bytes.len() {
            k1 = ((bytes[i] as u32) 
                | ((bytes[i+1] as u32) << 8) 
                | ((bytes[i+2] as u32) << 16) 
                | ((bytes[i+3] as u32) << 24)) as u32;

            k1 = k1.wrapping_mul(self.c1);
            k1 = (k1 << 15) | (k1 >> 17);
            k1 = k1.wrapping_mul(self.c2);

            h1 ^= k1;
            h1 = (h1 << 13) | (h1 >> 19);
            h1 = h1.wrapping_mul(5).wrapping_add(0xe6546b64);

            length += 4;
            i += 4;
        }

        self.h1 = h1;
        self.len = length;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MD5Hash;

impl MD5Hash {
    pub fn new() -> Self {
        Self
    }
}

impl HashFunction for MD5Hash {
    fn hash(&self, data: &[u8]) -> u64 {
        let mut hasher = md5::Context::new();
        hasher.consume(data);
        let digest = hasher.compute();
        u64::from_be_bytes([
            digest[0], digest[1], digest[2], digest[3],
            digest[4], digest[5], digest[6], digest[7]
        ])
    }

    fn hash_str(&self, data: &str) -> u64 {
        self.hash(data.as_bytes())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub weight: u32,
    pub metadata: HashMap<String, String>,
    pub last_heartbeat: u64,
    pub status: NodeStatus,
}

impl Node {
    pub fn new(id: &str, weight: u32) -> Self {
        let now = std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64;
        Self {
            id: id.to_string(),
            weight,
            metadata: HashMap::new(),
            last_heartbeat: now,
            status: NodeStatus::Active,
        }
    }

    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum NodeStatus {
    Active,
    Draining,
    Offline,
    Maintenance,
}

#[derive(Debug, Clone)]
pub struct VirtualNode {
    pub node_id: String,
    pub hash: u64,
    pub virtual_index: u32,
}

impl VirtualNode {
    pub fn new(node_id: &str, hash: u64, virtual_index: u32) -> Self {
        Self {
            node_id: node_id.to_string(),
            hash,
            virtual_index,
        }
    }
}

impl PartialEq for VirtualNode {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for VirtualNode {}

impl PartialOrd for VirtualNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VirtualNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.hash.cmp(&other.hash)
    }
}

pub struct HashRing {
    nodes: BTreeMap<u64, VirtualNode>,
    node_map: HashMap<String, Node>,
    virtual_node_counts: HashMap<String, u32>,
    hash_function: Box<dyn HashFunction + Send + Sync>,
    num_virtual_nodes: u32,
    stats: RingStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RingStats {
    pub ring_size: AtomicUsize,
    pub lookups: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub node_additions: AtomicU64,
    pub node_removals: AtomicU64,
    pub rebalancing_events: AtomicU64,
    pub avg_lookup_steps: AtomicU64,
}

impl Default for RingStats {
    fn default() -> Self {
        Self {
            ring_size: AtomicUsize::new(0),
            lookups: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            node_additions: AtomicU64::new(0),
            node_removals: AtomicU64::new(0),
            rebalancing_events: AtomicU64::new(0),
            avg_lookup_steps: AtomicU64::new(0),
        }
    }
}

impl HashRing {
    pub fn new(num_virtual_nodes: u32, hash_function: Box<dyn HashFunction + Send + Sync>) -> Result<Self, HashError> {
        if num_virtual_nodes == 0 {
            return Err(HashError::InvalidVirtualNodeCount(num_virtual_nodes as usize));
        }

        Ok(Self {
            nodes: BTreeMap::new(),
            node_map: HashMap::new(),
            virtual_node_counts: HashMap::new(),
            hash_function,
            num_virtual_nodes,
            stats: RingStats::default(),
        })
    }

    pub fn add_node(&mut self, node: Node) {
        let node_id = node.id.clone();
        let count = self.virtual_node_counts.entry(node_id.clone()).or_insert(0);

        for i in 0..self.num_virtual_nodes {
            let virtual_key = format!("{}-{}", node_id, i);
            let hash = self.hash_function.hash_str(&virtual_key);
            
            self.nodes.insert(hash, VirtualNode::new(&node_id, hash, i));
            *count += 1;
        }

        self.node_map.insert(node_id.clone(), node);
        self.stats.node_additions.fetch_add(1, Ordering::SeqCst);
        self.stats.ring_size.store(self.nodes.len(), Ordering::SeqCst);

        debug!("Added node {} with {} virtual nodes", node_id, count);
    }

    pub fn remove_node(&mut self, node_id: &str) -> Result<(), HashError> {
        if !self.node_map.contains_key(node_id) {
            return Err(HashError::NodeNotFound(node_id.to_string()));
        }

        let mut to_remove: Vec<u64> = Vec::new();
        for (hash, vnode) in &self.nodes {
            if vnode.node_id == node_id {
                to_remove.push(*hash);
            }
        }

        for hash in to_remove {
            self.nodes.remove(&hash);
        }

        self.node_map.remove(node_id);
        self.virtual_node_counts.remove(node_id);
        self.stats.node_removals.fetch_add(1, Ordering::SeqCst);
        self.stats.ring_size.store(self.nodes.len(), Ordering::SeqCst);
        self.stats.rebalancing_events.fetch_add(1, Ordering::SeqCst);

        debug!("Removed node {}", node_id);
        Ok(())
    }

    pub fn get_node(&self, key: &[u8]) -> Result<&Node, HashError> {
        if self.nodes.is_empty() {
            return Err(HashError::NoNodesAvailable);
        }

        self.stats.lookups.fetch_add(1, Ordering::SeqCst);

        let hash = self.hash_function.hash(key);
        let vnode = self.find_closest_node(hash)?;

        self.stats.cache_hits.fetch_add(1, Ordering::SeqCst);

        self.node_map.get(&vnode.node_id)
            .ok_or_else(|| HashError::NodeNotFound(vnode.node_id.clone()))
    }

    pub fn get_nodes(&self, key: &[u8], count: usize) -> Vec<&Node> {
        if self.nodes.is_empty() {
            return vec![];
        }

        self.stats.lookups.fetch_add(1, Ordering::SeqCst);

        let hash = self.hash_function.hash(key);
        let mut results: Vec<&Node> = Vec::with_capacity(count);
        let mut seen_nodes: HashSet<String> = HashSet::new();

        if let Some(vnode) = self.find_closest_node(hash) {
            if let Some(node) = self.node_map.get(&vnode.node_id) {
                if seen_nodes.insert(node.id.clone()) {
                    results.push(node);
                }
            }
        }

        for (&h, vnode) in self.nodes.iter() {
            if results.len() >= count {
                break;
            }

            if h == hash {
                continue;
            }

            if let Some(node) = self.node_map.get(&vnode.node_id) {
                if seen_nodes.insert(node.id.clone()) {
                    results.push(node);
                }
            }
        }

        if !results.is_empty() {
            self.stats.cache_hits.fetch_add(1, Ordering::SeqCst);
        } else {
            self.stats.cache_misses.fetch_add(1, Ordering::SeqCst);
        }

        results
    }

    fn find_closest_node(&self, hash: u64) -> Option<&VirtualNode> {
        let mut closest = None;
        let mut closest_dist = u64::MAX;

        for (&h, vnode) in &self.nodes {
            let dist = if h >= hash {
                h - hash
            } else {
                hash - h
            };

            if dist < closest_dist {
                closest_dist = dist;
                closest = Some(vnode);
            }
        }

        closest
    }

    pub fn update_node_weight(&mut self, node_id: &str, weight: u32) -> Result<(), HashError> {
        if let Some(node) = self.node_map.get_mut(node_id) {
            if node.weight != weight {
                self.remove_node(node_id)?;
                node.weight = weight;
                self.add_node(node.clone());
            }
            Ok(())
        } else {
            Err(HashError::NodeNotFound(node_id.to_string()))
        }
    }

    pub fn get_virtual_node_count(&self, node_id: &str) -> u32 {
        self.virtual_node_counts.get(node_id).copied().unwrap_or(0)
    }

    pub fn set_virtual_node_count(&mut self, node_id: &str, count: u32) -> Result<(), HashError> {
        if let Some(current) = self.virtual_node_counts.get(&node_id) {
            if *current != count {
                self.remove_node(node_id)?;
                for i in 0..count {
                    let virtual_key = format!("{}-{}", node_id, i);
                    let hash = self.hash_function.hash_str(&virtual_key);
                    self.nodes.insert(hash, VirtualNode::new(node_id, hash, i));
                }
                self.virtual_node_counts.insert(node_id.to_string(), count);
                if let Some(node) = self.node_map.get_mut(node_id) {
                    node.weight = count;
                }
                self.stats.rebalancing_events.fetch_add(1, Ordering::SeqCst);
            }
        } else {
            return Err(HashError::NodeNotFound(node_id.to_string()));
        }

        self.stats.ring_size.store(self.nodes.len(), Ordering::SeqCst);
        Ok(())
    }

    pub fn get_all_nodes(&self) -> Vec<&Node> {
        self.node_map.values().collect()
    }

    pub fn get_active_nodes(&self) -> Vec<&Node> {
        self.node_map.values()
            .filter(|n| n.status == NodeStatus::Active)
            .collect()
    }

    pub fn get_stats(&self) -> HashMap<String, u64> {
        let mut stats = HashMap::new();
        stats.insert("ring_size".to_string(), self.nodes.len() as u64);
        stats.insert("node_count".to_string(), self.node_map.len() as u64);
        stats.insert("lookups".to_string(), self.stats.lookups.load(Ordering::SeqCst));
        stats.insert("cache_hits".to_string(), self.stats.cache_hits.load(Ordering::SeqCst));
        stats.insert("cache_misses".to_string(), self.stats.cache_misses.load(Ordering::SeqCst));
        stats.insert("node_additions".to_string(), self.stats.node_additions.load(Ordering::SeqCst));
        stats.insert("node_removals".to_string(), self.stats.node_removals.load(Ordering::SeqCst));
        stats.insert("rebalancing_events".to_string(), self.stats.rebalancing_events.load(Ordering::SeqCst));

        stats
    }

    pub fn balance_ratio(&self) -> f64 {
        if self.node_map.is_empty() {
            return 1.0;
        }

        let mut counts: HashMap<&String, u32> = HashMap::new();
        for vnode in self.nodes.values() {
            *counts.entry(&vnode.node_id).or_insert(0) += 1;
        }

        let mut values: Vec<u32> = counts.values().copied().collect();
        values.sort();
        let min = values.first().copied().unwrap_or(0) as f64;
        let max = values.last().copied().unwrap_or(0) as f64;

        if max == 0.0 { 1.0 } else { min / max }
    }

    pub fn clear(&mut self) {
        self.nodes.clear();
        self.node_map.clear();
        self.virtual_node_counts.clear();
        self.stats.ring_size.store(0, Ordering::SeqCst);
        self.stats.rebalancing_events.fetch_add(1, Ordering::SeqCst);
    }
}

pub struct ConsistentHash {
    ring: HashRing,
    replicas: usize,
    enable_jitter: bool,
}

impl ConsistentHash {
    pub fn new(num_virtual_nodes: u32, replicas: usize, enable_jitter: bool) -> Result<Self, HashError> {
        let hash_function: Box<dyn HashFunction + Send + Sync> = Box::new(MurmurHash3::new(42));
        let ring = HashRing::new(num_virtual_nodes, hash_function)?;

        Ok(Self {
            ring,
            replicas,
            enable_jitter,
        })
    }

    pub fn with_custom_hash(
        num_virtual_nodes: u32,
        replicas: usize,
        hash_function: Box<dyn HashFunction + Send + Sync>,
    ) -> Result<Self, HashError> {
        let ring = HashRing::new(num_virtual_nodes, hash_function)?;
        Ok(Self {
            ring,
            replicas,
            enable_jitter: false,
        })
    }

    pub fn add_node(&mut self, node: Node) {
        self.ring.add_node(node);
    }

    pub fn remove_node(&mut self, node_id: &str) -> Result<(), HashError> {
        self.ring.remove_node(node_id)
    }

    pub fn get_node(&self, key: &[u8]) -> Result<&Node, HashError> {
        self.ring.get_node(key)
    }

    pub fn get_primary_and_replicas(&self, key: &[u8]) -> Vec<&Node> {
        if self.replicas == 0 {
            return self.ring.get_nodes(key, 1);
        }

        self.ring.get_nodes(key, self.replicas + 1)
    }

    pub fn get_all_nodes(&self) -> Vec<&Node> {
        self.ring.get_all_nodes()
    }

    pub fn get_stats(&self) -> HashMap<String, u64> {
        let mut stats = self.ring.get_stats();
        stats.insert("replicas".to_string(), self.replicas as u64);
        stats.insert("balance_ratio".to_string(), (self.ring.balance_ratio() * 100.0) as u64);
        stats
    }

    pub fn health_check(&mut self) {
        let now = std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64;
        let timeout = Duration::from_secs(30);

        let mut offline_nodes = Vec::new();
        for node in self.ring.get_all_nodes() {
            if now - node.last_heartbeat > timeout.as_millis() as u64 {
                if node.status == NodeStatus::Active {
                    offline_nodes.push(node.id.clone());
                }
            }
        }

        for node_id in offline_nodes {
            warn!("Node {} is offline, marking as such", node_id);
            if let Some(node) = self.ring.node_map.get_mut(&node_id) {
                node.status = NodeStatus::Offline;
            }
        }
    }
}

impl Drop for HashRing {
    fn drop(&mut self) {
        let stats = &self.stats;
        info!(
            "HashRing stats - Nodes: {}, Virtual: {}, Lookups: {}, Events: {}",
            self.node_map.len(),
            self.nodes.len(),
            stats.lookups.load(Ordering::SeqCst),
            stats.rebalancing_events.load(Ordering::SeqCst)
        );
    }
}
