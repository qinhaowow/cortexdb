use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

pub struct Cluster {
    nodes: Arc<RwLock<HashMap<String, Node>>>,
    metadata: Arc<ClusterMetadata>,
    leader: Arc<RwLock<Option<String>>>,
    membership: Arc<RwLock<MembershipState>>,
}

pub struct Node {
    id: String,
    address: SocketAddr,
    role: NodeRole,
    status: NodeStatus,
    last_heartbeat: Option<chrono::DateTime<chrono::Utc>>,
    metrics: NodeMetrics,
}

pub enum NodeRole {
    Leader,
    Follower,
    Candidate,
}

pub enum NodeStatus {
    Healthy,
    Unhealthy,
    Starting,
    Stopping,
}

pub struct NodeMetrics {
    cpu_usage: f64,
    memory_usage: usize,
    disk_usage: usize,
    request_count: usize,
    latency_ms: f64,
}

pub struct ClusterMetadata {
    cluster_id: String,
    version: String,
    topology: Topology,
    quorum_size: usize,
    election_timeout: Duration,
    heartbeat_interval: Duration,
}

pub enum Topology {
    SingleNode,
    MultiNode,
    MultiZone,
    MultiRegion,
}

pub enum MembershipState {
    Stable,
    AddingNode,
    RemovingNode,
    Rebalancing,
}

impl Cluster {
    pub fn new(cluster_id: &str) -> Self {
        let nodes = Arc::new(RwLock::new(HashMap::new()));
        let metadata = Arc::new(ClusterMetadata {
            cluster_id: cluster_id.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            topology: Topology::SingleNode,
            quorum_size: 1,
            election_timeout: Duration::from_millis(1500),
            heartbeat_interval: Duration::from_millis(150),
        });
        let leader = Arc::new(RwLock::new(None));
        let membership = Arc::new(RwLock::new(MembershipState::Stable));

        Self {
            nodes,
            metadata,
            leader,
            membership,
        }
    }

    pub fn add_node(&self, id: &str, address: SocketAddr) -> Result<(), ClusterError> {
        let node = Node {
            id: id.to_string(),
            address,
            role: NodeRole::Follower,
            status: NodeStatus::Starting,
            last_heartbeat: Some(chrono::Utc::now()),
            metrics: NodeMetrics::new(),
        };

        let mut nodes = self.nodes.write().unwrap();
        if nodes.contains_key(id) {
            return Err(ClusterError::NodeAlreadyExists(id.to_string()));
        }

        nodes.insert(id.to_string(), node);
        self.update_topology();
        Ok(())
    }

    pub fn remove_node(&self, id: &str) -> Result<(), ClusterError> {
        let mut nodes = self.nodes.write().unwrap();
        if !nodes.contains_key(id) {
            return Err(ClusterError::NodeNotFound(id.to_string()));
        }

        nodes.remove(id);
        self.update_topology();
        Ok(())
    }

    pub fn get_node(&self, id: &str) -> Result<Option<Node>, ClusterError> {
        let nodes = self.nodes.read().unwrap();
        Ok(nodes.get(id).cloned())
    }

    pub fn list_nodes(&self) -> Vec<Node> {
        let nodes = self.nodes.read().unwrap();
        nodes.values().cloned().collect()
    }

    pub fn node_count(&self) -> usize {
        self.nodes.read().unwrap().len()
    }

    pub fn set_leader(&self, node_id: &str) -> Result<(), ClusterError> {
        let nodes = self.nodes.read().unwrap();
        if !nodes.contains_key(node_id) {
            return Err(ClusterError::NodeNotFound(node_id.to_string()));
        }

        let mut leader = self.leader.write().unwrap();
        *leader = Some(node_id.to_string());

        let mut nodes_write = self.nodes.write().unwrap();
        for (id, node) in nodes_write.iter_mut() {
            if id == node_id {
                node.role = NodeRole::Leader;
            } else {
                node.role = NodeRole::Follower;
            }
        }

        Ok(())
    }

    pub fn get_leader(&self) -> Option<String> {
        let leader = self.leader.read().unwrap();
        leader.clone()
    }

    pub fn update_node_status(&self, node_id: &str, status: NodeStatus) -> Result<(), ClusterError> {
        let mut nodes = self.nodes.write().unwrap();
        if let Some(node) = nodes.get_mut(node_id) {
            node.status = status;
            Ok(())
        } else {
            Err(ClusterError::NodeNotFound(node_id.to_string()))
        }
    }

    pub fn update_node_heartbeat(&self, node_id: &str) -> Result<(), ClusterError> {
        let mut nodes = self.nodes.write().unwrap();
        if let Some(node) = nodes.get_mut(node_id) {
            node.last_heartbeat = Some(chrono::Utc::now());
            node.status = NodeStatus::Healthy;
            Ok(())
        } else {
            Err(ClusterError::NodeNotFound(node_id.to_string()))
        }
    }

    pub fn update_node_metrics(
        &self,
        node_id: &str,
        metrics: NodeMetrics,
    ) -> Result<(), ClusterError> {
        let mut nodes = self.nodes.write().unwrap();
        if let Some(node) = nodes.get_mut(node_id) {
            node.metrics = metrics;
            Ok(())
        } else {
            Err(ClusterError::NodeNotFound(node_id.to_string()))
        }
    }

    pub fn metadata(&self) -> Arc<ClusterMetadata> {
        self.metadata.clone()
    }

    pub fn check_health(&self) -> ClusterHealth {
        let nodes = self.nodes.read().unwrap();
        let healthy_nodes = nodes.values()
            .filter(|node| node.status == NodeStatus::Healthy)
            .count();

        let total_nodes = nodes.len();
        let health_score = if total_nodes > 0 {
            (healthy_nodes as f64 / total_nodes as f64) * 100.0
        } else {
            0.0
        };

        ClusterHealth {
            health_score,
            healthy_nodes,
            total_nodes,
            leader_available: self.get_leader().is_some(),
            issues: Vec::new(),
        }
    }

    fn update_topology(&self) {
        let node_count = self.node_count();
        let mut metadata = Arc::get_mut(&mut self.metadata).unwrap();
        
        metadata.topology = match node_count {
            1 => Topology::SingleNode,
            2..=4 => Topology::MultiNode,
            5..=10 => Topology::MultiZone,
            _ => Topology::MultiRegion,
        };

        metadata.quorum_size = (node_count / 2) + 1;
    }
}

pub struct ClusterHealth {
    health_score: f64,
    healthy_nodes: usize,
    total_nodes: usize,
    leader_available: bool,
    issues: Vec<ClusterIssue>,
}

pub struct ClusterIssue {
    severity: IssueSeverity,
    message: String,
    affected_nodes: Vec<String>,
}

pub enum IssueSeverity {
    Critical,
    Warning,
    Info,
}

impl NodeMetrics {
    pub fn new() -> Self {
        Self {
            cpu_usage: 0.0,
            memory_usage: 0,
            disk_usage: 0,
            request_count: 0,
            latency_ms: 0.0,
        }
    }

    pub fn update(&mut self, cpu: f64, memory: usize, disk: usize, requests: usize, latency: f64) {
        self.cpu_usage = cpu;
        self.memory_usage = memory;
        self.disk_usage = disk;
        self.request_count = requests;
        self.latency_ms = latency;
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("Node already exists: {0}")]
    NodeAlreadyExists(String),

    #[error("Node not found: {0}")]
    NodeNotFound(String),

    #[error("Invalid node address: {0}")]
    InvalidNodeAddress(String),

    #[error("Cluster is in unstable state: {0}")]
    ClusterUnstable(String),

    #[error("Insufficient quorum")]
    InsufficientQuorum,

    #[error("Internal error: {0}")]
    InternalError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_cluster_creation() {
        let cluster = Cluster::new("test-cluster");
        assert_eq!(cluster.node_count(), 0);
        assert_eq!(cluster.metadata().cluster_id, "test-cluster");
    }

    #[test]
    fn test_add_node() {
        let cluster = Cluster::new("test-cluster");
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        cluster.add_node("node1", address).unwrap();
        assert_eq!(cluster.node_count(), 1);
    }
}
