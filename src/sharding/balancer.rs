use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use rand::Rng;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info, warn};

#[derive(Debug, Error)]
pub enum BalancerError {
    #[error("No nodes available")]
    NoNodesAvailable,
    #[error("Node not found: {0}")]
    NodeNotFound(String),
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),
    #[error("Health check failed: {0}")]
    HealthCheckFailed(String),
    #[error("Operation timeout")]
    OperationTimeout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalancerConfig {
    pub health_check_interval: Duration,
    pub health_check_timeout: Duration,
    pub connection_timeout: Duration,
    pub failure_threshold: u32,
    pub recovery_threshold: u32,
    pub circuit_breaker_enabled: bool,
    pub circuit_breaker_timeout: Duration,
    pub enable_metrics: bool,
    pub window_size: usize,
    pub decay_factor: f64,
}

impl Default for BalancerConfig {
    fn default() -> Self {
        Self {
            health_check_interval: Duration::from_secs(10),
            health_check_timeout: Duration::from_secs(5),
            connection_timeout: Duration::from_secs(3),
            failure_threshold: 5,
            recovery_threshold: 3,
            circuit_breaker_enabled: true,
            circuit_breaker_timeout: Duration::from_secs(30),
            enable_metrics: true,
            window_size: 100,
            decay_factor: 0.9,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalancerStats {
    pub total_requests: AtomicU64,
    pub successful_requests: AtomicU64,
    pub failed_requests: AtomicU64,
    pub node_selections: AtomicU64,
    pub health_checks: AtomicU64,
    pub node_failures: AtomicU64,
    pub node_recoveries: AtomicU64,
    pub circuit_breaker_trips: AtomicU64,
    pub avg_response_time_ns: AtomicU64,
    pub current_active_connections: AtomicUsize,
    pub avg_queue_time_ns: AtomicU64,
}

impl Default for BalancerStats {
    fn default() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            successful_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            node_selections: AtomicU64::new(0),
            health_checks: AtomicU64::new(0),
            node_failures: AtomicU64::new(0),
            node_recoveries: AtomicU64::new(0),
            circuit_breaker_trips: AtomicU64::new(0),
            avg_response_time_ns: AtomicU64::new(0),
            current_active_connections: AtomicUsize::new(0),
            avg_queue_time_ns: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetrics {
    pub requests: Vec<u64>,
    pub successes: Vec<u64>,
    pub failures: Vec<u64>,
    pub response_times: Vec<u64>,
    pub queue_times: Vec<u64>,
    pub connections: AtomicUsize,
    pub last_health_check: u64,
    pub consecutive_failures: AtomicU64,
    pub consecutive_successes: AtomicU64,
    pub circuit_breaker_tripped: AtomicUsize,
    pub circuit_breaker_reset_time: AtomicU64,
    pub score: AtomicU64,
}

impl NodeMetrics {
    pub fn new() -> Self {
        let now = std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64;
        Self {
            requests: Vec::new(),
            successes: Vec::new(),
            failures: Vec::new(),
            response_times: Vec::new(),
            queue_times: Vec::new(),
            connections: AtomicUsize::new(0),
            last_health_check: now,
            consecutive_failures: AtomicU64::new(0),
            consecutive_successes: AtomicU64::new(0),
            circuit_breaker_tripped: AtomicUsize::new(0),
            circuit_breaker_reset_time: AtomicU64::new(0),
            score: AtomicU64::new(100),
        }
    }

    pub fn failure_rate(&self) -> f64 {
        let total: u64 = self.requests.iter().sum();
        if total == 0 { 0.0 } else { self.failures.iter().sum::<u64>() as f64 / total as f64 }
    }

    pub fn avg_response_time(&self) -> u64 {
        if self.response_times.is_empty() { 0 } else {
            self.response_times.iter().sum::<u64>() / self.response_times.len() as u64
        }
    }

    pub fn update_score(&mut self, config: &BalancerConfig) {
        let failure_rate = self.failure_rate();
        let avg_time = self.avg_response_time();
        let consecutive_failures = self.consecutive_failures.load(Ordering::SeqCst);

        let mut score = 100i64;

        score -= (failure_rate * 100.0) as i64;
        score -= (avg_time / 10000) as i64;
        score -= consecutive_failures as i64 * 5;

        if self.circuit_breaker_tripped.load(Ordering::SeqCst) > 0 {
            score -= 30;
        }

        score = score.max(0).min(100) as u64;
        self.score.store(score, Ordering::SeqCst);
    }
}

pub trait LoadBalancer {
    fn select_node(&self, key: &[u8], nodes: &[&str]) -> Result<String, BalancerError>;
    fn mark_success(&self, node_id: &str);
    fn mark_failure(&self, node_id: &str);
    fn add_node(&mut self, node_id: &str, weight: Option<u32>);
    fn remove_node(&self, node_id: &str);
    fn get_stats(&self) -> HashMap<String, u64>;
    fn get_node_metrics(&self, node_id: &str) -> Option<&NodeMetrics>;
}

pub struct RoundRobinBalancer {
    nodes: Vec<String>,
    weights: HashMap<String, u32>,
    current_index: Arc<AtomicUsize>,
    stats: Arc<BalancerStats>,
    config: BalancerConfig,
}

impl RoundRobinBalancer {
    pub fn new(config: Option<BalancerConfig>) -> Self {
        Self {
            nodes: Vec::new(),
            weights: HashMap::new(),
            current_index: Arc::new(AtomicUsize::new(0)),
            stats: Arc::new(BalancerStats::default()),
            config: config.unwrap_or_default(),
        }
    }
}

impl LoadBalancer for RoundRobinBalancer {
    fn select_node(&self, _key: &[u8], nodes: &[&str]) -> Result<String, BalancerError> {
        if nodes.is_empty() {
            return Err(BalancerError::NoNodesAvailable);
        }

        self.stats.node_selections.fetch_add(1, Ordering::SeqCst);

        let idx = self.current_index.fetch_add(1, Ordering::SeqCst) % nodes.len();
        let node = nodes[idx].to_string();

        debug!("Selected node {} (index {})", node, idx);
        Ok(node)
    }

    fn mark_success(&self, node_id: &str) {
        self.stats.successful_requests.fetch_add(1, Ordering::SeqCst);
        debug!("Marked success for node {}", node_id);
    }

    fn mark_failure(&self, node_id: &str) {
        self.stats.failed_requests.fetch_add(1, Ordering::SeqCst);
        warn!("Marked failure for node {}", node_id);
    }

    fn add_node(&mut self, node_id: &str, weight: Option<u32>) {
        if !self.nodes.contains(&node_id.to_string()) {
            self.nodes.push(node_id.to_string());
            self.weights.insert(node_id.to_string(), weight.unwrap_or(1));
        }
    }

    fn remove_node(&self, node_id: &str) {
        warn!("Remove node operation not supported in RoundRobinBalancer");
    }

    fn get_stats(&self) -> HashMap<String, u64> {
        let mut stats = HashMap::new();
        stats.insert("total_requests".to_string(), self.stats.total_requests.load(Ordering::SeqCst));
        stats.insert("successful_requests".to_string(), self.stats.successful_requests.load(Ordering::SeqCst));
        stats.insert("failed_requests".to_string(), self.stats.failed_requests.load(Ordering::SeqCst));
        stats.insert("node_selections".to_string(), self.stats.node_selections.load(Ordering::SeqCst));
        stats.insert("active_nodes".to_string(), self.nodes.len() as u64);
        stats
    }

    fn get_node_metrics(&self, _node_id: &str) -> Option<&NodeMetrics> {
        None
    }
}

pub struct LeastConnectionsBalancer {
    nodes: HashMap<String, NodeMetrics>,
    stats: Arc<BalancerStats>,
    config: BalancerConfig,
}

impl LeastConnectionsBalancer {
    pub fn new(config: Option<BalancerConfig>) -> Self {
        Self {
            nodes: HashMap::new(),
            stats: Arc::new(BalancerStats::default()),
            config: config.unwrap_or_default(),
        }
    }
}

impl LoadBalancer for LeastConnectionsBalancer {
    fn select_node(&self, _key: &[u8], nodes: &[&str]) -> Result<String, BalancerError> {
        if nodes.is_empty() {
            return Err(BalancerError::NoNodesAvailable);
        }

        self.stats.node_selections.fetch_add(1, Ordering::SeqCst);

        let mut selected = nodes[0];
        let mut min_connections = usize::MAX;

        for &node in nodes {
            if let Some(metrics) = self.nodes.get(node) {
                let conns = metrics.connections.load(Ordering::SeqCst);
                if conns < min_connections {
                    min_connections = conns;
                    selected = node;
                }
            } else {
                selected = node;
                break;
            }
        }

        if let Some(metrics) = self.nodes.get_mut(selected) {
            metrics.connections.fetch_add(1, Ordering::SeqCst);
        }

        debug!("Selected node {} with {} connections", selected, min_connections);
        Ok(selected.to_string())
    }

    fn mark_success(&self, node_id: &str) {
        self.stats.successful_requests.fetch_add(1, Ordering::SeqCst);
        if let Some(metrics) = self.nodes.get_mut(node_id) {
            metrics.consecutive_failures.store(0, Ordering::SeqCst);
            metrics.consecutive_successes.fetch_add(1, Ordering::SeqCst);
            metrics.connections.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn mark_failure(&self, node_id: &str) {
        self.stats.failed_requests.fetch_add(1, Ordering::SeqCst);
        self.stats.node_failures.fetch_add(1, Ordering::SeqCst);
        if let Some(metrics) = self.nodes.get_mut(node_id) {
            metrics.consecutive_failures.fetch_add(1, Ordering::SeqCst);
            metrics.consecutive_successes.store(0, Ordering::SeqCst);
            metrics.connections.fetch_sub(1, Ordering::SeqCst);

            if metrics.consecutive_failures.load(Ordering::SeqCst) >= self.config.failure_threshold {
                if self.config.circuit_breaker_enabled {
                    metrics.circuit_breaker_tripped.store(1, Ordering::SeqCst);
                    let reset_time = std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64 
                        + self.config.circuit_breaker_timeout.as_millis() as u64;
                    metrics.circuit_breaker_reset_time.store(reset_time, Ordering::SeqCst);
                    self.stats.circuit_breaker_trips.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    }

    fn add_node(&mut self, node_id: &str, _weight: Option<u32>) {
        self.nodes.insert(node_id.to_string(), NodeMetrics::new());
    }

    fn remove_node(&self, node_id: &str) {
        self.nodes.remove(node_id);
    }

    fn get_stats(&self) -> HashMap<String, u64> {
        let mut stats = HashMap::new();
        stats.insert("total_requests".to_string(), self.stats.total_requests.load(Ordering::SeqCst));
        stats.insert("successful_requests".to_string(), self.stats.successful_requests.load(Ordering::SeqCst));
        stats.insert("failed_requests".to_string(), self.stats.failed_requests.load(Ordering::SeqCst));
        stats.insert("node_selections".to_string(), self.stats.node_selections.load(Ordering::SeqCst));
        stats.insert("node_failures".to_string(), self.stats.node_failures.load(Ordering::SeqCst));
        stats.insert("circuit_breaker_trips".to_string(), self.stats.circuit_breaker_trips.load(Ordering::SeqCst));
        stats.insert("active_nodes".to_string(), self.nodes.len() as u64);
        stats
    }

    fn get_node_metrics(&self, node_id: &str) -> Option<&NodeMetrics> {
        self.nodes.get(node_id)
    }
}

pub struct WeightedBalancer {
    nodes: HashMap<String, NodeMetrics>,
    weights: HashMap<String, u32>,
    stats: Arc<BalancerStats>,
    config: BalancerConfig,
}

impl WeightedBalancer {
    pub fn new(config: Option<BalancerConfig>) -> Self {
        Self {
            nodes: HashMap::new(),
            weights: HashMap::new(),
            stats: Arc::new(BalancerStats::default()),
            config: config.unwrap_or_default(),
        }
    }
}

impl LoadBalancer for WeightedBalancer {
    fn select_node(&self, _key: &[u8], nodes: &[&str]) -> Result<String, BalancerError> {
        if nodes.is_empty() {
            return Err(BalancerError::NoNodesAvailable);
        }

        self.stats.node_selections.fetch_add(1, Ordering::SeqCst);

        let total_weight: u32 = nodes.iter()
            .filter_map(|n| self.weights.get(*n))
            .sum();

        if total_weight == 0 {
            let idx = rand::thread_rng().gen_range(0..nodes.len());
            return Ok(nodes[idx].to_string());
        }

        let mut rng = rand::thread_rng();
        let mut r: u32 = rng.gen_range(0..total_weight);

        for &node in nodes {
            if let Some(&weight) = self.weights.get(node) {
                if r < weight {
                    if let Some(metrics) = self.nodes.get_mut(node) {
                        metrics.connections.fetch_add(1, Ordering::SeqCst);
                    }
                    return Ok(node.to_string());
                }
                r -= weight;
            }
        }

        Ok(nodes[0].to_string())
    }

    fn mark_success(&self, node_id: &str) {
        self.stats.successful_requests.fetch_add(1, Ordering::SeqCst);
        if let Some(metrics) = self.nodes.get_mut(node_id) {
            metrics.consecutive_failures.store(0, Ordering::SeqCst);
            metrics.connections.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn mark_failure(&self, node_id: &str) {
        self.stats.failed_requests.fetch_add(1, Ordering::SeqCst);
        if let Some(metrics) = self.nodes.get_mut(node_id) {
            metrics.consecutive_failures.fetch_add(1, Ordering::SeqCst);
            metrics.connections.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn add_node(&mut self, node_id: &str, weight: Option<u32>) {
        self.nodes.insert(node_id.to_string(), NodeMetrics::new());
        self.weights.insert(node_id.to_string(), weight.unwrap_or(1));
    }

    fn remove_node(&self, node_id: &str) {
        self.nodes.remove(node_id);
        self.weights.remove(node_id);
    }

    fn get_stats(&self) -> HashMap<String, u64> {
        let mut stats = HashMap::new();
        stats.insert("total_requests".to_string(), self.stats.total_requests.load(Ordering::SeqCst));
        stats.insert("successful_requests".to_string(), self.stats.successful_requests.load(Ordering::SeqCst));
        stats.insert("failed_requests".to_string(), self.stats.failed_requests.load(Ordering::SeqCst));
        stats.insert("node_selections".to_string(), self.stats.node_selections.load(Ordering::SeqCst));
        stats.insert("active_nodes".to_string(), self.nodes.len() as u64);
        stats
    }

    fn get_node_metrics(&self, node_id: &str) -> Option<&NodeMetrics> {
        self.nodes.get(node_id)
    }
}

pub struct AdaptiveBalancer {
    nodes: HashMap<String, NodeMetrics>,
    weights: HashMap<String, u32>,
    stats: Arc<BalancerStats>,
    config: BalancerConfig,
}

impl AdaptiveBalancer {
    pub fn new(config: Option<BalancerConfig>) -> Self {
        Self {
            nodes: HashMap::new(),
            weights: HashMap::new(),
            stats: Arc::new(BalancerStats::default()),
            config: config.unwrap_or_default(),
        }
    }

    fn calculate_weight(&self, node_id: &str) -> u32 {
        if let Some(metrics) = self.nodes.get(node_id) {
            let score = metrics.score.load(Ordering::SeqCst);
            let connections = metrics.connections.load(Ordering::SeqCst);
            let failure_rate = metrics.failure_rate();

            let base_weight = (score as f64 / 100.0 * 10.0) as u32;
            let connection_factor = (100u32.saturating_sub(connections as u32)) / 10;
            let failure_penalty = (failure_rate * 10.0) as u32;

            base_weight.max(1) + connection_factor.saturating_sub(failure_penalty)
        } else {
            1
        }
    }
}

impl LoadBalancer for AdaptiveBalancer {
    fn select_node(&self, _key: &[u8], nodes: &[&str]) -> Result<String, BalancerError> {
        if nodes.is_empty() {
            return Err(BalancerError::NoNodesAvailable);
        }

        self.stats.node_selections.fetch_add(1, Ordering::SeqCst);

        for node_id in nodes {
            if let Some(metrics) = self.nodes.get(node_id) {
                if metrics.circuit_breaker_tripped.load(Ordering::SeqCst) > 0 {
                    let reset_time = metrics.circuit_breaker_reset_time.load(Ordering::SeqCst);
                    let now = std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64;
                    if now < reset_time {
                        continue;
                    } else {
                        metrics.circuit_breaker_tripped.store(0, Ordering::SeqCst);
                    }
                }
            }
        }

        let available_nodes: Vec<&str> = nodes.iter()
            .filter(|&&n| {
                if let Some(metrics) = self.nodes.get(n) {
                    metrics.circuit_breaker_tripped.load(Ordering::SeqCst) == 0
                } else {
                    true
                }
            })
            .copied()
            .collect();

        if available_nodes.is_empty() {
            return Err(BalancerError::NoNodesAvailable);
        }

        let total_weight: u32 = available_nodes.iter()
            .map(|&&n| self.calculate_weight(n))
            .sum();

        if total_weight == 0 {
            let idx = rand::thread_rng().gen_range(0..available_nodes.len());
            let node = available_nodes[idx].to_string();
            if let Some(metrics) = self.nodes.get_mut(&node) {
                metrics.connections.fetch_add(1, Ordering::SeqCst);
            }
            return Ok(node);
        }

        let mut rng = rand::thread_rng();
        let mut r: u32 = rng.gen_range(0..total_weight);

        for &node in &available_nodes {
            let weight = self.calculate_weight(node);
            if r < weight {
                if let Some(metrics) = self.nodes.get_mut(node) {
                    metrics.connections.fetch_add(1, Ordering::SeqCst);
                }
                return Ok(node.to_string());
            }
            r -= weight;
        }

        let node = available_nodes[0].to_string();
        if let Some(metrics) = self.nodes.get_mut(&node) {
            metrics.connections.fetch_add(1, Ordering::SeqCst);
        }
        Ok(node)
    }

    fn mark_success(&self, node_id: &str) {
        self.stats.successful_requests.fetch_add(1, Ordering::SeqCst);
        if let Some(metrics) = self.nodes.get_mut(node_id) {
            metrics.consecutive_failures.store(0, Ordering::SeqCst);
            metrics.consecutive_successes.fetch_add(1, Ordering::SeqCst);
            metrics.connections.fetch_sub(1, Ordering::SeqCst);
            metrics.update_score(&self.config);
        }
    }

    fn mark_failure(&self, node_id: &str) {
        self.stats.failed_requests.fetch_add(1, Ordering::SeqCst);
        if let Some(metrics) = self.nodes.get_mut(node_id) {
            metrics.consecutive_failures.fetch_add(1, Ordering::SeqCst);
            metrics.consecutive_successes.store(0, Ordering::SeqCst);
            metrics.connections.fetch_sub(1, Ordering::SeqCst);

            let failures = metrics.consecutive_failures.load(Ordering::SeqCst);
            if failures >= self.config.failure_threshold {
                if self.config.circuit_breaker_enabled {
                    metrics.circuit_breaker_tripped.store(1, Ordering::SeqCst);
                    let reset_time = std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64
                        + self.config.circuit_breaker_timeout.as_millis() as u64;
                    metrics.circuit_breaker_reset_time.store(reset_time, Ordering::SeqCst);
                    self.stats.circuit_breaker_trips.fetch_add(1, Ordering::SeqCst);
                }
            }
            metrics.update_score(&self.config);
        }
    }

    fn add_node(&mut self, node_id: &str, _weight: Option<u32>) {
        self.nodes.insert(node_id.to_string(), NodeMetrics::new());
    }

    fn remove_node(&self, node_id: &str) {
        self.nodes.remove(node_id);
        self.weights.remove(node_id);
    }

    fn get_stats(&self) -> HashMap<String, u64> {
        let mut stats = HashMap::new();
        stats.insert("total_requests".to_string(), self.stats.total_requests.load(Ordering::SeqCst));
        stats.insert("successful_requests".to_string(), self.stats.successful_requests.load(Ordering::SeqCst));
        stats.insert("failed_requests".to_string(), self.stats.failed_requests.load(Ordering::SeqCst));
        stats.insert("node_selections".to_string(), self.stats.node_selections.load(Ordering::SeqCst));
        stats.insert("circuit_breaker_trips".to_string(), self.stats.circuit_breaker_trips.load(Ordering::SeqCst));
        stats.insert("active_nodes".to_string(), self.nodes.len() as u64);
        
        let avg_score: u64 = if !self.nodes.is_empty() {
            let total: u64 = self.nodes.values()
                .map(|m| m.score.load(Ordering::SeqCst))
                .sum();
            total / self.nodes.len() as u64
        } else {
            0
        };
        stats.insert("avg_node_score".to_string(), avg_score);

        stats
    }

    fn get_node_metrics(&self, node_id: &str) -> Option<&NodeMetrics> {
        self.nodes.get(node_id)
    }
}

pub struct ShardBalancer {
    load_balancers: HashMap<String, Box<dyn LoadBalancer + Send + Sync>>,
    sharding_key_type: ShardingKeyType,
    default_balancer: Box<dyn LoadBalancer + Send + Sync>,
    stats: Arc<BalancerStats>,
    config: BalancerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ShardingKeyType {
    Hash(String),
    Range(String),
    Directory(String),
}

impl Default for ShardBalancer {
    fn default() -> Self {
        Self {
            load_balancers: HashMap::new(),
            sharding_key_type: ShardingKeyType::Hash("id".to_string()),
            default_balancer: Box::new(LeastConnectionsBalancer::new(None)),
            stats: Arc::new(BalancerStats::default()),
            config: BalancerConfig::default(),
        }
    }
}

impl ShardBalancer {
    pub fn new(config: Option<BalancerConfig>) -> Self {
        Self {
            load_balancers: HashMap::new(),
            sharding_key_type: ShardingKeyType::Hash("id".to_string()),
            default_balancer: Box::new(LeastConnectionsBalancer::new(config.clone())),
            stats: Arc::new(BalancerStats::default()),
            config: config.unwrap_or_default(),
        }
    }

    pub fn select_shard(&self, key: &[u8], shard_id: &str) -> Result<String, BalancerError> {
        let balancer = self.load_balancers.get(shard_id)
            .unwrap_or(&self.default_balancer);
        
        let nodes: Vec<&str> = self.load_balancers.keys()
            .filter(|s| s.starts_with(shard_id))
            .map(|s| s.as_str())
            .collect();

        if nodes.is_empty() {
            return Err(BalancerError::NoNodesAvailable);
        }

        balancer.select_node(key, &nodes)
    }

    pub fn register_shard(&mut self, shard_id: &str, nodes: &[&str], weights: Option<HashMap<&str, u32>>) {
        let balancer = LeastConnectionsBalancer::new(Some(self.config.clone()));
        self.load_balancers.insert(shard_id.to_string(), Box::new(balancer));

        for &node in nodes {
            self.load_balancers.get_mut(shard_id).unwrap().add_node(node, weights.and_then(|w| w.get(&node).copied()));
        }
    }

    pub fn deregister_shard(&self, shard_id: &str) {
        self.load_balancers.remove(shard_id);
    }

    pub fn get_shard_nodes(&self, shard_id: &str) -> Vec<String> {
        self.load_balancers.get(shard_id)
            .map(|balancer| {
                let stats = balancer.get_stats();
                stats.get("active_nodes")
                    .map(|&n| (0..n).map(|i| format!("{}-node-{}", shard_id, i)).collect())
                    .unwrap_or_default()
            })
            .unwrap_or_default()
    }
}

impl LoadBalancer for ShardBalancer {
    fn select_node(&self, key: &[u8], nodes: &[&str]) -> Result<String, BalancerError> {
        self.default_balancer.select_node(key, nodes)
    }

    fn mark_success(&self, node_id: &str) {
        self.default_balancer.mark_success(node_id);
    }

    fn mark_failure(&self, node_id: &str) {
        self.default_balancer.mark_failure(node_id);
    }

    fn add_node(&mut self, node_id: &str, weight: Option<u32>) {
        self.default_balancer.add_node(node_id, weight);
    }

    fn remove_node(&self, node_id: &str) {
        self.default_balancer.remove_node(node_id);
    }

    fn get_stats(&self) -> HashMap<String, u64> {
        let mut stats = self.default_balancer.get_stats();
        stats.insert("shards_registered".to_string(), self.load_balancers.len() as u64);
        stats
    }

    fn get_node_metrics(&self, node_id: &str) -> Option<&NodeMetrics> {
        self.default_balancer.get_node_metrics(node_id)
    }
}
