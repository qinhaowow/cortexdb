use std::sync::{Arc, RwLock};
use std::collections::{HashMap, HashSet, VecDeque};
use crate::distributed::cluster::Node;
use crate::distributed::sharding::{ShardingManager, ShardId};

pub struct QueryRouter {
    routes: Arc<RwLock<HashMap<RouteKey, Route>>>,
    sharding_manager: Arc<ShardingManager>,
    cluster_manager: Arc<dyn ClusterManager>,
    cache: Arc<RouteCache>,
    metadata: Arc<RoutingMetadata>,
}

pub struct RouteKey {
    collection: String,
    operation: OperationType,
    key: Option<Vec<u8>>,
}

pub enum OperationType {
    Read,
    Write,
    Admin,
    Analytics,
}

pub struct Route {
    node_id: String,
    shard_id: Option<ShardId>,
    priority: u32,
    health_score: f64,
    last_used: chrono::DateTime<chrono::Utc>,
    metrics: RouteMetrics,
}

pub struct RouteMetrics {
    success_count: usize,
    error_count: usize,
    latency_ms: f64,
    retry_count: usize,
}

pub trait ClusterManager: Send + Sync {
    fn get_nodes(&self) -> Vec<Node>;
    fn get_leader(&self) -> Option<String>;
    fn get_node(&self, node_id: &str) -> Option<Node>;
    fn is_node_healthy(&self, node_id: &str) -> bool;
}

pub struct RouteCache {
    entries: HashMap<RouteKey, Route>,
    lru: VecDeque<RouteKey>,
    capacity: usize,
}

pub struct RoutingMetadata {
    cache_size: usize,
    health_check_interval: std::time::Duration,
    route_expiry: std::time::Duration,
    max_retries: usize,
}

pub struct RoutingStrategy {
    prefer_leader: bool,
    use_replicas: bool,
    load_balance: bool,
    locality_aware: bool,
}

impl QueryRouter {
    pub fn new(
        sharding_manager: Arc<ShardingManager>,
        cluster_manager: Arc<dyn ClusterManager>,
    ) -> Self {
        let routes = Arc::new(RwLock::new(HashMap::new()));
        let cache = Arc::new(RouteCache::new(1000));
        let metadata = Arc::new(RoutingMetadata {
            cache_size: 1000,
            health_check_interval: std::time::Duration::from_secs(30),
            route_expiry: std::time::Duration::from_minutes(5),
            max_retries: 3,
        });

        Self {
            routes,
            sharding_manager,
            cluster_manager,
            cache,
            metadata,
        }
    }

    pub fn route(&self, collection: &str, operation: OperationType, key: Option<&[u8]>) -> Result<Route, RoutingError> {
        let route_key = RouteKey {
            collection: collection.to_string(),
            operation,
            key: key.map(|k| k.to_vec()),
        };

        if let Some(route) = self.cache.get(&route_key) {
            if self.is_route_valid(&route) {
                return Ok(route);
            }
        }

        let route = self.compute_route(&route_key)?;
        self.cache.put(route_key, route.clone());
        Ok(route)
    }

    pub fn update_route_metrics(&self, route: &Route, success: bool, latency: f64) {
        let mut routes = self.routes.write().unwrap();
        for (key, existing_route) in routes.iter_mut() {
            if existing_route.node_id == route.node_id {
                let mut metrics = &mut existing_route.metrics;
                if success {
                    metrics.success_count += 1;
                } else {
                    metrics.error_count += 1;
                }
                metrics.latency_ms = (metrics.latency_ms * 0.9 + latency * 0.1);
            }
        }
    }

    pub fn invalidate_route(&self, node_id: &str) {
        let mut routes = self.routes.write().unwrap();
        routes.retain(|_, route| route.node_id != node_id);
        self.cache.invalidate_node(node_id);
    }

    pub fn get_healthy_routes(&self) -> Vec<Route> {
        let routes = self.routes.read().unwrap();
        routes.values()
            .filter(|route| self.cluster_manager.is_node_healthy(&route.node_id))
            .cloned()
            .collect()
    }

    pub fn rebalance_routes(&self) {
        let nodes = self.cluster_manager.get_nodes();
        let healthy_nodes: Vec<Node> = nodes.into_iter()
            .filter(|node| self.cluster_manager.is_node_healthy(&node.id))
            .collect();

        let mut routes = self.routes.write().unwrap();
        for (key, route) in routes.iter_mut() {
            if !self.cluster_manager.is_node_healthy(&route.node_id) {
                let new_route = self.find_alternative_route(key, &healthy_nodes);
                if let Some(new_route) = new_route {
                    *route = new_route;
                }
            }
        }
    }

    fn compute_route(&self, key: &RouteKey) -> Result<Route, RoutingError> {
        match key.operation {
            OperationType::Write => self.route_write(key),
            OperationType::Read => self.route_read(key),
            OperationType::Admin => self.route_admin(key),
            OperationType::Analytics => self.route_analytics(key),
        }
    }

    fn route_write(&self, key: &RouteKey) -> Result<Route, RoutingError> {
        if let Some(leader) = self.cluster_manager.get_leader() {
            return Ok(Route {
                node_id: leader,
                shard_id: None,
                priority: 100,
                health_score: 1.0,
                last_used: chrono::Utc::now(),
                metrics: RouteMetrics::new(),
            });
        }

        Err(RoutingError::NoLeaderAvailable)
    }

    fn route_read(&self, key: &RouteKey) -> Result<Route, RoutingError> {
        if let Some(key_bytes) = &key.key {
            let shard_id = self.sharding_manager.get_shard(key_bytes)?;
            let shard = self.sharding_manager.shards.read().unwrap().get(&shard_id)
                .ok_or(RoutingError::ShardNotFound(shard_id))?;

            return Ok(Route {
                node_id: shard.primary_node.clone(),
                shard_id: Some(shard_id),
                priority: 90,
                health_score: 0.9,
                last_used: chrono::Utc::now(),
                metrics: RouteMetrics::new(),
            });
        }

        let leader = self.cluster_manager.get_leader().ok_or(RoutingError::NoLeaderAvailable)?;
        Ok(Route {
            node_id: leader,
            shard_id: None,
            priority: 80,
            health_score: 0.8,
            last_used: chrono::Utc::now(),
            metrics: RouteMetrics::new(),
        })
    }

    fn route_admin(&self, key: &RouteKey) -> Result<Route, RoutingError> {
        let leader = self.cluster_manager.get_leader().ok_or(RoutingError::NoLeaderAvailable)?;
        Ok(Route {
            node_id: leader,
            shard_id: None,
            priority: 100,
            health_score: 1.0,
            last_used: chrono::Utc::now(),
            metrics: RouteMetrics::new(),
        })
    }

    fn route_analytics(&self, key: &RouteKey) -> Result<Route, RoutingError> {
        let nodes = self.cluster_manager.get_nodes();
        let analytics_nodes: Vec<Node> = nodes.into_iter()
            .filter(|node| self.is_analytics_node(node))
            .collect();

        if analytics_nodes.is_empty() {
            let leader = self.cluster_manager.get_leader().ok_or(RoutingError::NoLeaderAvailable)?;
            Ok(Route {
                node_id: leader,
                shard_id: None,
                priority: 50,
                health_score: 0.7,
                last_used: chrono::Utc::now(),
                metrics: RouteMetrics::new(),
            })
        } else {
            let node = analytics_nodes.first().unwrap();
            Ok(Route {
                node_id: node.id.clone(),
                shard_id: None,
                priority: 70,
                health_score: 0.8,
                last_used: chrono::Utc::now(),
                metrics: RouteMetrics::new(),
            })
        }
    }

    fn is_route_valid(&self, route: &Route) -> bool {
        self.cluster_manager.is_node_healthy(&route.node_id)
    }

    fn find_alternative_route(&self, key: &RouteKey, healthy_nodes: &[Node]) -> Option<Route> {
        for node in healthy_nodes {
            if self.cluster_manager.is_node_healthy(&node.id) {
                return Some(Route {
                    node_id: node.id.clone(),
                    shard_id: None,
                    priority: 50,
                    health_score: 0.5,
                    last_used: chrono::Utc::now(),
                    metrics: RouteMetrics::new(),
                });
            }
        }
        None
    }

    fn is_analytics_node(&self, node: &Node) -> bool {
        true
    }
}

impl RouteCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            lru: VecDeque::new(),
            capacity,
        }
    }

    pub fn get(&self, key: &RouteKey) -> Option<Route> {
        if let Some(route) = self.entries.get(key) {
            Some(route.clone())
        } else {
            None
        }
    }

    pub fn put(&mut self, key: RouteKey, route: Route) {
        if self.entries.len() >= self.capacity {
            if let Some(old_key) = self.lru.pop_back() {
                self.entries.remove(&old_key);
            }
        }

        self.entries.insert(key.clone(), route);
        self.lru.push_front(key);
    }

    pub fn invalidate_node(&mut self, node_id: &str) {
        let keys_to_remove: Vec<RouteKey> = self.entries.iter()
            .filter(|(_, route)| route.node_id == node_id)
            .map(|(key, _)| key.clone())
            .collect();

        for key in keys_to_remove {
            self.entries.remove(&key);
            self.lru.retain(|k| k != &key);
        }
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.lru.clear();
    }

    pub fn size(&self) -> usize {
        self.entries.len()
    }
}

impl RouteMetrics {
    pub fn new() -> Self {
        Self {
            success_count: 0,
            error_count: 0,
            latency_ms: 0.0,
            retry_count: 0,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RoutingError {
    #[error("No leader available")]
    NoLeaderAvailable,

    #[error("Shard not found: {0}")]
    ShardNotFound(ShardId),

    #[error("No healthy nodes available")]
    NoHealthyNodes,

    #[error("Route not found")]
    RouteNotFound,

    #[error("Internal error: {0}")]
    InternalError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockClusterManager;

    impl ClusterManager for MockClusterManager {
        fn get_nodes(&self) -> Vec<Node> {
            Vec::new()
        }

        fn get_leader(&self) -> Option<String> {
            Some("node1".to_string())
        }

        fn get_node(&self, node_id: &str) -> Option<Node> {
            None
        }

        fn is_node_healthy(&self, node_id: &str) -> bool {
            true
        }
    }

    #[test]
    fn test_query_router() {
        let cluster_manager = Arc::new(MockClusterManager);
        let sharding_manager = Arc::new(ShardingManager::new(Box::new(HashShardingStrategy::new(16)), 3));
        let router = QueryRouter::new(sharding_manager, cluster_manager);

        let route_key = RouteKey {
            collection: "test".to_string(),
            operation: OperationType::Write,
            key: None,
        };

        let route = router.route(&route_key.collection, route_key.operation, route_key.key.as_deref()).unwrap();
        assert_eq!(route.node_id, "node1");
    }
}

use crate::distributed::sharding::HashShardingStrategy;
use crate::distributed::cluster::Node;
