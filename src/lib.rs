

#![doc(html_root_url = "https://docs.rs/coretexdb/0.1.0")]

pub mod coretex_core;
pub mod coretex_storage;
pub mod coretex_index;
pub mod coretex_query;
pub mod coretex_api;
pub mod coretex_backup;
pub mod coretex_cli;
pub mod coretex_distributed;
pub mod coretex_monitor;
pub mod coretex_security;
pub mod coretex_perf;
pub mod coretex_utils;

pub mod ai;
pub mod api;
pub mod compute;
pub mod core;
pub mod distributed;
pub mod index;
pub mod query;
pub mod storage;
pub mod transaction;

pub mod scheduler;
pub mod worker;
pub mod clustering;
pub mod discovery;
pub mod coordinator;

#[cfg(feature = "python")]
pub mod python;

#[cfg(feature = "grpc")]
pub mod grpc;

#[cfg(feature = "websocket")]
pub mod websocket;

#[cfg(feature = "distributed")]
pub mod raft;

#[cfg(feature = "postgres")]
pub mod postgres;

pub use coretex_core::{Vector, Document, CollectionSchema, DistanceMetric, IndexConfig, IndexType, CoretexError, Result};
pub use coretex_storage::{StorageEngine, MemoryStorage, PersistentStorage};
pub use coretex_index::{VectorIndex, BruteForceIndex, IndexManager, SearchResult};
pub use coretex_query::{QueryType, QueryParams, QueryResult, QueryProcessor, QueryPlanner};
pub use coretex_api::rest::{start_server, ApiConfig};

#[cfg(feature = "grpc")]
pub use coretex_api::grpc::start_server as start_grpc_server;

#[cfg(feature = "distributed")]
pub use coretex_distributed::{ClusterManager, QueryCoordinator, ShardingStrategy, MetadataManager};

#[cfg(feature = "distributed")]
pub use coretex_monitor::{MetricsCollector, HealthChecker, MonitoringDashboard};

#[cfg(feature = "distributed")]
pub use coretex_backup::{BackupManager, RestoreManager, ReplicationManager};

pub use coretex_security::{AuthManager, TokenManager, PermissionManager};
pub use coretex_perf::{QueryCache, QueryRouter, ParallelQueryExecutor, QueryOptimizer};
pub use coretex_cli::run_cli;

pub use scheduler::{Scheduler, SchedulerConfig, SchedulerStats, PriorityQueue, SchedulingPolicy};
pub use worker::{TaskExecutor, ExecutorConfig, HeartbeatManager, WorkerRegistration};
pub use clustering::{StreamingKMeans, BIRCH, ANNIndex, AnnoyIndex, HNSWIndex, LSHIndex};
pub use discovery::{EtcdClient, ConsulClient, ServiceRegistration};
pub use coordinator::{LeaderElection, ElectionConfig, ElectionState};

pub mod prelude {
    pub use crate::coretex_core::*;
    pub use crate::coretex_storage::*;
    pub use crate::coretex_index::*;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_basic_structure() {
        let db = CortexDB::new().await.unwrap();
        assert!(db.storage.is_ready().await);
    }

    #[test]
    fn test_vector_creation() {
        let vector = Vector::new(vec![1.0, 2.0, 3.0]);
        assert_eq!(vector.dimension(), 3);
    }
}

pub struct CoretexDB {
    storage: Box<dyn StorageEngine>,
    index_manager: IndexManager,
    config: CoretexConfig,
}

impl CortexDB {
    pub async fn new() -> Result<Self, CoretexError> {
        let config = CoretexConfig::default();
        let storage = Box::new(MemoryStorage::new());
        storage.init().await?;
        
        let index_manager = IndexManager::new();
        
        Ok(Self {
            storage,
            index_manager,
            config,
        })
    }

    pub async fn with_config(config: CortexConfig) -> Result<Self, CortexError> {
        let storage: Box<dyn StorageEngine> = match config.storage.engine.as_str() {
            "memory" => Box::new(MemoryStorage::new()),
            "persistent" => Box::new(PersistentStorage::new(&config.storage.directory)),
            _ => Box::new(MemoryStorage::new()),
        };
        storage.init().await?;
        
        let index_manager = IndexManager::new();
        
        Ok(Self {
            storage,
            index_manager,
            config,
        })
    }

    pub async fn store(&self, id: &str, vector: &[f32], metadata: &serde_json::Value) -> Result<(), CortexError> {
        self.storage.store(id, vector, metadata).await
    }

    pub async fn retrieve(&self, id: &str) -> Result<Option<(Vec<f32>, serde_json::Value)>, CortexError> {
        self.storage.retrieve(id).await
    }

    pub async fn delete(&self, id: &str) -> Result<bool, CortexError> {
        self.storage.delete(id).await
    }

    pub async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>, CortexError> {
        self.index_manager.search(query, k).await
    }

    pub fn index_manager(&self) -> &IndexManager {
        &self.index_manager
    }

    pub fn config(&self) -> &CortexConfig {
        &self.config
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct CortexConfig {
    pub storage: StorageConfig,
    pub api: ApiConfig,
    pub distributed: DistributedConfig,
    pub security: SecurityConfig,
    pub monitoring: MonitoringConfig,
}

impl Default for CortexConfig {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            api: ApiConfig::default(),
            distributed: DistributedConfig::default(),
            security: SecurityConfig::default(),
            monitoring: MonitoringConfig::default(),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct StorageConfig {
    pub engine: String,
    pub directory: String,
    pub memory_limit: usize,
    pub enable_compression: bool,
    pub compression_algorithm: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            engine: "memory".to_string(),
            directory: "./data".to_string(),
            memory_limit: 1024 * 1024 * 1024,
            enable_compression: true,
            compression_algorithm: "lz4".to_string(),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ApiConfig {
    pub host: String,
    pub port: u16,
    pub workers: usize,
    pub tls_enabled: bool,
    pub cors_enabled: bool,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8080,
            workers: 4,
            tls_enabled: false,
            cors_enabled: true,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct DistributedConfig {
    pub enabled: bool,
    pub cluster_name: String,
    pub node_id: String,
    pub seed_nodes: Vec<String>,
    pub heartbeat_interval: Duration,
    pub gossip_interval: Duration,
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cluster_name: "cortexdb".to_string(),
            node_id: uuid::Uuid::new_v4().to_string(),
            seed_nodes: Vec::new(),
            heartbeat_interval: Duration::from_secs(5),
            gossip_interval: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct SecurityConfig {
    pub authentication_enabled: bool,
    pub authorization_enabled: bool,
    pub jwt_secret: String,
    pub jwt_expiry: Duration,
    pub encryption_key: String,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            authentication_enabled: false,
            authorization_enabled: false,
            jwt_secret: "secret".to_string(),
            jwt_expiry: Duration::from_hours(24),
            encryption_key: "key".to_string(),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct MonitoringConfig {
    pub metrics_enabled: bool,
    pub tracing_enabled: bool,
    pub log_level: String,
    pub metrics_port: u16,
    pub health_check_interval: Duration,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            metrics_enabled: true,
            tracing_enabled: true,
            log_level: "info".to_string(),
            metrics_port: 9090,
            health_check_interval: Duration::from_secs(10),
        }
    }
}

#[cfg(feature = "python")]
pub mod python {
    use pyo3::prelude::*;
    use crate::CortexDB;
    use std::sync::Arc;
    use tokio::runtime::Runtime;

    #[pyclass]
    pub struct PyCortexDB {
        db: Arc<CortexDB>,
        rt: Runtime,
    }

    #[pymethods]
    impl PyCortexDB {
        #[new]
        fn new() -> PyResult<Self> {
            let rt = Runtime::new().map_err(|e| PyErr::from(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
            let db = rt.block_on(async { CortexDB::new().await.map_err(|e| PyErr::from(std::io::Error::new(std::io::ErrorKind::Other, e)))? });
            Ok(Self {
                db: Arc::new(db),
                rt,
            })
        }

        fn store(&self, id: &str, vector: Vec<f32>, metadata: String) -> PyResult<()> {
            let metadata: serde_json::Value = serde_json::from_str(&metadata)
                .map_err(|e| PyErr::from(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
            self.rt.block_on(async {
                self.db.store(id, &vector, &metadata).await
                    .map_err(|e| PyErr::from(std::io::Error::new(std::io::ErrorKind::Other, e)))
            })
        }

        fn search(&self, query: Vec<f32>, k: usize) -> PyResult<Vec<String>> {
            self.rt.block_on(async {
                self.db.search(&query, k).await
                    .map_err(|e| PyErr::from(std::io::Error::new(std::io::ErrorKind::Other, e)))
            }).map(|r| r.into_iter().map(|s| format!("{:?}", s)).collect())
        }
    }
}
