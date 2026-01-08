use std::sync::{Arc, RwLock};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

pub struct MetadataManager {
    metadata: Arc<RwLock<ClusterMetadata>>,
    version_store: Arc<RwLock<VersionStore>>,
    watchers: Arc<RwLock<Vec<Arc<dyn MetadataWatcher>>>>,
    persistence: Arc<Box<dyn MetadataPersistence>>,
}

pub struct ClusterMetadata {
    cluster_id: String,
    nodes: HashMap<String, NodeMetadata>,
    collections: HashMap<String, CollectionMetadata>,
    shards: HashMap<usize, ShardMetadata>,
    config: ClusterConfig,
    status: ClusterStatus,
    last_updated: chrono::DateTime<chrono::Utc>,
}

pub struct NodeMetadata {
    id: String,
    address: String,
    role: NodeRole,
    status: NodeStatus,
    capacity: NodeCapacity,
    metadata: HashMap<String, String>,
    last_heartbeat: chrono::DateTime<chrono::Utc>,
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

pub struct NodeCapacity {
    cpu_cores: usize,
    memory_gb: usize,
    disk_gb: usize,
    network_bandwidth_mbps: usize,
}

pub struct CollectionMetadata {
    name: String,
    schema: Schema,
    shard_count: usize,
    replication_factor: usize,
    status: CollectionStatus,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

pub struct Schema {
    fields: Vec<Field>,
    primary_key: Option<String>,
    vector_fields: Vec<String>,
}

pub struct Field {
    name: String,
    field_type: FieldType,
    nullable: bool,
    default_value: Option<serde_json::Value>,
}

pub enum FieldType {
    String,
    Integer,
    Float,
    Boolean,
    Vector { dimension: usize },
    Object,
    Array(Box<FieldType>),
    Timestamp,
}

pub enum CollectionStatus {
    Active,
    Creating,
    Dropping,
    Modifying,
    Error,
}

pub struct ShardMetadata {
    id: usize,
    collection: String,
    primary_node: String,
    replica_nodes: Vec<String>,
    status: ShardStatus,
    range: Option<KeyRange>,
    size_bytes: usize,
    document_count: usize,
}

pub struct KeyRange {
    start: Vec<u8>,
    end: Vec<u8>,
}

pub enum ShardStatus {
    Active,
    Creating,
    Migrating,
    Splitting,
    Merging,
    Dropping,
    Error,
}

pub struct ClusterConfig {
    version: String,
    election_timeout: Duration,
    heartbeat_interval: Duration,
    replication_factor: usize,
    shard_count: usize,
    max_shards_per_node: usize,
    storage_engine: String,
    index_type: String,
    security: SecurityConfig,
}

pub struct SecurityConfig {
    tls_enabled: bool,
    authentication_enabled: bool,
    authorization_enabled: bool,
    encryption_at_rest: bool,
}

pub enum ClusterStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Starting,
    Stopping,
}

pub struct VersionStore {
    current_version: u64,
    history: HashMap<u64, VersionEntry>,
    checkpoint_interval: Duration,
}

pub struct VersionEntry {
    version: u64,
    timestamp: chrono::DateTime<chrono::Utc>,
    changes: Vec<MetadataChange>,
    author: String,
}

pub enum MetadataChange {
    NodeAdded(NodeMetadata),
    NodeRemoved(String),
    NodeUpdated(String, NodeMetadata),
    CollectionAdded(CollectionMetadata),
    CollectionRemoved(String),
    CollectionUpdated(String, CollectionMetadata),
    ShardAdded(ShardMetadata),
    ShardRemoved(usize),
    ShardUpdated(usize, ShardMetadata),
    ConfigChanged(String, serde_json::Value),
}

pub trait MetadataWatcher: Send + Sync {
    fn on_metadata_change(&self, change: MetadataChange);
    fn on_version_change(&self, version: u64);
}

pub trait MetadataPersistence: Send + Sync {
    fn save(&self, metadata: &ClusterMetadata) -> Result<(), PersistenceError>;
    fn load(&self) -> Result<ClusterMetadata, PersistenceError>;
    fn checkpoint(&self, version: u64) -> Result<(), PersistenceError>;
    fn compact(&self, retained_versions: u64) -> Result<(), PersistenceError>;
}

pub struct InMemoryPersistence;

impl MetadataPersistence for InMemoryPersistence {
    fn save(&self, metadata: &ClusterMetadata) -> Result<(), PersistenceError> {
        Ok(())
    }

    fn load(&self) -> Result<ClusterMetadata, PersistenceError> {
        Err(PersistenceError::NotFound)
    }

    fn checkpoint(&self, version: u64) -> Result<(), PersistenceError> {
        Ok(())
    }

    fn compact(&self, retained_versions: u64) -> Result<(), PersistenceError> {
        Ok(())
    }
}

pub struct MetadataChangeEvent {
    change: MetadataChange,
    version: u64,
    timestamp: chrono::DateTime<chrono::Utc>,
}

impl MetadataManager {
    pub fn new(cluster_id: &str) -> Self {
        let metadata = Arc::new(RwLock::new(ClusterMetadata {
            cluster_id: cluster_id.to_string(),
            nodes: HashMap::new(),
            collections: HashMap::new(),
            shards: HashMap::new(),
            config: ClusterConfig::default(),
            status: ClusterStatus::Starting,
            last_updated: chrono::Utc::now(),
        }));

        let version_store = Arc::new(RwLock::new(VersionStore {
            current_version: 0,
            history: HashMap::new(),
            checkpoint_interval: Duration::from_minutes(5),
        }));

        let watchers = Arc::new(RwLock::new(Vec::new()));
        let persistence = Arc::new(Box::new(InMemoryPersistence));

        Self {
            metadata,
            version_store,
            watchers,
            persistence,
        }
    }

    pub fn add_node(&self, node: NodeMetadata) -> Result<(), MetadataError> {
        let mut metadata = self.metadata.write().unwrap();
        if metadata.nodes.contains_key(&node.id) {
            return Err(MetadataError::NodeAlreadyExists(node.id));
        }

        metadata.nodes.insert(node.id.clone(), node.clone());
        metadata.last_updated = chrono::Utc::now();

        let change = MetadataChange::NodeAdded(node);
        self.record_change(change)?;
        Ok(())
    }

    pub fn remove_node(&self, node_id: &str) -> Result<(), MetadataError> {
        let mut metadata = self.metadata.write().unwrap();
        if !metadata.nodes.contains_key(node_id) {
            return Err(MetadataError::NodeNotFound(node_id.to_string()));
        }

        metadata.nodes.remove(node_id);
        metadata.last_updated = chrono::Utc::now();

        let change = MetadataChange::NodeRemoved(node_id.to_string());
        self.record_change(change)?;
        Ok(())
    }

    pub fn add_collection(&self, collection: CollectionMetadata) -> Result<(), MetadataError> {
        let mut metadata = self.metadata.write().unwrap();
        if metadata.collections.contains_key(&collection.name) {
            return Err(MetadataError::CollectionAlreadyExists(collection.name));
        }

        metadata.collections.insert(collection.name.clone(), collection.clone());
        metadata.last_updated = chrono::Utc::now();

        let change = MetadataChange::CollectionAdded(collection);
        self.record_change(change)?;
        Ok(())
    }

    pub fn remove_collection(&self, collection_name: &str) -> Result<(), MetadataError> {
        let mut metadata = self.metadata.write().unwrap();
        if !metadata.collections.contains_key(collection_name) {
            return Err(MetadataError::CollectionNotFound(collection_name.to_string()));
        }

        metadata.collections.remove(collection_name);
        metadata.last_updated = chrono::Utc::now();

        let change = MetadataChange::CollectionRemoved(collection_name.to_string());
        self.record_change(change)?;
        Ok(())
    }

    pub fn add_shard(&self, shard: ShardMetadata) -> Result<(), MetadataError> {
        let mut metadata = self.metadata.write().unwrap();
        if metadata.shards.contains_key(&shard.id) {
            return Err(MetadataError::ShardAlreadyExists(shard.id));
        }

        metadata.shards.insert(shard.id, shard.clone());
        metadata.last_updated = chrono::Utc::now();

        let change = MetadataChange::ShardAdded(shard);
        self.record_change(change)?;
        Ok(())
    }

    pub fn remove_shard(&self, shard_id: usize) -> Result<(), MetadataError> {
        let mut metadata = self.metadata.write().unwrap();
        if !metadata.shards.contains_key(&shard_id) {
            return Err(MetadataError::ShardNotFound(shard_id));
        }

        metadata.shards.remove(&shard_id);
        metadata.last_updated = chrono::Utc::now();

        let change = MetadataChange::ShardRemoved(shard_id);
        self.record_change(change)?;
        Ok(())
    }

    pub fn get_metadata(&self) -> Arc<RwLock<ClusterMetadata>> {
        self.metadata.clone()
    }

    pub fn get_version(&self) -> u64 {
        let version_store = self.version_store.read().unwrap();
        version_store.current_version
    }

    pub fn add_watcher(&self, watcher: Arc<dyn MetadataWatcher>) {
        let mut watchers = self.watchers.write().unwrap();
        watchers.push(watcher);
    }

    pub fn remove_watcher(&self, watcher: Arc<dyn MetadataWatcher>) {
        let mut watchers = self.watchers.write().unwrap();
        watchers.retain(|w| !Arc::ptr_eq(w, &watcher));
    }

    fn record_change(&self, change: MetadataChange) -> Result<(), MetadataError> {
        let mut version_store = self.version_store.write().unwrap();
        version_store.current_version += 1;

        let entry = VersionEntry {
            version: version_store.current_version,
            timestamp: chrono::DateTime::now(&chrono::Utc),
            changes: vec![change.clone()],
            author: "system".to_string(),
        };

        version_store.history.insert(version_store.current_version, entry);

        let watchers = self.watchers.read().unwrap();
        for watcher in watchers.iter() {
            watcher.on_metadata_change(change.clone());
            watcher.on_version_change(version_store.current_version);
        }

        Ok(())
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_string(),
            election_timeout: Duration::from_millis(1500),
            heartbeat_interval: Duration::from_millis(150),
            replication_factor: 3,
            shard_count: 16,
            max_shards_per_node: 100,
            storage_engine: "rocksdb".to_string(),
            index_type: "hnsw".to_string(),
            security: SecurityConfig {
                tls_enabled: false,
                authentication_enabled: false,
                authorization_enabled: false,
                encryption_at_rest: false,
            },
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("Node already exists: {0}")]
    NodeAlreadyExists(String),

    #[error("Node not found: {0}")]
    NodeNotFound(String),

    #[error("Collection already exists: {0}")]
    CollectionAlreadyExists(String),

    #[error("Collection not found: {0}")]
    CollectionNotFound(String),

    #[error("Shard already exists: {0}")]
    ShardAlreadyExists(usize),

    #[error("Shard not found: {0}")]
    ShardNotFound(usize),

    #[error("Invalid metadata: {0}")]
    InvalidMetadata(String),

    #[error("Persistence error: {0}")]
    PersistenceError(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("Not found")]
    NotFound,

    #[error("IO error: {0}")]
    IoError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Version conflict: {0}")]
    VersionConflict(u64),

    #[error("Internal error: {0}")]
    InternalError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_manager() {
        let manager = MetadataManager::new("test-cluster");
        let node = NodeMetadata {
            id: "node1".to_string(),
            address: "127.0.0.1:8080".to_string(),
            role: NodeRole::Follower,
            status: NodeStatus::Healthy,
            capacity: NodeCapacity {
                cpu_co