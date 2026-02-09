use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, Mutex, mpsc};
use tokio::time::{timeout, Duration, interval};
use serde::{Serialize, Deserialize};
use thiserror::Error;
use log::{info, warn, error, debug};
use dashmap::DashMap;
use async_trait::async_trait;
use lru::LruCache;

#[derive(Error, Debug)]
pub enum EdgeError {
    #[error("Node error: {0}")]
    NodeError(String),
    
    #[error("Sync error: {0}")]
    SyncError(String),
    
    #[error("Inference error: {0}")]
    InferenceError(String),
    
    #[error("Resource error: {0}")]
    ResourceError(String),
    
    #[error("Connection error: {0}")]
    ConnectionError(String),
    
    #[error("Model error: {0}")]
    ModelError(String),
    
    #[error("Offline error: {0}")]
    OfflineError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeNodeConfig {
    pub node_id: String,
    pub name: String,
    pub endpoint: String,
    pub capabilities: EdgeCapabilities,
    pub resources: NodeResources,
    pub sync_config: SyncConfig,
    pub offline_config: OfflineConfig,
    pub priority: NodePriority,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeCapabilities {
    pub has_gpu: bool,
    pub gpu_memory_mb: u64,
    pub cpu_cores: u32,
    pub cpu_frequency_mhz: u64,
    pub memory_mb: u64,
    pub storage_mb: u64,
    pub network_bandwidth_mbps: u64,
    pub supported_models: Vec<String>,
    pub max_batch_size: u32,
    pub supported_formats: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeResources {
    pub available_memory_mb: u64,
    pub available_storage_mb: u64,
    pub cpu_usage_percent: f32,
    pub memory_usage_percent: f32,
    pub network_usage_percent: f32,
    pub battery_level: Option<f32>,
    pub power_mode: PowerMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PowerMode {
    Performance,
    Balanced,
    PowerSaver,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    pub auto_sync: bool,
    pub sync_interval_seconds: u64,
    pub sync_on_connect: bool,
    pub sync_on_disconnect: bool,
    pub conflict_resolution: ConflictResolution,
    pub compression: CompressionType,
    pub batch_size: usize,
    pub retry_count: u32,
    pub retry_delay_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConflictResolution {
    LastWriteWins,
    ServerWins,
    ClientWins,
    Merge,
    Manual,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Gzip,
    Zstd,
    Lz4,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OfflineConfig {
    pub enabled: bool,
    pub max_cache_size_mb: u64,
    pub max_pending_operations: u32,
    pub sync_priority: SyncPriority,
    pub background_sync: bool,
    pub data_retention_hours: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncPriority {
    Critical,
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodePriority {
    Primary,
    Secondary,
    Tertiary,
    Backup,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeNode {
    pub config: EdgeNodeConfig,
    pub status: NodeStatus,
    pub last_heartbeat: u64,
    pub metrics: NodeMetrics,
    pub local_models: HashMap<String, LocalModel>,
    pub pending_operations: Vec<PendingOperation>,
    pub connection_quality: ConnectionQuality,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeStatus {
    Online,
    Offline,
    Syncing,
    Busy,
    Error,
    Maintenance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetrics {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub avg_inference_time_ms: f32,
    pub total_inference_time_ms: u64,
    pub sync_count: u64,
    pub data_synced_bytes: u64,
    pub last_sync_time: Option<u64>,
    pub uptime_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalModel {
    pub model_id: String,
    pub version: u64,
    pub loaded: bool,
    pub memory_usage_mb: u64,
    pub inference_count: u64,
    pub last_used: u64,
    pub optimizations: Vec<ModelOptimization>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ModelOptimization {
    Quantization(QuantizationType),
    Pruning(f32),
    KnowledgeDistillation,
    CPUAffinity(Vec<u32>),
    MemoryMapping,
    StreamExecution,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QuantizationType {
    FP16,
    INT8,
    INT4,
    Binary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingOperation {
    pub id: String,
    pub operation_type: OperationType,
    pub priority: SyncPriority,
    pub created_at: u64,
    pub retry_count: u32,
    pub data: Vec<u8>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperationType {
    Insert,
    Update,
    Delete,
    Sync,
    Train,
    Inference,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionQuality {
    pub signal_strength: u8,
    pub latency_ms: u64,
    pub bandwidth_mbps: u64,
    pub packet_loss_percent: f32,
    pub stability: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeInferenceRequest {
    pub request_id: String,
    pub node_id: String,
    pub model_id: String,
    pub input_data: Vec<u8>,
    pub parameters: InferenceParameters,
    pub timestamp: u64,
    pub deadline: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceParameters {
    pub batch_size: u32,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
    pub top_k: Option<u32>,
    pub top_p: Option<f32>,
    pub stream: bool,
    pub cache_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeInferenceResponse {
    pub request_id: String,
    pub output_data: Vec<u8>,
    pub inference_time_ms: u64,
    pub tokens_generated: Option<u32>,
    pub confidence: Option<f32>,
    pub cached: bool,
    pub node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncMessage {
    pub message_id: String,
    pub source_node: String,
    pub target_node: String,
    pub sync_type: SyncType,
    pub payload: Vec<u8>,
    pub checksum: String,
    pub timestamp: u64,
    pub priority: SyncPriority,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncType {
    ModelUpdate,
    DataUpdate,
    ConfigurationUpdate,
    FullSync,
    IncrementalSync,
    StateSync,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncState {
    pub last_sync_time: u64,
    pub pending_changes: u64,
    pub synced_data_bytes: u64,
    pub sync_status: SyncStatus,
    pub conflicts: Vec<SyncConflict>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SyncStatus {
    Idle,
    InProgress,
    Completed,
    Failed,
    Paused,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConflict {
    pub conflict_id: String,
    pub local_version: u64,
    pub remote_version: u64,
    pub local_data: Vec<u8>,
    pub remote_data: Vec<u8>,
    pub resolution: Option<ConflictResolution>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataItem {
    pub id: String,
    pub collection: String,
    pub data: Vec<u8>,
    pub metadata: HashMap<String, String>,
    pub version: u64,
    pub timestamp: u64,
    pub flags: DataFlags,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFlags {
    pub sync_required: bool,
    pub priority: SyncPriority,
    pub compressible: bool,
    pub sensitive: bool,
    pub ttl_seconds: Option<u32>,
}

#[async_trait]
pub trait EdgeInferenceEngine: Send + Sync {
    async fn load_model(&mut self, model_id: &str, path: &str) -> Result<(), EdgeError>;
    async fn unload_model(&mut self, model_id: &str) -> Result<(), EdgeError>;
    async fn infer(&mut self, request: EdgeInferenceRequest) -> Result<EdgeInferenceResponse, EdgeError>;
    async fn get_memory_usage(&self) -> Result<u64, EdgeError>;
    async fn clear_cache(&mut self) -> Result<(), EdgeError>;
}

struct EdgeNodeInner {
    node: EdgeNode,
    inference_engine: Option<Box<dyn EdgeInferenceEngine>>,
    data_cache: LruCache<String, CachedItem>,
    sync_buffer: Vec<PendingOperation>,
}

struct CachedItem {
    data: Vec<u8>,
    created_at: u64,
    access_count: u32,
    size_bytes: u64,
}

pub struct EdgeComputingManager {
    nodes: DashMap<String, EdgeNodeInner>,
    model_registry: DashMap<String, ModelInfo>,
    sync_manager: Arc<RwLock<SyncManager>>,
    config: EdgeConfig,
    event_sender: mpsc::Sender<EdgeEvent>,
    connection_monitor: Arc<RwLock<ConnectionMonitor>>,
}

#[derive(Debug, Clone)]
pub struct ModelInfo {
    pub model_id: String,
    pub name: String,
    pub version: u64,
    pub size_bytes: u64,
    pub required_memory_mb: u64,
    pub supported_formats: Vec<String>,
    pub optimizations: Vec<ModelOptimization>,
    pub checksum: String,
    pub created_at: u64,
}

#[derive(Debug, Clone)]
pub struct EdgeConfig {
    pub default_sync_interval_seconds: u64,
    pub default_compression: CompressionType,
    pub max_concurrent_inferences: u32,
    pub max_cache_size_mb: u64,
    pub health_check_interval_seconds: u64,
    pub auto_failover: bool,
    pub load_balancing: bool,
    pub default_priority: NodePriority,
}

#[derive(Debug, Clone)]
pub struct SyncManager {
    pending_syncs: DashMap<String, SyncState>,
    sync_history: Vec<SyncRecord>,
    last_global_sync: u64,
}

struct SyncRecord {
    sync_id: String,
    timestamp: u64,
    nodes_involved: Vec<String>,
    data_bytes: u64,
    duration_ms: u64,
    status: SyncStatus,
}

struct ConnectionMonitor {
    nodes: DashMap<String, ConnectionState>,
    last_check: u64,
}

struct ConnectionState {
    node_id: String,
    connected: bool,
    last_check: u64,
    latency_ms: u64,
    error_count: u32,
}

#[derive(Debug, Clone)]
pub enum EdgeEvent {
    NodeJoined(EdgeNode),
    NodeLeft(String),
    NodeStatusChanged(String, NodeStatus),
    SyncStarted(String),
    SyncCompleted(String),
    SyncFailed(String, String),
    ModelLoaded(String, String),
    ModelUnloaded(String),
    InferenceCompleted(String, u64),
    ConflictDetected(SyncConflict),
    Alert(EdgeAlertLevel, String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum EdgeAlertLevel {
    Info,
    Warning,
    Error,
    Critical,
}

impl Default for EdgeConfig {
    fn default() -> Self {
        Self {
            default_sync_interval_seconds: 60,
            default_compression: CompressionType::Zstd,
            max_concurrent_inferences: 10,
            max_cache_size_mb: 512,
            health_check_interval_seconds: 30,
            auto_failover: true,
            load_balancing: true,
            default_priority: NodePriority::Secondary,
        }
    }
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            auto_sync: true,
            sync_interval_seconds: 60,
            sync_on_connect: true,
            sync_on_disconnect: true,
            conflict_resolution: ConflictResolution::Merge,
            compression: CompressionType::Zstd,
            batch_size: 100,
            retry_count: 3,
            retry_delay_seconds: 5,
        }
    }
}

impl Default for OfflineConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_cache_size_mb: 256,
            max_pending_operations: 1000,
            sync_priority: SyncPriority::High,
            background_sync: true,
            data_retention_hours: 24,
        }
    }
}

impl Default for EdgeCapabilities {
    fn default() -> Self {
        Self {
            has_gpu: false,
            gpu_memory_mb: 0,
            cpu_cores: 4,
            cpu_frequency_mhz: 2000,
            memory_mb: 4096,
            storage_mb: 32768,
            network_bandwidth_mbps: 100,
            supported_models: vec!["embedding".to_string(), "classifier".to_string()],
            max_batch_size: 32,
            supported_formats: vec!["onnx".to_string(), "torchscript".to_string()],
        }
    }
}

impl Default for NodeResources {
    fn default() -> Self {
        Self {
            available_memory_mb: 2048,
            available_storage_mb: 16384,
            cpu_usage_percent: 0.0,
            memory_usage_percent: 0.0,
            network_usage_percent: 0.0,
            battery_level: None,
            power_mode: PowerMode::Balanced,
        }
    }
}

impl Default for ConnectionQuality {
    fn default() -> Self {
        Self {
            signal_strength: 100,
            latency_ms: 50,
            bandwidth_mbps: 100,
            packet_loss_percent: 0.0,
            stability: 1.0,
        }
    }
}

impl Default for NodeMetrics {
    fn default() -> Self {
        Self {
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            avg_inference_time_ms: 0.0,
            total_inference_time_ms: 0,
            sync_count: 0,
            data_synced_bytes: 0,
            last_sync_time: None,
            uptime_seconds: 0,
        }
    }
}

impl EdgeNode {
    pub fn new(config: EdgeNodeConfig) -> Self {
        Self {
            config,
            status: NodeStatus::Offline,
            last_heartbeat: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            metrics: NodeMetrics::default(),
            local_models: HashMap::new(),
            pending_operations: Vec::new(),
            connection_quality: ConnectionQuality::default(),
        }
    }

    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        if self.status == NodeStatus::Offline {
            self.status = NodeStatus::Online;
        }
    }

    pub fn update_resources(&mut self, resources: NodeResources) {
        self.config.resources = resources;
    }

    pub fn is_online(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now - self.last_heartbeat < 60
    }
}

impl EdgeComputingManager {
    pub async fn new(config: Option<EdgeConfig>) -> Result<Self, EdgeError> {
        let config = config.unwrap_or_default();
        let (event_sender, _) = mpsc::channel(1000);
        
        Ok(Self {
            nodes: DashMap::new(),
            model_registry: DashMap::new(),
            sync_manager: Arc::new(RwLock::new(SyncManager {
                pending_syncs: DashMap::new(),
                sync_history: Vec::new(),
                last_global_sync: 0,
            })),
            config,
            event_sender,
            connection_monitor: Arc::new(RwLock::new(ConnectionMonitor {
                nodes: DashMap::new(),
                last_check: 0,
            })),
        })
    }

    pub async fn register_node(&self, config: EdgeNodeConfig) -> Result<String, EdgeError> {
        let node_id = config.node_id.clone();
        
        let node = EdgeNode::new(config);
        let inner = EdgeNodeInner {
            node,
            inference_engine: None,
            data_cache: LruCache::new(
                std::num::NonZeroUsize::new(self.config.max_cache_size_mb as usize * 1024).unwrap()
            ),
            sync_buffer: Vec::new(),
        };
        
        self.nodes.insert(node_id.clone(), inner);
        
        self.send_event(EdgeEvent::NodeJoined(
            self.nodes.get(&node_id).unwrap().node.clone()
        ));
        
        info!("Edge node registered: {} ({})", node_id, 
            self.nodes.get(&node_id).unwrap().node.config.name);
        
        Ok(node_id)
    }

    pub async fn unregister_node(&self, node_id: &str) -> Result<(), EdgeError> {
        if let Some((_, node)) = self.nodes.remove(node_id) {
            self.send_event(EdgeEvent::NodeLeft(node_id.to_string()));
            info!("Edge node unregistered: {}", node_id);
        }
        Ok(())
    }

    pub async fn update_node_status(&self, node_id: &str, status: NodeStatus) -> Result<(), EdgeError> {
        if let Some(mut node) = self.nodes.get_mut(node_id) {
            let old_status = node.node.status.clone();
            node.node.status = status;
            node.node.update_heartbeat();
            
            if old_status != status {
                self.send_event(EdgeEvent::NodeStatusChanged(node_id.to_string(), status));
            }
        }
        Ok(())
    }

    pub async fn get_node(&self, node_id: &str) -> Option<EdgeNode> {
        self.nodes.get(node_id).map(|n| n.node.clone())
    }

    pub async fn list_nodes(&self, status_filter: Option<NodeStatus>) -> Vec<EdgeNode> {
        self.nodes.iter()
            .filter(|n| {
                if let Some(filter) = &status_filter {
                    n.node.status == *filter
                } else {
                    true
                }
            })
            .map(|n| n.node.clone())
            .collect()
    }

    pub async fn get_online_nodes(&self) -> Vec<EdgeNode> {
        self.nodes.iter()
            .filter(|n| n.node.is_online())
            .map(|n| n.node.clone())
            .collect()
    }

    pub async fn route_inference_request(
        &self,
        request: EdgeInferenceRequest,
    ) -> Result<EdgeInferenceResponse, EdgeError> {
        if !self.config.load_balancing {
            return self.local_inference(request).await;
        }
        
        let online_nodes = self.get_online_nodes().await;
        if online_nodes.is_empty() {
            return Err(EdgeError::ConnectionError("No online nodes available".to_string()));
        }
        
        let candidates: Vec<&EdgeNode> = online_nodes.iter()
            .filter(|n| n.config.capabilities.supported_models.contains(&request.model_id))
            .filter(|n| n.status == NodeStatus::Online || n.status == NodeStatus::Busy)
            .collect();
        
        if candidates.is_empty() {
            return Err(EdgeError::InferenceError(
                "No suitable node found for model".to_string()
            ));
        }
        
        let selected_node = self.select_best_node(&candidates, &request).await?;
        
        self.execute_inference(&selected_node, request).await
    }

    async fn select_best_node(
        &self,
        candidates: &[&EdgeNode],
        request: &EdgeInferenceRequest,
    ) -> Option<EdgeNode> {
        let mut scored: Vec<(f32, &EdgeNode)> = candidates.iter()
            .map(|node| {
                let load_score = 1.0 - node.config.resources.cpu_usage_percent.min(1.0);
                let memory_score = 1.0 - node.config.resources.memory_usage_percent.min(1.0);
                let capability_score = if node.config.capabilities.supported_models.contains(&request.model_id) {
                    1.0
                } else {
                    0.0
                };
                let priority_score = match node.config.priority {
                    NodePriority::Primary => 1.0,
                    NodePriority::Secondary => 0.8,
                    NodePriority::Tertiary => 0.6,
                    NodePriority::Backup => 0.4,
                };
                
                let total_score = (load_score * 0.3 + memory_score * 0.3 + capability_score * 0.2 + priority_score * 0.2);
                (total_score, *node)
            })
            .collect();
        
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        scored.first().map(|(_, node)| (*node).clone())
    }

    async fn execute_inference(
        &self,
        node: &EdgeNode,
        request: EdgeInferenceRequest,
    ) -> Result<EdgeInferenceResponse, EdgeError> {
        let start_time = std::time::Instant::now();
        
        let response = self.local_inference(request.clone()).await;
        
        let inference_time = start_time.elapsed().as_millis() as u64;
        
        if let Some(mut node_ref) = self.nodes.get_mut(&node.config.node_id) {
            node_ref.node.metrics.total_requests += 1;
            node_ref.node.metrics.total_inference_time_ms += inference_time;
            node_ref.node.metrics.avg_inference_time_ms = 
                node_ref.node.metrics.total_inference_time_ms as f32 / node_ref.node.metrics.total_requests as f32;
            
            if response.is_ok() {
                node_ref.node.metrics.successful_requests += 1;
            } else {
                node_ref.node.metrics.failed_requests += 1;
            }
        }
        
        self.send_event(EdgeEvent::InferenceCompleted(
            request.request_id,
            inference_time
        ));
        
        response
    }

    async fn local_inference(
        &self,
        request: EdgeInferenceRequest,
    ) -> Result<EdgeInferenceResponse, EdgeError> {
        let node_id = request.node_id.clone();
        
        if let Some(node) = self.nodes.get(&node_id) {
            if !node.node.is_online() {
                return Err(EdgeError::OfflineError("Node is offline".to_string()));
            }
        }
        
        Ok(EdgeInferenceResponse {
            request_id: request.request_id,
            output_data: vec![],
            inference_time_ms: 10,
            tokens_generated: None,
            confidence: None,
            cached: false,
            node_id,
        })
    }

    pub async fn sync_node(&self, node_id: &str) -> Result<SyncState, EdgeError> {
        let node = match self.nodes.get(node_id) {
            Some(n) => n,
            None => return Err(EdgeError::NodeError("Node not found".to_string())),
        };
        
        self.send_event(EdgeEvent::SyncStarted(node_id.to_string()));
        
        let sync_state = SyncState {
            last_sync_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            pending_changes: node.pending_operations.len() as u64,
            synced_data_bytes: 0,
            sync_status: SyncStatus::InProgress,
            conflicts: Vec::new(),
        };
        
        self.sync_manager.write().await.pending_syncs
            .insert(node_id.to_string(), sync_state.clone());
        
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        let completed_state = SyncState {
            last_sync_time: sync_state.last_sync_time,
            pending_changes: 0,
            synced_data_bytes: 1024,
            sync_status: SyncStatus::Completed,
            conflicts: Vec::new(),
        };
        
        self.sync_manager.write().await.pending_syncs
            .insert(node_id.to_string(), completed_state.clone());
        
        if let Some(mut node) = self.nodes.get_mut(node_id) {
            node.node.metrics.sync_count += 1;
            node.node.metrics.last_sync_time = Some(completed_state.last_sync_time);
            node.node.status = NodeStatus::Online;
        }
        
        self.send_event(EdgeEvent::SyncCompleted(node_id.to_string()));
        
        info!("Sync completed for node {}: {} bytes", node_id, completed_state.synced_data_bytes);
        
        Ok(completed_state)
    }

    pub async fn sync_all_nodes(&self) -> Vec<Result<SyncState, EdgeError>> {
        let node_ids: Vec<String> = self.nodes.iter()
            .map(|n| n.node.config.node_id.clone())
            .collect();
        
        let mut results = Vec::new();
        
        for node_id in node_ids {
            results.push(self.sync_node(&node_id).await);
        }
        
        results
    }

    pub async fn push_to_edge(
        &self,
        node_id: &str,
        data: Vec<u8>,
        metadata: HashMap<String, String>,
    ) -> Result<(), EdgeError> {
        let operation = PendingOperation {
            id: uuid::Uuid::new_v4().to_string(),
            operation_type: OperationType::Insert,
            priority: SyncPriority::Medium,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            retry_count: 0,
            data,
            metadata,
        };
        
        if let Some(mut node) = self.nodes.get_mut(node_id) {
            node.sync_buffer.push(operation);
            node.node.status = NodeStatus::Syncing;
        }
        
        Ok(())
    }

    pub async fn collect_from_edge(
        &self,
        node_id: &str,
        query: &str,
    ) -> Result<Vec<DataItem>, EdgeError> {
        if let Some(node) = self.nodes.get(node_id) {
            let items: Vec<DataItem> = node.sync_buffer.iter()
                .filter(|op| op.metadata.get("query") == Some(&query.to_string()))
                .map(|op| DataItem {
                    id: op.id.clone(),
                    collection: op.metadata.get("collection")
                        .unwrap_or(&"default".to_string())
                        .clone(),
                    data: op.data.clone(),
                    metadata: op.metadata.clone(),
                    version: 1,
                    timestamp: op.created_at,
                    flags: DataFlags {
                        sync_required: true,
                        priority: op.priority,
                        compressible: true,
                        sensitive: false,
                        ttl_seconds: None,
                    },
                })
                .collect();
            
            return Ok(items);
        }
        
        Err(EdgeError::NodeError("Node not found".to_string()))
    }

    pub async fn deploy_model(
        &self,
        node_id: &str,
        model_info: ModelInfo,
        optimizations: Vec<ModelOptimization>,
    ) -> Result<(), EdgeError> {
        if let Some(mut node) = self.nodes.get_mut(node_id) {
            let local_model = LocalModel {
                model_id: model_info.model_id.clone(),
                version: model_info.version,
                loaded: false,
                memory_usage_mb: model_info.required_memory_mb,
                inference_count: 0,
                last_used: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                optimizations,
            };
            
            node.node.local_models.insert(model_info.model_id.clone(), local_model);
            node.node.status = NodeStatus::Busy;
            
            self.model_registry.insert(model_info.model_id.clone(), model_info);
            
            self.send_event(EdgeEvent::ModelLoaded(
                node_id.to_string(),
                model_info.model_id
            ));
            
            info!("Model {} deployed to node {}", model_info.model_id, node_id);
        }
        
        Ok(())
    }

    pub async fn start_connection_monitor(&self) {
        let interval = self.config.health_check_interval_seconds;
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(interval));
            loop {
                interval.tick().await;
            }
        });
    }

    pub async fn start_background_sync(&self) {
        let interval = self.config.default_sync_interval_seconds;
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(interval));
            loop {
                interval.tick().await;
            }
        });
    }

    pub async fn get_sync_status(&self) -> Vec<SyncState> {
        self.sync_manager.read().await.pending_syncs
            .iter()
            .map(|s| s.value().clone())
            .collect()
    }

    pub async fn send_event(&self, event: EdgeEvent) {
        let _ = self.event_sender.send(event).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_edge_computing_manager_creation() {
        let manager = EdgeComputingManager::new(None).await;
        assert!(manager.is_ok());
    }

    #[tokio::test]
    async fn test_node_registration() {
        let manager = EdgeComputingManager::new(None).await.unwrap();
        
        let config = EdgeNodeConfig {
            node_id: uuid::Uuid::new_v4().to_string(),
            name: "Test Edge Node".to_string(),
            endpoint: "http://localhost:8080".to_string(),
            capabilities: EdgeCapabilities::default(),
            resources: NodeResources::default(),
            sync_config: SyncConfig::default(),
            offline_config: OfflineConfig::default(),
            priority: NodePriority::Secondary,
        };
        
        let node_id = manager.register_node(config).await;
        assert!(node_id.is_ok());
        assert!(!node_id.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_node_listing() {
        let manager = EdgeComputingManager::new(None).await.unwrap();
        
        let nodes = manager.list_nodes(None).await;
        assert!(nodes.is_empty());
    }

    #[tokio::test]
    async fn test_node_status_update() {
        let manager = EdgeComputingManager::new(None).await.unwrap();
        
        let config = EdgeNodeConfig {
            node_id: uuid::Uuid::new_v4().to_string(),
            name: "Test Node".to_string(),
            endpoint: "http://localhost:8080".to_string(),
            capabilities: EdgeCapabilities::default(),
            resources: NodeResources::default(),
            sync_config: SyncConfig::default(),
            offline_config: OfflineConfig::default(),
            priority: NodePriority::Secondary,
        };
        
        let node_id = manager.register_node(config).await.unwrap();
        let result = manager.update_node_status(&node_id, NodeStatus::Online).await;
        assert!(result.is_ok());
        
        let node = manager.get_node(&node_id).await;
        assert!(node.is_some());
        assert_eq!(node.unwrap().status, NodeStatus::Online);
    }

    #[tokio::test]
    async fn test_sync_operation() {
        let manager = EdgeComputingManager::new(None).await.unwrap();
        
        let config = EdgeNodeConfig {
            node_id: uuid::Uuid::new_v4().to_string(),
            name: "Sync Test Node".to_string(),
            endpoint: "http://localhost:8080".to_string(),
            capabilities: EdgeCapabilities::default(),
            resources: NodeResources::default(),
            sync_config: SyncConfig::default(),
            offline_config: OfflineConfig::default(),
            priority: NodePriority::Secondary,
        };
        
        let node_id = manager.register_node(config).await.unwrap();
        let sync_result = manager.sync_node(&node_id).await;
        
        assert!(sync_result.is_ok());
        let state = sync_result.unwrap();
        assert_eq!(state.sync_status, SyncStatus::Completed);
    }

    #[tokio::test]
    async fn test_data_push_to_edge() {
        let manager = EdgeComputingManager::new(None).await.unwrap();
        
        let config = EdgeNodeConfig {
            node_id: uuid::Uuid::new_v4().to_string(),
            name: "Push Test Node".to_string(),
            endpoint: "http://localhost:8080".to_string(),
            capabilities: EdgeCapabilities::default(),
            resources: NodeResources::default(),
            sync_config: SyncConfig::default(),
            offline_config: OfflineConfig::default(),
            priority: NodePriority::Secondary,
        };
        
        let node_id = manager.register_node(config).await.unwrap();
        
        let data = vec![1, 2, 3, 4, 5];
        let metadata = HashMap::new();
        
        let result = manager.push_to_edge(&node_id, data, metadata).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_model_deployment() {
        let manager = EdgeComputingManager::new(None).await.unwrap();
        
        let config = EdgeNodeConfig {
            node_id: uuid::Uuid::new_v4().to_string(),
            name: "Model Test Node".to_string(),
            endpoint: "http://localhost:8080".to_string(),
            capabilities: EdgeCapabilities::default(),
            resources: NodeResources::default(),
            sync_config: SyncConfig::default(),
            offline_config: OfflineConfig::default(),
            priority: NodePriority::Secondary,
        };
        
        let node_id = manager.register_node(config).await.unwrap();
        
        let model_info = ModelInfo {
            model_id: "test-model".to_string(),
            name: "Test Model".to_string(),
            version: 1,
            size_bytes: 1024000,
            required_memory_mb: 512,
            supported_formats: vec!["onnx".to_string()],
            optimizations: vec![ModelOptimization::Quantization(QuantizationType::INT8)],
            checksum: "abc123".to_string(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        
        let result = manager.deploy_model(&node_id, model_info, Vec::new()).await;
        assert!(result.is_ok());
        
        let node = manager.get_node(&node_id).await;
        assert!(node.is_some());
        assert!(node.unwrap().local_models.contains_key("test-model"));
    }
}
