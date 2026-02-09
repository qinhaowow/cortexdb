use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock, broadcast};
use tokio::time::timeout;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, error, info, warn};

#[derive(Debug, Error)]
pub enum EtcdError {
    #[error("Connection failed: {0}")]
    ConnectionError(String),
    #[error("Request failed: {0}")]
    RequestError(String),
    #[error("Key not found: {0}")]
    KeyNotFound(String),
    #[error("Lease expired")]
    LeaseExpired,
    #[error("Watch cancelled")]
    WatchCancelled,
    #[error("Transaction failed")]
    TransactionFailed,
    #[error("Authentication failed")]
    AuthError(String),
    #[error("Invalid response")]
    InvalidResponse,
    #[error("Timeout")]
    Timeout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtcdConfig {
    pub endpoints: Vec<String>,
    pub timeout: Duration,
    pub username: Option<String>,
    pub password: Option<String>,
    pub max_retries: u32,
    pub retry_delay: Duration,
    pub keepalive_interval: Duration,
    pub tls_enabled: bool,
    pub ca_path: Option<String>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

impl Default for EtcdConfig {
    fn default() -> Self {
        Self {
            endpoints: vec!["http://127.0.0.1:2379".to_string()],
            timeout: Duration::from_secs(30),
            username: None,
            password: None,
            max_retries: 3,
            retry_delay: Duration::from_secs(1),
            keepalive_interval: Duration::from_secs(30),
            tls_enabled: false,
            ca_path: None,
            cert_path: None,
            key_path: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtcdStats {
    pub total_requests: AtomicU64,
    pub successful_requests: AtomicU64,
    pub failed_requests: AtomicU64,
    pub watch_events: AtomicU64,
    pub active_watches: AtomicUsize,
    pub active_leases: AtomicUsize,
    pub service_registrations: AtomicUsize,
    pub avg_request_time_ms: AtomicU64,
    pub reconnections: AtomicU64,
}

impl Default for EtcdStats {
    fn default() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            successful_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            watch_events: AtomicU64::new(0),
            active_watches: AtomicUsize::new(0),
            active_leases: AtomicUsize::new(0),
            service_registrations: AtomicUsize::new(0),
            avg_request_time_ms: AtomicU64::new(0),
            reconnections: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceRegistration {
    pub id: String,
    pub name: String,
    pub address: String,
    pub port: u16,
    pub tags: Vec<String>,
    pub metadata: HashMap<String, String>,
    pub ttl: Duration,
    pub health_check: Option<HealthCheck>,
    pub enable_around: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheck {
    pub http_path: String,
    pub interval: Duration,
    pub timeout: Duration,
    pub deregister_after: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInstance {
    pub id: String,
    pub name: String,
    pub address: String,
    pub port: u16,
    pub tags: Vec<String>,
    pub metadata: HashMap<String, String>,
    pub status: ServiceStatus,
    pub last_heartbeat: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ServiceStatus {
    Up,
    Passing,
    Warning,
    Critical,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct EtcdService {
    registration: ServiceRegistration,
    lease_id: u64,
    key: String,
    tx: mpsc::Sender<()>,
}

impl EtcdService {
    pub fn get_id(&self) -> &str {
        &self.registration.id
    }

    pub fn get_name(&self) -> &str {
        &self.registration.name
    }

    pub fn get_key(&self) -> &str {
        &self.key
    }

    pub fn get_lease_id(&self) -> u64 {
        self.lease_id
    }
}

pub struct EtcdClient {
    config: EtcdConfig,
    stats: Arc<EtcdStats>,
    services: RwLock<HashMap<String, EtcdService>>,
    watches: RwLock<HashMap<String, broadcast::Receiver<EtcdWatchEvent>>>,
    leases: RwLock<HashMap<u64, Instant>>,
    shutdown: broadcast::Sender<()>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtcdKeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub version: u64,
    pub create_revision: u64,
    pub mod_revision: u64,
    pub lease_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtcdWatchEvent {
    pub event_type: WatchEventType,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub version: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum WatchEventType {
    Put,
    Delete,
    Expire,
}

#[derive(Debug, Clone)]
pub struct EtcdTransaction {
    client: EtcdClient,
    operations: Vec<EtcdOperation>,
}

#[derive(Debug, Clone)]
pub enum EtcdOperation {
    Get { key: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8>, lease_id: Option<u64> },
    Delete { key: Vec<u8> },
    Txn { operations: Vec<EtcdOperation>, else_operations: Option<Vec<EtcdOperation>> },
}

impl EtcdClient {
    pub async fn new(config: Option<EtcdConfig>) -> Result<Self, EtcdError> {
        let config = config.unwrap_or_default();
        let stats = Arc::new(EtcdStats::default());
        let services = RwLock::new(HashMap::new());
        let watches = RwLock::new(HashMap::new());
        let leases = RwLock::new(HashMap::new());
        let (shutdown, _) = broadcast::channel(1);

        let client = Self {
            config,
            stats,
            services,
            watches,
            leases,
            shutdown,
        };

        client.health_check().await?;
        info!("EtcdClient initialized with {} endpoints", client.config.endpoints.len());
        Ok(client)
    }

    pub async fn health_check(&self) -> Result<(), EtcdError> {
        self.stats.total_requests.fetch_add(1, Ordering::SeqCst);
        let start = Instant::now();
        
        tokio::time::sleep(Duration::from_millis(1)).await;
        
        self.stats.successful_requests.fetch_add(1, Ordering::SeqCst);
        let elapsed = start.elapsed().as_millis() as u64;
        self.update_avg_request_time(elapsed);
        
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<EtcdKeyValue>, EtcdError> {
        self.stats.total_requests.fetch_add(1, Ordering::SeqCst);
        let start = Instant::now();
        
        tokio::time::sleep(Duration::from_micros(100)).await;
        
        self.stats.successful_requests.fetch_add(1, Ordering::SeqCst);
        let elapsed = start.elapsed().as_millis() as u64;
        self.update_avg_request_time(elapsed);
        
        debug!("GET key: {:?}", String::from_utf8_lossy(key));
        Ok(Some(EtcdKeyValue {
            key: key.to_vec(),
            value: vec![1, 2, 3],
            version: 1,
            create_revision: 1,
            mod_revision: 1,
            lease_id: 0,
        }))
    }

    pub async fn put(&self, key: &[u8], value: &[u8], lease_id: Option<u64>) -> Result<u64, EtcdError> {
        self.stats.total_requests.fetch_add(1, Ordering::SeqCst);
        let start = Instant::now();
        
        tokio::time::sleep(Duration::from_micros(100)).await;
        
        self.stats.successful_requests.fetch_add(1, Ordering::SeqCst);
        let elapsed = start.elapsed().as_millis() as u64;
        self.update_avg_request_time(elapsed);
        
        debug!("PUT key: {:?} = {:?}", String::from_utf8_lossy(key), String::from_utf8_lossy(value));
        Ok(1)
    }

    pub async fn delete(&self, key: &[u8]) -> Result<u64, EtcdError> {
        self.stats.total_requests.fetch_add(1, Ordering::SeqCst);
        
        tokio::time::sleep(Duration::from_micros(50)).await;
        
        self.stats.successful_requests.fetch_add(1, Ordering::SeqCst);
        debug!("DELETE key: {:?}", String::from_utf8_lossy(key));
        Ok(1)
    }

    pub async fn watch(&self, key: &[u8]) -> Result<mpsc::Receiver<EtcdWatchEvent>, EtcdError> {
        let (tx, rx) = mpsc::channel(100);
        let key_str = String::from_utf8_lossy(key).to_string();
        let watch_id = format!("watch-{}", key_str);
        
        self.stats.active_watches.fetch_add(1, Ordering::SeqCst);

        let shutdown_rx = self.shutdown.subscribe();
        let stats = self.stats.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let event = EtcdWatchEvent {
                            event_type: WatchEventType::Put,
                            key: key.to_vec(),
                            value: Some(vec![1]),
                            version: 1,
                        };
                        let _ = tx.send(event).await;
                        stats.watch_events.fetch_add(1, Ordering::SeqCst);
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });

        self.writes.lock().await.insert(watch_id, rx.clone());
        
        Ok(rx)
    }

    pub async fn watch_prefix(&self, prefix: &[u8]) -> Result<mpsc::Receiver<EtcdWatchEvent>, EtcdError> {
        self.watch(prefix).await
    }

    pub async fn lease_grant(&self, ttl: Duration) -> Result<u64, EtcdError> {
        self.stats.active_leases.fetch_add(1, Ordering::SeqCst);
        let lease_id = rand::random::<u64>();
        
        let now = Instant::now();
        self.leases.write().await.insert(lease_id, now + ttl);
        
        Ok(lease_id)
    }

    pub async fn lease_keep_alive(&self, lease_id: u64) -> Result<(), EtcdError> {
        let mut leases = self.leases.write().await;
        if let Some(expiry) = leases.get(&lease_id) {
            let new_expiry = *expiry + Duration::from_secs(30);
            leases.insert(lease_id, new_expiry);
        }
        Ok(())
    }

    pub async fn lease_revoke(&self, lease_id: u64) -> Result<(), EtcdError> {
        self.stats.active_leases.fetch_sub(1, Ordering::SeqCst);
        self.leases.write().await.remove(&lease_id);
        Ok(())
    }

    pub async fn register_service(&self, registration: ServiceRegistration) -> Result<EtcdService, EtcdError> {
        if registration.ttl < Duration::from_secs(10) {
            return Err(EtcdError::ConnectionError("TTL too short".to_string()));
        }

        let lease_id = self.lease_grant(registration.ttl).await?;
        let key = format!("/services/{}/{}", registration.name, registration.id);
        let value = serde_json::to_vec(&registration)
            .map_err(|e| EtcdError::ConnectionError(e.to_string()))?;
        
        self.put(key.as_bytes(), &value, Some(lease_id)).await?;
        
        let (tx, _) = mpsc::channel(1);
        
        let service = EtcdService {
            registration,
            lease_id,
            key,
            tx,
        };
        
        self.services.write().await.insert(service.registration.id.clone(), service.clone());
        self.stats.service_registrations.store(self.services.read().await.len(), Ordering::SeqCst);
        
        info!("Registered service: {} ({})", service.registration.name, service.registration.id);
        Ok(service)
    }

    pub async fn deregister_service(&self, service_id: &str) -> Result<(), EtcdError> {
        let services = self.services.read().await;
        if let Some(service) = services.get(service_id) {
            self.delete(service.key.as_bytes()).await?;
            self.lease_revoke(service.lease_id).await?;
            drop(services);
            
            self.services.write().await.remove(service_id);
            self.stats.service_registrations.store(self.services.read().await.len(), Ordering::SeqCst);
            
            info!("Deregistered service: {}", service_id);
        }
        Ok(())
    }

    pub async fn discover_services(&self, service_name: &str) -> Result<Vec<ServiceInstance>, EtcdError> {
        let prefix = format!("/services/{}/", service_name);
        let key = prefix.as_bytes();
        
        let result = self.get(key).await?;
        
        let mut instances = Vec::new();
        
        if let Some(kv) = result {
            let registration: ServiceRegistration = serde_json::from_slice(&kv.value)
                .map_err(|e| EtcdError::InvalidResponse)?;
            
            instances.push(ServiceInstance {
                id: registration.id,
                name: registration.name,
                address: registration.address,
                port: registration.port,
                tags: registration.tags,
                metadata: registration.metadata,
                status: ServiceStatus::Up,
                last_heartbeat: std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64,
            });
        }
        
        debug!("Discovered {} instances of service {}", instances.len(), service_name);
        Ok(instances)
    }

    pub async fn get_service_health(&self, service_name: &str) -> Result<Vec<ServiceInstance>, EtcdError> {
        let instances = self.discover_services(service_name).await?;
        
        let healthy: Vec<ServiceInstance> = instances.into_iter()
            .filter(|i| i.status == ServiceStatus::Up || i.status == ServiceStatus::Passing)
            .collect();
        
        debug!("Health check for {}: {} healthy out of {}", service_name, healthy.len(), instances.len());
        Ok(healthy)
    }

    pub async fn transaction(&self, operations: Vec<EtcdOperation>) -> Result<EtcdTransactionResult, EtcdError> {
        self.stats.total_requests.fetch_add(1, Ordering::SeqCst);
        
        tokio::time::sleep(Duration::from_micros(100)).await;
        
        self.stats.successful_requests.fetch_add(1, Ordering::SeqCst);
        
        Ok(EtcdTransactionResult {
            succeeded: true,
            responses: Vec::new(),
        })
    }

    pub async fn get_stats(&self) -> HashMap<String, String> {
        let mut stats = HashMap::new();
        stats.insert("total_requests".to_string(), self.stats.total_requests.load(Ordering::SeqCst).to_string());
        stats.insert("successful_requests".to_string(), self.stats.successful_requests.load(Ordering::SeqCst).to_string());
        stats.insert("failed_requests".to_string(), self.stats.failed_requests.load(Ordering::SeqCst).to_string());
        stats.insert("watch_events".to_string(), self.stats.watch_events.load(Ordering::SeqCst).to_string());
        stats.insert("active_watches".to_string(), self.stats.active_watches.load(Ordering::SeqCst).to_string());
        stats.insert("active_leases".to_string(), self.stats.active_leases.load(Ordering::SeqCst).to_string());
        stats.insert("service_registrations".to_string(), self.stats.service_registrations.load(Ordering::SeqCst).to_string());
        stats.insert("avg_request_time_ms".to_string(), self.stats.avg_request_time_ms.load(Ordering::SeqCst).to_string());
        stats.insert("registered_services".to_string(), self.services.read().await.len().to_string());
        
        stats
    }

    fn update_avg_request_time(&self, elapsed_ms: u64) {
        let avg = self.stats.avg_request_time_ms.load(Ordering::SeqCst);
        self.stats.avg_request_time_ms.store((avg + elapsed_ms) / 2, Ordering::SeqCst);
    }

    pub async fn close(&self) {
        let _ = self.shutdown.send(());
        
        for service in self.services.write().await.values() {
            let _ = self.deregister_service(&service.registration.id).await;
        }
        
        info!("EtcdClient shutdown completed");
    }
}

#[derive(Debug, Clone)]
pub struct EtcdTransactionResult {
    pub succeeded: bool,
    pub responses: Vec<EtcdOperationResponse>,
}

#[derive(Debug, Clone)]
pub enum EtcdOperationResponse {
    Get { key_value: Option<EtcdKeyValue> },
    Put { prev_key_value: Option<EtcdKeyValue> },
    Delete { deleted: u64 },
}

impl Drop for EtcdClient {
    fn drop(&mut self) {
        let stats = &self.stats;
        info!(
            "EtcdClient stats - Requests: {}, Success: {}, Failed: {}, Watches: {}, Services: {}",
            stats.total_requests.load(Ordering::SeqCst),
            stats.successful_requests.load(Ordering::SeqCst),
            stats.failed_requests.load(Ordering::SeqCst),
            stats.active_watches.load(Ordering::SeqCst),
            stats.service_registrations.load(Ordering::SeqCst)
        );
    }
}
